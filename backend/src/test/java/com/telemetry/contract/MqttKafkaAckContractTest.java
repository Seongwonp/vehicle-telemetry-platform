package com.telemetry.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.kafka.TelemetryProducer;
import com.telemetry.kafka.TelemetrySpool;
import com.telemetry.mqtt.MqttInvalidMessagePublisher;
import com.telemetry.mqtt.MqttMessageHandler;
import com.telemetry.support.TestDecoders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

// Docker image builders have no daemon; the host CI job separately enforces zero skipped contracts.
@Testcontainers(disabledWithoutDocker = true)
class MqttKafkaAckContractTest {
    @Container static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    @Container static final GenericContainer<?> BROKER = new GenericContainer<>("eclipse-mosquitto:2.0")
        .withExposedPorts(1883).withCopyToContainer(Transferable.of(
            "listener 1883\nallow_anonymous true\nlog_type all\nmax_inflight_messages 20\n"),
            "/mosquitto/config/mosquitto.conf").waitingFor(Wait.forListeningPort());
    private static final String SUBACK_TO_ADAPTER = "Sending SUBACK to normal-ack";
    @TempDir Path temporary;

    @Test @SuppressWarnings("unchecked")
    void compareNormalDeliveryAndRecoverKafkaOutageThroughSpool() throws Exception {
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("vehicle-telemetry", 3, (short) 1))).all().get(20, TimeUnit.SECONDS);
        }
        var producerFactory = new DefaultKafkaProducerFactory<String, String>(Map.of(
            "bootstrap.servers", KAFKA.getBootstrapServers(), "key.serializer", StringSerializer.class,
            "value.serializer", StringSerializer.class, "acks", "all", "enable.idempotence", true,
            "request.timeout.ms", 1000, "delivery.timeout.ms", 3000, "max.block.ms", 3000));
        var kafka = new KafkaTemplate<String, String>(producerFactory);
        var registry = new SimpleMeterRegistry();
        var spool = new TelemetrySpool(temporary.toString());
        var producer = new TelemetryProducer(kafka, new ObjectMapper(), spool, registry, 100);
        var handler = new MqttMessageHandler(producer, TestDecoders.telemetryDecoder(), registry,
            new MqttInvalidMessagePublisher(kafka, new ObjectMapper(), registry));
        String uri = "tcp://" + BROKER.getHost() + ":" + BROKER.getMappedPort(1883);
        var options = new MqttConnectOptions();
        options.setServerURIs(new String[]{uri}); options.setCleanSession(false);
        var factory = new DefaultMqttPahoClientFactory(); factory.setConnectionOptions(options);
        var adapter = new MqttPahoMessageDrivenChannelAdapter("normal-ack", factory, "vehicle/telemetry/ACK-001");
        adapter.setBeanFactory(new DefaultListableBeanFactory()); adapter.setQos(1);
        var channel = new DirectChannel();
        channel.subscribe(message -> handler.handle((Message<String>) message));
        adapter.setOutputChannel(channel); adapter.afterPropertiesSet();
        Set<String> observed = new HashSet<>();
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                 "bootstrap.servers", KAFKA.getBootstrapServers(), "group.id", "ack-contract-" + UUID.randomUUID(),
                 "auto.offset.reset", "earliest", "key.deserializer", StringDeserializer.class,
                 "value.deserializer", StringDeserializer.class));
             var publisher = new MqttAsyncClient(uri, "pub-normal", new MemoryPersistence())) {
            consumer.subscribe(List.of("vehicle-telemetry"));
            publisher.connect().waitForCompletion(10000);
            // Alternate modes: small local comparison, not a maximum-throughput benchmark.
            int sequence = 0;
            for (boolean manual : new boolean[]{false, true, true, false}) {
                adapter.setManualAcks(manual); startAndAwaitSubscription(adapter);
                int target = sequence + 100;
                long start = System.nanoTime();
                for (; sequence < target; sequence++) {
                    publisher.publish("vehicle/telemetry/ACK-001", payload(sequence).getBytes(java.nio.charset.StandardCharsets.UTF_8), 1, false)
                        .waitForCompletion(10000);
                }
                await().atMost(Duration.ofSeconds(30)).until(() -> {
                    consumer.poll(Duration.ofMillis(100)).forEach(record -> observed.add(record.value()));
                    return observed.size() == target;
                });
                System.out.printf("ACK_LOCAL_COMPARE manual=%s messages=100 elapsedMs=%d%n", manual,
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                assertThat(spool.depth()).isZero();
                adapter.stop();
            }
            adapter.setManualAcks(true); startAndAwaitSubscription(adapter);
            KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
            try {
                publisher.publish("vehicle/telemetry/ACK-001", payload(400).getBytes(java.nio.charset.StandardCharsets.UTF_8), 1, false)
                    .waitForCompletion(10000);
                await().atMost(Duration.ofSeconds(20)).until(() -> spool.depth() == 1);
            } finally { KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec(); }
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                producer.retryPending();
                consumer.poll(Duration.ofMillis(100)).forEach(record -> observed.add(record.value()));
                return observed.size() == 401 && spool.depth() == 0;
            });
            System.out.println("ACK_KAFKA_OUTAGE published=1 spooled=1 recovered=1 uniqueTotal=401");
            publisher.disconnect().waitForCompletion(10000);
        } finally { adapter.stop(); adapter.destroy(); kafka.destroy(); producerFactory.destroy(); }
    }

    // The broker log accumulates across start/stop cycles, so a plain contains() is already true after the
    // first start. Wait for one more SUBACK than before this start() to bind the wait to this subscription.
    private static void startAndAwaitSubscription(MqttPahoMessageDrivenChannelAdapter adapter) {
        int before = occurrences(BROKER.getLogs(), SUBACK_TO_ADAPTER);
        adapter.start();
        await().atMost(Duration.ofSeconds(10))
            .until(() -> occurrences(BROKER.getLogs(), SUBACK_TO_ADAPTER) > before);
    }

    private static int occurrences(String text, String needle) {
        int count = 0;
        for (int at = text.indexOf(needle); at >= 0; at = text.indexOf(needle, at + needle.length())) count++;
        return count;
    }

    private static String payload(int sequence) {
        return """
            {"vehicle_id":"ACK-001","timestamp":"2026-10-01T00:00:%02d.%03dZ","speed":10,"rpm":900,
            "engine_temp":85,"throttle_position":12,"fuel_level":55,"battery_voltage":13.8,
            "gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}
            """.formatted(sequence / 1000, sequence % 1000);
    }
}
