package com.telemetry.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.kafka.TelemetryProducer;
import com.telemetry.kafka.TelemetrySpool;
import com.telemetry.mqtt.MqttInvalidMessagePublisher;
import com.telemetry.mqtt.MqttMessageHandler;
import com.telemetry.support.TestDecoders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 저장 확인을 기다리는 동안 MQTT TCP 연결이 끊겼다 이어지는 경계(실험 D, ADR-029).
 *
 * <p>실제 Mosquitto와 실제 Spring Integration 어댑터·MqttMessageHandler를 쓰고, 연결 단절은
 * 테스트 안의 TCP 중계기를 닫아 만든다. Kafka 전송 완료만 통제한 future다 — 실제 Kafka 장애가 아니다.
 */
// Docker image builders have no daemon; the host CI job separately enforces zero skipped contracts.
@Testcontainers(disabledWithoutDocker = true)
class MqttReconnectAckContractTest {
    @Container static final GenericContainer<?> BROKER = new GenericContainer<>("eclipse-mosquitto:2.0")
        .withExposedPorts(1883).withCopyToContainer(Transferable.of(
            "listener 1883\nallow_anonymous true\nlog_type all\nmax_inflight_messages 20\n"),
            "/mosquitto/config/mosquitto.conf").waitingFor(Wait.forListeningPort());
    @TempDir Path temporary;

    /** client와 broker 사이에 끼는 단순 TCP 중계기. drop()은 현재 연결만 끊고 새 접속은 계속 받는다. */
    static final class Proxy implements AutoCloseable {
        final ServerSocket server;
        final String upstreamHost;
        final int upstreamPort;
        final List<Socket> sockets = new CopyOnWriteArrayList<>();
        volatile boolean open = true;

        Proxy(String host, int port) throws IOException {
            server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            upstreamHost = host;
            upstreamPort = port;
            Thread acceptor = new Thread(this::acceptLoop, "mqtt-proxy-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() { return server.getLocalPort(); }

        private void acceptLoop() {
            while (open) {
                try {
                    Socket client = server.accept();
                    Socket upstream = new Socket(upstreamHost, upstreamPort);
                    client.setTcpNoDelay(true);
                    upstream.setTcpNoDelay(true);
                    sockets.add(client);
                    sockets.add(upstream);
                    pump(client, upstream);
                    pump(upstream, client);
                } catch (IOException e) {
                    if (!open) return;
                }
            }
        }

        private void pump(Socket from, Socket to) {
            Thread t = new Thread(() -> {
                byte[] buffer = new byte[8192];
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    int n;
                    while ((n = in.read(buffer)) >= 0) { out.write(buffer, 0, n); out.flush(); }
                } catch (IOException ignored) {
                } finally { closeQuietly(from); closeQuietly(to); }
            }, "mqtt-proxy-pump");
            t.setDaemon(true);
            t.start();
        }

        void drop() { sockets.forEach(Proxy::closeQuietly); sockets.clear(); }

        static void closeQuietly(Socket s) { try { s.close(); } catch (IOException ignored) { } }

        @Override public void close() {
            open = false;
            drop();
            try { server.close(); } catch (IOException ignored) { }
        }
    }

    private static String payload(int sequence) {
        return """
            {"vehicle_id":"RECON-001","timestamp":"2026-10-01T00:00:00.%03dZ",
             "speed":10,"rpm":900,"engine_temp":85,"throttle_position":12,"fuel_level":55,
             "battery_voltage":13.8,"gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}
            """.formatted(sequence);
    }

    private static int sequenceOf(String json) {
        int at = json.indexOf("00:00:00.") + "00:00:00.".length();
        return Integer.parseInt(json.substring(at, at + 3));
    }

    @SuppressWarnings("unchecked")
    private static SendResult<String, String> sendResult() {
        var result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(
            new org.apache.kafka.clients.producer.RecordMetadata(
                new org.apache.kafka.common.TopicPartition("vehicle-telemetry", 0), 0, 0, 0L, 0, 0));
        return result;
    }

    private static long ms(long t0) { return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0); }

    @Test @SuppressWarnings("unchecked")
    void staleAckAfterReconnectDoesNotLoseOrBlockMessages() throws Exception {
        String brokerHost = BROKER.getHost();
        int brokerPort = BROKER.getMappedPort(1883);
        String clientId = "reconnect-" + UUID.randomUUID();
        List<String> timeline = Collections.synchronizedList(new ArrayList<>());
        long t0 = System.nanoTime();
        // sequence -> 그 시퀀스로 만들어진 모든 전송 future (재전달되면 둘 이상)
        Map<Integer, List<CompletableFuture<SendResult<String, String>>>> sends = new ConcurrentHashMap<>();
        AtomicBoolean autoComplete = new AtomicBoolean(false);

        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            int sequence = sequenceOf(invocation.getArgument(2, String.class));
            var future = new CompletableFuture<SendResult<String, String>>();
            var list = sends.computeIfAbsent(sequence, k -> new CopyOnWriteArrayList<>());
            list.add(future);
            timeline.add("%5dms send seq=%d attempt=%d thread=%s".formatted(
                ms(t0), sequence, list.size(), Thread.currentThread().getName()));
            if (autoComplete.get()) future.complete(sendResult());
            return future;
        });
        var registry = new SimpleMeterRegistry();
        var handler = new MqttMessageHandler(new TelemetryProducer(kafka, new ObjectMapper(),
            new TelemetrySpool(temporary.resolve("spool").toString()), registry, 100),
            TestDecoders.telemetryDecoder(), registry,
            new MqttInvalidMessagePublisher(kafka, new ObjectMapper(), registry));

        try (var proxy = new Proxy(brokerHost, brokerPort)) {
            var options = new MqttConnectOptions();
            options.setServerURIs(new String[]{"tcp://127.0.0.1:" + proxy.port()});
            options.setCleanSession(false);
            options.setAutomaticReconnect(true);
            options.setMaxReconnectDelay(1000);
            options.setKeepAliveInterval(60);
            var factory = new DefaultMqttPahoClientFactory();
            factory.setConnectionOptions(options);
            var adapter = new MqttPahoMessageDrivenChannelAdapter(clientId, factory, "vehicle/telemetry/RECON-001");
            adapter.setBeanFactory(new DefaultListableBeanFactory());
            adapter.setQos(1);
            adapter.setManualAcks(true);
            var channel = new DirectChannel();
            channel.subscribe(message -> {
                try {
                    handler.handle((Message<String>) message);
                } catch (RuntimeException e) {
                    timeline.add("%5dms handler threw %s".formatted(ms(t0), e.getMessage()));
                    throw e;
                }
            });
            // 운영에서는 Spring 컨텍스트가 연결 끊김 이벤트를 @EventListener로 전달한다.
            adapter.setApplicationEventPublisher(event -> {
                if (event instanceof org.springframework.integration.mqtt.event.MqttConnectionFailedEvent failed) {
                    timeline.add("%5dms MqttConnectionFailedEvent".formatted(ms(t0)));
                    handler.onConnectionLost(failed);
                }
            });
            adapter.setOutputChannel(channel);
            adapter.afterPropertiesSet();
            adapter.start();
            await().atMost(Duration.ofSeconds(15))
                .until(() -> BROKER.getLogs().contains("Received SUBSCRIBE from " + clientId));

            try (var publisher = new MqttClient("tcp://" + brokerHost + ":" + brokerPort,
                "pub-" + UUID.randomUUID(), new MemoryPersistence())) {
                publisher.connect();
                // seq 1: 저장 확인 대기 중에 연결이 끊긴다. seq 2·3: 이미 도착했거나 브로커에 남아 있다.
                for (int seq = 1; seq <= 3; seq++) {
                    publisher.publish("vehicle/telemetry/RECON-001", payload(seq).getBytes(StandardCharsets.UTF_8), 1, false);
                }
                await().atMost(Duration.ofSeconds(10)).until(() -> sends.containsKey(1));
                int connectsBefore = count(BROKER.getLogs(), " as " + clientId + " (");
                proxy.drop();
                timeline.add("%5dms connection dropped (seq 1 receipt pending)".formatted(ms(t0)));
                try {
                    // 저장 확인 대기(최대 150초)가 재연결을 막지 않아야 한다. 수정 전에는 여기서 시간 초과.
                    await().atMost(Duration.ofSeconds(30)).until(
                        () -> count(BROKER.getLogs(), " as " + clientId + " (") > connectsBefore);
                } catch (RuntimeException e) {
                    timeline.forEach(System.out::println);
                    System.out.println(BROKER.getLogs());
                    throw e;
                }
                timeline.add("%5dms broker saw reconnect".formatted(ms(t0)));

                // 재접속 뒤에 옛 연결 기준 저장이 완료된다 — 이 시점의 ACK가 새 연결에서 무엇이 되는지가 관찰 대상이다.
                autoComplete.set(true);
                sends.values().forEach(list -> list.forEach(f -> f.complete(sendResult())));
                timeline.add("%5dms pending receipts completed".formatted(ms(t0)));

                // (b) 유실 불가: 세 메시지 모두 최소 한 번 이상 저장 경로에 도달해야 한다. 중복은 허용.
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(sends.keySet()).containsExactlyInAnyOrder(1, 2, 3));
                Thread.sleep(3000);
                sends.values().forEach(list -> list.forEach(f -> f.complete(sendResult())));
                Thread.sleep(2000);
                publisher.disconnect();
            }
            adapter.stop();
            adapter.destroy();
            // (c) 저장 확인된 메시지가 모두 ACK됐다면 같은 client ID로 다시 붙어도 재전달이 없다.
            var redelivered = new LinkedBlockingQueue<Integer>();
            try (var resumed = new MqttClient("tcp://" + brokerHost + ":" + brokerPort, clientId, new MemoryPersistence())) {
                resumed.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        redelivered.add(sequenceOf(new String(message.getPayload(), StandardCharsets.UTF_8)));
                    }
                });
                var resumeOptions = new MqttConnectOptions();
                resumeOptions.setCleanSession(false);
                resumed.connect(resumeOptions);
                Thread.sleep(3000);
                resumed.disconnect();
            }
            timeline.forEach(System.out::println);
            sends.forEach((seq, list) -> System.out.printf("RECONNECT_BOUNDARY seq=%d sendAttempts=%d%n", seq, list.size()));
            System.out.println("RECONNECT_BOUNDARY redeliveredAfterResume=" + redelivered);
            assertThat(sends.keySet()).containsExactlyInAnyOrder(1, 2, 3);
            assertThat(redelivered).as("저장 확인된 메시지는 모두 ACK되어 재전달되지 않아야 한다").isEmpty();
        }
    }

    private static int count(String text, String needle) {
        int count = 0, from = 0;
        while ((from = text.indexOf(needle, from)) >= 0) { count++; from += needle.length(); }
        return count;
    }
}
