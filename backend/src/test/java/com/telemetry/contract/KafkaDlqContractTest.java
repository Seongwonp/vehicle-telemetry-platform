package com.telemetry.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.kafka.TelemetryConsumer;
import com.telemetry.service.AnomalyService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@Testcontainers(disabledWithoutDocker = true)
class KafkaDlqContractTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Test
    void malformedTelemetryIsPublishedToRealDlqBeforeAcknowledgment() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        try (AdminClient admin = AdminClient.create(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            admin.createTopics(List.of(new NewTopic("vehicle-telemetry-dlq", 1, (short) 1))).all().get();
        }

        var producerFactory = new DefaultKafkaProducerFactory<String, String>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.ACKS_CONFIG, "all"));
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        TelemetryConsumer listener = new TelemetryConsumer(
            mock(TelemetryRepository.class), mock(AnomalyService.class), new ObjectMapper(),
            kafkaTemplate, mock(SimpMessagingTemplate.class), new SimpleMeterRegistry());
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        listener.consumeForStorage(
            List.of(new org.apache.kafka.clients.consumer.ConsumerRecord<>(
                "vehicle-telemetry", 0, 42L, "TEST-001", "{broken-json")),
            acknowledgment);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.GROUP_ID_CONFIG, "contract-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of("vehicle-telemetry-dlq"));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records).singleElement().satisfies(record -> {
                assertThat(record.key()).isEqualTo("TEST-001");
                assertThat(record.value()).isEqualTo("{broken-json");
            });
        } finally {
            kafkaTemplate.destroy();
            producerFactory.destroy();
        }
        verify(acknowledgment).acknowledge();
    }
}
