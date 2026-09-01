package com.telemetry.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.write.Point;
import com.telemetry.config.KafkaConfig;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.kafka.TelemetryConsumer;
import com.telemetry.service.AnomalyService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Testcontainers(disabledWithoutDocker = true)
class KafkaStorageFailureContractTest {

    private static final String SOURCE_TOPIC = "vehicle-telemetry";
    private static final String DLQ_TOPIC = "vehicle-telemetry-dlq";
    private static final String PAYLOAD = """
        {"vehicle_id":"TEST-001","timestamp":"2026-08-21T00:00:00.123Z",\
        "speed":80.0,"rpm":2000,"engine_temp":90.0,"throttle_position":30.0,\
        "fuel_level":50.0,"battery_voltage":13.5}
        """;

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private final String groupId = "storage-failure-contract-" + UUID.randomUUID();
    private MessageListenerContainer listenerContainer;
    private KafkaTemplate<String, String> kafkaTemplate;
    private DefaultKafkaProducerFactory<String, String> producerFactory;

    @BeforeAll
    static void createTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                new NewTopic(SOURCE_TOPIC, 1, (short) 1),
                new NewTopic(DLQ_TOPIC, 1, (short) 1)
            )).all().get(10, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void tearDown() {
        if (listenerContainer != null) listenerContainer.stop();
        if (kafkaTemplate != null) kafkaTemplate.destroy();
        if (producerFactory != null) producerFactory.destroy();
    }

    @Test
    void permanentInfluxFailurePublishesToDlqBeforeCommittingSourceOffset() throws Exception {
        kafkaTemplate = createKafkaTemplate();

        TelemetryRepository telemetryRepository = mock(TelemetryRepository.class);
        given(telemetryRepository.toPoint(any())).willReturn(Point.measurement("vehicle_telemetry"));
        doThrow(new RuntimeException("InfluxDB unavailable"))
            .when(telemetryRepository).saveAll(any());

        TelemetryConsumer listener = new TelemetryConsumer(
            telemetryRepository,
            mock(AnomalyService.class),
            new ObjectMapper(),
            kafkaTemplate,
            mock(SimpMessagingTemplate.class),
            new SimpleMeterRegistry()
        );
        listenerContainer = startListener(listener);

        SendResult<String, String> sourceResult = kafkaTemplate
            .send(SOURCE_TOPIC, "TEST-001", PAYLOAD)
            .get(10, TimeUnit.SECONDS);

        var dlqRecord = pollRecordWithKey(DLQ_TOPIC, "TEST-001");
        assertThat(dlqRecord.key()).isEqualTo("TEST-001");
        assertThat(dlqRecord.value()).isEqualTo(PAYLOAD);
        // FixedBackOff(1000, 2) — 최초 1회 + 재시도 2회
        verify(telemetryRepository, times(3)).saveAll(any());

        TopicPartition sourcePartition = new TopicPartition(
            SOURCE_TOPIC, sourceResult.getRecordMetadata().partition());
        long committedOffset = awaitCommittedOffset(sourcePartition, Duration.ofSeconds(10));
        assertThat(committedOffset).isEqualTo(sourceResult.getRecordMetadata().offset() + 1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void dlqPublishFailureDoesNotCommitSourceOffset() throws Exception {
        kafkaTemplate = createKafkaTemplate();
        KafkaTemplate<String, String> failingDlqTemplate = mock(KafkaTemplate.class);
        CompletableFuture<SendResult<String, String>> failedSend = new CompletableFuture<>();
        failedSend.completeExceptionally(new RuntimeException("DLQ broker ACK failed"));
        org.mockito.Mockito.doReturn(failedSend)
            .when(failingDlqTemplate).send(any(ProducerRecord.class));

        TelemetryRepository telemetryRepository = mock(TelemetryRepository.class);
        given(telemetryRepository.toPoint(any())).willReturn(Point.measurement("vehicle_telemetry"));
        doThrow(new RuntimeException("InfluxDB unavailable"))
            .when(telemetryRepository).saveAll(any());
        TelemetryConsumer listener = new TelemetryConsumer(
            telemetryRepository,
            mock(AnomalyService.class),
            new ObjectMapper(),
            kafkaTemplate,
            mock(SimpMessagingTemplate.class),
            new SimpleMeterRegistry()
        );
        listenerContainer = startListener(listener, failingDlqTemplate);

        SendResult<String, String> sourceResult = kafkaTemplate
            .send(SOURCE_TOPIC, "TEST-002", PAYLOAD.replace("TEST-001", "TEST-002"))
            .get(10, TimeUnit.SECONDS);

        verify(telemetryRepository, timeout(8_000).atLeast(3)).saveAll(any());
        TopicPartition sourcePartition = new TopicPartition(
            SOURCE_TOPIC, sourceResult.getRecordMetadata().partition());

        // 핵심 계약: DLQ에 넣지 못했으면 원본을 건너뛰면 안 된다.
        // offset+1이 커밋되면 다음 폴에서 이 메시지를 지나쳐 조용히 유실된다.
        // 커밋이 아예 없거나(null), 있어도 이 레코드 offset 이하여야 재전달된다.
        Long committed = committedOffset(sourcePartition);
        long recordOffset = sourceResult.getRecordMetadata().offset();
        assertThat(committed == null || committed <= recordOffset)
            .withFailMessage("DLQ 발행 실패에도 원본 offset이 커밋돼 메시지가 스킵된다 — "
                + "committed=%s recordOffset=%s", committed, recordOffset)
            .isTrue();
    }

    private MessageListenerContainer startListener(TelemetryConsumer listener) {
        return startListener(listener, kafkaTemplate);
    }

    private MessageListenerContainer startListener(
        TelemetryConsumer listener,
        KafkaTemplate<String, String> recoveryTemplate
    ) {
        Map<String, Object> consumerProperties = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, groupId,
            // 두 테스트가 같은 Kafka 컨테이너와 토픽을 공유한다. earliest면 앞 테스트가
            // 일부러 커밋하지 않고 남긴 레코드까지 다시 집어 DLQ가 오염된다.
            // 리스너 할당(waitForAssignment)을 먼저 끝낸 뒤 발행하므로 latest로 충분하다.
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        var consumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerProperties);
        ContainerProperties properties = new ContainerProperties(SOURCE_TOPIC);
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // 저장 경로는 배치 리스너다(KafkaConfig.telemetryBatchListenerContainerFactory).
        // 배치 리스너 타입을 넘겨야 컨테이너가 배치 모드로 동작한다.
        properties.setMessageListener(
            (BatchAcknowledgingMessageListener<String, String>) (records, acknowledgment) ->
                listener.consumeForStorage(records, acknowledgment));

        var container = new ConcurrentMessageListenerContainer<>(consumerFactory, properties);
        container.setCommonErrorHandler(new KafkaConfig().kafkaErrorHandler(
            recoveryTemplate, new SimpleMeterRegistry()));
        container.start();
        ContainerTestUtils.waitForAssignment(container, 1);
        return container;
    }

    private KafkaTemplate<String, String> createKafkaTemplate() {
        producerFactory = new DefaultKafkaProducerFactory<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.ACKS_CONFIG, "all"
        ));
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * DLQ에서 지정한 key의 레코드를 찾는다.
     *
     * <p>DLQ 토픽도 두 테스트가 공유하므로 "레코드 1건"을 단정하면 앞 테스트가 남긴 것에
     * 걸린다. 이번에 넣은 key를 찾을 때까지 폴링한다.
     */
    private org.apache.kafka.clients.consumer.ConsumerRecord<String, String> pollRecordWithKey(
        String topic, String key
    ) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "dlq-observer-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofSeconds(2))) {
                    if (key.equals(record.key())) return record;
                }
            }
            throw new AssertionError("DLQ에서 key=" + key + " 레코드를 찾지 못했다");
        }
    }

    private long awaitCommittedOffset(TopicPartition partition, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        try (AdminClient admin = AdminClient.create(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            while (System.nanoTime() < deadline) {
                var offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                if (offsets.containsKey(partition)) return offsets.get(partition).offset();
                Thread.sleep(100);
            }
        }
        throw new AssertionError("committed offset was not observed for " + partition);
    }

    private Long committedOffset(TopicPartition partition) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            return offsets.containsKey(partition) ? offsets.get(partition).offset() : null;
        }
    }
}
