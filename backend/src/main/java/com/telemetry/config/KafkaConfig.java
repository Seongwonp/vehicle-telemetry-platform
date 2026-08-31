package com.telemetry.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    private static final String TELEMETRY_TOPIC = "vehicle-telemetry";
    private static final String ANOMALY_TOPIC = "vehicle-anomaly-alerts";

    // @KafkaListener 메서드가 예외를 던지면(예: telemetryRepository.saveAll() 실패) 이 핸들러가
    // 받는다. Spring Boot가 자동 구성하는 기본값은 FixedBackOff(0, 9) — 즉 재시도 사이 간격
    // 없이 최대 10번을 리스너 스레드 안에서 그대로 다시 호출한다. InfluxDB 쓰기에 명시적
    // 타임아웃(5초, InfluxDbConfig 참고)을 걸어둔 상태에서 이 기본값이 그대로 적용되면,
    // 한 레코드가 계속 실패할 때 최악의 경우 5초 × 10번 = 50초 동안 그 파티션의 컨슈머
    // 스레드가 통째로 막힌다. 재시도 횟수와 간격을 명시적으로 좁게 잡아 상한을 줄인다.
    //
    // recoverer를 지정하지 않으면 재시도 소진 시 DefaultErrorHandler는 그 레코드를 로그만
    // 남기고 **버린다**(실제로 로그에 "Backoff FixedBackOff{...} exhausted for
    // vehicle-telemetry-0@1383"가 찍혔고 그 메시지는 DLQ에도 없었다). InfluxDB 쓰기 실패를
    // 조용히 커밋하던 버그를 고쳐놓고 같은 유형의 유실이 이 경로로 남아 있던 셈이라,
    // DeadLetterPublishingRecoverer로 원본을 DLQ에 남긴다.
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<String, String> kafkaOperations,
                                                 MeterRegistry meterRegistry) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaOperations,
            // 이 핸들러는 두 리스너가 공유하므로 원본 토픽별로 DLQ를 갈라야 한다. 기본
            // 규칙(<topic>-dlt)을 쓰면 이미 운영 중인 DLQ 토픽/알림과 어긋난다.
            // 파티션 -1은 "프로듀서가 정한다" — DLQ 파티션 수가 바뀌어도 안전하다.
            (record, exception) -> new TopicPartition(resolveDlqTopic(record.topic()), -1));

        return new DefaultErrorHandler((record, exception) -> {
            // alerts.yml의 DLQ 알림이 보는 메트릭 — 컨슈머 내부 sendToDlq()와 같은 이름으로
            // 올려야 이 경로로 나간 것도 같이 잡힌다.
            meterRegistry.counter("telemetry.kafka.dlq.published",
                "topic", resolveDlqTopic(record.topic())).increment();
            recoverer.accept(record, exception);
        }, new FixedBackOff(1000L, 2L));
    }

    private static String resolveDlqTopic(String sourceTopic) {
        return switch (sourceTopic) {
            case TELEMETRY_TOPIC -> "vehicle-telemetry-dlq";
            case ANOMALY_TOPIC -> "vehicle-anomaly-alerts-dlq";
            default -> sourceTopic + "-dlq";
        };
    }

    /**
     * 저장 경로(Kafka→InfluxDB) 전용 배치 리스너 팩토리.
     *
     * <p>전역 {@code spring.kafka.listener.type: batch} 대신 별도 팩토리를 두는 이유:
     * 이상 이벤트 리스너({@code consumeAnomalyAlerts})는 배치화 이득이 없어 레코드
     * 리스너로 남겨야 하는데, 전역 설정을 바꾸면 둘 다 배치가 된다.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> telemetryBatchListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        // 커스텀 팩토리는 Spring Boot의 listener 자동 설정(application.yml의
        // ack-mode/concurrency)을 물려받지 않으므로 여기서 같은 값을 명시한다.
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    // init-topics.sh에서 이미 생성하지만, 백엔드 단독 실행 시 자동 생성 보장
    @Bean
    public NewTopic vehicleTelemetryTopic() {
        return TopicBuilder.name("vehicle-telemetry")
            .partitions(3)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic vehicleAnomalyAlertsTopic() {
        return TopicBuilder.name("vehicle-anomaly-alerts")
            .partitions(3)
            .replicas(1)
            .build();
    }

    // DLQ 토픽 — 저장 실패한 원본 메시지를 격리해 유실을 방지한다 (재처리 컨슈머는 아직 없음).
    @Bean
    public NewTopic vehicleTelemetryDlqTopic() {
        return TopicBuilder.name("vehicle-telemetry-dlq")
            .partitions(1)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic vehicleAnomalyAlertsDlqTopic() {
        return TopicBuilder.name("vehicle-anomaly-alerts-dlq")
            .partitions(1)
            .replicas(1)
            .build();
    }

    // 이상 감지(Python anomaly-detector) 처리 실패 원본 메시지 격리용.
    // vehicle-telemetry-dlq는 Java 저장 경로 전용이라 재사용하면 어느 경로가
    // 실패했는지 구분이 안 돼 별도 토픽을 둔다. 발행 주체는 Python이지만, Python
    // 쪽엔 이 자동 생성 안전망이 없어 백엔드가 대신 보장한다.
    @Bean
    public NewTopic vehicleTelemetryAnomalyDlqTopic() {
        return TopicBuilder.name("vehicle-telemetry-anomaly-dlq")
            .partitions(1)
            .replicas(1)
            .build();
    }

    @Bean
    public NewTopic vehicleTelemetryMqttDlqTopic() {
        return TopicBuilder.name("vehicle-telemetry-mqtt-dlq")
            .partitions(1)
            .replicas(1)
            .build();
    }
}
