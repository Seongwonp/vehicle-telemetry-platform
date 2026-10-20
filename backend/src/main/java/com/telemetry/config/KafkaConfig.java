package com.telemetry.config;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

@Slf4j
@Configuration
public class KafkaConfig {

    private static final String TELEMETRY_TOPIC = "vehicle-telemetry";
    private static final String ANOMALY_TOPIC = "vehicle-anomaly-alerts";

    // 여기까지 올라오는 예외는 saveAll() 실패, 즉 의존성 장애다(레코드별 변환 실패는 리스너가 DLQ로 보낸다).
    // 그래서 짧은 고정 재시도 대신 시간 예산을 둔 지수 백오프로 장애를 견딘다. 재시도는 멱등이라 안전하다.
    // recoverer가 없으면 재시도 소진 시 레코드를 로그만 남기고 버리므로 DLQ recoverer는 필수다.
    // 경위: load-test/fault-injection/RESULT_20260904_fault_injection.md
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaOperations<String, String> kafkaOperations,
            MeterRegistry meterRegistry,
            @Value("${telemetry.kafka.retry.initial-interval-ms:1000}") long initialIntervalMs,
            @Value("${telemetry.kafka.retry.multiplier:2.0}") double multiplier,
            @Value("${telemetry.kafka.retry.max-interval-ms:30000}") long maxIntervalMs,
            @Value("${telemetry.kafka.retry.budget-ms:180000}") long budgetMs) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaOperations,
            // 두 리스너가 공유하므로 원본 토픽별로 DLQ를 가른다(기본 <topic>-dlt는 기존 토픽/알림과 다름).
            // 파티션 -1은 프로듀서가 고른다는 뜻이라 DLQ 파티션 수가 바뀌어도 안전하다.
            (record, exception) -> new TopicPartition(resolveDlqTopic(record.topic()), -1));

        // 발행 결과를 확인하지 않으면 DLQ 발행이 실패해도 원본 offset이 커밋돼 조용히 유실된다.
        // (KafkaStorageFailureContractTest)
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(5));

        return new DefaultErrorHandler((record, exception) -> {
            // alerts.yml의 DLQ 알림이 보는 메트릭. 컨슈머 내부 sendToDlq()와 이름·의미를 맞춘다.
            // 성공 카운터는 accept()가 예외 없이 끝난 뒤에만 올린다.
            String dlqTopic = resolveDlqTopic(record.topic());
            com.telemetry.metrics.ContractMetrics.dlqPublishAttempt(meterRegistry, dlqTopic);
            // Spring Kafka의 재시도·복구 로그는 DEBUG라 현재 로그 레벨(WARN)에선 안 보인다.
            // DLQ 이동 흔적을 남기려면 여기서 직접 찍어야 한다.
            log.warn("[DLQ 이동] 재시도 예산 소진 — {}-{}@{} → {} ({}: {})",
                record.topic(), record.partition(), record.offset(),
                resolveDlqTopic(record.topic()),
                exception.getClass().getSimpleName(), rootMessage(exception));
            try {
                recoverer.accept(record, exception);
            } catch (RuntimeException publishFailure) {
                com.telemetry.metrics.ContractMetrics.dlqPublishFailed(meterRegistry, dlqTopic, publishFailure);
                throw publishFailure;
            }
            com.telemetry.metrics.ContractMetrics.dlqPublished(meterRegistry, dlqTopic);
        }, buildBackOff(initialIntervalMs, multiplier, maxIntervalMs, budgetMs));
    }

    /**
     * 지수 백오프. 예산은 재시도 횟수가 아니라 시간으로 잡는다.
     *
     * <p>주의: {@link ExponentialBackOff#setMaxElapsedTime}은 백오프로 쉰 시간의 합만 센다.
     * 리스너가 실패하는 데 쓴 시간은 빠지므로 실제로 견디는 시간은 예산보다 길다.
     * 재시도 중에는 retryBatch가 파티션을 pause한 채 poll을 계속 부르므로
     * {@code max.poll.interval.ms} 초과로 리밸런싱되지 않는다.
     * 근거: {@code load-test/long-outage/RESULT_20260906_influxdb_repeat.md}
     *
     * <p>{@code budgetMs <= 0}이면 재시도 없이 바로 recoverer로 보낸다(A/B 비교용).
     */
    private static BackOff buildBackOff(long initialIntervalMs, double multiplier,
                                        long maxIntervalMs, long budgetMs) {
        if (budgetMs <= 0) {
            return new FixedBackOff(0L, 0L);
        }
        ExponentialBackOff backOff = new ExponentialBackOff(initialIntervalMs, multiplier);
        backOff.setMaxInterval(maxIntervalMs);
        backOff.setMaxElapsedTime(budgetMs);
        return backOff;
    }

    /** ListenerExecutionFailedException 안쪽 원인의 메시지. 바깥 것만 찍으면 실패 이유가 안 보인다. */
    private static String rootMessage(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message == null ? cause.getClass().getName() : message;
    }

    private static String resolveDlqTopic(String sourceTopic) {
        return switch (sourceTopic) {
            case TELEMETRY_TOPIC -> "vehicle-telemetry-dlq";
            case ANOMALY_TOPIC -> "vehicle-anomaly-alerts-dlq";
            default -> sourceTopic + "-dlq";
        };
    }

    /**
     * 배치 리스너 팩토리. <b>두 리스너가 모두 이것을 쓴다</b>
     * ({@code consumeForStorage}, {@code consumeAnomalyAlerts}).
     *
     * 전역 {@code listener.type: batch} 대신 명시적 팩토리를 둬서 배치 여부가 리스너에서 보이게 한다.
     *
     * <p>{@code configurer}로 {@code spring.kafka.listener.*}를 물려받는다. 여기서 하드코딩하면
     * yml 값이 아무 효과 없는 설정이 된다.
     *
     * <p>순서 주의: {@code configure()} 뒤에 배치 여부와 에러 핸들러를 덮어써야 한다
     * (configurer가 single 타입과 Boot 기본 에러 핸들러를 넣는다).
     * 제네릭 {@code <Object, Object>}는 Boot configurer 시그니처에 맞춘 것이다.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> telemetryBatchListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }

    /**
     * {@code group.instance.id}가 빈 값이면 그 속성을 <b>아예 제거</b>한다 — 즉 정적 멤버십을 끈다.
     *
     * Kafka는 빈 문자열을 거부해 기동이 실패하므로, 재빌드 없이 끌 수 있게 빈 값을 끄기로 해석한다.
     * 기본값은 켜짐. 끄면 리밸런싱 폭풍 위험이 돌아오므로 근거 없이 끄지 않는다. 이유: ADR-024
     */
    @Bean
    public DefaultKafkaConsumerFactoryCustomizer staticMembershipOffWhenBlank() {
        return factory -> {
            Object id = factory.getConfigurationProperties().get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
            if (id == null || id.toString().isBlank()) {
                factory.removeConfig(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
                log.warn("group.instance.id가 비어 있어 정적 멤버십을 끕니다 — "
                    + "스케일 다운은 즉시 재할당되지만 리밸런싱 폭풍 위험이 돌아옵니다");
            }
        };
    }

    // init-topics.sh와 겹치지만 백엔드 단독 실행을 위해 둔다.
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

    // Python 이상 감지기의 DLQ. 실패 경로를 구분하려고 저장 DLQ와 분리한다.
    // Python 쪽엔 토픽 자동 생성이 없어 백엔드가 대신 만든다.
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
