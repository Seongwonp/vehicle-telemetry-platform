package com.telemetry.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

@Configuration
public class KafkaConfig {

    private static final String TELEMETRY_TOPIC = "vehicle-telemetry";
    private static final String ANOMALY_TOPIC = "vehicle-anomaly-alerts";

    // @KafkaListener 메서드가 예외를 던지면(예: telemetryRepository.saveAll() 실패) 이 핸들러가
    // 받는다. Spring Boot가 자동 구성하는 기본값은 FixedBackOff(0, 9) — 즉 재시도 사이 간격
    // 없이 최대 10번을 리스너 스레드 안에서 그대로 다시 호출한다.
    //
    // 예전에는 FixedBackOff(1000L, 2L)(3회 시도 / 약 2초)로 좁게 잡았다. "한 레코드가 계속
    // 실패하면 그 파티션이 오래 막힌다"는 걱정 때문이었는데, **그 걱정은 이 코드에
    // 해당하지 않는다** — consumeForStorage는 레코드별 역직렬화·변환 실패를 이미 개별
    // catch로 DLQ 처리하므로, 여기까지 올라오는 건 saveAll() 실패 = 의존성 장애다.
    // 개별 메시지가 나빠서가 아니라 InfluxDB가 죽어서 오는 것이다.
    //
    // 그런데 2초짜리 예산으로는 어떤 현실적인 장애도 못 견딘다. InfluxDB를 90초 정지시키자
    // 전체 트래픽의 47.6%(76,878건)가 DLQ로 갔다 — DLQ가 예외 경로가 아니라 주 경로가 됐고,
    // 복구가 사람이 Runbook 보고 돌리는 수동 절차가 됐다
    // (load-test/fault-injection/RESULT_20260904_fault_injection.md).
    //
    // 그래서 지수 백오프로 바꿨다. 앞쪽 재시도는 짧아서 순간적인 blip은 빠르게 넘기고,
    // 뒤로 갈수록 간격이 벌어져 긴 장애를 견딘다. 재시도는 멱등이라 안전하고
    // (load-test/storage-integrity/ — 재전달 68건에 InfluxDB 행 증가 0),
    // 재시도 중 쌓이는 lag은 이미 KafkaConsumerLagHigh 알림으로 드러난다.
    //
    // 예산은 재시도 횟수가 아니라 시간(budget-ms)으로 잡는다 — "몇 번 재시도하느냐"보다
    // "몇 초짜리 장애까지 견디느냐"가 실제로 정하고 싶은 값이다.
    // **다만 이 값은 벽시계 시간이 아니다** — 자세한 내용은 buildBackOff 주석 참고.
    //
    // recoverer를 지정하지 않으면 재시도 소진 시 DefaultErrorHandler는 그 레코드를 로그만
    // 남기고 **버린다**(실제로 로그에 "Backoff FixedBackOff{...} exhausted for
    // vehicle-telemetry-0@1383"가 찍혔고 그 메시지는 DLQ에도 없었다). InfluxDB 쓰기 실패를
    // 조용히 커밋하던 버그를 고쳐놓고 같은 유형의 유실이 이 경로로 남아 있던 셈이라,
    // DeadLetterPublishingRecoverer로 원본을 DLQ에 남긴다.
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
            // 이 핸들러는 두 리스너가 공유하므로 원본 토픽별로 DLQ를 갈라야 한다. 기본
            // 규칙(<topic>-dlt)을 쓰면 이미 운영 중인 DLQ 토픽/알림과 어긋난다.
            // 파티션 -1은 "프로듀서가 정한다" — DLQ 파티션 수가 바뀌어도 안전하다.
            (record, exception) -> new TopicPartition(resolveDlqTopic(record.topic()), -1));

        // DLQ 발행 결과를 실제로 확인한다. 기본값대로 두면 발행이 실패해도 recoverer가
        // 성공으로 간주해 원본 offset을 커밋해버린다 — 메시지가 DLQ에도 없고 원본도
        // 지나가버리는 조용한 유실이다(계약 테스트 KafkaStorageFailureContractTest로 발견).
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(5));

        return new DefaultErrorHandler((record, exception) -> {
            // alerts.yml의 DLQ 알림이 보는 메트릭 — 컨슈머 내부 sendToDlq()와 같은 이름으로
            // 올려야 이 경로로 나간 것도 같이 잡힌다.
            meterRegistry.counter("telemetry.kafka.dlq.published",
                "topic", resolveDlqTopic(record.topic())).increment();
            recoverer.accept(record, exception);
        }, buildBackOff(initialIntervalMs, multiplier, maxIntervalMs, budgetMs));
    }

    /**
     * 지수 백오프. 예산을 재시도 <b>횟수</b>가 아니라 <b>시간</b>으로 잡는다 —
     * 정하고 싶은 값이 "몇 번 재시도하느냐"가 아니라 "몇 초짜리 장애까지 견디느냐"이기
     * 때문이다.
     *
     * <p><b>주의 — 이 예산은 벽시계 시간이 아니다.</b>
     * {@link ExponentialBackOff#setMaxElapsedTime}이 세는 것은 <b>백오프로 쉰 시간의
     * 합</b>이고, 리스너가 실패하는 데 쓴 시간은 포함되지 않는다. 그래서 실제로 견디는
     * 시간은 예산보다 훨씬 길 수 있다.
     *
     * <p>PostgreSQL을 <b>300초</b> 정지시켜 재보니 DLQ가 <b>0건</b>이었다. 예산이 180초인데도
     * 그렇다. 시도마다 HikariCP {@code connectionTimeout}(30초)을 기다리느라 실패 자체에
     * 30초씩 쓰였고, 백오프 합(1+2+4+8+16+30…)은 그 사이 180초에 도달하지 못했기 때문이다.
     * 기본값 기준 실효 내성은 대략 <b>8분</b> 수준이다
     * ({@code load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md}).
     *
     * <p>따라서 "예산 &lt; {@code max.poll.interval.ms}"라는 관계도 그대로 성립하지 않는다.
     * 컨슈머가 쫓겨나는 기준은 <b>poll 사이의 벽시계 시간</b>이라, 리스너가 오래 붙잡히면
     * 예산과 무관하게 리밸런싱이 돌 수 있다. 300초 장애에서는 실측으로 0건이었다.
     *
     * <p>{@code budgetMs <= 0}이면 재시도 없이 곧바로 recoverer로 보낸다
     * (설정으로 예전 동작에 가깝게 되돌릴 수 있어야 A/B 측정이 된다).
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
