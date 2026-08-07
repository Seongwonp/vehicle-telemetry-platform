package com.telemetry.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    // @KafkaListener 메서드가 예외를 던지면(예: telemetryRepository.save() 실패) 이 핸들러가
    // 받는다. Spring Boot가 자동 구성하는 기본값은 FixedBackOff(0, 9) — 즉 재시도 사이 간격
    // 없이 최대 10번을 리스너 스레드 안에서 그대로 다시 호출한다. InfluxDB 쓰기에 명시적
    // 타임아웃(5초, InfluxDbConfig 참고)을 걸어둔 상태에서 이 기본값이 그대로 적용되면,
    // 한 레코드가 계속 실패할 때 최악의 경우 5초 × 10번 = 50초 동안 그 파티션의 컨슈머
    // 스레드가 통째로 막힌다 — fix 검증 중 처리량이 초당 7-8건까지 떨어진 원인으로 의심됨.
    // 재시도 횟수와 간격을 명시적으로 좁게 잡아 이 블로킹 상한을 줄인다.
    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, 2L));
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
