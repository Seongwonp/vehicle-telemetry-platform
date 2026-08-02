package com.telemetry.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

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
}
