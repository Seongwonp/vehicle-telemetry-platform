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
}
