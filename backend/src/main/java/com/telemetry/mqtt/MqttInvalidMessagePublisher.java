package com.telemetry.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class MqttInvalidMessagePublisher {

    public static final String TOPIC = "vehicle-telemetry-mqtt-dlq";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public void publish(String mqttTopic, String payload, String reason) {
        String envelope;
        try {
            envelope = objectMapper.writeValueAsString(Map.of(
                "mqtt_topic", mqttTopic == null ? "" : mqttTopic,
                "reason", reason,
                "payload", payload
            ));
            // **시도 시점**에 올린다. 직렬화 실패는 시도 전이라 여기 안 온다 —
            // 그것도 실패로는 세지만 "발행 시도"는 아니다.
            com.telemetry.metrics.ContractMetrics.dlqPublishAttempt(meterRegistry, TOPIC);
            kafkaTemplate.send(TOPIC, mqttTopic, envelope).get(10, TimeUnit.SECONDS);
            com.telemetry.metrics.ContractMetrics.dlqPublished(meterRegistry, TOPIC);
        } catch (JsonProcessingException e) {
            com.telemetry.metrics.ContractMetrics.dlqPublishFailed(meterRegistry, TOPIC, e);
            throw new IllegalStateException("MQTT DLQ 메시지 직렬화 실패", e);
        } catch (Exception e) {
            // timeout이면 브로커가 받았는지 **모른다**. dlqPublishFailed가 그걸 따로 센다.
            com.telemetry.metrics.ContractMetrics.dlqPublishFailed(meterRegistry, TOPIC, e);
            throw new IllegalStateException("MQTT DLQ 전송 실패", e);
        }
    }

    private Counter counter(String name, String tagKey, String tagValue) {
        return meterRegistry.counter(name, tagKey, tagValue);
    }
}
