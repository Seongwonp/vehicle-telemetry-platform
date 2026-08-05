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
            kafkaTemplate.send(TOPIC, mqttTopic, envelope).get(10, TimeUnit.SECONDS);
            counter("telemetry.kafka.dlq.published", "topic", TOPIC).increment();
        } catch (JsonProcessingException e) {
            counter("telemetry.kafka.dlq.publish.failures", "topic", TOPIC).increment();
            throw new IllegalStateException("MQTT DLQ 메시지 직렬화 실패", e);
        } catch (Exception e) {
            counter("telemetry.kafka.dlq.publish.failures", "topic", TOPIC).increment();
            throw new IllegalStateException("MQTT DLQ 전송 실패", e);
        }
    }

    private Counter counter(String name, String tagKey, String tagValue) {
        return meterRegistry.counter(name, tagKey, tagValue);
    }
}
