package com.telemetry.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MqttInvalidMessagePublisherTest {

    @Test
    void publishesOriginalPayloadAndReasonToDedicatedDlq() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ObjectMapper objectMapper = new ObjectMapper();
        String mqttTopic = "vehicle/telemetry/TEST-001";
        String payload = "{broken-json";
        @SuppressWarnings("unchecked")
        org.springframework.kafka.support.SendResult<String, String> sendResult =
            mock(org.springframework.kafka.support.SendResult.class);
        CompletableFuture<org.springframework.kafka.support.SendResult<String, String>> future =
            CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(
            eq(MqttInvalidMessagePublisher.TOPIC), eq(mqttTopic), org.mockito.ArgumentMatchers.anyString()))
            .thenReturn(future);
        MqttInvalidMessagePublisher publisher = new MqttInvalidMessagePublisher(
            kafkaTemplate, objectMapper, meterRegistry);

        publisher.publish(mqttTopic, payload, "MALFORMED_JSON");

        var envelope = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(MqttInvalidMessagePublisher.TOPIC), eq(mqttTopic), envelope.capture());
        assertThat(objectMapper.readTree(envelope.getValue()).path("payload").asText()).isEqualTo(payload);
        assertThat(objectMapper.readTree(envelope.getValue()).path("reason").asText())
            .isEqualTo("MALFORMED_JSON");
        assertThat(meterRegistry.get("telemetry.kafka.dlq.published").counter().count()).isEqualTo(1.0);
    }
}
