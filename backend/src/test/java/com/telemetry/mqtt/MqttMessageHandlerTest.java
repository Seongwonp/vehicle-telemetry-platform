package com.telemetry.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.kafka.TelemetryProducer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class MqttMessageHandlerTest {

    private TelemetryProducer telemetryProducer;
    private SimpleMeterRegistry meterRegistry;
    private MqttMessageHandler handler;
    private MqttInvalidMessagePublisher invalidMessagePublisher;

    @BeforeEach
    void setUp() {
        telemetryProducer = mock(TelemetryProducer.class);
        invalidMessagePublisher = mock(MqttInvalidMessagePublisher.class);
        meterRegistry = new SimpleMeterRegistry();
        handler = new MqttMessageHandler(
            telemetryProducer,
            new ObjectMapper(),
            meterRegistry,
            Validation.buildDefaultValidatorFactory().getValidator(),
            invalidMessagePublisher
        );
    }

    @Test
    void validPayloadIsForwardedAndCounted() {
        handler.handle(MessageBuilder.withPayload("""
                {"vehicle_id":"KR-GA-1234","timestamp":"2026-08-04T00:00:00Z","speed":10.0}
                """)
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build());

        verify(telemetryProducer).send(any(VehicleTelemetry.class));
        assertThat(counter("telemetry.mqtt.messages.received")).isEqualTo(1.0);
        assertThat(counter("telemetry.mqtt.messages.invalid")).isZero();
    }

    @Test
    void invalidPayloadIsDroppedWithoutLoggingRawPayload() {
        handler.handle(MessageBuilder.withPayload("{invalid-json-secret}")
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build());

        verify(telemetryProducer, never()).send(any());
        verify(invalidMessagePublisher).publish(
            "vehicle/telemetry/KR-GA-1234", "{invalid-json-secret}", "MALFORMED_JSON");
        assertThat(counter("telemetry.mqtt.messages.invalid")).isEqualTo(1.0);
        assertThat(MqttMessageHandler.sha256("{invalid-json-secret}"))
            .hasSize(64)
            .doesNotContain("secret");
    }

    @Test
    void producerFailureIsNotSwallowedAsParsingFailure() {
        doThrow(new IllegalStateException("spool failed"))
            .when(telemetryProducer).send(any(VehicleTelemetry.class));

        var message = MessageBuilder.withPayload(
                "{\"vehicle_id\":\"KR-GA-1234\",\"timestamp\":\"2026-08-04T00:00:00Z\"}")
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build();

        assertThatThrownBy(() -> handler.handle(message))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("spool failed");
        assertThat(counter("telemetry.mqtt.messages.invalid")).isZero();
    }

    @Test
    void topicVehicleMismatchIsSentToDedicatedDlq() {
        String payload = "{\"vehicle_id\":\"KR-GA-9999\",\"timestamp\":\"2026-08-04T00:00:00Z\"}";

        handler.handle(MessageBuilder.withPayload(payload)
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build());

        verify(telemetryProducer, never()).send(any());
        verify(invalidMessagePublisher).publish(
            "vehicle/telemetry/KR-GA-1234", payload, "TOPIC_VEHICLE_MISMATCH");
    }

    @Test
    void invalidTimestampAndGpsAreRejected() {
        String payload = """
            {"vehicle_id":"KR-GA-1234","timestamp":"not-a-time","gps":{"lat":91.0,"lng":127.0}}
            """;

        handler.handle(MessageBuilder.withPayload(payload)
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build());

        verify(invalidMessagePublisher).publish(
            "vehicle/telemetry/KR-GA-1234", payload, "PAYLOAD_VALIDATION_FAILED");
        verify(telemetryProducer, never()).send(any());
    }

    private double counter(String name) {
        return meterRegistry.get(name).counter().count();
    }
}
