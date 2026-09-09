package com.telemetry.mqtt;

import com.telemetry.support.TestDecoders;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.kafka.TelemetryProducer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
            TestDecoders.telemetryDecoder(),
            meterRegistry,
            invalidMessagePublisher
        );
    }

    /**
     * 계약을 충족하는 payload. 차량 ID만 바꿔 쓴다.
     *
     * <p>예전 fixture는 {@code vehicle_id}·{@code timestamp}만 담고 나머지를 비워뒀다.
     * primitive가 0으로 채워지고 검증도 통과하던 시절에나 성립하던 stub이고,
     * <b>그 동작이 P0-2에서 제거 대상이었다.</b> 실제 producer(시뮬레이터·부하 도구·
     * e2e 추적)는 전부 온전한 payload를 보낸다 — 확인하고 바꿨다.
     */
    private static String payloadFor(String vehicleId) {
        return """
            {"vehicle_id":"%s","timestamp":"2026-08-04T00:00:00Z",
             "speed":10.0,"rpm":900,"engine_temp":85.0,"throttle_position":12.0,
             "fuel_level":55.0,"battery_voltage":13.8,
             "gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}
            """.formatted(vehicleId);
    }

    @Test
    void validPayloadIsForwardedAndCounted() {
        handler.handle(MessageBuilder.withPayload(payloadFor("KR-GA-1234"))
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

        var message = MessageBuilder.withPayload(payloadFor("KR-GA-1234"))
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build();

        assertThatThrownBy(() -> handler.handle(message))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("spool failed");
        assertThat(counter("telemetry.mqtt.messages.invalid")).isZero();
    }

    @Test
    void topicVehicleMismatchIsSentToDedicatedDlq() {
        String payload = payloadFor("KR-GA-9999");

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

    // ── P0-2b: MQTT 입구의 사유별 카운터 ────────────────────────────
    private double count(String name, String... tags) {
        var c = meterRegistry.find(name).tags(tags).counter();
        return c == null ? 0.0 : c.count();
    }

    @Test
    @DisplayName("계약 위반은 사유별로 세고, 기존 invalid 카운터도 그대로 오른다")
    void 계약위반_사유별_카운터() {
        handler.handle(org.springframework.messaging.support.MessageBuilder
            .withPayload("{\"vehicle_id\":\"KR-GA-1234\",\"timestamp\":\"2026-09-09T10:00:00Z\"}")
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build());

        assertThat(count(com.telemetry.metrics.ContractMetrics.REJECTED_ATTEMPTS,
                "entrance", com.telemetry.metrics.ContractMetrics.ENTRANCE_MQTT,
                "reason", com.telemetry.domain.TelemetryContractException.PAYLOAD_VALIDATION_FAILED))
            .as("사유별 카운터").isEqualTo(1.0);
        assertThat(meterRegistry.counter("telemetry.mqtt.messages.invalid").count())
            .as("기존 지표도 그대로 오른다 — 이름·의미를 바꾸지 않았다").isEqualTo(1.0);
    }

    @Test
    @DisplayName("MQTT 고유 거부 사유는 계약 사유 카운터에 섞이지 않는다")
    void 토픽불일치는_계약사유가_아니다() {
        // payload는 계약을 지키는데 토픽의 차량 ID가 다르다 — MQTT 고유 검사다.
        String valid = "{\"vehicle_id\":\"KR-GA-1234\",\"timestamp\":\"2026-09-09T10:00:00.000Z\","
            + "\"speed\":80.0,\"rpm\":2000,\"engine_temp\":90.0,\"throttle_position\":30.0,"
            + "\"fuel_level\":50.0,\"battery_voltage\":13.5}";
        handler.handle(org.springframework.messaging.support.MessageBuilder
            .withPayload(valid)
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-9999")
            .build());

        assertThat(meterRegistry.counter("telemetry.mqtt.messages.invalid").count())
            .as("거부는 됐다").isEqualTo(1.0);
        assertThat(meterRegistry.find(com.telemetry.metrics.ContractMetrics.REJECTED_ATTEMPTS).counter())
            .as("TOPIC_VEHICLE_MISMATCH는 계약 사유가 아니다 — 섞으면 세 입구를 나란히 못 놓는다")
            .isNull();
    }
}
