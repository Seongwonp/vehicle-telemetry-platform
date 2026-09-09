package com.telemetry.mqtt;

import com.telemetry.domain.TelemetryContractException;
import com.telemetry.domain.TelemetryDecoder;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.kafka.TelemetryProducer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
public class MqttMessageHandler {

    private static final Pattern TELEMETRY_TOPIC = Pattern.compile(
        "^vehicle/telemetry/([A-Z0-9-]{4,20})$");

    private final TelemetryProducer telemetryProducer;
    private final TelemetryDecoder telemetryDecoder;
    private final MeterRegistry meterRegistry;
    private final Counter receivedCounter;
    private final Counter invalidCounter;
    private final MqttInvalidMessagePublisher invalidMessagePublisher;

    public MqttMessageHandler(
        TelemetryProducer telemetryProducer,
        TelemetryDecoder telemetryDecoder,
        MeterRegistry meterRegistry,
        MqttInvalidMessagePublisher invalidMessagePublisher
    ) {
        this.telemetryProducer = telemetryProducer;
        this.telemetryDecoder = telemetryDecoder;
        this.meterRegistry = meterRegistry;
        this.receivedCounter = meterRegistry.counter("telemetry.mqtt.messages.received");
        this.invalidCounter = meterRegistry.counter("telemetry.mqtt.messages.invalid");
        this.invalidMessagePublisher = invalidMessagePublisher;
    }

    // @ServiceActivator는 MqttConfig에서 선언한 mqttInputChannel과 이 메서드를 연결한다.
    // Spring Integration 채널 기반이라 별도 스레드 풀 없이 메시지 도착 즉시 호출된다.
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public void handle(Message<String> message) {
        String payload = message.getPayload();
        String topic = (String) message.getHeaders().get("mqtt_receivedTopic");
        receivedCounter.increment();

        // **두 입구가 같은 decoder를 쓴다.** 예전에는 여기서만 역직렬화 + Bean Validation을
        // 했고 Kafka 직접 주입은 검증이 없었다 — 같은 payload가 입구에 따라 통과하기도
        // 거부되기도 했다(2026-09-09 감사).
        VehicleTelemetry telemetry;
        try {
            telemetry = telemetryDecoder.decode(payload);
        } catch (TelemetryContractException e) {
            reject(topic, payload, e.getReason());
            return;
        }

        Matcher topicMatcher = topic == null ? null : TELEMETRY_TOPIC.matcher(topic);
        if (!validTimestamp(telemetry.getTimestamp())) {
            reject(topic, payload, "INVALID_TIMESTAMP");
            return;
        }
        if (topicMatcher == null || !topicMatcher.matches()
            || !topicMatcher.group(1).equals(telemetry.getVehicleId())) {
            reject(topic, payload, "TOPIC_VEHICLE_MISMATCH");
            return;
        }

        log.debug("[MQTT→Kafka] vehicle={} speed={} engine_temp={} battery_voltage={}",
            telemetry.getVehicleId(),
            telemetry.getSpeed(),
            telemetry.getEngineTemp(),
            telemetry.getBatteryVoltage());

        // spool/Kafka 전송 실패는 삼키지 않아 MQTT 어댑터가 실패를 인지하고 재처리할 수 있게 한다.
        telemetryProducer.send(telemetry);
    }

    private void reject(String topic, String payload, String reason) {
        // 기존 지표 — MQTT 입구의 **모든** 거부를 센다. 이름·의미를 바꾸지 않는다.
        invalidCounter.increment();
        // 사유별 지표는 **계약 사유일 때만** 올린다. `TOPIC_VEHICLE_MISMATCH`는 MQTT 고유
        // 검사라 계약 사유가 아니다 — 섞으면 세 입구를 나란히 놓을 수 없다.
        if (TelemetryContractException.isContractReason(reason)) {
            com.telemetry.metrics.ContractMetrics.rejected(meterRegistry, com.telemetry.metrics.ContractMetrics.ENTRANCE_MQTT, reason);
        }
        log.warn("[MQTT] 메시지 거부 — topic={} reason={} payloadLength={} payloadSha256={}",
            topic, reason, payload.length(), sha256(payload));
        invalidMessagePublisher.publish(topic, payload, reason);
    }

    private boolean validTimestamp(String timestamp) {
        try {
            Instant.parse(timestamp);
            return true;
        } catch (DateTimeParseException | NullPointerException e) {
            return false;
        }
    }

    static String sha256(String payload) {
        try {
            return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(payload.getBytes(StandardCharsets.UTF_8))
            );
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256을 사용할 수 없습니다", e);
        }
    }
}
