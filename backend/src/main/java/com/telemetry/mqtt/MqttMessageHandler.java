package com.telemetry.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.kafka.TelemetryProducer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
public class MqttMessageHandler {

    private static final Pattern TELEMETRY_TOPIC = Pattern.compile(
        "^vehicle/telemetry/([A-Z0-9-]{4,20})$");

    private final TelemetryProducer telemetryProducer;
    private final ObjectMapper objectMapper;
    private final Counter receivedCounter;
    private final Counter invalidCounter;
    private final Validator validator;
    private final MqttInvalidMessagePublisher invalidMessagePublisher;

    public MqttMessageHandler(
        TelemetryProducer telemetryProducer,
        ObjectMapper objectMapper,
        MeterRegistry meterRegistry,
        Validator validator,
        MqttInvalidMessagePublisher invalidMessagePublisher
    ) {
        this.telemetryProducer = telemetryProducer;
        this.objectMapper = objectMapper;
        this.receivedCounter = meterRegistry.counter("telemetry.mqtt.messages.received");
        this.invalidCounter = meterRegistry.counter("telemetry.mqtt.messages.invalid");
        this.validator = validator;
        this.invalidMessagePublisher = invalidMessagePublisher;
    }

    // @ServiceActivator는 MqttConfig에서 선언한 mqttInputChannel과 이 메서드를 연결한다.
    // Spring Integration 채널 기반이라 별도 스레드 풀 없이 메시지 도착 즉시 호출된다.
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public void handle(Message<String> message) {
        String payload = message.getPayload();
        String topic = (String) message.getHeaders().get("mqtt_receivedTopic");
        receivedCounter.increment();

        VehicleTelemetry telemetry;
        try {
            telemetry = objectMapper.readValue(payload, VehicleTelemetry.class);
        } catch (JsonProcessingException e) {
            reject(topic, payload, "MALFORMED_JSON");
            return;
        }

        Set<ConstraintViolation<VehicleTelemetry>> violations = validator.validate(telemetry);
        Matcher topicMatcher = topic == null ? null : TELEMETRY_TOPIC.matcher(topic);
        if (!violations.isEmpty()) {
            reject(topic, payload, "PAYLOAD_VALIDATION_FAILED");
            return;
        }
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
        invalidCounter.increment();
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
