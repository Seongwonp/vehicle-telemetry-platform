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
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.SimpleAcknowledgment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

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
            reject(topic, payload, e.getReason(), null);
            acknowledge(message, CompletableFuture.completedFuture(null));
            return;
        }

        Matcher topicMatcher = topic == null ? null : TELEMETRY_TOPIC.matcher(topic);
        if (!validTimestamp(telemetry.getTimestamp())) {
            reject(topic, payload, "INVALID_TIMESTAMP", telemetry);
            acknowledge(message, CompletableFuture.completedFuture(null));
            return;
        }
        if (topicMatcher == null || !topicMatcher.matches()
            || !topicMatcher.group(1).equals(telemetry.getVehicleId())) {
            reject(topic, payload, "TOPIC_VEHICLE_MISMATCH", telemetry);
            acknowledge(message, CompletableFuture.completedFuture(null));
            return;
        }

        // 추적 키는 전 구간 같은 이름이다 — vehicle=, ts=(원본 timestamp 문자열). ADR-028.
        log.debug("[MQTT→Kafka] vehicle={} ts={} speed={} engine_temp={} battery_voltage={}",
            telemetry.getVehicleId(),
            telemetry.getTimestamp(),
            telemetry.getSpeed(),
            telemetry.getEngineTemp(),
            telemetry.getBatteryVoltage());

        acknowledge(message, telemetryProducer.send(telemetry));
    }

    private void acknowledge(Message<?> message, CompletableFuture<Void> receipt) {
        var acknowledgment = message.getHeaders().get(
            IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, SimpleAcknowledgment.class);
        // Header-free calls are used by the legacy comparison and decoder unit tests.
        if (acknowledgment == null) return;
        try {
            // Stay on Paho's single callback thread: ordered ACKs, no old async callback after reconnect.
            // This deliberately trades concurrency for a small, auditable boundary (ADR-029).
            receipt.get(150, TimeUnit.SECONDS);
            acknowledgment.acknowledge();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("MQTT 저장 확인 중단 — ACK하지 않음", e);
        } catch (Exception e) {
            // Propagate through DirectChannel to Paho; disconnect/reconnect permits redelivery.
            throw new IllegalStateException("MQTT 저장 확인 실패 — ACK하지 않음", e);
        }
    }

    /**
     * @param decoded 계약은 통과했지만 MQTT 고유 검사에서 걸린 경우의 도메인 객체. 계약 위반이면 null.
     *                추적 키(vehicle·ts)를 남기되 <b>신뢰하지 않는다</b> — 거부된 메시지의 필드다.
     */
    private void reject(String topic, String payload, String reason, VehicleTelemetry decoded) {
        // 기존 지표 — MQTT 입구의 **모든** 거부를 센다. 이름·의미를 바꾸지 않는다.
        invalidCounter.increment();
        // 사유별 지표는 **계약 사유일 때만** 올린다. `TOPIC_VEHICLE_MISMATCH`는 MQTT 고유
        // 검사라 계약 사유가 아니다 — 섞으면 세 입구를 나란히 놓을 수 없다.
        if (TelemetryContractException.isContractReason(reason)) {
            com.telemetry.metrics.ContractMetrics.rejected(meterRegistry, com.telemetry.metrics.ContractMetrics.ENTRANCE_MQTT, reason);
        }
        // 거부 경로의 추적 키는 payloadSha256이다 — DLQ value와 대조한다. 값은 남기지 않는다.
        log.warn("[MQTT] 메시지 거부 — topic={} reason={} vehicle={} ts={} payloadLength={} payloadSha256={}",
            topic, reason,
            decoded == null ? "-" : decoded.getVehicleId(),
            decoded == null ? "-" : decoded.getTimestamp(),
            payload.length(), sha256(payload));
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
        return com.telemetry.domain.PayloadDigest.sha256(payload);
    }
}
