package com.telemetry.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@Slf4j
@Component
public class MqttMessageHandler {

    private final TelemetryProducer telemetryProducer;
    private final ObjectMapper objectMapper;
    private final Counter receivedCounter;
    private final Counter invalidCounter;

    public MqttMessageHandler(
        TelemetryProducer telemetryProducer,
        ObjectMapper objectMapper,
        MeterRegistry meterRegistry
    ) {
        this.telemetryProducer = telemetryProducer;
        this.objectMapper = objectMapper;
        this.receivedCounter = meterRegistry.counter("telemetry.mqtt.messages.received");
        this.invalidCounter = meterRegistry.counter("telemetry.mqtt.messages.invalid");
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
            invalidCounter.increment();
            // 원본 payload에는 위치·차량 정보가 포함될 수 있으므로 길이와 해시만 기록한다.
            log.warn("[MQTT] 역직렬화 실패 — topic={} payloadLength={} payloadSha256={}",
                topic, payload.length(), sha256(payload));
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
