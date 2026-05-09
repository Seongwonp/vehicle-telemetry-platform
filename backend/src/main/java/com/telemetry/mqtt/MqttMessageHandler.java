package com.telemetry.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.kafka.TelemetryProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MqttMessageHandler {

    private final TelemetryProducer telemetryProducer;
    private final ObjectMapper objectMapper;

    // @ServiceActivator는 MqttConfig에서 선언한 mqttInputChannel과 이 메서드를 연결한다.
    // Spring Integration 채널 기반이라 별도 스레드 풀 없이 메시지 도착 즉시 호출된다.
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public void handle(Message<String> message) {
        String payload = message.getPayload();
        String topic = (String) message.getHeaders().get("mqtt_receivedTopic");

        try {
            VehicleTelemetry telemetry = objectMapper.readValue(payload, VehicleTelemetry.class);

            // 전체 필드를 INFO로 남기면 초당 수십 건 로그가 쌓여 노이즈가 된다.
            // 이상 탐지와 직결되는 핵심 3개 필드만 남긴다.
            log.info("[MQTT→Kafka] vehicle={} speed={} engine_temp={} battery_voltage={}",
                telemetry.getVehicleId(),
                telemetry.getSpeed(),
                telemetry.getEngineTemp(),
                telemetry.getBatteryVoltage());

            telemetryProducer.send(telemetry);

        } catch (Exception e) {
            // 역직렬화 실패 시 어떤 토픽에서 어떤 페이로드가 왔는지 남겨야 원인 파악이 가능하다
            log.error("[MQTT] 역직렬화 실패 — topic={} payload={}", topic, payload, e);
        }
    }
}
