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

    /**
     * mqttInputChannel 로 들어온 메시지를 수신하여 Kafka로 전달.
     * MqttConfig에서 등록한 채널과 @ServiceActivator가 연결됨.
     */
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public void handle(Message<String> message) {
        String payload = message.getPayload();
        String topic = (String) message.getHeaders().get("mqtt_receivedTopic");

        try {
            VehicleTelemetry telemetry = objectMapper.readValue(payload, VehicleTelemetry.class);

            log.info("[MQTT→Kafka] vehicle={} speed={} rpm={} temp={} bat={}",
                telemetry.getVehicleId(),
                telemetry.getSpeed(),
                telemetry.getRpm(),
                telemetry.getEngineTemp(),
                telemetry.getBatteryVoltage()
            );

            telemetryProducer.send(telemetry);

        } catch (Exception e) {
            log.error("[MQTT] 메시지 처리 실패 topic={} payload={}", topic, payload, e);
        }
    }
}
