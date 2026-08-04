package com.telemetry.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.entity.AnomalyAlert;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.service.AnomalyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class TelemetryConsumer {

    private static final String TELEMETRY_DLQ_TOPIC = "vehicle-telemetry-dlq";
    private static final String ANOMALY_DLQ_TOPIC = "vehicle-anomaly-alerts-dlq";

    private final TelemetryRepository telemetryRepository;
    private final AnomalyService anomalyService;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;

    /**
     * Consumer 그룹을 두 개로 분리한 이유:
     * 같은 그룹으로 묶으면 하나의 파티션을 한 Consumer만 읽게 되어 InfluxDB 저장과 이상 탐지 중 하나가 메시지를 못 받는다.
     * 그룹을 분리하면 각 그룹이 토픽을 독립적으로 구독하므로 두 처리 경로가 모든 메시지를 각자 수신한다.
     */

    @KafkaListener(
        topics = "vehicle-telemetry",
        groupId = "telemetry-storage-group"
    )
    public void consumeForStorage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        VehicleTelemetry telemetry;
        try {
            telemetry = objectMapper.readValue(record.value(), VehicleTelemetry.class);
            telemetryRepository.save(telemetry);
            log.debug("[Kafka→InfluxDB] 저장 완료 — vehicle={} partition={} offset={}",
                telemetry.getVehicleId(),
                record.partition(),
                record.offset());
        } catch (Exception e) {
            // 역직렬화 실패 또는 포인트 구성 실패(예: 잘못된 timestamp 형식) — DLQ로 옮겨 유실 없이 격리한다
            log.error("[Kafka→InfluxDB] 저장 실패 — DLQ로 이동 vehicle={} offset={} partition={}",
                record.key(), record.offset(), record.partition(), e);
            sendToDlq(TELEMETRY_DLQ_TOPIC, record);
            acknowledgment.acknowledge();
            return;
        }
        acknowledgment.acknowledge();
        broadcastTelemetry(telemetry);
    }

    @KafkaListener(
        topics = "vehicle-anomaly-alerts",
        groupId = "anomaly-storage-group"
    )
    public void consumeAnomalyAlerts(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        AnomalyAlert saved;
        try {
            Map<String, Object> payload = objectMapper.readValue(
                record.value(), new TypeReference<>() {}
            );
            saved = anomalyService.save(payload);
            log.debug("[Kafka→Anomaly] 이상 이벤트 저장 완료 — vehicle={} offset={}",
                record.key(), record.offset());
        } catch (Exception e) {
            // Python anomaly-detector가 발행한 이벤트가 저장 안 된 경우 — 알림 누락으로 이어질 수 있어 DLQ로 격리한다
            log.error("[Kafka→Anomaly] 저장 실패 — DLQ로 이동 vehicle={} offset={} partition={}",
                record.key(), record.offset(), record.partition(), e);
            sendToDlq(ANOMALY_DLQ_TOPIC, record);
            acknowledgment.acknowledge();
            return;
        }
        acknowledgment.acknowledge();
        messagingTemplate.convertAndSend(
            "/topic/vehicle/" + saved.getVehicleId() + "/anomalies",
            new AnomalyResponse(saved)
        );
    }

    // REST의 TelemetryResponse와 동일한 JSON 형태로 만들어 보낸다 — 앱이
    // 폴링 응답과 스트리밍 응답을 같은 모델(Telemetry.fromJson)로 파싱할 수 있도록.
    private void broadcastTelemetry(VehicleTelemetry t) {
        TelemetryResponse response = TelemetryResponse.builder()
            .vehicleId(t.getVehicleId())
            .timestamp(t.getTimestamp())
            .speed(t.getSpeed())
            .rpm((double) t.getRpm())
            .engineTemp(t.getEngineTemp())
            .throttlePosition(t.getThrottlePosition())
            .fuelLevel(t.getFuelLevel())
            .batteryVoltage(t.getBatteryVoltage())
            .lat(t.getGps() != null ? t.getGps().getLat() : null)
            .lng(t.getGps() != null ? t.getGps().getLng() : null)
            .dtcCodes(t.getDtcCodes())
            .build();
        messagingTemplate.convertAndSend(
            "/topic/vehicle/" + t.getVehicleId() + "/telemetry", response);
    }

    // 저장 실패한 원본 메시지를 DLQ 토픽으로 옮긴다. key(vehicle_id)를 그대로 유지해
    // 나중에 차량별로 재처리/조사할 수 있게 한다. 재처리 컨슈머는 아직 없음 — 우선 유실 방지/가시성 확보까지.
    private void sendToDlq(String dlqTopic, ConsumerRecord<String, String> record) {
        try {
            kafkaTemplate.send(dlqTopic, record.key(), record.value()).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("[DLQ] {} 전송 실패 — 원본 offset을 커밋하지 않음 key={}",
                dlqTopic, record.key(), e);
            throw new IllegalStateException("DLQ 전송 실패", e);
        }
    }
}
