package com.telemetry.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.service.AnomalyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

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

    /**
     * Consumer 그룹을 두 개로 분리한 이유:
     * 같은 그룹으로 묶으면 하나의 파티션을 한 Consumer만 읽게 되어 InfluxDB 저장과 이상 탐지 중 하나가 메시지를 못 받는다.
     * 그룹을 분리하면 각 그룹이 토픽을 독립적으로 구독하므로 두 처리 경로가 모든 메시지를 각자 수신한다.
     */

    @KafkaListener(
        topics = "vehicle-telemetry",
        groupId = "telemetry-storage-group"
    )
    public void consumeForStorage(ConsumerRecord<String, String> record) {
        try {
            VehicleTelemetry telemetry = objectMapper.readValue(record.value(), VehicleTelemetry.class);
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
        }
    }

    @KafkaListener(
        topics = "vehicle-anomaly-alerts",
        groupId = "anomaly-storage-group"
    )
    public void consumeAnomalyAlerts(ConsumerRecord<String, String> record) {
        try {
            Map<String, Object> payload = objectMapper.readValue(
                record.value(), new TypeReference<>() {}
            );
            anomalyService.save(payload);
            log.debug("[Kafka→Anomaly] 이상 이벤트 저장 완료 — vehicle={} offset={}",
                record.key(), record.offset());
        } catch (Exception e) {
            // Python anomaly-detector가 발행한 이벤트가 저장 안 된 경우 — 알림 누락으로 이어질 수 있어 DLQ로 격리한다
            log.error("[Kafka→Anomaly] 저장 실패 — DLQ로 이동 vehicle={} offset={} partition={}",
                record.key(), record.offset(), record.partition(), e);
            sendToDlq(ANOMALY_DLQ_TOPIC, record);
        }
    }

    // 저장 실패한 원본 메시지를 DLQ 토픽으로 옮긴다. key(vehicle_id)를 그대로 유지해
    // 나중에 차량별로 재처리/조사할 수 있게 한다. 재처리 컨슈머는 아직 없음 — 우선 유실 방지/가시성 확보까지.
    private void sendToDlq(String dlqTopic, ConsumerRecord<String, String> record) {
        kafkaTemplate.send(dlqTopic, record.key(), record.value())
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("[DLQ] {} 전송조차 실패 — 메시지 완전 유실 key={}", dlqTopic, record.key(), ex);
                }
            });
    }
}
