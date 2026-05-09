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
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class TelemetryConsumer {

    private final TelemetryRepository telemetryRepository;
    private final AnomalyService anomalyService;
    private final ObjectMapper objectMapper;

    /**
     * Consumer 그룹을 두 개로 분리한 이유:
     * 같은 그룹으로 묶으면 하나의 파티션을 한 Consumer만 읽게 되어 InfluxDB 저장과 이상 탐지 중 하나가 메시지를 못 받는다.
     * 그룹을 분리하면 각 그룹이 토픽을 독립적으로 구독하므로 두 처리 경로가 모든 메시지를 각자 수신한다.
     */

    // TODO: 저장 실패 시 Dead Letter Queue(DLQ)로 보내는 처리가 없다. 운영 환경에서는 유실 방지를 위해 필요하다.

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
            // 역직렬화 실패 포함 — 어떤 차량의 어느 시점 데이터가 유실됐는지 추적할 수 있도록 남긴다
            log.error("[Kafka→InfluxDB] 저장 실패로 데이터 유실 — vehicle={} offset={} partition={}",
                record.key(), record.offset(), record.partition(), e);
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
            // Python anomaly-detector가 발행한 이벤트가 저장 안 된 경우 — 알림 누락으로 이어질 수 있다
            log.error("[Kafka→Anomaly] 이상 이벤트 저장 실패로 데이터 유실 — vehicle={} offset={} partition={}",
                record.key(), record.offset(), record.partition(), e);
        }
    }
}
