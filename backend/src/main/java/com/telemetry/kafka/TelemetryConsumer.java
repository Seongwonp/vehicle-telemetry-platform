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
     * Consumer A — vehicle-telemetry → InfluxDB 저장
     */
    @KafkaListener(
        topics = "vehicle-telemetry",
        groupId = "telemetry-storage-group"
    )
    public void consumeForStorage(ConsumerRecord<String, String> record) {
        try {
            VehicleTelemetry telemetry = objectMapper.readValue(record.value(), VehicleTelemetry.class);
            telemetryRepository.save(telemetry);
            log.debug("[Kafka→InfluxDB] 저장 완료 vehicle={}", telemetry.getVehicleId());
        } catch (Exception e) {
            log.error("[Kafka→InfluxDB] 저장 실패 key={} offset={}", record.key(), record.offset(), e);
        }
    }

    /**
     * Consumer B — vehicle-anomaly-alerts → PostgreSQL 저장
     * Python anomaly-detector가 감지한 이상 이벤트를 받아 DB에 저장
     */
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
        } catch (Exception e) {
            log.error("[Kafka→Anomaly] 저장 실패 key={} offset={}", record.key(), record.offset(), e);
        }
    }
}
