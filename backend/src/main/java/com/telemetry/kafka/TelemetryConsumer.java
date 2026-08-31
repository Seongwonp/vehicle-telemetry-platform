package com.telemetry.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.write.Point;
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
import io.micrometer.core.instrument.MeterRegistry;

import java.util.ArrayList;
import java.util.List;
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
    private final MeterRegistry meterRegistry;

    /**
     * Consumer 그룹을 두 개로 분리한 이유:
     * 같은 그룹으로 묶으면 하나의 파티션을 한 Consumer만 읽게 되어 InfluxDB 저장과 이상 탐지 중 하나가 메시지를 못 받는다.
     * 그룹을 분리하면 각 그룹이 토픽을 독립적으로 구독하므로 두 처리 경로가 모든 메시지를 각자 수신한다.
     */

    /**
     * poll 한 번에 받은 레코드를 묶어서 InfluxDB 요청 1건 + offset 커밋 1건으로 처리한다.
     *
     * <p>메시지마다 {@code writePoint()} + {@code acknowledge()}를 하던 구조에서는 건당
     * HTTP 요청 1회(= InfluxDB WAL fsync 1회) + Kafka 커밋 1회가 발생해, ~2,400 msg/s
     * 부하에서 처리량이 8 msg/s까지 떨어졌다. 배치로 묶으면 이 건당 오버헤드가
     * {@code max.poll.records}배만큼 줄어든다.
     */
    @KafkaListener(
        topics = "vehicle-telemetry",
        groupId = "telemetry-storage-group",
        containerFactory = "telemetryBatchListenerContainerFactory"
    )
    public void consumeForStorage(List<ConsumerRecord<String, String>> records,
                                  Acknowledgment acknowledgment) {
        List<Point> points = new ArrayList<>(records.size());
        List<VehicleTelemetry> converted = new ArrayList<>(records.size());

        for (ConsumerRecord<String, String> record : records) {
            try {
                VehicleTelemetry telemetry = objectMapper.readValue(record.value(), VehicleTelemetry.class);
                // 포인트 변환(타임스탬프 파싱 포함)까지 레코드별로 처리한다 — 여기서 실패하는 건
                // 메시지 자체가 영구적으로 처리 불가능한 경우라, 배치 전체를 실패시키지 않고
                // 그 한 건만 DLQ로 격리해야 정상 메시지가 재시도에 휘말리지 않는다.
                points.add(telemetryRepository.toPoint(telemetry));
                converted.add(telemetry);
            } catch (Exception e) {
                log.error("[Kafka→InfluxDB] 역직렬화/변환 실패 — DLQ로 이동 vehicle={} offset={} partition={}",
                    record.key(), record.offset(), record.partition(), e);
                sendToDlq(TELEMETRY_DLQ_TOPIC, record);
            }
        }

        // InfluxDB 쓰기 실패는 여기서 잡지 않는다. 예전엔 역직렬화 실패와 같은 catch에
        // 묶여 있어서, InfluxDB 장애 중에도 원본 메시지를 DLQ로 옮기고 offset을 커밋해버렸다
        // — 12시간 soak test에서 InfluxDB 쓰기가 테스트 시작 26초 만에 멈췄는데도
        // telemetry-storage-group의 Kafka lag은 끝까지 낮게 유지된 채(offset은 계속 커밋됨)
        // 텔레메트리가 12시간 내내 조용히 유실된 걸로 이 버그를 찾았다. 여기서 예외를
        // 그대로 던지면 offset이 커밋되지 않아 배치가 재시도되고, 계속 실패하면 Kafka
        // lag이 올라가 KafkaConsumerLagHigh/InfluxDbOperationFailures 알림으로 드러난다
        // — "장애가 나면 눈에 띄어야 한다"가 여기선 맞는 동작이다. 재시도까지 소진되면
        // KafkaConfig의 DeadLetterPublishingRecoverer가 DLQ로 보내므로 어느 경로로도
        // 조용히 사라지지 않는다.
        //
        // 트레이드오프: 배치가 재시도되면 위 루프에서 이미 DLQ로 보낸 역직렬화 실패
        // 레코드가 중복 발행된다. DLQ는 조사/재처리용이라 중복이 유실보다 낫다고 판단.
        telemetryRepository.saveAll(points);
        log.debug("[Kafka→InfluxDB] 배치 저장 완료 — 수신 {}건 중 {}건 저장",
            records.size(), points.size());
        acknowledgment.acknowledge();
        converted.forEach(this::broadcastTelemetry);
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
            meterRegistry.counter("telemetry.kafka.dlq.published", "topic", dlqTopic).increment();
        } catch (Exception e) {
            meterRegistry.counter("telemetry.kafka.dlq.publish.failures", "topic", dlqTopic).increment();
            log.error("[DLQ] {} 전송 실패 — 원본 offset을 커밋하지 않음 key={}",
                dlqTopic, record.key(), e);
            throw new IllegalStateException("DLQ 전송 실패", e);
        }
    }
}
