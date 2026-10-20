package com.telemetry.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.write.Point;
import com.telemetry.domain.TelemetryDecoder;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.entity.AnomalyAlert;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.service.AnomalyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
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
    private final TelemetryDecoder telemetryDecoder;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final MeterRegistry meterRegistry;

    /**
     * poll 한 번의 레코드를 InfluxDB 요청 1건 + offset 커밋 1건으로 처리한다(ADR-011).
     * 저장과 이상 감지는 consumer group이 달라야 둘 다 모든 메시지를 받는다(ADR-002).
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
                // MQTT 입구와 같은 decoder를 써야 이 경로로 계약을 우회할 수 없다.
                // 검증·변환 실패는 레코드별 catch로 그 건만 DLQ로 보낸다(배치 전체를 실패시키지 않는다).
                VehicleTelemetry telemetry = telemetryDecoder.decode(record.value());
                points.add(telemetryRepository.toPoint(telemetry));
                converted.add(telemetry);
            } catch (com.telemetry.domain.TelemetryContractException e) {
                // 사유별 거부 지표에는 계약 위반만 센다. 변환 실패와 섞으면 지표 뜻이 흐려진다.
                com.telemetry.metrics.ContractMetrics.rejected(meterRegistry, com.telemetry.metrics.ContractMetrics.ENTRANCE_KAFKA_STORAGE, e.getReason());
                // 추적 키는 payloadSha256 — MQTT 거부 로그와 같은 함수다(ADR-028).
                log.warn("[Kafka→InfluxDB] 계약 위반 {} — DLQ로 이동 vehicle={} partition={} offset={} payloadSha256={}",
                    e.getReason(), record.key(), record.partition(), record.offset(),
                    com.telemetry.domain.PayloadDigest.sha256(record.value()));
                sendToDlq(TELEMETRY_DLQ_TOPIC, record, e);
            } catch (Exception e) {
                log.error("[Kafka→InfluxDB] 역직렬화/변환 실패 — DLQ로 이동 vehicle={} partition={} offset={} payloadSha256={}",
                    record.key(), record.partition(), record.offset(),
                    com.telemetry.domain.PayloadDigest.sha256(record.value()), e);
                sendToDlq(TELEMETRY_DLQ_TOPIC, record, e);
            }
        }

        // InfluxDB 쓰기 실패는 DLQ로 보내지 않고 그대로 던진다 — offset이 커밋되지 않아 재시도되고,
        // 계속 실패하면 lag 알림으로 드러난다. 여기서 DLQ+커밋하면 장애 중 데이터가 조용히 빠져나간다.
        // 재시도 소진은 KafkaConfig의 DeadLetterPublishingRecoverer가 받는다.
        // 배치 재시도 시 위에서 DLQ로 보낸 레코드가 중복 발행될 수 있다(유실보다 낫다).
        // 경위: devlog, load-test/long-outage/RESULT_20260906_influxdb.md
        try {
            telemetryRepository.saveAll(points);
        } catch (RuntimeException e) {
            log.error("[Kafka→InfluxDB] 배치 저장 실패 {}건 — offset 미커밋, 재시도한다 ({})",
                points.size(), e.getClass().getSimpleName());
            throw e;
        }
        log.debug("[Kafka→InfluxDB] 배치 저장 완료 — 수신 {}건 중 {}건 저장",
            records.size(), points.size());
        acknowledgment.acknowledge();
        // 방송은 커밋 뒤, 실패해도 던지지 않는다(broadcastAnomaly 참고).
        converted.forEach(this::broadcastTelemetry);
    }

    /**
     * 알림을 PostgreSQL 트랜잭션 1건 + offset 커밋 1건으로 처리한다(ADR-022).
     * 파싱·변환은 레코드별로 격리하고(실패 시 그 건만 DLQ), DB 쓰기만 배치로 묶는다.
     */
    @KafkaListener(
        topics = "vehicle-anomaly-alerts",
        groupId = "anomaly-storage-group",
        containerFactory = "telemetryBatchListenerContainerFactory"
    )
    public void consumeAnomalyAlerts(List<ConsumerRecord<String, String>> records,
                                     Acknowledgment acknowledgment) {
        List<AnomalyAlert> toSave = new ArrayList<>(records.size());

        for (ConsumerRecord<String, String> record : records) {
            try {
                Map<String, Object> payload = objectMapper.readValue(
                    record.value(), new TypeReference<>() {}
                );
                // 변환은 트랜잭션 밖에서 끝낸다 — 안에서 터지면 배치 전체가 롤백된다.
                toSave.add(anomalyService.toEntity(payload));
            } catch (Exception e) {
                log.error("[Kafka→Anomaly] 변환 실패 — DLQ로 이동 vehicle={} partition={} offset={} payloadSha256={}",
                    record.key(), record.partition(), record.offset(),
                    com.telemetry.domain.PayloadDigest.sha256(record.value()), e);
                sendToDlq(ANOMALY_DLQ_TOPIC, record, e);
            }
        }

        List<AnomalyService.SaveResult> results;
        try {
            results = anomalyService.saveAll(toSave);
        } catch (Exception e) {
            // DB 장애는 DLQ로 보내지 않는다. offset 미커밋으로 재시도에 맡기고,
            // 소진되면 DeadLetterPublishingRecoverer가 처리한다.
            log.error("[Kafka→Anomaly] 배치 저장 실패 {}건 — offset 미커밋, 재시도한다",
                toSave.size(), e);
            throw e;
        }

        acknowledgment.acknowledge();
        // 새로 insert된 알림만 방송한다. 재처리는 이미 저장된 알림을 다시 넣게 되고,
        // 행 중복은 UNIQUE(event_id)가 막지만 방송 중복은 이 조건만 막는다(ADR-020).
        for (AnomalyService.SaveResult result : results) {
            if (!result.inserted()) continue;
            broadcastAnomaly(result.alert());
        }
        log.debug("[Kafka→Anomaly] 배치 처리 완료 — 수신 {}건 중 저장 시도 {}건",
            records.size(), toSave.size());
    }

    /**
     * 방송은 offset 커밋 뒤라 여기서 던지면 안 된다. 던지면 이미 저장·커밋된 배치가 재시도되고,
     * 알림은 재시도에서 inserted=false가 되어 방송이 영구히 빠진다. 실패는 세고 로그만 남긴다.
     */
    private void broadcastAnomaly(AnomalyAlert saved) {
        try {
            messagingTemplate.convertAndSend(
                "/topic/vehicle/" + saved.getVehicleId() + "/anomalies",
                new AnomalyResponse(saved));
        } catch (RuntimeException e) {
            countBroadcastFailure("anomalies");
            log.warn("[WebSocket] 알림 방송 실패 — 저장·커밋은 끝났다 vehicle={} event={} ({})",
                saved.getVehicleId(), saved.getEventId(), e.getClass().getSimpleName());
        }
    }

    private void countBroadcastFailure(String channel) {
        meterRegistry.counter("telemetry.websocket.broadcast.failures", "channel", channel).increment();
    }

    // REST TelemetryResponse와 같은 JSON 형태 — 앱이 폴링·스트리밍을 같은 모델로 파싱한다.
    private void broadcastTelemetry(VehicleTelemetry t) {
        TelemetryResponse response = TelemetryResponse.builder()
            .vehicleId(t.getVehicleId())
            .timestamp(t.getTimestamp())
            .speed(t.getSpeed())
            .rpm(t.getRpm())
            .engineTemp(t.getEngineTemp())
            .throttlePosition(t.getThrottlePosition())
            .fuelLevel(t.getFuelLevel())
            .batteryVoltage(t.getBatteryVoltage())
            .lat(t.getGps() != null ? t.getGps().getLat() : null)
            .lng(t.getGps() != null ? t.getGps().getLng() : null)
            .dtcCodes(t.getDtcCodes())
            .build();
        try {
            messagingTemplate.convertAndSend(
                "/topic/vehicle/" + t.getVehicleId() + "/telemetry", response);
        } catch (RuntimeException e) {
            countBroadcastFailure("telemetry");
            log.warn("[WebSocket] 텔레메트리 방송 실패 — 저장·커밋은 끝났다 vehicle={} ts={} ({})",
                t.getVehicleId(), t.getTimestamp(), e.getClass().getSimpleName());
        }
    }

    /**
     * 원본 key·payload는 그대로 두고 실패 원인을 {@code x-dlq-*} 헤더로 남긴다. 재처리 도구가
     * 영구 실패와 일시 실패를 가르는 유일한 근거다. 재시도 소진 경로의 {@code kafka_dlt-*}와
     * 같은 토픽에 섞이므로 도구는 둘 다 읽는다({@code dlq-tools/dlq.py}).
     * 발행이 확인되지 않으면 던진다 — 원본 offset을 커밋하면 안 된다.
     */
    private void sendToDlq(String dlqTopic, ConsumerRecord<String, String> record, Exception cause) {
        try {
            ProducerRecord<String, String> dlqRecord =
                new ProducerRecord<>(dlqTopic, null, record.key(), record.value());
            Headers headers = dlqRecord.headers();
            // replay-count는 반드시 이어받는다. 버리면 매번 0으로 리셋되어 영구 실패 메시지가
            // DLQ→원본→DLQ를 무한히 돌며 증식한다. 경위: devlog
            copyHeader(record, headers, "x-dlq-replay-count");
            addHeader(headers, "x-dlq-origin-topic", record.topic());
            addHeader(headers, "x-dlq-origin-partition", String.valueOf(record.partition()));
            addHeader(headers, "x-dlq-origin-offset", String.valueOf(record.offset()));
            addHeader(headers, "x-dlq-failed-at", Instant.now().toString());
            if (cause != null) {
                addHeader(headers, "x-dlq-failure-class", cause.getClass().getName());
                // Jackson 예외 메시지에 payload 조각이 섞이고, 헤더도 메시지 크기 제한에 들어가므로 자른다.
                addHeader(headers, "x-dlq-failure-message", truncate(cause.getMessage(), 512));
            }

            // 시도·성공·실패는 별개로 센다(docs/rejection-metrics-design.md).
            com.telemetry.metrics.ContractMetrics.dlqPublishAttempt(meterRegistry, dlqTopic);
            kafkaTemplate.send(dlqRecord).get(10, TimeUnit.SECONDS);
            com.telemetry.metrics.ContractMetrics.dlqPublished(meterRegistry, dlqTopic);
        } catch (Exception e) {
            // timeout은 발행 여부를 알 수 없다 — dlqPublishFailed가 따로 센다.
            com.telemetry.metrics.ContractMetrics.dlqPublishFailed(meterRegistry, dlqTopic, e);
            log.error("[DLQ] {} 전송 실패 — 원본 offset을 커밋하지 않음 vehicle={} partition={} offset={}",
                dlqTopic, record.key(), record.partition(), record.offset(), e);
            throw new IllegalStateException("DLQ 전송 실패", e);
        }
    }

    private static void copyHeader(ConsumerRecord<String, String> source, Headers target, String key) {
        Header found = source.headers().lastHeader(key);
        if (found != null && found.value() != null) {
            target.add(key, found.value());
        }
    }

    private static void addHeader(Headers headers, String key, String value) {
        if (value != null) {
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max) + "...(truncated)";
    }
}
