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
                sendToDlq(TELEMETRY_DLQ_TOPIC, record, e);
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
        AnomalyService.SaveResult result;
        try {
            Map<String, Object> payload = objectMapper.readValue(
                record.value(), new TypeReference<>() {}
            );
            result = anomalyService.save(payload);
            log.debug("[Kafka→Anomaly] 이상 이벤트 저장 완료 — vehicle={} offset={}",
                record.key(), record.offset());
        } catch (Exception e) {
            // Python anomaly-detector가 발행한 이벤트가 저장 안 된 경우 — 알림 누락으로 이어질 수 있어 DLQ로 격리한다
            log.error("[Kafka→Anomaly] 저장 실패 — DLQ로 이동 vehicle={} offset={} partition={}",
                record.key(), record.offset(), record.partition(), e);
            sendToDlq(ANOMALY_DLQ_TOPIC, record, e);
            acknowledgment.acknowledge();
            return;
        }
        acknowledgment.acknowledge();
        // 이미 저장돼 있던 이벤트면 알림을 보내지 않는다.
        //
        // 재처리는 "이미 저장된 알림"을 반드시 다시 넣게 된다. PostgreSQL 60초 장애를
        // 주입해 재보니 DLQ 9건 중 3건은 이미 저장된 것이었고(서버 커밋은 끝났는데
        // 연결이 끊겨 클라이언트만 실패로 본 경우), 재처리를 한 번 더 돌리면 9건 전부가
        // 그렇게 된다. 행은 UNIQUE(event_id) + ON CONFLICT DO NOTHING이 막지만
        // 브로드캐스트는 막을 것이 없어서, 예전 코드는 이 9건을 매번 다시 밀어냈다
        // (load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md).
        if (result.inserted()) {
            AnomalyAlert saved = result.alert();
            messagingTemplate.convertAndSend(
                "/topic/vehicle/" + saved.getVehicleId() + "/anomalies",
                new AnomalyResponse(saved)
            );
        }
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

    /**
     * 저장 실패한 원본 메시지를 DLQ 토픽으로 옮긴다. key(vehicle_id)와 payload는 원본
     * 그대로 두고, <b>왜 실패했는지를 헤더로 함께 남긴다.</b>
     *
     * <p>헤더가 없으면 재처리를 결정할 수가 없다. 깨진 JSON은 몇 번을 다시 넣어도
     * 같은 자리에서 실패하는 영구 실패고(재처리하면 DLQ→원본→DLQ 무한 루프가 된다),
     * InfluxDB 장애로 재시도가 소진된 건은 DB가 살아나면 그냥 성공한다. 둘을 가르는
     * 유일한 근거가 실패 원인이라, payload만 보고는 판단이 불가능하다.
     *
     * <p>재시도 소진 경로({@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer})는
     * Spring이 {@code kafka_dlt-*} 헤더를 자동으로 붙인다. 여기(레코드 단위 격리)는
     * 우리가 직접 붙여야 해서, 같은 정보를 {@code x-dlq-*}로 남긴다 — 두 경로가 같은
     * DLQ 토픽에 섞이므로 재처리 도구는 둘 다 읽을 수 있어야 한다
     * ({@code dlq-tools/dlq.py}, {@code docs/runbook/dlq-reprocessing.md}).
     */
    private void sendToDlq(String dlqTopic, ConsumerRecord<String, String> record, Exception cause) {
        try {
            ProducerRecord<String, String> dlqRecord =
                new ProducerRecord<>(dlqTopic, null, record.key(), record.value());
            Headers headers = dlqRecord.headers();
            // 재처리 이력은 반드시 이어받아야 한다. 재처리 도구가 DLQ 레코드를 원본
            // 토픽으로 되돌릴 때 x-dlq-replay-count를 올려서 보내는데, 여기서 그걸
            // 버리고 새 헤더만 만들면 카운터가 매번 0으로 리셋된다 — 그러면 영구 실패
            // 메시지가 DLQ→원본→DLQ를 무한히 돌고, 돌 때마다 DLQ 레코드가 배로 늘어난다
            // (실제로 재처리 4회에 2→4→8→16건으로 증식하는 걸 확인하고 고쳤다).
            copyHeader(record, headers, "x-dlq-replay-count");
            addHeader(headers, "x-dlq-origin-topic", record.topic());
            addHeader(headers, "x-dlq-origin-partition", String.valueOf(record.partition()));
            addHeader(headers, "x-dlq-origin-offset", String.valueOf(record.offset()));
            addHeader(headers, "x-dlq-failed-at", Instant.now().toString());
            if (cause != null) {
                addHeader(headers, "x-dlq-failure-class", cause.getClass().getName());
                // 예외 메시지에는 원본 payload 조각이 섞여 들어올 수 있어(Jackson이 그렇게 한다)
                // 길이를 잘라 둔다 — Kafka 헤더는 메시지 크기 제한에 함께 잡힌다.
                addHeader(headers, "x-dlq-failure-message", truncate(cause.getMessage(), 512));
            }

            kafkaTemplate.send(dlqRecord).get(10, TimeUnit.SECONDS);
            meterRegistry.counter("telemetry.kafka.dlq.published", "topic", dlqTopic).increment();
        } catch (Exception e) {
            meterRegistry.counter("telemetry.kafka.dlq.publish.failures", "topic", dlqTopic).increment();
            log.error("[DLQ] {} 전송 실패 — 원본 offset을 커밋하지 않음 key={}",
                dlqTopic, record.key(), e);
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
