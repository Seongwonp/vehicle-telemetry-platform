package com.telemetry.service;

import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.entity.AnomalyAlert;
import com.telemetry.repository.AnomalyAlertRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class AnomalyService {

    private final AnomalyAlertRepository anomalyAlertRepository;
    private final MeterRegistry meterRegistry;

    @Transactional
    public SaveResult save(Map<String, Object> payload) {
        return saveAll(List.of(toEntity(payload))).get(0);
    }

    /**
     * Kafka 페이로드를 엔티티로 옮긴다. <b>DB를 건드리지 않는다.</b>
     *
     * <p>{@link #saveAll}이 배치를 한 트랜잭션으로 묶으므로, 파싱 실패 하나가 정상 알림까지
     * 롤백시키지 않도록 파싱을 먼저 레코드별로 끝낸다(실패는 호출자가 DLQ로).
     *
     * <p>{@code NOT_SUPPORTED}를 지우지 말 것. 클래스 레벨 readOnly 트랜잭션을 물려받으면
     * DB 장애 중 변환 단계에서 실패하고, 호출자가 이를 잘못된 메시지로 보고 DLQ로 보낸다.
     * 경위: {@code load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md}
     */
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public AnomalyAlert toEntity(Map<String, Object> payload) {
        AnomalyAlert alert = new AnomalyAlert();
        alert.setEventId(resolveEventId(payload));
        alert.setVehicleId((String) payload.get("vehicle_id"));
        alert.setAnomalyType((String) payload.get("anomaly_type"));
        alert.setField((String) payload.get("field"));
        alert.setValue(toDouble(payload.get("value")));
        alert.setThreshold((String) payload.get("threshold"));
        alert.setSeverity((String) payload.get("severity"));
        alert.setDetector((String) payload.get("detector"));

        String vehicleTs = (String) payload.get("timestamp");
        if (vehicleTs != null) {
            alert.setVehicleTimestamp(Instant.parse(vehicleTs));
        }

        String detectedAt = (String) payload.get("detected_at");
        alert.setDetectedAt(detectedAt != null ? Instant.parse(detectedAt) : Instant.now());
        return alert;
    }

    /**
     * 배치를 <b>한 트랜잭션</b>으로 저장한다.
     *
     * 건별 트랜잭션은 알림마다 커밋(fsync)이 돌아 유입을 못 따라간다. 이유: ADR-022
     */
    @Transactional
    public List<SaveResult> saveAll(List<AnomalyAlert> alerts) {
        // 빈 배치로 트랜잭션을 열면 DB 장애 중 실패해 offset이 커밋되지 않고 같은 배치가 계속 돌아온다.
        if (alerts.isEmpty()) return List.of();

        List<SaveResult> results = new ArrayList<>(alerts.size());
        for (AnomalyAlert alert : alerts) {
            results.add(saveOne(alert));
        }
        return results;
    }

    private SaveResult saveOne(AnomalyAlert alert) {
        int inserted = anomalyAlertRepository.insertIfAbsent(
            alert.getEventId(), alert.getVehicleId(), alert.getAnomalyType(), alert.getField(),
            alert.getValue(), alert.getThreshold(), alert.getSeverity(), alert.getDetector(),
            alert.getVehicleTimestamp(), alert.getDetectedAt());
        AnomalyAlert saved = anomalyAlertRepository.findByEventId(alert.getEventId())
            .orElseThrow(() -> new IllegalStateException("이상 이벤트 저장 결과를 찾을 수 없습니다"));

        boolean isNew = inserted > 0;
        meterRegistry.counter("telemetry.anomaly.stored",
            "result", isNew ? "new" : "duplicate").increment();
        if (isNew) {
            // 원본 추적 키(vehicle·ts)와 알림 키(event)를 한 줄에 남긴다(ADR-028).
            log.info("[이상 저장] vehicle={} ts={} type={} severity={} event={}",
                alert.getVehicleId(), alert.getVehicleTimestamp(), alert.getAnomalyType(),
                alert.getSeverity(), alert.getEventId());
        } else {
            // 재처리에서는 정상. 로그를 나눠야 로그 건수가 실제 저장 건수와 맞고,
            // 중복 비율로 재처리 범위가 너무 넓은지 볼 수 있다.
            log.info("[이상 중복] 이미 저장된 이벤트라 건너뜀 vehicle={} ts={} type={} event={}",
                alert.getVehicleId(), alert.getVehicleTimestamp(), alert.getAnomalyType(), alert.getEventId());
        }
        return new SaveResult(saved, isNew);
    }

    /**
     * 저장 결과. 호출자는 {@code inserted}가 true일 때만 알림을 보내야 한다.
     * DLQ 재처리는 이미 저장된 알림을 다시 넣으므로, 행 중복은 ON CONFLICT가 막아도
     * 알림 중복은 이 플래그로 막는다. 이유: ADR-020
     */
    public record SaveResult(AnomalyAlert alert, boolean inserted) {}

    public List<AnomalyResponse> getRecent(String vehicleId, int limit) {
        return anomalyAlertRepository
            .findByVehicleIdOrderByDetectedAtDesc(vehicleId, PageRequest.of(0, limit))
            .stream()
            .map(AnomalyResponse::new)
            .toList();
    }

    public long countByVehicleId(String vehicleId) {
        return anomalyAlertRepository.countByVehicleId(vehicleId);
    }

    public Page<AnomalyResponse> search(
        String vehicleId, String severity, Instant from, Instant to, int page, int size
    ) {
        Specification<AnomalyAlert> spec = (root, query, cb) ->
            cb.equal(root.get("vehicleId"), vehicleId);
        if (severity != null && !severity.isBlank()) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("severity"), severity));
        }
        if (from != null) {
            spec = spec.and((root, query, cb) ->
                cb.greaterThanOrEqualTo(root.get("detectedAt"), from));
        }
        if (to != null) {
            spec = spec.and((root, query, cb) ->
                cb.lessThanOrEqualTo(root.get("detectedAt"), to));
        }
        return anomalyAlertRepository.findAll(
            spec, PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "detectedAt")))
            .map(AnomalyResponse::new);
    }

    private Double toDouble(Object val) {
        if (val instanceof Number n) return n.doubleValue();
        return null;
    }

    private String resolveEventId(Map<String, Object> payload) {
        Object supplied = payload.get("event_id");
        if (supplied instanceof String value && value.matches("^[a-f0-9]{64}$")) {
            return value;
        }
        String key = String.join("|",
            stringValue(payload.get("vehicle_id")),
            stringValue(payload.get("timestamp")),
            stringValue(payload.get("anomaly_type")),
            stringValue(payload.get("field")),
            stringValue(payload.get("detector")));
        try {
            return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256을 사용할 수 없습니다", e);
        }
    }

    private String stringValue(Object value) {
        return value == null ? "" : value.toString();
    }
}
