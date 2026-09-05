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
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
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
            log.info("[이상 저장] vehicle={} type={} severity={}",
                alert.getVehicleId(), alert.getAnomalyType(), alert.getSeverity());
        } else {
            // 재처리에서는 정상적인 결과다. 로그를 나누는 이유는 두 가지다 —
            // (1) 예전엔 중복도 "[이상 저장]"으로 찍혀서, 재처리 후 로그를 세면 실제
            //     저장된 건수보다 많이 나왔다(실측: 9건 재처리에 행 증가는 6건).
            // (2) 중복 비율이 높으면 재처리 범위가 필요 이상으로 넓다는 신호다.
            log.info("[이상 중복] 이미 저장된 이벤트라 건너뜀 vehicle={} type={} event={}",
                alert.getVehicleId(), alert.getAnomalyType(), alert.getEventId());
        }
        return new SaveResult(saved, isNew);
    }

    /**
     * 저장 결과. <b>새로 들어갔는지</b>가 호출자에게 필요하다.
     *
     * <p>DLQ 재처리는 이미 저장된 알림을 필연적으로 다시 넣는다 — PostgreSQL 60초 장애를
     * 주입해 재보니 DLQ 9건 중 <b>3건은 이미 저장돼 있었다</b>(서버에서는 커밋이 끝났는데
     * 연결이 끊겨 클라이언트만 실패로 본 경우). 행 중복은
     * {@code ON CONFLICT (event_id) DO NOTHING}이 막아주지만, 호출자가 이 구분을 모르면
     * <b>알림은 그대로 다시 나간다.</b>
     * ({@code load-test/anomaly-dlq-idempotency/})
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
