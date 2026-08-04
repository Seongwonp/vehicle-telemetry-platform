package com.telemetry.service;

import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.entity.AnomalyAlert;
import com.telemetry.repository.AnomalyAlertRepository;
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

    @Transactional
    public AnomalyAlert save(Map<String, Object> payload) {
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

        anomalyAlertRepository.insertIfAbsent(
            alert.getEventId(), alert.getVehicleId(), alert.getAnomalyType(), alert.getField(),
            alert.getValue(), alert.getThreshold(), alert.getSeverity(), alert.getDetector(),
            alert.getVehicleTimestamp(), alert.getDetectedAt());
        AnomalyAlert saved = anomalyAlertRepository.findByEventId(alert.getEventId())
            .orElseThrow(() -> new IllegalStateException("이상 이벤트 저장 결과를 찾을 수 없습니다"));
        log.info("[이상 저장] vehicle={} type={} severity={}",
            alert.getVehicleId(), alert.getAnomalyType(), alert.getSeverity());
        return saved;
    }

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
