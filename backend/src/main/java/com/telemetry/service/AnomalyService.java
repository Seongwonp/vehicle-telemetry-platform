package com.telemetry.service;

import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.entity.AnomalyAlert;
import com.telemetry.repository.AnomalyAlertRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class AnomalyService {

    private final AnomalyAlertRepository anomalyAlertRepository;

    @Transactional
    public void save(Map<String, Object> payload) {
        AnomalyAlert alert = new AnomalyAlert();
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

        anomalyAlertRepository.save(alert);
        log.info("[이상 저장] vehicle={} type={} severity={}",
            alert.getVehicleId(), alert.getAnomalyType(), alert.getSeverity());
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

    private Double toDouble(Object val) {
        if (val instanceof Number n) return n.doubleValue();
        return null;
    }
}
