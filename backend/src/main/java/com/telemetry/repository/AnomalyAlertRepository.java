package com.telemetry.repository;

import com.telemetry.entity.AnomalyAlert;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface AnomalyAlertRepository extends JpaRepository<AnomalyAlert, Long>,
    JpaSpecificationExecutor<AnomalyAlert> {

    List<AnomalyAlert> findByVehicleIdOrderByDetectedAtDesc(String vehicleId, Pageable pageable);

    long countByVehicleId(String vehicleId);

    Optional<AnomalyAlert> findByEventId(String eventId);

    @Modifying
    @Query(value = """
        INSERT INTO anomaly_alerts
            (event_id, vehicle_id, anomaly_type, field, value, threshold, severity, detector,
             vehicle_timestamp, detected_at)
        VALUES
            (:eventId, :vehicleId, :anomalyType, :field, :value, :threshold, :severity, :detector,
             :vehicleTimestamp, :detectedAt)
        ON CONFLICT (event_id) DO NOTHING
        """, nativeQuery = true)
    int insertIfAbsent(
        @Param("eventId") String eventId,
        @Param("vehicleId") String vehicleId,
        @Param("anomalyType") String anomalyType,
        @Param("field") String field,
        @Param("value") Double value,
        @Param("threshold") String threshold,
        @Param("severity") String severity,
        @Param("detector") String detector,
        @Param("vehicleTimestamp") Instant vehicleTimestamp,
        @Param("detectedAt") Instant detectedAt
    );
}
