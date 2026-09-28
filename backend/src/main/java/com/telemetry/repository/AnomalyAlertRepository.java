package com.telemetry.repository;

import com.telemetry.entity.AnomalyAlert;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface AnomalyAlertRepository extends JpaRepository<AnomalyAlert, Long>,
    JpaSpecificationExecutor<AnomalyAlert> {

    List<AnomalyAlert> findByVehicleIdOrderByDetectedAtDesc(String vehicleId, Pageable pageable);

    long countByVehicleId(String vehicleId);

    long countByVehicleIdAndSeverity(String vehicleId, String severity);

    Optional<AnomalyAlert> findByEventId(String eventId);

    /**
     * 차량 목록용 HIGH 건수를 한 번에 센다 — 차량마다 count를 부르면 목록 크기만큼 왕복한다(N+1).
     * V5 부분 인덱스를 활용한다. 실행계획은 데이터 분포와 visibility map에 따라 달라진다.
     * 건수 0인 차량은 결과에 없다.
     */
    @Query("""
        SELECT a.vehicleId AS vehicleId, count(a) AS count FROM AnomalyAlert a
        WHERE a.severity = 'HIGH' AND a.vehicleId IN :vehicleIds GROUP BY a.vehicleId
        """)
    List<HighCount> countHighByVehicleIds(@Param("vehicleIds") Collection<String> vehicleIds);

    interface HighCount {
        String getVehicleId();
        long getCount();
    }

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
