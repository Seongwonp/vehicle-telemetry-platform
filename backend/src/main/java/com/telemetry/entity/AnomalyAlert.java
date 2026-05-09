package com.telemetry.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "anomaly_alerts", indexes = {
    @Index(name = "idx_anomaly_vehicle_id", columnList = "vehicle_id"),
    @Index(name = "idx_anomaly_detected_at", columnList = "detected_at")
})
@Getter @Setter
@NoArgsConstructor
public class AnomalyAlert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "vehicle_id", nullable = false, length = 50)
    private String vehicleId;

    @Column(name = "anomaly_type", nullable = false)
    private String anomalyType;

    @Column(length = 50)
    private String field;

    private Double value;

    @Column(length = 200)
    private String threshold;

    @Column(length = 10)
    private String severity;

    @Column(length = 10)
    private String detector;

    @Column(name = "vehicle_timestamp")
    private Instant vehicleTimestamp;

    @Column(name = "detected_at", nullable = false)
    private Instant detectedAt;
}
