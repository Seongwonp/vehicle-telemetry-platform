package com.telemetry.repository;

import com.telemetry.entity.AnomalyAlert;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface AnomalyAlertRepository extends JpaRepository<AnomalyAlert, Long> {

    List<AnomalyAlert> findByVehicleIdOrderByDetectedAtDesc(String vehicleId, Pageable pageable);

    long countByVehicleId(String vehicleId);
}
