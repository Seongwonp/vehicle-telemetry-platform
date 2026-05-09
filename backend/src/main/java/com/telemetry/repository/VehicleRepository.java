package com.telemetry.repository;

import com.telemetry.entity.Vehicle;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface VehicleRepository extends JpaRepository<Vehicle, Long> {

    Optional<Vehicle> findByVehicleId(String vehicleId);

    boolean existsByVehicleId(String vehicleId);

    List<Vehicle> findAllByActiveTrue();
}
