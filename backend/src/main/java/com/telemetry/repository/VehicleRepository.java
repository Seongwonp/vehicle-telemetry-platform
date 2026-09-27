package com.telemetry.repository;

import com.telemetry.entity.Vehicle;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface VehicleRepository extends JpaRepository<Vehicle, Long> {

    @EntityGraph(attributePaths = "owner")
    Optional<Vehicle> findByVehicleId(String vehicleId);

    boolean existsByVehicleId(String vehicleId);

    @EntityGraph(attributePaths = "owner")
    List<Vehicle> findAllByActiveTrue();

    boolean existsByVehicleIdAndActiveTrue(String vehicleId);

    boolean existsByVehicleIdAndOwner_UsernameAndActiveTrue(String vehicleId, String username);

    @EntityGraph(attributePaths = "owner")
    List<Vehicle> findAllByOwner_UsernameAndActiveTrue(String username);
}
