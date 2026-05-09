package com.telemetry.service;

import com.telemetry.dto.request.VehicleRegisterRequest;
import com.telemetry.dto.response.VehicleResponse;
import com.telemetry.entity.Vehicle;
import com.telemetry.repository.VehicleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class VehicleService {

    private final VehicleRepository vehicleRepository;

    @Transactional
    public VehicleResponse register(VehicleRegisterRequest request) {
        if (vehicleRepository.existsByVehicleId(request.getVehicleId())) {
            throw new IllegalArgumentException("이미 등록된 차량 ID입니다: " + request.getVehicleId());
        }
        Vehicle vehicle = new Vehicle(request.getVehicleId(), request.getName(), request.getOwner());
        Vehicle saved = vehicleRepository.save(vehicle);
        log.info("차량 등록 완료: {}", saved.getVehicleId());
        return new VehicleResponse(saved);
    }

    public List<VehicleResponse> findAll() {
        return vehicleRepository.findAllByActiveTrue().stream()
            .map(VehicleResponse::new)
            .toList();
    }

    public VehicleResponse findByVehicleId(String vehicleId) {
        Vehicle vehicle = vehicleRepository.findByVehicleId(vehicleId)
            .orElseThrow(() -> new IllegalArgumentException("등록되지 않은 차량입니다: " + vehicleId));
        return new VehicleResponse(vehicle);
    }

    @Transactional
    public void deactivate(String vehicleId) {
        Vehicle vehicle = vehicleRepository.findByVehicleId(vehicleId)
            .orElseThrow(() -> new IllegalArgumentException("등록되지 않은 차량입니다: " + vehicleId));
        vehicle.setActive(false);
        log.info("차량 비활성화: {}", vehicleId);
    }
}
