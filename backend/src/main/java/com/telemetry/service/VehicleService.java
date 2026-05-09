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
// 클래스 레벨에 readOnly = true를 달아두면 조회 메서드에서 트랜잭션 생략 실수를 방지하고,
// JPA 더티체킹을 건너뛰어 조회 성능이 약간 올라간다. 쓰기 메서드는 @Transactional로 오버라이드한다.
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
        // TODO: 변경 주체(사용자/시스템) 추적이 필요하다. 현재는 admin 단일이지만 다중 사용자 지원 시 로그에 포함해야 한다.
        log.info("차량 등록 완료 — vehicleId={} owner={}", saved.getVehicleId(), saved.getOwner());
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

        // 물리 삭제 대신 active 플래그를 내린다.
        // 차량 ID는 InfluxDB 텔레메트리 데이터와 연결되어 있어 행을 지우면 이력 조회가 깨질 수 있다.
        vehicle.setActive(false);
        // TODO: 변경 주체(사용자/시스템) 추적이 필요하다. 현재는 admin 단일이지만 다중 사용자 지원 시 로그에 포함해야 한다.
        log.info("차량 비활성화 완료 — vehicleId={}", vehicleId);
    }
}
