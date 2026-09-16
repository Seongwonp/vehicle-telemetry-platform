package com.telemetry.service;

import com.telemetry.dto.request.VehicleRegisterRequest;
import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.dto.response.FleetSummaryStatus;
import com.telemetry.dto.response.VehicleResponse;
import com.telemetry.entity.Vehicle;
import com.telemetry.exception.ResourceConflictException;
import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.repository.AnomalyAlertRepository;
import com.telemetry.repository.VehicleRepository;
import com.telemetry.security.VehicleAccessService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehicleService {

    private final VehicleRepository vehicleRepository;
    private final AnomalyAlertRepository anomalyAlertRepository;
    private final TelemetryQueryService telemetryQueryService;
    private final VehicleAccessService vehicleAccessService;

    @Transactional
    public VehicleResponse register(VehicleRegisterRequest request, Authentication authentication) {
        // 남의 이름으로 등록하면 그 차량 ID의 텔레메트리를 그 사람이 가져가고, 반대로 내가 남의
        // 차량 ID를 선점할 수도 있다. 다른 사람 소유로 등록하는 것은 관리자만 할 수 있다.
        if (!vehicleAccessService.isAdmin(authentication)
            && (authentication == null || !request.getOwner().equals(authentication.getName()))) {
            throw new AccessDeniedException("다른 사용자 소유로는 차량을 등록할 수 없습니다");
        }
        if (vehicleRepository.existsByVehicleId(request.getVehicleId())) {
            throw new ResourceConflictException("이미 등록된 차량 ID입니다: " + request.getVehicleId());
        }
        Vehicle vehicle = new Vehicle(request.getVehicleId(), request.getName(), request.getOwner());
        Vehicle saved = vehicleRepository.save(vehicle);
        // TODO: 변경 주체(사용자/시스템) 추적이 필요하다. 현재는 admin 단일이지만 다중 사용자 지원 시 로그에 포함해야 한다.
        log.info("차량 등록 완료 — vehicleId={} owner={}", saved.getVehicleId(), saved.getOwner());
        return new VehicleResponse(saved);
    }

    public List<VehicleResponse> findAllVisibleTo(Authentication authentication) {
        // 단건 조회를 막아도 목록이 전부 보이면 차량 ID가 그대로 새어 나간다 — 목록도 같은 기준으로 자른다.
        // Spring Data repository 호출의 read-only 트랜잭션은 이 줄에서 종료된다.
        // 이후 InfluxDB 외부 호출이 PostgreSQL 트랜잭션/커넥션을 붙잡지 않는다.
        List<Vehicle> vehicles = vehicleAccessService.isAdmin(authentication)
            ? vehicleRepository.findAllByActiveTrue()
            : vehicleRepository.findAllByOwnerAndActiveTrue(
                authentication == null ? "" : authentication.getName());
        List<String> vehicleIds = vehicles.stream().map(Vehicle::getVehicleId).toList();

        Map<String, TelemetryResponse> latestByVehicle;
        boolean telemetryAvailable = true;
        try {
            latestByVehicle = telemetryQueryService.getLatestByVehicleIds(vehicleIds);
        } catch (Exception e) {
            telemetryAvailable = false;
            latestByVehicle = Map.of();
            log.warn("[Fleet 요약] 일괄 텔레메트리 조회 실패 vehicles={}", vehicleIds.size(), e);
        }

        boolean finalTelemetryAvailable = telemetryAvailable;
        Map<String, TelemetryResponse> finalLatestByVehicle = latestByVehicle;
        return vehicles.stream()
            .map(vehicle -> withFleetSummary(vehicle, finalLatestByVehicle, finalTelemetryAvailable))
            .toList();
    }

    // 목록 화면에서 차량마다 대시보드에 들어가지 않고도 상태를 비교할 수 있게
    // InfluxDB 최근 텔레메트리 + HIGH 이상 누적 건수를 함께 붙인다. 아직 데이터가
    // 없는 차량(방금 등록됨)은 조용히 null/0으로 둔다 — 목록 조회 자체가
    // 실패하면 안 되므로 텔레메트리 조회 실패를 전체 요청 실패로 전파하지 않는다.
    private VehicleResponse withFleetSummary(
        Vehicle vehicle,
        Map<String, TelemetryResponse> latestByVehicle,
        boolean telemetryAvailable
    ) {
        VehicleResponse response = new VehicleResponse(vehicle);
        response.setHighAnomalyCount(
            anomalyAlertRepository.countByVehicleIdAndSeverity(vehicle.getVehicleId(), "HIGH"));
        if (!telemetryAvailable) {
            response.setSummaryStatus(FleetSummaryStatus.UNAVAILABLE);
            return response;
        }
        TelemetryResponse latest = latestByVehicle.get(vehicle.getVehicleId());
        if (latest == null) return response;

        response.setSummaryStatus(FleetSummaryStatus.OK);
        response.setLatestSpeed(latest.getSpeed());
        if (latest.getTimestamp() != null) {
            response.setLastSeenAt(Instant.parse(latest.getTimestamp()));
        }
        return response;
    }

    public VehicleResponse findByVehicleId(String vehicleId) {
        Vehicle vehicle = vehicleRepository.findByVehicleId(vehicleId)
            .orElseThrow(() -> new ResourceNotFoundException("등록되지 않은 차량입니다: " + vehicleId));
        return new VehicleResponse(vehicle);
    }

    @Transactional
    public void deactivate(String vehicleId) {
        Vehicle vehicle = vehicleRepository.findByVehicleId(vehicleId)
            .orElseThrow(() -> new ResourceNotFoundException("등록되지 않은 차량입니다: " + vehicleId));

        // 물리 삭제 대신 active 플래그를 내린다.
        // 차량 ID는 InfluxDB 텔레메트리 데이터와 연결되어 있어 행을 지우면 이력 조회가 깨질 수 있다.
        vehicle.setActive(false);
        // TODO: 변경 주체(사용자/시스템) 추적이 필요하다. 현재는 admin 단일이지만 다중 사용자 지원 시 로그에 포함해야 한다.
        log.info("차량 비활성화 완료 — vehicleId={}", vehicleId);
    }
}
