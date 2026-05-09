package com.telemetry.controller;

import com.telemetry.dto.request.VehicleRegisterRequest;
import com.telemetry.dto.response.VehicleResponse;
import com.telemetry.service.VehicleService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/vehicles")
@RequiredArgsConstructor
@Tag(name = "Vehicle", description = "차량 등록 및 관리 API")
@SecurityRequirement(name = "bearerAuth")
public class VehicleController {

    private final VehicleService vehicleService;

    @PostMapping
    @Operation(summary = "차량 등록", description = "새 차량을 시스템에 등록합니다")
    public ResponseEntity<VehicleResponse> register(@Valid @RequestBody VehicleRegisterRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(vehicleService.register(request));
    }

    @GetMapping
    @Operation(summary = "차량 목록 조회", description = "등록된 활성 차량 전체 목록")
    public ResponseEntity<List<VehicleResponse>> findAll() {
        return ResponseEntity.ok(vehicleService.findAll());
    }

    @GetMapping("/{vehicleId}")
    @Operation(summary = "차량 단건 조회", description = "차량 ID로 조회")
    public ResponseEntity<VehicleResponse> findOne(@PathVariable String vehicleId) {
        return ResponseEntity.ok(vehicleService.findByVehicleId(vehicleId));
    }

    @DeleteMapping("/{vehicleId}")
    @Operation(summary = "차량 비활성화", description = "차량을 소프트 삭제(비활성화)합니다")
    public ResponseEntity<Void> deactivate(@PathVariable String vehicleId) {
        vehicleService.deactivate(vehicleId);
        return ResponseEntity.noContent().build();
    }
}
