package com.telemetry.controller;

import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.service.TelemetryQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/vehicles/{vehicleId}/telemetry")
@RequiredArgsConstructor
@Tag(name = "Telemetry", description = "차량 텔레메트리 데이터 조회 API")
@SecurityRequirement(name = "bearerAuth")
public class TelemetryController {

    private final TelemetryQueryService telemetryQueryService;

    @GetMapping
    @Operation(
        summary = "최근 텔레메트리 목록",
        description = "최근 1시간 이내 데이터를 최신순으로 반환 (기본 20건)"
    )
    public ResponseEntity<List<TelemetryResponse>> getRecent(
        @PathVariable String vehicleId,
        @Parameter(description = "조회 건수 (최대 100)", example = "20")
        @RequestParam(defaultValue = "20") int limit
    ) {
        int safeLimit = Math.min(limit, 100);
        return ResponseEntity.ok(telemetryQueryService.getRecent(vehicleId, safeLimit));
    }

    @GetMapping("/latest")
    @Operation(summary = "최신 텔레메트리 1건", description = "가장 최근 수신된 데이터 1건 반환")
    public ResponseEntity<TelemetryResponse> getLatest(@PathVariable String vehicleId) {
        return ResponseEntity.ok(telemetryQueryService.getLatest(vehicleId));
    }
}
