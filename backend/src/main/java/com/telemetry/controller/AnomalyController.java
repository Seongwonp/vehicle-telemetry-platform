package com.telemetry.controller;

import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.service.AnomalyService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/vehicles/{vehicleId}/anomalies")
@RequiredArgsConstructor
@Tag(name = "Anomaly", description = "차량 이상 감지 이력 API")
@SecurityRequirement(name = "bearerAuth")
public class AnomalyController {

    private final AnomalyService anomalyService;

    @GetMapping
    @Operation(
        summary = "이상 감지 이력 조회",
        description = "최신순으로 이상 감지 이벤트 목록 반환 (기본 20건)"
    )
    public ResponseEntity<List<AnomalyResponse>> getAnomalies(
        @PathVariable String vehicleId,
        @Parameter(description = "조회 건수 (최대 100)", example = "20")
        @RequestParam(defaultValue = "20") int limit
    ) {
        int safeLimit = Math.min(limit, 100);
        return ResponseEntity.ok(anomalyService.getRecent(vehicleId, safeLimit));
    }

    @GetMapping("/count")
    @Operation(summary = "이상 감지 총 건수")
    public ResponseEntity<Map<String, Long>> getCount(@PathVariable String vehicleId) {
        return ResponseEntity.ok(Map.of("count", anomalyService.countByVehicleId(vehicleId)));
    }
}
