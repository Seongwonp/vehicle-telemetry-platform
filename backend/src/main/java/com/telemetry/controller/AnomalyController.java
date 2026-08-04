package com.telemetry.controller;

import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.dto.response.AnomalyPageResponse;
import com.telemetry.service.AnomalyService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.validation.annotation.Validated;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.Instant;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/vehicles/{vehicleId}/anomalies")
@RequiredArgsConstructor
@Tag(name = "Anomaly", description = "차량 이상 감지 이력 API")
@SecurityRequirement(name = "bearerAuth")
@Validated
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
        @RequestParam(defaultValue = "20") @Min(1) @Max(100) int limit
    ) {
        return ResponseEntity.ok(anomalyService.getRecent(vehicleId, limit));
    }

    @GetMapping("/count")
    @Operation(summary = "이상 감지 총 건수")
    public ResponseEntity<Map<String, Long>> getCount(@PathVariable String vehicleId) {
        return ResponseEntity.ok(Map.of("count", anomalyService.countByVehicleId(vehicleId)));
    }

    @GetMapping("/page")
    @Operation(summary = "이상 감지 이력 필터·페이지 조회")
    public ResponseEntity<AnomalyPageResponse> search(
        @PathVariable String vehicleId,
        @RequestParam(required = false) @Pattern(regexp = "HIGH|MEDIUM") String severity,
        @RequestParam(required = false)
        @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
        @RequestParam(required = false)
        @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
        @RequestParam(defaultValue = "0") @Min(0) int page,
        @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size
    ) {
        return ResponseEntity.ok(AnomalyPageResponse.from(
            anomalyService.search(vehicleId, severity, from, to, page, size)));
    }
}
