package com.telemetry.controller;

import com.telemetry.dto.response.DiagnosisResponse;
import com.telemetry.service.DiagnosisService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/vehicles/{vehicleId}/diagnosis")
@RequiredArgsConstructor
@Tag(name = "Diagnosis", description = "Gemini 기반 AI 차량 진단 API")
@SecurityRequirement(name = "bearerAuth")
public class DiagnosisController {

    private final DiagnosisService diagnosisService;

    @GetMapping
    @Operation(
        summary = "AI 진단 요청",
        description = "최근 텔레메트리와 이상 이력을 종합해 Gemini API로 진단 텍스트를 생성한다"
    )
    public ResponseEntity<DiagnosisResponse> diagnose(@PathVariable String vehicleId) {
        return ResponseEntity.ok(diagnosisService.diagnose(vehicleId));
    }
}
