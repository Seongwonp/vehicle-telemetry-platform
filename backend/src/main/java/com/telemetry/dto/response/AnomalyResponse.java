package com.telemetry.dto.response;

import com.telemetry.entity.AnomalyAlert;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;

import java.time.Instant;

@Getter
@Schema(description = "이상 감지 이벤트 응답")
public class AnomalyResponse {

    @Schema(description = "PK")
    private final Long id;

    @Schema(description = "차량 ID", example = "SIM-001")
    private final String vehicleId;

    @Schema(description = "이상 유형", example = "엔진 과열")
    private final String anomalyType;

    @Schema(description = "이상 감지 필드", example = "engine_temp")
    private final String field;

    @Schema(description = "이상 감지 시 값", example = "108.5")
    private final Double value;

    @Schema(description = "임계값 설명", example = "engine_temp > 105°C")
    private final String threshold;

    @Schema(description = "심각도", example = "HIGH")
    private final String severity;

    @Schema(description = "감지기 유형", example = "RULE")
    private final String detector;

    @Schema(description = "차량 데이터 타임스탬프")
    private final Instant vehicleTimestamp;

    @Schema(description = "이상 감지 시각")
    private final Instant detectedAt;

    public AnomalyResponse(AnomalyAlert alert) {
        this.id = alert.getId();
        this.vehicleId = alert.getVehicleId();
        this.anomalyType = alert.getAnomalyType();
        this.field = alert.getField();
        this.value = alert.getValue();
        this.threshold = alert.getThreshold();
        this.severity = alert.getSeverity();
        this.detector = alert.getDetector();
        this.vehicleTimestamp = alert.getVehicleTimestamp();
        this.detectedAt = alert.getDetectedAt();
    }
}
