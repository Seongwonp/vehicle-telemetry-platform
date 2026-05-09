package com.telemetry.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
@Schema(description = "차량 텔레메트리 데이터 응답")
public class TelemetryResponse {

    @Schema(description = "차량 ID", example = "KR-GA-1234")
    private String vehicleId;

    @Schema(description = "수집 시각 (UTC)", example = "2026-05-09T10:00:00Z")
    private String timestamp;

    @Schema(description = "속도 (km/h)", example = "87.3")
    private Double speed;

    @Schema(description = "RPM", example = "2400")
    private Double rpm;

    @Schema(description = "엔진 온도 (°C)", example = "92.1")
    private Double engineTemp;

    @Schema(description = "스로틀 포지션 (%)", example = "34.5")
    private Double throttlePosition;

    @Schema(description = "연료 잔량 (%)", example = "67.0")
    private Double fuelLevel;

    @Schema(description = "배터리 전압 (V)", example = "13.8")
    private Double batteryVoltage;

    @Schema(description = "위도", example = "37.123456")
    private Double lat;

    @Schema(description = "경도", example = "127.654321")
    private Double lng;

    @Schema(description = "DTC 진단 코드 목록")
    private List<String> dtcCodes;
}
