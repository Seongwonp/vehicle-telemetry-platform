package com.telemetry.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;

@Getter
@Builder
@Schema(description = "주행 구간(트립) 요약 — 연속된 텔레메트리 수신 구간을 시간 간격으로 나눈 단위")
public class TripResponse {

    @Schema(description = "트립 시작 시각")
    private final Instant startTime;

    @Schema(description = "트립 종료 시각")
    private final Instant endTime;

    @Schema(description = "주행 시간 (분)")
    private final long durationMinutes;

    @Schema(description = "주행 거리 (km, GPS 좌표 haversine 합산)")
    private final double distanceKm;

    @Schema(description = "평균 속도 (km/h)")
    private final double avgSpeedKmh;

    @Schema(description = "최고 속도 (km/h)")
    private final double maxSpeedKmh;

    @Schema(description = "이 트립에 포함된 텔레메트리 포인트 수")
    private final int pointCount;
}
