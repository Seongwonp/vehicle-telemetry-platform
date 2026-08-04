package com.telemetry.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Schema(description = "AI 진단 결과 응답")
public class DiagnosisResponse {

    @Schema(description = "차량 상태 등급", example = "B", allowableValues = {"A", "B", "C", "D", "E", "F"})
    private final String grade;

    @Schema(description = "차량 상태 점수 (0~100, 높을수록 양호)", example = "78")
    private final int score;

    @Schema(description = "Gemini가 생성한 진단 텍스트 (마크다운)")
    private final String diagnosis;

    @Schema(description = "진단에 사용된 최근 텔레메트리 데이터 건수", example = "20")
    private final int dataPoints;
}
