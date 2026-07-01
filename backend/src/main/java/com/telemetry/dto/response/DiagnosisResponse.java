package com.telemetry.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Schema(description = "AI 진단 결과 응답")
public class DiagnosisResponse {

    @Schema(description = "Gemini가 생성한 진단 텍스트")
    private final String diagnosis;

    @Schema(description = "진단에 사용된 최근 텔레메트리 데이터 건수", example = "20")
    private final int dataPoints;
}
