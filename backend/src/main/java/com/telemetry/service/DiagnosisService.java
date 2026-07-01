package com.telemetry.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.dto.response.DiagnosisResponse;
import com.telemetry.dto.response.TelemetryResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 최근 텔레메트리 + 이상 이력을 프롬프트로 구성해 Gemini API에 던지고 진단 텍스트를 받아온다.
 * 단발성 블로킹 HTTP 호출 하나뿐이라 WebFlux/WebClient 의존성을 새로 추가하지 않고
 * JDK 내장 java.net.http.HttpClient를 사용한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DiagnosisService {

    private static final int TELEMETRY_SAMPLE_SIZE = 20;
    private static final int ANOMALY_SAMPLE_SIZE = 10;

    private final TelemetryQueryService telemetryQueryService;
    private final AnomalyService anomalyService;
    private final ObjectMapper objectMapper;

    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();

    @Value("${gemini.api-key}")
    private String apiKey;

    @Value("${gemini.model}")
    private String model;

    public DiagnosisResponse diagnose(String vehicleId) {
        List<TelemetryResponse> recent = telemetryQueryService.getRecent(vehicleId, TELEMETRY_SAMPLE_SIZE);
        if (recent.isEmpty()) {
            throw new IllegalStateException("진단할 텔레메트리 데이터가 없습니다: " + vehicleId);
        }
        List<AnomalyResponse> anomalies = anomalyService.getRecent(vehicleId, ANOMALY_SAMPLE_SIZE);

        String prompt = buildPrompt(vehicleId, recent, anomalies);
        String diagnosisText = callGemini(prompt);

        return new DiagnosisResponse(diagnosisText, recent.size());
    }

    private String buildPrompt(String vehicleId, List<TelemetryResponse> recent, List<AnomalyResponse> anomalies) {
        TelemetryResponse latest = recent.get(0);
        StringBuilder sb = new StringBuilder();
        sb.append("당신은 자동차 정비 전문가입니다. 아래 차량 센서 데이터를 보고 현재 상태를 진단하고, ")
          .append("이상 징후가 있다면 원인 추정과 조치 방법을 한국어로 간결하게 설명하세요.\n\n");
        sb.append("차량 ID: ").append(vehicleId).append('\n');
        sb.append("최신 센서값 — 속도: ").append(latest.getSpeed()).append("km/h, RPM: ").append(latest.getRpm())
          .append(", 엔진온도: ").append(latest.getEngineTemp()).append("°C, 배터리전압: ")
          .append(latest.getBatteryVoltage()).append("V, 연료: ").append(latest.getFuelLevel()).append("%\n");
        if (latest.getDtcCodes() != null && !latest.getDtcCodes().isEmpty()) {
            sb.append("DTC 코드: ").append(String.join(", ", latest.getDtcCodes())).append('\n');
        }
        sb.append("최근 ").append(recent.size()).append("건의 텔레메트리 데이터를 기반으로 분석하세요.\n");

        if (anomalies.isEmpty()) {
            sb.append("\n최근 이상 감지 이력 없음.\n");
        } else {
            sb.append("\n최근 이상 감지 이력:\n");
            anomalies.forEach(a -> sb.append("- ").append(a.getAnomalyType())
                .append(" (").append(a.getField()).append('=').append(a.getValue())
                .append(", 심각도 ").append(a.getSeverity()).append(")\n"));
        }
        return sb.toString();
    }

    private String callGemini(String prompt) {
        try {
            Map<String, Object> requestBody = Map.of(
                "contents", List.of(Map.of("parts", List.of(Map.of("text", prompt))))
            );

            URI uri = URI.create(
                "https://generativelanguage.googleapis.com/v1beta/models/" + model
                    + ":generateContent?key=" + apiKey
            );

            HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestBody)))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                log.error("[Gemini] API 호출 실패 status={} body={}", response.statusCode(), response.body());
                // 업스트림(Gemini) 오류 — "리소스 없음"이 아니므로 IllegalStateException(404 매핑)은 쓰지 않는다.
                // GlobalExceptionHandler의 범용 Exception 핸들러가 받아 500으로 응답한다.
                throw new RuntimeException("AI 진단 호출 실패 (status=" + response.statusCode() + ")");
            }

            JsonNode textNode = objectMapper.readTree(response.body())
                .at("/candidates/0/content/parts/0/text");
            if (textNode.isMissingNode()) {
                log.error("[Gemini] 응답 파싱 실패 body={}", response.body());
                throw new RuntimeException("AI 진단 응답을 해석할 수 없습니다");
            }
            return textNode.asText();

        } catch (IOException e) {
            log.error("[Gemini] 호출 중 오류", e);
            throw new RuntimeException("AI 진단 서비스 호출 실패", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("AI 진단 서비스 호출이 중단되었습니다", e);
        }
    }
}
