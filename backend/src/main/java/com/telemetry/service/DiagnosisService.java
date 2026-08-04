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
        GeminiDiagnosisResult result = callGemini(prompt);

        return new DiagnosisResponse(result.grade(), result.score(), result.diagnosis(), recent.size());
    }

    private record GeminiDiagnosisResult(String grade, int score, String diagnosis) {
    }

    private String buildPrompt(String vehicleId, List<TelemetryResponse> recent, List<AnomalyResponse> anomalies) {
        TelemetryResponse latest = recent.get(0);
        StringBuilder sb = new StringBuilder();
        sb.append("당신은 자동차 정비 전문가입니다. 아래 차량 센서 데이터를 보고 현재 상태를 상세히 진단하세요.\n\n")
          .append("응답은 JSON의 diagnosis 필드에 마크다운으로 작성하되, 다음 구조를 반드시 갖추세요 ")
          .append("(각 섹션을 생략하거나 한두 문장으로 축약하지 말고, 기존 정비 리포트 수준으로 충분히 상세하게 작성):\n")
          .append("### 1. 현재 상태 진단\n실시간 수치 평가와 종합 소견을 문단으로 작성.\n")
          .append("### 2. 이상 징후 및 원인 추정\n항목별로 현상과 추정 원인을 글머리 기호로 구체적으로 나열.\n")
          .append("### 3. 조치 방법\n번호를 매겨 구체적인 정비/점검 항목을 나열.\n")
          .append("마지막에 한 문장 요약도 포함하세요.\n\n")
          .append("diagnosis 필드와 별도로, 차량의 전반적 상태를 A(매우 양호)~F(즉시 정비 필요) 등급과 ")
          .append("0~100점 점수로도 함께 평가해 grade/score 필드에 담으세요.\n\n");
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

    private static final Map<String, Object> RESPONSE_SCHEMA = Map.of(
        "type", "OBJECT",
        "properties", Map.of(
            "grade", Map.of("type", "STRING", "enum", List.of("A", "B", "C", "D", "E", "F")),
            "score", Map.of("type", "INTEGER"),
            "diagnosis", Map.of("type", "STRING")
        ),
        "required", List.of("grade", "score", "diagnosis")
    );

    private GeminiDiagnosisResult callGemini(String prompt) {
        try {
            Map<String, Object> requestBody = Map.of(
                "contents", List.of(Map.of("parts", List.of(Map.of("text", prompt)))),
                "generationConfig", Map.of(
                    "responseMimeType", "application/json",
                    "responseSchema", RESPONSE_SCHEMA,
                    // gemini-3 계열은 "thinking" 추론 토큰도 이 상한을 함께 소모한다.
                    // 상세 3섹션 리포트 + 추론 토큰을 감안해 넉넉히 잡는다 — 4096에서는
                    // diagnosis 문자열이 중간에 잘려 JSON 파싱이 깨지는 문제가 있었다.
                    "maxOutputTokens", 8192
                )
            );

            URI uri = URI.create(
                "https://generativelanguage.googleapis.com/v1beta/models/" + model
                    + ":generateContent?key=" + apiKey
            );

            HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(90))
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

            // responseSchema 지정 시 text 필드 안에 JSON 문자열이 들어온다 (JSON-in-JSON).
            JsonNode structured = objectMapper.readTree(textNode.asText());
            String grade = structured.path("grade").asText("C");
            int score = structured.path("score").asInt(50);
            String diagnosisText = structured.path("diagnosis").asText();
            if (diagnosisText.isBlank()) {
                log.error("[Gemini] 구조화 응답에 diagnosis 누락 body={}", response.body());
                throw new RuntimeException("AI 진단 응답을 해석할 수 없습니다");
            }
            return new GeminiDiagnosisResult(grade, score, diagnosisText);

        } catch (IOException e) {
            log.error("[Gemini] 호출 중 오류", e);
            throw new RuntimeException("AI 진단 서비스 호출 실패", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("AI 진단 서비스 호출이 중단되었습니다", e);
        }
    }
}
