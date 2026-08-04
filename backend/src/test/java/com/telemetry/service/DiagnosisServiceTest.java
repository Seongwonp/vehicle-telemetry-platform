package com.telemetry.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.exception.ResourceNotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.List;

class DiagnosisServiceTest {

    @Test
    void geminiApiKeyIsSentInHeaderNotQueryString() throws Exception {
        DiagnosisService service = new DiagnosisService(
            mock(TelemetryQueryService.class), mock(AnomalyService.class), new ObjectMapper());
        ReflectionTestUtils.setField(service, "model", "gemini-test");
        ReflectionTestUtils.setField(service, "apiKey", "secret-key");

        var request = service.buildGeminiRequest("prompt");

        assertThat(request.uri().getQuery()).isNull();
        assertThat(request.headers().firstValue("x-goog-api-key")).contains("secret-key");
    }

    @Test
    void diagnoseWithoutTelemetryUsesDomainException() {
        TelemetryQueryService telemetryQueryService = mock(TelemetryQueryService.class);
        given(telemetryQueryService.getRecent("KR-GA-1234", 20)).willReturn(List.of());
        DiagnosisService service = new DiagnosisService(
            telemetryQueryService, mock(AnomalyService.class), new ObjectMapper());

        assertThatThrownBy(() -> service.diagnose("KR-GA-1234"))
            .isInstanceOf(ResourceNotFoundException.class)
            .hasMessageContaining("진단할 텔레메트리 데이터가 없습니다");
    }
}
