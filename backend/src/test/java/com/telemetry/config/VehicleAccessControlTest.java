package com.telemetry.config;

import com.telemetry.controller.AnomalyController;
import com.telemetry.controller.DiagnosisController;
import com.telemetry.controller.TelemetryController;
import com.telemetry.controller.VehicleController;
import com.telemetry.security.ClientIpResolver;
import com.telemetry.security.VehicleAccessService;
import com.telemetry.service.AnomalyService;
import com.telemetry.service.DiagnosisService;
import com.telemetry.service.TelemetryQueryService;
import com.telemetry.service.VehicleService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 차량 하위 REST 엔드포인트가 <b>소유자·관리자만</b> 통과하는지 본다.
 *
 * <p>2026-09-16 이전에는 이 검사가 WebSocket 구독에만 있었다. 인증만 하면 남의 차량
 * 텔레메트리·이상 이력·진단을 REST로 읽을 수 있었고, 그 사실을 잡는 테스트가 없었다.
 *
 * <p><b>404를 기대하는 이유</b>: 403은 "그 차량은 존재한다"를 알려준다. 접근 못 하는 차량과
 * 없는 차량의 응답을 같게 둔다.
 */
@WebMvcTest({VehicleController.class, TelemetryController.class,
    AnomalyController.class, DiagnosisController.class})
@ImportAutoConfiguration({MetricsAutoConfiguration.class, SimpleMetricsExportAutoConfiguration.class})
@DisplayName("차량 접근 제어 — 소유자·관리자만")
class VehicleAccessControlTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean private VehicleService vehicleService;
    @MockBean private TelemetryQueryService telemetryQueryService;
    @MockBean private AnomalyService anomalyService;
    @MockBean private DiagnosisService diagnosisService;
    @MockBean private VehicleAccessService vehicleAccessService;
    @MockBean private StringRedisTemplate redisTemplate;
    @MockBean private ValueOperations<String, String> valueOperations;
    @MockBean private ClientIpResolver clientIpResolver;

    @BeforeEach
    void rateLimitPasses() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.increment(anyString())).willReturn(1L);
        given(clientIpResolver.resolve(any())).willReturn("127.0.0.1");
    }

    @Test
    @WithMockUser(username = "other")
    @DisplayName("접근 권한이 없는 차량의 텔레메트리 → 404, 서비스는 호출되지 않는다")
    void 남의차량_텔레메트리_404() throws Exception {
        given(vehicleAccessService.canAccess(any(), anyString())).willReturn(false);

        mockMvc.perform(get("/api/vehicles/KR-GA-1234/telemetry/latest"))
            .andExpect(status().isNotFound());

        verify(telemetryQueryService, never()).getLatest(anyString());
    }

    @Test
    @WithMockUser(username = "other")
    @DisplayName("접근 권한이 없는 차량의 이상 이력 → 404")
    void 남의차량_이상이력_404() throws Exception {
        given(vehicleAccessService.canAccess(any(), anyString())).willReturn(false);

        mockMvc.perform(get("/api/vehicles/KR-GA-1234/anomalies"))
            .andExpect(status().isNotFound());

        verify(anomalyService, never()).getRecent(anyString(), anyInt());
    }

    @Test
    @WithMockUser(username = "other")
    @DisplayName("접근 권한이 없는 차량의 진단 → 404, 외부 AI 호출까지 가지 않는다")
    void 남의차량_진단_404() throws Exception {
        given(vehicleAccessService.canAccess(any(), anyString())).willReturn(false);

        mockMvc.perform(get("/api/vehicles/KR-GA-1234/diagnosis"))
            .andExpect(status().isNotFound());

        verify(diagnosisService, never()).diagnose(anyString());
    }

    @Test
    @WithMockUser(username = "hong")
    @DisplayName("접근 권한이 있으면 그대로 통과한다")
    void 소유자_통과() throws Exception {
        given(vehicleAccessService.canAccess(any(), anyString())).willReturn(true);
        given(anomalyService.getRecent("KR-GA-1234", 20)).willReturn(List.of());

        mockMvc.perform(get("/api/vehicles/KR-GA-1234/anomalies"))
            .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(username = "hong")
    @DisplayName("목록은 차량을 지정하지 않으므로 이 검사를 타지 않는다 — 대신 서비스가 소유자로 자른다")
    void 목록은_접근검사_대상아님() throws Exception {
        given(vehicleService.findAllVisibleTo(any())).willReturn(List.of());

        mockMvc.perform(get("/api/vehicles"))
            .andExpect(status().isOk());

        verify(vehicleAccessService, never()).canAccess(any(), anyString());
    }
}
