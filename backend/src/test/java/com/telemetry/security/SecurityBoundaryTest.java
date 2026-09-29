package com.telemetry.security;

import com.telemetry.controller.AuthController;
import com.telemetry.controller.VehicleController;
import com.telemetry.service.VehicleService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.MediaType;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Production SecurityFilterChain: test actual 401/403, not the default slice security. */
@WebMvcTest(controllers = {AuthController.class, VehicleController.class}, properties = {
    "admin.password=test-only-admin-password", "cors.allowed-origin-patterns=http://localhost:*"
})
@Import(SecurityConfig.class)
@org.springframework.boot.autoconfigure.ImportAutoConfiguration({
    org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration.class,
    org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration.class
})
class SecurityBoundaryTest {
    @Autowired MockMvc mvc;
    @MockBean JwtTokenProvider jwt;
    @MockBean UserDetailsService users;
    @MockBean VehicleService vehicles;
    @MockBean VehicleAccessService access;
    @MockBean RefreshTokenService refresh;
    @MockBean BruteForceDetector bruteForce;
    @MockBean LoginRateLimiter loginLimiter;
    @MockBean ClientIpResolver ips;
    @MockBean StringRedisTemplate redis;
    @MockBean ValueOperations<String, String> values;

    @org.junit.jupiter.api.BeforeEach
    void stubRateLimiter() {
        // RateLimitInterceptor는 인증 뒤 모든 /api/** 요청에서 Redis를 친다. 슬라이스에는 Redis가
        // 없으므로 mock이 null을 돌려주고 500이 났다 — 인가 결과가 아니라 인터셉터 NPE였다.
        given(redis.opsForValue()).willReturn(values);
        given(values.increment(anyString())).willReturn(1L);
        given(ips.resolve(any())).willReturn("127.0.0.1");
    }

    @Test
    void anonymousMeRequiresAuthentication() throws Exception {
        mvc.perform(get("/api/auth/me")).andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
    }

    @Test
    void validTokenForInactiveAccountReturns401() throws Exception {
        given(jwt.validate("token")).willReturn(true);
        given(jwt.getUsername("token")).willReturn("inactive");
        given(users.loadUserByUsername("inactive")).willThrow(new UsernameNotFoundException("unavailable"));
        mvc.perform(get("/api/auth/me").header("Authorization", "Bearer token"))
            .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
    }

    @Test
    @WithMockUser(username = "driver", roles = "USER")
    void meUsesAuthenticatedAuthorities() throws Exception {
        mvc.perform(get("/api/auth/me")).andExpect(status().isOk())
            .andExpect(jsonPath("$.username").value("driver"))
            .andExpect(jsonPath("$.roles[0]").value("ROLE_USER"));
    }

    @Test
    @WithMockUser(roles = "USER")
    void ordinaryUserCannotClaimVehicleEvenWithoutOwner() throws Exception {
        mvc.perform(post("/api/vehicles").contentType(MediaType.APPLICATION_JSON)
            .content("{\"vehicleId\":\"TEST-001\",\"name\":\"test\"}"))
            .andExpect(status().isForbidden())
            // 본문까지 본다 — sendError(403)였다면 MockMvc는 본문이 비고, 실제 Tomcat은 /error 디스패치에서
            // 401로 바뀐다(2026-09-28 E2E). JSON을 직접 쓰는 handler만 이 assertion을 통과한다.
            .andExpect(jsonPath("$.code").value("FORBIDDEN"));
        verify(vehicles, never()).register(any(), any());
    }

    @Test
    @WithMockUser(username = "driver", roles = "USER")
    void ownerCannotDeactivateOwnVehicle() throws Exception {
        // 비활성 차량은 관리자도 못 보고 재등록은 409다 — 일반 사용자가 되돌릴 수 없는 삭제를 하던 경로(2026-09-29).
        given(access.canAccess(any(), anyString())).willReturn(true);
        mvc.perform(delete("/api/vehicles/TEST-001"))
            .andExpect(status().isForbidden())
            .andExpect(jsonPath("$.code").value("FORBIDDEN"));
        verify(vehicles, never()).deactivate(anyString());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void adminCanDeactivateVehicle() throws Exception {
        given(access.canAccess(any(), anyString())).willReturn(true);
        mvc.perform(delete("/api/vehicles/TEST-001")).andExpect(status().isNoContent());
        verify(vehicles).deactivate("TEST-001");
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void adminCanRegisterVehicle() throws Exception {
        mvc.perform(post("/api/vehicles").contentType(MediaType.APPLICATION_JSON)
            .content("{\"vehicleId\":\"TEST-001\",\"name\":\"test\",\"owner\":\"driver\"}"))
            .andExpect(status().isCreated());
        verify(vehicles).register(any(), any());
    }
}
