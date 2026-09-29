package com.telemetry.security;

import com.telemetry.controller.AuthController;
import com.telemetry.controller.UserController;
import com.telemetry.exception.CurrentPasswordMismatchException;
import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.service.UserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 비밀번호 변경·초기화 API를 <b>운영 SecurityFilterChain</b>으로 본다 — 상태 코드와 {@code $.code}까지.
 * (슬라이스 기본 보안이면 401/403 본문이 운영과 달라진다. SecurityBoundaryTest와 같은 이유.)
 */
@WebMvcTest(controllers = {AuthController.class, UserController.class}, properties = {
    "admin.password=test-only-admin-password", "cors.allowed-origin-patterns=http://localhost:*"
})
@Import(SecurityConfig.class)
@org.springframework.boot.autoconfigure.ImportAutoConfiguration({
    org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration.class,
    org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration.class
})
@DisplayName("비밀번호 변경·초기화 API — 운영 보안 체인")
class PasswordApiSecurityTest {

    private static final String CHANGE = "{\"currentPassword\":\"dummy-old-pw-not-real\",\"newPassword\":\"dummy-new-pw-not-real\"}";
    private static final String RESET = "{\"newPassword\":\"dummy-new-pw-not-real\"}";

    @Autowired MockMvc mvc;
    @MockBean UserService userService;
    @MockBean VehicleAccessService access;
    @MockBean JwtTokenProvider jwt;
    @MockBean UserDetailsService users;
    @MockBean RefreshTokenService refresh;
    @MockBean BruteForceDetector bruteForce;
    @MockBean LoginRateLimiter loginLimiter;
    @MockBean ClientIpResolver ips;
    @MockBean StringRedisTemplate redis;
    @MockBean ValueOperations<String, String> values;

    @BeforeEach
    void stubRateLimiter() {
        // RateLimitInterceptor가 mock Redis에서 NPE → 500이 되던 이력 (SecurityBoundaryTest와 같은 stub).
        given(redis.opsForValue()).willReturn(values);
        given(values.increment(anyString())).willReturn(1L);
        given(ips.resolve(any())).willReturn("127.0.0.1");
    }

    // ── 본인 변경 ──────────────────────────────────────────────

    @Test
    @DisplayName("익명 → 401 UNAUTHORIZED (/api/auth/**가 permitAll이어도 이 경로는 인증 필요)")
    void 익명_401() throws Exception {
        mvc.perform(post("/api/auth/password").contentType(MediaType.APPLICATION_JSON).content(CHANGE))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
        verify(userService, never()).changePassword(any(), any(), any());
    }

    @Test
    @WithMockUser(username = "hong", roles = "USER")
    @DisplayName("본인 성공 → 204, 토큰의 사용자 이름으로 서비스를 호출한다")
    void 본인_성공_204() throws Exception {
        mvc.perform(post("/api/auth/password").contentType(MediaType.APPLICATION_JSON).content(CHANGE))
            .andExpect(status().isNoContent());
        verify(userService).changePassword("hong", "dummy-old-pw-not-real", "dummy-new-pw-not-real");
    }

    @Test
    @WithMockUser(username = "hong", roles = "USER")
    @DisplayName("현재 비밀번호 틀림 → 400 CURRENT_PASSWORD_INCORRECT (401이면 클라이언트가 세션 만료로 읽는다)")
    void 현재비밀번호_틀림_400() throws Exception {
        willThrow(new CurrentPasswordMismatchException()).given(userService).changePassword(any(), any(), any());
        mvc.perform(post("/api/auth/password").contentType(MediaType.APPLICATION_JSON).content(CHANGE))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("CURRENT_PASSWORD_INCORRECT"));
    }

    @Test
    @WithMockUser(username = "hong", roles = "USER")
    @DisplayName("정책 위반(짧은 새 비밀번호) → 400 VALIDATION_FAILED, 서비스는 호출되지 않는다")
    void 정책위반_400() throws Exception {
        mvc.perform(post("/api/auth/password").contentType(MediaType.APPLICATION_JSON)
                .content("{\"currentPassword\":\"dummy-old-pw-not-real\",\"newPassword\":\"short\"}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
        verify(userService, never()).changePassword(any(), any(), any());
    }

    @Test
    @WithMockUser(username = "hong", roles = "USER")
    @DisplayName("정책 위반(73자 초과) → 400 VALIDATION_FAILED")
    void 정책위반_너무긴_400() throws Exception {
        mvc.perform(post("/api/auth/password").contentType(MediaType.APPLICATION_JSON)
                .content("{\"currentPassword\":\"dummy-old-pw-not-real\",\"newPassword\":\"" + "x".repeat(73) + "\"}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
    }

    @Test
    @WithMockUser(username = "hong", roles = "USER")
    @DisplayName("현재 비밀번호 누락 → 400 VALIDATION_FAILED")
    void 현재비밀번호_누락_400() throws Exception {
        mvc.perform(post("/api/auth/password").contentType(MediaType.APPLICATION_JSON)
                .content("{\"newPassword\":\"dummy-new-pw-not-real\"}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
    }

    @Test
    @DisplayName("유효한 JWT지만 비활성 계정 → 필터가 인증을 세우지 않아 401, 서비스는 호출되지 않는다")
    void 비활성계정_401() throws Exception {
        given(jwt.validate("token")).willReturn(true);
        given(jwt.getUsername("token")).willReturn("inactive");
        given(users.loadUserByUsername("inactive")).willThrow(new UsernameNotFoundException("unavailable"));

        mvc.perform(post("/api/auth/password").header("Authorization", "Bearer token")
                .contentType(MediaType.APPLICATION_JSON).content(CHANGE))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
        verify(userService, never()).changePassword(any(), any(), any());
    }

    @Test
    @WithMockUser(username = "hong", roles = "USER")
    @DisplayName("서비스가 로그인 불가 계정으로 판단(BadCredentials) → 401 UNAUTHORIZED, CURRENT_PASSWORD_INCORRECT와 구분")
    void 서비스단_비활성_401() throws Exception {
        willThrow(new BadCredentialsException("x")).given(userService).changePassword(any(), any(), any());
        mvc.perform(post("/api/auth/password").contentType(MediaType.APPLICATION_JSON).content(CHANGE))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
    }

    // ── 관리자 초기화 ──────────────────────────────────────────

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    @DisplayName("관리자 초기화 → 204")
    void 관리자_초기화_204() throws Exception {
        mvc.perform(put("/api/users/hong/password").contentType(MediaType.APPLICATION_JSON).content(RESET))
            .andExpect(status().isNoContent());
        verify(userService).resetPassword("hong", "dummy-new-pw-not-real");
    }

    @Test
    @WithMockUser(username = "hong", roles = "USER")
    @DisplayName("일반 사용자 → 403 FORBIDDEN, 본인 것이어도 서비스는 호출되지 않는다")
    void 일반사용자_초기화_403() throws Exception {
        mvc.perform(put("/api/users/hong/password").contentType(MediaType.APPLICATION_JSON).content(RESET))
            .andExpect(status().isForbidden())
            .andExpect(jsonPath("$.code").value("FORBIDDEN"));
        verify(userService, never()).resetPassword(any(), any());
    }

    @Test
    @DisplayName("익명 → 401 UNAUTHORIZED")
    void 익명_초기화_401() throws Exception {
        mvc.perform(put("/api/users/hong/password").contentType(MediaType.APPLICATION_JSON).content(RESET))
            .andExpect(status().isUnauthorized())
            .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
    }

    @Test
    @WithMockUser(username = "admin", roles = "ADMIN")
    @DisplayName("관리자 초기화 — 정책 위반 400, 없는 사용자 404 NOT_FOUND")
    void 관리자_초기화_오류() throws Exception {
        mvc.perform(put("/api/users/hong/password").contentType(MediaType.APPLICATION_JSON)
                .content("{\"newPassword\":\"short\"}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));

        willThrow(new ResourceNotFoundException("없음")).given(userService).resetPassword(any(), any());
        mvc.perform(put("/api/users/ghost/password").contentType(MediaType.APPLICATION_JSON).content(RESET))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.code").value("NOT_FOUND"));
    }
}
