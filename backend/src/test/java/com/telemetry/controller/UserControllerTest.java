package com.telemetry.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.dto.request.UserCreateRequest;
import com.telemetry.dto.response.UserResponse;
import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.security.ClientIpResolver;
import com.telemetry.security.VehicleAccessService;
import com.telemetry.service.UserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 사용자 생성은 관리자만 — 메서드 보안({@code @PreAuthorize})이 실제로 걸리는지 본다.
 * 자가 가입이 열리면 남의 차량 ID를 먼저 등록해 텔레메트리를 가져가는 길이 생긴다(ADR-027).
 */
@WebMvcTest(UserController.class)
@Import(UserControllerTest.MethodSecurity.class)
@ImportAutoConfiguration({MetricsAutoConfiguration.class, SimpleMetricsExportAutoConfiguration.class})
@DisplayName("UserController — 관리자 전용")
class UserControllerTest {

    /** 슬라이스에는 SecurityConfig가 없어 {@code @EnableMethodSecurity}를 여기서 켠다 — 운영은 SecurityConfig가 켠다. */
    @TestConfiguration
    @EnableMethodSecurity
    static class MethodSecurity {}

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;
    @MockBean UserService userService;
    @MockBean StringRedisTemplate redisTemplate;
    @MockBean ValueOperations<String, String> valueOperations;
    @MockBean ClientIpResolver clientIpResolver;
    @MockBean VehicleAccessService vehicleAccessService;

    @BeforeEach
    void rateLimitPasses() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.increment(anyString())).willReturn(1L);
        given(clientIpResolver.resolve(any())).willReturn("127.0.0.1");
    }

    private String body() throws Exception {
        UserCreateRequest request = new UserCreateRequest();
        request.setUsername("hong");
        request.setPassword("correct-horse-battery");
        request.setRole(Role.USER);
        return objectMapper.writeValueAsString(request);
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    @DisplayName("관리자 → 201, 응답에 비밀번호 해시가 없다")
    void 관리자_201() throws Exception {
        given(userService.create(any())).willReturn(new UserResponse(new User("hong", "$2a$hash", Role.USER)));

        mockMvc.perform(post("/api/users").with(csrf()).contentType(MediaType.APPLICATION_JSON).content(body()))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.username").value("hong"))
            .andExpect(jsonPath("$.passwordHash").doesNotExist());
    }

    @Test
    @WithMockUser(username = "hong", roles = "USER")
    @DisplayName("일반 사용자 → 403, 서비스는 호출되지 않는다")
    void 일반사용자_403() throws Exception {
        mockMvc.perform(post("/api/users").with(csrf()).contentType(MediaType.APPLICATION_JSON).content(body()))
            .andExpect(status().isForbidden());
        verify(userService, never()).create(any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    @DisplayName("짧은 비밀번호 → 400")
    void 짧은비밀번호_400() throws Exception {
        UserCreateRequest request = new UserCreateRequest();
        request.setUsername("hong");
        request.setPassword("short");
        request.setRole(Role.USER);

        mockMvc.perform(post("/api/users").with(csrf()).contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isBadRequest());
    }
}
