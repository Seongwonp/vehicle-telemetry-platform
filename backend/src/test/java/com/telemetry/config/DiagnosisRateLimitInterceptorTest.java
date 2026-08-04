package com.telemetry.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class DiagnosisRateLimitInterceptorTest {

    @Mock StringRedisTemplate redisTemplate;
    @Mock ValueOperations<String, String> valueOperations;
    @InjectMocks DiagnosisRateLimitInterceptor interceptor;

    @Test
    void limitsPerAuthenticatedUserAndVehicle() throws Exception {
        ReflectionTestUtils.setField(interceptor, "requestsPerHour", 1);
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.increment(startsWith("diagnosis_rate:admin:KR-GA-1234")))
            .willReturn(1L, 2L);
        SecurityContextHolder.getContext().setAuthentication(
            new UsernamePasswordAuthenticationToken("admin", null));
        MockHttpServletRequest request = new MockHttpServletRequest("GET",
            "/api/vehicles/KR-GA-1234/diagnosis");

        assertThat(interceptor.preHandle(request, new MockHttpServletResponse(), new Object())).isTrue();
        assertThat(interceptor.preHandle(request, new MockHttpServletResponse(), new Object())).isFalse();
        SecurityContextHolder.clearContext();
    }
}
