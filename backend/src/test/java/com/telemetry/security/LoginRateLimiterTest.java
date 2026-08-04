package com.telemetry.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LoginRateLimiterTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private LoginRateLimiter rateLimiter;

    @Test
    void limitsByIpAndUsernameAndSetsTtlOnFirstAttempt() {
        ReflectionTestUtils.setField(rateLimiter, "attemptsPerMinute", 2);
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.increment(startsWith("login_rate:203.0.113.7:")))
            .willReturn(1L, 2L, 3L);

        assertThat(rateLimiter.tryAcquire("203.0.113.7", "admin")).isTrue();
        assertThat(rateLimiter.tryAcquire("203.0.113.7", "admin")).isTrue();
        assertThat(rateLimiter.tryAcquire("203.0.113.7", "admin")).isFalse();
        verify(redisTemplate).expire(startsWith("login_rate:203.0.113.7:"),
            org.mockito.ArgumentMatchers.any());
    }
}
