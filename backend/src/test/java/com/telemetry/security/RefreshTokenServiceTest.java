package com.telemetry.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("RefreshTokenService 단위 테스트")
class RefreshTokenServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private RefreshTokenService refreshTokenService;

    // opsForValue()를 쓰지 않는 revoke() 테스트까지 공용으로 스텁하면 Mockito strict stubs가
    // "사용되지 않은 스텁"으로 그 테스트를 실패시키므로, 필요한 테스트에만 개별 스텁한다.

    @Test
    @DisplayName("발급 시 Redis에 TTL과 함께 저장하고 토큰 문자열을 반환")
    void issue_토큰_저장() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);

        String token = refreshTokenService.issue("admin");

        assertThat(token).isNotBlank();
        verify(valueOperations).set(eq("refresh_token:" + token), eq("admin"), eq(Duration.ofDays(14)));
    }

    @Test
    @DisplayName("유효한 토큰으로 rotate 시 username 반환하고 기존 토큰 삭제")
    void rotate_유효토큰_성공() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get("refresh_token:valid-token")).willReturn("admin");

        Optional<String> result = refreshTokenService.rotate("valid-token");

        assertThat(result).contains("admin");
        verify(redisTemplate).delete("refresh_token:valid-token");
    }

    @Test
    @DisplayName("존재하지 않는 토큰으로 rotate 시 빈 Optional 반환하고 삭제하지 않음")
    void rotate_유효하지않은토큰_빈값() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get(anyString())).willReturn(null);

        Optional<String> result = refreshTokenService.rotate("unknown-token");

        assertThat(result).isEmpty();
        verify(redisTemplate, org.mockito.Mockito.never()).delete(anyString());
    }

    @Test
    @DisplayName("로그아웃 시 토큰 삭제")
    void revoke_토큰_삭제() {
        refreshTokenService.revoke("some-token");

        verify(redisTemplate).delete("refresh_token:some-token");
    }
}
