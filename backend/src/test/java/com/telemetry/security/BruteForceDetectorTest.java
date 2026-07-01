package com.telemetry.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("BruteForceDetector 단위 테스트")
class BruteForceDetectorTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private BruteForceDetector bruteForceDetector;

    // opsForValue()를 쓰지 않는 테스트(recordSuccess)까지 공용 @BeforeEach로 스텁하면
    // Mockito strict stubs가 "사용되지 않은 스텁"으로 그 테스트를 실패시킨다.
    // 그래서 각 테스트마다 필요한 경우에만 개별적으로 스텁한다.

    @Test
    @DisplayName("실패 이력 없는 IP → 차단 안 됨")
    void isBlocked_이력없음_false() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get(anyString())).willReturn(null);

        assertThat(bruteForceDetector.isBlocked("192.168.1.1")).isFalse();
    }

    @Test
    @DisplayName("4회 실패 → 아직 차단 안 됨")
    void isBlocked_4회실패_false() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get(anyString())).willReturn("4");

        assertThat(bruteForceDetector.isBlocked("192.168.1.1")).isFalse();
    }

    @Test
    @DisplayName("5회 실패 → 차단")
    void isBlocked_5회실패_true() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get(anyString())).willReturn("5");

        assertThat(bruteForceDetector.isBlocked("192.168.1.1")).isTrue();
    }

    @Test
    @DisplayName("6회 이상 실패도 차단 유지")
    void isBlocked_6회이상_true() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get(anyString())).willReturn("10");

        assertThat(bruteForceDetector.isBlocked("192.168.1.1")).isTrue();
    }

    @Test
    @DisplayName("로그인 성공 시 Redis 카운터 삭제")
    void recordSuccess_카운터_삭제() {
        bruteForceDetector.recordSuccess("192.168.1.1");

        verify(redisTemplate).delete(contains("192.168.1.1"));
    }

    @Test
    @DisplayName("첫 실패 시 Redis TTL 설정")
    void recordFailure_첫실패_TTL설정() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.increment(anyString())).willReturn(1L);

        bruteForceDetector.recordFailure("192.168.1.1");

        verify(redisTemplate).expire(anyString(), any());
    }
}
