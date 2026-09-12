package com.telemetry.config;

import com.telemetry.security.BruteForceDetector;
import com.telemetry.security.LoginRateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * <b>fail-closed로 남기기로 한 경로들</b>을 고정한다 (2026-09-12 결정).
 *
 * <p>{@code RateLimitFailOpenTest}와 <b>반대 방향인 것이 요점이다.</b>
 * "Redis 장애 시 fail-open" 하나로 묶으면 <b>Redis를 죽이는 것이 곧 brute force 방어를
 * 끄는 방법</b>이 된다. {@code docs/redis-failure-policy.md} §3-2 ③④.
 *
 * <p>이 테스트들은 <b>예외가 밖으로 나가는 것</b>을 확인한다. 그게 fail-closed의 구현이다 —
 * {@code GlobalExceptionHandler}가 그걸 503 {@code REDIS_UNAVAILABLE}로 바꾼다.
 * <b>여기 try/catch가 추가되면 그 순간 조용히 fail-open이 되므로 이 테스트가 막는다.</b>
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("fail-closed 경로 — 예외를 삼키지 않는다")
class FailClosedPathsTest {

    @Mock
    private StringRedisTemplate redisTemplate;
    @Mock
    private ValueOperations<String, String> valueOps;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
    }

    private static RedisConnectionFailureException down() {
        return new RedisConnectionFailureException("Unable to connect to Redis");
    }

    @Test
    @DisplayName("진단 제한: Redis가 없으면 거부된다 — 조회와 일부러 다르다")
    void 진단은_fail_closed() {
        when(valueOps.increment(anyString())).thenThrow(down());

        DiagnosisRateLimitInterceptor interceptor =
            new DiagnosisRateLimitInterceptor(redisTemplate);
        ReflectionTestUtils.setField(interceptor, "requestsPerHour", 5);

        MockHttpServletRequest request =
            new MockHttpServletRequest("POST", "/api/vehicles/KR-GA-1234/diagnosis");

        assertThatThrownBy(() ->
            interceptor.preHandle(request, new MockHttpServletResponse(), new Object()))
            .as("진단은 비용이 드는 호출이라 장애 중 무제한 허용이 비용 폭증이 된다")
            .isInstanceOf(DataAccessException.class);
    }

    @Test
    @DisplayName("진단 제한: '세지 못했다'를 429로 보고하지 않는다")
    void 진단_null은_429가_아니다() {
        when(valueOps.increment(anyString())).thenReturn(null);

        DiagnosisRateLimitInterceptor interceptor =
            new DiagnosisRateLimitInterceptor(redisTemplate);
        ReflectionTestUtils.setField(interceptor, "requestsPerHour", 5);

        MockHttpServletRequest request =
            new MockHttpServletRequest("POST", "/api/vehicles/KR-GA-1234/diagnosis");
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertThatThrownBy(() -> interceptor.preHandle(request, response, new Object()))
            .as("429는 '한도를 다 썼다'는 뜻이고 그건 사실이 아니다 — 저장소가 답을 안 준 것이다")
            .isInstanceOf(DataAccessException.class);
        assertThat(response.getStatus()).isNotEqualTo(429);
    }

    @Test
    @DisplayName("진단 외 경로는 이 인터셉터가 Redis를 건드리지 않는다")
    void 진단_아닌_경로는_통과() throws Exception {
        DiagnosisRateLimitInterceptor interceptor =
            new DiagnosisRateLimitInterceptor(redisTemplate);
        ReflectionTestUtils.setField(interceptor, "requestsPerHour", 5);

        boolean passed = interceptor.preHandle(
            new MockHttpServletRequest("GET", "/api/vehicles"),
            new MockHttpServletResponse(), new Object());

        assertThat(passed).isTrue();
    }

    @Test
    @DisplayName("로그인 rate limit: Redis 예외를 삼키지 않는다")
    void 로그인_제한은_fail_closed() {
        when(valueOps.increment(anyString())).thenThrow(down());

        LoginRateLimiter limiter = new LoginRateLimiter(redisTemplate);
        ReflectionTestUtils.setField(limiter, "attemptsPerMinute", 10);

        assertThatThrownBy(() -> limiter.tryAcquire("10.0.0.1", "someone"))
            .as("여기서 true를 돌려주면 Redis를 죽이는 것이 곧 제한 해제가 된다")
            .isInstanceOf(DataAccessException.class);
    }

    @Test
    @DisplayName("brute force 차단 판정: Redis 예외를 삼키지 않는다")
    void 차단판정은_fail_closed() {
        when(valueOps.get(anyString())).thenThrow(down());

        BruteForceDetector detector = new BruteForceDetector(redisTemplate);

        assertThatThrownBy(() -> detector.isBlocked("10.0.0.1"))
            .as("false(차단 아님)를 돌려주면 차단된 IP가 장애 중에 풀린다")
            .isInstanceOf(DataAccessException.class);
    }

    @Test
    @DisplayName("brute force 실패 기록: Redis 예외를 삼키지 않는다")
    void 실패기록은_fail_closed() {
        when(valueOps.increment(anyString())).thenThrow(down());

        BruteForceDetector detector = new BruteForceDetector(redisTemplate);

        assertThatThrownBy(() -> detector.recordFailure("10.0.0.1"))
            .as("기록에 실패했는데 로그인 흐름이 계속되면 실패 횟수가 안 쌓인다 — "
                + "장애 중 무제한 시도가 된다")
            .isInstanceOf(DataAccessException.class);
    }
}
