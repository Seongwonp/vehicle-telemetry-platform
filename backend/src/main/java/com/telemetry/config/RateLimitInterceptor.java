package com.telemetry.config;

import com.telemetry.metrics.RedisMetrics;
import com.telemetry.security.ClientIpResolver;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.time.Duration;

/**
 * IP당 분당 요청 수 제한. <b>Redis가 없으면 통과시킨다(fail-open).</b>
 *
 * <h3>왜 fail-open인가 — 결정(2026-09-12)</h3>
 *
 * 이 제한은 <b>남용 방어</b>이지 데이터 정확성이나 인증과 무관하다. 그런데 인터셉터가
 * {@code /api/**} 전체에 걸려 있어서, fail-closed로 두면 <b>Redis 하나가 죽을 때
 * 모든 조회 API가 멈춘다.</b> 정상 사용자 전체를 막는 비용이 장애 중 남용 위험보다 크다.
 *
 * <p><b>여기서만 그렇다.</b> 같은 문서의 다른 결정은 반대 방향이다 —
 * 로그인 보호({@code BruteForceDetector}·{@code LoginRateLimiter})와
 * 진단 제한({@code DiagnosisRateLimitInterceptor})은 <b>fail-closed 유지</b>다.
 * 하나로 묶으면 <b>Redis를 죽이는 것이 곧 brute force 방어를 끄는 방법</b>이 된다.
 * {@code docs/redis-failure-policy.md} §3-2.
 *
 * <h3>통과시키되 조용히 통과시키지 않는다</h3>
 *
 * {@link RedisMetrics#RATE_LIMIT_FAIL_OPEN}을 올리고 WARN을 남긴다.
 * <b>이 지표가 오르는 동안은 남용 방어가 없는 상태다.</b> 그게 안 보이면
 * fail-open은 그냥 "제한이 사라진 줄 모르는 상태"가 된다.
 *
 * <h3>저장소 장애만 열어준다</h3>
 *
 * {@link DataAccessException}만 잡는다. 우리 코드의 버그까지 삼키면
 * 버그가 "장애 중 정상 통과"로 위장된다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RateLimitInterceptor implements HandlerInterceptor {

    private final StringRedisTemplate redisTemplate;
    private final ClientIpResolver clientIpResolver;
    private final MeterRegistry meterRegistry;

    @Value("${rate-limit.requests-per-minute}")
    private int requestsPerMinute;

    @Override
    public boolean preHandle(
        HttpServletRequest request,
        HttpServletResponse response,
        Object handler
    ) throws Exception {
        String clientIp = clientIpResolver.resolve(request);
        String key = "rate_limit:" + clientIp;

        Long count;
        try {
            // INCR는 원자 연산이라 동시 요청이 와도 카운터가 정확하다.
            count = redisTemplate.opsForValue().increment(key);

            if (count != null && count == 1L) {
                // TTL을 첫 요청 시점에만 설정한다. 이후 요청마다 expire를 호출하면
                // 윈도우가 매 요청마다 리셋되어 사실상 제한이 걸리지 않는다.
                redisTemplate.expire(key, Duration.ofMinutes(1));
            }
        } catch (DataAccessException e) {
            return failOpen(request, e);
        }

        if (count == null) {
            // 예외 없이 null이 오는 경우도 "세지 못했다"이다. 세지 못한 것을 초과로 읽지 않는다.
            return failOpen(request, null);
        }

        if (count > requestsPerMinute) {
            log.warn("[RateLimit] 초과 IP={} count={}/{}", clientIp, count, requestsPerMinute);
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            response.setContentType("application/json");
            response.getWriter().write(
                "{\"error\":\"Too Many Requests\",\"message\":\"분당 " + requestsPerMinute + "회 초과\"}"
            );
            return false;
        }

        response.addHeader("X-RateLimit-Limit", String.valueOf(requestsPerMinute));
        response.addHeader("X-RateLimit-Remaining", String.valueOf(requestsPerMinute - count));
        return true;
    }

    /**
     * 세지 못했으므로 통과시킨다. <b>제한 헤더는 붙이지 않는다</b> —
     * 남은 횟수를 모르는데 숫자를 적으면 클라이언트에게 거짓말이 된다.
     */
    private boolean failOpen(HttpServletRequest request, DataAccessException e) {
        String route = RedisMetrics.routeTemplate(request);
        RedisMetrics.rateLimitFailOpen(meterRegistry, route);
        // 예외 메시지에 접속 정보가 섞일 수 있어 클래스 이름만 남긴다.
        log.warn("[RateLimit] Redis 사용 불가 — 제한 없이 통과시킨다(fail-open) route={} cause={}",
            route, e == null ? "null-count" : e.getClass().getSimpleName());
        return true;
    }
}
