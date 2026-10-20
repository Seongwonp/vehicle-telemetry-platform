package com.telemetry.config;

import com.telemetry.metrics.RedisMetrics;
import com.telemetry.security.ClientIpResolver;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
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
 * IP당 분당 요청 수 제한. Redis가 없으면 통과시킨다(fail-open).
 *
 * <p>{@code /api/**} 전체에 걸리므로 fail-closed면 Redis 장애가 모든 조회를 멈춘다.
 * 로그인 보호와 진단 제한은 반대로 fail-closed다. 같이 열면 Redis를 죽여 brute force
 * 방어를 끌 수 있다. 정책: {@code docs/redis-failure-policy.md} §3-2
 *
 * <p>통과할 때는 {@link RedisMetrics#RATE_LIMIT_FAIL_OPEN}과 WARN을 남긴다.
 * 버그를 삼키지 않도록 {@link DataAccessException}만 잡는다.
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

    /** 합계 카운터를 기동 시 0으로 등록한다({@link RedisMetrics#RATE_LIMIT_FAIL_OPEN_ALL}). */
    @PostConstruct
    void preRegisterMetrics() {
        RedisMetrics.preRegister(meterRegistry);
    }

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
            count = redisTemplate.opsForValue().increment(key);

            if (count != null && count == 1L) {
                // TTL은 첫 요청에만 건다. 매번 걸면 윈도우가 계속 밀려 제한이 안 걸린다.
                redisTemplate.expire(key, Duration.ofMinutes(1));
            }
        } catch (DataAccessException e) {
            return failOpen(request, e);
        }

        if (count == null) {
            // null도 세지 못한 것으로 보고 fail-open한다.
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
     * 세지 못했으므로 통과시킨다. 남은 횟수를 모르므로 제한 헤더는 붙이지 않는다.
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
