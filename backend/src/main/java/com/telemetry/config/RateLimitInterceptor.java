package com.telemetry.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class RateLimitInterceptor implements HandlerInterceptor {

    private final StringRedisTemplate redisTemplate;

    @Value("${rate-limit.requests-per-minute}")
    private int requestsPerMinute;

    @Override
    public boolean preHandle(
        HttpServletRequest request,
        HttpServletResponse response,
        Object handler
    ) throws Exception {
        String clientIp = getClientIp(request);
        String key = "rate_limit:" + clientIp;

        Long count = redisTemplate.opsForValue().increment(key);

        if (count == 1) {
            // 첫 요청이면 TTL 1분 설정
            redisTemplate.expire(key, Duration.ofMinutes(1));
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

    private String getClientIp(HttpServletRequest request) {
        String xForwardedFor = request.getHeader("X-Forwarded-For");
        if (xForwardedFor != null && !xForwardedFor.isBlank()) {
            return xForwardedFor.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
