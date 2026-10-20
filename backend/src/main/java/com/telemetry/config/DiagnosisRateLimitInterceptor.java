package com.telemetry.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 사용자·차량당 시간당 진단 횟수 제한. Redis가 없으면 거부한다(fail-closed).
 *
 * <p>{@link RateLimitInterceptor}와 일부러 반대다. 진단은 비용이 드는 외부 호출이라
 * 장애 중 무제한 허용이 비용 폭증이 된다. 정책: {@code docs/redis-failure-policy.md} §3-2
 *
 * <p>Redis 예외를 그대로 올려 {@code GlobalExceptionHandler}가 503으로 만든다.
 * 여기에 try/catch를 넣으면 fail-open이 된다.
 */
@Component
@RequiredArgsConstructor
public class DiagnosisRateLimitInterceptor implements HandlerInterceptor {

    private static final Pattern PATH = Pattern.compile("^/api/vehicles/([A-Z0-9-]{4,20})/diagnosis$");
    private final StringRedisTemplate redisTemplate;

    @Value("${rate-limit.diagnosis-requests-per-hour:5}")
    private int requestsPerHour;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
        throws Exception {
        Matcher matcher = PATH.matcher(request.getRequestURI());
        if (!matcher.matches()) return true;
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String username = authentication == null ? "anonymous" : authentication.getName();
        String key = "diagnosis_rate:" + username + ":" + matcher.group(1);

        // Redis 예외는 잡지 않는다(fail-closed).
        Long count = redisTemplate.opsForValue().increment(key);

        if (count == null) {
            // 세지 못한 것을 429(한도 초과)로 보고하지 않는다.
            throw new DataAccessResourceFailureException("진단 제한 카운터를 읽지 못했다");
        }

        if (count == 1L) {
            redisTemplate.expire(key, Duration.ofHours(1));
        }

        if (count > requestsPerHour) {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            response.setContentType("application/json");
            response.getWriter().write("{\"code\":\"TOO_MANY_REQUESTS\",\"message\":\"AI 진단 요청 한도를 초과했습니다\"}");
            return false;
        }
        return true;
    }
}
