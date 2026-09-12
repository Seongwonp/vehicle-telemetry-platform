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
 * 사용자·차량당 시간당 진단 횟수 제한. <b>Redis가 없으면 거부한다(fail-closed).</b>
 *
 * <h3>왜 조회와 반대인가 — 결정(2026-09-12)</h3>
 *
 * 바로 옆의 {@link RateLimitInterceptor}는 fail-open인데 여기는 fail-closed다.
 * <b>일부러 다르다.</b> 일반 조회 제한은 남용 방어지만 <b>진단은 비용이 드는 호출</b>이라,
 * 장애 중 무제한 허용이 비용 폭증이 된다. 조회는 막으면 정상 사용자 전체를 잃지만
 * 진단은 막아도 잃는 것이 적다. {@code docs/redis-failure-policy.md} §3-2 ③.
 *
 * <p>거부는 {@code GlobalExceptionHandler}에서 <b>503 {@code REDIS_UNAVAILABLE}</b>이 된다 —
 * Redis 예외를 여기서 잡지 않고 그대로 올려보내는 것이 그 경로다.
 * <b>여기에 try/catch를 추가하면 그 순간 fail-open이 된다.</b>
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

        // Redis 예외는 잡지 않는다 — fail-closed가 이 경로로 실현된다.
        Long count = redisTemplate.opsForValue().increment(key);

        if (count == null) {
            // **"세지 못했다"를 "초과했다"로 보고하지 않는다.** 429는 사용자에게
            // "한도를 다 썼다"는 뜻이고 그건 사실이 아니다. 저장소가 답을 안 준 것이다.
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
