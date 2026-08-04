package com.telemetry.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        Long count = redisTemplate.opsForValue().increment(key);
        if (count != null && count == 1L) redisTemplate.expire(key, Duration.ofHours(1));
        if (count == null || count > requestsPerHour) {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            response.setContentType("application/json");
            response.getWriter().write("{\"code\":\"TOO_MANY_REQUESTS\",\"message\":\"AI 진단 요청 한도를 초과했습니다\"}");
            return false;
        }
        return true;
    }
}
