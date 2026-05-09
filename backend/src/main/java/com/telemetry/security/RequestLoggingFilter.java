package com.telemetry.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * 모든 API 요청/응답을 MDC 컨텍스트와 함께 로깅.
 * traceId로 요청 추적 가능, 4xx/5xx는 경고/에러 레벨로 기록.
 */
@Slf4j
@Component
@Order(1)
public class RequestLoggingFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(
        HttpServletRequest request,
        HttpServletResponse response,
        FilterChain chain
    ) throws ServletException, IOException {

        String traceId = UUID.randomUUID().toString().substring(0, 8);
        String ip = resolveClientIp(request);
        String method = request.getMethod();
        String uri = request.getRequestURI();

        MDC.put("traceId", traceId);
        MDC.put("clientIp", ip);

        // 클라이언트가 요청 추적할 수 있도록 응답 헤더에도 포함
        response.addHeader("X-Trace-Id", traceId);

        long start = System.currentTimeMillis();
        try {
            chain.doFilter(request, response);
        } finally {
            long duration = System.currentTimeMillis() - start;
            int status = response.getStatus();

            String msg = "[{}] {} {} → {} ({}ms) ip={}";

            if (status >= 500) {
                log.error(msg, traceId, method, uri, status, duration, ip);
            } else if (status >= 400) {
                log.warn(msg, traceId, method, uri, status, duration, ip);
            } else {
                log.info(msg, traceId, method, uri, status, duration, ip);
            }

            MDC.clear();
        }
    }

    private String resolveClientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
