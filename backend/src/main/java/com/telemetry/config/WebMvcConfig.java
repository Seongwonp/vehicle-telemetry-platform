package com.telemetry.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@RequiredArgsConstructor
public class WebMvcConfig implements WebMvcConfigurer {

    private final RateLimitInterceptor rateLimitInterceptor;
    private final DiagnosisRateLimitInterceptor diagnosisRateLimitInterceptor;
    private final VehicleAccessInterceptor vehicleAccessInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(rateLimitInterceptor)
            .addPathPatterns("/api/**")
            .excludePathPatterns("/api/auth/login"); // 로그인은 Rate Limit 제외
        registry.addInterceptor(diagnosisRateLimitInterceptor)
            .addPathPatterns("/api/vehicles/*/diagnosis");
        // 접근 검사는 rate limit 뒤에 둔다. 순서를 바꾸면 Redis 장애 시 경로별 동작(fail-open/closed)이 달라진다.
        registry.addInterceptor(vehicleAccessInterceptor)
            .addPathPatterns("/api/vehicles/*", "/api/vehicles/*/**");
    }
}
