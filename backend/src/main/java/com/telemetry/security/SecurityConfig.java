package com.telemetry.security;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtTokenProvider jwtTokenProvider;
    // DB 기반(DbUserDetailsService). 2026-09-27까지는 InMemory admin 한 명이었다(ADR-006 → ADR-027).
    private final UserDetailsService userDetailsService;

    @Value("${admin.password}")
    private String adminPassword;

    @Value("${cors.allowed-origin-patterns}")
    private String allowedOriginPatterns;

    // application.yml의 기본값(ADMIN_PASSWORD 미설정 시 "changeme")이 그대로
    // 배포되면 관리자 계정이 공개 기본 비밀번호로 열려버린다. .env 없이 실수로
    // 띄우는 걸 배포 전에 바로 드러내기 위한 fail-fast 검증이다.
    private static final String INSECURE_DEFAULT_PASSWORD = "changeme";

    @PostConstruct
    void validateAdminPassword() {
        if (adminPassword == null || adminPassword.isBlank()
            || adminPassword.equals(INSECURE_DEFAULT_PASSWORD)) {
            throw new IllegalStateException(
                "ADMIN_PASSWORD가 설정되지 않았거나 기본값(\"" + INSECURE_DEFAULT_PASSWORD + "\")입니다. "
                    + ".env에 실제 비밀번호를 설정하세요.");
        }
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            // ── CORS ────────────────────────────────────────────────
            // Flutter web(브라우저)에서 호출할 때만 필요 — 네이티브 앱/curl은 브라우저가
            // 아니라 CORS 검사 자체를 안 받아서 이 설정 없이도 지금까지는 문제가 안 보였다.
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))

            // ── CSRF 비활성화 (Stateless JWT 방식) ─────────────────
            .csrf(AbstractHttpConfigurer::disable)

            // ── Stateless 세션 ──────────────────────────────────────
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .exceptionHandling(errors -> errors
                .authenticationEntryPoint((request, response, exception) ->
                    writeJson(response, 401, "UNAUTHORIZED", "인증이 필요합니다"))
                // 403도 직접 쓴다. 기본 AccessDeniedHandlerImpl은 sendError(403)를 부르고, 그러면 Tomcat이
                // /error로 ERROR 디스패치를 한다 — 그 디스패치에는 JWT 필터가 돌지 않아(OncePerRequestFilter는
                // ERROR 디스패치를 건너뛴다) /error가 anyRequest().authenticated()에 걸려 위 entry point의
                // **401로 바뀌어 나갔다.** MockMvc는 ERROR 디스패치를 하지 않아 테스트는 403을 봤고, 실제
                // 컨테이너 E2E(2026-09-28)에서만 401이 나왔다.
                .accessDeniedHandler((request, response, exception) ->
                    writeJson(response, 403, "FORBIDDEN", "접근 권한이 없습니다")))

            // ── 보안 헤더 ───────────────────────────────────────────
            .headers(headers -> headers
                .frameOptions(frame -> frame.deny())                       // Clickjacking 방지
                .contentTypeOptions(ct -> {})                              // MIME sniffing 방지
                .httpStrictTransportSecurity(hsts -> hsts                  // HTTPS 강제 (운영)
                    .maxAgeInSeconds(31536000)
                    .includeSubDomains(true)
                )
                .referrerPolicy(referrer -> referrer
                    .policy(ReferrerPolicyHeaderWriter.ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN)
                )
            )

            // ── 엔드포인트 인가 ─────────────────────────────────────
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/auth/me").authenticated()
                .requestMatchers("/api/auth/**").permitAll()
                .requestMatchers(org.springframework.http.HttpMethod.POST, "/api/vehicles").hasRole("ADMIN")
                // 비활성화도 관리자만. 등록만 관리자고 비활성화는 소유자도 되던 비대칭이었다 — 비활성 차량은 관리자도
                // 접근할 수 없고 같은 ID 재등록은 409라, 일반 사용자가 되돌릴 수 없는 삭제를 할 수 있었다(2026-09-29 리뷰).
                .requestMatchers(org.springframework.http.HttpMethod.DELETE, "/api/vehicles/*").hasRole("ADMIN")
                // WebSocket 핸드셰이크(HTTP 업그레이드) 자체는 열어두고, 실제 인증은
                // STOMP CONNECT 프레임에서 WebSocketConfig의 인터셉터가 검사한다.
                .requestMatchers("/ws/**").permitAll()
                .requestMatchers("/swagger-ui/**", "/swagger-ui.html", "/v3/api-docs/**").permitAll()
                // /actuator/prometheus는 Prometheus 스크레이핑용 — 별도 인증 없이 접근해야 정상 수집됨.
                // 운영 배포 시엔 애플리케이션 레벨 인증 대신 보안그룹/리버스프록시로 내부망만 접근 허용해야 한다.
                // /actuator/health/** 는 liveness·readiness 그룹까지 포함한다.
                // show-details: never라서 세 경로 모두 status 한 줄만 나간다 —
                // 오케스트레이터가 probe할 수 있어야 하므로 인증을 요구하지 않는다.
                .requestMatchers("/actuator/health", "/actuator/health/**", "/actuator/prometheus").permitAll()
                // 사용자 관리는 관리자만. 컨트롤러의 @PreAuthorize와 이중이다 — 한쪽이 빠져도 열리지 않게.
                .requestMatchers("/api/users/**").hasRole("ADMIN")
                .anyRequest().authenticated()
            )

            // ── JWT 필터 ────────────────────────────────────────────
            .addFilterBefore(
                new JwtAuthenticationFilter(jwtTokenProvider, userDetailsService),
                UsernamePasswordAuthenticationFilter.class
            );

        return http.build();
    }

    private static void writeJson(jakarta.servlet.http.HttpServletResponse response, int status,
                                  String code, String message) throws java.io.IOException {
        response.setStatus(status);
        response.setContentType("application/json;charset=UTF-8");
        response.getWriter().write("{\"code\":\"" + code + "\",\"message\":\"" + message + "\"}");
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        // 콤마로 구분된 패턴 목록. 포트가 매번 바뀌는 로컬 개발 편의를 위해
        // setAllowedOrigins가 아닌 setAllowedOriginPatterns를 쓴다 — 후자만 "localhost:*"
        // 같은 포트 와일드카드를 지원한다.
        configuration.setAllowedOriginPatterns(Arrays.asList(allowedOriginPatterns.split(",")));
        configuration.setAllowedMethods(List.of("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"));
        configuration.setAllowedHeaders(List.of("*"));
        configuration.setAllowCredentials(true);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration config) throws Exception {
        return config.getAuthenticationManager();
    }
}
