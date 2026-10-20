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
    // DB 기반(DbUserDetailsService). ADR-027
    private final UserDetailsService userDetailsService;

    @Value("${admin.password}")
    private String adminPassword;

    @Value("${cors.allowed-origin-patterns}")
    private String allowedOriginPatterns;

    // 기본 비밀번호 그대로 뜨면 관리자 계정이 공개 비밀번호로 열린다. 기동 시 fail-fast로 막는다.
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
            // Flutter web(브라우저) 호출용. 네이티브 앱/curl은 CORS 검사를 받지 않는다.
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))

            // ── CSRF 비활성화 (Stateless JWT 방식) ─────────────────
            .csrf(AbstractHttpConfigurer::disable)

            // ── Stateless 세션 ──────────────────────────────────────
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .exceptionHandling(errors -> errors
                .authenticationEntryPoint((request, response, exception) ->
                    writeJson(response, 401, "UNAUTHORIZED", "인증이 필요합니다"))
                // 403도 직접 쓴다. 기본 핸들러의 sendError(403)는 /error ERROR 디스패치를 타는데, 거기선
                // JWT 필터가 돌지 않아 실제 컨테이너에서 401로 바뀐다(MockMvc에선 재현 안 됨).
                // 경위: docs/verification/2026-09-28-multi-user-e2e.md
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
                .requestMatchers("/api/auth/me", "/api/auth/password").authenticated()
                .requestMatchers("/api/auth/**").permitAll()
                .requestMatchers(org.springframework.http.HttpMethod.POST, "/api/vehicles").hasRole("ADMIN")
                // 비활성화도 관리자만. 소유자가 하면 되돌릴 수 없는 삭제가 된다(재등록 409). ADR-027
                .requestMatchers(org.springframework.http.HttpMethod.DELETE, "/api/vehicles/*").hasRole("ADMIN")
                // 핸드셰이크는 열어두고 인증은 STOMP CONNECT에서 인터셉터가 검사한다.
                .requestMatchers("/ws/**").permitAll()
                .requestMatchers("/swagger-ui/**", "/swagger-ui.html", "/v3/api-docs/**").permitAll()
                // probe·스크레이핑용이라 인증 없이 연다(show-details: never).
                // 운영에서는 보안그룹/리버스 프록시로 내부망에서만 접근하게 해야 한다.
                .requestMatchers("/actuator/health", "/actuator/health/**", "/actuator/prometheus").permitAll()
                // 컨트롤러 @PreAuthorize와 이중 검사. 한쪽이 빠져도 열리지 않게 둘 다 둔다.
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
        // 콤마 구분 패턴. "localhost:*" 같은 포트 와일드카드는 Patterns 쪽만 지원한다.
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
