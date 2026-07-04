package com.telemetry.security;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
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
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtTokenProvider jwtTokenProvider;

    @Value("${admin.username}")
    private String adminUsername;

    @Value("${admin.password}")
    private String adminPassword;

    @Value("${cors.allowed-origin-patterns}")
    private String allowedOriginPatterns;

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
                .requestMatchers("/api/auth/**").permitAll()
                .requestMatchers("/swagger-ui/**", "/swagger-ui.html", "/v3/api-docs/**").permitAll()
                // /actuator/prometheus는 Prometheus 스크레이핑용 — 별도 인증 없이 접근해야 정상 수집됨.
                // 운영 배포 시엔 애플리케이션 레벨 인증 대신 보안그룹/리버스프록시로 내부망만 접근 허용해야 한다.
                .requestMatchers("/actuator/health", "/actuator/prometheus").permitAll()
                .anyRequest().authenticated()
            )

            // ── JWT 필터 ────────────────────────────────────────────
            .addFilterBefore(
                new JwtAuthenticationFilter(jwtTokenProvider, userDetailsService()),
                UsernamePasswordAuthenticationFilter.class
            );

        return http.build();
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
    public UserDetailsService userDetailsService() {
        // Phase 4: 운영 배포 전 DB 기반 사용자 관리로 교체 필요
        var admin = User.builder()
            .username(adminUsername)
            .password(passwordEncoder().encode(adminPassword))
            .roles("ADMIN")
            .build();
        return new InMemoryUserDetailsManager(admin);
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
