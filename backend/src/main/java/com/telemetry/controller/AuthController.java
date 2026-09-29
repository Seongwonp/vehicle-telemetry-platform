package com.telemetry.controller;

import com.telemetry.dto.request.LoginRequest;
import com.telemetry.dto.request.PasswordChangeRequest;
import com.telemetry.dto.request.RefreshRequest;
import com.telemetry.dto.response.LoginResponse;
import com.telemetry.exception.ErrorResponse;
import com.telemetry.security.BruteForceDetector;
import com.telemetry.security.ClientIpResolver;
import com.telemetry.security.JwtTokenProvider;
import com.telemetry.security.LoginRateLimiter;
import com.telemetry.security.RefreshTokenService;
import com.telemetry.service.UserService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
@Tag(name = "Auth", description = "JWT 인증 API")
public class AuthController {

    private final AuthenticationManager authenticationManager;
    private final JwtTokenProvider jwtTokenProvider;
    private final BruteForceDetector bruteForceDetector;
    private final RefreshTokenService refreshTokenService;
    private final ClientIpResolver clientIpResolver;
    private final LoginRateLimiter loginRateLimiter;
    // refresh는 DB를 다시 본다 — 토큰이 살아 있어도 계정이 비활성이면 재발급하지 않는다(ADR-027).
    private final UserDetailsService userDetailsService;
    private final UserService userService;

    @org.springframework.web.bind.annotation.GetMapping("/me")
    @org.springframework.security.access.prepost.PreAuthorize("isAuthenticated()")
    @Operation(summary = "현재 사용자와 권한 조회")
    public java.util.Map<String, Object> me(Authentication authentication) {
        return java.util.Map.of("username", authentication.getName(), "roles",
            authentication.getAuthorities().stream()
                .map(org.springframework.security.core.GrantedAuthority::getAuthority).toList());
    }

    @PostMapping("/login")
    @Operation(summary = "로그인", description = "username/password로 JWT Access/Refresh 토큰 발급. 5회 실패 시 15분 IP 차단.")
    public ResponseEntity<?> login(
        @Valid @RequestBody LoginRequest request,
        HttpServletRequest httpRequest
    ) {
        String ip = clientIpResolver.resolve(httpRequest);

        if (!loginRateLimiter.tryAcquire(ip, request.getUsername())) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body(new ErrorResponse("TOO_MANY_REQUESTS", "로그인 요청이 너무 많습니다"));
        }

        if (bruteForceDetector.isBlocked(ip)) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .body("로그인 시도 초과. 잠시 후 다시 시도하세요.");
        }

        try {
            Authentication auth = authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(request.getUsername(), request.getPassword())
            );
            bruteForceDetector.recordSuccess(ip);
            String accessToken = jwtTokenProvider.generateToken(auth.getName());
            String refreshToken = refreshTokenService.issue(auth.getName());
            return ResponseEntity.ok(
                new LoginResponse(accessToken, refreshToken, jwtTokenProvider.getExpirationMs())
            );

        } catch (BadCredentialsException e) {
            bruteForceDetector.recordFailure(ip);
            throw e;
        }
    }

    @PostMapping("/refresh")
    @Operation(
        summary = "토큰 재발급",
        description = "Refresh Token으로 새 Access/Refresh Token을 발급한다. 기존 Refresh Token은 즉시 폐기된다(rotation)."
    )
    public ResponseEntity<?> refresh(@Valid @RequestBody RefreshRequest request) {
        return refreshTokenService.rotate(request.getRefreshToken())
            .filter(this::canStillLogin)
            .<ResponseEntity<?>>map(username -> {
                String accessToken = jwtTokenProvider.generateToken(username);
                String newRefreshToken = refreshTokenService.issue(username);
                return ResponseEntity.ok(
                    new LoginResponse(accessToken, newRefreshToken, jwtTokenProvider.getExpirationMs())
                );
            })
            .orElseGet(() -> ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(new ErrorResponse("UNAUTHORIZED", "유효하지 않거나 만료된 리프레시 토큰입니다")));
    }

    private boolean canStillLogin(String username) {
        try {
            userDetailsService.loadUserByUsername(username);
            return true;
        } catch (UsernameNotFoundException e) {
            // rotate()가 이미 옛 토큰을 지웠으므로 비활성 계정의 refresh 체인은 여기서 끝난다.
            return false;
        }
    }

    @PostMapping("/password")
    @PreAuthorize("isAuthenticated()")
    @SecurityRequirement(name = "bearerAuth")
    @Operation(
        summary = "비밀번호 변경 (본인)",
        description = "현재 비밀번호를 확인하고 새 비밀번호로 바꾼다. 성공하면 이 사용자의 refresh token이 모두 폐기되어 "
            + "다시 로그인해야 한다. 이미 발급된 Access Token은 자체 만료시간까지는 유효하다. "
            + "현재 비밀번호 불일치는 400 CURRENT_PASSWORD_INCORRECT(401이 아니다)."
    )
    public ResponseEntity<Void> changePassword(
        @Valid @RequestBody PasswordChangeRequest request, Authentication authentication
    ) {
        userService.changePassword(authentication.getName(), request.getCurrentPassword(), request.getNewPassword());
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/logout")
    @Operation(
        summary = "로그아웃",
        description = "Refresh Token을 무효화해 재발급을 차단한다. 이미 발급된 Access Token은 자체 만료시간까지는 유효하다."
    )
    public ResponseEntity<Void> logout(@Valid @RequestBody RefreshRequest request) {
        refreshTokenService.revoke(request.getRefreshToken());
        return ResponseEntity.noContent().build();
    }

}
