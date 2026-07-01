package com.telemetry.controller;

import com.telemetry.dto.request.LoginRequest;
import com.telemetry.dto.request.RefreshRequest;
import com.telemetry.dto.response.LoginResponse;
import com.telemetry.exception.ErrorResponse;
import com.telemetry.security.BruteForceDetector;
import com.telemetry.security.JwtTokenProvider;
import com.telemetry.security.RefreshTokenService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
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

    @PostMapping("/login")
    @Operation(summary = "로그인", description = "username/password로 JWT Access/Refresh 토큰 발급. 5회 실패 시 15분 IP 차단.")
    public ResponseEntity<?> login(
        @Valid @RequestBody LoginRequest request,
        HttpServletRequest httpRequest
    ) {
        String ip = resolveIp(httpRequest);

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

    @PostMapping("/logout")
    @Operation(
        summary = "로그아웃",
        description = "Refresh Token을 무효화해 재발급을 차단한다. 이미 발급된 Access Token은 자체 만료시간까지는 유효하다."
    )
    public ResponseEntity<Void> logout(@Valid @RequestBody RefreshRequest request) {
        refreshTokenService.revoke(request.getRefreshToken());
        return ResponseEntity.noContent().build();
    }

    private String resolveIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) return forwarded.split(",")[0].trim();
        return request.getRemoteAddr();
    }
}
