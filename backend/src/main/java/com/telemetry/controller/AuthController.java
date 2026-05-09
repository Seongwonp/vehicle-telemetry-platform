package com.telemetry.controller;

import com.telemetry.dto.request.LoginRequest;
import com.telemetry.dto.response.LoginResponse;
import com.telemetry.security.BruteForceDetector;
import com.telemetry.security.JwtTokenProvider;
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

    @PostMapping("/login")
    @Operation(summary = "로그인", description = "username/password로 JWT 토큰 발급. 5회 실패 시 15분 IP 차단.")
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
            String token = jwtTokenProvider.generateToken(auth.getName());
            return ResponseEntity.ok(new LoginResponse(token, jwtTokenProvider.getExpirationMs()));

        } catch (BadCredentialsException e) {
            bruteForceDetector.recordFailure(ip);
            throw e;
        }
    }

    private String resolveIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) return forwarded.split(",")[0].trim();
        return request.getRemoteAddr();
    }
}
