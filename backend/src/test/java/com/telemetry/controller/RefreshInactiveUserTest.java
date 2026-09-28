package com.telemetry.controller;

import com.telemetry.dto.request.RefreshRequest;
import com.telemetry.security.BruteForceDetector;
import com.telemetry.security.ClientIpResolver;
import com.telemetry.security.JwtTokenProvider;
import com.telemetry.security.LoginRateLimiter;
import com.telemetry.security.RefreshTokenService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * refresh token이 Redis에 살아 있어도 <b>계정이 비활성이면 재발급하지 않는다</b>(ADR-027).
 * 2026-09-27까지는 DB를 보지 않아 "재발급은 되는데 API는 401"인 상태가 생길 수 있었다.
 */
@DisplayName("refresh — 비활성·삭제 계정")
class RefreshInactiveUserTest {

    private final JwtTokenProvider jwt = mock(JwtTokenProvider.class);
    private final RefreshTokenService refresh = mock(RefreshTokenService.class);
    private final UserDetailsService users = mock(UserDetailsService.class);
    private final AuthController controller = new AuthController(mock(AuthenticationManager.class), jwt,
        mock(BruteForceDetector.class), refresh, mock(ClientIpResolver.class), mock(LoginRateLimiter.class), users);

    private static RefreshRequest request() {
        RefreshRequest r = new RefreshRequest();
        r.setRefreshToken("rt");
        return r;
    }

    @Test
    @DisplayName("계정이 로그인 불가면 401이고 새 토큰을 발급하지 않는다")
    void 비활성계정_401() {
        given(refresh.rotate("rt")).willReturn(Optional.of("off"));
        given(users.loadUserByUsername("off")).willThrow(new UsernameNotFoundException("x"));

        ResponseEntity<?> response = controller.refresh(request());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        verify(jwt, never()).generateToken("off");
        verify(refresh, never()).issue("off");
    }

    @Test
    @DisplayName("정상 계정은 재발급된다")
    void 정상계정_재발급() {
        given(refresh.rotate("rt")).willReturn(Optional.of("hong"));
        given(users.loadUserByUsername("hong")).willReturn(User.withUsername("hong").password("x").roles("USER").build());
        given(jwt.generateToken("hong")).willReturn("access");
        given(refresh.issue("hong")).willReturn("rt2");

        ResponseEntity<?> response = controller.refresh(request());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
