package com.telemetry.controller;

import com.telemetry.exception.ErrorResponse;
import com.telemetry.exception.GlobalExceptionHandler;
import com.telemetry.security.BruteForceDetector;
import com.telemetry.security.ClientIpResolver;
import com.telemetry.security.JwtTokenProvider;
import com.telemetry.security.LoginRateLimiter;
import com.telemetry.security.RefreshTokenService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * <b>"비밀번호가 틀렸다"와 "Redis가 없다"가 구분되는지</b> 고정한다.
 *
 * <p>둘 다 로그인이 안 되는 상황이지만 <b>클라이언트가 해야 할 일이 다르다</b> —
 * 401은 다시 입력하라는 뜻이고 503은 잠시 후 재시도하라는 뜻이다.
 * 섞이면 사용자가 멀쩡한 비밀번호를 의심하거나, 반대로 틀린 비밀번호를 계속 재시도한다.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("로그인 실패 구분 — 인증 오류 vs Redis 장애")
class LoginFailureDistinctionTest {

    @Mock
    private AuthenticationManager authenticationManager;
    @Mock
    private JwtTokenProvider jwtTokenProvider;
    @Mock
    private RefreshTokenService refreshTokenService;
    @Mock
    private LoginRateLimiter loginRateLimiter;
    @Mock
    private BruteForceDetector bruteForceDetector;
    @Mock
    private ClientIpResolver clientIpResolver;

    private AuthController controller;
    private GlobalExceptionHandler handler;

    @BeforeEach
    void setUp() {
        // @RequiredArgsConstructor는 **필드 선언 순서**로 생성자를 만든다.
        controller = new AuthController(authenticationManager, jwtTokenProvider,
            bruteForceDetector, refreshTokenService, clientIpResolver, loginRateLimiter);
        handler = new GlobalExceptionHandler(new SimpleMeterRegistry());
        when(clientIpResolver.resolve(any())).thenReturn("10.0.0.1");
    }

    private static com.telemetry.dto.request.LoginRequest login(String user, String password) {
        com.telemetry.dto.request.LoginRequest r = new com.telemetry.dto.request.LoginRequest();
        r.setUsername(user);
        r.setPassword(password);
        return r;
    }

    @Test
    @DisplayName("비밀번호가 틀리면 401 — Redis는 멀쩡하다")
    void 틀린_비밀번호는_401() {
        when(loginRateLimiter.tryAcquire(anyString(), anyString())).thenReturn(true);
        when(bruteForceDetector.isBlocked(anyString())).thenReturn(false);
        when(authenticationManager.authenticate(any()))
            .thenThrow(new BadCredentialsException("Bad credentials"));

        assertThatThrownBy(() -> controller.login(login("someone", "wrong"), new MockHttpServletRequest()))
            .isInstanceOf(BadCredentialsException.class);

        // 실제 응답 코드는 GlobalExceptionHandler가 만든다.
        ResponseEntity<ErrorResponse> response =
            handler.handleBadCredentials(new BadCredentialsException("Bad credentials"));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getBody().getCode()).isEqualTo("UNAUTHORIZED");

        // 실패가 기록돼야 brute force 방어가 동작한다.
        verify(bruteForceDetector).recordFailure("10.0.0.1");
    }

    @Test
    @DisplayName("Redis가 없으면 503 — **자격증명은 검사조차 하지 않는다**")
    void redis_장애는_503_이고_인증은_시도도_안_한다() {
        when(loginRateLimiter.tryAcquire(anyString(), anyString()))
            .thenThrow(new RedisConnectionFailureException("Unable to connect"));

        assertThatThrownBy(() -> controller.login(login("someone", "correct"), new MockHttpServletRequest()))
            .isInstanceOf(DataAccessException.class);

        verify(authenticationManager, never()).authenticate(any());

        ResponseEntity<ErrorResponse> response = handler.handleRedisUnavailable(
            new RedisConnectionFailureException("down"), new MockHttpServletRequest());
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().getCode()).isEqualTo("REDIS_UNAVAILABLE");
    }

    @Test
    @DisplayName("비밀번호가 맞아도 Redis가 없으면 같은 503이다 — 정답 여부가 새지 않는다")
    void 맞는_비밀번호도_같은_503() {
        when(loginRateLimiter.tryAcquire(anyString(), anyString()))
            .thenThrow(new RedisConnectionFailureException("Unable to connect"));

        assertThatThrownBy(() -> controller.login(login("someone", "correct"), new MockHttpServletRequest()))
            .isInstanceOf(DataAccessException.class);
        assertThatThrownBy(() -> controller.login(login("someone", "wrong"), new MockHttpServletRequest()))
            .isInstanceOf(DataAccessException.class);
        // 두 경우가 구분 불가능해야 한다. 구분되면 Redis를 죽인 채로 계정을 열거할 수 있다.
    }

    /**
     * <b>경계 하나를 여기서 고정한다.</b>
     *
     * <p>{@code recordFailure}는 인증이 <b>실패한 뒤</b>에 불린다. Redis가 그 사이에 죽으면
     * <b>틀린 비밀번호가 401이 아니라 503</b>으로 나간다.
     *
     * <p><b>이게 맞는 동작이다.</b> 여기서 401을 돌려주려면 실패 기록을 포기해야 하고,
     * 그러면 <b>Redis를 죽이는 것이 곧 brute force 카운터를 끄는 방법</b>이 된다 —
     * 이번 결정(§3-2 ④ fail-closed)이 막으려던 바로 그것이다.
     *
     * <p>정보도 새지 않는다. 성공 경로의 {@code recordSuccess}도 같은 이유로 503이 되므로
     * <b>맞았을 때와 틀렸을 때가 구분되지 않는다.</b>
     */
    @Test
    @DisplayName("인증 직후 Redis가 죽으면 401이 아니라 503이다 — 기록을 포기하지 않는다")
    void 기록_실패는_503으로_나간다() {
        when(loginRateLimiter.tryAcquire(anyString(), anyString())).thenReturn(true);
        when(bruteForceDetector.isBlocked(anyString())).thenReturn(false);
        when(authenticationManager.authenticate(any()))
            .thenThrow(new BadCredentialsException("Bad credentials"));
        // 인증 실패는 났는데 그 실패를 기록하지 못하는 상황.
        org.mockito.Mockito.doThrow(new RedisConnectionFailureException("down"))
            .when(bruteForceDetector).recordFailure(anyString());

        assertThatThrownBy(() -> controller.login(login("someone", "wrong"), new MockHttpServletRequest()))
            .as("BadCredentials(401)가 아니라 Redis 예외(503)가 나가야 한다 — "
                + "실패를 못 세는데 401로 답하면 무제한 시도가 된다")
            .isInstanceOf(DataAccessException.class);
    }
}
