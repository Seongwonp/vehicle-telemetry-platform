package com.telemetry.exception;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.servlet.HandlerMapping;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis가 없을 때 <b>무엇으로 응답하고 무엇을 남기는지</b> 고정한다.
 *
 * <p>2026-09-12 실측에서 Redis를 내리면 인증 API·로그인·refresh가 전부
 * <b>정체불명의 500</b>이었다({@code docs/redis-failure-policy.md} 2절).
 * 500은 "우리 코드가 깨졌다"로 읽히는데 실제로는 <b>의존 서비스가 없는 상태</b>다.
 *
 * <p><b>이 테스트가 고정하지 않는 것</b>: 무엇을 허용하고 무엇을 막을지.
 * 여기서는 <b>거부의 표현</b>만 본다. 어느 경로가 거부되고 어느 경로가 통과하는지는
 * {@code RateLimitFailOpenTest}와 {@code LoginProtectionFailClosedTest}가 고정한다.
 */
@DisplayName("Redis 사용 불가 응답")
class RedisUnavailableHandlerTest {

    private SimpleMeterRegistry registry;
    private GlobalExceptionHandler handler;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        handler = new GlobalExceptionHandler(registry);
    }

    private static HttpServletRequest requestOn(String routeTemplate) {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/vehicles/KR-GA-1234/anomalies");
        if (routeTemplate != null) {
            request.setAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE, routeTemplate);
        }
        return request;
    }

    private double count(String route) {
        var c = registry.find(GlobalExceptionHandler.REDIS_UNAVAILABLE_METRIC)
            .tags("route", route).counter();
        return c == null ? 0.0 : c.count();
    }

    @Test
    @DisplayName("연결 실패는 500이 아니라 503이다")
    void 연결실패는_503() {
        ResponseEntity<ErrorResponse> response = handler.handleRedisUnavailable(
            new RedisConnectionFailureException("Unable to connect to Redis"),
            requestOn("/api/vehicles/{vehicleId}/anomalies"));

        assertThat(response.getStatusCode())
            .as("의존 서비스가 없는 상태다 — 우리 코드의 버그(500)가 아니라 재시도 가능(503)")
            .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody().getCode()).isEqualTo("REDIS_UNAVAILABLE");
    }

    @Test
    @DisplayName("명령 timeout도 같은 경로로 처리된다")
    void timeout도_503() {
        // timeout을 2초로 묶었으므로 이제 60초 매달림 대신 이 예외가 온다.
        ResponseEntity<ErrorResponse> response = handler.handleRedisUnavailable(
            new QueryTimeoutException("Redis command timed out"),
            requestOn("/api/auth/login"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(count("/api/auth/login")).isEqualTo(1.0);
    }

    @Test
    @DisplayName("지표 라벨은 **경로 템플릿**이다 — 실제 URI가 아니다")
    void 라벨은_경로_템플릿() {
        handler.handleRedisUnavailable(
            new DataAccessResourceFailureException("down"),
            requestOn("/api/vehicles/{vehicleId}/anomalies"));

        assertThat(count("/api/vehicles/{vehicleId}/anomalies"))
            .as("템플릿으로 세야 카디널리티가 유한하다").isEqualTo(1.0);
        assertThat(count("/api/vehicles/KR-GA-1234/anomalies"))
            .as("실제 URI로 세면 차량 수만큼 시계열이 늘고, Prometheus 라벨은 "
                + "보존 기간 내내 남아 개인정보가 샌다")
            .isEqualTo(0.0);
    }

    @Test
    @DisplayName("경로를 못 알아내도 죽지 않는다")
    void 경로_없어도_동작() {
        ResponseEntity<ErrorResponse> response = handler.handleRedisUnavailable(
            new DataAccessResourceFailureException("down"), requestOn(null));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(count("(unknown)")).isEqualTo(1.0);
    }

    @Test
    @DisplayName("MeterRegistry가 없어도 예외 처리는 살아 있다")
    void registry_없어도_응답한다() {
        // @WebMvcTest 슬라이스에는 Micrometer 자동 구성이 없다. 여기서 생성자가 터지면
        // 컨트롤러 테스트의 컨텍스트가 통째로 못 뜬다 — 실제로 한 번 깨뜨렸다.
        GlobalExceptionHandler noRegistry =
            new GlobalExceptionHandler(new org.springframework.beans.factory.ObjectProvider<
                io.micrometer.core.instrument.MeterRegistry>() {
                @Override
                public io.micrometer.core.instrument.MeterRegistry getObject() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public io.micrometer.core.instrument.MeterRegistry getObject(Object... args) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public io.micrometer.core.instrument.MeterRegistry getIfAvailable() {
                    return null;
                }

                @Override
                public io.micrometer.core.instrument.MeterRegistry getIfUnique() {
                    return null;
                }
            });

        ResponseEntity<ErrorResponse> response = noRegistry.handleRedisUnavailable(
            new DataAccessResourceFailureException("down"), requestOn("/api/vehicles"));

        assertThat(response.getStatusCode())
            .as("지표를 못 올리는 것과 응답을 못 하는 것은 다른 문제다")
            .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    @DisplayName("응답 본문에 접속 정보나 예외 메시지를 싣지 않는다")
    void 본문에_내부정보_없음() {
        ResponseEntity<ErrorResponse> response = handler.handleRedisUnavailable(
            new RedisConnectionFailureException(
                "Unable to connect to redis://:hunter2@redis-prod-01:6379"),
            requestOn("/api/vehicles"));

        String body = response.getBody().getCode() + " " + response.getBody().getMessage();
        assertThat(body)
            .as("예외 메시지를 그대로 내보내면 호스트·자격증명이 샐 수 있다")
            .doesNotContain("redis://").doesNotContain("hunter2").doesNotContain("6379");
    }
}
