package com.telemetry.metrics;

import com.telemetry.config.RateLimitInterceptor;
import com.telemetry.exception.GlobalExceptionHandler;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;

/**
 * Redis 장애 알림이 <b>첫 실패부터 보이게</b> 하는 계약을 고정한다.
 *
 * <p>2026-09-13 측정에서 경로별 카운터는 첫 실패 요청에서야 시계열이 생겨, Prometheus의 첫 샘플이
 * 이미 1이었고 {@code increase()}가 그 증가를 보지 못했다. 합계 카운터를 <b>기동 시 0</b>으로 두어
 * 그 구멍을 닫는다. 여기서는 (1) 0으로 등록되는지 (2) 증가가 합계에도 반영되는지
 * (3) 알림 규칙이 실제로 그 합계를 보는지를 묶는다 — 셋 중 하나만 어긋나도 구멍이 다시 열린다.
 */
@DisplayName("Redis 장애 알림용 합계 카운터")
class RedisAlertingMetricsTest {

    private static double count(SimpleMeterRegistry r, String name) {
        Counter c = r.find(name).counter();
        return c == null ? Double.NaN : c.count();
    }

    @Test
    @DisplayName("기동 시 0으로 등록된다 — 첫 scrape에 0이 있어야 첫 실패가 increase()에 보인다")
    void 기동_시_0() {
        SimpleMeterRegistry r = new SimpleMeterRegistry();
        new GlobalExceptionHandler(r);

        assertThat(count(r, RedisMetrics.UNAVAILABLE_ALL)).isZero();
        assertThat(count(r, RedisMetrics.RATE_LIMIT_FAIL_OPEN_ALL)).isZero();
    }

    @Test
    @DisplayName("fail-open 인터셉터도 기동 시 등록한다")
    void 인터셉터도_등록() {
        SimpleMeterRegistry r = new SimpleMeterRegistry();
        RateLimitInterceptor interceptor = new RateLimitInterceptor(
            mock(org.springframework.data.redis.core.StringRedisTemplate.class),
            mock(com.telemetry.security.ClientIpResolver.class), r);
        ReflectionTestUtils.invokeMethod(interceptor, "preRegisterMetrics");

        assertThat(count(r, RedisMetrics.RATE_LIMIT_FAIL_OPEN_ALL)).isZero();
    }

    @Test
    @DisplayName("경로별 증가가 합계에도 반영된다")
    void 합계에_반영() {
        SimpleMeterRegistry r = new SimpleMeterRegistry();
        RedisMetrics.preRegister(r);
        RedisMetrics.unavailable(r, "/api/auth/login");
        RedisMetrics.unavailable(r, "/api/vehicles/{vehicleId}/diagnosis");
        RedisMetrics.rateLimitFailOpen(r, "/api/vehicles");

        assertThat(count(r, RedisMetrics.UNAVAILABLE_ALL)).isEqualTo(2.0);
        assertThat(count(r, RedisMetrics.RATE_LIMIT_FAIL_OPEN_ALL)).isEqualTo(1.0);
        assertThat(r.find(RedisMetrics.UNAVAILABLE).tag("route", "/api/auth/login").counter().count())
            .as("경로별 카운터는 그대로 남는다 — '어디서'를 보는 용도").isEqualTo(1.0);
    }

    @Test
    @DisplayName("합계 카운터에는 라벨이 없다 — 알림용 시계열은 인스턴스당 하나")
    void 합계는_라벨_없음() {
        SimpleMeterRegistry r = new SimpleMeterRegistry();
        RedisMetrics.preRegister(r);
        RedisMetrics.unavailable(r, "/api/auth/login");

        assertThat(r.find(RedisMetrics.UNAVAILABLE_ALL).counters()).hasSize(1);
        assertThat(r.find(RedisMetrics.UNAVAILABLE_ALL).counter().getId().getTags()).isEmpty();
        assertThat(r.find(RedisMetrics.RATE_LIMIT_FAIL_OPEN_ALL).counter().getId().getTags()).isEmpty();
    }

    /**
     * 알림 규칙이 <b>사전 등록된 합계</b>를 보는지 확인한다. 규칙이 경로별 카운터를 다시 보면
     * 코드를 고쳐도 첫 증가가 안 보이는 구멍이 그대로다.
     *
     * <p>Docker 빌드처럼 저장소 루트가 없는 곳에서는 건너뛴다. 저장소 루트가 있는데 규칙 파일이
     * 없으면 <b>실패</b>한다 — 파일 삭제를 skip으로 조용히 넘기지 않는다.
     */
    @Test
    @DisplayName("알림 규칙이 사전 등록된 합계 카운터를 본다")
    void 알림_규칙이_합계를_본다() throws IOException {
        Path repoRoot = Path.of("..").toAbsolutePath().normalize();
        assumeThat(Files.exists(repoRoot.resolve("docker-compose.yml")))
            .as("저장소 루트가 없는 빌드 컨텍스트 — 규칙 파일 대조를 건너뛴다").isTrue();
        Path rules = repoRoot.resolve("monitoring/prometheus/alerts.yml");
        assertThat(rules).as("저장소 루트는 있는데 알림 규칙 파일이 없다").exists();

        String yaml = Files.readString(rules, StandardCharsets.UTF_8);
        assertThat(exprOf(yaml, "RedisUnavailableRejections")).contains("telemetry_redis_unavailable_all_total");
        assertThat(exprOf(yaml, "RateLimitFailingOpen")).contains("telemetry_ratelimit_failopen_all_total");
    }

    private static String exprOf(String yaml, String alert) {
        // `- alert:`과 `expr:` 사이에 설명 주석이 올 수 있다 — 그 알림 뒤의 **첫** expr을 잡는다.
        Matcher m = Pattern.compile("- alert: " + alert + "\\b.*?\\n\\s*expr:\\s*([^\\n]+)", Pattern.DOTALL)
            .matcher(yaml);
        assertThat(m.find()).as("알림 %s를 규칙 파일에서 못 찾았다", alert).isTrue();
        return m.group(1);
    }
}
