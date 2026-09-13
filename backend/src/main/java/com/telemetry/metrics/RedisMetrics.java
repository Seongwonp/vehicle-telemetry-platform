package com.telemetry.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.dao.DataAccessException;
import org.springframework.web.servlet.HandlerMapping;

/**
 * Redis 장애 상태를 <b>보이게</b> 만드는 지표 (P1-3).
 *
 * <h3>왜 필요한가</h3>
 *
 * 일반 조회 rate limit은 <b>fail-open</b>으로 정했다. 그 말은 Redis가 죽으면
 * <b>제한이 안 걸린 채로 요청이 통과한다</b>는 뜻이고, <b>그건 아무 증상 없이 조용하다.</b>
 * {@code docs/redis-failure-policy.md} §4에 미리 적어둔 원칙이 여기 적용된다 —
 * <b>조용한 성공이 제일 나쁘다.</b>
 *
 * <h3>{@code ContractMetrics}와 같은 이유로 정적 유틸이다</h3>
 *
 * 상태를 들지 않고 전달받은 registry에만 올린다. 빈으로 만들면 이미 {@code MeterRegistry}를
 * 받는 컴포넌트들의 생성자가 줄줄이 바뀐다 (P0-2b에서 한 번 되돌렸다).
 *
 * <h3>라벨 규칙</h3>
 *
 * 라벨은 <b>경로 템플릿</b>뿐이다({@code /api/vehicles/{vehicleId}/anomalies}).
 * 실제 URI를 쓰면 시계열이 차량 수만큼 늘고, <b>Prometheus 라벨은 보존 기간 내내 남아</b>
 * 차량 ID가 {@code docs/data-retention.md} 정책 밖에 남는다.
 */
public final class RedisMetrics {

    /** Redis에 못 닿아 요청을 <b>거부</b>했다(fail-closed 경로). */
    public static final String UNAVAILABLE = "telemetry.redis.unavailable";

    /**
     * Redis에 못 닿았는데 요청을 <b>통과</b>시켰다(fail-open 경로).
     *
     * <p>이 값이 0보다 크면 <b>그 시간 동안 남용 방어가 없었다</b>는 뜻이다.
     * "장애가 있었다"가 아니라 <b>"제한이 적용되지 않은 요청이 이만큼 있었다"</b>로 읽는다.
     */
    public static final String RATE_LIMIT_FAIL_OPEN = "telemetry.ratelimit.failopen";

    /**
     * 위 두 지표의 <b>경로 합계</b> — 라벨 없음, <b>기동 시 0으로 등록</b>된다. 알림은 이것을 본다.
     *
     * <h3>왜 따로 두나</h3>
     *
     * 경로별 카운터는 <b>첫 실패 요청에서 시계열이 생긴다.</b> Prometheus가 처음 긁는 값이 이미 1이라
     * {@code increase()}가 그 첫 증가를 보지 못하고, 다음 증가가 긁힐 때까지 알림이 한 scrape 주기 늦어진다.
     * 2026-09-13 90초 중단에서 첫 샘플(+12.9s)이 무시되고 다음 샘플(+27s)에서야 pending이 됐다
     * ({@code load-test/redis-outage/evidence/20260913-supplement-v1-tsdb}).
     *
     * <p>경로별 시계열을 기동 시 전부 0으로 만드는 방법도 있지만 경로 목록을 따라가야 하고, 새 경로가
     * 생기면 같은 구멍이 다시 열린다. 합계 하나를 0으로 두면 경로와 무관하게 닫힌다.
     * <b>"어디서"는 경로별 카운터, "지금 일어나는가"는 합계</b>로 역할을 나눈다.
     */
    public static final String UNAVAILABLE_ALL = "telemetry.redis.unavailable.all";
    public static final String RATE_LIMIT_FAIL_OPEN_ALL = "telemetry.ratelimit.failopen.all";

    public static final String TAG_ROUTE = "route";

    /** 핸들러 매핑이 패턴을 못 남긴 경우. 실제 URI로 대체하지 <b>않는다</b>. */
    public static final String UNKNOWN_ROUTE = "(unknown)";

    private RedisMetrics() {
    }

    /**
     * 경로 <b>템플릿</b>을 돌려준다. 인터셉터는 핸들러 매핑 <b>뒤에</b> 돌기 때문에
     * {@code preHandle}에서도 이 속성이 채워져 있다.
     */
    public static String routeTemplate(HttpServletRequest request) {
        Object pattern = request == null ? null
            : request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        return pattern == null ? UNKNOWN_ROUTE : pattern.toString();
    }

    /**
     * 알림용 합계 카운터를 0으로 만든다. 여러 번 불러도 같은 카운터다.
     *
     * <p><b>요청이 없으면 이 값은 오르지 않는다.</b> Redis가 죽어 있어도 그동안 Redis를 쓰는 요청이
     * 없으면 0에 머문다 — 이 지표는 "Redis가 내려갔다"가 아니라 "Redis 때문에 요청이 거부·무제한 통과됐다"를 센다.
     */
    public static void preRegister(MeterRegistry registry) {
        registry.counter(UNAVAILABLE_ALL);
        registry.counter(RATE_LIMIT_FAIL_OPEN_ALL);
    }

    public static void unavailable(MeterRegistry registry, String route) {
        registry.counter(UNAVAILABLE, TAG_ROUTE, route).increment();
        registry.counter(UNAVAILABLE_ALL).increment();
    }

    public static void rateLimitFailOpen(MeterRegistry registry, String route) {
        registry.counter(RATE_LIMIT_FAIL_OPEN, TAG_ROUTE, route).increment();
        registry.counter(RATE_LIMIT_FAIL_OPEN_ALL).increment();
    }

    /**
     * <b>저장소가 응답하지 않은 것</b>인지 가른다 — fail-open은 여기에만 적용한다.
     *
     * <p>Spring이 Redis 예외를 {@link DataAccessException} 계층으로 번역한다
     * (연결 실패는 {@code RedisConnectionFailureException}, 명령 timeout은
     * {@code QueryTimeoutException}).
     *
     * <p><b>다른 예외는 통과시키지 않는다.</b> 예를 들어 우리 코드의 NPE까지 fail-open으로
     * 삼키면 버그가 "장애 중 정상 통과"로 위장된다. 그건 조용한 성공이다.
     */
    public static boolean isStoreUnavailable(Throwable t) {
        return t instanceof DataAccessException;
    }
}
