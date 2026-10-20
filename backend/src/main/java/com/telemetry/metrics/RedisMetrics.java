package com.telemetry.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.dao.DataAccessException;
import org.springframework.web.servlet.HandlerMapping;

/**
 * Redis 장애 영향을 보이게 하는 지표(docs/redis-failure-policy.md).
 * fail-open 조회는 Redis가 죽어도 조용히 통과하므로 지표가 없으면 아무 증상이 없다.
 *
 * <p>라벨은 경로 템플릿만 쓴다. 실제 URI를 쓰면 차량 ID가 라벨로 남는다(카디널리티·보존 정책).
 */
public final class RedisMetrics {

    /** fail-closed 경로에서 거부한 요청. */
    public static final String UNAVAILABLE = "telemetry.redis.unavailable";

    /** fail-open 경로에서 rate limit 없이 통과한 요청 수. */
    public static final String RATE_LIMIT_FAIL_OPEN = "telemetry.ratelimit.failopen";

    /**
     * 경로 합계, 기동 시 0으로 등록하며 알림은 이것을 본다. 경로별 카운터는 첫 실패에서야 생겨
     * {@code increase()}가 첫 증가를 놓친다. 경위: docs/redis-failure-policy.md §10
     */
    public static final String UNAVAILABLE_ALL = "telemetry.redis.unavailable.all";
    public static final String RATE_LIMIT_FAIL_OPEN_ALL = "telemetry.ratelimit.failopen.all";

    public static final String TAG_ROUTE = "route";

    /** 패턴이 없을 때도 실제 URI로 대체하지 않는다. */
    public static final String UNKNOWN_ROUTE = "(unknown)";

    private RedisMetrics() {
    }

    /** 인터셉터는 핸들러 매핑 뒤에 돌므로 preHandle에서도 이 속성이 채워져 있다. */
    public static String routeTemplate(HttpServletRequest request) {
        Object pattern = request == null ? null
            : request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        return pattern == null ? UNKNOWN_ROUTE : pattern.toString();
    }

    /** 요청이 없으면 오르지 않는다 — Redis 가용성이 아니라 요청이 받은 영향을 센다. */
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
     * fail-open은 저장소 장애({@link DataAccessException})에만 적용한다.
     * NPE 같은 버그까지 삼키면 "장애 중 정상 통과"로 위장된다.
     */
    public static boolean isStoreUnavailable(Throwable t) {
        return t instanceof DataAccessException;
    }
}
