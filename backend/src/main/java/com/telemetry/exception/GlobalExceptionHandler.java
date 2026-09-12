package com.telemetry.exception;

import com.telemetry.metrics.RedisMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /** Redis가 안 될 때 오르는 카운터. 라벨은 **경로 템플릿**이라 카디널리티가 유한하다. */
    public static final String REDIS_UNAVAILABLE_METRIC = RedisMetrics.UNAVAILABLE;

    private final MeterRegistry meterRegistry;

    /**
     * <b>{@code MeterRegistry}를 필수 의존으로 걸지 않는다.</b>
     *
     * <p>처음에 생성자 인자로 그냥 받았더니 {@code @WebMvcTest} 슬라이스의 컨텍스트가
     * 통째로 못 떴다 — 슬라이스는 Micrometer 자동 구성을 안 올리는데 이 advice는
     * 슬라이스에 포함되기 때문이다. <b>지표 하나 때문에 예외 처리 전체가 사라지는 것</b>은
     * 맞는 교환이 아니다.
     *
     * <p>그래서 없으면 버리는 registry로 대신한다. 다만 <b>조용히</b> 넘어가지 않는다 —
     * 운영에서 이 분기를 타면 지표가 Prometheus에 안 올라간다는 뜻이고 그건 보여야 한다.
     */
    @Autowired
    public GlobalExceptionHandler(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        MeterRegistry found = meterRegistryProvider.getIfAvailable();
        if (found == null) {
            log.warn("[지표] MeterRegistry 빈이 없다 — {} 카운터는 수집되지 않는다",
                REDIS_UNAVAILABLE_METRIC);
            found = new SimpleMeterRegistry();
        }
        this.meterRegistry = found;
    }

    /** 테스트와 직접 조립용. */
    public GlobalExceptionHandler(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Redis에 못 닿았다 — <b>503으로 명시한다.</b>
     *
     * <h3>왜 500이 아니라 503인가</h3>
     *
     * 지금까지는 이 예외가 {@code handleGeneral}로 떨어져 <b>정체불명의 500</b>이 됐다.
     * 실제로는 <b>우리 코드의 버그가 아니라 의존 서비스가 없는 상태</b>이고, 그건
     * 클라이언트와 운영자가 다르게 대응해야 하는 일이다 — 503은 재시도 가능함을 뜻한다.
     *
     * <h3>여기 오는 것과 안 오는 것</h3>
     *
     * 이 핸들러는 <b>거부하기로 정한 경로</b>의 표현만 담당한다 — 로그인 보호,
     * 진단 제한, refresh 토큰이다(전부 fail-closed).
     * <b>일반 조회는 여기 오지 않는다</b> — {@code RateLimitInterceptor}가
     * fail-open으로 통과시키고 {@code telemetry.ratelimit.failopen}을 올린다.
     * 두 지표가 <b>같이 오르지 않는 것이 정상</b>이다
     * ({@code docs/redis-failure-policy.md} §3-2).
     *
     * <h3>조용히 지나가지 않게 한다</h3>
     *
     * 지표를 올리고 WARN을 남긴다. 어느 경로가 막히는지 알아야 "Redis가 죽어서 조회가
     * 안 된다"와 "그 API가 원래 깨졌다"를 가를 수 있다.
     * <b>차량 ID 같은 값은 라벨에 안 들어간다</b> — 경로 <b>템플릿</b>을 쓴다.
     */
    @ExceptionHandler({DataAccessResourceFailureException.class, QueryTimeoutException.class})
    public ResponseEntity<ErrorResponse> handleRedisUnavailable(
        RuntimeException e, HttpServletRequest request
    ) {
        String route = RedisMetrics.routeTemplate(request);
        RedisMetrics.unavailable(meterRegistry, route);
        // 예외 메시지에 접속 정보가 섞일 수 있어 클래스 이름만 남긴다.
        log.warn("[Redis] 사용 불가 — 요청 거부 route={} cause={}",
            route, e.getClass().getSimpleName());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(new ErrorResponse("REDIS_UNAVAILABLE",
                "요청 제한·인증 저장소에 일시적으로 접근할 수 없습니다. 잠시 후 다시 시도해 주세요"));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException e) {
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("BAD_REQUEST", e.getMessage()));
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleResourceNotFound(ResourceNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(new ErrorResponse("NOT_FOUND", e.getMessage()));
    }

    @ExceptionHandler(ResourceConflictException.class)
    public ResponseEntity<ErrorResponse> handleResourceConflict(ResourceConflictException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(new ErrorResponse("CONFLICT", e.getMessage()));
    }

    @ExceptionHandler(BadCredentialsException.class)
    public ResponseEntity<ErrorResponse> handleBadCredentials(BadCredentialsException e) {
        // "아이디가 없다" / "비밀번호가 틀렸다"를 구분하지 않는다.
        // 구분하면 공격자가 유효한 계정을 열거(user enumeration)할 수 있다.
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
            .body(new ErrorResponse("UNAUTHORIZED", "아이디 또는 비밀번호가 올바르지 않습니다"));
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ErrorResponse> handleAccessDenied(AccessDeniedException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
            .body(new ErrorResponse("FORBIDDEN", "접근 권한이 없습니다"));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleMessageNotReadable(HttpMessageNotReadableException e) {
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("BAD_REQUEST", "요청 본문을 읽을 수 없습니다"));
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<ErrorResponse> handleDataIntegrityViolation(DataIntegrityViolationException e) {
        // 유니크 제약 위반 등 — 원본 예외 메시지는 DB 구조를 노출할 수 있어 응답에 담지 않는다.
        log.warn("데이터 무결성 제약 위반", e);
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .body(new ErrorResponse("CONFLICT", "이미 존재하거나 다른 데이터와 충돌합니다"));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException e) {
        Map<String, String> errors = e.getBindingResult().getFieldErrors().stream()
            .collect(Collectors.toMap(FieldError::getField, FieldError::getDefaultMessage));
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("VALIDATION_FAILED", errors.toString()));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(ConstraintViolationException e) {
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("VALIDATION_FAILED", "요청 파라미터 범위가 올바르지 않습니다"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneral(Exception e) {
        log.error("처리되지 않은 예외", e);
        return ResponseEntity.internalServerError()
            .body(new ErrorResponse("INTERNAL_ERROR", "서버 내부 오류가 발생했습니다"));
    }

    @ExceptionHandler(ServiceUnavailableException.class)
    public ResponseEntity<ErrorResponse> handleServiceUnavailable(ServiceUnavailableException e) {
        log.warn("외부 서비스 일시 사용 불가: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(new ErrorResponse("SERVICE_UNAVAILABLE", e.getMessage()));
    }
}
