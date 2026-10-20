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

    /** Redis 사용 불가 카운터. 라벨은 경로 템플릿이라 카디널리티가 유한하다. */
    public static final String REDIS_UNAVAILABLE_METRIC = RedisMetrics.UNAVAILABLE;

    private final MeterRegistry meterRegistry;

    /**
     * {@code MeterRegistry}를 필수 의존으로 걸지 않는다. {@code @WebMvcTest} 슬라이스에는
     * Micrometer가 없어서, 필수로 걸면 이 advice 때문에 컨텍스트가 뜨지 않는다.
     * 없으면 임시 registry로 대신하되 WARN을 남긴다.
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
        // 합계 카운터를 기동 시 0으로 등록해야 첫 실패가 increase()에 잡힌다.
        RedisMetrics.preRegister(this.meterRegistry);
    }

    /** 테스트와 직접 조립용. */
    public GlobalExceptionHandler(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        RedisMetrics.preRegister(this.meterRegistry);
    }

    /**
     * Redis 사용 불가는 코드 버그가 아니라 의존성 장애이므로 500이 아닌 503으로 응답한다.
     *
     * <p>fail-closed 경로(로그인 보호, 진단 제한, refresh 토큰)만 여기 온다. 일반 조회는
     * {@code RateLimitInterceptor}가 fail-open으로 통과시킨다.
     * 정책: {@code docs/redis-failure-policy.md} §3-2
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

    /**
     * 현재 비밀번호 불일치는 401이 아니라 400이다. 401이면 클라이언트가 세션 만료로 보고
     * refresh나 로그아웃을 시도한다.
     */
    @ExceptionHandler(CurrentPasswordMismatchException.class)
    public ResponseEntity<ErrorResponse> handleCurrentPasswordMismatch(CurrentPasswordMismatchException e) {
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("CURRENT_PASSWORD_INCORRECT", e.getMessage()));
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
