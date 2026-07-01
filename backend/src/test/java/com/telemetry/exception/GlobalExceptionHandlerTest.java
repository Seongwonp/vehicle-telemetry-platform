package com.telemetry.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("GlobalExceptionHandler 단위 테스트")
class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    @DisplayName("IllegalArgumentException → 400")
    void handleIllegalArgument_400() {
        ResponseEntity<ErrorResponse> response =
            handler.handleIllegalArgument(new IllegalArgumentException("잘못된 요청"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().getCode()).isEqualTo("BAD_REQUEST");
    }

    @Test
    @DisplayName("IllegalStateException → 404")
    void handleIllegalState_404() {
        ResponseEntity<ErrorResponse> response =
            handler.handleIllegalState(new IllegalStateException("데이터 없음"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("BadCredentialsException → 401, 계정 존재 여부를 알 수 없는 통일된 메시지")
    void handleBadCredentials_401_통일메시지() {
        ResponseEntity<ErrorResponse> response =
            handler.handleBadCredentials(new BadCredentialsException("어떤 계정이 없습니다"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getBody().getMessage()).isEqualTo("아이디 또는 비밀번호가 올바르지 않습니다");
    }

    @Test
    @DisplayName("AccessDeniedException → 403")
    void handleAccessDenied_403() {
        ResponseEntity<ErrorResponse> response =
            handler.handleAccessDenied(new AccessDeniedException("권한 없음"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody().getCode()).isEqualTo("FORBIDDEN");
    }

    @Test
    @DisplayName("HttpMessageNotReadableException → 400")
    @SuppressWarnings("deprecation") // 실제 HttpInputMessage 없이 테스트하려면 deprecated 생성자가 유일한 선택지
    void handleMessageNotReadable_400() {
        ResponseEntity<ErrorResponse> response =
            handler.handleMessageNotReadable(new HttpMessageNotReadableException("파싱 실패"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().getCode()).isEqualTo("BAD_REQUEST");
    }

    @Test
    @DisplayName("DataIntegrityViolationException → 409, DB 원본 메시지는 노출하지 않음")
    void handleDataIntegrityViolation_409_메시지비노출() {
        ResponseEntity<ErrorResponse> response = handler.handleDataIntegrityViolation(
            new DataIntegrityViolationException("duplicate key value violates unique constraint \"vehicles_pkey\"")
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().getMessage()).doesNotContain("constraint");
    }

    @Test
    @DisplayName("처리되지 않은 예외 → 500, 스택트레이스 대신 일반 메시지")
    void handleGeneral_500_일반메시지() {
        ResponseEntity<ErrorResponse> response =
            handler.handleGeneral(new RuntimeException("내부 상세 오류: DB 커넥션 풀 고갈"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody().getMessage()).isEqualTo("서버 내부 오류가 발생했습니다");
    }
}
