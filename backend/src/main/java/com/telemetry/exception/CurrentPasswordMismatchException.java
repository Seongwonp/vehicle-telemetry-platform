package com.telemetry.exception;

/** 비밀번호 변경에서 현재 비밀번호가 틀렸다. 401이 아니라 400으로 나간다 — {@link GlobalExceptionHandler} 참고. */
public class CurrentPasswordMismatchException extends RuntimeException {

    public CurrentPasswordMismatchException() {
        super("현재 비밀번호가 올바르지 않습니다");
    }
}
