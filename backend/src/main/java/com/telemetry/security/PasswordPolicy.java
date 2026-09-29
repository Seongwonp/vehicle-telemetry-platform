package com.telemetry.security;

/**
 * 비밀번호 규칙의 단일 출처. 사용자 생성·본인 변경·관리자 초기화가 모두 {@link ValidPassword}로 이걸 쓴다.
 * 규칙을 바꾸면 세 경로가 같이 바뀐다 — 경로마다 {@code @Size}를 따로 적으면 한 곳만 고쳐지는 일이 생긴다.
 *
 * <p>상한 72는 BCrypt가 입력을 72바이트까지만 쓰기 때문이다(ADR-027).
 */
public final class PasswordPolicy {

    public static final int MIN_LENGTH = 8;
    public static final int MAX_LENGTH = 72;
    public static final String MESSAGE = "비밀번호는 " + MIN_LENGTH + "~" + MAX_LENGTH + "자";

    private PasswordPolicy() {}
}
