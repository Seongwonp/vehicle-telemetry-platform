package com.telemetry.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("JwtTokenProvider 단위 테스트")
class JwtTokenProviderTest {

    private JwtTokenProvider jwtTokenProvider;

    // 테스트용 시크릿 (32자 이상 필수)
    private static final String SECRET = "test-secret-key-must-be-32-chars-minimum!!";
    private static final long EXPIRATION_MS = 3_600_000L; // 1시간

    @BeforeEach
    void setUp() {
        jwtTokenProvider = new JwtTokenProvider(SECRET, EXPIRATION_MS);
    }

    @Test
    @DisplayName("토큰 생성 후 사용자명 추출 성공")
    void generateToken_사용자명_추출() {
        String token = jwtTokenProvider.generateToken("admin");

        assertThat(jwtTokenProvider.getUsername(token)).isEqualTo("admin");
    }

    @Test
    @DisplayName("유효한 토큰 검증 성공")
    void validate_유효한토큰_true() {
        String token = jwtTokenProvider.generateToken("admin");

        assertThat(jwtTokenProvider.validate(token)).isTrue();
    }

    @Test
    @DisplayName("만료된 토큰 검증 실패")
    void validate_만료토큰_false() throws InterruptedException {
        JwtTokenProvider shortLived = new JwtTokenProvider(SECRET, 1L); // 1ms 만료
        String token = shortLived.generateToken("admin");

        Thread.sleep(10); // 만료 대기

        assertThat(shortLived.validate(token)).isFalse();
    }

    @Test
    @DisplayName("변조된 토큰 검증 실패")
    void validate_변조토큰_false() {
        String token = jwtTokenProvider.generateToken("admin");
        String tampered = token + "TAMPERED";

        assertThat(jwtTokenProvider.validate(tampered)).isFalse();
    }

    @Test
    @DisplayName("빈 문자열 토큰 검증 실패")
    void validate_빈문자열_false() {
        assertThat(jwtTokenProvider.validate("")).isFalse();
    }

    @Test
    @DisplayName("서로 다른 사용자의 토큰은 다름")
    void generateToken_다른사용자_다른토큰() {
        String token1 = jwtTokenProvider.generateToken("user1");
        String token2 = jwtTokenProvider.generateToken("user2");

        assertThat(token1).isNotEqualTo(token2);
        assertThat(jwtTokenProvider.getUsername(token1)).isEqualTo("user1");
        assertThat(jwtTokenProvider.getUsername(token2)).isEqualTo("user2");
    }
}
