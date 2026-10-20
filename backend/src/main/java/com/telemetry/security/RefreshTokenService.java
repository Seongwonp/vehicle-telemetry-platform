package com.telemetry.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Redis에 opaque refresh token을 저장한다. 키를 지우면 재발급이 막혀 로그아웃 시
 * 토큰을 무효화할 수 있다. 이유: ADR-010
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RefreshTokenService {

    private static final String PREFIX = "refresh_token:";
    private static final Duration TTL = Duration.ofDays(14);

    private final StringRedisTemplate redisTemplate;

    public String issue(String username) {
        String token = UUID.randomUUID().toString();
        redisTemplate.opsForValue().set(PREFIX + token, username, TTL);
        return token;
    }

    /**
     * 토큰을 검증하고 즉시 폐기한다(rotation). 탈취 토큰의 재사용 창을 줄인다.
     * 호출자는 반환된 username으로 새 토큰을 발급해야 한다.
     */
    public Optional<String> rotate(String token) {
        String key = PREFIX + token;
        // GETDEL로 검증과 폐기를 원자적으로 해서 동시 재사용을 막는다.
        String username = redisTemplate.opsForValue().getAndDelete(key);
        if (username == null) {
            log.warn("[RefreshToken] 유효하지 않거나 만료된 토큰으로 재발급 시도");
            return Optional.empty();
        }
        return Optional.of(username);
    }

    public void revoke(String token) {
        redisTemplate.delete(PREFIX + token);
    }

    /**
     * 한 사용자의 refresh token을 전부 폐기한다(비밀번호 변경·초기화). 사용자별 색인이 없어
     * {@code SCAN}으로 훑는다(ADR-027). Redis 오류는 전파해야 호출자가 비밀번호 변경을 되돌린다.
     *
     * @return 지운 토큰 수
     */
    public int revokeAll(String username) {
        int removed = 0;
        ScanOptions options = ScanOptions.scanOptions().match(PREFIX + "*").count(500).build();
        try (Cursor<String> cursor = redisTemplate.scan(options)) {
            while (cursor.hasNext()) {
                String key = cursor.next();
                if (username.equals(redisTemplate.opsForValue().get(key))
                    && Boolean.TRUE.equals(redisTemplate.delete(key))) {
                    removed++;
                }
            }
        }
        log.info("[RefreshToken] 사용자 토큰 전체 폐기 — username={} removed={}", username, removed);
        return removed;
    }
}
