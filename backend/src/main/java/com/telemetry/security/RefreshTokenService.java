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
 * Redis에 opaque refresh token을 저장한다 (JWT 서명 방식이 아닌 랜덤 문자열 + 서버 측 조회).
 * Access Token은 Stateless JWT라 발급 후 서버가 강제로 무효화할 수 없다.
 * Refresh Token을 Redis에 두면 로그아웃 시 해당 키를 지워 재발급을 막을 수 있어,
 * 사실상 "로그아웃 후 토큰 무효화" 요구사항을 이 방식으로 충족한다.
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
     * 토큰을 검증하고 즉시 폐기한다 (rotation). 탈취된 refresh token이 재사용되는 창구를
     * 최소화하기 위함 — 정상 사용자가 재발급받으면 이전 토큰은 더 이상 쓸 수 없다.
     * 호출자는 반환된 username으로 새 access/refresh token을 발급해야 한다.
     */
    public Optional<String> rotate(String token) {
        String key = PREFIX + token;
        // Redis GETDEL: 검증과 폐기를 단일 원자 연산으로 수행해 동시 재사용을 막는다.
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
     * 한 사용자의 refresh token을 전부 폐기한다(비밀번호 변경·초기화). 저장 구조가 {@code token -> username}
     * 단방향이라 사용자별 색인이 없다 — {@code SCAN}으로 prefix 키를 훑어 값이 일치하는 것을 지운다.
     * 키 수에 비례하는 비용이라 대량 사용자에는 맞지 않지만, 사용자별 색인을 추가하면 색인 도입 전에 발급된
     * 토큰이 남는다. 이 프로젝트 규모(사용자 소수, 14일 TTL)에서는 색인 없이 확실히 지우는 쪽을 골랐다.
     * Redis 오류는 그대로 전파한다 — 호출자가 비밀번호 변경을 되돌리도록.
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
