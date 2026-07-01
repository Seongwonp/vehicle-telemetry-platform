package com.telemetry.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
        String username = redisTemplate.opsForValue().get(key);
        if (username == null) {
            log.warn("[RefreshToken] 유효하지 않거나 만료된 토큰으로 재발급 시도");
            return Optional.empty();
        }
        redisTemplate.delete(key);
        return Optional.of(username);
    }

    public void revoke(String token) {
        redisTemplate.delete(PREFIX + token);
    }
}
