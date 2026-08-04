package com.telemetry.security;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

/** 로그인 요청 전용 고정 윈도우 제한. 신뢰할 수 있는 IP와 username을 함께 키로 사용한다. */
@Component
@RequiredArgsConstructor
public class LoginRateLimiter {

    private static final String PREFIX = "login_rate:";

    private final StringRedisTemplate redisTemplate;

    @Value("${rate-limit.login-attempts-per-minute:10}")
    private int attemptsPerMinute;

    public boolean tryAcquire(String clientIp, String username) {
        String key = PREFIX + clientIp + ":" + hashUsername(username);
        Long count = redisTemplate.opsForValue().increment(key);
        if (count != null && count == 1L) {
            redisTemplate.expire(key, Duration.ofMinutes(1));
        }
        return count != null && count <= attemptsPerMinute;
    }

    private String hashUsername(String username) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(username.trim().toLowerCase().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256을 사용할 수 없습니다", e);
        }
    }
}
