package com.telemetry.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Redis 기반 브루트포스 로그인 차단.
 * IP당 N회 실패 시 M분간 차단.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BruteForceDetector {

    private static final String PREFIX = "brute_force:";
    private static final int MAX_ATTEMPTS = 5;
    private static final int BLOCK_MINUTES = 15;

    private final StringRedisTemplate redisTemplate;

    public boolean isBlocked(String ip) {
        String key = PREFIX + ip;
        String val = redisTemplate.opsForValue().get(key);
        if (val == null) return false;
        boolean blocked = Integer.parseInt(val) >= MAX_ATTEMPTS;
        if (blocked) {
            log.warn("[BruteForce] 차단된 IP 접근 시도: {}", ip);
        }
        return blocked;
    }

    public void recordFailure(String ip) {
        String key = PREFIX + ip;
        Long count = redisTemplate.opsForValue().increment(key);
        if (count == 1) {
            // 첫 실패 시 TTL 설정
            redisTemplate.expire(key, Duration.ofMinutes(BLOCK_MINUTES));
        }
        log.warn("[BruteForce] 로그인 실패 ip={} count={}/{}", ip, count, MAX_ATTEMPTS);
        if (count >= MAX_ATTEMPTS) {
            log.warn("[BruteForce] IP 차단 적용: {} ({}분)", ip, BLOCK_MINUTES);
        }
    }

    public void recordSuccess(String ip) {
        // 성공 시 카운터 초기화
        redisTemplate.delete(PREFIX + ip);
    }
}
