package com.telemetry.config;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class WebSocketSessionRegistry {

    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> expirationTasks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "websocket-jwt-expiration");
        thread.setDaemon(true);
        return thread;
    });

    public void register(WebSocketSession session) {
        sessions.put(session.getId(), session);
    }

    public void unregister(String sessionId) {
        sessions.remove(sessionId);
        ScheduledFuture<?> task = expirationTasks.remove(sessionId);
        if (task != null) task.cancel(false);
    }

    public void scheduleExpiration(String sessionId, Instant expiresAt) {
        long delayMs = Math.max(0, Duration.between(Instant.now(), expiresAt).toMillis());
        ScheduledFuture<?> previous = expirationTasks.put(sessionId,
            scheduler.schedule(() -> closeExpired(sessionId), delayMs, TimeUnit.MILLISECONDS));
        if (previous != null) previous.cancel(false);
    }

    public void closeExpired(String sessionId) {
        WebSocketSession session = sessions.get(sessionId);
        if (session == null || !session.isOpen()) return;
        try {
            session.close(CloseStatus.POLICY_VIOLATION.withReason("JWT expired"));
        } catch (IOException e) {
            log.warn("만료된 WebSocket 세션 종료 실패 session={}", sessionId, e);
        } finally {
            unregister(sessionId);
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
