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
    private final Map<String, String> sessionUsers = new ConcurrentHashMap<>();
    private final Map<String, String> sessionVehicles = new ConcurrentHashMap<>();
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
        sessionUsers.remove(sessionId);
        sessionVehicles.remove(sessionId);
        ScheduledFuture<?> task = expirationTasks.remove(sessionId);
        if (task != null) task.cancel(false);
    }

    public void registerUser(String sessionId, String username) {
        sessionUsers.put(sessionId, username);
    }

    public void registerSubscription(String sessionId, String vehicleId) {
        sessionVehicles.put(sessionId, vehicleId);
    }

    public void closeVehicleSessions(String vehicleId) {
        sessionVehicles.forEach((sessionId, subscribedVehicle) -> {
            if (vehicleId.equals(subscribedVehicle)) close(sessionId, "Vehicle access revoked");
        });
    }

    public void closeUserSessions(String username) {
        sessionUsers.forEach((sessionId, sessionUser) -> {
            if (username.equals(sessionUser)) close(sessionId, "User authorization revoked");
        });
    }

    public void scheduleExpiration(String sessionId, Instant expiresAt) {
        long delayMs = Math.max(0, Duration.between(Instant.now(), expiresAt).toMillis());
        ScheduledFuture<?> previous = expirationTasks.put(sessionId,
            scheduler.schedule(() -> closeExpired(sessionId), delayMs, TimeUnit.MILLISECONDS));
        if (previous != null) previous.cancel(false);
    }

    public void closeExpired(String sessionId) {
        close(sessionId, "JWT expired");
    }

    private void close(String sessionId, String reason) {
        WebSocketSession session = sessions.get(sessionId);
        if (session == null || !session.isOpen()) return;
        try {
            session.close(CloseStatus.POLICY_VIOLATION.withReason(reason));
        } catch (IOException e) {
            log.warn("WebSocket 세션 종료 실패 session={} reason={}", sessionId, reason, e);
        } finally {
            unregister(sessionId);
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
