package com.telemetry.config;

import com.telemetry.security.JwtTokenProvider;
import com.telemetry.security.VehicleAccessService;
import lombok.RequiredArgsConstructor;
import org.springframework.lang.NonNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@RequiredArgsConstructor
public class WebSocketAuthChannelInterceptor implements ChannelInterceptor {

    private static final String JWT_EXPIRES_AT = "jwtExpiresAt";
    private static final Pattern VEHICLE_TOPIC = Pattern.compile(
        "^/topic/vehicle/([A-Z0-9-]{4,20})/(telemetry|anomalies)$");

    private final JwtTokenProvider jwtTokenProvider;
    private final UserDetailsService userDetailsService;
    private final VehicleAccessService vehicleAccessService;
    private final WebSocketSessionRegistry sessionRegistry;

    @Override
    public Message<?> preSend(@NonNull Message<?> message, @NonNull MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (accessor == null || accessor.getCommand() == null) return message;

        if (StompCommand.CONNECT.equals(accessor.getCommand())) {
            authenticate(accessor);
        } else {
            rejectExpired(accessor);
        }

        if (StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
            authorizeSubscription(accessor);
        } else if (StompCommand.DISCONNECT.equals(accessor.getCommand()) && accessor.getSessionId() != null) {
            sessionRegistry.unregister(accessor.getSessionId());
        }
        return message;
    }

    private void authenticate(StompHeaderAccessor accessor) {
        String token = extractToken(accessor);
        if (token == null || !jwtTokenProvider.validate(token)) {
            throw new AuthenticationCredentialsNotFoundException("WebSocket 인증 실패: 유효하지 않은 토큰");
        }
        UserDetails user = userDetailsService.loadUserByUsername(jwtTokenProvider.getUsername(token));
        accessor.setUser(new UsernamePasswordAuthenticationToken(user, null, user.getAuthorities()));

        Instant expiresAt = jwtTokenProvider.getExpiration(token);
        Map<String, Object> attributes = accessor.getSessionAttributes();
        if (attributes != null) attributes.put(JWT_EXPIRES_AT, expiresAt);
        if (accessor.getSessionId() != null) {
            sessionRegistry.scheduleExpiration(accessor.getSessionId(), expiresAt);
        }
    }

    private void rejectExpired(StompHeaderAccessor accessor) {
        Map<String, Object> attributes = accessor.getSessionAttributes();
        Instant expiresAt = attributes == null ? null : (Instant) attributes.get(JWT_EXPIRES_AT);
        if (expiresAt != null && !expiresAt.isAfter(Instant.now())) {
            if (accessor.getSessionId() != null) sessionRegistry.closeExpired(accessor.getSessionId());
            throw new AuthenticationCredentialsNotFoundException("WebSocket JWT가 만료되었습니다");
        }
    }

    private void authorizeSubscription(StompHeaderAccessor accessor) {
        String destination = accessor.getDestination();
        Matcher matcher = destination == null ? null : VEHICLE_TOPIC.matcher(destination);
        if (matcher == null || !matcher.matches()) {
            throw new AccessDeniedException("허용되지 않은 WebSocket 구독 경로입니다");
        }
        if (!(accessor.getUser() instanceof Authentication authentication)
            || !vehicleAccessService.canAccess(authentication, matcher.group(1))) {
            throw new AccessDeniedException("차량 WebSocket 구독 권한이 없습니다");
        }
    }

    private String extractToken(StompHeaderAccessor accessor) {
        String header = accessor.getFirstNativeHeader("Authorization");
        return header != null && header.startsWith("Bearer ") ? header.substring(7) : null;
    }
}
