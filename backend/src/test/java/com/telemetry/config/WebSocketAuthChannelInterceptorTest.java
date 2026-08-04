package com.telemetry.config;

import com.telemetry.security.JwtTokenProvider;
import com.telemetry.security.VehicleAccessService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;

import java.time.Instant;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class WebSocketAuthChannelInterceptorTest {

    @Mock JwtTokenProvider jwtTokenProvider;
    @Mock UserDetailsService userDetailsService;
    @Mock VehicleAccessService vehicleAccessService;
    @Mock WebSocketSessionRegistry sessionRegistry;
    @Mock MessageChannel channel;

    private WebSocketAuthChannelInterceptor interceptor;

    @BeforeEach
    void setUp() {
        interceptor = new WebSocketAuthChannelInterceptor(
            jwtTokenProvider, userDetailsService, vehicleAccessService, sessionRegistry);
    }

    @Test
    void connectRestoresUserAndSchedulesJwtExpiration() {
        var user = User.withUsername("admin").password("x").roles("ADMIN").build();
        Instant expiration = Instant.now().plusSeconds(300);
        given(jwtTokenProvider.validate("token")).willReturn(true);
        given(jwtTokenProvider.getUsername("token")).willReturn("admin");
        given(jwtTokenProvider.getExpiration("token")).willReturn(expiration);
        given(userDetailsService.loadUserByUsername("admin")).willReturn(user);

        StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.CONNECT);
        accessor.setNativeHeader("Authorization", "Bearer token");
        accessor.setSessionId("session-1");
        accessor.setSessionAttributes(new HashMap<>());
        interceptor.preSend(message(accessor), channel);

        org.mockito.Mockito.verify(sessionRegistry).scheduleExpiration("session-1", expiration);
    }

    @Test
    void rejectsSubscriptionToVehicleWithoutOwnership() {
        var authentication = new UsernamePasswordAuthenticationToken(
            "user", null, java.util.List.of());
        given(vehicleAccessService.canAccess(authentication, "KR-GA-1234")).willReturn(false);
        StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
        accessor.setDestination("/topic/vehicle/KR-GA-1234/telemetry");
        accessor.setUser(authentication);
        accessor.setSessionAttributes(new HashMap<>());

        assertThatThrownBy(() -> interceptor.preSend(message(accessor), channel))
            .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void rejectsFramesAfterJwtExpirationAndClosesSession() {
        StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
        accessor.setDestination("/topic/vehicle/KR-GA-1234/telemetry");
        accessor.setSessionId("expired-session");
        var attributes = new HashMap<String, Object>();
        attributes.put("jwtExpiresAt", Instant.now().minusSeconds(1));
        accessor.setSessionAttributes(attributes);

        assertThatThrownBy(() -> interceptor.preSend(message(accessor), channel))
            .isInstanceOf(AuthenticationCredentialsNotFoundException.class);
        org.mockito.Mockito.verify(sessionRegistry).closeExpired("expired-session");
    }

    private Message<byte[]> message(StompHeaderAccessor accessor) {
        accessor.setLeaveMutable(true);
        return MessageBuilder.createMessage(new byte[0], accessor.getMessageHeaders());
    }
}
