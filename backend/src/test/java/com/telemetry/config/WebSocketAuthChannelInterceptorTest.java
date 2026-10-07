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
        given(userDetailsService.loadUserByUsername("user"))
            .willReturn(User.withUsername("user").password("x").roles("USER").build());
        given(vehicleAccessService.canAccess(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("KR-GA-1234"))).willReturn(false);
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

    @Test
    void rejectsClientSendEvenFromAuthenticatedOwner() {
        // 구독 권한이 있는 소유자라도 발행은 못 한다 — 발행 경로는 서버뿐이다.
        var authentication = new UsernamePasswordAuthenticationToken("hong", null, java.util.List.of());
        StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.SEND);
        accessor.setDestination("/topic/vehicle/KR-GA-1234/telemetry");
        accessor.setUser(authentication);
        accessor.setSessionAttributes(new HashMap<>());

        assertThatThrownBy(() -> interceptor.preSend(message(accessor), channel))
            .isInstanceOf(AccessDeniedException.class);
        org.mockito.Mockito.verifyNoInteractions(vehicleAccessService);
    }

    @Test
    void simpleBrokerKeepsDeliveringAfterAccessIsRevoked() {
        // ADR-027 한계("비활성화가 열린 구독을 끊지 않는다 — 수정하지 않기로")의 근거를 고정한다.
        // 권한 검사는 SUBSCRIBE 한 번뿐이고 simple broker는 프레임마다 다시 묻지 않는다 — 비활성화 뒤에도
        // 이미 맺은 구독에는 계속 나간다. 이 동작을 바꾸면(세션 종료·outbound 재검사) 이 테스트를 뒤집는다.
        var authentication = new UsernamePasswordAuthenticationToken("hong", null, java.util.List.of());
        given(userDetailsService.loadUserByUsername("hong"))
            .willReturn(User.withUsername("hong").password("x").roles("USER").build());
        given(vehicleAccessService.canAccess(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("KR-GA-1234"))).willReturn(true);
        StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
        accessor.setDestination("/topic/vehicle/KR-GA-1234/telemetry");
        accessor.setSessionId("s1");
        accessor.setSubscriptionId("sub-0");
        accessor.setUser(authentication);
        accessor.setSessionAttributes(new HashMap<>());
        Message<?> subscribe = interceptor.preSend(message(accessor), channel);

        var outbound = org.mockito.Mockito.mock(MessageChannel.class);
        var broker = new org.springframework.messaging.simp.broker.SimpleBrokerMessageHandler(
            org.mockito.Mockito.mock(org.springframework.messaging.SubscribableChannel.class), outbound,
            org.mockito.Mockito.mock(org.springframework.messaging.SubscribableChannel.class),
            java.util.List.of("/topic"));
        broker.start();
        // 브로커는 CONNECT로 세션을 알아야 그 세션에 내보낸다(CONNECT_ACK는 검증에서 뺀다).
        var connect = org.springframework.messaging.simp.SimpMessageHeaderAccessor
            .create(org.springframework.messaging.simp.SimpMessageType.CONNECT);
        connect.setSessionId("s1");
        broker.handleMessage(MessageBuilder.createMessage(new byte[0], connect.getMessageHeaders()));
        broker.handleMessage(subscribe);
        org.mockito.Mockito.clearInvocations(outbound);

        // 여기서 차량이 비활성화돼도 브로커는 canAccess를 다시 묻지 않는다(아래 times(1)).
        var headers = org.springframework.messaging.simp.SimpMessageHeaderAccessor
            .create(org.springframework.messaging.simp.SimpMessageType.MESSAGE);
        headers.setDestination("/topic/vehicle/KR-GA-1234/telemetry");
        broker.handleMessage(MessageBuilder.createMessage(new byte[0], headers.getMessageHeaders()));

        org.mockito.Mockito.verify(outbound).send(org.mockito.ArgumentMatchers.any());
        org.mockito.Mockito.verify(vehicleAccessService, org.mockito.Mockito.times(1))
            .canAccess(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("KR-GA-1234"));
        broker.stop();
    }

    private Message<byte[]> message(StompHeaderAccessor accessor) {
        accessor.setLeaveMutable(true);
        return MessageBuilder.createMessage(new byte[0], accessor.getMessageHeaders());
    }
}
