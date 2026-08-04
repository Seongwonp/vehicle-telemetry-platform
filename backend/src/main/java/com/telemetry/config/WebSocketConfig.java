package com.telemetry.config;

import com.telemetry.security.JwtTokenProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import java.util.List;

/**
 * 대시보드 실시간 스트리밍용 STOMP 엔드포인트.
 * REST API는 JwtAuthenticationFilter가 매 요청 헤더를 검사하지만, WebSocket은
 * HTTP 업그레이드 이후 하나의 연결을 계속 재사용하므로 인증을 CONNECT
 * 프레임 시점에 한 번 검사한다 — HTTP 핸드셰이크 자체는 SecurityConfig에서
 * permitAll로 열어두고, 실제 인가는 여기 ChannelInterceptor에서 처리한다.
 */
@Configuration
@EnableWebSocketMessageBroker
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final JwtTokenProvider jwtTokenProvider;

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // 네이티브 앱(Flutter)은 SockJS 폴백이 필요 없어 순수 WebSocket만 노출한다.
        registry.addEndpoint("/ws").setAllowedOriginPatterns("*");
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(new ChannelInterceptor() {
            @Override
            public Message<?> preSend(@NonNull Message<?> message, @NonNull MessageChannel channel) {
                StompHeaderAccessor accessor =
                    StompHeaderAccessor.wrap(message);
                if (StompCommand.CONNECT.equals(accessor.getCommand())) {
                    String token = extractToken(accessor);
                    if (token == null || !jwtTokenProvider.validate(token)) {
                        throw new AuthenticationCredentialsNotFoundException(
                            "WebSocket 인증 실패: 유효하지 않은 토큰");
                    }
                    String username = jwtTokenProvider.getUsername(token);
                    accessor.setUser(
                        new UsernamePasswordAuthenticationToken(username, null, List.of()));
                }
                return message;
            }
        });
    }

    private String extractToken(StompHeaderAccessor accessor) {
        String header = accessor.getFirstNativeHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            return header.substring(7);
        }
        return null;
    }
}
