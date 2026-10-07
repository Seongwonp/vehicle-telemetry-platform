package com.telemetry.config;

import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.repository.UserRepository;
import com.telemetry.repository.VehicleRepository;
import com.telemetry.security.DbUserDetailsService;
import com.telemetry.security.JwtTokenProvider;
import com.telemetry.security.VehicleAccessService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.ServletWebServerFactoryAutoConfiguration;
import org.springframework.boot.autoconfigure.websocket.servlet.WebSocketServletAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * 이미 열린 연결이 <b>언제</b> 닫히고 언제 브로드캐스트를 그만 받는지를 실제 Tomcat + STOMP 클라이언트로 잰다.
 *
 * <p>앱 전체가 아니라 WebSocket 구성(WebSocketConfig·인터셉터·세션 등록부·simple broker)만 띄운다.
 * HTTP 보안 필터 체인은 없다 — /ws 핸드셰이크는 원래 permitAll이고 인증은 STOMP CONNECT 인터셉터가 한다.
 * 저장소만 메모리 모형이다.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = WebSocketExpiryIntegrationTest.WsOnlyApp.class,
    properties = {"cors.allowed-origin-patterns=*", "spring.main.banner-mode=off"})
class WebSocketExpiryIntegrationTest {

    static final String SECRET = "test-secret-for-websocket-expiry-integration-0123456789";
    static final String TOPIC = "/topic/vehicle/HONG-0001/telemetry";

    // @SpringBootConfiguration이면 같은 패키지의 @WebMvcTest들이 이 클래스를 앱 설정으로 집어 간다 — 평범한 @Configuration으로 둔다.
    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    @ImportAutoConfiguration({ServletWebServerFactoryAutoConfiguration.class,
        DispatcherServletAutoConfiguration.class, WebSocketServletAutoConfiguration.class})
    @Import({WebSocketConfig.class, WebSocketAuthChannelInterceptor.class, WebSocketSessionRegistry.class,
        VehicleAccessService.class, DbUserDetailsService.class})
    static class WsOnlyApp {
        /** 짧은 수명(2.5초 → exp 초 절삭으로 실제 1.5~2.5초). */
        @Bean
        JwtTokenProvider jwtTokenProvider() {
            return new JwtTokenProvider(SECRET, 2_500);
        }
    }

    @MockBean UserRepository userRepository;
    @MockBean VehicleRepository vehicleRepository;
    @Autowired SimpMessagingTemplate broker;
    @Autowired JwtTokenProvider shortJwt;
    @LocalServerPort int port;

    private final Map<String, User> users = new ConcurrentHashMap<>();
    private WebSocketStompClient client;

    @BeforeEach
    void setUp() {
        users.put("hong", new User("hong", "$2a$10$hash", Role.USER));
        when(userRepository.findByUsername(anyString())).thenAnswer(inv -> {
            User u = users.get(inv.<String>getArgument(0));
            if (u == null) return Optional.empty();
            User copy = new User(u.getUsername(), u.getPasswordHash(), u.getRole());
            copy.setActive(u.isActive());
            return Optional.of(copy);
        });
        when(vehicleRepository.existsByVehicleIdAndOwner_UsernameAndActiveTrue("HONG-0001", "hong"))
            .thenReturn(true);
        client = new WebSocketStompClient(new StandardWebSocketClient());
        client.setMessageConverter(new StringMessageConverter());
        // 하트비트 없음(스케줄러 미설정) — "클라이언트가 아무것도 보내지 않는" 연결을 만든다.
    }

    @AfterEach
    void tearDown() {
        client.stop();
    }

    @Test
    void JWT만료시각에_서버가_연결을닫고_기존구독으로의_브로드캐스트가_끊긴다() throws Exception {
        String token = shortJwt.generateToken("hong");
        Instant expiresAt = shortJwt.getExpiration(token);
        Connection c = connect(token);
        c.subscribe(TOPIC);
        assertThat(c.deliver(broker, "before")).isEqualTo("before");

        // 클라이언트는 아무 프레임도 보내지 않는다. 서버 스케줄러(scheduleExpiration)만이 닫을 수 있다.
        long waitMs = Math.max(0, expiresAt.toEpochMilli() - System.currentTimeMillis()) + 1_500;
        assertThat(c.closed.poll(waitMs, TimeUnit.MILLISECONDS)).as("만료 뒤 서버 측 종료").isNotNull();
        assertThat(c.session.isConnected()).isFalse();

        broker.convertAndSend(TOPIC, "after");
        assertThat(c.received.poll(1, TimeUnit.SECONDS)).as("만료 뒤 브로드캐스트").isNull();
    }

    @Test
    void 비활성화는_기존구독전달을_멈추지않고_새SUBSCRIBE는_거부되어_연결이닫힌다() throws Exception {
        // 수명이 긴 토큰을 따로 만든다 — 만료 종료와 섞이지 않게.
        String token = new JwtTokenProvider(SECRET, 60_000).generateToken("hong");
        Connection c = connect(token);
        c.subscribe(TOPIC);
        assertThat(c.deliver(broker, "before")).isEqualTo("before");

        users.get("hong").setActive(false);

        // 한계: 이미 맺은 구독에는 계속 나간다(브로커는 프레임마다 묻지 않는다).
        assertThat(c.deliver(broker, "after-deactivation")).isEqualTo("after-deactivation");
        assertThat(c.session.isConnected()).isTrue();

        // 새 SUBSCRIBE는 거부된다 → 서버가 ERROR 프레임을 보내고 연결을 닫는다.
        c.subscribe("/topic/vehicle/HONG-0001/anomalies");
        assertThat(c.closed.poll(3, TimeUnit.SECONDS)).as("거부 뒤 서버 측 종료").isNotNull();
        assertThat(c.session.isConnected()).isFalse();
        broker.convertAndSend(TOPIC, "after-close");
        assertThat(c.received.poll(1, TimeUnit.SECONDS)).isNull();
    }

    // ---- helpers ----

    private Connection connect(String token) throws Exception {
        Connection c = new Connection();
        StompHeaders connectHeaders = new StompHeaders();
        connectHeaders.add("Authorization", "Bearer " + token);
        c.session = client.connectAsync("ws://localhost:" + port + "/ws", new WebSocketHttpHeaders(),
            connectHeaders, c.handler).get(5, TimeUnit.SECONDS);
        return c;
    }

    static final class Connection {
        final BlockingQueue<String> received = new LinkedBlockingQueue<>();
        final BlockingQueue<Throwable> closed = new LinkedBlockingQueue<>();
        StompSession session;

        final StompSessionHandlerAdapter handler = new StompSessionHandlerAdapter() {
            @Override
            public void handleTransportError(StompSession s, Throwable exception) {
                closed.add(exception);
            }
        };

        void subscribe(String destination) throws InterruptedException {
            session.subscribe(destination, new StompFrameHandler() {
                @Override public Type getPayloadType(StompHeaders headers) { return String.class; }
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    received.add((String) payload);
                }
            });
            // SUBSCRIBE는 응답이 없다. 브로커 등록까지 짧게 기다린다.
            Thread.sleep(300);
        }

        String deliver(SimpMessagingTemplate broker, String payload) throws InterruptedException {
            broker.convertAndSend(TOPIC, payload);
            return received.poll(3, TimeUnit.SECONDS);
        }
    }
}
