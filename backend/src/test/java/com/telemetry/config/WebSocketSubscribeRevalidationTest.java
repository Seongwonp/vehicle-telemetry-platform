package com.telemetry.config;

import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.repository.UserRepository;
import com.telemetry.repository.VehicleRepository;
import com.telemetry.security.DbUserDetailsService;
import com.telemetry.security.JwtTokenProvider;
import com.telemetry.security.VehicleAccessService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.core.Authentication;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 열린 WebSocket 세션에서 새 SUBSCRIBE가 <b>현재</b> 계정·권한·차량 상태로 판정되는지.
 *
 * <p>CONNECT부터 같은 세션 속성·principal로 이어가며, 실제 {@link DbUserDetailsService}·
 * {@link VehicleAccessService}·{@link JwtTokenProvider}를 쓴다(저장소만 메모리 모형).
 * 이미 맺은 구독의 전달은 이 테스트 범위가 아니다 — {@code WebSocketExpiryIntegrationTest}.
 */
class WebSocketSubscribeRevalidationTest {

    private static final String SECRET = "test-secret-for-websocket-subscribe-revalidation-0123456789";

    /** username → (role, active). 없으면 삭제된 계정. */
    private final Map<String, User> users = new ConcurrentHashMap<>();
    /** vehicleId → owner username. */
    private final Map<String, String> vehicleOwners = new ConcurrentHashMap<>();
    private final Map<String, Boolean> vehicleActive = new ConcurrentHashMap<>();

    private final WebSocketSessionRegistry sessionRegistry = mock(WebSocketSessionRegistry.class);
    private final MessageChannel channel = mock(MessageChannel.class);
    private JwtTokenProvider jwt;
    private WebSocketAuthChannelInterceptor interceptor;

    @BeforeEach
    void setUp() {
        UserRepository userRepository = mock(UserRepository.class);
        when(userRepository.findByUsername(anyString()))
            .thenAnswer(inv -> Optional.ofNullable(users.get(inv.<String>getArgument(0))));
        VehicleRepository vehicleRepository = mock(VehicleRepository.class);
        when(vehicleRepository.existsByVehicleIdAndActiveTrue(anyString()))
            .thenAnswer(inv -> vehicleActive.getOrDefault(inv.<String>getArgument(0), false));
        when(vehicleRepository.existsByVehicleIdAndOwner_UsernameAndActiveTrue(anyString(), anyString()))
            .thenAnswer(inv -> vehicleActive.getOrDefault(inv.<String>getArgument(0), false)
                && inv.getArgument(1).equals(vehicleOwners.get(inv.<String>getArgument(0))));

        jwt = new JwtTokenProvider(SECRET, 60_000);
        interceptor = new WebSocketAuthChannelInterceptor(
            jwt, new DbUserDetailsService(userRepository), new VehicleAccessService(vehicleRepository),
            sessionRegistry);

        putUser("hong", Role.USER);
        putUser("kim", Role.USER);
        putUser("boss", Role.ADMIN);
        putVehicle("HONG-0001", "hong", true);
        putVehicle("KIM-0001", "kim", true);
        putVehicle("HONG-OLD1", "hong", false);
    }

    @Test
    void 같은세션_비활성화전_구독허용_비활성화후_새구독거부() {
        Session s = connect("hong", jwt);
        assertThatCode(() -> s.subscribe("HONG-0001")).doesNotThrowAnyException();

        users.get("hong").setActive(false);

        assertThatThrownBy(() -> s.subscribe("HONG-0001")).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void 같은세션_계정삭제후_새구독거부() {
        Session s = connect("hong", jwt);
        assertThatCode(() -> s.subscribe("HONG-0001")).doesNotThrowAnyException();

        users.remove("hong");

        assertThatThrownBy(() -> s.subscribe("HONG-0001")).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void 같은세션_JWT만료전_구독허용_만료후_프레임거부_세션종료요청() throws Exception {
        JwtTokenProvider shortJwt = new JwtTokenProvider(SECRET, 1_500);
        Session s = connect("hong", shortJwt);
        assertThatCode(() -> s.subscribe("HONG-0001")).doesNotThrowAnyException();

        // JWT exp는 초 단위로 잘린다. 세션 속성에 기록된 실제 만료 시각 뒤까지 기다린다.
        Instant expiresAt = (Instant) s.attributes.get("jwtExpiresAt");
        Thread.sleep(Math.max(0, expiresAt.toEpochMilli() - System.currentTimeMillis()) + 50);

        assertThatThrownBy(() -> s.subscribe("HONG-0001"))
            .isInstanceOf(AuthenticationCredentialsNotFoundException.class);
        verify(sessionRegistry).closeExpired(s.sessionId);
    }

    @Test
    void 강등된관리자_같은세션에서_남의차량_새구독거부_자기권한범위는유지() {
        putVehicle("BOSS-0001", "boss", true);
        Session s = connect("boss", jwt);
        assertThatCode(() -> s.subscribe("KIM-0001")).doesNotThrowAnyException();

        users.get("boss").setRole(Role.USER);

        assertThatThrownBy(() -> s.subscribe("KIM-0001")).isInstanceOf(AccessDeniedException.class);
        assertThatCode(() -> s.subscribe("BOSS-0001")).doesNotThrowAnyException();
    }

    @Test
    void 차량접근_소유자허용_비소유자거부_비활성차량거부() {
        Session hong = connect("hong", jwt);
        Session boss = connect("boss", jwt);

        assertThatCode(() -> hong.subscribe("HONG-0001")).doesNotThrowAnyException();
        assertThatThrownBy(() -> hong.subscribe("KIM-0001")).isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> hong.subscribe("HONG-OLD1")).isInstanceOf(AccessDeniedException.class);
        // 관리자도 비활성 차량은 못 본다(ADR-025).
        assertThatThrownBy(() -> boss.subscribe("HONG-OLD1")).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void 연결중_차량비활성화후_새구독거부() {
        Session s = connect("hong", jwt);
        assertThatCode(() -> s.subscribe("HONG-0001")).doesNotThrowAnyException();

        vehicleActive.put("HONG-0001", false);

        assertThatThrownBy(() -> s.subscribe("HONG-0001")).isInstanceOf(AccessDeniedException.class);
    }

    // ---- helpers ----

    private void putUser(String username, Role role) {
        users.put(username, new User(username, "$2a$10$hash", role));
    }

    private void putVehicle(String vehicleId, String owner, boolean active) {
        vehicleOwners.put(vehicleId, owner);
        vehicleActive.put(vehicleId, active);
    }

    private Session connect(String username, JwtTokenProvider provider) {
        Session s = new Session("session-" + username + "-" + System.nanoTime());
        StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.CONNECT);
        accessor.setNativeHeader("Authorization", "Bearer " + provider.generateToken(username));
        accessor.setSessionId(s.sessionId);
        accessor.setSessionAttributes(s.attributes);
        accessor.setLeaveMutable(true);
        Message<byte[]> connect = MessageBuilder.createMessage(new byte[0], accessor.getMessageHeaders());
        interceptor.preSend(connect, channel);
        // 서버의 StompSubProtocolHandler가 CONNECT에서 세운 user를 이후 프레임에 실어 보내는 것을 흉내 낸다.
        s.user = (Authentication) StompHeaderAccessor.wrap(connect).getUser();
        return s;
    }

    private final class Session {
        final String sessionId;
        final Map<String, Object> attributes = new HashMap<>();
        Authentication user;
        int subscriptions;

        Session(String sessionId) { this.sessionId = sessionId; }

        void subscribe(String vehicleId) {
            StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
            accessor.setDestination("/topic/vehicle/" + vehicleId + "/telemetry");
            accessor.setSessionId(sessionId);
            accessor.setSubscriptionId("sub-" + subscriptions++);
            accessor.setUser(user);
            accessor.setSessionAttributes(attributes);
            accessor.setLeaveMutable(true);
            interceptor.preSend(MessageBuilder.createMessage(new byte[0], accessor.getMessageHeaders()), channel);
        }
    }
}
