package com.telemetry.config;

import com.telemetry.controller.VehicleController;
import com.telemetry.security.ClientIpResolver;
import com.telemetry.security.VehicleAccessService;
import com.telemetry.service.VehicleService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * fail-open이 <b>rate limit만</b> 생략하는지 고정한다.
 *
 * <h3>왜 이 테스트가 따로 필요한가</h3>
 *
 * {@code RateLimitFailOpenTest}는 인터셉터를 <b>직접 호출</b>해서 통과 여부만 본다.
 * 그건 "인터셉터가 true를 돌려준다"까지이고 <b>요청이 실제로 처리된다</b>는 뜻이 아니다.
 * 여기서는 {@code MockMvc}로 <b>보안 필터 체인을 포함한 전체 경로</b>를 통과시켜,
 * Redis가 없을 때 <b>인증·인가가 그대로 살아 있는지</b>를 본다.
 *
 * <p><b>fail-open이 열어준 것은 "제한을 세지 못했을 때의 통과"뿐이다.</b>
 * 인증까지 열렸다면 그건 fail-open이 아니라 인증 우회다.
 * ({@code docs/redis-failure-policy.md} §8-1)
 *
 * <p>구조적으로는 Spring Security 필터가 {@code HandlerInterceptor}보다 <b>앞</b>이라
 * 미인증 요청은 인터셉터에 닿지도 않는다. 그 구조에 기대는 대신 여기서 잰다 —
 * 인터셉터 등록 순서나 필터 설정이 바뀌면 조용히 깨질 수 있는 전제이기 때문이다.
 */
@WebMvcTest(VehicleController.class)
@ImportAutoConfiguration({MetricsAutoConfiguration.class, SimpleMetricsExportAutoConfiguration.class})
@DisplayName("Redis 장애 중에도 인증·인가는 유지된다")
class FailOpenKeepsSecurityTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private VehicleService vehicleService;
    @MockBean
    private StringRedisTemplate redisTemplate;
    @MockBean
    private ValueOperations<String, String> valueOperations;
    @MockBean
    private ClientIpResolver clientIpResolver;
    // WebMvcConfig의 VehicleAccessInterceptor가 이 슬라이스에도 뜬다.
    @MockBean
    private VehicleAccessService vehicleAccessService;

    @BeforeEach
    void redisIsDown() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.increment(anyString()))
            .willThrow(new RedisConnectionFailureException("Unable to connect to Redis"));
        given(clientIpResolver.resolve(any())).willReturn("10.0.0.1");
    }

    @Test
    @DisplayName("미인증 요청은 Redis가 없어도 여전히 거부된다")
    void 미인증은_여전히_거부() throws Exception {
        mockMvc.perform(get("/api/vehicles"))
            .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("인증된 요청은 rate limit 없이 정상 처리된다 — 이게 fail-open이 산 것이다")
    void 인증된_요청은_통과() throws Exception {
        given(vehicleService.findAllVisibleTo(any())).willReturn(List.of());

        mockMvc.perform(get("/api/vehicles").with(
                org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors
                    .user("tester").roles("ADMIN")))
            .andExpect(status().isOk())
            // 세지 못했으므로 남은 횟수를 적지 않는다. 숫자를 적으면 거짓말이 된다.
            .andExpect(header().doesNotExist("X-RateLimit-Remaining"));
    }

    /**
     * <b>인가 규칙이 그대로인지</b>를 "Redis를 건드리지도 않는다"로 본다.
     *
     * <p>이 저장소의 인가 규칙은 현재 {@code anyRequest().authenticated()}뿐이다 —
     * {@code /api/vehicles}에 역할 제한이 없어서 "권한 부족"을 만들 수 있는 요청이 없다.
     * 그래서 인가를 <b>응답 코드로</b> 재는 대신 <b>순서</b>를 잰다:
     * 보안 필터가 인터셉터보다 앞이면 거부된 요청은 rate limit 코드에 닿지 않는다.
     *
     * <p>순서가 뒤집히면 fail-open 경로가 <b>인증 전에</b> 도는 셈이 되어, 언젠가
     * 인터셉터에서 사용자 기준 판단을 하게 될 때 조용히 무너진다.
     */
    @Test
    @DisplayName("거부된 요청은 rate limit 코드에 닿지도 않는다 — 보안 필터가 앞이다")
    void 미인증은_인터셉터에_닿지_않는다() throws Exception {
        mockMvc.perform(get("/api/vehicles"))
            .andExpect(status().isUnauthorized());

        org.mockito.Mockito.verify(redisTemplate, org.mockito.Mockito.never()).opsForValue();
    }
}
