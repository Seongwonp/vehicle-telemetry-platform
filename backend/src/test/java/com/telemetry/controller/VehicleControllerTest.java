package com.telemetry.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.dto.request.VehicleRegisterRequest;
import com.telemetry.dto.response.VehicleResponse;
import com.telemetry.entity.Vehicle;
import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.service.VehicleService;
import com.telemetry.security.ClientIpResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * <p><b>왜 지표 자동 구성을 따로 넣는가</b>: 이 슬라이스는 {@code WebMvcConfig}가 등록한
 * 인터셉터를 함께 띄우는데, {@code RateLimitInterceptor}는 fail-open 여부를 세려고
 * {@code MeterRegistry}를 <b>필수로</b> 받는다. 슬라이스에는 Micrometer 자동 구성이 없어
 * 컨텍스트가 안 뜬다.
 *
 * <p>여기서 인터셉터 쪽을 {@code ObjectProvider}로 무르게 만들지 <b>않았다</b> —
 * {@code GlobalExceptionHandler}와 판단이 갈린다. 예외 처리는 지표가 없어도 살아야 하지만,
 * <b>fail-open 카운터가 없는 fail-open은 만들면 안 되는 물건</b>이다(조용히 제한이 사라진다).
 * 그래서 운영 코드는 빈이 없으면 <b>기동 단계에서 실패하게</b> 두고, 테스트가 운영과 같은
 * 자동 구성을 올린다. {@code docs/redis-failure-policy.md} §7.
 */
@WebMvcTest(VehicleController.class)
@ImportAutoConfiguration({MetricsAutoConfiguration.class, SimpleMetricsExportAutoConfiguration.class})
@DisplayName("VehicleController 통합 테스트")
class VehicleControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private VehicleService vehicleService;

    // WebMvcConfig가 등록한 RateLimitInterceptor가 이 슬라이스 테스트에도 함께 실행되므로
    // StringRedisTemplate을 채워주지 않으면 opsForValue() 호출에서 NPE가 난다.
    @MockBean
    private StringRedisTemplate redisTemplate;

    @MockBean
    private ValueOperations<String, String> valueOperations;

    @MockBean
    private ClientIpResolver clientIpResolver;

    @BeforeEach
    void setUpRateLimit() {
        given(redisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.increment(anyString())).willReturn(1L);
        given(clientIpResolver.resolve(any())).willReturn("127.0.0.1");
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    @DisplayName("차량 등록 API 성공 → 201 Created")
    void register_성공_201() throws Exception {
        VehicleRegisterRequest request = new VehicleRegisterRequest();
        request.setVehicleId("KR-GA-1234");
        request.setName("현대 아반떼");
        request.setOwner("홍길동");

        VehicleResponse response = new VehicleResponse(new Vehicle("KR-GA-1234", "현대 아반떼", "홍길동"));
        given(vehicleService.register(any())).willReturn(response);

        mockMvc.perform(post("/api/vehicles")
                .with(csrf())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.vehicleId").value("KR-GA-1234"))
            .andExpect(jsonPath("$.name").value("현대 아반떼"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    @DisplayName("차량 등록 — vehicleId 빈 값 → 400 Bad Request")
    void register_빈vehicleId_400() throws Exception {
        VehicleRegisterRequest request = new VehicleRegisterRequest();
        request.setVehicleId("");   // 유효성 검사 실패
        request.setName("아반떼");
        request.setOwner("홍길동");

        mockMvc.perform(post("/api/vehicles")
                .with(csrf())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(request)))
            .andExpect(status().isBadRequest());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    @DisplayName("차량 목록 조회 → 200 OK")
    void findAll_200() throws Exception {
        given(vehicleService.findAll()).willReturn(List.of(
            new VehicleResponse(new Vehicle("KR-GA-1234", "아반떼", "홍길동")),
            new VehicleResponse(new Vehicle("KR-GA-5678", "소나타", "김철수"))
        ));

        mockMvc.perform(get("/api/vehicles"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    @DisplayName("인증 없이 접근 → 401 Unauthorized")
    void 인증없이_401() throws Exception {
        mockMvc.perform(get("/api/vehicles"))
            .andExpect(status().isUnauthorized());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    @DisplayName("존재하지 않는 차량 조회 → 404 Not Found")
    void findOne_없는차량_404() throws Exception {
        given(vehicleService.findByVehicleId("UNKNOWN"))
            .willThrow(new ResourceNotFoundException("등록되지 않은 차량입니다: UNKNOWN"));

        mockMvc.perform(get("/api/vehicles/UNKNOWN"))
            .andExpect(status().isNotFound())
            .andExpect(jsonPath("$.code").value("NOT_FOUND"));
    }
}
