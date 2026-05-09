package com.telemetry.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.dto.request.VehicleRegisterRequest;
import com.telemetry.dto.response.VehicleResponse;
import com.telemetry.entity.Vehicle;
import com.telemetry.service.VehicleService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(VehicleController.class)
@DisplayName("VehicleController 통합 테스트")
class VehicleControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private VehicleService vehicleService;

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
    @DisplayName("존재하지 않는 차량 조회 → 400 Bad Request")
    void findOne_없는차량_400() throws Exception {
        given(vehicleService.findByVehicleId("UNKNOWN"))
            .willThrow(new IllegalArgumentException("등록되지 않은 차량입니다: UNKNOWN"));

        mockMvc.perform(get("/api/vehicles/UNKNOWN"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }
}
