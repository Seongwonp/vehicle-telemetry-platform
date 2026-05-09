package com.telemetry.service;

import com.telemetry.dto.request.VehicleRegisterRequest;
import com.telemetry.dto.response.VehicleResponse;
import com.telemetry.entity.Vehicle;
import com.telemetry.repository.VehicleRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("VehicleService 단위 테스트")
class VehicleServiceTest {

    @Mock
    private VehicleRepository vehicleRepository;

    @InjectMocks
    private VehicleService vehicleService;

    @Test
    @DisplayName("차량 등록 성공")
    void register_성공() {
        // given
        VehicleRegisterRequest request = makeRequest("KR-GA-1234", "현대 아반떼", "홍길동");
        given(vehicleRepository.existsByVehicleId("KR-GA-1234")).willReturn(false);
        given(vehicleRepository.save(any(Vehicle.class)))
            .willAnswer(inv -> inv.getArgument(0));

        // when
        VehicleResponse result = vehicleService.register(request);

        // then
        assertThat(result.getVehicleId()).isEqualTo("KR-GA-1234");
        assertThat(result.getName()).isEqualTo("현대 아반떼");
        assertThat(result.getOwner()).isEqualTo("홍길동");
        assertThat(result.isActive()).isTrue();
        verify(vehicleRepository).save(any(Vehicle.class));
    }

    @Test
    @DisplayName("중복 차량 ID 등록 시 예외 발생")
    void register_중복ID_예외() {
        // given
        VehicleRegisterRequest request = makeRequest("KR-GA-1234", "아반떼", "홍길동");
        given(vehicleRepository.existsByVehicleId("KR-GA-1234")).willReturn(true);

        // when & then
        assertThatThrownBy(() -> vehicleService.register(request))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("이미 등록된 차량 ID");
    }

    @Test
    @DisplayName("활성 차량 목록 조회")
    void findAll_활성차량만_반환() {
        // given
        Vehicle v1 = new Vehicle("KR-GA-1234", "아반떼", "홍길동");
        Vehicle v2 = new Vehicle("KR-GA-5678", "소나타", "김철수");
        given(vehicleRepository.findAllByActiveTrue()).willReturn(List.of(v1, v2));

        // when
        List<VehicleResponse> result = vehicleService.findAll();

        // then
        assertThat(result).hasSize(2);
        assertThat(result).extracting(VehicleResponse::getVehicleId)
            .containsExactly("KR-GA-1234", "KR-GA-5678");
    }

    @Test
    @DisplayName("존재하지 않는 차량 ID 조회 시 예외")
    void findByVehicleId_없는차량_예외() {
        // given
        given(vehicleRepository.findByVehicleId("UNKNOWN")).willReturn(Optional.empty());

        // when & then
        assertThatThrownBy(() -> vehicleService.findByVehicleId("UNKNOWN"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("등록되지 않은 차량");
    }

    @Test
    @DisplayName("차량 비활성화 성공")
    void deactivate_성공() {
        // given
        Vehicle vehicle = new Vehicle("KR-GA-1234", "아반떼", "홍길동");
        given(vehicleRepository.findByVehicleId("KR-GA-1234")).willReturn(Optional.of(vehicle));

        // when
        vehicleService.deactivate("KR-GA-1234");

        // then
        assertThat(vehicle.isActive()).isFalse();
    }

    private VehicleRegisterRequest makeRequest(String vehicleId, String name, String owner) {
        VehicleRegisterRequest req = new VehicleRegisterRequest();
        req.setVehicleId(vehicleId);
        req.setName(name);
        req.setOwner(owner);
        return req;
    }
}
