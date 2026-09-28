package com.telemetry.service;

import com.telemetry.dto.request.VehicleRegisterRequest;
import com.telemetry.dto.response.VehicleResponse;
import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.dto.response.FleetSummaryStatus;
import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.entity.Vehicle;
import com.telemetry.repository.UserRepository;
import com.telemetry.exception.ResourceConflictException;
import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.repository.AnomalyAlertRepository;
import com.telemetry.repository.VehicleRepository;
import com.telemetry.security.VehicleAccessService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;
import org.mockito.junit.jupiter.MockitoSettings;

import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.List;
import java.util.Optional;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("VehicleService 단위 테스트")
class VehicleServiceTest {

    @Mock
    private VehicleRepository vehicleRepository;

    @Mock
    private AnomalyAlertRepository anomalyAlertRepository;

    @Mock
    private TelemetryQueryService telemetryQueryService;

    @Mock
    private VehicleAccessService vehicleAccessService;
    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private VehicleService vehicleService;

    private static final Authentication ADMIN =
        new UsernamePasswordAuthenticationToken("admin", null,
            List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));
    private static final Authentication OWNER =
        new UsernamePasswordAuthenticationToken("hong", null, List.of());
    private static final User HONG = new User("hong", "x", Role.USER);
    private static final User KIM = new User("kim", "x", Role.USER);

    @org.junit.jupiter.api.BeforeEach
    void 관리자로_본다() {
        // 기존 테스트들은 관리자 맥락이다 — 소유자 제한은 아래 전용 테스트가 따로 본다.
        given(vehicleAccessService.isAdmin(ADMIN)).willReturn(true);
        given(userRepository.findByUsername("hong")).willReturn(Optional.of(HONG));
        given(userRepository.findByUsername("kim")).willReturn(Optional.of(KIM));
    }

    @Test
    @DisplayName("차량 등록 성공")
    void register_성공() {
        // given
        VehicleRegisterRequest request = makeRequest("KR-GA-1234", "현대 아반떼", "hong");
        given(vehicleRepository.existsByVehicleId("KR-GA-1234")).willReturn(false);
        given(vehicleRepository.save(any(Vehicle.class)))
            .willAnswer(inv -> inv.getArgument(0));

        // when
        VehicleResponse result = vehicleService.register(request, ADMIN);

        // then
        assertThat(result.getVehicleId()).isEqualTo("KR-GA-1234");
        assertThat(result.getName()).isEqualTo("현대 아반떼");
        assertThat(result.getOwner()).isEqualTo("hong");
        assertThat(result.isActive()).isTrue();
        verify(vehicleRepository).save(any(Vehicle.class));
    }

    @Test
    @DisplayName("중복 차량 ID 등록 시 예외 발생")
    void register_중복ID_예외() {
        // given
        VehicleRegisterRequest request = makeRequest("KR-GA-1234", "아반떼", "hong");
        given(vehicleRepository.existsByVehicleId("KR-GA-1234")).willReturn(true);

        // when & then
        assertThatThrownBy(() -> vehicleService.register(request, ADMIN))
            .isInstanceOf(ResourceConflictException.class)
            .hasMessageContaining("이미 등록된 차량 ID");
    }

    @Test
    @DisplayName("활성 차량 목록 조회")
    void findAll_활성차량만_반환() {
        // given
        Vehicle v1 = new Vehicle("KR-GA-1234", "아반떼", HONG);
        Vehicle v2 = new Vehicle("KR-GA-5678", "소나타", KIM);
        given(vehicleRepository.findAllByActiveTrue()).willReturn(List.of(v1, v2));
        given(anomalyAlertRepository.countHighByVehicleIds(List.of("KR-GA-1234", "KR-GA-5678")))
            .willReturn(List.of(highCount("KR-GA-1234", 3L)));
        given(telemetryQueryService.getLatestByVehicleIds(List.of("KR-GA-1234", "KR-GA-5678")))
            .willReturn(Map.of("KR-GA-1234", TelemetryResponse.builder()
                .vehicleId("KR-GA-1234").timestamp("2026-08-05T00:00:00Z").speed(80.0).build()));

        // when
        List<VehicleResponse> result = vehicleService.findAllVisibleTo(ADMIN);

        // then
        assertThat(result).hasSize(2);
        assertThat(result).extracting(VehicleResponse::getVehicleId)
            .containsExactly("KR-GA-1234", "KR-GA-5678");
        assertThat(result.get(0).getSummaryStatus()).isEqualTo(FleetSummaryStatus.OK);
        assertThat(result.get(1).getSummaryStatus()).isEqualTo(FleetSummaryStatus.NO_DATA);
        assertThat(result.get(0).getHighAnomalyCount()).isEqualTo(3L);
        assertThat(result.get(1).getHighAnomalyCount()).isZero();
        verify(anomalyAlertRepository, times(1)).countHighByVehicleIds(List.of("KR-GA-1234", "KR-GA-5678"));
        verify(telemetryQueryService, times(1))
            .getLatestByVehicleIds(List.of("KR-GA-1234", "KR-GA-5678"));
    }

    @Test
    @DisplayName("InfluxDB 장애 시 fleet 요약 상태를 UNAVAILABLE로 반환")
    void findAll_인프라장애_상태표시() {
        Vehicle vehicle = new Vehicle("KR-GA-1234", "아반떼", HONG);
        given(vehicleRepository.findAllByActiveTrue()).willReturn(List.of(vehicle));
        given(telemetryQueryService.getLatestByVehicleIds(List.of("KR-GA-1234")))
            .willThrow(new RuntimeException("InfluxDB down"));

        List<VehicleResponse> result = vehicleService.findAllVisibleTo(ADMIN);

        assertThat(result).singleElement()
            .extracting(VehicleResponse::getSummaryStatus)
            .isEqualTo(FleetSummaryStatus.UNAVAILABLE);
    }

    @Test
    @DisplayName("존재하지 않는 차량 ID 조회 시 예외")
    void findByVehicleId_없는차량_예외() {
        // given
        given(vehicleRepository.findByVehicleId("UNKNOWN")).willReturn(Optional.empty());

        // when & then
        assertThatThrownBy(() -> vehicleService.findByVehicleId("UNKNOWN"))
            .isInstanceOf(ResourceNotFoundException.class)
            .hasMessageContaining("등록되지 않은 차량");
    }

    @Test
    @DisplayName("차량 비활성화 성공")
    void deactivate_성공() {
        // given
        Vehicle vehicle = new Vehicle("KR-GA-1234", "아반떼", HONG);
        given(vehicleRepository.findByVehicleId("KR-GA-1234")).willReturn(Optional.of(vehicle));

        // when
        vehicleService.deactivate("KR-GA-1234");

        // then
        assertThat(vehicle.isActive()).isFalse();
    }

    @Test
    @DisplayName("일반 사용자는 자기 소유로도 등록하지 못한다 — 미등록 차량 ID 선점 경로를 막는다")
    void register_일반사용자_403() {
        VehicleRegisterRequest request = makeRequest("KR-GA-1234", "아반떼", "hong");

        assertThatThrownBy(() -> vehicleService.register(request, OWNER))
            .isInstanceOf(org.springframework.security.access.AccessDeniedException.class);
        verify(vehicleRepository, never()).save(any());
    }

    @Test
    @DisplayName("관리자가 소유자를 비우면 관리자 본인 소유가 된다")
    void register_소유자생략_관리자본인() {
        VehicleRegisterRequest request = makeRequest("KR-GA-1234", "아반떼", null);
        given(userRepository.findByUsername("admin")).willReturn(Optional.of(new User("admin", "x", Role.ADMIN)));
        given(vehicleRepository.save(any(Vehicle.class))).willAnswer(inv -> inv.getArgument(0));

        VehicleResponse result = vehicleService.register(request, ADMIN);

        assertThat(result.getOwner()).isEqualTo("admin");
    }

    @Test
    @DisplayName("존재하지 않는 사용자를 소유자로 지정하면 400 — 문자열 시절엔 오타가 조용히 들어갔다")
    void register_없는사용자_예외() {
        VehicleRegisterRequest request = makeRequest("KR-GA-1234", "아반떼", "ghost");
        given(userRepository.findByUsername("ghost")).willReturn(Optional.empty());

        assertThatThrownBy(() -> vehicleService.register(request, ADMIN))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("존재하지 않는 사용자");
        verify(vehicleRepository, never()).save(any());
    }

    private static AnomalyAlertRepository.HighCount highCount(String vehicleId, long count) {
        return new AnomalyAlertRepository.HighCount() {
            @Override public String getVehicleId() { return vehicleId; }
            @Override public long getCount() { return count; }
        };
    }

    private VehicleRegisterRequest makeRequest(String vehicleId, String name, String owner) {
        VehicleRegisterRequest req = new VehicleRegisterRequest();
        req.setVehicleId(vehicleId);
        req.setName(name);
        req.setOwner(owner);
        return req;
    }
}
