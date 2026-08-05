package com.telemetry.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.telemetry.exception.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TelemetryQueryServiceTest {

    @Mock
    private InfluxDBClient influxDBClient;

    @Mock
    private QueryApi queryApi;

    private TelemetryQueryService service;

    @BeforeEach
    void setUp() {
        service = new TelemetryQueryService(influxDBClient);
        ReflectionTestUtils.setField(service, "bucket", "telemetry");
        ReflectionTestUtils.setField(service, "influxOrg", "vehicle-telemetry");
    }

    // InfluxDB 2.7 서버는 클라이언트의 params.xxx 바인딩 문법을 지원하지 않는다
    // ("undefined identifier params" 컴파일 에러) — vehicleId는 이미
    // validateVehicleId()로 ^[A-Z0-9-]{4,20}$ 형식만 통과시킨 뒤라 문자열
    // 삽입이 안전해서 검증된 값을 직접 Flux 문자열에 넣는 방식으로 되돌렸다.
    // 아래 테스트는 그 방식이 실제로 쿼리를 만든다는 것과, 애초에 형식이 안
    // 맞는 입력은 쿼리 문자열을 만들기도 전에 걸러진다는 것을 함께 검증한다.
    @Test
    void queryEmbedsValidatedVehicleId() {
        given(influxDBClient.getQueryApi()).willReturn(queryApi);
        given(queryApi.query(anyString(), eq("vehicle-telemetry"))).willReturn(List.of());

        service.getRecent("KR-GA-1234", 20);

        verify(queryApi).query(
            org.mockito.ArgumentMatchers.<String>argThat(query ->
                query.contains("r.vehicle_id == \"KR-GA-1234\"")
                    && query.contains("from(bucket: \"telemetry\")")
                    && query.contains("limit(n: 20)")),
            eq("vehicle-telemetry")
        );
    }

    @Test
    void rejectsFluxInjectionPayloadAsVehicleId() {
        assertThatThrownBy(() -> service.getRecent("X\" or true", 20))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("차량 ID 형식");
    }

    @Test
    void latestTelemetryNotFoundUsesDomainException() {
        given(influxDBClient.getQueryApi()).willReturn(queryApi);
        given(queryApi.query(anyString(), eq("vehicle-telemetry"))).willReturn(List.of());

        assertThatThrownBy(() -> service.getLatest("KR-GA-1234"))
            .isInstanceOf(ResourceNotFoundException.class)
            .hasMessageContaining("텔레메트리 데이터가 없습니다");
    }
}
