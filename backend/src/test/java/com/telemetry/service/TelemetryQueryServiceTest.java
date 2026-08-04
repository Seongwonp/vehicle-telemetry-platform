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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyMap;
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

    @Test
    void queryUsesBoundParameters() {
        given(influxDBClient.getQueryApi()).willReturn(queryApi);
        given(queryApi.query(
            org.mockito.ArgumentMatchers.anyString(),
            eq("vehicle-telemetry"),
            anyMap()
        )).willReturn(List.of());

        service.getRecent("KR-GA-1234", 20);

        verify(queryApi).query(
            org.mockito.ArgumentMatchers.argThat(query ->
                query.contains("params.vehicleId") && !query.contains("KR-GA-1234")),
            eq("vehicle-telemetry"),
            eq(Map.of("bucket", "telemetry", "vehicleId", "KR-GA-1234", "limit", 20))
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
        given(queryApi.query(
            org.mockito.ArgumentMatchers.anyString(),
            eq("vehicle-telemetry"),
            anyMap()
        )).willReturn(List.of());

        assertThatThrownBy(() -> service.getLatest("KR-GA-1234"))
            .isInstanceOf(ResourceNotFoundException.class)
            .hasMessageContaining("텔레메트리 데이터가 없습니다");
    }
}
