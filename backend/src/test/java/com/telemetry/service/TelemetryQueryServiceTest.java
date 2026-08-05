package com.telemetry.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.repository.VehicleRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.List;
import java.time.Instant;
import java.util.stream.IntStream;

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

    @Mock
    private VehicleRepository vehicleRepository;

    private TelemetryQueryService service;

    @BeforeEach
    void setUp() {
        service = new TelemetryQueryService(influxDBClient, vehicleRepository, new SimpleMeterRegistry());
        ReflectionTestUtils.setField(service, "bucket", "telemetry");
        ReflectionTestUtils.setField(service, "influxOrg", "vehicle-telemetry");
        ReflectionTestUtils.setField(service, "defaultTripHours", 1);
        ReflectionTestUtils.setField(service, "tripPointLimit", 10_000);
        ReflectionTestUtils.setField(service, "tripGapThreshold", java.time.Duration.ofMinutes(3));
        ReflectionTestUtils.setField(service, "tripMinPoints", 3);
        ReflectionTestUtils.setField(service, "maxGpsSpeedKmh", 250.0);
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

    @Test
    void fleetLatestUsesOneGroupedFluxQuery() {
        given(influxDBClient.getQueryApi()).willReturn(queryApi);
        given(queryApi.query(anyString(), eq("vehicle-telemetry"))).willReturn(List.of());

        service.getLatestByVehicleIds(List.of("KR-GA-1234", "KR-GA-5678"));

        verify(queryApi).query(
            org.mockito.ArgumentMatchers.<String>argThat(query ->
                query.contains("contains(value: r.vehicle_id")
                    && query.contains("\"KR-GA-1234\"")
                    && query.contains("\"KR-GA-5678\"")
                    && query.contains("group(columns: [\"vehicle_id\"])")
                    && query.contains("limit(n: 1)")),
            eq("vehicle-telemetry")
        );
    }

    @Test
    void gapExactlyThreeMinutesStaysInSameTripAndLongerGapSplits() {
        List<TelemetryResponse> points = List.of(
            point("2026-08-05T00:00:00Z", 10.0, null, null),
            point("2026-08-05T00:03:00Z", 20.0, null, null),
            point("2026-08-05T00:06:00Z", 30.0, null, null),
            point("2026-08-05T00:09:01Z", 40.0, null, null),
            point("2026-08-05T00:10:01Z", 50.0, null, null),
            point("2026-08-05T00:11:01Z", 60.0, null, null)
        );

        var trips = service.segmentIntoTrips(points);

        org.assertj.core.api.Assertions.assertThat(trips).hasSize(2);
        org.assertj.core.api.Assertions.assertThat(trips.get(0).getPointCount()).isEqualTo(3);
        org.assertj.core.api.Assertions.assertThat(trips.get(1).getPointCount()).isEqualTo(3);
    }

    @Test
    void segmentsWithFewerThanConfiguredPointsAreDropped() {
        var trips = service.segmentIntoTrips(List.of(
            point("2026-08-05T00:00:00Z", 10.0, null, null),
            point("2026-08-05T00:01:00Z", 20.0, null, null)
        ));

        org.assertj.core.api.Assertions.assertThat(trips).isEmpty();
    }

    @Test
    void averageSpeedIsWeightedByElapsedTime() {
        var trip = service.segmentIntoTrips(List.of(
            point("2026-08-05T00:00:00Z", 0.0, null, null),
            point("2026-08-05T00:01:00Z", 100.0, null, null),
            point("2026-08-05T00:03:00Z", 100.0, null, null)
        )).get(0);

        org.assertj.core.api.Assertions.assertThat(trip.getAvgSpeedKmh()).isEqualTo(83.3);
    }

    @Test
    void missingOrImplausibleGpsSegmentsDoNotInflateDistance() {
        var missingGps = service.segmentIntoTrips(List.of(
            point("2026-08-05T00:00:00Z", 10.0, 37.0, 127.0),
            point("2026-08-05T00:01:00Z", 10.0, null, null),
            point("2026-08-05T00:02:00Z", 10.0, 37.0001, 127.0001)
        )).get(0);
        var gpsJump = service.segmentIntoTrips(List.of(
            point("2026-08-05T00:00:00Z", 10.0, 37.0, 127.0),
            point("2026-08-05T00:01:00Z", 10.0, 0.0, 0.0),
            point("2026-08-05T00:02:00Z", 10.0, 37.0001, 127.0001)
        )).get(0);

        org.assertj.core.api.Assertions.assertThat(missingGps.getDistanceKm()).isZero();
        org.assertj.core.api.Assertions.assertThat(gpsJump.getDistanceKm()).isZero();
    }

    @Test
    void segmentationHandlesMoreThanTenThousandPoints() {
        Instant start = Instant.parse("2026-08-05T00:00:00Z");
        List<TelemetryResponse> points = IntStream.range(0, 10_001)
            .mapToObj(i -> point(start.plusSeconds(i).toString(), 30.0, null, null))
            .toList();

        var trips = service.segmentIntoTrips(points);

        org.assertj.core.api.Assertions.assertThat(trips).singleElement()
            .extracting(com.telemetry.dto.response.TripResponse::getPointCount)
            .isEqualTo(10_001);
    }

    @Test
    void unregisteredVehicleTripReturnsNotFound() {
        given(vehicleRepository.existsByVehicleIdAndActiveTrue("KR-GA-1234")).willReturn(false);

        assertThatThrownBy(() -> service.getTrips("KR-GA-1234", 1))
            .isInstanceOf(ResourceNotFoundException.class)
            .hasMessageContaining("등록되지 않은 차량");
    }

    private TelemetryResponse point(String timestamp, Double speed, Double lat, Double lng) {
        return TelemetryResponse.builder()
            .vehicleId("KR-GA-1234")
            .timestamp(timestamp)
            .speed(speed)
            .lat(lat)
            .lng(lng)
            .build();
    }
}
