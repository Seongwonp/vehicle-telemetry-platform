package com.telemetry.influxdb;

import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import com.telemetry.domain.VehicleTelemetry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TelemetryRepositoryTest {

    @Mock WriteApiBlocking writeApi;
    TelemetryRepository repository;

    @Test
    void waitsForBlockingWriteAndPropagatesFailure() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        repository = new TelemetryRepository(writeApi, meterRegistry);
        VehicleTelemetry telemetry = telemetry();
        repository.save(telemetry);
        verify(writeApi).writePoints(anyList());

        doThrow(new RuntimeException("InfluxDB down")).when(writeApi).writePoints(anyList());
        assertThatThrownBy(() -> repository.save(telemetry))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("InfluxDB down");
        assertThat(meterRegistry.get("telemetry.influx.write.failures").counter().count()).isEqualTo(1.0);
    }

    @Test
    void writesWholeBatchInSingleRequest() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        repository = new TelemetryRepository(writeApi, meterRegistry);

        List<Point> points = List.of(
            repository.toPoint(telemetry()),
            repository.toPoint(telemetry()),
            repository.toPoint(telemetry()));
        repository.saveAll(points);

        // 건당 요청이 아니라 배치 1회여야 한다 — InfluxDB WAL fsync 횟수를 줄이는 게 핵심.
        ArgumentCaptor<List<Point>> captor = ArgumentCaptor.forClass(List.class);
        verify(writeApi).writePoints(captor.capture());
        assertThat(captor.getValue()).hasSize(3);
        assertThat(meterRegistry.get("telemetry.influx.write.batch.size").summary().totalAmount())
            .isEqualTo(3.0);
    }

    @Test
    void skipsRequestWhenBatchIsEmpty() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        repository = new TelemetryRepository(writeApi, meterRegistry);

        // 배치 전체가 역직렬화 실패로 DLQ에 간 경우 — 빈 요청을 InfluxDB에 보내지 않는다.
        repository.saveAll(List.of());

        verify(writeApi, never()).writePoints(anyList());
    }

    @Test
    void 유한하지_않은_값은_필드가_사라지는_대신_예외가_된다() {
        // 실측에서 나온 테스트다. {"speed": 1e309}는 유효한 JSON이고 Jackson이 Infinity로
        // 파싱하는데, 쓰기가 **성공하면서 speed 필드만 조용히 빠진 채** 저장됐다
        // (DLQ 0, 에러 0, 카운터 변화 0 — load-test/poison-message/RESULT_20260906_poison.md).
        // 던져야 컨슈머의 레코드 단위 catch가 그 레코드만 DLQ로 보내 유실이 보이게 된다.
        repository = new TelemetryRepository(writeApi, new SimpleMeterRegistry());

        VehicleTelemetry infinite = telemetry();
        infinite.setSpeed(Double.POSITIVE_INFINITY);
        assertThatThrownBy(() -> repository.toPoint(infinite))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("speed");

        VehicleTelemetry nan = telemetry();
        nan.setEngineTemp(Double.NaN);
        assertThatThrownBy(() -> repository.toPoint(nan))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("engine_temp");

        // GPS도 같은 경로를 탄다 — 별도 블록이라 빠뜨리기 쉽다.
        VehicleTelemetry badGps = telemetry();
        VehicleTelemetry.GpsLocation gps = new VehicleTelemetry.GpsLocation();
        gps.setLat(Double.NEGATIVE_INFINITY);
        gps.setLng(127.0);
        badGps.setGps(gps);
        assertThatThrownBy(() -> repository.toPoint(badGps))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("lat");

        // 정상 값은 그대로 통과한다.
        assertThat(repository.toPoint(telemetry())).isNotNull();
    }

    @Test
    void 선택필드가_없으면_NPE없이_그_필드를_쓰지_않는다() {
        // ADR-030. 예전 toPoint는 finite(name, double)에 null을 언박싱해 NPE가 났다 — 계약은 통과한
        // 레코드가 저장 단계에서 DLQ로 가는 것이다. 그렇다고 0으로 쓰면 "누락이 0으로 저장되던" 결함이 돌아온다.
        repository = new TelemetryRepository(writeApi, new SimpleMeterRegistry());

        VehicleTelemetry both = telemetry();
        both.setFuelLevel(null);
        both.setBatteryVoltage(null);
        String line = repository.toPoint(both).toLineProtocol();
        assertThat(line)
            .doesNotContain("fuel_level")
            .doesNotContain("battery_voltage")
            .contains("speed=80.0")
            .contains("throttle_position=30.0");

        // 하나만 없을 때 — 있는 쪽은 그대로 쓴다.
        VehicleTelemetry noFuel = telemetry();
        noFuel.setFuelLevel(null);
        assertThat(repository.toPoint(noFuel).toLineProtocol())
            .doesNotContain("fuel_level")
            .contains("battery_voltage=13.8");

        VehicleTelemetry noBattery = telemetry();
        noBattery.setBatteryVoltage(null);
        assertThat(repository.toPoint(noBattery).toLineProtocol())
            .doesNotContain("battery_voltage")
            .contains("fuel_level=50.0");

        // 있으면 유한성 검사는 그대로다 — 선택이 "검사 생략"이 아니다.
        VehicleTelemetry infiniteFuel = telemetry();
        infiniteFuel.setFuelLevel(Double.POSITIVE_INFINITY);
        assertThatThrownBy(() -> repository.toPoint(infiniteFuel))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("fuel_level");
    }

    private VehicleTelemetry telemetry() {
        VehicleTelemetry telemetry = new VehicleTelemetry();
        telemetry.setVehicleId("SIM-001");
        telemetry.setTimestamp("2026-05-09T10:00:00Z");
        telemetry.setSpeed(80.0);
        telemetry.setRpm(2_000.0);
        telemetry.setEngineTemp(90.0);
        telemetry.setThrottlePosition(30.0);
        telemetry.setFuelLevel(50.0);
        telemetry.setBatteryVoltage(13.8);
        return telemetry;
    }
}
