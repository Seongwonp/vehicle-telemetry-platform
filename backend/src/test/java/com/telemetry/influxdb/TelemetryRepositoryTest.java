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

    private VehicleTelemetry telemetry() {
        VehicleTelemetry telemetry = new VehicleTelemetry();
        telemetry.setVehicleId("SIM-001");
        telemetry.setTimestamp("2026-05-09T10:00:00Z");
        telemetry.setSpeed(80.0);
        telemetry.setRpm(2_000);
        telemetry.setEngineTemp(90.0);
        telemetry.setThrottlePosition(30.0);
        telemetry.setFuelLevel(50.0);
        telemetry.setBatteryVoltage(13.8);
        return telemetry;
    }
}
