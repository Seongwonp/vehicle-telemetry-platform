package com.telemetry.influxdb;

import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import com.telemetry.domain.VehicleTelemetry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
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
        verify(writeApi).writePoint(any(Point.class));

        doThrow(new RuntimeException("InfluxDB down")).when(writeApi).writePoint(any(Point.class));
        assertThatThrownBy(() -> repository.save(telemetry))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("InfluxDB down");
        org.assertj.core.api.Assertions.assertThat(
            meterRegistry.get("telemetry.influx.write.failures").counter().count()).isEqualTo(1.0);
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
