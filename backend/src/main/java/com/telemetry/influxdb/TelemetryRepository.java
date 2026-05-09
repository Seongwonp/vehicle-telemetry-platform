package com.telemetry.influxdb;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.telemetry.domain.VehicleTelemetry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Slf4j
@Repository
@RequiredArgsConstructor
public class TelemetryRepository {

    private final InfluxDBClient influxDBClient;

    public void save(VehicleTelemetry telemetry) {
        try {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

            Point point = Point.measurement("vehicle_telemetry")
                .addTag("vehicle_id", telemetry.getVehicleId())    // 태그: 인덱싱됨 (쿼리 필터용)
                .addField("speed", telemetry.getSpeed())
                .addField("rpm", (double) telemetry.getRpm())
                .addField("engine_temp", telemetry.getEngineTemp())
                .addField("throttle_position", telemetry.getThrottlePosition())
                .addField("fuel_level", telemetry.getFuelLevel())
                .addField("battery_voltage", telemetry.getBatteryVoltage())
                .time(Instant.parse(telemetry.getTimestamp()), WritePrecision.S);

            if (telemetry.getGps() != null) {
                point.addField("lat", telemetry.getGps().getLat())
                     .addField("lng", telemetry.getGps().getLng());
            }

            if (telemetry.getDtcCodes() != null && !telemetry.getDtcCodes().isEmpty()) {
                point.addField("dtc_codes", String.join(",", telemetry.getDtcCodes()));
            }

            writeApi.writePoint(point);

        } catch (Exception e) {
            log.error("[InfluxDB] 쓰기 실패 vehicle={} timestamp={}",
                telemetry.getVehicleId(), telemetry.getTimestamp(), e);
            throw new RuntimeException("InfluxDB 저장 실패", e);
        }
    }
}
