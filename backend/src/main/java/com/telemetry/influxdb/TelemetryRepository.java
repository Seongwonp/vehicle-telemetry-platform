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

    /**
     * TODO: 차량 수가 많아지면 메시지마다 단건 쓰기를 하는 현재 방식은 InfluxDB에 부하가 크다.
     *       배치 사이즈와 플러시 주기를 설정한 비동기 WriteApi로 교체를 검토해야 한다.
     */
    public void save(VehicleTelemetry telemetry) {
        try {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

            // vehicle_id는 tag로 설정한다. InfluxDB에서 tag는 자동으로 인덱싱되어
            // "특정 차량의 데이터만 조회"하는 쿼리가 field 필터보다 훨씬 빠르다.
            Point point = Point.measurement("vehicle_telemetry")
                .addTag("vehicle_id", telemetry.getVehicleId())
                .addField("speed", telemetry.getSpeed())
                .addField("rpm", (double) telemetry.getRpm())
                .addField("engine_temp", telemetry.getEngineTemp())
                .addField("throttle_position", telemetry.getThrottlePosition())
                .addField("fuel_level", telemetry.getFuelLevel())
                .addField("battery_voltage", telemetry.getBatteryVoltage())
                // timestamp는 시뮬레이터가 보낸 ISO-8601 문자열을 파싱한다.
                // 형식이 맞지 않으면 Instant.parse()에서 DateTimeParseException이 발생한다.
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
            log.error("[InfluxDB] 쓰기 실패 — vehicle={} timestamp={}",
                telemetry.getVehicleId(), telemetry.getTimestamp(), e);
            throw new RuntimeException("InfluxDB 저장 실패", e);
        }
    }
}
