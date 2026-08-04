package com.telemetry.influxdb;

import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.telemetry.domain.VehicleTelemetry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
@RequiredArgsConstructor
public class TelemetryRepository {

    private final WriteApiBlocking writeApi;

    /** 실제 InfluxDB 응답까지 기다린다. 정상 반환은 Kafka offset 커밋의 전제 조건이다. */
    public void save(VehicleTelemetry telemetry) {
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
            // WritePrecision.S(초 단위)였을 때는 PUBLISH_INTERVAL이 1초 미만이면 같은 차량의
            // 여러 포인트가 (측정값, 태그, 타임스탬프)가 같아져 뒤 포인트가 앞 포인트를 조용히
            // 덮어썼다 — 부하 테스트로 발견한 실데이터 유실 버그. 시뮬레이터가 이제 밀리초까지
            // 보내므로 정밀도를 맞춘다.
            .time(Instant.parse(telemetry.getTimestamp()), WritePrecision.MS);

        if (telemetry.getGps() != null) {
            point.addField("lat", telemetry.getGps().getLat())
                 .addField("lng", telemetry.getGps().getLng());
        }

        if (telemetry.getDtcCodes() != null && !telemetry.getDtcCodes().isEmpty()) {
            point.addField("dtc_codes", String.join(",", telemetry.getDtcCodes()));
        }

        writeApi.writePoint(point);
    }
}
