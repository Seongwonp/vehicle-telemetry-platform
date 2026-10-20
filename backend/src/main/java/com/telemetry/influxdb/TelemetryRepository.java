package com.telemetry.influxdb;

import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.telemetry.domain.VehicleTelemetry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public class TelemetryRepository {

    private final WriteApiBlocking writeApi;
    private final Counter writeFailureCounter;
    // 쓰기 지연과 배치 크기를 상시 계측한다. 경위: ADR-011
    private final Timer writeTimer;
    private final DistributionSummary batchSizeSummary;
    /**
     * 실제로 저장에 성공한 포인트 수. batchSizeSummary는 실패해도 오르므로 "시도"일 뿐이다.
     * 단계별 유입/저장 대조에 쓰인다(runbook/pipeline-observability.md, ADR-017).
     */
    private final Counter pointsWrittenCounter;

    public TelemetryRepository(WriteApiBlocking writeApi, MeterRegistry meterRegistry) {
        this.writeApi = writeApi;
        this.writeFailureCounter = meterRegistry.counter("telemetry.influx.write.failures");
        this.writeTimer = meterRegistry.timer("telemetry.influx.write");
        this.batchSizeSummary = meterRegistry.summary("telemetry.influx.write.batch.size");
        this.pointsWrittenCounter = meterRegistry.counter("telemetry.influx.points.written");
    }

    /**
     * 여러 포인트를 InfluxDB 요청 1건으로 동기 저장한다. 정상 반환이 Kafka offset 커밋의 전제다.
     * 요청마다 fsync가 드므로 묶어서 보낸다(ADR-011). 모든 포인트가 같은 precision이어야 1건으로 합쳐진다.
     */
    public void saveAll(List<Point> points) {
        if (points.isEmpty()) {
            return;
        }
        batchSizeSummary.record(points.size());
        Timer.Sample sample = Timer.start();
        try {
            writeApi.writePoints(points);
            // 성공한 뒤에만 센다.
            pointsWrittenCounter.increment(points.size());
        } catch (RuntimeException e) {
            writeFailureCounter.increment();
            throw e;
        } finally {
            sample.stop(writeTimer);
        }
    }

    public void save(VehicleTelemetry telemetry) {
        saveAll(List.of(toPoint(telemetry)));
    }

    /**
     * 배치 컨슈머가 레코드별 try/catch 안에서 부르도록 public이다.
     * 데이터 오류는 그 레코드만 DLQ로 보내야 하고, 배치 전체를 실패시키면 안 된다.
     */
    public Point toPoint(VehicleTelemetry telemetry) {
        // 태그는 vehicle_id만. 카디널리티가 높은 값을 태그로 넣으면 시리즈 인덱스가 무너진다.
        Point point = Point.measurement("vehicle_telemetry")
            .addTag("vehicle_id", telemetry.getVehicleId())
            .addField("speed", finite("speed", telemetry.getSpeed()))
            .addField("rpm", finite("rpm", telemetry.getRpm()))
            .addField("engine_temp", finite("engine_temp", telemetry.getEngineTemp()))
            .addField("throttle_position", finite("throttle_position", telemetry.getThrottlePosition()))
            // 밀리초 정밀도 필수: (measurement, tag, time)이 같으면 뒤 포인트가 앞을 조용히 덮어쓴다.
            // 그래서 차량당 1,000 msg/s부터 유실이 시작된다. 올려야 하면 시퀀스 태그가 아니라
            // WritePrecision.US로 간다. 경위: ADR-014, load-test/storage-integrity/RESULT_20260905_ms_collision.md
            .time(Instant.parse(telemetry.getTimestamp()), WritePrecision.MS);

        // 선택 필드(ADR-030)는 없으면 필드를 쓰지 않는다. 0으로 쓰면 실제 0과 구분할 수 없고,
        // null을 그대로 넘기면 언박싱 NPE로 정상 레코드가 DLQ로 간다.
        if (telemetry.getFuelLevel() != null) {
            point.addField("fuel_level", finite("fuel_level", telemetry.getFuelLevel()));
        }
        if (telemetry.getBatteryVoltage() != null) {
            point.addField("battery_voltage", finite("battery_voltage", telemetry.getBatteryVoltage()));
        }

        if (telemetry.getGps() != null) {
            point.addField("lat", finite("lat", telemetry.getGps().getLat()))
                 .addField("lng", finite("lng", telemetry.getGps().getLng()));
        }

        if (telemetry.getDtcCodes() != null && !telemetry.getDtcCodes().isEmpty()) {
            // 쉼표 구분 문자열로 접는다. null 원소·쉼표 포함 코드는 계약(VehicleTelemetry)이 막는다.
            point.addField("dtc_codes", String.join(",", telemetry.getDtcCodes()));
        }

        return point;
    }

    /**
     * Infinity/NaN이면 예외를 던진다. InfluxDB는 이런 값을 에러 없이 그 필드만 빼고 저장하므로,
     * 던져서 레코드 단위 catch가 DLQ로 보내게 한다(보이고 되돌릴 수 있는 유실).
     * NaN은 현재 JSON 입구로는 오지 않지만, 산술로 NaN이 생기는 경로를 대비해 남긴다.
     * 경위: load-test/poison-message/RESULT_20260906_poison.md, RESULT_20260908_nan_neginf.md
     */
    private static double finite(String field, double value) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException(
                "InfluxDB에 쓸 수 없는 값 — " + field + "=" + value
                    + " (유한하지 않은 값은 필드가 조용히 사라진다)");
        }
        return value;
    }
}
