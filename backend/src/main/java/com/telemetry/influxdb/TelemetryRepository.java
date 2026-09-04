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
    // 부하 테스트에서 "건당 310ms"의 정체를 추측으로만 좁히다 두 번 빗나갔다(InfluxDB 타임아웃
    // 설정 → Kafka 재시도 폭주 → 실제로는 요청당 fsync). 다음엔 숫자로 바로 답할 수 있도록
    // 쓰기 소요 시간과 실제 배치 크기를 상시 계측한다.
    private final Timer writeTimer;
    private final DistributionSummary batchSizeSummary;
    /**
     * <b>실제로 저장에 성공한</b> 포인트 수. batchSizeSummary는 saveAll 진입 시점에
     * 기록되므로 쓰기가 실패해도 올라간다 — 즉 "저장 시도"이지 "저장 성공"이 아니다.
     * 파이프라인 단계별 유입/저장을 대조하려면 성공량이 정확해야 한다:
     * 12시간 soak에서 InfluxDB 쓰기가 26초 만에 멈췄는데 Kafka lag은 끝까지 0이라
     * 정상으로 보였던 사고가 바로 이 대조가 없어서 안 보인 것이다(ADR-017).
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
     * 여러 포인트를 InfluxDB 요청 1건으로 쓴다. 실제 응답까지 기다리므로 정상 반환은
     * Kafka offset 커밋의 전제 조건이다.
     *
     * <p>단건 {@code writePoint()}로 메시지마다 HTTP 요청을 보내던 구조에서는 요청 하나가
     * InfluxDB의 WAL fsync 한 번을 유발해, ~2,400 msg/s 부하에서 처리량이 8 msg/s까지
     * 무너졌다(InfluxDB CPU는 1.66%로 놀고 있는데 쓰기만 느린 I/O 대기 패턴이었다).
     * 모든 포인트가 같은 {@link WritePrecision#MS}라 클라이언트 내부 precision 그룹핑에서도
     * 요청 1건으로 합쳐진다.
     */
    public void saveAll(List<Point> points) {
        if (points.isEmpty()) {
            return;
        }
        batchSizeSummary.record(points.size());
        Timer.Sample sample = Timer.start();
        try {
            writeApi.writePoints(points);
            // 예외 없이 돌아온 뒤에만 센다 — 이 카운터가 파이프라인 대조의 마지막 단계다.
            pointsWrittenCounter.increment(points.size());
        } catch (RuntimeException e) {
            writeFailureCounter.increment();
            throw e;
        } finally {
            sample.stop(writeTimer);
        }
    }

    /** 단건 저장 — 배치 경로({@link #saveAll})에 위임한다. */
    public void save(VehicleTelemetry telemetry) {
        saveAll(List.of(toPoint(telemetry)));
    }

    /**
     * 텔레메트리를 InfluxDB 포인트로 변환한다.
     *
     * <p>배치 컨슈머가 레코드별 try/catch 안에서 직접 호출할 수 있도록 public이다 —
     * 여기서 발생하는 {@link java.time.format.DateTimeParseException} 같은 데이터 오류는
     * 그 레코드 하나만 DLQ로 보내야지, 배치 전체를 실패시키면 정상 메시지까지 재시도된다.
     */
    public Point toPoint(VehicleTelemetry telemetry) {
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

        return point;
    }
}
