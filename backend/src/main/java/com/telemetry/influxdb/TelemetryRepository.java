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
            .addField("speed", finite("speed", telemetry.getSpeed()))
            // rpm은 int라 finite() 검사가 필요 없다 — 정수는 Infinity/NaN이 될 수 없고,
            // 범위를 넘는 값(1e309, 99999999999)은 **역직렬화 단계에서** Jackson이 막는다
            // (2026-09-08 실측, load-test/poison-message/RESULT_20260908_rpm_int.md).
            // 같은 1e309가 speed(double)에서는 파싱에 성공해 아래 finite()까지 온다 —
            // **막는 문이 타입에 따라 다르다.**
            //
            // 다만 `"rpm": 2000.7`은 아무 데서도 안 걸리고 **2000으로 잘려 저장된다**
            // (Jackson ACCEPT_FLOAT_AS_INT 기본 활성). DLQ·에러·카운터 전부 0이다.
            // 손실이 1 rpm 미만이고 이상 감지는 원본 JSON을 보므로 고치지 않기로 했다.
            // **주의**: 저장 데이터로 룰을 다시 돌리면 6000.9가 6000이라 안 걸린다.
            .addField("rpm", (double) telemetry.getRpm())
            .addField("engine_temp", finite("engine_temp", telemetry.getEngineTemp()))
            .addField("throttle_position", finite("throttle_position", telemetry.getThrottlePosition()))
            .addField("fuel_level", finite("fuel_level", telemetry.getFuelLevel()))
            .addField("battery_voltage", finite("battery_voltage", telemetry.getBatteryVoltage()))
            // timestamp는 시뮬레이터가 보낸 ISO-8601 문자열을 파싱한다.
            // 형식이 맞지 않으면 Instant.parse()에서 DateTimeParseException이 발생한다.
            // WritePrecision.S(초 단위)였을 때는 PUBLISH_INTERVAL이 1초 미만이면 같은 차량의
            // 여러 포인트가 (측정값, 태그, 타임스탬프)가 같아져 뒤 포인트가 앞 포인트를 조용히
            // 덮어썼다 — 부하 테스트로 발견한 실데이터 유실 버그. 시뮬레이터가 이제 밀리초까지
            // 보내므로 정밀도를 맞춘다.
            //
            // ★ 발행 주기를 줄이려면 여기부터 보라. 이 identity는 **차량당 1,000 msg/s**에서
            // 무너진다(밀리초당 1건). 실측 임계값이다:
            //   500 msg/s → 유실 0.02% | 1,000 → 0.64% | 2,000 → 50.20%
            // 그 위로는 대략 (1 - 1000/R)만큼 조용히 사라진다. 에러도 로그도 없고,
            // MQTT·Kafka·InfluxDB 쓰기가 전부 성공으로 보인다 — 행 수를 직접 세야 보인다
            // (같은 타임스탬프 500건 발행 → 1행: load-test/storage-integrity/
            //  RESULT_20260905_ms_collision.md).
            //
            // 현재는 차량당 5 msg/s라 200배 여유가 있어 그대로 둔다. 총량은 차량 수로
            // 늘리므로(vehicle_id가 태그라 identity가 갈린다) 총 처리량과는 무관하다.
            // 차량당 속도를 올려야 한다면 시퀀스 태그가 아니라 WritePrecision.US로 가라 —
            // 태그는 포인트마다 시리즈를 하나씩 만들어 인덱스를 무너뜨린다.
            .time(Instant.parse(telemetry.getTimestamp()), WritePrecision.MS);

        if (telemetry.getGps() != null) {
            point.addField("lat", finite("lat", telemetry.getGps().getLat()))
                 .addField("lng", finite("lng", telemetry.getGps().getLng()));
        }

        if (telemetry.getDtcCodes() != null && !telemetry.getDtcCodes().isEmpty()) {
            // **배열을 문자열로 접는 유일한 자리다. 여기서 두 가지가 샌다**
            // (2026-09-09 실측, load-test/poison-message/RESULT_20260909_dtc_codes.md):
            //   1. null 원소는 String.join이 **"null"이라는 네 글자**로 만든다(NPE가 아니다).
            //   2. 코드 자체에 쉼표가 있으면 구분자와 충돌해, 저장된 문자열만으로는
            //      코드 1개인지 2개인지 **되돌릴 수 없다**.
            // 둘 다 DLQ·에러·카운터에 아무 신호가 없다.
            //
            // 고치지 않기로 했다 — 값이 사라지지 않고(형식이 안 맞아 눈에 띈다),
            // 쉼표는 스펙(`[PBCU]\d{4}`)을 지키는 입력에서 나올 수 없으며,
            // 이상 감지는 원본 JSON을 보므로 영향이 없다. 구분자를 바꾸는 것은
            // 저장 포맷 계약을 바꾸는 일이라 대가가 더 크다.
            //
            // **다시 볼 조건**: 실제 장치에서 null 원소나 쉼표 포함 코드가 한 번이라도
            // 관찰되면 이 결정을 뒤집고 finite()처럼 예외를 던진다.
            // 저장된 dtc_codes를 다시 쪼개 세는 코드를 쓸 사람은 위 두 경우를 알고 써야 한다.
            point.addField("dtc_codes", String.join(",", telemetry.getDtcCodes()));
        }

        return point;
    }

    /**
     * 유한하지 않은 값(Infinity, NaN)이면 예외를 던진다.
     *
     * <p><b>이 검사가 없을 때 무슨 일이 있었나:</b> {@code {"speed": 1e309}}는 유효한
     * JSON이고 Jackson이 {@code Double.POSITIVE_INFINITY}로 파싱한다. 쓰기가 실패할 것으로
     * 예상했는데 실제로는 <b>성공했고, speed 필드만 빠진 채 저장됐다</b> — DLQ 0, 에러 로그 0,
     * 카운터 변화 0. 행은 남으므로 정합성 대조(토픽 수 = 행 수)로도 안 잡힌다.
     * 레코드 단위 유실만 세는 지금 방식이 <b>필드 단위 유실은 못 본다</b>
     * ({@code load-test/poison-message/RESULT_20260906_poison.md}).
     *
     * <p><b>왜 건너뛰지 않고 던지나:</b> 던지면 컨슈머의 레코드 단위 catch가 그 레코드만
     * DLQ로 보낸다 — 유실이 <b>보이고 되돌릴 수 있는</b> 형태가 된다. 필드만 빼고 저장하면
     * 아무 신호 없이 값이 사라진다.
     *
     * <p><b>비용:</b> 포인트당 비교 8회다. 같은 포인트를 쓰는 HTTP 요청 하나가 이 호스트에서
     * 약 0.8초(요청당 고정비, 배치 크기와 거의 무관)라 측정 가능한 수준이 아니다.
     * 2026-09-06까지는 "발생 가능성을 확인하지 않은 채 모든 메시지에 비용을 물릴지"를
     * 판단하지 못해 미결정으로 뒀는데, 비용 쪽이 이 정도면 판단할 것이 없다.
     *
     * <p><b>어떤 값이 실제로 여기까지 오는가(2026-09-08 실측):</b>
     * {@code 1e309}와 {@code -1e309}는 온다 — 유효한 JSON이고 Jackson이 각각
     * {@code POSITIVE_INFINITY}/{@code NEGATIVE_INFINITY}로 파싱한다. 부호와 무관하게
     * 같은 예외로 DLQ({@code permanent})로 간다.
     * <b>NaN은 오지 않는다.</b> {@code NaN}은 JSON 리터럴이 아니고 Boot 기본
     * {@code ObjectMapper}는 {@code ALLOW_NON_NUMERIC_NUMBERS}가 꺼져 있어
     * <b>역직렬화 단계에서 거부</b>한다. 즉 {@code Double.isFinite}의 NaN 쪽은
     * 현재 입구(Kafka/MQTT JSON)에 대해서는 잉여다 — 산술로 NaN이 생기는 경로가
     * 새로 들어오면 그때 유효해진다. 지우지 않는 이유가 그것이다.
     * ({@code load-test/poison-message/RESULT_20260908_nan_neginf.md})
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
