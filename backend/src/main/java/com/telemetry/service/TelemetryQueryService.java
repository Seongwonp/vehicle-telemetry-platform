package com.telemetry.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.dto.response.TripResponse;
import com.telemetry.exception.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TelemetryQueryService {

    private static final Pattern VEHICLE_ID_PATTERN = Pattern.compile("^[A-Z0-9-]{4,20}$");

    // 트립 조회 파라미터 — hours는 검증된 int(자유 문자열 아님)라 Flux 문자열에
    // 직접 삽입해도 injection 위험이 없다(파라미터 바인딩은 값 리터럴만 지원하고
    // range() duration 인자에는 쓸 수 없어 다른 쿼리들과 달리 String.format을 쓴다).
    private static final int DEFAULT_TRIP_HOURS = 1;
    private static final int MAX_TRIP_HOURS = 6;
    private static final int TRIP_POINT_LIMIT = 10_000;
    private static final Duration TRIP_GAP_THRESHOLD = Duration.ofMinutes(3);
    private static final double EARTH_RADIUS_KM = 6371.0;

    private final InfluxDBClient influxDBClient;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.org}")
    private String influxOrg;

    public List<TelemetryResponse> getRecent(String vehicleId, int limit) {
        validateVehicleId(vehicleId);
        // InfluxDB는 기본적으로 필드마다 별도 행을 반환한다.
        // pivot으로 _time 기준으로 묶어야 한 타임스탬프 = 한 레코드 구조가 만들어진다.
        // range(start: -1h)는 전체 스캔 방지용 가드 — 없으면 전 기간을 읽어 OOM 위험이 있다.
        //
        // vehicleId는 이미 validateVehicleId()로 ^[A-Z0-9-]{4,20}$ 형식만 통과시킨
        // 뒤라 따옴표/역슬래시/개행 등 Flux 구문을 깨뜨릴 문자가 들어올 수 없어
        // 문자열 삽입이 안전하다(injection 여지 없음). bucket도 애플리케이션
        // 설정값이라 사용자 입력이 아니다. InfluxDB 2.7 서버가 클라이언트의
        // 파라미터 바인딩(`params.xxx`) 기능을 지원하지 않아("undefined identifier
        // params" 컴파일 에러로 모든 조회가 깨졌었다) 검증된 값 직접 삽입 방식으로 되돌렸다.
        String flux = String.format("""
            from(bucket: "%s")
              |> range(start: -1h)
              |> filter(fn: (r) => r._measurement == "vehicle_telemetry")
              |> filter(fn: (r) => r.vehicle_id == "%s")
              |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> sort(columns: ["_time"], desc: true)
              |> limit(n: %d)
            """, bucket, vehicleId, limit);

        return executeQuery(vehicleId, flux);
    }

    public TelemetryResponse getLatest(String vehicleId) {
        List<TelemetryResponse> results = getRecent(vehicleId, 1);
        if (results.isEmpty()) {
            throw new ResourceNotFoundException("수신된 텔레메트리 데이터가 없습니다: " + vehicleId);
        }
        return results.get(0);
    }

    // 연속 수신 구간을 시간 간격(TRIP_GAP_THRESHOLD) 기준으로 트립으로 나눈다.
    // 시뮬레이터처럼 끊김 없이 계속 송신하는 소스는 트립 하나로 합쳐지는 게
    // 맞는 동작이다 — 실제 OBD-II 동글처럼 시동이 꺼지며 송신이 끊기는 경우를
    // 위한 일반적인 로직이라 지금 당장 여러 트립으로 안 나뉘는 건 버그가 아니다.
    public List<TripResponse> getTrips(String vehicleId, Integer hours) {
        validateVehicleId(vehicleId);
        int safeHours = hours == null
            ? DEFAULT_TRIP_HOURS
            : Math.min(Math.max(hours, 1), MAX_TRIP_HOURS);

        String flux = String.format("""
            from(bucket: "%s")
              |> range(start: -%dh)
              |> filter(fn: (r) => r._measurement == "vehicle_telemetry")
              |> filter(fn: (r) => r.vehicle_id == "%s")
              |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> sort(columns: ["_time"], desc: false)
              |> limit(n: %d)
            """, bucket, safeHours, vehicleId, TRIP_POINT_LIMIT);

        List<TelemetryResponse> points = executeQuery(vehicleId, flux);

        List<TripResponse> trips = segmentIntoTrips(points);
        Collections.reverse(trips); // 최신 트립이 먼저 오도록
        return trips;
    }

    private List<TripResponse> segmentIntoTrips(List<TelemetryResponse> points) {
        List<TripResponse> trips = new ArrayList<>();
        List<TelemetryResponse> current = new ArrayList<>();
        Instant prevTime = null;

        for (TelemetryResponse point : points) {
            if (point.getTimestamp() == null) continue;
            Instant time = Instant.parse(point.getTimestamp());
            if (prevTime != null
                && Duration.between(prevTime, time).compareTo(TRIP_GAP_THRESHOLD) > 0) {
                addTripIfMeaningful(trips, current);
                current = new ArrayList<>();
            }
            current.add(point);
            prevTime = time;
        }
        addTripIfMeaningful(trips, current);
        return trips;
    }

    // 포인트 1~2개짜리 구간(신호가 잠깐 끊겼다 바로 돌아온 경우 등)은 실제
    // 주행으로 보기 어려워 트립 목록에서 제외한다.
    private void addTripIfMeaningful(List<TripResponse> trips, List<TelemetryResponse> points) {
        if (points.size() < 3) return;

        Instant start = Instant.parse(points.get(0).getTimestamp());
        Instant end = Instant.parse(points.get(points.size() - 1).getTimestamp());

        double distanceKm = 0.0;
        double speedSum = 0.0;
        double maxSpeed = 0.0;
        for (int i = 0; i < points.size(); i++) {
            TelemetryResponse p = points.get(i);
            double speed = p.getSpeed() != null ? p.getSpeed() : 0.0;
            speedSum += speed;
            maxSpeed = Math.max(maxSpeed, speed);
            if (i > 0) {
                TelemetryResponse prev = points.get(i - 1);
                if (prev.getLat() != null && prev.getLng() != null
                    && p.getLat() != null && p.getLng() != null) {
                    distanceKm += haversineKm(prev.getLat(), prev.getLng(), p.getLat(), p.getLng());
                }
            }
        }

        trips.add(TripResponse.builder()
            .startTime(start)
            .endTime(end)
            .durationMinutes(Duration.between(start, end).toMinutes())
            .distanceKm(Math.round(distanceKm * 100) / 100.0)
            .avgSpeedKmh(Math.round((speedSum / points.size()) * 10) / 10.0)
            .maxSpeedKmh(Math.round(maxSpeed * 10) / 10.0)
            .pointCount(points.size())
            .build());
    }

    private double haversineKm(double lat1, double lng1, double lat2, double lng2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
            * Math.sin(dLng / 2) * Math.sin(dLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_KM * c;
    }

    private List<TelemetryResponse> executeQuery(String vehicleId, String flux) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        List<TelemetryResponse> results = new ArrayList<>();

        try {
            List<FluxTable> tables = queryApi.query(flux, influxOrg);

            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {
                    results.add(mapToResponse(vehicleId, record));
                }
            }
        } catch (Exception e) {
            log.error("[InfluxDB] 쿼리 실패 vehicle={}", vehicleId, e);
            throw new RuntimeException("텔레메트리 조회 실패", e);
        }

        return results;
    }

    private void validateVehicleId(String vehicleId) {
        if (vehicleId == null || !VEHICLE_ID_PATTERN.matcher(vehicleId).matches()) {
            throw new IllegalArgumentException("차량 ID 형식이 올바르지 않습니다");
        }
    }

    private TelemetryResponse mapToResponse(String vehicleId, FluxRecord record) {
        Map<String, Object> values = record.getValues();

        Instant time = record.getTime();
        String timestamp = time != null ? time.toString() : null;

        String dtcRaw = getStr(values, "dtc_codes");
        List<String> dtcCodes = (dtcRaw != null && !dtcRaw.isEmpty())
            ? Arrays.asList(dtcRaw.split(","))
            : List.of();

        return TelemetryResponse.builder()
            .vehicleId(vehicleId)
            .timestamp(timestamp)
            .speed(getDouble(values, "speed"))
            .rpm(getDouble(values, "rpm"))
            .engineTemp(getDouble(values, "engine_temp"))
            .throttlePosition(getDouble(values, "throttle_position"))
            .fuelLevel(getDouble(values, "fuel_level"))
            .batteryVoltage(getDouble(values, "battery_voltage"))
            .lat(getDouble(values, "lat"))
            .lng(getDouble(values, "lng"))
            .dtcCodes(dtcCodes)
            .build();
    }

    private Double getDouble(Map<String, Object> values, String key) {
        Object val = values.get(key);
        if (val instanceof Number n) return n.doubleValue();
        return null;
    }

    private String getStr(Map<String, Object> values, String key) {
        Object val = values.get(key);
        return val != null ? val.toString() : null;
    }
}
