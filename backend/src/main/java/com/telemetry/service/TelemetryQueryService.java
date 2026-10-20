package com.telemetry.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.dto.response.TripResponse;
import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.repository.VehicleRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
public class TelemetryQueryService {

    private static final Pattern VEHICLE_ID_PATTERN = Pattern.compile("^[A-Z0-9-]{4,20}$");

    // hours는 범위를 자른 int라 Flux에 직접 넣어도 injection 위험이 없다.
    private static final int MAX_TRIP_HOURS = 6;
    private static final double EARTH_RADIUS_KM = 6371.0;

    private final InfluxDBClient influxDBClient;
    private final VehicleRepository vehicleRepository;
    private final Counter queryFailureCounter;

    public TelemetryQueryService(
        InfluxDBClient influxDBClient,
        VehicleRepository vehicleRepository,
        MeterRegistry meterRegistry
    ) {
        this.influxDBClient = influxDBClient;
        this.vehicleRepository = vehicleRepository;
        this.queryFailureCounter = meterRegistry.counter("telemetry.influx.query.failures");
    }

    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.org}")
    private String influxOrg;

    @Value("${telemetry.trip.default-hours:1}")
    private int defaultTripHours;

    @Value("${telemetry.trip.point-limit:10000}")
    private int tripPointLimit;

    @Value("${telemetry.trip.gap-threshold:3m}")
    private Duration tripGapThreshold;

    @Value("${telemetry.trip.min-points:3}")
    private int tripMinPoints;

    @Value("${telemetry.trip.max-gps-speed-kmh:250}")
    private double maxGpsSpeedKmh;

    // getRecent는 1시간만 보므로, 오래 정지한 차량의 lastSeenAt이 null이 되지 않게 최신값 조회는 창을 넓힌다.
    private static final int LATEST_LOOKBACK_HOURS = 24;

    public List<TelemetryResponse> getRecent(String vehicleId, int limit) {
        validateVehicleId(vehicleId);
        return getRecent(vehicleId, limit, 1);
    }

    public TelemetryResponse getLatest(String vehicleId) {
        validateVehicleId(vehicleId);
        List<TelemetryResponse> results = getRecent(vehicleId, 1, LATEST_LOOKBACK_HOURS);
        if (results.isEmpty()) {
            throw new ResourceNotFoundException("수신된 텔레메트리 데이터가 없습니다: " + vehicleId);
        }
        return results.get(0);
    }

    public Map<String, TelemetryResponse> getLatestByVehicleIds(List<String> vehicleIds) {
        if (vehicleIds.isEmpty()) return Map.of();
        vehicleIds.forEach(this::validateVehicleId);
        // contains() 대신 등호 OR을 쓴다. contains()는 스토리지 푸시다운이 안 돼 read timeout을 넘긴다.
        // 경위: docs/verification/2026-09-28-multi-user-e2e.md
        String idFilter = vehicleIds.stream()
            .distinct()
            .map(id -> "r.vehicle_id == \"" + id + "\"")
            .collect(Collectors.joining(" or "));
        // 선택 필드(ADR-030)가 최신 포인트에 없으면 pivot 뒤 행이 둘이 된다. sort+limit로 최신 행만 남겨
        // 그 필드는 null이 된다. 옛 값을 섞지 않으려는 의도다(InfluxDbContractTest).
        String flux = String.format("""
            from(bucket: "%s")
              |> range(start: -%dh)
              |> filter(fn: (r) => r._measurement == "vehicle_telemetry")
              |> filter(fn: (r) => %s)
              |> last()
              |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> group(columns: ["vehicle_id"])
              |> sort(columns: ["_time"], desc: true)
              |> limit(n: 1)
            """, bucket, LATEST_LOOKBACK_HOURS, idFilter);

        Map<String, TelemetryResponse> latest = new HashMap<>();
        for (FluxTable table : query(flux, "fleet")) {
            for (FluxRecord record : table.getRecords()) {
                String vehicleId = getStr(record.getValues(), "vehicle_id");
                if (vehicleId != null) latest.put(vehicleId, mapToResponse(vehicleId, record));
            }
        }
        return Map.copyOf(latest);
    }

    // range()는 전체 스캔(OOM) 방지 가드라 빼지 말 것.
    // InfluxDB 2.7은 Flux 파라미터 바인딩을 지원하지 않아 문자열로 넣는다. vehicleId는
    // validateVehicleId()를 통과한 값만 와야 한다(injection 방지).
    private List<TelemetryResponse> getRecent(String vehicleId, int limit, int rangeHours) {
        String flux = String.format("""
            from(bucket: "%s")
              |> range(start: -%dh)
              |> filter(fn: (r) => r._measurement == "vehicle_telemetry")
              |> filter(fn: (r) => r.vehicle_id == "%s")
              |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> sort(columns: ["_time"], desc: true)
              |> limit(n: %d)
            """, bucket, rangeHours, vehicleId, limit);

        return executeQuery(vehicleId, flux);
    }

    // 수신 간격이 gap-threshold를 넘으면 트립을 나눈다. 끊김 없이 송신하는 시뮬레이터는 트립 하나가 맞다.
    public List<TripResponse> getTrips(String vehicleId, Integer hours) {
        validateVehicleId(vehicleId);
        if (!vehicleRepository.existsByVehicleIdAndActiveTrue(vehicleId)) {
            throw new ResourceNotFoundException("등록되지 않은 차량입니다: " + vehicleId);
        }
        int safeHours = hours == null
            ? defaultTripHours
            : Math.min(Math.max(hours, 1), MAX_TRIP_HOURS);

        // 내림차순으로 최신 N개를 자른 뒤 Java에서 뒤집는다. 오름차순 후 limit이면 최근 트립이 잘린다.
        String flux = String.format("""
            from(bucket: "%s")
              |> range(start: -%dh)
              |> filter(fn: (r) => r._measurement == "vehicle_telemetry")
              |> filter(fn: (r) => r.vehicle_id == "%s")
              |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> sort(columns: ["_time"], desc: true)
              |> limit(n: %d)
            """, bucket, safeHours, vehicleId, tripPointLimit);

        List<TelemetryResponse> points = executeQuery(vehicleId, flux);
        Collections.reverse(points); // 세그멘테이션은 오름차순을 기대한다

        List<TripResponse> trips = segmentIntoTrips(points);
        Collections.reverse(trips); // 최신 트립이 먼저 오도록
        return trips;
    }

    List<TripResponse> segmentIntoTrips(List<TelemetryResponse> points) {
        List<TripResponse> trips = new ArrayList<>();
        List<TelemetryResponse> current = new ArrayList<>();
        Instant prevTime = null;

        for (TelemetryResponse point : points) {
            if (point.getTimestamp() == null) continue;
            Instant time = Instant.parse(point.getTimestamp());
            if (prevTime != null
                && Duration.between(prevTime, time).compareTo(tripGapThreshold) > 0) {
                addTripIfMeaningful(trips, current);
                current = new ArrayList<>();
            }
            current.add(point);
            prevTime = time;
        }
        addTripIfMeaningful(trips, current);
        return trips;
    }

    // 포인트가 너무 적은 구간(잠깐 끊겼다 돌아온 경우)은 주행으로 보지 않는다.
    private void addTripIfMeaningful(List<TripResponse> trips, List<TelemetryResponse> points) {
        if (points.size() < tripMinPoints) return;

        Instant start = Instant.parse(points.get(0).getTimestamp());
        Instant end = Instant.parse(points.get(points.size() - 1).getTimestamp());

        double distanceKm = 0.0;
        double weightedSpeedSeconds = 0.0;
        long weightedDurationSeconds = 0;
        double maxSpeed = 0.0;
        for (int i = 0; i < points.size(); i++) {
            TelemetryResponse p = points.get(i);
            double speed = p.getSpeed() != null ? p.getSpeed() : 0.0;
            maxSpeed = Math.max(maxSpeed, speed);
            if (i > 0) {
                TelemetryResponse prev = points.get(i - 1);
                Instant previousTime = Instant.parse(prev.getTimestamp());
                long seconds = Duration.between(previousTime, Instant.parse(p.getTimestamp())).toSeconds();
                if (seconds > 0) {
                    double previousSpeed = prev.getSpeed() != null ? prev.getSpeed() : 0.0;
                    weightedSpeedSeconds += ((previousSpeed + speed) / 2.0) * seconds;
                    weightedDurationSeconds += seconds;
                }
                if (prev.getLat() != null && prev.getLng() != null
                    && p.getLat() != null && p.getLng() != null
                    && validCoordinates(prev.getLat(), prev.getLng())
                    && validCoordinates(p.getLat(), p.getLng())
                    && seconds > 0) {
                    double segmentKm = haversineKm(prev.getLat(), prev.getLng(), p.getLat(), p.getLng());
                    double impliedSpeed = segmentKm / (seconds / 3600.0);
                    if (impliedSpeed <= maxGpsSpeedKmh) distanceKm += segmentKm;
                }
            }
        }

        double averageSpeed = weightedDurationSeconds == 0
            ? (points.get(0).getSpeed() == null ? 0.0 : points.get(0).getSpeed())
            : weightedSpeedSeconds / weightedDurationSeconds;

        trips.add(TripResponse.builder()
            .startTime(start)
            .endTime(end)
            .durationMinutes(Duration.between(start, end).toMinutes())
            .distanceKm(Math.round(distanceKm * 100) / 100.0)
            .avgSpeedKmh(Math.round(averageSpeed * 10) / 10.0)
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

    private boolean validCoordinates(double lat, double lng) {
        return lat >= -90.0 && lat <= 90.0 && lng >= -180.0 && lng <= 180.0;
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
            queryFailureCounter.increment();
            log.error("[InfluxDB] 쿼리 실패 vehicle={}", vehicleId, e);
            throw new RuntimeException("텔레메트리 조회 실패", e);
        }

        return results;
    }

    private List<FluxTable> query(String flux, String context) {
        try {
            return influxDBClient.getQueryApi().query(flux, influxOrg);
        } catch (Exception e) {
            queryFailureCounter.increment();
            log.error("[InfluxDB] 쿼리 실패 context={}", context, e);
            throw new RuntimeException("텔레메트리 조회 실패", e);
        }
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
