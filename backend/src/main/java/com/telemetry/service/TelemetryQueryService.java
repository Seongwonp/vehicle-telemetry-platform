package com.telemetry.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.telemetry.dto.response.TelemetryResponse;
import com.telemetry.exception.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TelemetryQueryService {

    private static final Pattern VEHICLE_ID_PATTERN = Pattern.compile("^[A-Z0-9-]{4,20}$");

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
        String flux = """
            from(bucket: params.bucket)
              |> range(start: -1h)
              |> filter(fn: (r) => r._measurement == "vehicle_telemetry")
              |> filter(fn: (r) => r.vehicle_id == params.vehicleId)
              |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> sort(columns: ["_time"], desc: true)
              |> limit(n: params.limit)
            """;

        return executeQuery(vehicleId, flux, Map.of(
            "bucket", bucket,
            "vehicleId", vehicleId,
            "limit", limit
        ));
    }

    public TelemetryResponse getLatest(String vehicleId) {
        List<TelemetryResponse> results = getRecent(vehicleId, 1);
        if (results.isEmpty()) {
            throw new ResourceNotFoundException("수신된 텔레메트리 데이터가 없습니다: " + vehicleId);
        }
        return results.get(0);
    }

    private List<TelemetryResponse> executeQuery(
        String vehicleId,
        String flux,
        Map<String, Object> params
    ) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        List<TelemetryResponse> results = new ArrayList<>();

        try {
            List<FluxTable> tables = queryApi.query(flux, influxOrg, params);

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
