package com.telemetry.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

import java.util.List;

/**
 * 차량 텔레메트리 입력 계약.
 *
 * <p><b>숫자와 정책의 단일 기준은 {@code docs/telemetry-schema-decision-table.md}다.</b>
 * 여기 애너테이션과 그 문서가 어긋나면 문서를 고치는 게 아니라 둘 중 하나가 틀린 것이다.
 *
 * <h2>범위의 근거</h2>
 *
 * 범위는 <b>해당 OBD-II PID의 표현 범위를 채택한 프로젝트 입력 계약</b>이다.
 * <b>물리적 한계를 입증한 것이 아니다.</b> 처음에는 "물리적으로 불가능한 값"을 기준으로
 * 속도 500km/h 같은 값을 골랐는데, 그건 이상 감지 임계값 위에 여유를 얹어 고른 것이지
 * 근거가 아니었다(2026-09-09 정정).
 *
 * <h2>검증과 이상 감지의 경계</h2>
 *
 * <b>계약 밖의 값은 거부하고, 계약 안의 이상치는 감지에 맡긴다.</b>
 * 검증 범위는 이상 감지 임계값보다 반드시 넓어야 한다 — 좁으면 이상 데이터가 저장되지
 * 않아 감지 자체가 무의미해진다. 예: 온도 상한 215는 감지 임계 105보다 넓으므로
 * 106°C는 통과해서 저장되고 이상으로 잡힌다.
 *
 * <h2>왜 primitive가 아니라 wrapper인가</h2>
 *
 * primitive였을 때는 <b>필드가 없거나 null이면 0이 됐고 검증도 통과했다.</b>
 * "시속 0으로 주행 중"이 정상 데이터로 저장된다. wrapper + {@code @NotNull}이라야
 * 누락이 거부된다. 다만 이 변경은 <b>두 입구가 모두 검증을 거친다는 전제</b>에서만 안전하다
 * ({@link TelemetryDecoder}).
 */
@Data
public class VehicleTelemetry {

    @JsonProperty("vehicle_id")
    @NotBlank
    @Pattern(regexp = "^[A-Z0-9-]{4,20}$")
    private String vehicleId;

    @NotBlank
    private String timestamp;

    /** PID 0D는 1바이트 부호 없음 — <b>음수를 허용하지 않는다.</b> 속력이지 속도가 아니다. */
    @NotNull
    @DecimalMin("0")
    @DecimalMax("255")
    private Double speed;

    /**
     * PID 0C는 2바이트를 4로 나눈다 → <b>0.25 단위이므로 소수가 유효하다.</b>
     *
     * <p>예전에는 {@code int}였고 {@code 2000.7}이 {@code 2000}으로 조용히 잘렸다.
     * 그런데 저장({@code toPoint})과 응답 DTO는 이미 둘 다 double이었다 —
     * <b>여기 하나가 파이프라인의 유일한 축소 지점이었고, 자른 값을 다시 넓혀 저장했다.</b>
     * JSON 입력에 PID 해상도(0.25)를 강제하지는 않는다. 범위 안의 유한한 값이면 보존한다.
     */
    @NotNull
    @DecimalMin("0")
    @DecimalMax("16383.75")
    private Double rpm;

    /** <b>냉각수 온도</b>(PID 05)로 정의한다. 1바이트에서 40을 뺀 범위다. */
    @JsonProperty("engine_temp")
    @NotNull
    @DecimalMin("-40")
    @DecimalMax("215")
    private Double engineTemp;

    /** PID 11, 정의상 백분율. */
    @JsonProperty("throttle_position")
    @NotNull
    @DecimalMin("0")
    @DecimalMax("100")
    private Double throttlePosition;

    /** PID 2F, 정의상 백분율. */
    @JsonProperty("fuel_level")
    @NotNull
    @DecimalMin("0")
    @DecimalMax("100")
    private Double fuelLevel;

    /**
     * <b>제어 모듈 전압</b>(PID 42)으로 정의한다. 동글이 자체 측정하는 전압과는 다른 값이다.
     *
     * <p>이상 감지 룰 {@code 11.5~15V}는 <b>12V 계통을 전제로 한 정책</b>이다.
     * 24V·48V 계통에는 그대로 적용할 수 없다.
     */
    @JsonProperty("battery_voltage")
    @NotNull
    @DecimalMin("0")
    @DecimalMax("65.535")
    private Double batteryVoltage;

    /** 실내·터널에서 없을 수 있으므로 <b>선택 필드</b>다. 다만 있으면 안이 완전해야 한다. */
    @Valid
    private GpsLocation gps;

    /**
     * 선택 필드다. 다만 <b>원소는 형식을 지켜야 한다.</b>
     *
     * <p>{@code @Pattern}만으로는 null 원소가 통과한다(Jakarta Validation 계약 —
     * 실측으로 확인했다). 그래서 {@code @NotNull}을 같이 건다.
     * 이걸로 {@code [null]}(저장 시 문자열 {@code "null"}이 되던 것)과
     * 쉼표가 든 코드(저장 후 코드 2개와 구분 불가)가 둘 다 막힌다.
     */
    @JsonProperty("dtc_codes")
    private List<@NotNull @Pattern(regexp = "^[PBCU][0-9]{4}$") String> dtcCodes;

    @Data
    public static class GpsLocation {
        /** gps 객체가 있는데 한쪽이 없는 것은 계약 위반이다 — 예전에는 0.0이 됐다. */
        @NotNull
        @DecimalMin("-90.0")
        @DecimalMax("90.0")
        private Double lat;

        @NotNull
        @DecimalMin("-180.0")
        @DecimalMax("180.0")
        private Double lng;
    }
}
