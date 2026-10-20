package com.telemetry.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
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
 * 차량 텔레메트리 입력 계약. 숫자와 정책의 기준은 {@code docs/telemetry-schema-decision-table.md}다.
 *
 * <p>범위는 OBD-II PID 표현 범위를 채택한 것이다(물리적 한계가 아님). 검증 범위는 이상 감지
 * 임계값보다 반드시 넓어야 한다 — 좁으면 이상 데이터가 거부되어 감지에 도달하지 못한다.
 *
 * <p>필수 숫자는 wrapper + {@code @NotNull}이다. primitive면 누락이 0이 되어 정상값으로 저장된다.
 * 두 입구가 모두 {@link TelemetryDecoder}를 거친다는 전제에서만 안전하다.
 *
 * <p>{@code fuelLevel}·{@code batteryVoltage}만 선택이다(ADR-030). 없으면 null 그대로 두고
 * 저장 시 필드를 쓰지 않는다. 목록은 {@code anomaly-detector/contract.py}의 {@code OPTIONAL_NUMERIC}과 같아야 한다.
 */
@Data
public class VehicleTelemetry {

    @JsonProperty("vehicle_id")
    @NotBlank
    @Pattern(regexp = "^[A-Z0-9-]{4,20}$")
    private String vehicleId;

    /**
     * ISO-8601, 오프셋 필수. 형식만 보고 신선도는 보지 않는다 — 과거 시각을 거부하면
     * DLQ 재처리와 백필이 불가능해진다. 달력 검증은 {@link TelemetryDecoder}가 한다.
     * Java·Python 파서가 서로 다른 입력을 받으므로 이 패턴이 계약이다.
     */
    @NotBlank
    @Pattern(regexp = TIMESTAMP_PATTERN)
    private String timestamp;

    /** {@code anomaly-detector/contract.py}의 {@code _TIMESTAMP}와 <b>같아야 한다.</b> */
    public static final String TIMESTAMP_PATTERN =
        "^\\d{4}-\\d{2}-\\d{2}[Tt]\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?([Zz]|[+-]\\d{2}:\\d{2})$";

    /** PID 0D, 1바이트 부호 없음 — 음수 불가. */
    @NotNull
    @DecimalMin("0")
    @DecimalMax("255")
    private Double speed;

    /** PID 0C는 0.25 단위라 소수가 유효하다. int로 바꾸면 값이 조용히 잘린다. */
    @NotNull
    @DecimalMin("0")
    @DecimalMax("16383.75")
    private Double rpm;

    /** 냉각수 온도(PID 05). */
    @JsonProperty("engine_temp")
    @NotNull
    @DecimalMin("-40")
    @DecimalMax("215")
    private Double engineTemp;

    /** PID 11. */
    @JsonProperty("throttle_position")
    @NotNull
    @DecimalMin("0")
    @DecimalMax("100")
    private Double throttlePosition;

    /**
     * PID 2F, 선택(ADR-030). {@code NON_NULL}: Kafka로 재직렬화할 때 원본에 없던
     * {@code "fuel_level": null}을 만들지 않는다.
     */
    @JsonProperty("fuel_level")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @DecimalMin("0")
    @DecimalMax("100")
    private Double fuelLevel;

    /**
     * 제어 모듈 전압(PID 42), 동글 자체 측정 전압과 다르다. 선택(ADR-030) — 없으면 감지기가
     * 전압 룰을 평가하지 않는다(0으로 보면 매번 저전압). 감지 룰 11.5~15V는 12V 계통 전제다.
     */
    @JsonProperty("battery_voltage")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @DecimalMin("0")
    @DecimalMax("65.535")
    private Double batteryVoltage;

    /** 실내·터널에서 없을 수 있어 선택. 있으면 lat/lng 둘 다 있어야 한다. */
    @Valid
    private GpsLocation gps;

    /**
     * 선택 필드. {@code @Pattern}만으로는 null 원소가 통과하므로 {@code @NotNull}을 같이 건다.
     * 저장 시 쉼표로 접으므로 null·쉼표 포함 코드를 여기서 막아야 한다.
     */
    @JsonProperty("dtc_codes")
    private List<@NotNull @Pattern(regexp = "^[PBCU][0-9]{4}$") String> dtcCodes;

    @Data
    public static class GpsLocation {
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
