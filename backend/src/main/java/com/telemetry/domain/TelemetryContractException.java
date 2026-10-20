package com.telemetry.domain;

/**
 * telemetry 입력 계약 위반. MQTT·Kafka 두 입구가 같은 사유 코드를 쓰게 하는 타입이다.
 *
 * @see TelemetryDecoder
 */
public class TelemetryContractException extends RuntimeException {

    public static final String MALFORMED_JSON = "MALFORMED_JSON";

    /** 필드명 오타를 잡는 유일한 경로다. */
    public static final String UNKNOWN_FIELD = "UNKNOWN_FIELD";

    public static final String TYPE_MISMATCH = "TYPE_MISMATCH";

    /** 범위 밖이거나 필수 필드 누락. */
    public static final String PAYLOAD_VALIDATION_FAILED = "PAYLOAD_VALIDATION_FAILED";

    private final String reason;

    public TelemetryContractException(String reason, String detail, Throwable cause) {
        super(reason + ": " + detail, cause);
        this.reason = reason;
    }

    public TelemetryContractException(String reason, String detail) {
        this(reason, detail, null);
    }

    public String getReason() {
        return reason;
    }

    /** 지표 라벨에는 이 4종만 들어간다(docs/rejection-metrics-design.md). */
    public static final java.util.Set<String> REASONS = java.util.Set.of(
        MALFORMED_JSON, UNKNOWN_FIELD, TYPE_MISMATCH, PAYLOAD_VALIDATION_FAILED);

    /**
     * MQTT 고유 사유({@code TOPIC_VEHICLE_MISMATCH})는 계약 사유가 아니다.
     * 섞으면 세 입구의 사유별 지표를 나란히 비교할 수 없다.
     */
    public static boolean isContractReason(String reason) {
        return REASONS.contains(reason);
    }
}
