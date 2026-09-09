package com.telemetry.domain;

/**
 * telemetry payload가 입력 계약을 위반했을 때 던진다.
 *
 * <p><b>두 입구가 같은 이유 문자열을 쓰게 하려고 만든 타입이다.</b>
 * 예전에는 MQTT 입구만 검증을 했고 그 거부 사유는 {@code MqttMessageHandler} 안의
 * 문자열 리터럴이었다. Kafka 직접 주입에는 검증 자체가 없어서 비교할 사유도 없었다.
 *
 * @see TelemetryDecoder
 */
public class TelemetryContractException extends RuntimeException {

    /** JSON 자체가 깨졌다. */
    public static final String MALFORMED_JSON = "MALFORMED_JSON";

    /** 계약에 없는 필드가 들어왔다(필드명 오타를 잡는 유일한 경로다). */
    public static final String UNKNOWN_FIELD = "UNKNOWN_FIELD";

    /** 타입이 맞지 않는다(숫자 자리에 boolean·배열·객체 등). */
    public static final String TYPE_MISMATCH = "TYPE_MISMATCH";

    /** 값이 계약 범위 밖이거나 필수 필드가 없다. */
    public static final String PAYLOAD_VALIDATION_FAILED = "PAYLOAD_VALIDATION_FAILED";

    private final String reason;

    public TelemetryContractException(String reason, String detail, Throwable cause) {
        super(reason + ": " + detail, cause);
        this.reason = reason;
    }

    public TelemetryContractException(String reason, String detail) {
        this(reason, detail, null);
    }

    /** 거부 사유 코드. 두 입구가 이 값을 그대로 쓴다. */
    public String getReason() {
        return reason;
    }
}
