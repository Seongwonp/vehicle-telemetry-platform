package com.telemetry.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * telemetry payload를 MQTT·Kafka 두 입구가 똑같이 해석하고 검증한다.
 * 한쪽만 검증하면 다른 입구로 계약을 우회할 수 있다. 계약 자체는 {@link VehicleTelemetry},
 * 숫자와 정책은 {@code docs/telemetry-schema-decision-table.md}가 기준이다.
 *
 * <p>사유 코드가 payload 필드 순서에 따라 바뀌지 않도록 단계를 고정된 순서로 본다
 * (Jackson은 처음 만난 문제에서 멈추므로 readValue 한 번으로는 순서를 정할 수 없다).
 *
 * <ol>
 *   <li>{@code MALFORMED_JSON} — 문서 전체가 JSON인가</li>
 *   <li>{@code TYPE_MISMATCH} — 최상위가 객체인가</li>
 *   <li>{@code UNKNOWN_FIELD} — 계약에 없는 필드 (이름순 첫 번째)</li>
 *   <li>{@code TYPE_MISMATCH} — 각 필드가 바인딩되는가</li>
 *   <li>{@code PAYLOAD_VALIDATION_FAILED} — 값이 범위 안인가 (위반은 정렬해서 보고)</li>
 * </ol>
 *
 * <p>이 순서는 {@code anomaly-detector/contract.py}와 같아야 한다({@code contract-fixtures/cases.json}).
 * 전역 Boot 매퍼는 다른 곳이 같이 쓰므로 건드리지 않고, {@code copy()}해서 여기서만 조인다.
 */
@Component
public class TelemetryDecoder {

    /**
     * {@link VehicleTelemetry}의 JSON 필드명과 정확히 같아야 한다. unknown 필드를 바인딩 전에
     * 보려고 손으로 적는다. 어긋나면 {@code TelemetryContractTest}가 잡는다.
     */
    static final Set<String> KNOWN_FIELDS = Set.of(
        "vehicle_id", "timestamp", "speed", "rpm", "engine_temp",
        "throttle_position", "fuel_level", "battery_voltage", "gps", "dtc_codes");

    /** 선택 숫자 필드(ADR-030). {@code anomaly-detector/contract.py}의 {@code OPTIONAL_NUMERIC}과 같아야 한다. */
    static final List<String> OPTIONAL_NUMERIC = List.of("battery_voltage", "fuel_level");

    private final ObjectMapper strictMapper;
    private final Validator validator;

    public TelemetryDecoder(ObjectMapper applicationObjectMapper, Validator validator) {
        // 중첩 객체(gps)의 unknown 필드는 이 설정이 잡는다. 최상위는 손으로 본다.
        this.strictMapper = applicationObjectMapper.copy()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            // `{...} garbage`도 거부한다. Python json.loads와 판정을 맞추기 위해서다.
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        this.validator = validator;
    }

    /**
     * payload를 계약에 맞게 해석한다.
     *
     * @throws TelemetryContractException 계약 위반. {@code getReason()}이 사유 코드다.
     */
    public VehicleTelemetry decode(String payload) {
        // 1) 문서 전체가 JSON인가
        JsonNode root;
        try {
            root = strictMapper.readTree(payload);
        } catch (JsonProcessingException e) {
            throw new TelemetryContractException(
                TelemetryContractException.MALFORMED_JSON, e.getOriginalMessage(), e);
        }

        // 2) 최상위가 객체인가
        // 빈 문서(readTree가 null 반환)는 MALFORMED_JSON, `null` 리터럴은 TYPE_MISMATCH.
        // Python 쪽 사유 코드와 맞추려고 둘을 나눈다.
        if (root == null || root.isMissingNode()) {
            throw new TelemetryContractException(
                TelemetryContractException.MALFORMED_JSON, "(빈 문서)");
        }
        if (root.isNull()) {
            throw new TelemetryContractException(
                TelemetryContractException.TYPE_MISMATCH, "(최상위 null)");
        }
        if (!root.isObject()) {
            throw new TelemetryContractException(
                TelemetryContractException.TYPE_MISMATCH,
                "(최상위가 객체가 아니다: " + root.getNodeType() + ")");
        }

        // 3) 계약에 없는 필드 — 사유가 payload 순서에 좌우되지 않도록 이름순으로 정렬한다.
        List<String> unknown = new ArrayList<>();
        for (Iterator<String> it = root.fieldNames(); it.hasNext(); ) {
            String name = it.next();
            if (!KNOWN_FIELDS.contains(name)) {
                unknown.add(name);
            }
        }
        if (!unknown.isEmpty()) {
            unknown.sort(null);
            throw new TelemetryContractException(
                TelemetryContractException.UNKNOWN_FIELD, String.join(",", unknown));
        }

        // 4) 바인딩
        VehicleTelemetry telemetry;
        try {
            telemetry = strictMapper.treeToValue(root, VehicleTelemetry.class);
        } catch (UnrecognizedPropertyException e) {
            // 중첩 객체(gps)의 unknown 필드
            throw new TelemetryContractException(
                TelemetryContractException.UNKNOWN_FIELD, e.getPropertyName(), e);
        } catch (MismatchedInputException e) {
            throw new TelemetryContractException(
                TelemetryContractException.TYPE_MISMATCH, shortPath(e), e);
        } catch (JsonProcessingException e) {
            throw new TelemetryContractException(
                TelemetryContractException.MALFORMED_JSON, e.getOriginalMessage(), e);
        }
        if (telemetry == null) {
            throw new TelemetryContractException(
                TelemetryContractException.TYPE_MISMATCH, "(최상위 null)");
        }

        // 5) 값의 범위
        Set<ConstraintViolation<VehicleTelemetry>> violations = validator.validate(telemetry);
        // 선택 필드의 빈 문자열은 "없음"이 아니라 위반이다(ADR-030). Jackson이 ""를 null로
        // 바꾸므로 원본 트리에서 본다. 필수 필드는 @NotNull이 잡는다.
        List<String> blankOptional = new ArrayList<>();
        for (String name : OPTIONAL_NUMERIC) {
            JsonNode node = root.get(name);
            if (node != null && node.isTextual() && node.asText().isBlank()) {
                blankOptional.add(name + " must not be blank");
            }
        }
        if (!violations.isEmpty() || !blankOptional.isEmpty()) {
            List<String> all = new ArrayList<>(blankOptional);
            if (!violations.isEmpty()) {
                all.add(describe(violations));
            }
            all.sort(null);
            throw new TelemetryContractException(
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED, String.join("; ", all));
        }

        // 5-b) 달력에 있는 날인가 — @Pattern은 모양만 본다(2026-13-45도 통과).
        // 저장 단계가 아니라 여기서 걸러야 감지 경로와 판정이 같아진다.
        try {
            Instant.parse(telemetry.getTimestamp());
        } catch (DateTimeParseException e) {
            throw new TelemetryContractException(
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED,
                "timestamp is not a real instant", e);
        }
        return telemetry;
    }

    /** 필드 경로만 남긴다. payload를 사유에 넣지 않는다. */
    private static String shortPath(MismatchedInputException e) {
        return e.getPath().isEmpty() ? "(경로 없음)"
            : e.getPath().stream()
                .map(r -> r.getFieldName() == null ? "[" + r.getIndex() + "]" : r.getFieldName())
                .collect(Collectors.joining("."));
    }

    /**
     * 정렬해서 같은 입력이 항상 같은 문자열을 내게 한다. 값은 넣지 않는다 — 사유는 로그와
     * DLQ 헤더에 남으므로 좌표·차량 식별자가 섞이면 안 된다({@code docs/data-retention.md}).
     */
    private static String describe(Set<ConstraintViolation<VehicleTelemetry>> violations) {
        return violations.stream()
            .map(v -> v.getPropertyPath() + " " + v.getMessage())
            .sorted()
            .collect(Collectors.joining("; "));
    }
}
