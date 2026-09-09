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
 * telemetry payload를 <b>모든 경로가 똑같이</b> 해석하고 검증한다.
 *
 * <p><b>왜 필요한가</b>: 2026-09-09 감사 전까지 MQTT 입구만 Bean Validation을 했고
 * Kafka 직접 주입은 {@code readValue}만 했다. 부하 도구와 다른 producer가 그 경로로
 * 들어오면 입력 계약을 통째로 우회한다. 같은 payload가 입구에 따라 통과하기도 하고
 * 거부되기도 하는 상태였다.
 *
 * <p>계약 자체(범위·필수 여부)는 {@link VehicleTelemetry}의 애너테이션에 있고,
 * 이 클래스는 <b>그 계약을 같은 방식으로 적용</b>하는 역할만 한다.
 * 숫자와 정책의 단일 기준은 {@code docs/telemetry-schema-decision-table.md}다.
 *
 * <h2>사유 선택 순서를 고정한다 — 이게 이 클래스의 두 번째 일이다</h2>
 *
 * 예전에는 {@code readValue} 한 번으로 끝냈고, 그래서 <b>사유가 payload의 필드 순서에
 * 따라 달라졌다</b>(2026-09-09 실측):
 *
 * <pre>
 *   {"zzz":1, "speed":"fast"}  →  UNKNOWN_FIELD
 *   {"speed":"fast", "zzz":1}  →  TYPE_MISMATCH     ← 같은 오류 둘인데 순서만 다르다
 *   {"zzz":1, "speed":         →  UNKNOWN_FIELD     ← **잘린 JSON인데 필드명 문제로 보고된다**
 * </pre>
 *
 * 마지막 줄이 제일 나쁘다 — 운영자가 존재하지도 않는 필드명을 고치러 간다.
 * Jackson은 문서를 앞에서부터 읽다가 <b>처음 만난 문제</b>에서 멈추므로, 한 번의
 * {@code readValue}로는 순서를 정할 수 없다.
 *
 * <p>그래서 단계를 나눠 <b>정해진 순서</b>로 본다.
 *
 * <ol>
 *   <li>{@code MALFORMED_JSON} — 문서 전체가 JSON으로 읽히는가 ({@code readTree})</li>
 *   <li>{@code TYPE_MISMATCH} — 최상위가 객체인가 (배열·숫자·문자열·{@code null} 거부)</li>
 *   <li>{@code UNKNOWN_FIELD} — 계약에 없는 필드가 있는가 (<b>이름순 첫 번째</b>를 보고)</li>
 *   <li>{@code TYPE_MISMATCH} — 각 필드가 바인딩되는가</li>
 *   <li>{@code PAYLOAD_VALIDATION_FAILED} — 값이 계약 범위 안인가 (위반은 <b>정렬</b>해서 보고)</li>
 * </ol>
 *
 * <p>같은 순서를 Python 쪽({@code anomaly-detector/contract.py})이 그대로 구현한다.
 * 두 구현이 어긋나지 않는지는 {@code contract-fixtures/cases.json}을 양쪽이 읽어 검사한다.
 *
 * <p><b>비용</b>: 파싱이 한 번 더 도는 것이 아니다. {@code readTree}로 한 번 파싱하고
 * {@code treeToValue}는 그 트리에서 바인딩하므로 렉싱은 1회다.
 *
 * <h2>매퍼를 따로 두는 이유</h2>
 *
 * 전역 Boot 매퍼는 <b>건드리지 않는다.</b> REST 응답·설정·JWT 등 다른 곳이 같은 매퍼를
 * 쓰는데, 거기에 {@code FAIL_ON_UNKNOWN_PROPERTIES}를 켜면 이번 변경과 무관한 곳이 깨진다.
 * 대신 앱의 실제 매퍼를 {@code copy()}해서 <b>telemetry에 필요한 것만</b> 조인다 —
 * 앱 설정을 물려받으므로 "테스트만 통과하는 매퍼"가 되지 않는다.
 */
@Component
public class TelemetryDecoder {

    /**
     * 계약이 정의한 최상위 필드 이름. {@link VehicleTelemetry}의
     * {@code @JsonProperty}/필드명과 <b>정확히 같아야 한다.</b>
     *
     * <p>왜 손으로 적나: unknown 필드를 <b>바인딩 전에</b> 봐야 사유 순서가 고정된다.
     * 어긋나면 {@code TelemetryContractTest.계약_필드_집합이_클래스와_일치한다}가 깨진다 —
     * 리플렉션으로 대조하므로 필드를 추가하고 여기 안 적으면 바로 잡힌다.
     */
    static final Set<String> KNOWN_FIELDS = Set.of(
        "vehicle_id", "timestamp", "speed", "rpm", "engine_temp",
        "throttle_position", "fuel_level", "battery_voltage", "gps", "dtc_codes");

    private final ObjectMapper strictMapper;
    private final Validator validator;

    public TelemetryDecoder(ObjectMapper applicationObjectMapper, Validator validator) {
        // copy()는 앱 매퍼의 설정을 그대로 가져온다. 거기서 하나만 조인다.
        // 중첩 객체(gps)의 unknown 필드는 이 설정이 잡는다 — 최상위만 손으로 본다.
        this.strictMapper = applicationObjectMapper.copy()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            // **뒤에 남은 내용도 거부한다.** 기본값은 무시라서 `{...} garbage`가 통과했고,
            // Python의 `json.loads`는 같은 입력을 거부한다 — 공통 계약이 거기서 깨졌다
            // (2026-09-09 P0-2a 측정). 잘린 메시지가 다른 메시지 뒤에 붙는 경우가
            // 실제 시나리오라 **받아들이면 안 되는 쪽**이 맞다.
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        this.validator = validator;
    }

    /**
     * payload를 계약에 맞게 해석한다.
     *
     * @throws TelemetryContractException 계약 위반. {@code getReason()}이 사유 코드다.
     */
    public VehicleTelemetry decode(String payload) {
        // ── 1) 문서 전체가 JSON인가 ────────────────────────────────
        JsonNode root;
        try {
            root = strictMapper.readTree(payload);
        } catch (JsonProcessingException e) {
            throw new TelemetryContractException(
                TelemetryContractException.MALFORMED_JSON, e.getOriginalMessage(), e);
        }

        // ── 2) 최상위가 객체인가 ───────────────────────────────────
        // **빈 문서와 `null` 리터럴은 다르다.**
        // `readTree("")`는 예외 없이 **null 참조**를 돌려준다 — 문서가 아예 없는 것이므로
        // `MALFORMED_JSON`이다(Python의 `json.loads("")`도 JSONDecodeError를 낸다).
        // `readTree("null")`은 NullNode다 — 문서는 있고 최상위 타입이 틀린 것이라 TYPE_MISMATCH.
        // 처음에 둘을 한데 묶었다가 Python과 사유 코드가 갈려서 나눴다(2026-09-09).
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

        // ── 3) 계약에 없는 필드 ────────────────────────────────────
        // **이름순 첫 번째**를 보고한다. 문서 순서로 하면 같은 오류가 payload 순서에 따라
        // 다른 필드를 가리켜서, 사유가 재현되지 않는다.
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

        // ── 4) 바인딩 ──────────────────────────────────────────────
        VehicleTelemetry telemetry;
        try {
            telemetry = strictMapper.treeToValue(root, VehicleTelemetry.class);
        } catch (UnrecognizedPropertyException e) {
            // 중첩 객체(gps)의 unknown 필드가 여기로 온다 — 최상위는 3)에서 이미 걸렀다.
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

        // ── 5) 값의 범위 ───────────────────────────────────────────
        Set<ConstraintViolation<VehicleTelemetry>> violations = validator.validate(telemetry);
        if (!violations.isEmpty()) {
            throw new TelemetryContractException(
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED, describe(violations));
        }

        // ── 5-b) 달력에 있는 날인가 ────────────────────────────────
        // `@Pattern`은 **모양**만 본다. `2026-13-45T99:00:00Z`는 패턴을 통과하지만
        // 그런 날은 없다. 예전에는 이게 저장 단계(`toPoint()`의 Instant.parse)까지 가서
        // DateTimeParseException으로 DLQ에 갔는데, **감지 경로에는 그 단계가 없어서**
        // 같은 payload가 경로에 따라 다르게 끝났다. 계약 단계로 끌어올린다.
        //
        // 패턴이 이미 오프셋을 강제하므로 여기서 걸리는 것은 달력 위반뿐이다.
        try {
            Instant.parse(telemetry.getTimestamp());
        } catch (DateTimeParseException e) {
            throw new TelemetryContractException(
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED,
                "timestamp is not a real instant", e);
        }
        return telemetry;
    }

    /** 어느 필드에서 났는지만 남긴다. payload 전체를 사유에 넣지 않는다. */
    private static String shortPath(MismatchedInputException e) {
        return e.getPath().isEmpty() ? "(경로 없음)"
            : e.getPath().stream()
                .map(r -> r.getFieldName() == null ? "[" + r.getIndex() + "]" : r.getFieldName())
                .collect(Collectors.joining("."));
    }

    /**
     * 위반을 "필드 사유" 목록으로 만든다. <b>정렬해서</b> 같은 입력이 항상 같은 문자열을 낸다.
     *
     * <p><b>값 자체는 넣지 않는다.</b> 거부 사유는 로그와 DLQ 헤더에 남는데,
     * 좌표나 차량 식별자가 거기 섞이면 개인정보가 로그 보존 기간만큼 남는다
     * ({@code docs/data-retention.md}).
     */
    private static String describe(Set<ConstraintViolation<VehicleTelemetry>> violations) {
        return violations.stream()
            .map(v -> v.getPropertyPath() + " " + v.getMessage())
            .sorted()
            .collect(Collectors.joining("; "));
    }
}
