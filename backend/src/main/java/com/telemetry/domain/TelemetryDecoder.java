package com.telemetry.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * telemetry payload를 <b>두 입구가 똑같이</b> 해석하고 검증한다.
 *
 * <p><b>왜 필요한가</b>: 2026-09-09 감사 전까지 MQTT 입구만 Bean Validation을 했고
 * Kafka 직접 주입은 {@code readValue}만 했다. 부하 도구와 다른 producer가 그 경로로
 * 들어오면 입력 계약을 통째로 우회한다. 같은 payload가 입구에 따라 통과하기도 하고
 * 거부되기도 하는 상태였다.
 *
 * <p>계약 자체(범위·필수 여부)는 {@link VehicleTelemetry}의 애너테이션에 있고,
 * 이 클래스는 <b>그 계약을 두 입구에 같은 방식으로 적용</b>하는 역할만 한다.
 * 숫자와 정책의 단일 기준은 {@code docs/telemetry-schema-decision-table.md}다.
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

    private final ObjectMapper strictMapper;
    private final Validator validator;

    public TelemetryDecoder(ObjectMapper applicationObjectMapper, Validator validator) {
        // copy()는 앱 매퍼의 설정을 그대로 가져온다. 거기서 하나만 조인다.
        this.strictMapper = applicationObjectMapper.copy()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        this.validator = validator;
    }

    /**
     * payload를 계약에 맞게 해석한다.
     *
     * @throws TelemetryContractException 계약 위반. {@code getReason()}이 사유 코드다.
     */
    public VehicleTelemetry decode(String payload) {
        VehicleTelemetry telemetry;
        try {
            telemetry = strictMapper.readValue(payload, VehicleTelemetry.class);
        } catch (UnrecognizedPropertyException e) {
            // 필드명 오타가 여기서 걸린다. 이게 없으면 `sped`가 조용히 버려지고
            // speed는 기본값이 된다 — 2026-09-09 결정표의 14번 칸.
            throw new TelemetryContractException(
                TelemetryContractException.UNKNOWN_FIELD, e.getPropertyName(), e);
        } catch (MismatchedInputException e) {
            throw new TelemetryContractException(
                TelemetryContractException.TYPE_MISMATCH, shortPath(e), e);
        } catch (JsonProcessingException e) {
            throw new TelemetryContractException(
                TelemetryContractException.MALFORMED_JSON, e.getOriginalMessage(), e);
        }

        // **JSON `null` 리터럴은 readValue가 예외 없이 null을 돌려준다.**
        // 그대로 validate()에 넘기면 Hibernate Validator가 IllegalArgumentException(HV000116)을
        // 던지는데, 그건 계약 예외가 아니다 — MqttMessageHandler는 TelemetryContractException만
        // 잡으므로 **거부가 아니라 핸들러 밖으로 전파**되고(QoS 1 재전달 루프),
        // Kafka 쪽은 잡히긴 해도 dlq.py가 낯선 타입이라 `unknown`으로 분류한다.
        // 빈 문자열("")은 여기 안 온다 — readValue가 MismatchedInputException을 던진다.
        if (telemetry == null) {
            throw new TelemetryContractException(
                TelemetryContractException.TYPE_MISMATCH, "(최상위 null)");
        }

        Set<ConstraintViolation<VehicleTelemetry>> violations = validator.validate(telemetry);
        if (!violations.isEmpty()) {
            throw new TelemetryContractException(
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED, describe(violations));
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
     * 위반을 "필드=사유" 목록으로 만든다.
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
