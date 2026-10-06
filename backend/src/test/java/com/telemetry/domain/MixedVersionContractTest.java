package com.telemetry.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.legacy.VehicleTelemetryV1;
import com.telemetry.support.TestDecoders;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 혼합 버전 — <b>배포 순서(감지기 → 백엔드 → 브리지)가 왜 필요한지</b>를 테스트로 고정한다 (ADR-030).
 *
 * <p>{@code docs/event-correlation-design.md} §10-5와 같은 원칙이다. 받는 쪽이 새 모양을 모르면 메시지 전체를
 * 거부한다. 이번 "새 모양"은 필드 추가가 아니라 <b>필드 생략</b>(연료량·제어 모듈 전압 없음/null)이다.
 *
 * <ul>
 *   <li><b>브리지가 백엔드보다 먼저</b> → 구 백엔드가 브리지 payload를 거부한다 — 아래 {@code 구_계약은_새_payload를_거부한다}.</li>
 *   <li><b>백엔드가 감지기보다 먼저</b> → 새 백엔드가 Kafka로 재직렬화한 값(키 없음)을 구 감지기가 거부한다 —
 *       아래 {@code 새_백엔드의_Kafka_value는_구_계약이_거부한다}. 구 감지기(Python)의 같은 판정은
 *       {@code anomaly-detector/tests/test_mixed_version.py}가 동결본으로 고정한다.</li>
 * </ul>
 *
 * <p>구 계약은 {@link VehicleTelemetryV1}(동결본)이다 — 새 계약을 고친 뒤에는 현재 코드로 구 동작을 재현할 수 없다.
 */
@DisplayName("혼합 버전 — 구 계약은 선택 필드가 빠진 새 payload를 거부한다")
class MixedVersionContractTest {

    private final TelemetryDecoder newDecoder = TestDecoders.telemetryDecoder();
    private final ObjectMapper bootMapper = TestDecoders.bootObjectMapper();
    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    private static final String FULL = """
        {"vehicle_id":"KR-GA-1234","timestamp":"2026-10-06T03:00:00.000Z",
         "speed":50.0,"rpm":2000.25,"engine_temp":90.0,"throttle_position":20.0,
         "fuel_level":60.0,"battery_voltage":13.9}
        """;

    private static String shape(String name) {
        return switch (name) {
            case "fuel_absent" -> FULL.replace("\"fuel_level\":60.0,", "");
            case "voltage_absent" -> FULL.replace(",\"battery_voltage\":13.9", "");
            case "both_absent" -> """
                {"vehicle_id":"KR-GA-1234","timestamp":"2026-10-06T03:00:00.000Z",
                 "speed":50.0,"rpm":2000.25,"engine_temp":90.0,"throttle_position":20.0}
                """;
            case "fuel_null" -> FULL.replace("\"fuel_level\":60.0", "\"fuel_level\":null");
            case "voltage_null" -> FULL.replace("\"battery_voltage\":13.9", "\"battery_voltage\":null");
            default -> throw new IllegalArgumentException(name);
        };
    }

    /** 구 백엔드의 판정 — 같은 strict 매퍼로 동결본에 바인딩하고 같은 Validator로 검사한다. 위반 필드 이름을 돌려준다. */
    private Set<String> v1Violations(String json) throws Exception {
        VehicleTelemetryV1 t = TestDecoders.strictMapperLikeBefore().readValue(json, VehicleTelemetryV1.class);
        return validator.validate(t).stream()
            .map(ConstraintViolation::getPropertyPath)
            .map(Object::toString)
            .collect(Collectors.toSet());
    }

    @Test
    @DisplayName("동결본은 정말 구버전이다 — 두 필드에 @NotNull이 있고, 현재 클래스에는 없다")
    void 동결본은_구버전이다() throws Exception {
        for (String f : new String[] {"fuelLevel", "batteryVoltage"}) {
            assertThat(VehicleTelemetryV1.class.getDeclaredField(f).isAnnotationPresent(NotNull.class))
                .as("동결본 %s에 @NotNull이 없다 — 동결본을 고친 것이다", f).isTrue();
            assertThat(VehicleTelemetry.class.getDeclaredField(f).isAnnotationPresent(NotNull.class))
                .as("현재 %s에 @NotNull이 있다 — 선택화가 되돌려졌다", f).isFalse();
        }
        // 필수 넷은 현재도 @NotNull — 선택화 범위가 두 필드뿐이다.
        for (String f : new String[] {"speed", "rpm", "engineTemp", "throttlePosition"}) {
            assertThat(VehicleTelemetry.class.getDeclaredField(f).isAnnotationPresent(NotNull.class))
                .as("%s는 여전히 필수여야 한다", f).isTrue();
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"fuel_absent", "voltage_absent", "both_absent", "fuel_null", "voltage_null"})
    @DisplayName("구 계약은 새 payload를 거부한다 — 브리지를 백엔드보다 먼저 올리면 안 되는 이유")
    void 구_계약은_새_payload를_거부한다(String name) throws Exception {
        String json = shape(name);
        assertThat(v1Violations(json)).isNotEmpty()
            .allMatch(p -> p.equals("fuelLevel") || p.equals("batteryVoltage"));

        // 새 계약은 받는다 — 값을 지어내지 않고 null 그대로.
        VehicleTelemetry t = newDecoder.decode(json);
        assertThat(t.getSpeed()).isEqualTo(50.0);
        if (name.startsWith("fuel") || name.startsWith("both")) {
            assertThat(t.getFuelLevel()).isNull();
        }
        if (name.startsWith("voltage") || name.startsWith("both")) {
            assertThat(t.getBatteryVoltage()).isNull();
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"fuel_absent", "voltage_absent", "both_absent", "fuel_null", "voltage_null"})
    @DisplayName("새 백엔드의 Kafka value(재직렬화)는 키를 싣지 않고, 구 계약은 그걸 거부한다 — 감지기를 먼저 올리는 이유")
    void 새_백엔드의_Kafka_value는_구_계약이_거부한다(String name) throws Exception {
        // TelemetryProducer.send와 같은 경로: decode된 객체를 앱 매퍼로 다시 쓴다.
        String kafkaValue = bootMapper.writeValueAsString(newDecoder.decode(shape(name)));
        JsonNode node = bootMapper.readTree(kafkaValue);

        // NON_NULL — 없던 값을 "fuel_level": null로 만들어 내보내지 않는다(null 입력도 키 생략으로 정규화).
        String missing = name.startsWith("fuel") ? "fuel_level"
            : name.startsWith("voltage") ? "battery_voltage" : null;
        for (String f : new String[] {"fuel_level", "battery_voltage"}) {
            boolean expectAbsent = missing == null || missing.equals(f);
            assertThat(node.has(f))
                .as("%s 키 존재 여부가 틀렸다(null이면 키 자체가 없어야 한다): %s", f, kafkaValue)
                .isEqualTo(!expectAbsent);
        }
        assertThat(node.has("speed") && node.has("rpm") && node.has("engine_temp") && node.has("throttle_position"))
            .as("필수 필드는 그대로 실린다: %s", kafkaValue).isTrue();

        // 구 수신자(구 감지기와 같은 v1 계약)는 이 value를 거부한다.
        assertThat(v1Violations(kafkaValue)).isNotEmpty();
        // 새 수신자는 받는다(spool 드레인·재처리 경로도 같은 decoder를 탄다).
        newDecoder.decode(kafkaValue);
    }

    @Test
    @DisplayName("온전한 payload는 구·신 모두 받는다 — 감지기를 먼저 올려도 안전한 이유")
    void 온전한_payload는_양쪽_다_받는다() throws Exception {
        assertThat(v1Violations(FULL)).isEmpty();
        VehicleTelemetry t = newDecoder.decode(FULL);
        String kafkaValue = bootMapper.writeValueAsString(t);
        assertThat(bootMapper.readTree(kafkaValue).get("fuel_level").asDouble()).isEqualTo(60.0);
        assertThat(bootMapper.readTree(kafkaValue).get("battery_voltage").asDouble()).isEqualTo(13.9);
        assertThat(v1Violations(kafkaValue)).isEmpty();
    }
}
