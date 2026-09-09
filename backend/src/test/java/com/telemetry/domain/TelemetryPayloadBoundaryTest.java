package com.telemetry.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.concurrent.atomic.AtomicReference;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * telemetry payload 경계의 <b>현재 동작</b>을 고정한다 (P0-2 strict schema의 1단계).
 *
 * <p><b>이 테스트는 "옳은 동작"이 아니라 "지금 동작"을 적은 것이다.</b>
 * 2026-09-09 감사가 "누락 숫자 필드가 0으로 조용히 변환될 수 있다"를 P0으로 올렸고,
 * 정책을 정하기 전에 <b>각 칸이 실제로 어떻게 되는지부터</b> 재기로 했다.
 * 코드를 읽고 표를 쓰면 틀린다 — 이 프로젝트에서 그런 추론이 여러 번 빗나갔다.
 *
 * <p><b>두 입구의 차이가 이 테스트의 핵심이다.</b>
 * <ul>
 *   <li>MQTT: {@code MqttMessageHandler}가 역직렬화 → Bean Validation → timestamp →
 *       topic/payload 차량 ID를 차례로 검사하고, 실패하면 거부한다.</li>
 *   <li>Kafka 직접 주입: {@code TelemetryConsumer}는 {@code readValue}만 한다.
 *       <b>Bean Validation을 전혀 거치지 않는다.</b></li>
 * </ul>
 * 그래서 아래에서 "역직렬화 성공 + 위반 0"이면 <b>두 입구 모두 통과</b>이고,
 * "역직렬화 성공 + 위반 있음"이면 <b>MQTT는 막지만 Kafka는 통과</b>다.
 * 이 차이가 감사가 지적한 "다른 producer가 입력 계약을 우회한다"의 실체다.
 *
 * <p>정책을 바꾸면 여기 단언들이 깨진다. <b>깨지는 것이 목적이다</b> —
 * 결정표(`docs/telemetry-schema-decision-table.md`)와 이 파일이 같이 움직여야 한다.
 */
@DisplayName("telemetry payload 경계 — 현재 동작")
class TelemetryPayloadBoundaryTest {

    /**
     * <b>앱이 실제로 쓰는 매퍼를 가져온다.</b> 처음엔 {@code new ObjectMapper()}로 짰는데,
     * 그건 {@code FAIL_ON_UNKNOWN_PROPERTIES}가 <b>켜져 있어</b> 모르는 필드에서 예외를 던진다.
     * Spring Boot의 자동 구성 매퍼는 그것을 <b>끈다</b>. 즉 raw 매퍼로 잰 표는
     * <b>운영과 다른 것을 재는 표</b>다 — 실제로 첫 실행에서 이 차이 때문에 한 칸이 틀렸다.
     *
     * <p>어제 `session.timeout.ms` 계약 테스트에서 "테스트에 값을 다시 적으면 운영 설정을
     * 못 잡는다"를 배웠는데, 여기서는 <b>매퍼 자체</b>가 같은 함정이었다.
     */
    private static final ObjectMapper MAPPER = bootObjectMapper();

    private static ObjectMapper bootObjectMapper() {
        AtomicReference<ObjectMapper> ref = new AtomicReference<>();
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
            .run(context -> ref.set(context.getBean(ObjectMapper.class)));
        return ref.get();
    }

    private static final Validator VALIDATOR;

    static {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        VALIDATOR = factory.getValidator();
    }

    /** 정상 payload. 각 fixture는 여기서 한 군데만 바꾼다. */
    private static String base() {
        return """
            {"vehicle_id":"KR-GA-1234","timestamp":"2026-09-09T10:00:00.000Z",
             "speed":87.3,"rpm":2400,"engine_temp":92.1,"throttle_position":34.5,
             "fuel_level":67.0,"battery_voltage":13.8,
             "gps":{"lat":37.123456,"lng":127.654321},"dtc_codes":[]}
            """;
    }

    private VehicleTelemetry deserialize(String json) throws Exception {
        return MAPPER.readValue(json, VehicleTelemetry.class);
    }

    private Set<ConstraintViolation<VehicleTelemetry>> violations(VehicleTelemetry t) {
        return VALIDATOR.validate(t);
    }

    @Test
    @DisplayName("기준선 — 정상 payload는 두 입구 모두 통과한다")
    void 정상_payload() throws Exception {
        VehicleTelemetry t = deserialize(base());

        assertThat(violations(t)).isEmpty();
        assertThat(t.getSpeed()).isEqualTo(87.3);
        assertThat(t.getRpm()).isEqualTo(2400);
    }

    @Nested
    @DisplayName("숫자 필드 — 감사가 P0으로 올린 구간")
    class 숫자필드 {

        @Test
        @DisplayName("**필드가 없으면 0이 되고 아무 데서도 안 걸린다** (두 입구 모두 통과)")
        void 누락된_숫자필드는_0이_된다() throws Exception {
            String json = base().replace("\"speed\":87.3,", "");

            VehicleTelemetry t = deserialize(json);

            // primitive double이라 Jackson이 기본값 0.0을 넣는다. @NotNull을 붙일 수도 없다
            // (primitive는 null이 될 수 없어서 항상 통과한다).
            assertThat(t.getSpeed()).isEqualTo(0.0);
            // **검증도 통과한다.** 그래서 "속도 0으로 달리는 차"가 정상 데이터로 저장된다.
            assertThat(violations(t)).isEmpty();
        }

        @Test
        @DisplayName("**명시적 null도 0이 되고 안 걸린다** (두 입구 모두 통과)")
        void null_숫자필드는_0이_된다() throws Exception {
            String json = base().replace("\"speed\":87.3", "\"speed\":null");

            VehicleTelemetry t = deserialize(json);

            assertThat(t.getSpeed()).isEqualTo(0.0);
            assertThat(violations(t)).isEmpty();
        }

        @Test
        @DisplayName("타입이 틀리면 역직렬화에서 막힌다 (두 입구 모두 거부)")
        void 문자열을_숫자필드에_넣으면_역직렬화가_막는다() {
            String json = base().replace("\"speed\":87.3", "\"speed\":\"fast\"");

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> deserialize(json))
                .isInstanceOf(com.fasterxml.jackson.databind.exc.InvalidFormatException.class);
        }

        @Test
        @DisplayName("**물리적으로 불가능한 값도 안 걸린다** — 범위 제약이 하나도 없다")
        void 범위를_벗어난_값은_안_걸린다() throws Exception {
            String json = base()
                .replace("\"speed\":87.3", "\"speed\":-40.0")
                .replace("\"engine_temp\":92.1", "\"engine_temp\":9999.0")
                .replace("\"fuel_level\":67.0", "\"fuel_level\":250.0");

            VehicleTelemetry t = deserialize(json);

            // 후진 속도 -40, 엔진 온도 9999도, 연료 250%가 전부 정상으로 통과한다.
            // 이상 감지 룰(engine_temp > 105)은 이걸 이상으로 잡지만, **저장은 그대로 된다.**
            assertThat(violations(t)).isEmpty();
            assertThat(t.getSpeed()).isEqualTo(-40.0);
            assertThat(t.getEngineTemp()).isEqualTo(9999.0);
        }

        @Test
        @DisplayName("**소수 rpm은 잘려서 통과한다** (2026-09-08 실측을 단위 테스트로 고정)")
        void 소수_rpm은_잘린다() throws Exception {
            String json = base().replace("\"rpm\":2400", "\"rpm\":2400.7");

            VehicleTelemetry t = deserialize(json);

            assertThat(t.getRpm()).isEqualTo(2400);   // .7이 사라진다
            assertThat(violations(t)).isEmpty();
        }
    }

    @Nested
    @DisplayName("필수 필드 — 여기만 제약이 걸려 있다")
    class 필수필드 {

        @Test
        @DisplayName("vehicle_id가 없으면 MQTT는 막고 **Kafka 직접 주입은 통과한다**")
        void vehicle_id_누락() throws Exception {
            String json = base().replace("\"vehicle_id\":\"KR-GA-1234\",", "");

            VehicleTelemetry t = deserialize(json);   // 역직렬화는 성공한다

            // MQTT 입구: @NotBlank 위반 → PAYLOAD_VALIDATION_FAILED로 거부
            assertThat(violations(t)).isNotEmpty();
            // Kafka 직접 주입: 검증을 안 하므로 vehicleId=null로 저장 단계까지 간다.
            assertThat(t.getVehicleId()).isNull();
        }

        @Test
        @DisplayName("timestamp가 없으면 MQTT는 막고 **Kafka 직접 주입은 통과한다**")
        void timestamp_누락() throws Exception {
            String json = base().replace("\"timestamp\":\"2026-09-09T10:00:00.000Z\",", "");

            VehicleTelemetry t = deserialize(json);

            assertThat(violations(t)).isNotEmpty();
            assertThat(t.getTimestamp()).isNull();
        }

        @Test
        @DisplayName("vehicle_id 형식이 틀리면 MQTT는 막고 **Kafka 직접 주입은 통과한다**")
        void vehicle_id_형식위반() throws Exception {
            String json = base().replace("\"vehicle_id\":\"KR-GA-1234\"", "\"vehicle_id\":\"소문자 아이디!\"");

            VehicleTelemetry t = deserialize(json);

            assertThat(violations(t)).isNotEmpty();   // @Pattern 위반
        }
    }

    @Nested
    @DisplayName("중첩 객체와 배열")
    class 중첩과배열 {

        @Test
        @DisplayName("gps가 아예 없으면 통과한다 — 위치는 선택 필드다")
        void gps_누락() throws Exception {
            String json = base().replace("\"gps\":{\"lat\":37.123456,\"lng\":127.654321},", "");

            VehicleTelemetry t = deserialize(json);

            assertThat(t.getGps()).isNull();
            assertThat(violations(t)).isEmpty();
            // toPoint()가 null 체크를 하므로 lat/lng 필드 없이 저장된다. 이건 의도된 동작이다.
        }

        @Test
        @DisplayName("**gps는 있는데 lat이 없으면 0.0이 된다** — 적도 한가운데로 저장된다")
        void gps_안의_lat_누락() throws Exception {
            String json = base().replace("\"lat\":37.123456,", "");

            VehicleTelemetry t = deserialize(json);

            assertThat(t.getGps().getLat()).isEqualTo(0.0);
            assertThat(violations(t)).isEmpty();
            // lat=0, lng=127.65는 실재하는 좌표다(인도네시아 앞바다). 값이 비어 보이지 않는다.
        }

        @Test
        @DisplayName("dtc_codes가 없으면 null이고 통과한다")
        void dtc_codes_누락() throws Exception {
            String json = base().replace(",\"dtc_codes\":[]", "");

            VehicleTelemetry t = deserialize(json);

            assertThat(t.getDtcCodes()).isNull();
            assertThat(violations(t)).isEmpty();
            // toPoint()가 null/빈 배열을 건너뛰므로 저장은 안전하다.
        }

        @Test
        @DisplayName("**dtc_codes의 null 원소는 통과한다** (2026-09-09 실측을 단위 테스트로 고정)")
        void dtc_codes_null_원소() throws Exception {
            String json = base().replace("\"dtc_codes\":[]", "\"dtc_codes\":[null]");

            VehicleTelemetry t = deserialize(json);

            assertThat(t.getDtcCodes()).containsExactly((String) null);
            assertThat(violations(t)).isEmpty();
            // toPoint()의 String.join이 이걸 "null"이라는 네 글자로 만든다.
        }

        @Test
        @DisplayName("**DTC 코드 형식은 검사하지 않는다** — 아무 문자열이나 통과한다")
        void dtc_codes_형식_미검사() throws Exception {
            String json = base().replace("\"dtc_codes\":[]", "\"dtc_codes\":[\"P0301,P0420\",\"안녕\",\"\"]");

            VehicleTelemetry t = deserialize(json);

            assertThat(violations(t)).isEmpty();
            // 쉼표가 든 코드는 저장 후 코드 2개와 구분되지 않는다(2026-09-09 실측).
        }
    }

    @Nested
    @DisplayName("알 수 없는 필드")
    class 알수없는필드 {

        @Test
        @DisplayName("모르는 필드는 조용히 버려진다 — 오타가 안 잡힌다")
        void 모르는_필드는_무시된다() throws Exception {
            // "speed"를 "sped"로 잘못 보내면 speed는 0이 되고 sped는 버려진다.
            String json = base().replace("\"speed\":87.3", "\"sped\":87.3");

            VehicleTelemetry t = deserialize(json);

            assertThat(t.getSpeed()).isEqualTo(0.0);
            assertThat(violations(t)).isEmpty();
            // FAIL_ON_UNKNOWN_PROPERTIES가 꺼져 있다(Boot 기본값).
            // 필드명 오타 하나가 "속도 0"으로 저장되고 아무 신호가 없다.
        }
    }
}
