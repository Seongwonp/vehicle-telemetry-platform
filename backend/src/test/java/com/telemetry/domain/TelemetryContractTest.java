package com.telemetry.domain;

import com.telemetry.support.TestDecoders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * telemetry 입력 계약을 <b>운영과 같은 decoder</b>로 검증한다 (P0-2).
 *
 * <p>이 파일의 fixture는 원래 {@code TelemetryPayloadBoundaryTest}에서
 * <b>변경 전 동작</b>을 고정하던 것이다. 같은 fixture를 그대로 두고 기대값만 뒤집었으므로,
 * 커밋 {@code 80c5230}과 비교하면 어떤 칸이 어떻게 바뀌었는지 그대로 보인다.
 * 변경 전 측정표는 {@code docs/telemetry-schema-decision-table.md} 2절에 있다.
 *
 * <p><b>{@link TestDecoders}로 앱과 같은 매퍼를 쓴다.</b> {@code new ObjectMapper()}로
 * 만들면 {@code FAIL_ON_UNKNOWN_PROPERTIES} 기본값이 달라 운영과 다른 것을 검증하게 된다 —
 * 실제로 그 차이로 결정표의 한 칸이 틀린 적이 있다.
 *
 * <p><b>두 입구가 같은 계약을 쓴다는 것이 이 테스트의 요점이다.</b>
 * MQTT({@code MqttMessageHandler})와 Kafka 저장({@code TelemetryConsumer})이 모두
 * {@link TelemetryDecoder}를 호출하므로, 여기서 거부되는 payload는 양쪽에서 거부된다.
 */
@DisplayName("telemetry 입력 계약")
class TelemetryContractTest {

    private final TelemetryDecoder decoder = TestDecoders.telemetryDecoder();

    /** 계약을 충족하는 payload. 각 fixture는 여기서 한 군데만 바꾼다. */
    private static String base() {
        return """
            {"vehicle_id":"KR-GA-1234","timestamp":"2026-09-09T10:00:00.000Z",
             "speed":87.3,"rpm":2400,"engine_temp":92.1,"throttle_position":34.5,
             "fuel_level":67.0,"battery_voltage":13.8,
             "gps":{"lat":37.123456,"lng":127.654321},"dtc_codes":[]}
            """;
    }

    private void assertRejected(String json, String expectedReason) {
        assertThatThrownBy(() -> decoder.decode(json))
            .isInstanceOf(TelemetryContractException.class)
            .extracting(e -> ((TelemetryContractException) e).getReason())
            .isEqualTo(expectedReason);
    }

    @Test
    @DisplayName("기준선 — 계약을 지킨 payload는 통과한다")
    void 정상_payload() {
        VehicleTelemetry t = decoder.decode(base());

        assertThat(t.getSpeed()).isEqualTo(87.3);
        assertThat(t.getRpm()).isEqualTo(2400.0);
        assertThat(t.getGps().getLat()).isEqualTo(37.123456);
    }

    @Nested
    @DisplayName("숫자 필드 — 변경 전에는 전부 통과하던 칸들")
    class 숫자필드 {

        @Test
        @DisplayName("필드 누락은 거부된다 (변경 전: 0.0으로 저장)")
        void 누락된_숫자필드() {
            assertRejected(base().replace("\"speed\":87.3,", ""),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("명시적 null도 거부된다 (변경 전: 0.0으로 저장)")
        void null_숫자필드() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":null"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("타입 오류는 TYPE_MISMATCH로 거부된다")
        void 타입오류() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":true"),
                TelemetryContractException.TYPE_MISMATCH);
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":[1]"),
                TelemetryContractException.TYPE_MISMATCH);
        }

        @Test
        @DisplayName("숫자 문자열은 **통과한다** — 손실 없는 변환이라 계약에 허용으로 명시")
        void 숫자문자열은_허용() {
            VehicleTelemetry t = decoder.decode(base().replace("\"speed\":87.3", "\"speed\":\"87.3\""));

            assertThat(t.getSpeed()).isEqualTo(87.3);
        }

        @Test
        @DisplayName("빈 문자열은 null이 되어 거부된다")
        void 빈문자열() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":\"\""),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("계약 범위 밖은 거부된다 (변경 전: 그대로 저장)")
        void 범위_밖() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":-40.0"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":255.1"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
            assertRejected(base().replace("\"engine_temp\":92.1", "\"engine_temp\":9999.0"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
            assertRejected(base().replace("\"fuel_level\":67.0", "\"fuel_level\":250.0"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("**경계값은 통과한다** — 상·하한 자체는 계약 안이다")
        void 경계값() {
            assertThat(decoder.decode(base().replace("\"speed\":87.3", "\"speed\":0")).getSpeed())
                .isEqualTo(0.0);
            assertThat(decoder.decode(base().replace("\"speed\":87.3", "\"speed\":255")).getSpeed())
                .isEqualTo(255.0);
            assertThat(decoder.decode(base().replace("\"rpm\":2400", "\"rpm\":16383.75")).getRpm())
                .isEqualTo(16383.75);
            assertThat(decoder.decode(base().replace("\"engine_temp\":92.1", "\"engine_temp\":-40")).getEngineTemp())
                .isEqualTo(-40.0);
        }

        @Test
        @DisplayName("**소수 rpm은 보존된다** (변경 전: 2400으로 잘림)")
        void 소수_rpm_보존() {
            assertThat(decoder.decode(base().replace("\"rpm\":2400", "\"rpm\":2400.7")).getRpm())
                .isEqualTo(2400.7);
            // OBD-II RPM의 실제 해상도인 0.25 단위도 그대로 남는다.
            assertThat(decoder.decode(base().replace("\"rpm\":2400", "\"rpm\":2400.25")).getRpm())
                .isEqualTo(2400.25);
        }

        @Test
        @DisplayName("비유한 값은 거부된다 — 범위 제약이 NaN·Infinity를 함께 잡는다")
        void 비유한값() {
            // NaN 리터럴은 JSON이 아니라 파싱에서 막힌다.
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":NaN"),
                TelemetryContractException.MALFORMED_JSON);
            // 1e309는 유효한 JSON이고 Infinity가 되는데, @DecimalMax가 잡는다.
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":1e309"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }
    }

    @Nested
    @DisplayName("이상 감지 대상 값은 반드시 통과해야 한다")
    class 이상감지_통과 {

        @Test
        @DisplayName("**감지 임계값을 넘는 값이 계약 안에 있다** — 좁으면 감지가 무의미해진다")
        void 이상값이_통과한다() {
            // 감지 룰: speed>200, rpm>6000, engine_temp>105, battery<11.5 또는 >15
            assertThat(decoder.decode(base().replace("\"speed\":87.3", "\"speed\":201")).getSpeed())
                .isEqualTo(201.0);
            assertThat(decoder.decode(base().replace("\"rpm\":2400", "\"rpm\":6001")).getRpm())
                .isEqualTo(6001.0);
            assertThat(decoder.decode(base().replace("\"engine_temp\":92.1", "\"engine_temp\":106")).getEngineTemp())
                .isEqualTo(106.0);
            assertThat(decoder.decode(base().replace("\"battery_voltage\":13.8", "\"battery_voltage\":10.0")).getBatteryVoltage())
                .isEqualTo(10.0);
            assertThat(decoder.decode(base().replace("\"battery_voltage\":13.8", "\"battery_voltage\":16.0")).getBatteryVoltage())
                .isEqualTo(16.0);
            // 시뮬레이터가 실제로 주입하는 이상값의 상한도 통과해야 한다.
            assertThat(decoder.decode(base().replace("\"speed\":87.3", "\"speed\":230")).getSpeed())
                .isEqualTo(230.0);
        }
    }

    @Nested
    @DisplayName("필수 필드")
    class 필수필드 {

        @Test
        @DisplayName("vehicle_id 누락·형식 위반은 거부된다 (변경 전: Kafka 직접 주입은 통과)")
        void vehicle_id() {
            assertRejected(base().replace("\"vehicle_id\":\"KR-GA-1234\",", ""),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
            assertRejected(base().replace("\"vehicle_id\":\"KR-GA-1234\"", "\"vehicle_id\":\"소문자!\""),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("timestamp 누락은 거부된다 (변경 전: Kafka 직접 주입은 통과)")
        void timestamp() {
            assertRejected(base().replace("\"timestamp\":\"2026-09-09T10:00:00.000Z\",", ""),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }
    }

    @Nested
    @DisplayName("중첩 객체와 배열")
    class 중첩과배열 {

        @Test
        @DisplayName("gps가 아예 없으면 통과한다 — 선택 필드다(실내·터널)")
        void gps_누락은_허용() {
            VehicleTelemetry t = decoder.decode(
                base().replace("\"gps\":{\"lat\":37.123456,\"lng\":127.654321},", ""));

            assertThat(t.getGps()).isNull();
        }

        @Test
        @DisplayName("**gps가 있는데 lat이 없으면 거부된다** (변경 전: lat=0으로 저장)")
        void gps_안의_lat_누락() {
            assertRejected(base().replace("\"lat\":37.123456,", ""),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("**정상 좌표 0은 누락과 구분되어 통과한다**")
        void 좌표_0은_정상값() {
            VehicleTelemetry t = decoder.decode(base().replace("\"lat\":37.123456", "\"lat\":0"));

            assertThat(t.getGps().getLat()).isEqualTo(0.0);
        }

        @Test
        @DisplayName("gps 좌표 범위 밖은 거부된다")
        void gps_범위() {
            assertRejected(base().replace("\"lat\":37.123456", "\"lat\":91"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
            assertRejected(base().replace("\"lng\":127.654321", "\"lng\":181"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("dtc_codes 누락과 빈 배열은 둘 다 통과한다 — 다른 뜻이지만 둘 다 유효하다")
        void dtc_누락과_빈배열() {
            // 누락 = 이 장치가 DTC를 보고하지 않음. 빈 배열 = 보고했고 코드가 없음.
            // 저장 단계에서는 둘 다 필드가 생략되므로 현재는 구분되지 않는다(한계로 기록).
            assertThat(decoder.decode(base().replace(",\"dtc_codes\":[]", "")).getDtcCodes()).isNull();
            assertThat(decoder.decode(base()).getDtcCodes()).isEmpty();
        }

        @Test
        @DisplayName("**dtc_codes의 null 원소는 거부된다** (변경 전: 문자열 \"null\"로 저장)")
        void dtc_null_원소() {
            assertRejected(base().replace("\"dtc_codes\":[]", "\"dtc_codes\":[null]"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("**쉼표가 든 DTC는 거부된다** (변경 전: 코드 2개와 구분 불가하게 저장)")
        void dtc_쉼표_충돌() {
            assertRejected(base().replace("\"dtc_codes\":[]", "\"dtc_codes\":[\"P0301,P0420\"]"),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("정상 DTC 코드는 통과한다")
        void dtc_정상() {
            VehicleTelemetry t = decoder.decode(
                base().replace("\"dtc_codes\":[]", "\"dtc_codes\":[\"P0301\",\"B1234\",\"C0561\",\"U0100\"]"));

            assertThat(t.getDtcCodes()).hasSize(4);
        }
    }

    @Nested
    @DisplayName("알 수 없는 필드")
    class 알수없는필드 {

        @Test
        @DisplayName("**필드명 오타가 거부된다** (변경 전: 조용히 버려지고 speed=0.0)")
        void 오타는_거부된다() {
            assertRejected(base().replace("\"speed\":87.3", "\"sped\":87.3"),
                TelemetryContractException.UNKNOWN_FIELD);
        }

        @Test
        @DisplayName("계약에 없는 여분 필드도 거부된다")
        void 여분_필드() {
            assertRejected(base().replace("\"dtc_codes\":[]", "\"dtc_codes\":[],\"extra\":1"),
                TelemetryContractException.UNKNOWN_FIELD);
        }
    }

    /**
     * <b>이 계약은 "JSON 숫자 타입만 허용"이 아니다.</b> 숫자 문자열은 통과한다 —
     * 결정이지 누락이 아니다. 근거와 대가는
     * {@code docs/telemetry-schema-decision-table.md} 3절에 있다.
     *
     * <p>여기 각 칸이 그 문서의 표와 1:1로 대응한다. 한쪽만 고치면 둘이 갈라진다.
     */
    @Nested
    @DisplayName("숫자 문자열 — 허용하되 범위는 그대로 건다")
    class 숫자_문자열 {

        @Test
        @DisplayName("숫자 문자열은 통과하고 값이 보존된다")
        void 통과한다() {
            VehicleTelemetry t = decoder.decode(base().replace("\"speed\":87.3", "\"speed\":\"87.3\""));
            assertThat(t.getSpeed()).isEqualTo(87.3);
        }

        @Test
        @DisplayName("문자열이어도 범위는 그대로 걸린다 — 변환은 되고 검증에서 막힌다")
        void 범위는_걸린다() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":\"300\""),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("빈 문자열은 null이 되고 @NotNull이 잡는다")
        void 빈_문자열() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":\"\""),
                TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        }

        @Test
        @DisplayName("숫자가 아닌 문자열은 TYPE_MISMATCH")
        void 변환_불가() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":\"fast\""),
                TelemetryContractException.TYPE_MISMATCH);
        }

        @Test
        @DisplayName("boolean·배열·객체는 강제 변환 대상이 아니다 — TYPE_MISMATCH")
        void 다른_타입은_거부() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":true"),
                TelemetryContractException.TYPE_MISMATCH);
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":[1]"),
                TelemetryContractException.TYPE_MISMATCH);
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":{}"),
                TelemetryContractException.TYPE_MISMATCH);
        }
    }

    @Nested
    @DisplayName("JSON 자체가 깨진 경우")
    class 깨진_JSON {

        @Test
        @DisplayName("MALFORMED_JSON으로 거부된다")
        void 깨진_json() {
            assertRejected("{not-valid-json", TelemetryContractException.MALFORMED_JSON);
        }
    }

    /**
     * <b>decoder가 계약 위반을 전부 {@link TelemetryContractException}으로 감싸는가.</b>
     *
     * <p>이 질문이 왜 중요한가 — 두 입구의 catch 범위가 다르다.
     * {@code TelemetryConsumer}는 {@code catch (Exception)}이라 무엇이 나와도 그 레코드만
     * DLQ로 가지만, {@code MqttMessageHandler}는 {@code TelemetryContractException}만
     * 잡는다. 다른 예외가 새면 <b>MQTT 쪽은 거부가 아니라 핸들러 밖으로 전파</b>된다.
     * 그리고 Kafka 쪽도 낯선 예외 타입은 {@code dlq-tools/dlq.py}에서 {@code unknown}으로
     * 분류된다(2026-09-06에 실제로 겪은 일이다).
     *
     * <p>그래서 "레코드별 try/catch가 있으니 안전하다"로 끝내지 않고
     * <b>새는 예외가 있는지 직접 던져본다.</b>
     */
    /**
     * <b>사유 선택 순서를 고정한다.</b>
     *
     * <p>예전에는 {@code readValue} 한 번으로 끝냈고, Jackson이 문서를 앞에서부터 읽다가
     * 처음 만난 문제에서 멈추므로 <b>사유가 payload의 필드 순서에 따라 달라졌다.</b>
     * 같은 오류 조합인데 필드 순서만 바꾸면 다른 사유가 나오고, 잘린 JSON이
     * {@code UNKNOWN_FIELD}로 보고됐다 — 운영자가 없는 필드명을 고치러 간다.
     *
     * <p>순서는 {@code docs/anomaly-path-contract.md}에 적혀 있고
     * {@code anomaly-detector/contract.py}가 같은 순서를 구현한다.
     */
    @Nested
    @DisplayName("사유 선택 순서 — payload 순서에 흔들리지 않는다")
    class 사유_순서 {

        @Test
        @DisplayName("필드 순서를 바꿔도 같은 사유가 나온다")
        void 순서_무관() {
            String unknownFirst = base().replace("\"speed\":87.3", "\"zzz\":1,\"speed\":\"fast\"");
            String typeFirst = base().replace("\"speed\":87.3", "\"speed\":\"fast\",\"zzz\":1");

            assertRejected(unknownFirst, TelemetryContractException.UNKNOWN_FIELD);
            assertRejected(typeFirst, TelemetryContractException.UNKNOWN_FIELD);
        }

        @Test
        @DisplayName("잘린 JSON은 MALFORMED_JSON이다 — 앞에 unknown 필드가 있어도")
        void 잘린_json이_먼저() {
            // 이 칸이 바뀐 동작이다. 예전에는 UNKNOWN_FIELD로 나왔다.
            assertRejected("{\"zzz\":1,\"speed\":", TelemetryContractException.MALFORMED_JSON);
        }

        @Test
        @DisplayName("unknown 필드가 범위 위반보다 먼저다")
        void unknown이_검증보다_먼저() {
            assertRejected(base().replace("\"speed\":87.3", "\"zzz\":1,\"speed\":300.0"),
                TelemetryContractException.UNKNOWN_FIELD);
        }

        @Test
        @DisplayName("타입 오류가 범위 위반보다 먼저다")
        void 타입이_검증보다_먼저() {
            assertRejected(base().replace("\"speed\":87.3", "\"speed\":\"fast\",\"rpm\":99999"),
                TelemetryContractException.TYPE_MISMATCH);
        }

        @Test
        @DisplayName("unknown 필드가 여럿이면 이름순 첫 번째부터 전부 보고한다")
        void unknown_여럿은_정렬() {
            assertThatThrownBy(() -> decoder.decode(
                    base().replace("\"speed\":87.3", "\"zzz\":1,\"aaa\":2,\"speed\":87.3")))
                .isInstanceOf(TelemetryContractException.class)
                .hasMessageContaining("aaa,zzz");
        }

        @Test
        @DisplayName("범위 위반이 여럿이면 정렬해서 보고한다 — 같은 입력은 같은 문자열")
        void 검증_위반은_정렬() {
            String payload = base()
                .replace("\"speed\":87.3", "\"speed\":300.0")
                .replace("\"fuel_level\":67.0", "\"fuel_level\":200.0");
            assertThatThrownBy(() -> decoder.decode(payload))
                .isInstanceOf(TelemetryContractException.class)
                .satisfies(e -> {
                    String m = e.getMessage();
                    assertThat(m.indexOf("fuelLevel"))
                        .as("정렬되면 fuelLevel이 speed보다 앞이다: " + m)
                        .isLessThan(m.indexOf("speed"));
                });
        }
    }

    @Test
    @DisplayName("계약 필드 집합이 클래스와 일치한다 — 필드를 늘리고 목록을 안 고치면 잡힌다")
    void 계약_필드_집합이_클래스와_일치한다() {
        java.util.Set<String> fromClass = java.util.Arrays
            .stream(VehicleTelemetry.class.getDeclaredFields())
            // static은 계약 필드가 아니다 — TIMESTAMP_PATTERN 같은 상수가 여기 섞이면
            // 목록이 영원히 안 맞는다.
            .filter(f -> !f.isSynthetic() && !java.lang.reflect.Modifier.isStatic(f.getModifiers()))
            .map(f -> {
                com.fasterxml.jackson.annotation.JsonProperty a =
                    f.getAnnotation(com.fasterxml.jackson.annotation.JsonProperty.class);
                return a != null ? a.value() : f.getName();
            })
            .collect(java.util.stream.Collectors.toSet());

        assertThat(TelemetryDecoder.KNOWN_FIELDS)
            .as("TelemetryDecoder.KNOWN_FIELDS가 VehicleTelemetry와 어긋난다. "
                + "필드를 추가했으면 목록에도 넣어라 — 안 넣으면 정상 payload가 UNKNOWN_FIELD로 거부된다")
            .isEqualTo(fromClass);
    }

    @Nested
    @DisplayName("적대적 입력도 계약 예외로만 나오는가")
    class 예외_타입_봉인 {

        private void assertOnlyContractException(String json, String label) {
            assertThatThrownBy(() -> decoder.decode(json))
                .as(label + " — 계약 예외 밖으로 새면 MQTT 입구에서 거부가 아니라 전파된다")
                .isInstanceOf(TelemetryContractException.class);
        }

        @Test
        @DisplayName("빈 문자열·공백·JSON이 아닌 텍스트")
        void 빈_입력() {
            assertOnlyContractException("", "빈 문자열");
            assertOnlyContractException("   ", "공백");
            assertOnlyContractException("hello", "평문");
        }

        @Test
        @DisplayName("최상위 타입이 객체가 아닌 경우")
        void 최상위_타입() {
            assertOnlyContractException("[]", "배열");
            assertOnlyContractException("123", "숫자");
            assertOnlyContractException("\"str\"", "문자열");
            assertOnlyContractException("true", "boolean");
        }

        /**
         * <b>이 테스트가 실제로 구멍을 하나 찾았다(2026-09-09).</b>
         * payload가 {@code null} 네 글자면 {@code readValue}는 예외 없이 null을 돌려주고,
         * 그걸 {@code validate()}에 넘기면 Hibernate Validator가
         * {@code IllegalArgumentException: HV000116}을 던졌다 — 계약 예외가 아니다.
         * MQTT 입구는 그걸 안 잡으므로 거부가 아니라 전파됐다.
         * decoder에 null 가드를 넣어 {@code TYPE_MISMATCH}로 막았다.
         */
        @Test
        @DisplayName("JSON null 리터럴 — 계약 예외로 막힌다 (구멍이었다)")
        void 널_리터럴() {
            assertOnlyContractException("null", "null 리터럴");
            assertRejected("null", TelemetryContractException.TYPE_MISMATCH);
        }

        @Test
        @DisplayName("깊게 중첩된 JSON — 파서 상한에 걸려도 계약 예외여야 한다")
        void 깊은_중첩() {
            assertOnlyContractException("[".repeat(5000) + "]".repeat(5000), "5000단 중첩");
        }

        @Test
        @DisplayName("자릿수가 극단적인 숫자")
        void 거대_숫자() {
            assertOnlyContractException(
                base().replace("\"speed\":87.3", "\"speed\":" + "9".repeat(10000)),
                "1만 자리 정수");
        }
    }
}
