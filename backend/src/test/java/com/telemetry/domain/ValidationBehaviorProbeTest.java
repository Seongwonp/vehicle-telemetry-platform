package com.telemetry.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * P0-2 구현 전에 <b>이 버전들이 실제로 어떻게 동작하는지</b> 잰다.
 *
 * <p>설계에 필요한 사실을 문서에서 읽고 가정하지 않는다. 확인 대상:
 * <ul>
 *   <li>{@code @Pattern}이 null을 어떻게 보는가 (DTC 원소 검증 설계에 직결)</li>
 *   <li>{@code @DecimalMin/@DecimalMax}가 NaN·Infinity를 거르는가</li>
 *   <li>Jackson이 숫자 필드에 문자열·빈 문자열·boolean·배열을 어떻게 강제 변환하는가</li>
 * </ul>
 *
 * <p>측정에 쓴 버전: Spring Boot 3.2.5 / jackson-databind 2.15.4 /
 * hibernate-validator 8.0.1.Final / jakarta.validation-api 3.0.2.
 * <b>버전을 올리면 이 표가 달라질 수 있다.</b>
 */
@DisplayName("검증·역직렬화 동작 실측 (설계 근거)")
class ValidationBehaviorProbeTest {

    private static final Validator VALIDATOR =
        Validation.buildDefaultValidatorFactory().getValidator();

    private static final ObjectMapper BOOT_MAPPER = bootMapper();

    private static ObjectMapper bootMapper() {
        AtomicReference<ObjectMapper> ref = new AtomicReference<>();
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
            .run(context -> ref.set(context.getBean(ObjectMapper.class)));
        return ref.get();
    }

    // ── @Pattern과 null ────────────────────────────────────────
    static class PatternHolder {
        @Pattern(regexp = "^[PBCU][0-9]{4}$")
        String code;

        PatternHolder(String code) { this.code = code; }
    }

    static class PatternAndNotNullHolder {
        @NotNull
        @Pattern(regexp = "^[PBCU][0-9]{4}$")
        String code;

        PatternAndNotNullHolder(String code) { this.code = code; }
    }

    @Test
    @DisplayName("@Pattern만으로는 null이 통과한다 — DTC 원소에 @NotNull이 같이 필요하다")
    void pattern_은_null을_통과시킨다() {
        assertThat(VALIDATOR.validate(new PatternHolder(null))).isEmpty();
        assertThat(VALIDATOR.validate(new PatternHolder("P0301"))).isEmpty();
        assertThat(VALIDATOR.validate(new PatternHolder("nope"))).isNotEmpty();

        // @NotNull을 같이 붙여야 null이 걸린다.
        assertThat(VALIDATOR.validate(new PatternAndNotNullHolder(null))).isNotEmpty();
    }

    // ── 컨테이너 원소 검증 ─────────────────────────────────────
    static class ElementHolder {
        List<@NotNull @Pattern(regexp = "^[PBCU][0-9]{4}$") String> codes;

        ElementHolder(List<String> codes) { this.codes = codes; }
    }

    @Test
    @DisplayName("List 원소 제약이 실제로 걸린다 — null 원소와 형식 위반을 둘 다 잡는다")
    void 리스트_원소_제약이_동작한다() {
        assertThat(VALIDATOR.validate(new ElementHolder(List.of("P0301")))).isEmpty();
        // List.of는 null을 못 담으므로 nullable 리스트로 만든다.
        java.util.List<String> withNull = new java.util.ArrayList<>();
        withNull.add(null);
        assertThat(VALIDATOR.validate(new ElementHolder(withNull))).isNotEmpty();
        assertThat(VALIDATOR.validate(new ElementHolder(List.of("P0301,P0420")))).isNotEmpty();
        assertThat(VALIDATOR.validate(new ElementHolder(List.of("")))).isNotEmpty();
    }

    // ── 숫자 범위 제약과 NaN/Infinity ──────────────────────────
    static class RangeHolder {
        @NotNull
        @DecimalMin("0") @DecimalMax("255")
        Double speed;

        RangeHolder(Double speed) { this.speed = speed; }
    }

    @Test
    @DisplayName("**@DecimalMin/@DecimalMax가 NaN·Infinity를 어떻게 보는가** — 설계에 직결된다")
    void 범위제약과_비유한값() {
        assertThat(VALIDATOR.validate(new RangeHolder(87.3))).isEmpty();
        assertThat(VALIDATOR.validate(new RangeHolder(-1.0))).isNotEmpty();
        assertThat(VALIDATOR.validate(new RangeHolder(255.1))).isNotEmpty();
        assertThat(VALIDATOR.validate(new RangeHolder(null))).isNotEmpty();

        // 여기가 핵심이다. 아래 두 단언이 이 버전의 실제 동작이다.
        boolean nanRejected = !VALIDATOR.validate(new RangeHolder(Double.NaN)).isEmpty();
        boolean posInfRejected = !VALIDATOR.validate(new RangeHolder(Double.POSITIVE_INFINITY)).isEmpty();
        boolean negInfRejected = !VALIDATOR.validate(new RangeHolder(Double.NEGATIVE_INFINITY)).isEmpty();

        VALIDATOR.validate(new RangeHolder(Double.NaN)).forEach(v ->
            System.out.println("[PROBE] NaN 위반: " + v.getPropertyPath() + " " + v.getMessage()
                + " / " + v.getConstraintDescriptor().getAnnotation().annotationType().getSimpleName()));
        System.out.println("[PROBE] NaN 거부=" + nanRejected
            + " +Inf 거부=" + posInfRejected + " -Inf 거부=" + negInfRejected);

        // +Infinity는 255를 넘으므로 @DecimalMax가 잡을 것으로 예상한다.
        assertThat(posInfRejected).isTrue();
        // -Infinity는 0 미만이므로 @DecimalMin이 잡을 것으로 예상한다.
        assertThat(negInfRejected).isTrue();
        // **NaN은 어떤 비교도 false라서 범위 제약으로 안 걸릴 것으로 예상한다.**
        // 그렇다면 유한값 검사를 따로 둬야 한다(toPoint의 finite()가 그 역할을 한다).
        // **예상이 틀렸다.** NaN도 거부된다 — 아래 PROBE 출력이 어느 제약이 잡는지 보여준다.
        // 이걸 안 쟀으면 "범위 제약은 NaN을 못 잡는다"고 가정하고 별도 유한값 검사를
        // 검증 계층에 하나 더 넣을 뻔했다.
        assertThat(nanRejected).isTrue();
    }

    // ── Jackson 강제 변환 ──────────────────────────────────────
    record NumberBox(Double speed) { }

    private Double coerce(String jsonValue) throws Exception {
        return BOOT_MAPPER.readValue("{\"speed\":" + jsonValue + "}", NumberBox.class).speed();
    }

    @Test
    @DisplayName("숫자 문자열은 조용히 변환된다 — 계약에 명시해야 한다")
    void 숫자문자열은_변환된다() throws Exception {
        assertThat(coerce("\"87.3\"")).isEqualTo(87.3);
    }

    @Test
    @DisplayName("빈 문자열은 null이 된다 — @NotNull이 있어야 걸린다")
    void 빈문자열은_null이_된다() throws Exception {
        assertThat(coerce("\"\"")).isNull();
    }

    @Test
    @DisplayName("boolean은 거부된다")
    void boolean은_거부된다() {
        assertThatThrownBy(() -> coerce("true"))
            .isInstanceOf(com.fasterxml.jackson.databind.exc.MismatchedInputException.class);
    }

    @Test
    @DisplayName("배열·객체는 거부된다")
    void 배열과_객체는_거부된다() {
        assertThatThrownBy(() -> coerce("[1]"))
            .isInstanceOf(com.fasterxml.jackson.databind.exc.MismatchedInputException.class);
        assertThatThrownBy(() -> coerce("{\"v\":1}"))
            .isInstanceOf(com.fasterxml.jackson.databind.exc.MismatchedInputException.class);
    }

    @Test
    @DisplayName("**NaN·Infinity 리터럴은 기본 매퍼가 거부한다** (2026-09-08 실측 재확인)")
    void 비유한_리터럴은_거부된다() {
        assertThatThrownBy(() -> coerce("NaN"))
            .isInstanceOf(com.fasterxml.jackson.core.JsonParseException.class);
        // 1e309는 유효한 JSON이라 파싱에 성공하고 Infinity가 된다.
        assertThatThrownBy(() -> {
            Double v = coerce("1e309");
            assertThat(v).isInfinite();
            throw new IllegalStateException("파싱 성공 — Infinity=" + v);
        }).isInstanceOf(IllegalStateException.class);
    }
}
