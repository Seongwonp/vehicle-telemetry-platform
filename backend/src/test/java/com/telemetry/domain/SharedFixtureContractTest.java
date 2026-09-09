package com.telemetry.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.support.TestDecoders;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 공유 fixture로 <b>저장 경로의 계약</b>을 검사한다 (P0-2a).
 *
 * <h2>왜 파일 하나를 양쪽이 읽나</h2>
 *
 * 같은 입력에 대한 판정을 Java와 Python이 <b>각자 자기 테스트에 적어두면 갈라진다.</b>
 * 그리고 갈라진 것은 <b>양쪽 다 자기 테스트를 통과하므로 측정에서 안 드러난다</b> —
 * compose 환경변수를 YAML 앵커 하나로 묶은 것과 같은 이유다(2026-09-07).
 *
 * <p>그래서 입력과 기대 판정을 {@code contract-fixtures/cases.json}에 두고,
 * 이 테스트가 {@code storage} 칸을, {@code anomaly-detector/tests/test_shared_fixtures.py}가
 * {@code detector} 칸을 읽는다.
 *
 * <h2>두 칸이 같아야 한다는 뜻이 아니다</h2>
 *
 * 저장은 <b>나중에 다시 읽을 데이터</b>를 남기고 감지는 <b>지금 위험한 상태</b>를 알린다.
 * 목적이 다르면 엄격함의 방향도 다르다 — {@code speed: 300}은 저장 계약 밖이지만
 * 감지 관점에서는 과속이 맞다. 그래서 fixture는 칸을 <b>따로</b> 갖는다.
 * 어느 쪽을 어떻게 맞출지는 {@code docs/anomaly-path-contract.md} 3-2절에서 정한다.
 *
 * <h2>{@link TelemetryContractTest}와 무엇이 다른가</h2>
 *
 * 저 파일은 계약 자체를 <b>손으로 적은 fixture</b>로 촘촘히 본다(경계값, 사유 코드별).
 * 여기는 <b>두 경로가 공유하는 입력</b>만 본다. 겹치는 칸이 있지만 목적이 다르다 —
 * 여기서 깨지면 "감지 쪽과 맞춰둔 표가 낡았다"는 뜻이다.
 */
@DisplayName("공유 fixture — 저장 경로 계약")
class SharedFixtureContractTest {

    /** 테스트 작업 디렉터리가 {@code backend/}라 한 단계 올라간다. */
    private static final Path FIXTURES =
        Path.of("..", "contract-fixtures", "cases.json");

    private final TelemetryDecoder decoder = TestDecoders.telemetryDecoder();

    private record Case(String id, String payload, String verdict, String reason) {
        @Override
        public String toString() {
            return id;
        }
    }

    /** fixture를 못 읽는 환경에서 목록이 비면 JUnit이 오류를 낸다. 그때 이 한 건만 돌고 skip된다. */
    private static final Case NO_FIXTURE = new Case("(fixture 없음)", null, null, null);

    static List<Case> cases() throws Exception {
        // **빈 목록을 돌려주면 안 된다.** 이 JUnit 버전은 인자가 0건인 @ParameterizedTest를
        // 설정 오류로 본다(PreconditionViolationException) — backend/Dockerfile처럼 저장소
        // 루트가 없는 빌드 컨텍스트에서 실제로 이미지 빌드를 깨뜨렸다(2026-09-09).
        if (!Files.exists(FIXTURES)) {
            return List.of(NO_FIXTURE);
        }
        JsonNode root = new ObjectMapper().readTree(Files.readString(FIXTURES));
        List<Case> out = new ArrayList<>();
        for (JsonNode c : root.get("cases")) {
            JsonNode storage = c.get("storage");
            out.add(new Case(
                c.get("id").asText(),
                c.get("payload").asText(),
                storage.get("verdict").asText(),
                storage.get("reason").isNull() ? null : storage.get("reason").asText()));
        }
        return out;
    }

    /**
     * 저장소 루트가 곁에 있는가. {@code backend/Dockerfile}은 컨텍스트가 {@code ./backend}라
     * 루트가 아예 없는 상태로 {@code gradle test}를 돌린다 — 거기서는 fixture를 못 읽는 게
     * <b>정상</b>이다. 그 한 경우만 건너뛰고, 나머지에서는 없으면 실패시킨다.
     */
    private static boolean repoRootPresent() {
        return Files.isDirectory(Path.of("..", "docs"))
            && Files.isDirectory(Path.of("..", "load-test"));
    }

    @Test
    @DisplayName("fixture 파일이 실제로 읽힌다 — 조용히 0건이 되면 안 된다")
    void fixture가_비어있지_않다() throws Exception {
        // 파라미터 테스트는 목록이 비면 **통과처럼 보인다.** 그걸 막는 게 이 테스트다.
        // 다만 "없어서 못 읽는 것"과 "지워져서 못 읽는 것"을 갈라야 한다.
        Assumptions.assumeTrue(repoRootPresent(),
            "저장소 루트가 없는 빌드 컨텍스트다(backend/Dockerfile) — 여기서는 공유 fixture를 못 읽는 게 정상이다. "
                + "이 검사는 CI의 tests 잡과 로컬 실행이 담당한다.");

        assertThat(Files.exists(FIXTURES))
            .as("저장소 루트는 있는데 공유 fixture가 없다: %s (작업 디렉터리 = %s). "
                + "옮겼거나 지운 것이다 — contract-fixtures/README.md 참고",
                FIXTURES, Path.of("").toAbsolutePath())
            .isTrue();
        assertThat(cases()).hasSizeGreaterThanOrEqualTo(20);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    @DisplayName("저장 경로 판정이 fixture와 같다")
    void 저장_판정(Case c) {
        Assumptions.assumeFalse(c == NO_FIXTURE,
            "공유 fixture를 읽을 수 없는 빌드 컨텍스트다 — fixture가 지워진 경우는 위 테스트가 잡는다");

        String verdict;
        String reason = null;
        try {
            decoder.decode(c.payload());
            verdict = "accept";
        } catch (TelemetryContractException e) {
            verdict = "reject";
            reason = e.getReason();
        } catch (RuntimeException e) {
            // **계약 예외 밖으로 새면 여기서 잡힌다.** MqttMessageHandler는
            // TelemetryContractException만 잡으므로, 다른 예외는 거부가 아니라 전파된다.
            throw new AssertionError(
                "[" + c.id() + "] 계약 예외가 아닌 것이 나왔다: "
                    + e.getClass().getName() + " — MQTT 입구에서는 거부가 아니라 전파된다", e);
        }

        assertThat(verdict)
            .as("[%s] 저장 판정이 fixture와 다르다. 의도한 변경이면 "
                + "contract-fixtures/cases.json의 storage 칸을 같이 고쳐라", c.id())
            .isEqualTo(c.verdict());

        if (c.reason() != null) {
            assertThat(reason)
                .as("[%s] 거부 사유 코드가 fixture와 다르다", c.id())
                .isEqualTo(c.reason());
        }
    }
}
