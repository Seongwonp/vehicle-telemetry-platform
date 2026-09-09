package com.telemetry.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * 거부·격리 지표를 <b>한 곳에서</b> 만든다 (P0-2b).
 *
 * <h2>왜 한 곳인가</h2>
 *
 * 이름과 라벨이 흩어지면 <b>같은 뜻인데 다른 이름</b>이 생기고, 그러면 대시보드에서
 * 두 수를 나란히 놓을 수가 없다. 라벨에 무엇을 넣지 않기로 했는지도 한 곳에 있어야
 * 테스트로 고정할 수 있다({@code ContractMetricsTest}).
 *
 * <h2>네 단계를 구분한다 — 뺄셈으로 유도하지 않는다</h2>
 *
 * <table>
 *   <tr><th>지표</th><th>세는 것</th></tr>
 *   <tr><td>{@link #REJECTED_ATTEMPTS}</td><td><b>계약 거부 판정</b> 횟수</td></tr>
 *   <tr><td>{@link #DLQ_PUBLISH_ATTEMPTS}</td><td>그 격리를 위한 <b>DLQ 발행 시도</b> 횟수</td></tr>
 *   <tr><td>{@link #DLQ_PUBLISHED}(기존)</td><td>DLQ 발행 <b>성공을 확인한</b> 횟수</td></tr>
 *   <tr><td>{@link #DLQ_PUBLISH_FAILURES}(기존)</td><td>DLQ 발행 <b>실패·timeout을 관찰한</b> 횟수</td></tr>
 *   <tr><td>{@link #DLQ_PUBLISH_INDETERMINATE}</td><td>그중 <b>발행 여부를 알 수 없는</b> 횟수(timeout)</td></tr>
 * </table>
 *
 * <p><b>이 수들을 빼서 다른 뜻을 만들면 안 된다.</b> 초안에서
 * "{@code invalid − published} = 격리하지 못한 건수"라고 썼다가 지적을 받았다 —
 * 두 지표는 <b>처리 단계가 다르고, 집계 대상이 다르고, 재시도 횟수도 다를 수 있다.</b>
 * 프로세스가 재시작하면 카운터는 0부터 다시 세는데 그 시점도 서로 다르다.
 * 각 단계는 <b>자기 이름으로만</b> 읽는다.
 *
 * <h2>timeout은 "발행되지 않았다"가 아니다 — 다만 적용 범위가 좁다</h2>
 *
 * {@code send(...).get(10s)}가 timeout이면 <b>브로커가 받았는지 알 수 없다.</b>
 * 그래서 {@link #DLQ_PUBLISH_INDETERMINATE}를 따로 센다 —
 * {@link #DLQ_PUBLISH_FAILURES}의 <b>부분집합</b>이고, 이 값이 0이 아니면
 * "실패했다"가 아니라 <b>"모른다"</b>로 읽어야 한다.
 *
 * <p><b>이 카운터가 세는 것은 "관찰한 timeout 중 발행 여부가 불명인 것"뿐이다.</b>
 * {@link #isIndeterminate}가 알아보는 예외는 두 종류({@code java.util.concurrent}와
 * Kafka 클라이언트의 timeout)이고, 그 밖의 실패는 여기 안 잡힌다.
 *
 * <p><b>값이 0이라고 모든 발행 결과가 확정됐다는 뜻이 아니다.</b> 우리가 분류하지 못한
 * 예외 중에도 실제로는 브로커에 닿았을 수 있는 것이 있을 수 있다. 여기서 주장하는 것은
 * "이만큼은 확실히 불명이다"이지 "나머지는 확실하다"가 아니다.
 * 분류를 넓히는 것은 실제로 그런 예외를 관찰한 뒤에 한다 — 지금 지어내지 않는다.
 *
 * <h2>라벨에 넣지 않는 것</h2>
 *
 * <b>차량 ID·payload·예외 메시지·필드 경로.</b> 카디널리티도 문제지만 Prometheus 라벨은
 * 보존 기간 내내 남아 {@code docs/data-retention.md}의 정책 밖으로 개인정보가 새는
 * 경로가 된다. 어느 필드가 문제인지는 지표가 아니라 DLQ에서 본다
 * ({@code docs/runbook/dlq-reprocessing.md} 2-1절).
 */
public final class ContractMetrics {

    // **정적 유틸이다.** 주입 대상으로 만들면 이미 MeterRegistry를 받고 있는 컴포넌트들의
    // 생성자가 전부 바뀌고, 그 생성자를 직접 부르는 테스트가 줄줄이 깨진다.
    // 여기가 하는 일은 이름·라벨을 한곳에 모으는 것뿐이라 상태가 필요 없다.
    private ContractMetrics() {
    }


    /** 계약 거부 판정 횟수. 재전달되면 <b>다시 오른다</b> — 고유 메시지 수가 아니다. */
    public static final String REJECTED_ATTEMPTS = "telemetry.contract.rejected.attempts";
    /** DLQ 발행 시도 횟수. 성공·실패를 가리지 않고 시도할 때마다. */
    public static final String DLQ_PUBLISH_ATTEMPTS = "telemetry.kafka.dlq.publish.attempts";
    /** 발행 성공을 확인한 횟수. <b>기존 지표 — alerts.yml이 본다. 이름·의미를 바꾸지 않는다.</b> */
    public static final String DLQ_PUBLISHED = "telemetry.kafka.dlq.published";
    /** 발행 실패·timeout을 관찰한 횟수. <b>기존 지표 — alerts.yml이 본다.</b> */
    public static final String DLQ_PUBLISH_FAILURES = "telemetry.kafka.dlq.publish.failures";
    /**
     * 위 실패 중 <b>우리가 timeout으로 알아본</b> 것. failures의 부분집합이다.
     * <b>0이라고 나머지가 확정됐다는 뜻은 아니다</b> — 클래스 javadoc 참고.
     */
    public static final String DLQ_PUBLISH_INDETERMINATE = "telemetry.kafka.dlq.publish.indeterminate";

    /** 거부가 어느 입구에서 났나. DLQ 지표는 {@code topic}이 이미 입구를 가르므로 안 붙인다. */
    public static final String TAG_ENTRANCE = "entrance";
    /** 계약 사유 코드 4종. 그 밖의 값을 넣지 않는다. */
    public static final String TAG_REASON = "reason";
    public static final String TAG_TOPIC = "topic";

    public static final String ENTRANCE_MQTT = "mqtt";
    public static final String ENTRANCE_KAFKA_STORAGE = "kafka-storage";
    /** Python 감지기가 같은 이름·같은 라벨로 올린다({@code anomaly-detector/anomaly_detector.py}). */
    public static final String ENTRANCE_ANOMALY_DETECTOR = "anomaly-detector";

    /** 라벨에 허용되는 키. 테스트가 이 집합을 고정한다. */
    public static final Set<String> ALLOWED_TAG_KEYS =
        Set.of(TAG_ENTRANCE, TAG_REASON, TAG_TOPIC);

    /**
     * 계약 거부를 <b>판정</b>했다. DLQ로 갔는지는 여기서 모른다 — 그건 다음 단계다.
     *
     * @param reason {@code TelemetryContractException}의 사유 코드. 다른 값을 넣지 마라.
     */
    public static void rejected(MeterRegistry registry, String entrance, String reason) {
        counter(registry, REJECTED_ATTEMPTS, TAG_ENTRANCE, entrance, TAG_REASON, reason).increment();
    }

    /** DLQ 발행을 <b>시도</b>했다. 결과와 무관하게 시도 시점에 올린다. */
    public static void dlqPublishAttempt(MeterRegistry registry, String topic) {
        counter(registry, DLQ_PUBLISH_ATTEMPTS, TAG_TOPIC, topic).increment();
    }

    /** DLQ 발행 <b>성공을 확인</b>했다. */
    public static void dlqPublished(MeterRegistry registry, String topic) {
        counter(registry, DLQ_PUBLISHED, TAG_TOPIC, topic).increment();
    }

    /**
     * DLQ 발행 <b>실패·timeout을 관찰</b>했다.
     *
     * <p>timeout이면 {@link #DLQ_PUBLISH_INDETERMINATE}도 같이 올린다 —
     * 그 경우 <b>브로커가 받았는지 알 수 없다.</b> "발행되지 않았다"고 읽으면 안 된다.
     */
    public static void dlqPublishFailed(MeterRegistry registry, String topic, Throwable cause) {
        counter(registry, DLQ_PUBLISH_FAILURES, TAG_TOPIC, topic).increment();
        if (isIndeterminate(cause)) {
            counter(registry, DLQ_PUBLISH_INDETERMINATE, TAG_TOPIC, topic).increment();
        }
    }

    /**
     * <b>우리가 알아볼 수 있는</b> timeout인가.
     *
     * <p>timeout은 요청이 브로커에 닿았는지 자체가 불확실하다. 반면 직렬화 실패나
     * 크기 초과는 클라이언트에서 확정적으로 실패한 것이라 여기 해당하지 않는다.
     *
     * <p><b>여기서 false라고 "발행되지 않은 것이 확실"하다는 뜻은 아니다.</b>
     * 아는 두 종류만 판별한다 — 목록을 넓히는 것은 실제로 다른 예외를 관찰한 뒤에 한다.
     */
    static boolean isIndeterminate(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause()) {
            if (t instanceof TimeoutException
                || t instanceof org.apache.kafka.common.errors.TimeoutException) {
                return true;
            }
            if (t == t.getCause()) {
                break;   // 자기 참조 방어
            }
        }
        return false;
    }

    private static Counter counter(MeterRegistry registry, String name, String... tags) {
        return registry.counter(name, tags);
    }
}
