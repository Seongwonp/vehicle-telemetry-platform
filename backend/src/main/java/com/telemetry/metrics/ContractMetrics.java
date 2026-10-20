package com.telemetry.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * 거부·격리 지표의 이름과 라벨을 한 곳에 모은다(docs/rejection-metrics-design.md).
 *
 * <p>거부 판정 / DLQ 발행 시도 / 성공 확인 / 실패·timeout 관찰은 각각 별개 수다.
 * 처리 단계·집계 대상·재시작 시점이 달라서 서로 빼서 다른 뜻을 만들면 안 된다.
 *
 * <p>라벨에 차량 ID·payload·예외 메시지·필드 경로를 넣지 않는다. 카디널리티 문제이고,
 * 라벨은 보존 기간 내내 남아 개인정보가 새는 경로가 된다. 상세는 DLQ에서 본다.
 */
public final class ContractMetrics {

    // 상태가 없어 정적 유틸로 둔다.
    private ContractMetrics() {
    }


    /** 재전달되면 다시 오른다 — 고유 메시지 수가 아니다. */
    public static final String REJECTED_ATTEMPTS = "telemetry.contract.rejected.attempts";
    public static final String DLQ_PUBLISH_ATTEMPTS = "telemetry.kafka.dlq.publish.attempts";
    /** alerts.yml이 본다 — 이름·의미를 바꾸지 않는다. */
    public static final String DLQ_PUBLISHED = "telemetry.kafka.dlq.published";
    /** alerts.yml이 본다 — 이름·의미를 바꾸지 않는다. */
    public static final String DLQ_PUBLISH_FAILURES = "telemetry.kafka.dlq.publish.failures";
    /**
     * failures 중 timeout으로 알아본 것(발행 여부 불명). 부분집합이다.
     * 0이라고 나머지 발행 결과가 확정됐다는 뜻은 아니다.
     */
    public static final String DLQ_PUBLISH_INDETERMINATE = "telemetry.kafka.dlq.publish.indeterminate";

    /** DLQ 지표에는 안 붙인다 — {@code topic}이 이미 입구를 가른다. */
    public static final String TAG_ENTRANCE = "entrance";
    /** 계약 사유 코드 4종만 넣는다. */
    public static final String TAG_REASON = "reason";
    public static final String TAG_TOPIC = "topic";

    public static final String ENTRANCE_MQTT = "mqtt";
    public static final String ENTRANCE_KAFKA_STORAGE = "kafka-storage";
    /** Python 감지기가 같은 이름·라벨로 올린다({@code anomaly-detector/anomaly_detector.py}). */
    public static final String ENTRANCE_ANOMALY_DETECTOR = "anomaly-detector";

    /** {@code ContractMetricsTest}가 이 집합을 고정한다. */
    public static final Set<String> ALLOWED_TAG_KEYS =
        Set.of(TAG_ENTRANCE, TAG_REASON, TAG_TOPIC);

    /** @param reason {@code TelemetryContractException}의 사유 코드만. */
    public static void rejected(MeterRegistry registry, String entrance, String reason) {
        counter(registry, REJECTED_ATTEMPTS, TAG_ENTRANCE, entrance, TAG_REASON, reason).increment();
    }

    public static void dlqPublishAttempt(MeterRegistry registry, String topic) {
        counter(registry, DLQ_PUBLISH_ATTEMPTS, TAG_TOPIC, topic).increment();
    }

    public static void dlqPublished(MeterRegistry registry, String topic) {
        counter(registry, DLQ_PUBLISHED, TAG_TOPIC, topic).increment();
    }

    /** timeout이면 브로커가 받았는지 알 수 없으므로 indeterminate도 같이 올린다. */
    public static void dlqPublishFailed(MeterRegistry registry, String topic, Throwable cause) {
        counter(registry, DLQ_PUBLISH_FAILURES, TAG_TOPIC, topic).increment();
        if (isIndeterminate(cause)) {
            counter(registry, DLQ_PUBLISH_INDETERMINATE, TAG_TOPIC, topic).increment();
        }
    }

    /**
     * 아는 timeout 두 종류만 판별한다. false가 "발행 안 됨 확실"이라는 뜻은 아니다.
     * 목록은 실제로 다른 예외를 관찰한 뒤에 넓힌다.
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
