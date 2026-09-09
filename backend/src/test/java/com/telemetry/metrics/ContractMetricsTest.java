package com.telemetry.metrics;

import com.telemetry.domain.TelemetryContractException;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 지표의 <b>경계</b>를 고정한다 (P0-2b).
 *
 * <p>여기서 지키는 것은 값이 아니라 규칙이다 — 라벨에 무엇이 들어가고 무엇이 안 들어가는지,
 * timeout을 어떻게 다루는지. 값이 맞게 오르는지는
 * {@code TelemetryConsumerTest}/{@code MqttMessageHandlerTest}가 본다.
 */
@DisplayName("거부·격리 지표의 경계")
class ContractMetricsTest {

    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
    }

    private Set<String> tagKeysOf(String name) {
        return registry.getMeters().stream()
            .filter(m -> m.getId().getName().equals(name))
            .flatMap(m -> m.getId().getTags().stream())
            .map(Tag::getKey)
            .collect(Collectors.toSet());
    }

    @Test
    @DisplayName("라벨에 개인정보·고카디널리티 키가 들어가지 않는다")
    void 라벨_경계() {
        ContractMetrics.rejected(registry, ContractMetrics.ENTRANCE_MQTT,
            TelemetryContractException.PAYLOAD_VALIDATION_FAILED);
        ContractMetrics.dlqPublishAttempt(registry, "vehicle-telemetry-dlq");
        ContractMetrics.dlqPublished(registry, "vehicle-telemetry-dlq");
        ContractMetrics.dlqPublishFailed(registry, "vehicle-telemetry-dlq", new TimeoutException("x"));

        Set<String> used = registry.getMeters().stream()
            .map(Meter::getId)
            .flatMap(id -> id.getTags().stream())
            .map(Tag::getKey)
            .collect(Collectors.toSet());

        assertThat(used)
            .as("허용되지 않은 라벨 키가 생겼다. 차량 ID·payload·예외 메시지·필드 경로는 "
                + "지표 라벨에 넣지 않는다 — Prometheus 라벨은 보존 기간 내내 남아 "
                + "docs/data-retention.md 정책 밖으로 개인정보가 샌다")
            .isSubsetOf(ContractMetrics.ALLOWED_TAG_KEYS);
    }

    @Test
    @DisplayName("거부 지표의 라벨은 entrance와 reason뿐이다")
    void 거부_지표_라벨() {
        ContractMetrics.rejected(registry, ContractMetrics.ENTRANCE_KAFKA_STORAGE,
            TelemetryContractException.UNKNOWN_FIELD);
        assertThat(tagKeysOf(ContractMetrics.REJECTED_ATTEMPTS))
            .containsExactlyInAnyOrder(ContractMetrics.TAG_ENTRANCE, ContractMetrics.TAG_REASON);
    }

    @Test
    @DisplayName("DLQ 지표는 topic만 붙는다 — topic이 이미 입구를 가른다")
    void dlq_지표_라벨() {
        ContractMetrics.dlqPublishAttempt(registry, "vehicle-telemetry-mqtt-dlq");
        assertThat(tagKeysOf(ContractMetrics.DLQ_PUBLISH_ATTEMPTS))
            .containsExactly(ContractMetrics.TAG_TOPIC);
    }

    @Test
    @DisplayName("timeout은 '발행 실패'이면서 동시에 '발행 여부 불명'이다")
    void timeout은_불명으로도_센다() {
        ContractMetrics.dlqPublishFailed(registry, "t", new IllegalStateException("wrapped",
            new TimeoutException("브로커 응답 없음")));

        assertThat(registry.counter(ContractMetrics.DLQ_PUBLISH_FAILURES, "topic", "t").count())
            .as("관찰된 실패로도 세야 한다").isEqualTo(1.0);
        assertThat(registry.counter(ContractMetrics.DLQ_PUBLISH_INDETERMINATE, "topic", "t").count())
            .as("timeout은 브로커가 받았는지 모른다 — '확실히 발행되지 않았다'가 아니다")
            .isEqualTo(1.0);
    }

    @Test
    @DisplayName("확정적 실패는 불명으로 세지 않는다")
    void 확정적_실패는_불명이_아니다() {
        ContractMetrics.dlqPublishFailed(registry, "t", new IllegalArgumentException("직렬화 실패"));

        assertThat(registry.counter(ContractMetrics.DLQ_PUBLISH_FAILURES, "topic", "t").count())
            .isEqualTo(1.0);
        assertThat(registry.find(ContractMetrics.DLQ_PUBLISH_INDETERMINATE).counter())
            .as("클라이언트에서 확정적으로 실패한 것은 발행 여부가 불명이 아니다")
            .isNull();
    }

    @Test
    @DisplayName("Kafka 클라이언트의 timeout도 불명으로 센다")
    void kafka_timeout도_불명() {
        ContractMetrics.dlqPublishFailed(registry, "t",
            new org.apache.kafka.common.errors.TimeoutException("expiring record"));
        assertThat(registry.counter(ContractMetrics.DLQ_PUBLISH_INDETERMINATE, "topic", "t").count())
            .isEqualTo(1.0);
    }

    @Test
    @DisplayName("계약 사유 4종만 사유 라벨에 쓴다 — MQTT 고유 사유는 계약이 아니다")
    void 계약_사유_판별() {
        for (String reason : TelemetryContractException.REASONS) {
            assertThat(TelemetryContractException.isContractReason(reason)).isTrue();
        }
        assertThat(TelemetryContractException.isContractReason("TOPIC_VEHICLE_MISMATCH"))
            .as("MQTT 고유 검사라 계약 사유가 아니다 — 섞으면 세 입구를 나란히 못 놓는다")
            .isFalse();
        assertThat(TelemetryContractException.REASONS).hasSize(4);
    }

    @Test
    @DisplayName("기존 알림이 쓰는 지표 이름은 그대로다")
    void 기존_이름_유지() {
        // alerts.yml이 telemetry_kafka_dlq_published_total / _publish_failures_total을 본다.
        // 이름이 바뀌면 알림이 조용히 죽는다.
        assertThat(ContractMetrics.DLQ_PUBLISHED).isEqualTo("telemetry.kafka.dlq.published");
        assertThat(ContractMetrics.DLQ_PUBLISH_FAILURES)
            .isEqualTo("telemetry.kafka.dlq.publish.failures");
    }

    @Test
    @DisplayName("전역 상태를 들고 있지 않다 — 전달받은 registry에만 올린다")
    void 전역_상태_없음() {
        // (1) 필드가 전부 상수인가. registry나 Counter를 보관하면 여러 registry가
        //     섞이거나 테스트 간에 값이 새어나간다.
        for (java.lang.reflect.Field f : ContractMetrics.class.getDeclaredFields()) {
            if (f.isSynthetic()) {
                continue;
            }
            int m = f.getModifiers();
            assertThat(java.lang.reflect.Modifier.isStatic(m) && java.lang.reflect.Modifier.isFinal(m))
                .as("필드 %s가 static final이 아니다 — 전역 mutable 상태다", f.getName())
                .isTrue();
            assertThat(io.micrometer.core.instrument.MeterRegistry.class.isAssignableFrom(f.getType())
                    || io.micrometer.core.instrument.Counter.class.isAssignableFrom(f.getType()))
                .as("필드 %s가 registry/counter를 보관한다", f.getName())
                .isFalse();
        }

        // (2) 서로 다른 registry에 올리면 서로 섞이지 않는가.
        SimpleMeterRegistry a = new SimpleMeterRegistry();
        SimpleMeterRegistry b = new SimpleMeterRegistry();
        ContractMetrics.dlqPublished(a, "t");
        ContractMetrics.dlqPublished(a, "t");
        ContractMetrics.dlqPublished(b, "t");

        assertThat(a.counter(ContractMetrics.DLQ_PUBLISHED, "topic", "t").count()).isEqualTo(2.0);
        assertThat(b.counter(ContractMetrics.DLQ_PUBLISHED, "topic", "t").count())
            .as("registry가 섞였다 — 전달받은 것에만 올려야 한다").isEqualTo(1.0);
    }
}
