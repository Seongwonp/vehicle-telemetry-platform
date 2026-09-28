package com.telemetry.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 재시도 소진 경로에서 <b>DLQ 발행이 실패하면 성공 카운터가 오르지 않는지</b>.
 *
 * <p>2026-09-27까지 {@code published}를 {@code recoverer.accept()} <b>전에</b> 올려서 발행 실패도 성공으로 셌다.
 * offset은 {@code setFailIfSendResultIsError(true)}가 지켰지만, 조사 근거인 카운터가 틀렸다.
 * {@link KafkaDlqLoggingTest}는 성공 경로만 봐서 놓쳤다.
 */
@DisplayName("DLQ 발행 실패 지표 — 실패를 성공으로 세지 않는다")
class KafkaDlqPublishFailureMetricTest {

    @Test
    @SuppressWarnings("unchecked")
    void 발행실패면_published는_0이고_failures가_1이다() {
        KafkaOperations<String, String> operations = mock(KafkaOperations.class);
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(operations.send(any(ProducerRecord.class))).thenReturn(failed);
        MeterRegistry registry = new SimpleMeterRegistry();
        DefaultErrorHandler handler = new KafkaConfig().kafkaErrorHandler(operations, registry, 1L, 1.0, 1L, 0L);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("vehicle-telemetry", 0, 7L, "SIM-001", "{}");

        // handleOne은 recoverer 실패를 잡고 false(복구 미완료)를 반환한다.
        assertThat(handler.handleOne(new IllegalStateException("influx down"),
                record, mock(Consumer.class), mock(MessageListenerContainer.class))).isFalse();

        assertThat(registry.counter("telemetry.kafka.dlq.publish.attempts", "topic", "vehicle-telemetry-dlq").count()).isEqualTo(1.0);
        assertThat(registry.counter("telemetry.kafka.dlq.published", "topic", "vehicle-telemetry-dlq").count()).isZero();
        assertThat(registry.find("telemetry.kafka.dlq.publish.failures").counters()
            .stream().mapToDouble(io.micrometer.core.instrument.Counter::count).sum()).isEqualTo(1.0);
    }
}
