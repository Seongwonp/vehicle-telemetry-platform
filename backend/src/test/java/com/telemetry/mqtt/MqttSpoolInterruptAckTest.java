package com.telemetry.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.kafka.TelemetryProducer;
import com.telemetry.kafka.TelemetrySpool;
import com.telemetry.support.TestDecoders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.SimpleAcknowledgment;
import org.springframework.integration.mqtt.event.MqttConnectionFailedEvent;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 연결 끊김 인터럽트와 동기 spool 쓰기(백로그 경로)의 순서 — ACK가 저장 실패 창에서 나가지 않는지(실험 D3).
 *
 * <p>실제 {@link TelemetryProducer}·{@link TelemetrySpool}을 쓴다. Kafka send가 동기적으로 던지게 해서
 * D3처럼 콜백 스레드가 직접 spool에 쓰게 만들고, spool의 store() 앞/뒤에서 {@code onConnectionLost}를 호출해
 * 인터럽트가 닿는 시점을 고정한다.
 */
class MqttSpoolInterruptAckTest {
    private static final String PAYLOAD = """
        {"vehicle_id":"ACK-001","timestamp":"2026-10-01T00:00:00Z","speed":10,"rpm":900,
        "engine_temp":85,"throttle_position":12,"fuel_level":55,"battery_voltage":13.8,
        "gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}
        """;

    @TempDir Path dir;

    enum When { BEFORE_STORE, AFTER_STORE, NONE }

    /** store() 직전에 인터럽트(D3의 창) → 저장 실패, ACK 없음, .json·.tmp 없음, 플래그 비누수, 다음 메시지 정상 ACK. */
    @Test
    void interruptLandingBeforeSpoolWrite_failsWithoutAck_andLeavesNothing() throws Exception {
        var when = new AtomicReference<>(When.BEFORE_STORE);
        var storeFailure = new AtomicReference<Throwable>();
        var registry = new SimpleMeterRegistry();
        MqttMessageHandler[] holder = new MqttMessageHandler[1];
        var spool = new TelemetrySpool(dir.toString()) {
            @Override public Path store(String payload) {
                if (when.getAndSet(When.NONE) == When.BEFORE_STORE) holder[0].onConnectionLost(lost());
                try {
                    return super.store(payload);
                } catch (RuntimeException e) {
                    storeFailure.set(e.getCause());
                    throw e;
                }
            }
        };
        holder[0] = handler(spool, registry);
        var ack = mock(SimpleAcknowledgment.class);
        var worker = Executors.newSingleThreadExecutor();
        try {
            var first = worker.submit(() -> holder[0].handle(message(ack)));
            assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS)).hasRootCauseInstanceOf(ClosedByInterruptException.class);
            assertThat(storeFailure.get()).isInstanceOf(ClosedByInterruptException.class);
            verifyNoInteractions(ack);
            assertThat(files(".json")).as("저장 실패 창에서는 아무것도 영속되지 않았다").isEmpty();
            assertThat(files(".tmp")).as("실패한 저장이 .tmp를 남기지 않는다").isEmpty();
            // D3와 같이 failed로 센다(예외가 InterruptedException이 아니므로) — 관측 분류는 이번 수정 범위 밖.
            assertThat(registry.get("telemetry.mqtt.ack.wait").tag("outcome", "failed").timer().count()).isEqualTo(1);
            assertThat(worker.submit(() -> Thread.currentThread().isInterrupted()).get(5, TimeUnit.SECONDS))
                .as("연결 끊김 인터럽트가 다음 메시지로 새지 않는다").isFalse();

            // 재전달된 같은 메시지: 정상 저장 뒤 ACK.
            worker.submit(() -> holder[0].handle(message(ack))).get(10, TimeUnit.SECONDS);
            verify(ack, times(1)).acknowledge();
            assertThat(files(".json")).hasSize(1);
            assertThat(files(".tmp")).isEmpty();
        } finally { worker.shutdownNow(); }
    }

    /** store()가 끝난 뒤(.json rename 완료) 인터럽트 → 영속된 뒤이므로 ACK한다(계약 허용), 플래그는 지워진다. */
    @Test
    void interruptLandingAfterSpoolRename_acksBecauseAlreadyPersisted() throws Exception {
        var registry = new SimpleMeterRegistry();
        MqttMessageHandler[] holder = new MqttMessageHandler[1];
        var ackSawJson = new AtomicReference<Boolean>();
        var spool = new TelemetrySpool(dir.toString()) {
            @Override public Path store(String payload) {
                Path stored = super.store(payload);
                holder[0].onConnectionLost(lost());
                return stored;
            }
        };
        holder[0] = handler(spool, registry);
        var ack = mock(SimpleAcknowledgment.class);
        doAnswer(call -> { ackSawJson.set(files(".json").size() == 1); return null; }).when(ack).acknowledge();
        var worker = Executors.newSingleThreadExecutor();
        try {
            worker.submit(() -> holder[0].handle(message(ack))).get(10, TimeUnit.SECONDS);
            verify(ack).acknowledge();
            assertThat(ackSawJson.get()).as("ACK 시점에 .json이 이미 있었다").isTrue();
            assertThat(files(".tmp")).isEmpty();
            assertThat(worker.submit(() -> Thread.currentThread().isInterrupted()).get(5, TimeUnit.SECONDS)).isFalse();
        } finally { worker.shutdownNow(); }
    }

    /**
     * 두 번째 경로(백로그 아님): 콜백 스레드가 {@code kafka.send()}의 동기 구간(max.block — 메타데이터 대기)에서 막혀 있을 때
     * {@code onConnectionLost}. 실제 {@link KafkaTemplate} + 막히는 가짜 Producer(인터럽트되면 KafkaProducer처럼
     * {@code InterruptException} — 생성자가 플래그를 다시 세운다) → {@code sendDirect} catch → 같은 스레드의 spool 쓰기가
     * {@code ClosedByInterruptException}. ACK 없음, .json·.tmp 없음, Timer {@code failed}, 플래그 비누수.
     */
    @Test
    @SuppressWarnings("unchecked")
    void interruptWhileBlockedInSynchronousKafkaSend_spoolFallbackFails_noAck_noFiles() throws Exception {
        var registry = new SimpleMeterRegistry();
        var entered = new java.util.concurrent.CountDownLatch(1);
        org.apache.kafka.clients.producer.Producer<String, String> blocking =
            mock(org.apache.kafka.clients.producer.Producer.class);
        when(blocking.send(any(), any())).thenAnswer(call -> {
            entered.countDown();
            try {
                new java.util.concurrent.CountDownLatch(1).await(10, TimeUnit.SECONDS); // max.block.ms 10초 대역
            } catch (InterruptedException e) {
                throw new org.apache.kafka.common.errors.InterruptException(e);
            }
            throw new org.apache.kafka.common.errors.TimeoutException("not present in metadata after 10000 ms");
        });
        org.springframework.kafka.core.ProducerFactory<String, String> factory = mock(
            org.springframework.kafka.core.ProducerFactory.class,
            call -> call.getMethod().getReturnType() == org.apache.kafka.clients.producer.Producer.class
                ? blocking : org.mockito.Answers.RETURNS_DEFAULTS.answer(call));
        var spool = new TelemetrySpool(dir.toString());
        var producer = new TelemetryProducer(new KafkaTemplate<>(factory), new ObjectMapper(), spool, registry, 100);
        // backlog 기본값 false(빈 spool) — sendDirect 경로
        var handler = new MqttMessageHandler(producer, TestDecoders.telemetryDecoder(), registry,
            mock(MqttInvalidMessagePublisher.class));
        var ack = mock(SimpleAcknowledgment.class);
        var worker = Executors.newSingleThreadExecutor();
        try {
            var first = worker.submit(() -> handler.handle(message(ack)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).as("콜백 스레드가 send()의 동기 구간에 들어갔다").isTrue();
            handler.onConnectionLost(lost());
            assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(ClosedByInterruptException.class);
            verifyNoInteractions(ack);
            assertThat(files(".json")).as("영속되지 않았다 — 회복은 브로커 재전달뿐").isEmpty();
            assertThat(files(".tmp")).isEmpty();
            assertThat(registry.get("telemetry.mqtt.ack.wait").tag("outcome", "failed").timer().count()).isEqualTo(1);
            assertThat(worker.submit(() -> Thread.currentThread().isInterrupted()).get(5, TimeUnit.SECONDS))
                .as("exitWait가 연결 끊김 인터럽트를 지운다").isFalse();
        } finally { worker.shutdownNow(); }
    }

    @SuppressWarnings("unchecked")
    private MqttMessageHandler handler(TelemetrySpool spool, SimpleMeterRegistry registry) {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        // 동기 실패 → sendDirect의 catch → storeForRetry가 콜백 스레드에서 spool에 쓴다(D3의 백로그 경로와 같은 스레드).
        when(kafka.send(anyString(), anyString(), anyString())).thenThrow(new org.apache.kafka.common.errors.TimeoutException("kafka paused"));
        var producer = new TelemetryProducer(kafka, new ObjectMapper(), spool, registry, 100);
        return new MqttMessageHandler(producer, TestDecoders.telemetryDecoder(), registry,
            mock(MqttInvalidMessagePublisher.class));
    }

    private static MqttConnectionFailedEvent lost() {
        var adapter = mock(MqttPahoMessageDrivenChannelAdapter.class);
        when(adapter.getTopic()).thenReturn(new String[]{"vehicle/telemetry/+"});
        return new MqttConnectionFailedEvent(adapter);
    }

    private static org.springframework.messaging.Message<String> message(SimpleAcknowledgment ack) {
        return MessageBuilder.withPayload(PAYLOAD).setHeader("mqtt_receivedTopic", "vehicle/telemetry/ACK-001")
            .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();
    }

    private List<Path> files(String suffix) throws IOException {
        if (!Files.exists(dir)) return List.of();
        try (var s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(suffix)).toList();
        }
    }
}
