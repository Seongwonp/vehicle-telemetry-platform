package com.telemetry.mqtt;

import com.telemetry.kafka.TelemetryProducer;
import com.telemetry.support.TestDecoders;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.SimpleAcknowledgment;
import org.springframework.integration.mqtt.event.MqttConnectionFailedEvent;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * 저장 확인 대기 관측 지표 — 끝난 대기(Timer)와 진행 중인 대기(게이지 2개).
 * 끝난 대기만으로는 콜백 스레드가 막혀 있는 순간이 안 보인다(실험 D2). 알림은 elapsed 게이지를 본다.
 */
class MqttAckWaitMetricsTest {
    private static final String PAYLOAD = """
        {"vehicle_id":"ACK-001","timestamp":"2026-10-01T00:00:00Z","speed":10,"rpm":900,
        "engine_temp":85,"throttle_position":12,"fuel_level":55,"battery_voltage":13.8,
        "gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}
        """;

    @Test void gaugesAreRegisteredAtConstructionAsZero_soAbsentIsNotZero() {
        var registry = new SimpleMeterRegistry();
        handler(mock(TelemetryProducer.class), registry);

        assertThat(registry.get("telemetry.mqtt.ack.wait.in.progress").gauge().value()).isZero();
        assertThat(registry.get("telemetry.mqtt.ack.wait.elapsed.seconds").gauge().value()).isZero();
        for (String outcome : new String[]{"acked", "failed", "interrupted"}) {
            assertThat(registry.get("telemetry.mqtt.ack.wait").tag("outcome", outcome).timer().count()).isZero();
        }
    }

    @Test void whilePendingGaugeIsOneAndElapsedGrows_thenTimerRecordsAcked() throws Exception {
        var registry = new SimpleMeterRegistry();
        var producer = mock(TelemetryProducer.class);
        var receipt = new CompletableFuture<Void>();
        var entered = new CountDownLatch(1);
        when(producer.send(any())).thenAnswer(call -> { entered.countDown(); return receipt; });
        var handler = handler(producer, registry);
        var ack = mock(SimpleAcknowledgment.class);
        var worker = Executors.newSingleThreadExecutor();
        try {
            var result = worker.submit(() -> handler.handle(message(ack)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(gauge(registry, "telemetry.mqtt.ack.wait.in.progress")).isEqualTo(1.0);
            double first = gauge(registry, "telemetry.mqtt.ack.wait.elapsed.seconds");
            Thread.sleep(120);
            double second = gauge(registry, "telemetry.mqtt.ack.wait.elapsed.seconds");
            assertThat(second).isGreaterThan(first).isGreaterThanOrEqualTo(0.1);
            assertThat(timer(registry, "acked").count()).isZero(); // 끝나기 전에는 Timer에 없다

            receipt.complete(null);
            result.get(5, TimeUnit.SECONDS);
            verify(ack).acknowledge();
            assertThat(gauge(registry, "telemetry.mqtt.ack.wait.in.progress")).isZero();
            assertThat(gauge(registry, "telemetry.mqtt.ack.wait.elapsed.seconds")).isZero();
            assertThat(timer(registry, "acked").count()).isEqualTo(1);
            assertThat(timer(registry, "acked").totalTime(TimeUnit.SECONDS)).isGreaterThanOrEqualTo(0.1);
            assertThat(timer(registry, "failed").count()).isZero();
        } finally { worker.shutdownNow(); }
    }

    @Test void failedReceiptIsRecordedAsFailed() {
        var registry = new SimpleMeterRegistry();
        var producer = mock(TelemetryProducer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("disk full")));
        var ack = mock(SimpleAcknowledgment.class);

        assertThatThrownBy(() -> handler(producer, registry).handle(message(ack)))
            .isInstanceOf(IllegalStateException.class);

        assertThat(timer(registry, "failed").count()).isEqualTo(1);
        assertThat(timer(registry, "acked").count()).isZero();
        assertThat(gauge(registry, "telemetry.mqtt.ack.wait.in.progress")).isZero();
    }

    @Test void connectionLossInterruptIsRecordedAsInterruptedAndGaugesReset() throws Exception {
        var registry = new SimpleMeterRegistry();
        var producer = mock(TelemetryProducer.class);
        var receipt = new CompletableFuture<Void>();
        var entered = new CountDownLatch(1);
        var handlerThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        when(producer.send(any())).thenAnswer(call -> {
            handlerThread.set(Thread.currentThread()); entered.countDown(); return receipt; });
        var handler = handler(producer, registry);
        var adapter = mock(MqttPahoMessageDrivenChannelAdapter.class);
        when(adapter.getTopic()).thenReturn(new String[]{"vehicle/telemetry/+"});
        var ack = mock(SimpleAcknowledgment.class);
        var worker = Executors.newSingleThreadExecutor();
        try {
            var result = worker.submit(() -> handler.handle(message(ack)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (handlerThread.get().getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(gauge(registry, "telemetry.mqtt.ack.wait.in.progress")).isEqualTo(1.0);

            handler.onConnectionLost(new MqttConnectionFailedEvent(adapter));
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasRootCauseInstanceOf(InterruptedException.class);

            verifyNoInteractions(ack);
            assertThat(timer(registry, "interrupted").count()).isEqualTo(1);
            assertThat(timer(registry, "acked").count()).isZero();
            assertThat(timer(registry, "failed").count()).isZero();
            assertThat(gauge(registry, "telemetry.mqtt.ack.wait.in.progress")).isZero();
            assertThat(gauge(registry, "telemetry.mqtt.ack.wait.elapsed.seconds")).isZero();
        } finally { worker.shutdownNow(); }
    }

    @Test void prometheusExportHasFixedBucketsAndNamesTheAlertUses() {
        // 알림(MqttAckWaitStuck)이 보는 이름과 히스토그램 버킷 경계가 실제 Prometheus 출력에 있는지.
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        var producer = mock(TelemetryProducer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(null));
        handler(producer, registry).handle(message(mock(SimpleAcknowledgment.class)));

        String scrape = registry.scrape();
        assertThat(scrape)
            .contains("telemetry_mqtt_ack_wait_in_progress ")
            .contains("telemetry_mqtt_ack_wait_elapsed_seconds ")
            // simpleclient 형식은 라벨 끝에 쉼표가 붙는다(`{outcome="acked",le="40.0",}`).
            .containsPattern("telemetry_mqtt_ack_wait_seconds_bucket\\{outcome=\"acked\",le=\"40.0\",?\\}")
            .containsPattern("telemetry_mqtt_ack_wait_seconds_bucket\\{outcome=\"acked\",le=\"45.0\",?\\}")
            .containsPattern("telemetry_mqtt_ack_wait_seconds_count\\{outcome=\"acked\",?\\} 1(\\.0)?\\n");
        // 카디널리티: 결과 3종 × (버킷 12 + +Inf)뿐이다.
        long buckets = scrape.lines().filter(l -> l.startsWith("telemetry_mqtt_ack_wait_seconds_bucket")).count();
        assertThat(buckets).isEqualTo(3 * 13);
    }

    private static double gauge(MeterRegistry registry, String name) {
        return registry.get(name).gauge().value();
    }
    private static io.micrometer.core.instrument.Timer timer(MeterRegistry registry, String outcome) {
        return registry.get("telemetry.mqtt.ack.wait").tag("outcome", outcome).timer();
    }
    private static org.springframework.messaging.Message<String> message(SimpleAcknowledgment ack) {
        return MessageBuilder.withPayload(PAYLOAD).setHeader("mqtt_receivedTopic", "vehicle/telemetry/ACK-001")
            .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();
    }
    private static MqttMessageHandler handler(TelemetryProducer producer, MeterRegistry registry) {
        return new MqttMessageHandler(producer, TestDecoders.telemetryDecoder(), registry,
            mock(MqttInvalidMessagePublisher.class));
    }
}
