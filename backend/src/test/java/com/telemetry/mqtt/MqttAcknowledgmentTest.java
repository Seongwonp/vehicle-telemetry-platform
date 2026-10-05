package com.telemetry.mqtt;

import com.telemetry.kafka.TelemetryProducer;
import com.telemetry.support.TestDecoders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.SimpleAcknowledgment;
import org.springframework.messaging.support.MessageBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class MqttAcknowledgmentTest {
    private static final String PAYLOAD = """
        {"vehicle_id":"ACK-001","timestamp":"2026-10-01T00:00:00Z","speed":10,"rpm":900,
        "engine_temp":85,"throttle_position":12,"fuel_level":55,"battery_voltage":13.8,
        "gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}
        """;

    @Test void ackWaitsForReceiptAndRunsOnDeliveryThread() throws Exception {
        var producer = mock(TelemetryProducer.class);
        var receipt = new CompletableFuture<Void>();
        var entered = new java.util.concurrent.CountDownLatch(1);
        when(producer.send(any())).thenAnswer(call -> { entered.countDown(); return receipt; });
        var handler = handler(producer, mock(MqttInvalidMessagePublisher.class));
        var ack = mock(SimpleAcknowledgment.class);
        var worker = Executors.newSingleThreadExecutor();
        try {
            var result = worker.submit(() -> handler.handle(message(PAYLOAD, ack)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            verifyNoInteractions(ack);
            receipt.complete(null);
            result.get(5, TimeUnit.SECONDS);
            verify(ack).acknowledge();
        } finally { worker.shutdownNow(); }
    }

    @Test void failedSpoolReceiptDoesNotAck() {
        var producer = mock(TelemetryProducer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("disk full")));
        var ack = mock(SimpleAcknowledgment.class);
        assertThatThrownBy(() -> handler(producer, mock(MqttInvalidMessagePublisher.class))
            .handle(message(PAYLOAD, ack))).isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(ack);
    }

    @Test void dlqSuccessPrecedesAckAndDlqFailureDoesNotAck() {
        var dlq = mock(MqttInvalidMessagePublisher.class);
        var ack = mock(SimpleAcknowledgment.class);
        var handler = handler(mock(TelemetryProducer.class), dlq);
        handler.handle(message("{bad", ack));
        var order = inOrder(dlq, ack);
        order.verify(dlq).publish(any(), any(), any());
        order.verify(ack).acknowledge();
        reset(ack);
        doThrow(new IllegalStateException("DLQ unavailable")).when(dlq).publish(any(), any(), any());
        assertThatThrownBy(() -> handler.handle(message("{bad", ack))).isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(ack);
    }

    @Test void unsendableTelemetryIsQuarantinedThenAcked_notLeftToRedeliverForever() {
        // 직렬화 실패는 다시 보내도 같다. ACK 없이 던지면 브로커가 재접속마다 맨 앞에서 재전달해 구독이 막힌다.
        var producer = mock(TelemetryProducer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.failedFuture(
            new TelemetryProducer.UnsendableTelemetryException(new RuntimeException("boom"))));
        var dlq = mock(MqttInvalidMessagePublisher.class);
        var ack = mock(SimpleAcknowledgment.class);

        handler(producer, dlq).handle(message(PAYLOAD, ack));

        var order = inOrder(dlq, ack);
        order.verify(dlq).publish(eq("vehicle/telemetry/ACK-001"), eq(PAYLOAD), eq("SERIALIZATION_FAILED"));
        order.verify(ack).acknowledge();
    }

    @Test void unsendableTelemetryWithDlqFailureIsNotAcked() {
        var producer = mock(TelemetryProducer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.failedFuture(
            new TelemetryProducer.UnsendableTelemetryException(new RuntimeException("boom"))));
        var dlq = mock(MqttInvalidMessagePublisher.class);
        doThrow(new IllegalStateException("DLQ unavailable")).when(dlq).publish(any(), any(), any());
        var ack = mock(SimpleAcknowledgment.class);

        assertThatThrownBy(() -> handler(producer, dlq).handle(message(PAYLOAD, ack)))
            .isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(ack);
    }

    @Test void nonContractDecodeFailureIsQuarantinedThenAcked() {
        var decoder = mock(com.telemetry.domain.TelemetryDecoder.class);
        when(decoder.decode(any())).thenThrow(new IllegalArgumentException("validator bug"));
        var dlq = mock(MqttInvalidMessagePublisher.class);
        var ack = mock(SimpleAcknowledgment.class);
        var handler = new MqttMessageHandler(mock(TelemetryProducer.class), decoder, new SimpleMeterRegistry(), dlq);

        handler.handle(message(PAYLOAD, ack));

        var order = inOrder(dlq, ack);
        order.verify(dlq).publish(any(), any(), eq("DECODE_FAILED"));
        order.verify(ack).acknowledge();
    }

    @Test void missingAckCallbackIsCounted_soAManualAckRegressionIsVisible() {
        var producer = mock(TelemetryProducer.class);
        when(producer.send(any())).thenReturn(CompletableFuture.completedFuture(null));
        var registry = new SimpleMeterRegistry();
        var handler = new MqttMessageHandler(producer, TestDecoders.telemetryDecoder(), registry,
            mock(MqttInvalidMessagePublisher.class));

        handler.handle(MessageBuilder.withPayload(PAYLOAD)
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/ACK-001").build());

        assertThat(registry.counter("telemetry.mqtt.ack.callback.missing").count()).isEqualTo(1.0);
    }

    @Test void connectionLossWakesWaitingThreadWithoutAckAndSysAdapterDoesNot() throws Exception {
        var producer = mock(TelemetryProducer.class);
        var receipt = new CompletableFuture<Void>();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var handlerThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        when(producer.send(any())).thenAnswer(call -> {
            handlerThread.set(Thread.currentThread()); entered.countDown(); return receipt; });
        var handler = handler(producer, mock(MqttInvalidMessagePublisher.class));
        var ack = mock(SimpleAcknowledgment.class);
        var telemetryAdapter = mock(org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter.class);
        when(telemetryAdapter.getTopic()).thenReturn(new String[]{"vehicle/telemetry/+"});
        var sysAdapter = mock(org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter.class);
        when(sysAdapter.getTopic()).thenReturn(new String[]{"$SYS/broker/clients/connected"});
        var worker = Executors.newSingleThreadExecutor();
        try {
            // 대기 중이 아닐 때의 이벤트는 아무것도 깨우지 않는다.
            handler.onConnectionLost(new org.springframework.integration.mqtt.event.MqttConnectionFailedEvent(telemetryAdapter));
            var result = worker.submit(() -> handler.handle(message(PAYLOAD, ack)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            // send() 반환 뒤 receipt.get()에 들어갈 때까지 기다린다 — 그 전에 보낸 이벤트는 깨울 대상이 없다.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (handlerThread.get().getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            handler.onConnectionLost(new org.springframework.integration.mqtt.event.MqttConnectionFailedEvent(sysAdapter));
            assertThat(result.isDone()).isFalse();
            handler.onConnectionLost(new org.springframework.integration.mqtt.event.MqttConnectionFailedEvent(telemetryAdapter));
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(InterruptedException.class);
            verifyNoInteractions(ack);
            // 중단이 스레드에 인터럽트 플래그를 남기지 않는다 — 같은 스레드의 다음 메시지가 정상 ACK된다.
            receipt.complete(null);
            var next = worker.submit(() -> handler.handle(message(PAYLOAD, ack)));
            next.get(5, TimeUnit.SECONDS);
            verify(ack).acknowledge();
        } finally { worker.shutdownNow(); }
    }

    private static org.springframework.messaging.Message<String> message(String payload, SimpleAcknowledgment ack) {
        return MessageBuilder.withPayload(payload).setHeader("mqtt_receivedTopic", "vehicle/telemetry/ACK-001")
            .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();
    }
    private static MqttMessageHandler handler(TelemetryProducer producer, MqttInvalidMessagePublisher dlq) {
        return new MqttMessageHandler(producer, TestDecoders.telemetryDecoder(), new SimpleMeterRegistry(), dlq);
    }
}
