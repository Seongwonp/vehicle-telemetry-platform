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

    private static org.springframework.messaging.Message<String> message(String payload, SimpleAcknowledgment ack) {
        return MessageBuilder.withPayload(payload).setHeader("mqtt_receivedTopic", "vehicle/telemetry/ACK-001")
            .setHeader(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ack).build();
    }
    private static MqttMessageHandler handler(TelemetryProducer producer, MqttInvalidMessagePublisher dlq) {
        return new MqttMessageHandler(producer, TestDecoders.telemetryDecoder(), new SimpleMeterRegistry(), dlq);
    }
}
