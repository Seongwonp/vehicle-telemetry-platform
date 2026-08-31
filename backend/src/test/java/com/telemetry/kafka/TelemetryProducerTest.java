package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TelemetryProducerTest {

    @TempDir Path tempDirectory;
    @Mock KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void kafkaFailureKeepsWriteAheadSpoolFile() {
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool);
        producer.initializeBacklog();
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("Kafka down"));
        given(kafkaTemplate.send(anyString(), anyString(), anyString())).willReturn(failed);

        producer.send(telemetry());

        assertThat(spool.pending(10)).hasSize(1);
    }

    @Test
    void successfulSendDoesNotTouchTheSpool() {
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool);
        producer.initializeBacklog();
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(sendResult()));

        producer.send(telemetry());

        // 정상 경로에서 spool 파일을 쓰면 메시지마다 파일시스템 연산 약 5회가 붙는다.
        // 이게 MQTT 수집을 초당 20건으로 묶어 브로커 큐 오버플로 유실을 만들던 원인이었다.
        assertThat(spool.pending(10)).isEmpty();
    }

    @Test
    void backlogRoutesNewMessagesToSpoolToPreservePerVehicleOrder() {
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T09:59:59Z\"}");
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool);
        producer.initializeBacklog(); // 기존 spool을 감지해 backlog=true

        producer.send(telemetry());

        // 밀린 게 있는 동안 새 메시지가 Kafka로 먼저 가면 차량별 순서가 뒤집힌다.
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        assertThat(spool.pending(10)).hasSize(2);
    }

    private SendResult<String, String> sendResult() {
        return new SendResult<>(
            new ProducerRecord<>("vehicle-telemetry", "SIM-001", "{}"),
            new RecordMetadata(new TopicPartition("vehicle-telemetry", 0), 0, 0, 0L, 0, 0));
    }

    private VehicleTelemetry telemetry() {
        VehicleTelemetry telemetry = new VehicleTelemetry();
        telemetry.setVehicleId("SIM-001");
        telemetry.setTimestamp("2026-05-09T10:00:00Z");
        return telemetry;
    }
}
