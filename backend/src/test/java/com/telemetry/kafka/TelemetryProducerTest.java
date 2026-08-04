package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
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

    private VehicleTelemetry telemetry() {
        VehicleTelemetry telemetry = new VehicleTelemetry();
        telemetry.setVehicleId("SIM-001");
        telemetry.setTimestamp("2026-05-09T10:00:00Z");
        return telemetry;
    }
}
