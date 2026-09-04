package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 2000);
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
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 2000);
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
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 2000);
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

    @Test
    void retryBatchSizeCapsHowManySpoolFilesOneCycleTakes() {
        // 이 값과 재전송 주기가 곱해져 드레인 속도의 상한이 된다. 예전 값(100/5초)은
        // 20 msg/s였고, 유입 약 1,700 msg/s에 비해 89배 느려 90초 장애가 35분 복구를
        // 만들었다 — 그래서 설정으로 뺐다. 배치가 실제로 지켜지는지 고정한다.
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        for (int i = 0; i < 7; i++) {
            spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T10:00:0" + i + "Z\"}");
        }
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 3);
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(sendResult()));

        producer.retryPending();

        // 배치 3이므로 한 주기에 3건만 빠지고 4건이 남아야 한다.
        assertThat(spool.pending(100)).hasSize(4);
    }

    @Test
    void drainedCounterCountsFilesActuallyRemovedNotSelected() {
        // 선택 시점에 세면 이전 주기의 전송이 안 끝난 파일을 다음 주기가 또 집어
        // 중복 계산된다(실측에서 drained 379,536 > MQTT 수신 261,340으로 드러났다).
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T10:00:00Z\"}");
        spool.store("{\"vehicle_id\":\"SIM-002\",\"timestamp\":\"2026-05-09T10:00:01Z\"}");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            registry, 10);
        // 한 건은 성공, 한 건은 실패 — 실패분은 파일이 남으므로 세면 안 된다.
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("Kafka down"));
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(sendResult()), failed);

        producer.retryPending();

        assertThat(registry.get("telemetry.spool.drained").counter().count()).isEqualTo(1.0);
        assertThat(spool.pending(100)).hasSize(1);
    }
}
