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

    @Test
    void receiptFailsWhenKafkaAndSpoolBothFail() {
        TelemetrySpool spool = org.mockito.Mockito.mock(TelemetrySpool.class);
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));
        given(spool.store(anyString())).willThrow(new IllegalStateException("disk full"));
        var producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool, new SimpleMeterRegistry(), 10);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> producer.send(telemetry()).join())
            .hasRootCauseMessage("disk full");
    }

    @Test
    void receiptWaitsForKafkaOrCompletedSpoolWrite() {
        var pending = new CompletableFuture<SendResult<String, String>>();
        given(kafkaTemplate.send(anyString(), anyString(), anyString())).willReturn(pending);
        var spool = new TelemetrySpool(tempDirectory.toString());
        var producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool, new SimpleMeterRegistry(), 10);
        var receipt = producer.send(telemetry());
        assertThat(receipt).isNotDone();
        pending.completeExceptionally(new IllegalStateException("Kafka unavailable"));
        receipt.join();
        assertThat(spool.pending(10)).hasSize(1);
    }

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

    /**
     * 해석할 수 없는 spool 파일 하나가 backlog를 영원히 켜 두던 결함(2026-09-29 리뷰). 0바이트는 전원 차단 후의 전형,
     * 잘린 JSON과 vehicle_id 없는 객체도 같다. 격리 뒤에는 backlog가 풀리고 새 메시지가 디스크를 거치지 않아야 한다.
     */
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"", "{\"vehicle_id\":\"SIM-0", "{}"})
    void unreadableSpoolFileIsQuarantinedAndBacklogClears(String content) throws Exception {
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        Path broken = tempDirectory.resolve("0000000000001-00000000000000000000-broken.json");
        java.nio.file.Files.writeString(broken, content);
        io.micrometer.core.instrument.MeterRegistry registry = new SimpleMeterRegistry();
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool, registry, 2000);
        producer.initializeBacklog();
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(sendResult()));

        producer.retryPending();   // 격리
        producer.retryPending();   // pending이 비었으니 backlog 해제

        assertThat(spool.pending(10)).isEmpty();
        assertThat(tempDirectory.resolve(broken.getFileName() + ".corrupt")).exists().hasContent(content);
        assertThat(registry.counter("telemetry.spool.corrupt").count()).isEqualTo(1.0);

        producer.send(telemetry());
        // backlog가 풀렸으므로 spool이 아니라 Kafka로 바로 간다.
        verify(kafkaTemplate).send(anyString(), anyString(), anyString());
        assertThat(spool.pending(10)).isEmpty();
    }

    @Test
    void backlogClearsInTheSamePassOnceTheSpoolIsEmpty_notOnlyOnTheNextScheduledScan() {
        // 2026-09-29 실측: 3대가 1초마다 보내는 스택에서 pending이 10에 고정되고 정상 메시지가 전부 spool을
        // 거쳤다(발행→수신 약 6초). backlog를 "다음 5초 스캔이 빈 것을 볼 때"만 풀면, 유입이 있는 한 스캔은
        // 영원히 비지 않는다. 한 주기 안에서 빌 때까지 돌고 그 자리에서 풀어야 한다.
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T09:59:59Z\"}");
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 2000);
        producer.initializeBacklog();
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(sendResult()));

        producer.retryPending();          // 1건 드레인 → 재스캔 빔 → 즉시 해제
        producer.send(telemetry());       // 다음 주기 없이도 Kafka로 바로 가야 한다

        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(anyString(), anyString(), anyString());
        assertThat(spool.pending(10)).isEmpty();
    }

    @Test
    void filesSpooledWhileDrainingAreDrainedInTheSamePass_beforeDirectSendsResume() {
        // 수정 1차(부분 배치 완료 시 즉시 해제) 실측에서 드레인 중 spool에 들어간 73건보다 새 직접 전송이 먼저 나가
        // 같은 차량 순서가 뒤집혔다(역전 132쌍). 배치가 끝난 뒤 다시 스캔해 그것들도 이번 주기에 내보내야 한다.
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T09:59:58Z\"}");
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 2000);
        producer.initializeBacklog();
        // 첫 배치의 전송이 끝나기 전에 새 메시지가 들어와 spool로 간다(backlog가 켜져 있으므로).
        java.util.concurrent.atomic.AtomicBoolean lateStored = new java.util.concurrent.atomic.AtomicBoolean();
        given(kafkaTemplate.send(anyString(), anyString(), anyString())).willAnswer(inv -> {
            if (lateStored.compareAndSet(false, true)) {
                spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T09:59:59Z\"}");
            }
            return CompletableFuture.completedFuture(sendResult());
        });

        producer.retryPending();

        // 늦게 들어온 것까지 같은 주기에 나가고, 그 뒤에야 backlog가 풀린다.
        assertThat(spool.pending(10)).isEmpty();
        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(anyString(), anyString(), anyString());
        producer.send(telemetry());
        verify(kafkaTemplate, org.mockito.Mockito.times(3)).send(anyString(), anyString(), anyString());
    }

    @Test
    void backlogStaysWhenADrainedMessageFailed() {
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T09:59:59Z\"}");
        spool.store("{\"vehicle_id\":\"SIM-002\",\"timestamp\":\"2026-05-09T09:59:59Z\"}");
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 2000);
        producer.initializeBacklog();
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("Kafka down"));
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(sendResult()), failed);

        producer.retryPending();          // 1건 성공, 1건 실패 → 실패 파일이 남았으니 backlog 유지
        producer.send(telemetry());       // spool로 가야 한다(순서 보존)

        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(anyString(), anyString(), anyString());
        assertThat(spool.pending(10)).hasSize(2);
    }

    @Test
    void aFullBatchKeepsDrainingInTheSamePassUntilEmpty() {
        // 배치 1로 두 건이면 예전엔 주기마다 한 건씩 5초 간격으로 나갔다. 같은 주기에 이어서 비운다.
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T09:59:58Z\"}");
        spool.store("{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T09:59:59Z\"}");
        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 1);
        producer.initializeBacklog();
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(sendResult()));

        producer.retryPending();

        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(anyString(), anyString(), anyString());
        assertThat(spool.pending(10)).isEmpty();
        producer.send(telemetry());       // 비었으니 직접 전송
        verify(kafkaTemplate, org.mockito.Mockito.times(3)).send(anyString(), anyString(), anyString());
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

        // 2026-09-29부터 한 주기는 빌 때까지 배치를 이어 보낸다(순서 보존을 위해 배치 완료를 기다리며).
        // 배치는 "한 번에 집는 파일 수"이지 주기당 상한이 아니다 — 7건이 3·3·1로 같은 주기에 다 나간다.
        verify(kafkaTemplate, org.mockito.Mockito.times(7)).send(anyString(), anyString(), anyString());
        assertThat(spool.pending(100)).isEmpty();
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
