package com.telemetry.kafka;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * spool 쓰기에 인터럽트가 닿는 경계(실험 D3 결함 후보, {@code docs/verification/2026-10-08-spool-interrupt-durability.md}).
 *
 * <p>D3에서는 연결 끊김 인터럽트가 Paho 콜백 스레드의 동기 spool 쓰기에 닿아 {@code ClosedByInterruptException}으로
 * 저장이 실패했고 0바이트 {@code .tmp}가 남았다. 여기서는 그 상태를 별도 디렉터리(@TempDir)에서 만든다.
 */
class TelemetrySpoolInterruptTest {

    private static final String PAYLOAD = "{\"vehicle_id\":\"INT-001\",\"timestamp\":\"2026-10-08T00:00:00Z\"}";

    @TempDir Path dir;

    /** D3와 같은 창: 인터럽트가 store() 진입 전에 이미 걸려 있다 → open은 되고 write가 실패한다. */
    @Test
    void pendingInterruptFailsStoreWithoutJson_andLeavesNoTmp() throws IOException {
        TelemetrySpool spool = new TelemetrySpool(dir.toString());
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> spool.store(PAYLOAD))
                .isInstanceOf(IllegalStateException.class)
                .hasCauseInstanceOf(ClosedByInterruptException.class);
            // 인터럽트 상태는 그대로 남는다 — 호출자(핸들러)가 중단을 계속 알아야 한다.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(files(".json")).as("실패한 저장은 .json을 만들지 않는다").isEmpty();
        assertThat(files(".tmp")).as("실패한 저장이 .tmp를 남기지 않는다(D3에서 0바이트 1개가 남았다)").isEmpty();
        assertThat(spool.depth()).isZero();
        // 같은 스레드의 다음 저장은 정상이다.
        Path stored = spool.store(PAYLOAD);
        assertThat(spool.read(stored)).isEqualTo(PAYLOAD);
    }

    /**
     * 인터럽트를 무작위 시점에 보낸다(쓰기 전·쓰기 중·force 중·rename 중·끝난 뒤). 어느 창이든
     * (1) store()가 정상 반환했으면 그 .json은 완전한 내용이고, (2) 던졌으면 .json이 생기지 않았으며,
     * (3) .tmp가 남지 않는다. 결과 분포는 stdout에 남긴다(어느 창에 닿았는지는 실행마다 다르다).
     *
     * <p><b>정리 경로를 실제로 탔는지도 단언한다</b> — 디스크가 빠르면 8MB 쓰기가 30ms 안에 끝나 모든 회차가 정상 반환하고
     * .tmp 단언이 공짜로 통과할 수 있다. 그래서 4회에 1회는 지연 0(시작 직후 인터럽트)으로 보내 쓰기 전·초반 창을 확보하고,
     * 최소 {@code MIN_ITERATIONS}회 뒤에도 던진 회차가 없으면 {@code MAX_ITERATIONS}까지 더 돈다. 던진 회차는 open 뒤에만
     * 실패하므로(open은 인터럽트로 끊기지 않는다) 매번 .tmp를 만들고 지우는 경로를 탄다.
     * 정상 반환한 .json은 회차마다 확인한 뒤 지운다(8MB × 수백 회가 쌓이지 않게).
     */
    @Test
    void interruptAtRandomPoint_neverYieldsJsonOnFailure_norPartialJson_norLeftoverTmp() throws Exception {
        final int MIN_ITERATIONS = 40;
        final int MAX_ITERATIONS = 400;
        TelemetrySpool spool = new TelemetrySpool(dir.toString());
        String big = PAYLOAD + " ".repeat(8 * 1024 * 1024); // 쓰기·force가 측정 가능한 시간을 쓰게
        long bigBytes = big.getBytes(StandardCharsets.UTF_8).length;
        java.util.Set<Path> seenTmp = new java.util.HashSet<>();
        int iterations = 0;
        int threw = 0;
        Map<String, Integer> outcomes = new TreeMap<>();
        while (iterations < MIN_ITERATIONS || (threw == 0 && iterations < MAX_ITERATIONS)) {
            int i = iterations++;
            AtomicReference<Object> result = new AtomicReference<>();
            Thread worker = new Thread(() -> {
                try {
                    Path stored = spool.store(big);
                    // 정상 반환인데 플래그가 서 있으면 인터럽트가 force 이후(close·rename·반환 사이)에 닿은 것이다.
                    result.set(Thread.currentThread().isInterrupted()
                        ? new Object[]{"returned:interruptAfterForce", stored} : stored);
                } catch (RuntimeException e) {
                    result.set(e);
                } finally {
                    Thread.interrupted();
                }
            }, "spool-writer-" + i);
            worker.start();
            long delayNanos = i % 4 == 0 ? 0 : ThreadLocalRandom.current().nextLong(0, TimeUnit.MILLISECONDS.toNanos(30));
            if (delayNanos > 0) LockSupport.parkNanos(delayNanos);
            worker.interrupt();
            worker.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(worker.isAlive()).isFalse();
            Object r = result.get();
            Path stored = null;
            if (r instanceof Path p) {
                stored = p;
                outcomes.merge("returned:noInterruptSeen", 1, Integer::sum);
            } else if (r instanceof Object[] labelled) {
                stored = (Path) labelled[1];
                outcomes.merge((String) labelled[0], 1, Integer::sum);
            } else {
                threw++;
                Throwable cause = ((Throwable) r).getCause();
                outcomes.merge("threw:" + (cause == null ? r.getClass().getSimpleName() : cause.getClass().getSimpleName()), 1, Integer::sum);
            }
            List<Path> json = files(".json");
            if (stored != null) {
                assertThat(json).as("정상 반환 회차는 .json 정확히 1개").containsExactly(stored);
                assertThat(Files.size(stored)).as("rename된 파일은 항상 완전하다").isEqualTo(bigBytes);
                Files.delete(stored);
            } else {
                assertThat(json).as("실패 회차에서 .json이 생기지 않는다").isEmpty();
            }
            // 이번 회차에 새로 남은 .tmp를 크기별로 센다(수정 전 분포 관찰용).
            for (Path tmp : files(".tmp")) {
                if (!seenTmp.add(tmp)) continue;
                long size = Files.size(tmp);
                String kind = size == 0 ? "tmp:0byte" : size < bigBytes ? "tmp:partial" : "tmp:complete";
                outcomes.merge(kind, 1, Integer::sum);
            }
        }
        System.out.println("SPOOL_INTERRUPT_RANDOM iterations=" + iterations + " outcomes=" + outcomes);
        assertThat(threw).as("실패(정리) 경로를 최소 1회 탔다 — 아니면 아래 .tmp 단언은 아무것도 증명하지 않는다").isPositive();
        assertThat(files(".tmp")).as("어느 창에서 실패해도 .tmp가 남지 않는다").isEmpty();
    }

    /**
     * 반복 인터럽트(Q4). 수정 전에는 실패 1회당 0바이트 .tmp가 1개씩 쌓였다. 그 사이의 정상 저장·depth·드레인 대상은 영향이 없어야 한다.
     */
    @Test
    void repeatedInterruptedStores_doNotAccumulateTmp_andNormalStoresStillWork() throws IOException {
        TelemetrySpool spool = new TelemetrySpool(dir.toString());
        int interrupted = 200;
        int failures = 0;
        for (int i = 0; i < interrupted; i++) {
            try {
                Thread.currentThread().interrupt();
                spool.store(PAYLOAD);
            } catch (IllegalStateException e) {
                failures++;
            } finally {
                Thread.interrupted();
            }
            if (i % 50 == 0) spool.store(PAYLOAD); // 사이사이 정상 저장
        }
        System.out.println("SPOOL_INTERRUPT_REPEAT failures=" + failures + " tmpLeft=" + files(".tmp").size()
            + " json=" + files(".json").size());
        assertThat(failures).isEqualTo(interrupted);
        assertThat(files(".json")).hasSize(4);
        assertThat(spool.depth()).isEqualTo(4);
        assertThat(spool.pending(100)).hasSize(4).allMatch(p -> p.toString().endsWith(".json"));
        assertThat(files(".tmp")).as("반복 실패가 .tmp를 쌓지 않는다").isEmpty();
    }

    /**
     * 재시작 때 남아 있는 .tmp(Q3) — 전원 차단 등 store() 밖의 이유로 남은 0바이트·잘린 .tmp는 수정 뒤에도 생길 수 있다.
     * 기동(initializeBacklog)·드레인이 예외 없이 .json만 보내고, .tmp는 세지도 지우지도 않는다(증거 보존).
     */
    @Test @SuppressWarnings("unchecked")
    void leftoverZeroByteAndPartialTmp_areIgnoredByStartupAndDrain_andNotCountedNorDeleted() throws Exception {
        Files.createDirectories(dir);
        Path zero = Files.createFile(dir.resolve("1791298387712-00000000000000000002-11c1b58d-52cd-423b-8ee4-e3772d8ac524.tmp"));
        Path partial = Files.writeString(dir.resolve("1791298387713-00000000000000000003-aaaaaaaa-52cd-423b-8ee4-e3772d8ac524.tmp"),
            "{\"vehicle_id\":\"INT-0", StandardCharsets.UTF_8);
        TelemetrySpool spool = new TelemetrySpool(dir.toString());
        Path normal = spool.store(PAYLOAD);

        assertThat(spool.pending(10)).containsExactly(normal);
        assertThat(spool.depth()).isEqualTo(1);

        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        var sent = mock(SendResult.class);
        when(sent.getRecordMetadata()).thenReturn(new org.apache.kafka.clients.producer.RecordMetadata(
            new org.apache.kafka.common.TopicPartition("vehicle-telemetry", 0), 0, 0, 0L, 0, 0));
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(sent));
        var registry = new SimpleMeterRegistry();
        var producer = new TelemetryProducer(kafka, new ObjectMapper(), spool, registry, 100);
        producer.initializeBacklog();
        producer.retryPending();

        verify(kafka).send("vehicle-telemetry", "INT-001", PAYLOAD);
        assertThat(Files.exists(normal)).as("드레인된 .json은 지워진다").isFalse();
        assertThat(spool.pending(10)).isEmpty();
        assertThat(spool.depth()).isZero();
        assertThat(registry.get("telemetry.spool.drained").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("telemetry.spool.corrupt").counter().count()).isZero();
        assertThat(registry.get("telemetry.spool.pending").gauge().value()).isZero();
        assertThat(zero).exists().isEmptyFile();
        assertThat(partial).exists().hasContent("{\"vehicle_id\":\"INT-0");
    }

    /**
     * 두 번째 경로(리뷰 지적): 백로그가 아니어도 콜백 스레드가 {@code kafka.send()}의 <b>동기 구간</b>(메타데이터 대기·버퍼 할당,
     * 상한 {@code max.block.ms})에서 막혀 있을 때 인터럽트가 오면. kafka-clients 3.6.2의 {@code KafkaProducer.doSend}는
     * {@code InterruptedException}을 {@code InterruptException}으로 바꿔 던지고(바이트코드 확인), 그 생성자는
     * {@code Thread.interrupt()}로 플래그를 다시 세운다. spring-kafka 3.1.4 {@code KafkaTemplate.doSend}는
     * {@code producer.send} 호출을 감싸지 않는다(catch는 이미 완료된 future의 {@code get()}에만 있다).
     *
     * <p>여기서는 실제 {@link KafkaTemplate}에 가짜 {@code Producer}를 물린다 — send가 메타데이터 대기처럼 막혔다가 인터럽트되면
     * KafkaProducer와 같은 방식으로 {@code new InterruptException(e)}를 던진다. <b>실제 KafkaProducer의 대기 코드를 돌린 것은 아니다.</b>
     */
    @Test
    void kafkaTemplateRethrowsInterruptExceptionFromBlockingSend_withFlagReSet() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        KafkaTemplate<String, String> kafka = realTemplateOverBlockingProducer(entered);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        var flagAfter = new java.util.concurrent.atomic.AtomicBoolean();
        Thread worker = new Thread(() -> {
            try {
                kafka.send("vehicle-telemetry", "INT-001", PAYLOAD);
            } catch (Throwable t) {
                thrown.set(t);
                flagAfter.set(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
        }, "kafka-send-blocked");
        worker.start();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        worker.interrupt();
        worker.join(TimeUnit.SECONDS.toMillis(15));
        assertThat(worker.isAlive()).isFalse();
        assertThat(thrown.get()).as("KafkaTemplate이 감싸지 않고 그대로 던진다")
            .isInstanceOf(org.apache.kafka.common.errors.InterruptException.class);
        assertThat(flagAfter.get()).as("InterruptException 생성자가 플래그를 다시 세운다").isTrue();
    }

    /**
     * 위 예외가 {@code TelemetryProducer.sendDirect}의 {@code catch (Exception)} → {@code storeForRetry}로 가면, 같은 스레드에
     * 플래그가 선 채로 spool에 쓴다 → {@code write}에서 {@code ClosedByInterruptException} → 영속 안 됨, 반환 future는 실패
     * (= MQTT 입구가 ACK하지 않는다), .tmp는 이번 수정으로 지워진다. 플래그는 send 반환 뒤에도 서 있다(지우는 것은 핸들러의 exitWait).
     */
    @Test
    void interruptDuringBlockingKafkaSend_fallsBackToSpoolOnFlaggedThread_failsWithoutJsonOrTmp() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        TelemetrySpool spool = new TelemetrySpool(dir.toString());
        var producer = new TelemetryProducer(realTemplateOverBlockingProducer(entered), new ObjectMapper(), spool,
            new SimpleMeterRegistry(), 100);
        producer.initializeBacklog(); // 빈 spool — 백로그 아님, sendDirect 경로
        var telemetry = new com.telemetry.domain.VehicleTelemetry();
        telemetry.setVehicleId("INT-001");
        telemetry.setTimestamp("2026-10-08T00:00:00Z");
        AtomicReference<CompletableFuture<Void>> receipt = new AtomicReference<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        var flagAfter = new java.util.concurrent.atomic.AtomicBoolean();
        Thread worker = new Thread(() -> {
            try {
                receipt.set(producer.send(telemetry));
                flagAfter.set(Thread.currentThread().isInterrupted());
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                Thread.interrupted();
            }
        }, "mqtt-callback-like");
        worker.start();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        worker.interrupt();
        worker.join(TimeUnit.SECONDS.toMillis(15));
        assertThat(worker.isAlive()).isFalse();

        assertThat(thrown.get()).as("send()는 던지지 않고 future로 실패를 돌려준다").isNull();
        assertThat(receipt.get()).as("ACK를 허용하는 완료가 아니다").isCompletedExceptionally();
        assertThatThrownBy(() -> receipt.get().join())
            .hasCauseInstanceOf(IllegalStateException.class)
            .hasRootCauseInstanceOf(ClosedByInterruptException.class);
        assertThat(flagAfter.get()).as("플래그는 producer 안에서 지워지지 않는다").isTrue();
        assertThat(files(".json")).as("영속되지 않았다").isEmpty();
        assertThat(files(".tmp")).as("실패한 spool 쓰기의 .tmp는 지워진다").isEmpty();
        assertThat(spool.depth()).isZero();
    }

    /** 실제 KafkaTemplate + 메타데이터 대기처럼 막히는 가짜 Producer(최대 10초 = max.block.ms 대역, 인터럽트되면 InterruptException). */
    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> realTemplateOverBlockingProducer(java.util.concurrent.CountDownLatch entered) {
        org.apache.kafka.clients.producer.Producer<String, String> blocking =
            mock(org.apache.kafka.clients.producer.Producer.class);
        when(blocking.send(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any())).thenAnswer(call -> {
            entered.countDown();
            try {
                new java.util.concurrent.CountDownLatch(1).await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new org.apache.kafka.common.errors.InterruptException(e); // KafkaProducer.doSend와 같은 변환
            }
            throw new org.apache.kafka.common.errors.TimeoutException("Topic vehicle-telemetry not present in metadata after 10000 ms.");
        });
        org.springframework.kafka.core.ProducerFactory<String, String> factory = mock(
            org.springframework.kafka.core.ProducerFactory.class,
            call -> call.getMethod().getReturnType() == org.apache.kafka.clients.producer.Producer.class
                ? blocking : org.mockito.Answers.RETURNS_DEFAULTS.answer(call));
        return new KafkaTemplate<>(factory);
    }

    private List<Path> files(String suffix) throws IOException {
        if (!Files.exists(dir)) return List.of();
        try (var s = Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(suffix)).toList();
        }
    }

    @Test
    void cleanupFailureDoesNotMaskOriginalWriteError() throws IOException {
        // 지울 수 없는 "임시 파일"(비어 있지 않은 디렉터리)로 정리 실패를 만든다 — 원래 쓰기 오류가 그대로 남고
        // 정리 실패는 suppressed로만 붙어야 한다(호출자가 보는 실패 원인이 바뀌면 안 된다).
        Path undeletable = Files.createDirectory(dir.resolve("stuck.tmp"));
        Files.writeString(undeletable.resolve("inner"), "x");
        IOException original = new ClosedByInterruptException();

        TelemetrySpool.discardTemporary(undeletable, original);

        assertThat(original).isInstanceOf(ClosedByInterruptException.class);
        assertThat(original.getSuppressed()).hasSize(1);
        assertThat(original.getSuppressed()[0]).isInstanceOf(java.nio.file.DirectoryNotEmptyException.class);
        assertThat(undeletable).exists();
    }
}
