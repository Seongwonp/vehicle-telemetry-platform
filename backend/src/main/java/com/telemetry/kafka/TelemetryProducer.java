package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import com.telemetry.domain.VehicleTelemetry;
import lombok.extern.slf4j.Slf4j;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class TelemetryProducer {

    private static final String TOPIC = "vehicle-telemetry";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final TelemetrySpool telemetrySpool;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean backlog = new AtomicBoolean();
    /** 한 주기의 최대 드레인 바퀴 수. 유입이 더 빠르면 여기서 멈추고 backlog를 유지한다. */
    private static final int MAX_DRAIN_PASSES = 20;

    /**
     * 드레인 배치 하나의 크기. 드레인 속도가 유입을 넘어야 backlog가 줄어든다(드레인 중엔 새 메시지도
     * spool로 가므로). 스캔은 limit과 무관하게 디렉터리 전체를 정렬하므로 주기를 줄이기보다 배치를 키운다.
     * 경위: load-test/fault-injection/RESULT_20260904_fault_injection.md
     */
    private final int retryBatchSize;

    private final Timer spoolScanTimer;
    private final Counter spoolDrainedCounter;
    private final Counter spoolCorruptCounter;
    private final AtomicLong spoolDepth = new AtomicLong();

    public TelemetryProducer(KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper,
                             TelemetrySpool telemetrySpool,
                             MeterRegistry meterRegistry,
                             @Value("${telemetry.spool.retry-batch:10000}") int retryBatchSize) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.telemetrySpool = telemetrySpool;
        this.retryBatchSize = retryBatchSize;
        this.spoolScanTimer = meterRegistry.timer("telemetry.spool.scan");
        this.spoolDrainedCounter = meterRegistry.counter("telemetry.spool.drained");
        // 0이 아니면 .corrupt 파일을 사람이 봐야 한다(유실 후보).
        this.spoolCorruptCounter = meterRegistry.counter("telemetry.spool.corrupt");
        meterRegistry.gauge("telemetry.spool.pending", spoolDepth, AtomicLong::get);
    }

    @PostConstruct
    void initializeBacklog() {
        backlog.set(!telemetrySpool.pending(1).isEmpty());
    }

    /**
     * vehicle_id를 파티션 키로 쓴다 — 같은 차량의 순서를 유지하기 위해서다(ADR-003).
     *
     * <p>정상 경로는 디스크를 거치지 않고, 전송 실패 시에만 spool에 적는다(ADR-019).
     * 반환 future는 Kafka 또는 spool 기록 성공 뒤에만 완료되고, MQTT 입구는 이를 기다려 ACK한다(ADR-029).
     * 직렬화 실패는 재시도해도 같으므로 {@link UnsendableTelemetryException}으로 구분한다.
     */
    public CompletableFuture<Void> send(VehicleTelemetry telemetry) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(telemetry);
        } catch (Exception e) {
            log.error("[Kafka] 직렬화 실패로 전송 불가 — vehicle={} ts={} speed={} rpm={}",
                telemetry.getVehicleId(),
                telemetry.getTimestamp(),
                telemetry.getSpeed(),
                telemetry.getRpm(),
                e);
            return CompletableFuture.failedFuture(new UnsendableTelemetryException(e));
        }

        // 밀린 spool이 있으면 새 메시지도 spool로 보낸다 — 파일명 순 드레인으로 차량별 순서를 지킨다.
        if (backlog.get()) {
            return storeForRetry(telemetry.getVehicleId(), telemetry.getTimestamp(), payload, null);
        }
        return sendDirect(telemetry.getVehicleId(), telemetry.getTimestamp(), payload);
    }

    /**
     * spool 실패분을 재전송한다. {@link #send}와 락을 공유하지 않는다 — 공유하면 스캔 동안 MQTT 수집이 멈춘다.
     * 그 대가로 복구 직후 짧은 구간에 순서가 뒤집힐 수 있다(backlog는 best-effort). 저장은 timestamp 기준이라 영향 없다.
     */
    @Scheduled(fixedDelayString = "${telemetry.spool.retry-ms:5000}")
    public synchronized void retryPending() {
        // 배치 전송이 끝나기를 기다린 뒤 다시 스캔하고, 빈 스캔을 본 자리에서만 backlog를 푼다.
        // 드레인 완료 전에 풀면 새 직접 전송이 spool보다 먼저 나가 차량별 순서가 뒤집히고,
        // 다음 주기까지 미루면 유입이 있는 한 영원히 안 풀린다. 경위: docs/verification/2026-09-29-spool-backlog-fix.md
        for (int pass = 0; pass < MAX_DRAIN_PASSES; pass++) {
            var pending = telemetrySpool.pending(retryBatchSize);
            spoolScanTimer.record(telemetrySpool.lastScanNanos(), TimeUnit.NANOSECONDS);
            if (pending.isEmpty()) {
                backlog.set(false);
                spoolDepth.set(0);
                return;
            }
            backlog.set(true);
            // 정확한 깊이는 비싸므로 "최소 이만큼"만 넣는다. 알림에는 충분하다.
            spoolDepth.set(pending.size());
            if (!drainBatch(pending)) {
                return; // 파일이 남았으니 backlog 유지
            }
        }
    }

    /** 배치 하나를 보내고 완료를 기다린다. 전부 성공했으면 true. */
    private boolean drainBatch(List<Path> pending) {
        boolean clean = true;
        List<CompletableFuture<Boolean>> sends = new ArrayList<>(pending.size());
        for (Path spoolFile : pending) {
            if (!inFlight.add(spoolFile)) continue;
            String payload;
            try {
                payload = telemetrySpool.read(spoolFile);
            } catch (Exception e) {
                // 일시적일 수 있다(잠금 등).
                inFlight.remove(spoolFile);
                clean = false;
                log.error("[Kafka] spool 읽기 실패 — 다음 주기에 재시도 path={}", spoolFile, e);
                continue;
            }
            VehicleTelemetry telemetry;
            try {
                telemetry = objectMapper.readValue(payload, VehicleTelemetry.class);
                if (telemetry == null || telemetry.getVehicleId() == null) {
                    throw new IllegalArgumentException("vehicle_id 없음");
                }
            } catch (Exception e) {
                // 해석 불가 파일은 다시 읽어도 같다. 남겨두면 spool이 영원히 안 비어 backlog가 안 풀리므로
                // 격리하고 센다. 격리는 spool에서 빠진 것이라 clean을 깨지 않는다.
                inFlight.remove(spoolFile);
                spoolCorruptCounter.increment();
                Path moved = telemetrySpool.quarantine(spoolFile);
                log.error("[Kafka] spool 손상 파일 격리 — 드레인에서 제외 path={} quarantined={} bytes={} payloadSha256={}",
                    spoolFile, moved, payload.length(), com.telemetry.domain.PayloadDigest.sha256(payload), e);
                continue;
            }
            sends.add(sendSpooled(spoolFile, telemetry.getVehicleId(), telemetry.getTimestamp(), payload));
        }
        for (CompletableFuture<Boolean> send : sends) {
            // 스케줄러 스레드라 기다려도 수집은 막히지 않는다.
            if (!Boolean.TRUE.equals(send.join())) clean = false;
        }
        return clean;
    }

    private CompletableFuture<Void> sendDirect(String vehicleId, String ts, String payload) {
        CompletableFuture<SendResult<String, String>> future;
        try {
            future = kafkaTemplate.send(TOPIC, vehicleId, payload);
        } catch (Exception e) {
            return storeForRetry(vehicleId, ts, payload, e);
        }

        return future.handle((result, ex) -> {
            if (ex != null) {
                return storeForRetry(vehicleId, ts, payload, ex);
            } else {
                // vehicle·ts와 (partition, offset)을 한 줄에 — 추적 키(ADR-028).
                log.debug("[Kafka] 전송 완료 — vehicle={} ts={} partition={} offset={}",
                    vehicleId, ts,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
                return CompletableFuture.<Void>completedFuture(null);
            }
        }).thenCompose(receipt -> receipt);
    }

    /** 전송 성공 뒤에만 파일을 지운다. */
    private CompletableFuture<Boolean> sendSpooled(Path spoolFile, String vehicleId, String ts, String payload) {
        CompletableFuture<SendResult<String, String>> future;
        try {
            future = kafkaTemplate.send(TOPIC, vehicleId, payload);
        } catch (Exception e) {
            inFlight.remove(spoolFile);
            backlog.set(true);
            log.error("[Kafka] spool 재전송 시작 실패 — 파일 유지 vehicle={} ts={}", vehicleId, ts, e);
            return CompletableFuture.completedFuture(false);
        }

        return future.handle((result, ex) -> {
            inFlight.remove(spoolFile);
            if (ex != null) {
                backlog.set(true);
                log.error("[Kafka] spool 재전송 실패 — 파일 유지 vehicle={} ts={} topic={}",
                    vehicleId, ts, TOPIC, ex);
                return false;
            } else {
                // 드레인된 메시지는 새 offset을 받는다 — 추적 시 이 줄이 좌표를 잇는다.
                log.debug("[Kafka] spool 드레인 완료 — vehicle={} ts={} partition={} offset={}",
                    vehicleId, ts,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
                telemetrySpool.delete(spoolFile);
                // 선택 시점이 아니라 삭제 뒤에 센다 — 선택 시점에 세면 중복 계산된다.
                spoolDrainedCounter.increment();
                return true;
            }
        });
    }

    private CompletableFuture<Void> storeForRetry(String vehicleId, String ts, String payload, Throwable cause) {
        backlog.set(true);
        try {
            telemetrySpool.store(payload);
            if (cause != null) {
                log.error("[Kafka] 브로커 전송 실패 — spool에 보관 vehicle={} ts={} topic={}",
                    vehicleId, ts, TOPIC, cause);
            }
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException spoolFailure) {
            log.error("[Kafka] 전송 실패 후 spool 저장까지 실패 — MQTT ACK 금지 vehicle={} ts={}",
                vehicleId, ts, spoolFailure);
            return CompletableFuture.failedFuture(spoolFailure);
        }
    }

    /**
     * 재시도해도 같은 실패(직렬화). 일시 실패와 구분해야 MQTT 입구가 DLQ 후 ACK한다 —
     * ACK 없이 두면 브로커가 재접속마다 맨 앞에서 재전달해 구독 전체가 막힌다.
     */
    public static class UnsendableTelemetryException extends RuntimeException {
        public UnsendableTelemetryException(Throwable cause) {
            super("telemetry를 Kafka payload로 만들 수 없다", cause);
        }
    }
}
