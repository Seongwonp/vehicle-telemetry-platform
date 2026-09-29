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

    /**
     * 한 번의 재전송 주기에 처리할 spool 파일 수.
     *
     * <p>이 값과 {@code telemetry.spool.retry-ms}가 곱해져 드레인 속도의 <b>상한</b>이 된다 —
     * 부하나 브로커 용량과 무관하다. 예전 값(100 / 5초)은 20 msg/s였는데, 유입이
     * 약 1,700 msg/s라 90초 장애가 약 35분의 복구 시간을 만들었다
     * ({@code load-test/fault-injection/RESULT_20260904_fault_injection.md}).
     *
     * <p>주기를 줄이는 것보다 배치를 키우는 쪽이 낫다 — {@link TelemetrySpool#pending(int)}이
     * limit과 무관하게 디렉터리 전체를 정렬하므로, 호출 횟수를 늘리면 그 비용이 그대로 늘어난다.
     * 실측하면 파일 15만 개에서 스캔 1회가 약 121ms라, 5초 주기의 2.4%다 — 스캔은 병목이 아니고
     * 배치 크기가 그대로 상한이 된다.
     *
     * <p>측정값(유입 약 1,700 msg/s, 180초 장애):
     * <pre>
     *   배치    100 →  19 msg/s   (잔여 드레인 105분)
     *   배치  2,000 → 387 msg/s   (잔여 드레인 5분)
     *   배치 10,000 → 부하 정지 후 약 1분 내 완전 드레인
     * </pre>
     *
     * <p><b>주의</b>: 드레인 중에는 {@code backlog} 플래그 때문에 새 메시지도 spool로 가므로,
     * 부하가 계속되는 동안 백로그가 줄어드는 속도는 <i>드레인 속도 − 유입 속도</i>다.
     * 즉 유입과 같은 속도로는 부족하고 <b>넘어서야</b> 줄어든다.
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
        // 0이 아니면 사람이 .corrupt 파일을 봐야 한다 — 유실 후보다.
        this.spoolCorruptCounter = meterRegistry.counter("telemetry.spool.corrupt");
        // 백로그가 쌓이는데 안 줄어드는 상황을 알림으로 잡으려면 깊이가 지표로 있어야 한다.
        meterRegistry.gauge("telemetry.spool.pending", spoolDepth, AtomicLong::get);
    }

    @PostConstruct
    void initializeBacklog() {
        backlog.set(!telemetrySpool.pending(1).isEmpty());
    }

    /**
     * vehicle_id를 파티션 키로 사용한다.
     * 같은 차량의 메시지가 항상 동일 파티션에 쌓이기 때문에, Consumer가 순서를 보장한 채로 처리할 수 있다.
     * 키 없이 라운드로빈으로 보내면 시계열 순서가 뒤섞여 InfluxDB 저장 시 이상 탐지가 오동작할 수 있다.
     *
     * <p><b>정상 경로에서는 디스크를 거치지 않는다.</b> 예전에는 메시지마다 spool 파일을 먼저
     * 쓰고(디렉터리 생성 → 파일 쓰기 → rename, 전송 성공 후 삭제까지 파일시스템 연산 약 5회)
     * Kafka로 보냈는데, 이 메서드가 MQTT Paho 콜백 단일 스레드에서 {@code synchronized}로
     * 호출되다 보니 수집 전체가 디스크 지연에 직렬로 묶였다. 실측 결과 시뮬레이터가
     * 초당 약 10,000건을 발행하고 mosquitto가 전량 PUBACK하는 동안 Kafka에는 초당 20건만
     * 도착했다 — 백엔드가 못 받아가 브로커 큐가 넘치면서 나머지가 조용히 버려지고 있었다.
     *
     * <p>spool의 목적은 <i>Kafka 브로커 장애 시 유실 방지</i>인데, Kafka 프로듀서 자체가
     * 내부 버퍼와 재시도(acks=all, retries=3)를 갖고 있다. 그래서 전송에 실패했을 때만
     * spool에 적는다. 트레이드오프: 백엔드가 Kafka ack 전에 죽으면 그 인플라이트 구간은
     * 유실된다. 항상 spool하던 예전 방식은 이 구간까지 지켰지만, 그 대가로 실제로는
     * 99.8%를 잃고 있었다.
     */
    public void send(VehicleTelemetry telemetry) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(telemetry);
        } catch (Exception e) {
            // 직렬화 실패는 도메인 객체 자체의 문제일 가능성이 높아 데이터 내용을 남긴다
            log.error("[Kafka] 직렬화 실패로 전송 불가 — vehicle={} ts={} speed={} rpm={}",
                telemetry.getVehicleId(),
                telemetry.getTimestamp(),
                telemetry.getSpeed(),
                telemetry.getRpm(),
                e);
            return;
        }

        // 이미 밀린 spool이 있으면 새 메시지도 spool로 보낸다 — 그래야 retryPending()이
        // 파일명(타임스탬프+시퀀스) 순서대로 드레인하면서 차량별 순서가 유지된다.
        if (backlog.get()) {
            storeForRetry(telemetry.getVehicleId(), telemetry.getTimestamp(), payload, null);
            return;
        }
        sendDirect(telemetry.getVehicleId(), telemetry.getTimestamp(), payload);
    }

    /**
     * spool에 남은 실패분을 주기적으로 재전송한다.
     *
     * <p>자기 자신에 대해서만 {@code synchronized}다 — 예전에는 {@link #send}와 같은 락을
     * 공유해서, 5초마다 도는 이 스케줄러가 spool 디렉터리를 스캔하는 동안 MQTT 수집
     * 스레드까지 멈춰 세웠다. 대신 브로커 복구 직후 아주 짧은 구간에서는 새 메시지가
     * spool 드레인보다 먼저 Kafka에 닿아 같은 차량 메시지의 순서가 뒤집힐 수 있다
     * (backlog 플래그가 best-effort라서). 각 메시지가 자체 timestamp를 갖고 있어
     * 저장/조회는 영향받지 않는다.
     */
    @Scheduled(fixedDelayString = "${telemetry.spool.retry-ms:5000}")
    public synchronized void retryPending() {
        var pending = telemetrySpool.pending(retryBatchSize);
        spoolScanTimer.record(telemetrySpool.lastScanNanos(), TimeUnit.NANOSECONDS);
        if (pending.isEmpty()) {
            backlog.set(false);
            spoolDepth.set(0);
            return;
        }
        backlog.set(true);
        // 배치를 가득 채워 왔다면 아직 더 남았다는 뜻이다. 정확한 깊이는 별도 스캔이
        // 필요해서 비싸므로, 게이지에는 "최소 이만큼"을 넣는다 — 알림 목적에는 충분하다.
        spoolDepth.set(pending.size());
        List<CompletableFuture<Boolean>> sends = new ArrayList<>(pending.size());
        for (Path spoolFile : pending) {
            if (!inFlight.add(spoolFile)) continue;
            String payload;
            try {
                payload = telemetrySpool.read(spoolFile);
            } catch (Exception e) {
                // 읽기 실패는 일시적일 수 있다(잠금 등) — 다음 주기에 다시 본다.
                inFlight.remove(spoolFile);
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
                // **내용을 해석할 수 없는 파일은 몇 번을 다시 읽어도 같다.** 예전엔 여기서 로그만 남기고 파일을 뒀다 —
                // pending이 영원히 비지 않아 backlog가 켜진 채로 모든 새 메시지가 디스크를 거쳤다(ADR-019가 고친 병목).
                // 격리하고 센다. 0바이트·잘린 JSON이 전형이다(전원 차단 — TelemetrySpool.store의 force 참고).
                inFlight.remove(spoolFile);
                spoolCorruptCounter.increment();
                Path moved = telemetrySpool.quarantine(spoolFile);
                log.error("[Kafka] spool 손상 파일 격리 — 드레인에서 제외 path={} quarantined={} bytes={} payloadSha256={}",
                    spoolFile, moved, payload.length(), com.telemetry.domain.PayloadDigest.sha256(payload), e);
                continue;
            }
            sends.add(sendSpooled(spoolFile, telemetry.getVehicleId(), telemetry.getTimestamp(), payload));
        }
        // **디스크에 있던 것을 이번 주기에 전부 집었고 전부 나갔으면 backlog를 여기서 푼다.**
        // "다음 스캔이 빈 것을 볼 때"까지 기다리면, 유입이 있는 한 스캔은 영원히 비지 않는다 —
        // backlog가 켜져 새 메시지가 spool로 가고, 5초 뒤 스캔이 그 파일들을 보고 다시 켜고…
        // 정상 경로가 5초 주기 디스크 왕복에 갇힌다(2026-09-29 실측: pending 10 고정, 발행→수신 약 6초).
        // 가득 찬 배치는 아직 남았다는 뜻이라 풀지 않는다. 드레인 중 spool로 간 새 메시지는 다음 주기가 집는다.
        if (pending.size() < retryBatchSize) {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).whenComplete((v, ex) -> {
                if (sends.stream().allMatch(f -> Boolean.TRUE.equals(f.getNow(false)))) {
                    backlog.set(false);
                }
            });
        }
    }

    /** 정상 경로 — spool 파일 없이 바로 보내고, 실패했을 때만 spool에 남긴다. */
    private void sendDirect(String vehicleId, String ts, String payload) {
        CompletableFuture<SendResult<String, String>> future;
        try {
            future = kafkaTemplate.send(TOPIC, vehicleId, payload);
        } catch (Exception e) {
            storeForRetry(vehicleId, ts, payload, e);
            return;
        }

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                storeForRetry(vehicleId, ts, payload, ex);
            } else {
                // vehicle·ts가 (partition, offset)과 한 줄에 있어야 로그에서 Kafka 좌표로 건너갈 수 있다(ADR-028).
                log.debug("[Kafka] 전송 완료 — vehicle={} ts={} partition={} offset={}",
                    vehicleId, ts,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });
    }

    /** spool에서 꺼낸 메시지 재전송 — 성공해야만 파일을 지운다. 성공 여부를 돌려준다(backlog 해제 판단용). */
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
                // 드레인된 메시지는 새 offset을 받는다 — 추적할 때 이 줄이 원래 좌표와 새 좌표를 잇는다.
                log.debug("[Kafka] spool 드레인 완료 — vehicle={} ts={} partition={} offset={}",
                    vehicleId, ts,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
                telemetrySpool.delete(spoolFile);
                // 선택된 파일이 아니라 **실제로 빠져나간 파일**을 센다. 선택 시점에 세면
                // 이전 주기의 전송이 아직 안 끝난 파일을 다음 주기가 또 집어 중복 계산된다
                // (실측에서 drained 379,536 > MQTT 수신 261,340으로 드러났다).
                spoolDrainedCounter.increment();
                return true;
            }
        });
    }

    /** 전송 실패분을 spool에 적어 재전송 대상으로 남긴다. */
    private void storeForRetry(String vehicleId, String ts, String payload, Throwable cause) {
        backlog.set(true);
        try {
            telemetrySpool.store(payload);
            if (cause != null) {
                log.error("[Kafka] 브로커 전송 실패 — spool에 보관 vehicle={} ts={} topic={}",
                    vehicleId, ts, TOPIC, cause);
            }
        } catch (RuntimeException spoolFailure) {
            // 여기까지 실패하면 이 메시지는 정말로 유실된다 — 조용히 넘기지 않는다.
            log.error("[Kafka] 전송 실패 후 spool 저장까지 실패 — 메시지 유실 vehicle={} ts={}",
                vehicleId, ts, spoolFailure);
        }
    }
}
