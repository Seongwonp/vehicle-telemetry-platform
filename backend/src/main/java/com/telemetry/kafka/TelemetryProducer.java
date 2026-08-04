package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import jakarta.annotation.PostConstruct;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class TelemetryProducer {

    private static final String TOPIC = "vehicle-telemetry";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final TelemetrySpool telemetrySpool;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean backlog = new AtomicBoolean();

    @PostConstruct
    void initializeBacklog() {
        backlog.set(!telemetrySpool.pending(1).isEmpty());
    }

    /**
     * vehicle_id를 파티션 키로 사용한다.
     * 같은 차량의 메시지가 항상 동일 파티션에 쌓이기 때문에, Consumer가 순서를 보장한 채로 처리할 수 있다.
     * 키 없이 라운드로빈으로 보내면 시계열 순서가 뒤섞여 InfluxDB 저장 시 이상 탐지가 오동작할 수 있다.
     */
    public synchronized void send(VehicleTelemetry telemetry) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(telemetry);
        } catch (Exception e) {
            // 직렬화 실패는 도메인 객체 자체의 문제일 가능성이 높아 데이터 내용을 남긴다
            log.error("[Kafka] 직렬화 실패로 전송 불가 — vehicle={} timestamp={} speed={} rpm={}",
                telemetry.getVehicleId(),
                telemetry.getTimestamp(),
                telemetry.getSpeed(),
                telemetry.getRpm(),
                e);
            return;
        }

        Path spoolFile = telemetrySpool.store(payload);
        if (backlog.get()) return;
        inFlight.add(spoolFile);
        send(spoolFile, telemetry.getVehicleId(), payload);
    }

    @Scheduled(fixedDelayString = "${telemetry.spool.retry-ms:5000}")
    public synchronized void retryPending() {
        var pending = telemetrySpool.pending(100);
        if (pending.isEmpty()) {
            backlog.set(false);
            return;
        }
        backlog.set(true);
        for (Path spoolFile : pending) {
            if (!inFlight.add(spoolFile)) continue;
            try {
                String payload = telemetrySpool.read(spoolFile);
                VehicleTelemetry telemetry = objectMapper.readValue(payload, VehicleTelemetry.class);
                send(spoolFile, telemetry.getVehicleId(), payload);
            } catch (Exception e) {
                inFlight.remove(spoolFile);
                log.error("[Kafka] spool 재전송 준비 실패 path={}", spoolFile, e);
            }
        }
    }

    private void send(Path spoolFile, String vehicleId, String payload) {
        CompletableFuture<SendResult<String, String>> future;
        try {
            future = kafkaTemplate.send(TOPIC, vehicleId, payload);
        } catch (Exception e) {
            inFlight.remove(spoolFile);
            backlog.set(true);
            log.error("[Kafka] 브로커 전송 시작 실패 — spool 유지 vehicle={}", vehicleId, e);
            return;
        }

        future.whenComplete((result, ex) -> {
            inFlight.remove(spoolFile);
            if (ex != null) {
                backlog.set(true);
                log.error("[Kafka] 브로커 전송 실패 — spool 유지 vehicle={} topic={}",
                    vehicleId, TOPIC, ex);
            } else {
                telemetrySpool.delete(spoolFile);
                log.debug("[Kafka] 전송 완료 — vehicle={} partition={} offset={}",
                    vehicleId,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });
    }
}
