package com.telemetry.mqtt;

import com.telemetry.domain.TelemetryContractException;
import com.telemetry.domain.TelemetryDecoder;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.kafka.TelemetryProducer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.acks.SimpleAcknowledgment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.springframework.context.event.EventListener;
import org.springframework.integration.mqtt.event.MqttConnectionFailedEvent;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
public class MqttMessageHandler {

    private static final Pattern TELEMETRY_TOPIC = Pattern.compile(
        "^vehicle/telemetry/([A-Z0-9-]{4,20})$");

    private final TelemetryProducer telemetryProducer;
    private final TelemetryDecoder telemetryDecoder;
    private final MeterRegistry meterRegistry;
    private final Counter receivedCounter;
    private final Counter invalidCounter;
    private final MqttInvalidMessagePublisher invalidMessagePublisher;
    private final Counter missingAckCallbackCounter;
    private final Counter decodeFailedCounter;
    /**
     * 연결 끊김 인터럽트가 대기 구간 안에서만 닿게 하는 잠금. 구간을 닫은 뒤(ACK·다음 메시지·Paho 내부)에
     * 인터럽트가 새면 안 된다. 아래 세 필드는 이 잠금으로 보호한다.
     */
    private final Object waitLock = new Object();
    private Thread awaitingThread;
    private boolean interruptedByConnectionLoss;
    private long waitStartNanos;
    /** 끝난 대기만 잰다. 막혀 있는 순간은 진행 중 게이지가 보여준다. */
    private final Timer ackWaitAcked;
    private final Timer ackWaitFailed;
    private final Timer ackWaitInterrupted;
    /** producer 최악 대기(40s)·알림 문턱(45s)·keepAlive(60s)·get 상한(150s)을 경계에 둔다. */
    private static final Duration[] ACK_WAIT_BUCKETS = {
        Duration.ofMillis(10), Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(500),
        Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(30),
        Duration.ofSeconds(40), Duration.ofSeconds(45), Duration.ofSeconds(60), Duration.ofSeconds(150)};

    public MqttMessageHandler(
        TelemetryProducer telemetryProducer,
        TelemetryDecoder telemetryDecoder,
        MeterRegistry meterRegistry,
        MqttInvalidMessagePublisher invalidMessagePublisher
    ) {
        this.telemetryProducer = telemetryProducer;
        this.telemetryDecoder = telemetryDecoder;
        this.meterRegistry = meterRegistry;
        this.receivedCounter = meterRegistry.counter("telemetry.mqtt.messages.received");
        this.invalidCounter = meterRegistry.counter("telemetry.mqtt.messages.invalid");
        this.invalidMessagePublisher = invalidMessagePublisher;
        // 0이 아니면 manualAcks가 꺼진 것이다(ADR-029 보장 소실).
        this.missingAckCallbackCounter = meterRegistry.counter("telemetry.mqtt.ack.callback.missing");
        // 계약 예외가 아닌 decode 실패. 0이 아니면 validator·매퍼 회귀를 의심한다.
        this.decodeFailedCounter = meterRegistry.counter("telemetry.mqtt.decode.failed");
        this.ackWaitAcked = ackWaitTimer(meterRegistry, "acked");
        this.ackWaitFailed = ackWaitTimer(meterRegistry, "failed");
        this.ackWaitInterrupted = ackWaitTimer(meterRegistry, "interrupted");
        // 기동 때 등록해야 시계열 부재(설정 문제)와 0(대기 없음)이 구분된다.
        Gauge.builder("telemetry.mqtt.ack.wait.in.progress", this, h -> h.waitInProgress() ? 1 : 0)
            .description("Paho 콜백 스레드가 지금 저장 확인을 기다리는 중이면 1")
            .register(meterRegistry);
        Gauge.builder("telemetry.mqtt.ack.wait.elapsed.seconds", this, MqttMessageHandler::currentWaitSeconds)
            .description("진행 중인 저장 확인 대기의 경과 초(scrape 시점 계산, 대기 없으면 0)")
            .register(meterRegistry);
    }

    private static Timer ackWaitTimer(MeterRegistry registry, String outcome) {
        return Timer.builder("telemetry.mqtt.ack.wait")
            .description("Paho 콜백 스레드의 저장 확인 대기 시간(끝난 대기만)")
            .tag("outcome", outcome)
            .serviceLevelObjectives(ACK_WAIT_BUCKETS)
            .register(registry);
    }

    @ServiceActivator(inputChannel = "mqttInputChannel")
    public void handle(Message<String> message) {
        receivedCounter.increment();
        var acknowledgment = message.getHeaders().get(
            IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, SimpleAcknowledgment.class);
        // 헤더가 없으면 자동 ACK(manualAcks 꺼짐)다. 대조 실험·단위 테스트용이며 운영에서는 설정 회귀다.
        if (acknowledgment == null) {
            missingAckCallbackCounter.increment();
            process(message);
            return;
        }

        // ACK는 저장 확인(Kafka·spool·DLQ) 뒤에만 보낸다(ADR-029). Paho 콜백 스레드에서 기다려야
        // ACK가 수신 순서로 나간다. 대기 구간은 처리 전체를 덮어 어디서 막혀도 연결 끊김이 깨울 수 있다.
        boolean interrupted = false;
        Exception failure = null;
        long started = enterWait();
        try {
            process(message).get(150, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
            failure = e;
        } catch (Exception e) {
            failure = e;
        } finally {
            boolean ours = exitWait();
            (interrupted ? ackWaitInterrupted : failure != null ? ackWaitFailed : ackWaitAcked)
                .record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
            // 연결 끊김 인터럽트는 여기서 삼킨다(Paho 스레드는 재연결을 이어가야 한다). 다른 출처는 복원한다.
            if (interrupted && !ours) Thread.currentThread().interrupt();
        }
        if (failure != null) {
            // Paho까지 던져 ACK 없이 끝낸다 — 재접속 뒤 브로커 재전달에 맡긴다(ADR-029).
            throw new IllegalStateException(interrupted
                ? "MQTT 저장 확인 중단 — ACK하지 않음" : "MQTT 저장 확인 실패 — ACK하지 않음", failure);
        }
        acknowledgment.acknowledge();
    }

    /**
     * ACK해도 되는 시점에 완료되는 future를 돌려준다.
     * 거부는 DLQ 발행 성공 뒤(실패하면 던진다), 정상은 Kafka 또는 spool 기록 뒤.
     */
    private CompletableFuture<Void> process(Message<String> message) {
        String payload = message.getPayload();
        String topic = (String) message.getHeaders().get("mqtt_receivedTopic");

        // Kafka 저장 입구와 같은 decoder를 쓴다 — 입구에 따라 판정이 갈리면 안 된다.
        VehicleTelemetry telemetry;
        try {
            telemetry = telemetryDecoder.decode(payload);
        } catch (TelemetryContractException e) {
            reject(topic, payload, e.getReason(), null);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            // decoder는 외부 의존이 없어 같은 payload면 매번 같은 실패다. ACK 없이 던지면 재접속마다
            // 맨 앞에서 재전달되어 구독 전체가 막힌다. 격리 후 ACK한다(DLQ 실패면 ACK하지 않는다).
            log.warn("[MQTT] decode 중 계약 예외가 아닌 실패 — 격리 topic={} payloadSha256={}",
                topic, sha256(payload), e);
            decodeFailedCounter.increment();
            reject(topic, payload, "DECODE_FAILED", null);
            return CompletableFuture.completedFuture(null);
        }

        Matcher topicMatcher = topic == null ? null : TELEMETRY_TOPIC.matcher(topic);
        if (!validTimestamp(telemetry.getTimestamp())) {
            reject(topic, payload, "INVALID_TIMESTAMP", telemetry);
            return CompletableFuture.completedFuture(null);
        }
        if (topicMatcher == null || !topicMatcher.matches()
            || !topicMatcher.group(1).equals(telemetry.getVehicleId())) {
            reject(topic, payload, "TOPIC_VEHICLE_MISMATCH", telemetry);
            return CompletableFuture.completedFuture(null);
        }

        // 추적 키 vehicle=, ts=는 전 구간 같은 이름이다(ADR-028).
        log.debug("[MQTT→Kafka] vehicle={} ts={} speed={} engine_temp={} battery_voltage={}",
            telemetry.getVehicleId(),
            telemetry.getTimestamp(),
            telemetry.getSpeed(),
            telemetry.getEngineTemp(),
            telemetry.getBatteryVoltage());

        // Unsendable은 send() 안에서 동기적으로 실패하므로 아래 reject()의 DLQ get이 이 콜백 스레드에서 돈다.
        // 비동기 실패를 여기서 격리하면 Kafka I/O 스레드에서 get()이 자기 교착된다 — 일시 실패로 남긴다.
        return telemetryProducer.send(telemetry)
            .exceptionallyCompose(e -> {
                Throwable cause = e instanceof java.util.concurrent.CompletionException && e.getCause() != null
                    ? e.getCause() : e;
                if (!(cause instanceof TelemetryProducer.UnsendableTelemetryException)) {
                    return CompletableFuture.failedFuture(cause); // 일시 실패 — ACK하지 않고 재전달을 기다린다
                }
                // 격리 뒤 ACK. DLQ 발행 실패면 그대로 실패로 남아 ACK하지 않는다.
                reject(topic, payload, "SERIALIZATION_FAILED", telemetry);
                return CompletableFuture.completedFuture(null);
            });
    }

    /** @return 대기 시작 시각(System.nanoTime) */
    private long enterWait() {
        synchronized (waitLock) {
            awaitingThread = Thread.currentThread();
            interruptedByConnectionLoss = false;
            waitStartNanos = System.nanoTime();
            return waitStartNanos;
        }
    }

    private boolean waitInProgress() {
        synchronized (waitLock) {
            return awaitingThread != null;
        }
    }

    /** scrape 때 계산하므로 막혀 있는 동안에도 자란다. */
    private double currentWaitSeconds() {
        synchronized (waitLock) {
            return awaitingThread == null ? 0.0 : (System.nanoTime() - waitStartNanos) / 1e9;
        }
    }

    /** 연결 끊김 인터럽트가 걸렸었으면 true. 남은 인터럽트 플래그도 여기서 지운다. */
    private boolean exitWait() {
        synchronized (waitLock) {
            awaitingThread = null;
            boolean ours = interruptedByConnectionLoss;
            interruptedByConnectionLoss = false;
            // get()이 정상 반환한 뒤 닿은 인터럽트가 구간 밖으로 새지 않게 한다.
            if (ours) Thread.interrupted();
            return ours;
        }
    }

    /**
     * 연결 끊김 때 저장 확인을 기다리는 Paho 콜백 스레드를 깨운다. Paho 재연결은 옛 콜백 스레드가
     * 끝날 때까지 기다리므로, 깨우지 않으면 브로커가 살아 있어도 최대 150초 재연결이 멈춘다(ADR-029).
     * 옛 연결의 ACK는 어차피 쓸 수 없으니 ACK 없이 빠져나와 재전달에 맡긴다(중복은 허용).
     * 콜백 스레드가 spool에 쓰던 중이면 쓰기가 실패하고 ACK하지 않는다
     * ({@code docs/verification/2026-10-08-spool-interrupt-durability.md}).
     */
    @EventListener
    public void onConnectionLost(MqttConnectionFailedEvent event) {
        // $SYS 어댑터의 연결 끊김은 텔레메트리 ACK와 무관하다. 판별은 토픽 문자열에 기댄다.
        if (event.getSource() instanceof MqttPahoMessageDrivenChannelAdapter adapter
            && java.util.Arrays.stream(adapter.getTopic()).anyMatch(t -> t.startsWith("$SYS"))) {
            return;
        }
        synchronized (waitLock) {
            if (awaitingThread != null) {
                log.warn("[MQTT] 연결 끊김 — 저장 확인 대기 중단, ACK 없이 재전달 대기");
                interruptedByConnectionLoss = true;
                awaitingThread.interrupt();
            }
        }
    }

    /**
     * @param decoded MQTT 고유 검사에서 걸린 경우의 객체, 계약 위반이면 null.
     *                추적 키로만 쓰고 신뢰하지 않는다.
     */
    private void reject(String topic, String payload, String reason, VehicleTelemetry decoded) {
        // 모든 거부를 센다. 기존 지표라 이름·의미를 바꾸지 않는다.
        invalidCounter.increment();
        // 사유별 지표는 계약 사유만. MQTT 고유 사유를 섞으면 세 입구를 비교할 수 없다.
        if (TelemetryContractException.isContractReason(reason)) {
            com.telemetry.metrics.ContractMetrics.rejected(meterRegistry, com.telemetry.metrics.ContractMetrics.ENTRANCE_MQTT, reason);
        }
        // payload 값은 로그에 남기지 않고 해시로 DLQ value와 대조한다.
        log.warn("[MQTT] 메시지 거부 — topic={} reason={} vehicle={} ts={} payloadLength={} payloadSha256={}",
            topic, reason,
            decoded == null ? "-" : decoded.getVehicleId(),
            decoded == null ? "-" : decoded.getTimestamp(),
            payload.length(), sha256(payload));
        invalidMessagePublisher.publish(topic, payload, reason);
    }

    private boolean validTimestamp(String timestamp) {
        try {
            Instant.parse(timestamp);
            return true;
        } catch (DateTimeParseException | NullPointerException e) {
            return false;
        }
    }

    static String sha256(String payload) {
        return com.telemetry.domain.PayloadDigest.sha256(payload);
    }
}
