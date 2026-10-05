package com.telemetry.mqtt;

import com.telemetry.domain.TelemetryContractException;
import com.telemetry.domain.TelemetryDecoder;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.kafka.TelemetryProducer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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
     * 연결 끊김 인터럽트를 **대기 구간 안에서만** 보내기 위한 잠금. 인터럽트를 보내는 쪽과 대기 구간을 닫는 쪽이
     * 같은 잠금을 쓰므로, 구간을 닫은 뒤(ACK·다음 메시지·Paho 내부 코드)에 인터럽트가 닿지 않는다.
     */
    private final Object waitLock = new Object();
    /** 지금 저장 확인을 기다리는 Paho 콜백 스레드. {@link #waitLock}으로 보호. */
    private Thread awaitingThread;
    /** 이번 대기 구간의 인터럽트가 연결 끊김 때문인지 — 아니면 플래그를 복원한다. {@link #waitLock}으로 보호. */
    private boolean interruptedByConnectionLoss;

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
        // 0이 아니면 manualAcks가 꺼진 것이다 — ACK가 저장 확인을 기다리지 않는다(ADR-029 보장 소실).
        this.missingAckCallbackCounter = meterRegistry.counter("telemetry.mqtt.ack.callback.missing");
        // 계약 예외가 아닌 decode 실패로 격리한 수. 0이 아니면 validator·매퍼 회귀를 의심한다(사유별 계약 지표에는 안 잡힌다).
        this.decodeFailedCounter = meterRegistry.counter("telemetry.mqtt.decode.failed");
    }

    // @ServiceActivator는 MqttConfig에서 선언한 mqttInputChannel과 이 메서드를 연결한다.
    // Spring Integration 채널 기반이라 별도 스레드 풀 없이 메시지 도착 즉시 호출된다.
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public void handle(Message<String> message) {
        receivedCounter.increment();
        var acknowledgment = message.getHeaders().get(
            IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, SimpleAcknowledgment.class);
        // 헤더가 없으면 어댑터가 자동 ACK다(manualAcks 꺼짐) — 기다려도 ACK 시점을 바꿀 수 없다.
        // 자동 ACK 대조 실험과 decoder 단위 테스트가 이 경로를 쓴다. 운영에서 0이 아니면 설정 회귀다.
        if (acknowledgment == null) {
            missingAckCallbackCounter.increment();
            process(message);
            return;
        }

        // Paho 단일 콜백 스레드에서 기다린다 — ACK가 수신 순서로 나가고 Kafka 완료 스레드가 ACK하지 않는다.
        // 동시성을 내주고 작고 검토 가능한 경계를 샀다(ADR-029). 대기 구간은 처리 전체(DLQ 발행 get(10s)·
        // Kafka send의 max.block 포함)를 덮는다 — 어디서 막혀 있든 연결 끊김이 깨울 수 있게.
        boolean interrupted = false;
        Exception failure = null;
        enterWait();
        try {
            process(message).get(150, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
            failure = e;
        } catch (Exception e) {
            failure = e;
        } finally {
            boolean ours = exitWait();
            // 연결 끊김 인터럽트는 여기서 끝낸다(Paho 스레드는 재연결을 이어가야 한다). 다른 출처의 인터럽트는 복원한다.
            if (interrupted && !ours) Thread.currentThread().interrupt();
        }
        if (failure != null) {
            // DirectChannel로 Paho까지 던진다 — 연결이 끊기고 재접속 뒤 재전달되는 것을 기대한다
            // (실제 브로커에서의 재전달은 ADR-029 검증 범위 참고).
            throw new IllegalStateException(interrupted
                ? "MQTT 저장 확인 중단 — ACK하지 않음" : "MQTT 저장 확인 실패 — ACK하지 않음", failure);
        }
        acknowledgment.acknowledge();
    }

    /**
     * 메시지를 저장 경로로 넘기고, ACK해도 되는 시점에 완료되는 future를 돌려준다.
     * 거부는 DLQ 발행 성공 뒤 완료(실패하면 이 메서드가 던진다), 정상은 Kafka 또는 spool 기록 뒤 완료.
     */
    private CompletableFuture<Void> process(Message<String> message) {
        String payload = message.getPayload();
        String topic = (String) message.getHeaders().get("mqtt_receivedTopic");

        // **두 입구가 같은 decoder를 쓴다.** 예전에는 여기서만 역직렬화 + Bean Validation을
        // 했고 Kafka 직접 주입은 검증이 없었다 — 같은 payload가 입구에 따라 통과하기도
        // 거부되기도 했다(2026-09-09 감사).
        VehicleTelemetry telemetry;
        try {
            telemetry = telemetryDecoder.decode(payload);
        } catch (TelemetryContractException e) {
            reject(topic, payload, e.getReason(), null);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            // 계약 예외가 아닌 decode 실패도 같은 payload면 매번 같다(decoder는 Jackson·Validator뿐, 외부 의존 없음).
            // ACK 없이 던지면 브로커가 재접속마다 맨 앞에서 재전달해 구독 전체가 막힌다(poison 루프).
            // 격리하고 ACK한다 — DLQ 실패면 ACK하지 않는다. 원인 예외는 로그에 남긴다(DLQ 사유는 한 단어뿐이다).
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

        // 추적 키는 전 구간 같은 이름이다 — vehicle=, ts=(원본 timestamp 문자열). ADR-028.
        log.debug("[MQTT→Kafka] vehicle={} ts={} speed={} engine_temp={} battery_voltage={}",
            telemetry.getVehicleId(),
            telemetry.getTimestamp(),
            telemetry.getSpeed(),
            telemetry.getEngineTemp(),
            telemetry.getBatteryVoltage());

        // 직렬화 실패(Unsendable)는 send() 안에서 **동기적으로** 실패 future가 된다 — 아래 reject()의 DLQ get은
        // 이 콜백 스레드(대기 구간 안)에서 돈다. Kafka I/O 스레드에서 돌면 send().get()이 자기 교착이므로
        // 비동기 실패를 여기서 격리하지 않는다(일시 실패로 남겨 재전달).
        return telemetryProducer.send(telemetry)
            .exceptionallyCompose(e -> {
                Throwable cause = e instanceof java.util.concurrent.CompletionException && e.getCause() != null
                    ? e.getCause() : e;
                if (!(cause instanceof TelemetryProducer.UnsendableTelemetryException)) {
                    return CompletableFuture.failedFuture(cause); // 일시 실패 — ACK하지 않고 재전달을 기다린다
                }
                // 재시도해도 같은 실패 — 격리 뒤 ACK. DLQ 발행 실패는 그대로 실패로 남아 ACK하지 않는다.
                reject(topic, payload, "SERIALIZATION_FAILED", telemetry);
                return CompletableFuture.completedFuture(null);
            });
    }

    private void enterWait() {
        synchronized (waitLock) {
            awaitingThread = Thread.currentThread();
            interruptedByConnectionLoss = false;
        }
    }

    /** 대기 구간을 닫는다. 연결 끊김 인터럽트가 걸렸었으면 true — 남은 플래그도 여기서 지운다. */
    private boolean exitWait() {
        synchronized (waitLock) {
            awaitingThread = null;
            boolean ours = interruptedByConnectionLoss;
            interruptedByConnectionLoss = false;
            // 저장 완료와 경합해 get()이 정상 반환한 뒤 닿은 인터럽트 — 구간 밖으로 새지 않게 지운다.
            if (ours) Thread.interrupted();
            return ours;
        }
    }

    /**
     * 저장 확인을 기다리는 Paho 콜백 스레드를 연결 끊김 때 깨운다.
     *
     * <p>Paho의 재연결(ConnectBG)은 옛 콜백 스레드가 끝날 때까지 기다린다(CommsCallback.start).
     * 콜백 스레드가 {@code receipt.get}에서 최대 150초 막혀 있으면 브로커가 살아 있어도 그동안 재연결이
     * 멈춘다(실험 D, ADR-029). 옛 연결 기준 ACK는 어차피 쓸 수 없으니 기다림을 끝내고 ACK 없이 빠져나온다.
     * 저장 경로는 그대로 진행되므로 브로커의 재전달은 중복이 될 뿐 유실이 아니다.
     * {@code $SYS} 어댑터의 연결 끊김은 텔레메트리 ACK와 무관하다.
     */
    @EventListener
    public void onConnectionLost(MqttConnectionFailedEvent event) {
        // $SYS 어댑터 판별은 토픽 문자열에 기댄다 — 어댑터가 더 생기면 다시 본다.
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
     * @param decoded 계약은 통과했지만 MQTT 고유 검사에서 걸린 경우의 도메인 객체. 계약 위반이면 null.
     *                추적 키(vehicle·ts)를 남기되 <b>신뢰하지 않는다</b> — 거부된 메시지의 필드다.
     */
    private void reject(String topic, String payload, String reason, VehicleTelemetry decoded) {
        // 기존 지표 — MQTT 입구의 **모든** 거부를 센다. 이름·의미를 바꾸지 않는다.
        invalidCounter.increment();
        // 사유별 지표는 **계약 사유일 때만** 올린다. `TOPIC_VEHICLE_MISMATCH`는 MQTT 고유
        // 검사라 계약 사유가 아니다 — 섞으면 세 입구를 나란히 놓을 수 없다.
        if (TelemetryContractException.isContractReason(reason)) {
            com.telemetry.metrics.ContractMetrics.rejected(meterRegistry, com.telemetry.metrics.ContractMetrics.ENTRANCE_MQTT, reason);
        }
        // 거부 경로의 추적 키는 payloadSha256이다 — DLQ value와 대조한다. 값은 남기지 않는다.
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
