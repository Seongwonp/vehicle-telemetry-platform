package com.telemetry.mqtt;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * mosquitto {@code $SYS} 통계를 게이지로 옮긴다. 브로커가 버린 메시지는 앱 지표(받은 것만 센다)로는
 * 보이지 않고 브로커만 안다. 핵심은 {@code publish/messages/dropped} — 늘면 곧 유실이다. 경위: ADR-019
 */
@Slf4j
@Component
public class MqttBrokerMetricsHandler {

    private static final Map<String, String> TRACKED_TOPICS = Map.of(
        "$SYS/broker/publish/messages/dropped", "telemetry.mqtt.broker.messages.dropped",
        // publish/messages/received여야 한다. messages/received는 PUBACK 등 모든 패킷을 세어
        // 백엔드 PUBLISH 수신량과 대조하면 MqttIngestFallingBehind가 항상 울린다.
        "$SYS/broker/publish/messages/received", "telemetry.mqtt.broker.messages.received",
        "$SYS/broker/clients/connected", "telemetry.mqtt.broker.clients.connected"
    );

    /** 갱신 시각용. 추적 토픽은 값이 바뀔 때만 와서 심장 박동이 못 된다. */
    public static final String HEARTBEAT_TOPIC = "$SYS/broker/uptime";

    private final MeterRegistry meterRegistry;
    private final Map<String, AtomicLong> values = new ConcurrentHashMap<>();
    /**
     * 마지막 $SYS 수신 시각(epoch ms). 게이지는 $SYS가 끊겨도 옛 값을 계속 노출하므로 이 시각으로
     * 멈춤을 따로 알린다(MqttBrokerMetricsStale). 첫 수신 때 등록 — 미수신은 MqttBrokerMetricsMissing이 본다.
     */
    private final AtomicLong lastUpdateMillis = new AtomicLong();

    public MqttBrokerMetricsHandler(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @ServiceActivator(inputChannel = "mqttBrokerMetricsChannel")
    public void handle(Message<String> message) {
        String topic = (String) message.getHeaders().get("mqtt_receivedTopic");
        if (HEARTBEAT_TOPIC.equals(topic)) {
            touch();
            return;
        }
        String metricName = TRACKED_TOPICS.get(topic);
        if (metricName == null) {
            return;
        }

        long value;
        try {
            // $SYS 값은 대체로 정수지만 일부 토픽은 소수(이동평균)를 담는다.
            value = (long) Double.parseDouble(message.getPayload().trim());
        } catch (NumberFormatException e) {
            log.warn("[MQTT $SYS] 숫자가 아닌 값 — topic={} payload={}", topic, message.getPayload());
            return;
        }

        // Micrometer 게이지는 참조를 약하게 들고 있으므로 holder를 계속 보관해야 한다.
        values.computeIfAbsent(metricName, name -> {
            AtomicLong holder = new AtomicLong();
            meterRegistry.gauge(name, holder, AtomicLong::get);
            return holder;
        }).set(value);
        touch();
    }

    private void touch() {
        if (lastUpdateMillis.getAndSet(System.currentTimeMillis()) == 0) {
            meterRegistry.gauge("telemetry.mqtt.broker.last.update.seconds", lastUpdateMillis, v -> v.get() / 1000.0);
        }
    }
}
