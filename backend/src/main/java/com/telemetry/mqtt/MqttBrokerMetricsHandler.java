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
 * mosquitto가 주기적으로 발행하는 {@code $SYS} 통계를 Prometheus 게이지로 옮긴다.
 *
 * <p><b>왜 필요한가</b>: 브로커가 구독자에게 못 넘긴 메시지를 버리면 백엔드는 그 사실을
 * 알 방법이 없다. 애플리케이션 지표는 <i>받은 것</i>만 셀 수 있기 때문이다. 실제로
 * 수집 경로가 초당 20건으로 막혀 있던 동안 시뮬레이터는 초당 약 10,000건을 발행했는데,
 * 백엔드의 MQTT 수신 카운터도 Kafka 발행 수도 똑같이 20건이라 서로 일치했고,
 * Kafka lag은 들어온 게 없으니 정상으로 보였다 — 어떤 애플리케이션 지표로도 안 잡혔다.
 * 유일하게 이 유실을 아는 건 브로커 자신이라, 브로커의 통계를 직접 가져온다.
 *
 * <p>핵심 지표는 {@code $SYS/broker/publish/messages/dropped}로, 큐/인플라이트 한도를
 * 넘겨 브로커가 버린 메시지 수다. 이 값이 늘어난다는 건 곧 데이터 유실이다.
 */
@Slf4j
@Component
public class MqttBrokerMetricsHandler {

    /** 구독한 `$SYS` 토픽 → Micrometer 게이지 이름. 여기 없는 토픽은 무시한다. */
    private static final Map<String, String> TRACKED_TOPICS = Map.of(
        "$SYS/broker/publish/messages/dropped", "telemetry.mqtt.broker.messages.dropped",
        // publish/messages/received여야 한다. $SYS/broker/messages/received는 PUBLISH뿐 아니라
        // PUBACK·PINGREQ·SUBSCRIBE 등 **모든 MQTT 패킷 타입**을 센다 — 실측하면 정상 부하에서
        // 정확히 2배가 나온다(139,009 vs 69,242). 그 값을 백엔드의 PUBLISH 수신량과 빼면
        // 구조적으로 항상 큰 양수가 나와, MqttIngestFallingBehind 알림이 아무 문제가 없어도
        // 계속 울린다(파이프라인 단계 대조 대시보드를 만들다 발견했다).
        "$SYS/broker/publish/messages/received", "telemetry.mqtt.broker.messages.received",
        "$SYS/broker/clients/connected", "telemetry.mqtt.broker.clients.connected"
    );

    /** 갱신 시각만 움직이는 토픽 — 게이지로 내보내지 않는다. 추적 토픽은 값이 바뀔 때만 와서 심장 박동이 못 된다. */
    public static final String HEARTBEAT_TOPIC = "$SYS/broker/uptime";

    private final MeterRegistry meterRegistry;
    private final Map<String, AtomicLong> values = new ConcurrentHashMap<>();
    /**
     * 마지막으로 $SYS(추적 토픽 또는 uptime 심장 박동)를 받은 시각(epoch ms). 위 게이지들은 한 번 등록되면 지워지지 않아,
     * $SYS가 끊겨도 마지막 값이 그대로 노출된다 — 그 상태에서 MqttIngestStopped는 옛 값의 rate(=0)를 보고
     * 조용해진다. 이 시각으로 "값이 멈췄다"를 따로 알린다(MqttBrokerMetricsStale). 첫 수신 때 등록한다 —
     * 한 번도 안 왔으면 게이지가 없고, 그 경우는 MqttBrokerMetricsMissing이 본다.
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

        // 게이지는 최초 1회만 등록하고 이후에는 같은 AtomicLong을 갱신한다
        // (Micrometer 게이지는 참조를 약하게 들고 있으므로 인스턴스를 계속 보관해야 한다).
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
