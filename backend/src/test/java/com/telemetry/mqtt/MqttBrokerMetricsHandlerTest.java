package com.telemetry.mqtt;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MqttBrokerMetricsHandler 단위 테스트")
class MqttBrokerMetricsHandlerTest {

    private SimpleMeterRegistry meterRegistry;
    private MqttBrokerMetricsHandler handler;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        handler = new MqttBrokerMetricsHandler(meterRegistry);
    }

    private Message<String> sysMessage(String topic, String payload) {
        return MessageBuilder.withPayload(payload)
            .setHeader("mqtt_receivedTopic", topic)
            .build();
    }

    @Test
    @DisplayName("버려진 메시지 수를 게이지로 노출한다 — 애플리케이션 지표로는 못 잡는 유실 신호")
    void 버려진메시지_게이지노출() {
        handler.handle(sysMessage("$SYS/broker/publish/messages/dropped", "42"));

        assertThat(meterRegistry.get("telemetry.mqtt.broker.messages.dropped").gauge().value())
            .isEqualTo(42.0);
    }

    @Test
    @DisplayName("같은 토픽이 다시 오면 게이지를 갱신한다 (10초마다 재발행되므로)")
    void 반복수신_게이지갱신() {
        handler.handle(sysMessage("$SYS/broker/publish/messages/received", "100"));
        handler.handle(sysMessage("$SYS/broker/publish/messages/received", "250"));

        assertThat(meterRegistry.get("telemetry.mqtt.broker.messages.received").gauge().value())
            .isEqualTo(250.0);
        // 게이지가 중복 등록되지 않아야 한다 — 등록될 때마다 새로 만들면 값이 갈린다.
        assertThat(meterRegistry.find("telemetry.mqtt.broker.messages.received").gauges()).hasSize(1);
    }

    @Test
    @DisplayName("소수 값도 받아들인다 — 일부 $SYS 토픽은 이동평균을 담는다")
    void 소수값_처리() {
        handler.handle(sysMessage("$SYS/broker/clients/connected", "3.0"));

        assertThat(meterRegistry.get("telemetry.mqtt.broker.clients.connected").gauge().value())
            .isEqualTo(3.0);
    }

    @Test
    @DisplayName("추적 대상이 아닌 $SYS 토픽은 무시한다")
    void 미추적토픽_무시() {
        handler.handle(sysMessage("$SYS/broker/uptime", "1234 seconds"));

        assertThat(meterRegistry.getMeters()).isEmpty();
    }

    @Test
    @DisplayName("숫자가 아닌 값은 게이지를 만들지 않고 넘어간다")
    void 비숫자값_무시() {
        handler.handle(sysMessage("$SYS/broker/publish/messages/received", "N/A"));

        assertThat(meterRegistry.find("telemetry.mqtt.broker.messages.received").gauge()).isNull();
    }
}
