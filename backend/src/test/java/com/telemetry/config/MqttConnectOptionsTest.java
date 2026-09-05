package com.telemetry.config;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MQTT 연결 옵션 회귀 테스트.
 *
 * <p>여기 있는 값들은 <b>정상 동작 중에는 아무 차이도 만들지 않는다.</b> 브로커가
 * 죽었다 살아나는 순간에만 효과가 드러나서, 잘못 바뀌어도 일반 테스트로는 안 잡힌다.
 * 실제로 재연결 백오프 상한이 Paho 기본값(128초)인 채로 오래 있었고, 브로커 90초
 * 장애 실험에서 그 틈에 <b>129,447건이 브로커에서 버려지는 것</b>을 보고 나서야 찾았다
 * ({@code load-test/fault-injection/RESULT_20260905_mqtt_broker.md}).
 */
@DisplayName("MQTT 연결 옵션")
class MqttConnectOptionsTest {

    private MqttConnectOptions optionsWith(int maxReconnectDelayMs) {
        MqttConfig config = new MqttConfig();
        ReflectionTestUtils.setField(config, "host", "localhost");
        ReflectionTestUtils.setField(config, "port", 1883);
        ReflectionTestUtils.setField(config, "clientId", "telemetry-backend");
        ReflectionTestUtils.setField(config, "tlsEnabled", false);
        ReflectionTestUtils.setField(config, "maxReconnectDelayMs", maxReconnectDelayMs);

        DefaultMqttPahoClientFactory factory =
            (DefaultMqttPahoClientFactory) config.mqttClientFactory();
        return factory.getConnectionOptions();
    }

    @Test
    @DisplayName("재연결 백오프 상한을 명시한다 — Paho 기본값 128초는 유실 구간이 된다")
    void 재연결_백오프_상한_명시() {
        MqttConnectOptions options = optionsWith(5000);

        assertThat(options.isAutomaticReconnect()).isTrue();
        // 이 값이 크면 브로커 복구 후에도 한참 붙지 않고, 그동안 브로커가 우리 세션
        // 앞으로 오는 메시지를 max_queued_messages까지만 담고 나머지를 버린다.
        assertThat(options.getMaxReconnectDelay()).isEqualTo(5000);
    }

    @Test
    @DisplayName("cleanSession=false를 유지한다 — 끊긴 동안 브로커가 대신 큐잉해준다")
    void 세션_유지() {
        MqttConnectOptions options = optionsWith(5000);

        // true로 바꾸면 재연결할 때마다 구독이 초기화되고, 끊긴 동안 브로커가
        // 쌓아둔 메시지도 통째로 사라진다. 재연결 백오프 상한과 짝을 이루는 설정이다.
        assertThat(options.isCleanSession()).isFalse();
    }

    @Test
    @DisplayName("설정값이 그대로 반영된다 — 환경변수로 조정 가능해야 한다")
    void 설정값_반영() {
        assertThat(optionsWith(2000).getMaxReconnectDelay()).isEqualTo(2000);
    }
}
