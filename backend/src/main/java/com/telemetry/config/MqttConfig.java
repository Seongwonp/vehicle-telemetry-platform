package com.telemetry.config;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.MessageChannel;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

@Configuration
public class MqttConfig {

    @Value("${mqtt.host}")
    private String host;

    @Value("${mqtt.port}")
    private int port;

    @Value("${mqtt.topic}")
    private String topic;

    @Value("${mqtt.client-id:telemetry-backend}")
    private String clientId;

    // 평소 로컬 개발 사이클은 인증서 없이 평문(1883)으로 돌리고,
    // 데모/보안 검증 때만 이 플래그를 켜서 mTLS(8883)로 전환한다.
    // broker/certs/generate-certs.sh 실행 후 mosquitto.conf의 TLS 섹션 주석을 해제해야 실제로 연결된다.
    @Value("${mqtt.tls.enabled:false}")
    private boolean tlsEnabled;

    @Value("${mqtt.tls.keystore-path:}")
    private String keystorePath;

    @Value("${mqtt.tls.truststore-path:}")
    private String truststorePath;

    @Value("${mqtt.tls.store-password:changeit}")
    private String storePassword;

    @Value("${mqtt.max-reconnect-delay-ms:5000}")
    private int maxReconnectDelayMs;

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();

        String scheme = tlsEnabled ? "ssl" : "tcp";
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{scheme + "://" + host + ":" + port});
        options.setCleanSession(false);
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(60);
        // 브로커 재시작이나 네트워크 단절 시 자동 재연결 — 수동 복구 없이 파이프라인 유지
        options.setAutomaticReconnect(true);
        // 재연결 백오프의 상한을 반드시 낮춰야 한다. Paho는 1초에서 시작해 매번 두 배로
        // 늘리고 기본 상한이 **128초**인데, 그 시간은 그냥 기다리는 시간이 아니다 —
        // cleanSession=false라 브로커가 우리 세션 앞으로 오는 메시지를 대신 쌓아두다가
        // max_queued_messages(10,000)를 넘기면 **말없이 버린다.**
        //
        // 브로커를 90초 정지시켰다 살리는 실험에서, 브로커가 살아난 뒤에도 백오프가
        // 이미 커져 있어 백엔드가 한참 뒤에야 붙었다. 그 틈에 시뮬레이터는 밀렸던 것을
        // 한꺼번에 쏟아냈고, 브로커는 10,000건만 큐에 담고 **129,447건을 버렸다.**
        // 백엔드가 재연결 후 실제로 받은 건 10,002건 — 큐 크기와 정확히 일치한다.
        // 유실을 아는 유일한 지표가 브로커의 $SYS dropped였다
        // (load-test/fault-injection/RESULT_20260905_mqtt_broker.md).
        //
        // 상한을 낮추면 브로커가 죽어 있는 동안 재연결 시도가 잦아지지만, 5초 간격은
        // 초당 0.2회라 브로커 부하로는 무시할 수준이다. 유실 위험과 바꿀 것이 못 된다.
        options.setMaxReconnectDelay(maxReconnectDelayMs);

        if (tlsEnabled) {
            options.setSocketFactory(buildSslSocketFactory());
        }

        factory.setConnectionOptions(options);
        return factory;
    }

    /**
     * client.p12(클라이언트 인증서+키)와 truststore.p12(CA 인증서)로 mTLS 소켓 팩토리를 만든다.
     * openssl이 만드는 PEM/PKCS#1 키는 Java가 직접 못 읽기 때문에, generate-certs.sh가
     * PKCS12로 미리 변환해둔 파일을 표준 javax.net.ssl API로 로드한다.
     */
    private SSLSocketFactory buildSslSocketFactory() {
        try {
            char[] password = storePassword.toCharArray();

            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(keystorePath)) {
                keyStore.load(fis, password);
            }
            KeyManagerFactory keyManagerFactory =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, password);

            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(truststorePath)) {
                trustStore.load(fis, password);
            }
            TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
            return sslContext.getSocketFactory();

        } catch (Exception e) {
            throw new IllegalStateException(
                "MQTT TLS 소켓 팩토리 초기화 실패 — mqtt.tls.keystore-path/truststore-path 설정을 확인하세요", e);
        }
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        // DirectChannel: 발행 스레드가 곧 소비 스레드 — 별도 스레드풀 없이 낮은 지연으로 메시지 전달
        return new DirectChannel();
    }

    @Bean
    public MqttPahoMessageDrivenChannelAdapter mqttInbound() {
        MqttPahoMessageDrivenChannelAdapter adapter =
            new MqttPahoMessageDrivenChannelAdapter(clientId, mqttClientFactory(), topic);

        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        // QoS 1: 최소 1회 전달 보장. QoS 0은 유실 가능, QoS 2는 핸드셰이크 2배로 처리량 감소
        adapter.setQos(1);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    public MessageChannel mqttBrokerMetricsChannel() {
        return new DirectChannel();
    }

    /**
     * 브로커 자체 통계(`$SYS`) 구독 — 텔레메트리 경로와 완전히 분리한다.
     *
     * <p>같은 어댑터에 `$SYS` 토픽을 얹으면 `MqttMessageHandler`가 이 메시지들을 받아
     * 차량 토픽 패턴에 안 맞는다며 전부 거부 카운터와 DLQ로 보낸다. 별도 어댑터·채널로
     * 분리해 그런 오염을 막는다.
     *
     * <p>QoS 0인 이유: mosquitto는 이 값들을 기본 10초마다 최신값으로 다시 발행한다.
     * 한 번 놓쳐도 다음 주기에 정확한 누적값이 오므로 재전송 보장이 필요 없다.
     */
    @Bean
    public MqttPahoMessageDrivenChannelAdapter mqttBrokerMetricsInbound() {
        MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter(
            clientId + "-sys", mqttClientFactory(),
            "$SYS/broker/publish/messages/dropped",
            "$SYS/broker/publish/messages/received",
            "$SYS/broker/clients/connected");

        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(0);
        adapter.setOutputChannel(mqttBrokerMetricsChannel());
        return adapter;
    }
}
