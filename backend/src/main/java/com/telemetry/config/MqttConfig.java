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
import java.util.UUID;

@Configuration
public class MqttConfig {

    @Value("${mqtt.host}")
    private String host;

    @Value("${mqtt.port}")
    private int port;

    @Value("${mqtt.topic}")
    private String topic;

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

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();

        String scheme = tlsEnabled ? "ssl" : "tcp";
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{scheme + "://" + host + ":" + port});
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(60);
        // 브로커 재시작이나 네트워크 단절 시 자동 재연결 — 수동 복구 없이 파이프라인 유지
        options.setAutomaticReconnect(true);

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
        // 재시작마다 다른 clientId를 쓴다. 같은 ID로 재접속하면 브로커가 이전 세션을
        // 복원하려 하는데, cleanSession=true와 조합하면 예측 불가한 동작이 생긴다.
        String clientId = "backend-" + UUID.randomUUID().toString().substring(0, 8);

        MqttPahoMessageDrivenChannelAdapter adapter =
            new MqttPahoMessageDrivenChannelAdapter(clientId, mqttClientFactory(), topic);

        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        // QoS 1: 최소 1회 전달 보장. QoS 0은 유실 가능, QoS 2는 핸드셰이크 2배로 처리량 감소
        adapter.setQos(1);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }
}
