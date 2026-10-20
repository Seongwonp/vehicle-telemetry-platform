package com.telemetry.config;

import com.telemetry.mqtt.MqttBrokerMetricsHandler;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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

    /** MQTT keepAlive(초). producer 실패 확정 시간은 이보다 짧아야 한다(KafkaProducerContractTest). */
    public static final int KEEP_ALIVE_SECONDS = 60;

    @Value("${mqtt.host}")
    private String host;

    @Value("${mqtt.port}")
    private int port;

    @Value("${mqtt.topic}")
    private String topic;

    @Value("${mqtt.client-id:telemetry-backend}")
    private String clientId;

    // 로컬 개발은 평문(1883), 켜면 mTLS(8883). broker/certs/generate-certs.sh로 인증서를 먼저 만든다.
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
        options.setKeepAliveInterval(KEEP_ALIVE_SECONDS);
        options.setAutomaticReconnect(true);
        // 재연결 상한을 낮게 유지할 것. Paho 기본 상한(128초) 동안 브로커는 세션 큐가 넘친
        // 메시지를 말없이 버린다(cleanSession=false). 이유: ADR-021
        options.setMaxReconnectDelay(maxReconnectDelayMs);

        if (tlsEnabled) {
            options.setSocketFactory(buildSslSocketFactory());
        }

        factory.setConnectionOptions(options);
        return factory;
    }

    /**
     * client.p12(클라이언트 인증서+키)와 truststore.p12(CA)로 mTLS 소켓 팩토리를 만든다.
     * Java가 PEM 키를 직접 못 읽어 generate-certs.sh가 PKCS12로 변환해 둔다.
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
        return new DirectChannel();
    }

    /**
     * MQTT 수집 어댑터. 인스턴스마다 켜고 끌 수 있다(수집 1대 + 저장 N대).
     * client-id가 고정이라 수집 인스턴스가 둘이면 서로의 세션을 끊는다. 이유: ADR-023
     * {@code $SYS} 구독도 같이 꺼진다.
     */
    @Bean
    @ConditionalOnProperty(name = "mqtt.ingest.enabled", havingValue = "true", matchIfMissing = true)
    public MqttPahoMessageDrivenChannelAdapter mqttInbound() {
        MqttPahoMessageDrivenChannelAdapter adapter =
            new MqttPahoMessageDrivenChannelAdapter(clientId, mqttClientFactory(), topic);

        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        // QoS 1: 최소 1회 전달 보장. QoS 0은 유실 가능, QoS 2는 핸드셰이크 2배로 처리량 감소
        adapter.setQos(1);
        adapter.setManualAcks(true);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    public MessageChannel mqttBrokerMetricsChannel() {
        return new DirectChannel();
    }

    /**
     * 브로커 통계(`$SYS`) 구독. 텔레메트리 어댑터와 섞으면 MqttMessageHandler가
     * 거부/DLQ로 보내므로 별도 어댑터·채널로 둔다.
     * QoS 0: 다음 주기에 누적값이 다시 오므로 재전송이 필요 없다.
     */
    @Bean
    @ConditionalOnProperty(name = "mqtt.ingest.enabled", havingValue = "true", matchIfMissing = true)
    public MqttPahoMessageDrivenChannelAdapter mqttBrokerMetricsInbound() {
        MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter(
            clientId + "-sys", mqttClientFactory(),
            "$SYS/broker/publish/messages/dropped",
            "$SYS/broker/publish/messages/received",
            "$SYS/broker/clients/connected",
            // 위 셋은 값이 바뀔 때만 와서 유휴 시 stale 알림이 거짓으로 뜬다. uptime을 심장 박동으로 쓴다.
            MqttBrokerMetricsHandler.HEARTBEAT_TOPIC);

        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(0);
        adapter.setOutputChannel(mqttBrokerMetricsChannel());
        return adapter;
    }
}
