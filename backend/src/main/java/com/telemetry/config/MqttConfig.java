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

import java.util.UUID;

@Configuration
public class MqttConfig {

    @Value("${mqtt.host}")
    private String host;

    @Value("${mqtt.port}")
    private int port;

    @Value("${mqtt.topic}")
    private String topic;

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();

        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{"tcp://" + host + ":" + port});
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        options.setKeepAliveInterval(60);
        options.setAutomaticReconnect(true);

        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MqttPahoMessageDrivenChannelAdapter mqttInbound() {
        // 클라이언트 ID는 재시작마다 고유하게 (충돌 방지)
        String clientId = "backend-" + UUID.randomUUID().toString().substring(0, 8);

        MqttPahoMessageDrivenChannelAdapter adapter =
            new MqttPahoMessageDrivenChannelAdapter(clientId, mqttClientFactory(), topic);

        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(1);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }
}
