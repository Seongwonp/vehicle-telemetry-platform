package com.telemetry.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.kafka.TelemetryProducer;
import com.telemetry.kafka.TelemetrySpool;
import com.telemetry.mqtt.MqttInvalidMessagePublisher;
import com.telemetry.mqtt.MqttMessageHandler;
import com.telemetry.support.TestDecoders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** Separate JVM killed by the parent. Kafka completion is deliberately held, not a real Kafka outage. */
public class MqttCrashWorker {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Path state = Path.of(args[2]);
        var registry = new SimpleMeterRegistry();
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            Files.writeString(state.resolve("forwarded.txt"), invocation.getArgument(2, String.class));
            return new CompletableFuture<>();
        });
        var handler = new MqttMessageHandler(new TelemetryProducer(kafka, new ObjectMapper(),
            new TelemetrySpool(state.resolve("spool").toString()), registry, 100),
            TestDecoders.telemetryDecoder(), registry, new MqttInvalidMessagePublisher(kafka, new ObjectMapper(), registry));
        var options = new MqttConnectOptions();
        options.setServerURIs(new String[]{args[0]});
        options.setCleanSession(false);
        var factory = new DefaultMqttPahoClientFactory();
        factory.setConnectionOptions(options);
        var adapter = new MqttPahoMessageDrivenChannelAdapter(args[1], factory, "vehicle/telemetry/CRASH-001");
        adapter.setQos(1);
        adapter.setManualAcks(Boolean.parseBoolean(args[3]));
        adapter.setBeanFactory(new DefaultListableBeanFactory());
        var channel = new DirectChannel();
        channel.subscribe(message -> handler.handle((Message<String>) message));
        adapter.setOutputChannel(channel);
        adapter.afterPropertiesSet();
        adapter.start();
        Files.writeString(state.resolve("ready.txt"), "ready");
        Thread.currentThread().join();
    }
}
