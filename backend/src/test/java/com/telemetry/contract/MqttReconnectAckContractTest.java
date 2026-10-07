package com.telemetry.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.kafka.TelemetryProducer;
import com.telemetry.kafka.TelemetrySpool;
import com.telemetry.mqtt.MqttInvalidMessagePublisher;
import com.telemetry.mqtt.MqttMessageHandler;
import com.telemetry.support.TestDecoders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 저장 확인을 기다리는 동안 MQTT TCP 연결이 끊겼다 이어지는 경계(실험 D, ADR-029).
 *
 * <p>실제 Mosquitto와 실제 Spring Integration 어댑터·MqttMessageHandler를 쓰고, 연결 단절은
 * 테스트 안의 TCP 중계기를 닫아 만든다. Kafka 전송 완료만 통제한 future다 — 실제 Kafka 장애가 아니다.
 */
// Docker image builders have no daemon; the host CI job separately enforces zero skipped contracts.
@Testcontainers(disabledWithoutDocker = true)
class MqttReconnectAckContractTest {
    @Container static final GenericContainer<?> BROKER = new GenericContainer<>("eclipse-mosquitto:2.0")
        .withExposedPorts(1883).withCopyToContainer(Transferable.of(
            "listener 1883\nallow_anonymous true\nlog_type all\nmax_inflight_messages 20\n"),
            "/mosquitto/config/mosquitto.conf").waitingFor(Wait.forListeningPort());
    @TempDir Path temporary;

    /** client와 broker 사이에 끼는 단순 TCP 중계기. drop()은 현재 연결만 끊고 새 접속은 계속 받는다. */
    static final class Proxy implements AutoCloseable {
        final ServerSocket server;
        final String upstreamHost;
        final int upstreamPort;
        final List<Socket> sockets = new CopyOnWriteArrayList<>();
        volatile boolean open = true;

        Proxy(String host, int port) throws IOException {
            server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            upstreamHost = host;
            upstreamPort = port;
            Thread acceptor = new Thread(this::acceptLoop, "mqtt-proxy-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        int port() { return server.getLocalPort(); }

        private void acceptLoop() {
            while (open) {
                try {
                    Socket client = server.accept();
                    Socket upstream = new Socket(upstreamHost, upstreamPort);
                    client.setTcpNoDelay(true);
                    upstream.setTcpNoDelay(true);
                    sockets.add(client);
                    sockets.add(upstream);
                    pump(client, upstream);
                    pump(upstream, client);
                } catch (IOException e) {
                    if (!open) return;
                }
            }
        }

        private void pump(Socket from, Socket to) {
            Thread t = new Thread(() -> {
                byte[] buffer = new byte[8192];
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    int n;
                    while ((n = in.read(buffer)) >= 0) { out.write(buffer, 0, n); out.flush(); }
                } catch (IOException ignored) {
                } finally { closeQuietly(from); closeQuietly(to); }
            }, "mqtt-proxy-pump");
            t.setDaemon(true);
            t.start();
        }

        void drop() { sockets.forEach(Proxy::closeQuietly); sockets.clear(); }

        static void closeQuietly(Socket s) { try { s.close(); } catch (IOException ignored) { } }

        @Override public void close() {
            open = false;
            drop();
            try { server.close(); } catch (IOException ignored) { }
        }
    }

    private static String payload(int sequence) {
        return """
            {"vehicle_id":"RECON-001","timestamp":"2026-10-01T00:00:00.%03dZ",
             "speed":10,"rpm":900,"engine_temp":85,"throttle_position":12,"fuel_level":55,
             "battery_voltage":13.8,"gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}
            """.formatted(sequence);
    }

    private static int sequenceOf(String json) {
        int at = json.indexOf("00:00:00.") + "00:00:00.".length();
        return Integer.parseInt(json.substring(at, at + 3));
    }

    @SuppressWarnings("unchecked")
    private static SendResult<String, String> sendResult() {
        var result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(
            new org.apache.kafka.clients.producer.RecordMetadata(
                new org.apache.kafka.common.TopicPartition("vehicle-telemetry", 0), 0, 0, 0L, 0, 0));
        return result;
    }

    private static long ms(long t0) { return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0); }

    @Test @SuppressWarnings("unchecked")
    void ackPendingAcrossReconnectDoesNotLoseOrBlockMessages() throws Exception {
        String brokerHost = BROKER.getHost();
        int brokerPort = BROKER.getMappedPort(1883);
        String clientId = "reconnect-" + UUID.randomUUID();
        List<String> timeline = Collections.synchronizedList(new ArrayList<>());
        long t0 = System.nanoTime();
        // sequence -> 그 시퀀스로 만들어진 모든 전송 future (재전달되면 둘 이상)
        Map<Integer, List<CompletableFuture<SendResult<String, String>>>> sends = new ConcurrentHashMap<>();
        AtomicBoolean autoComplete = new AtomicBoolean(false);

        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            int sequence = sequenceOf(invocation.getArgument(2, String.class));
            var future = new CompletableFuture<SendResult<String, String>>();
            var list = sends.computeIfAbsent(sequence, k -> new CopyOnWriteArrayList<>());
            list.add(future);
            timeline.add("%5dms send seq=%d attempt=%d thread=%s".formatted(
                ms(t0), sequence, list.size(), Thread.currentThread().getName()));
            if (autoComplete.get()) future.complete(sendResult());
            return future;
        });
        var registry = new SimpleMeterRegistry();
        var handler = new MqttMessageHandler(new TelemetryProducer(kafka, new ObjectMapper(),
            new TelemetrySpool(temporary.resolve("spool").toString()), registry, 100),
            TestDecoders.telemetryDecoder(), registry,
            new MqttInvalidMessagePublisher(kafka, new ObjectMapper(), registry));

        try (var proxy = new Proxy(brokerHost, brokerPort)) {
            var options = new MqttConnectOptions();
            options.setServerURIs(new String[]{"tcp://127.0.0.1:" + proxy.port()});
            options.setCleanSession(false);
            options.setAutomaticReconnect(true);
            options.setMaxReconnectDelay(1000);
            options.setKeepAliveInterval(60);
            var factory = new DefaultMqttPahoClientFactory();
            factory.setConnectionOptions(options);
            var adapter = new MqttPahoMessageDrivenChannelAdapter(clientId, factory, "vehicle/telemetry/RECON-001");
            adapter.setBeanFactory(new DefaultListableBeanFactory());
            adapter.setQos(1);
            adapter.setManualAcks(true);
            var channel = new DirectChannel();
            channel.subscribe(message -> {
                try {
                    handler.handle((Message<String>) message);
                } catch (RuntimeException e) {
                    timeline.add("%5dms handler threw %s".formatted(ms(t0), e.getMessage()));
                    throw e;
                }
            });
            // 운영에서는 Spring 컨텍스트가 연결 끊김 이벤트를 @EventListener로 전달한다.
            adapter.setApplicationEventPublisher(event -> {
                if (event instanceof org.springframework.integration.mqtt.event.MqttConnectionFailedEvent failed) {
                    timeline.add("%5dms MqttConnectionFailedEvent".formatted(ms(t0)));
                    handler.onConnectionLost(failed);
                }
            });
            adapter.setOutputChannel(channel);
            adapter.afterPropertiesSet();
            adapter.start();
            await().atMost(Duration.ofSeconds(15))
                .until(() -> BROKER.getLogs().contains("Received SUBSCRIBE from " + clientId));

            try (var publisher = new MqttClient("tcp://" + brokerHost + ":" + brokerPort,
                "pub-" + UUID.randomUUID(), new MemoryPersistence())) {
                publisher.connect();
                // seq 1: 저장 확인 대기 중에 연결이 끊긴다. seq 2·3: 이미 도착했거나 브로커에 남아 있다.
                for (int seq = 1; seq <= 3; seq++) {
                    publisher.publish("vehicle/telemetry/RECON-001", payload(seq).getBytes(StandardCharsets.UTF_8), 1, false);
                }
                await().atMost(Duration.ofSeconds(10)).until(() -> sends.containsKey(1));
                int connectsBefore = count(BROKER.getLogs(), " as " + clientId + " (");
                proxy.drop();
                timeline.add("%5dms connection dropped (seq 1 receipt pending)".formatted(ms(t0)));
                try {
                    // 저장 확인 대기(최대 150초)가 재연결을 막지 않아야 한다. 수정 전에는 여기서 시간 초과.
                    await().atMost(Duration.ofSeconds(30)).until(
                        () -> count(BROKER.getLogs(), " as " + clientId + " (") > connectsBefore);
                } catch (RuntimeException e) {
                    timeline.forEach(System.out::println);
                    System.out.println(BROKER.getLogs());
                    throw e;
                }
                timeline.add("%5dms broker saw reconnect".formatted(ms(t0)));

                // 재접속 뒤에 옛 연결 기준 저장이 완료된다 — 이 시점의 ACK가 새 연결에서 무엇이 되는지가 관찰 대상이다.
                autoComplete.set(true);
                sends.values().forEach(list -> list.forEach(f -> f.complete(sendResult())));
                timeline.add("%5dms pending receipts completed".formatted(ms(t0)));

                // (b) 유실 불가: 세 메시지 모두 최소 한 번 이상 저장 경로에 도달해야 한다. 중복은 허용.
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(sends.keySet()).containsExactlyInAnyOrder(1, 2, 3));
                Thread.sleep(3000);
                sends.values().forEach(list -> list.forEach(f -> f.complete(sendResult())));
                Thread.sleep(2000);
                publisher.disconnect();
            }
            adapter.stop();
            adapter.destroy();
            // (c) 저장 확인된 메시지가 모두 ACK됐다면 같은 client ID로 다시 붙어도 재전달이 없다.
            var redelivered = new LinkedBlockingQueue<Integer>();
            try (var resumed = new MqttClient("tcp://" + brokerHost + ":" + brokerPort, clientId, new MemoryPersistence())) {
                resumed.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        redelivered.add(sequenceOf(new String(message.getPayload(), StandardCharsets.UTF_8)));
                    }
                });
                var resumeOptions = new MqttConnectOptions();
                resumeOptions.setCleanSession(false);
                resumed.connect(resumeOptions);
                Thread.sleep(3000);
                resumed.disconnect();
            }
            timeline.forEach(System.out::println);
            sends.forEach((seq, list) -> System.out.printf("RECONNECT_BOUNDARY seq=%d sendAttempts=%d%n", seq, list.size()));
            System.out.println("RECONNECT_BOUNDARY redeliveredAfterResume=" + redelivered);
            assertThat(sends.keySet()).containsExactlyInAnyOrder(1, 2, 3);
            assertThat(redelivered).as("저장 확인된 메시지는 모두 ACK되어 재전달되지 않아야 한다").isEmpty();
        }
    }

    /**
     * 실험 D3 결함 후보의 재현: 백로그(spool 직행) 경로에서 콜백 스레드가 spool에 쓰는 중에 연결 끊김 인터럽트가 닿는다.
     * 기대(계약): 그 메시지는 영속되지 않았으므로 PUBACK이 나가지 않고, 재접속 뒤 브로커가 재전달해 spool에 정확히 1건 남는다.
     * 인터럽트가 쓰기 구간에 닿도록, 첫 store()가 중계기를 끊고 인터럽트 플래그가 설 때까지 기다린 뒤 실제 쓰기를 한다.
     */
    @Test @SuppressWarnings("unchecked")
    void interruptDuringSpoolWriteIsNotAckedThenRedeliveredAndSpooledOnce() throws Exception {
        String brokerHost = BROKER.getHost();
        int brokerPort = BROKER.getMappedPort(1883);
        String clientId = "spoolint-" + UUID.randomUUID();
        List<String> timeline = Collections.synchronizedList(new ArrayList<>());
        long t0 = System.nanoTime();
        Path spoolDir = temporary.resolve("spool-interrupt");
        AtomicBoolean firstStore = new AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicReference<Throwable> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Runnable> dropConnection = new java.util.concurrent.atomic.AtomicReference<>();
        List<Boolean> deliveryDuplicateFlags = new CopyOnWriteArrayList<>();

        var spool = new TelemetrySpool(spoolDir.toString()) {
            @Override public Path store(String json) {
                if (firstStore.getAndSet(false)) {
                    dropConnection.get().run();
                    timeline.add("%5dms first store: connection dropped, waiting for interrupt".formatted(ms(t0)));
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    // parkNanos는 인터럽트 플래그를 지우지 않는다 — 플래그를 단 채로 실제 쓰기에 들어간다.
                    while (!Thread.currentThread().isInterrupted() && System.nanoTime() < deadline) {
                        java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                    }
                    timeline.add("%5dms first store: interrupted=%s".formatted(ms(t0), Thread.currentThread().isInterrupted()));
                    try {
                        return super.store(json);
                    } catch (RuntimeException e) {
                        firstFailure.set(e.getCause());
                        timeline.add("%5dms first store failed: %s".formatted(ms(t0), e.getCause()));
                        throw e;
                    }
                }
                Path stored = super.store(json);
                timeline.add("%5dms store ok seq=%d".formatted(ms(t0), sequenceOf(json)));
                return stored;
            }
        };
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        // Kafka가 멈춘 상태를 동기 실패로 흉내 낸다 — 모든 정상 메시지가 콜백 스레드에서 spool로 간다(D3의 백로그 경로).
        when(kafka.send(anyString(), anyString(), anyString()))
            .thenThrow(new org.apache.kafka.common.errors.TimeoutException("kafka paused (test)"));
        var registry = new SimpleMeterRegistry();
        var handler = new MqttMessageHandler(new TelemetryProducer(kafka, new ObjectMapper(), spool, registry, 100),
            TestDecoders.telemetryDecoder(), registry, new MqttInvalidMessagePublisher(kafka, new ObjectMapper(), registry));

        try (var proxy = new Proxy(brokerHost, brokerPort)) {
            dropConnection.set(proxy::drop);
            var options = new MqttConnectOptions();
            options.setServerURIs(new String[]{"tcp://127.0.0.1:" + proxy.port()});
            options.setCleanSession(false);
            options.setAutomaticReconnect(true);
            options.setMaxReconnectDelay(1000);
            options.setKeepAliveInterval(60);
            var factory = new DefaultMqttPahoClientFactory();
            factory.setConnectionOptions(options);
            var adapter = new MqttPahoMessageDrivenChannelAdapter(clientId, factory, "vehicle/telemetry/RECON-001");
            adapter.setBeanFactory(new DefaultListableBeanFactory());
            adapter.setQos(1);
            adapter.setManualAcks(true);
            var channel = new DirectChannel();
            channel.subscribe(message -> {
                Boolean dup = message.getHeaders().get("mqtt_duplicate", Boolean.class);
                deliveryDuplicateFlags.add(dup);
                timeline.add("%5dms delivered seq=%d dup=%s".formatted(ms(t0), sequenceOf((String) message.getPayload()), dup));
                try {
                    handler.handle((Message<String>) message);
                    timeline.add("%5dms handler acked".formatted(ms(t0)));
                } catch (RuntimeException e) {
                    timeline.add("%5dms handler threw %s".formatted(ms(t0), e.getMessage()));
                    throw e;
                }
            });
            adapter.setApplicationEventPublisher(event -> {
                if (event instanceof org.springframework.integration.mqtt.event.MqttConnectionFailedEvent failed) {
                    timeline.add("%5dms MqttConnectionFailedEvent".formatted(ms(t0)));
                    handler.onConnectionLost(failed);
                }
            });
            adapter.setOutputChannel(channel);
            adapter.afterPropertiesSet();
            adapter.start();
            await().atMost(Duration.ofSeconds(15))
                .until(() -> BROKER.getLogs().contains("Received SUBSCRIBE from " + clientId));
            int connectsBefore = count(BROKER.getLogs(), " as " + clientId + " (");

            try (var publisher = new MqttClient("tcp://" + brokerHost + ":" + brokerPort,
                "pub-" + UUID.randomUUID(), new MemoryPersistence())) {
                publisher.connect();
                publisher.publish("vehicle/telemetry/RECON-001", payload(1).getBytes(StandardCharsets.UTF_8), 1, false);
                try {
                    await().atMost(Duration.ofSeconds(30)).until(
                        () -> count(BROKER.getLogs(), " as " + clientId + " (") > connectsBefore);
                    // 재전달된 seq 1이 spool에 정상 기록될 때까지
                    await().atMost(Duration.ofSeconds(30)).until(() -> jsonCount(spoolDir) >= 1);
                } catch (RuntimeException e) {
                    timeline.forEach(System.out::println);
                    System.out.println(BROKER.getLogs());
                    throw e;
                }
                // 재전달분 ACK가 브로커에 닿을 시간을 둔다
                Thread.sleep(2000);
                publisher.disconnect();
            }
            adapter.stop();
            adapter.destroy();
            // 저장된 메시지의 ACK가 닿았다면 같은 client ID로 다시 붙어도 재전달이 없다.
            var redelivered = new LinkedBlockingQueue<Integer>();
            try (var resumed = new MqttClient("tcp://" + brokerHost + ":" + brokerPort, clientId, new MemoryPersistence())) {
                resumed.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        redelivered.add(sequenceOf(new String(message.getPayload(), StandardCharsets.UTF_8)));
                    }
                });
                var resumeOptions = new MqttConnectOptions();
                resumeOptions.setCleanSession(false);
                resumed.connect(resumeOptions);
                Thread.sleep(3000);
                resumed.disconnect();
            }
            List<Path> json;
            List<Path> tmp;
            try (var s = java.nio.file.Files.list(spoolDir)) {
                var all = s.toList();
                json = all.stream().filter(p -> p.getFileName().toString().endsWith(".json")).toList();
                tmp = all.stream().filter(p -> p.getFileName().toString().endsWith(".tmp")).toList();
            }
            String logs = BROKER.getLogs();
            timeline.forEach(System.out::println);
            System.out.println("SPOOL_INTERRUPT_CONTRACT deliveries=" + deliveryDuplicateFlags
                + " json=" + json.size() + " tmp=" + tmp.size()
                + " pubackFromClient=" + count(logs, "Received PUBACK from " + clientId)
                + " redeliveredAfterResume=" + redelivered
                + " firstFailure=" + firstFailure.get());

            assertThat(firstFailure.get()).as("첫 쓰기는 인터럽트로 실패해야 재현이다")
                .isInstanceOf(java.nio.channels.ClosedByInterruptException.class);
            assertThat(deliveryDuplicateFlags).as("PUBACK이 없었으므로 브로커가 재전달했다(DUP)").containsExactly(false, true);
            assertThat(json).as("최종: spool에 정확히 1건").hasSize(1);
            assertThat(sequenceOf(java.nio.file.Files.readString(json.get(0)))).isEqualTo(1);
            assertThat(redelivered).as("재전달분은 저장 뒤 ACK되어 더 이상 재전달되지 않는다").isEmpty();
            assertThat(tmp).as("실패한 쓰기가 .tmp를 남기지 않는다").isEmpty();
        }
    }

    private static long jsonCount(Path dir) throws IOException {
        if (!java.nio.file.Files.exists(dir)) return 0;
        try (var s = java.nio.file.Files.list(dir)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".json")).count();
        }
    }

    private static int count(String text, String needle) {
        int count = 0, from = 0;
        while ((from = text.indexOf(needle, from)) >= 0) { count++; from += needle.length(); }
        return count;
    }
}
