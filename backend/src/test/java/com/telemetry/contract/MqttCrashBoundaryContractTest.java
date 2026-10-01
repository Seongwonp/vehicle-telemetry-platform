package com.telemetry.contract;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.images.builder.Transferable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real MQTT + killed worker JVM; the Kafka completion boundary is a controlled pending future. */
// Docker image builders have no daemon; the host CI job separately enforces zero skipped contracts.
@Testcontainers(disabledWithoutDocker = true)
class MqttCrashBoundaryContractTest {
    @Container static final GenericContainer<?> BROKER = new GenericContainer<>("eclipse-mosquitto:2.0")
        .withExposedPorts(1883)
        .withCopyToContainer(Transferable.of(("listener 1883\nallow_anonymous true\nlog_type all\n"
            + "max_queued_messages 100000\nmax_inflight_messages 20\n").getBytes(StandardCharsets.UTF_8)),
            "/mosquitto/config/mosquitto.conf")
        .waitingFor(Wait.forListeningPort());

    @TempDir Path temporary;

    @Test void automaticAckLosesPendingMessageAfterProcessKill() throws Exception {
        for (int i = 0; i < 3; i++) runCrash(false, i);
    }

    @Test void manualAckRetainsPendingMessageAfterProcessKill() throws Exception {
        for (int i = 0; i < 3; i++) runCrash(true, i);
    }

    private void runCrash(boolean manual, int iteration) throws Exception {
        String uri = "tcp://" + BROKER.getHost() + ":" + BROKER.getMappedPort(1883);
        String id = "crash-" + UUID.randomUUID();
        Path state = Files.createDirectory(temporary.resolve(id));
        Path classpathJar = state.resolve("classpath.jar");
        String classpath = Files.readString(Path.of(System.getProperty("test.runtime.classpath.file")));
        var manifest = new java.util.jar.Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("Class-Path", java.util.Arrays.stream(classpath.split(
            java.util.regex.Pattern.quote(java.io.File.pathSeparator)))
            .map(p -> Path.of(p).toUri().toASCIIString()).collect(java.util.stream.Collectors.joining(" ")));
        try (var jar = new java.util.jar.JarOutputStream(Files.newOutputStream(classpathJar), manifest)) { }
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Dfile.encoding=UTF-8", "-cp", classpathJar.toString(), MqttCrashWorker.class.getName(), uri, id, state.toString(),
            Boolean.toString(manual)).redirectErrorStream(true).redirectOutput(state.resolve("worker.log").toFile()).start();
        String payload = """
            {"vehicle_id":"CRASH-001","timestamp":"2026-10-01T00:00:00.%03dZ",
             "speed":10,"rpm":900,"engine_temp":85,"throttle_position":12,"fuel_level":55,
             "battery_voltage":13.8,"gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}
            """.formatted(iteration);
        try {
            await().atMost(Duration.ofSeconds(40)).until(() -> Files.exists(state.resolve("ready.txt")));
            await().atMost(Duration.ofSeconds(10)).until(() -> BROKER.getLogs().contains("Received SUBSCRIBE from " + id));
            try (var publisher = new MqttClient(uri, "pub-" + UUID.randomUUID(), new MemoryPersistence())) {
                publisher.connect();
                publisher.publish("vehicle/telemetry/CRASH-001", payload.getBytes(StandardCharsets.UTF_8), 1, false);
                publisher.disconnect();
            }
            await().atMost(Duration.ofSeconds(10)).until(() -> Files.exists(state.resolve("forwarded.txt")));
            if (!manual) await().atMost(Duration.ofSeconds(10))
                .until(() -> BROKER.getLogs().contains("Received PUBACK from " + id));
            child.destroyForcibly();
            assertThat(child.waitFor(10, TimeUnit.SECONDS)).isTrue();
            var received = new LinkedBlockingQueue<String>();
            try (var resumed = new MqttClient(uri, id, new MemoryPersistence())) {
                resumed.setCallback(new MqttCallback() {
                    public void connectionLost(Throwable cause) { }
                    public void deliveryComplete(IMqttDeliveryToken token) { }
                    public void messageArrived(String topic, MqttMessage message) {
                        received.add(new String(message.getPayload(), StandardCharsets.UTF_8));
                    }
                });
                var options = new MqttConnectOptions();
                options.setCleanSession(false);
                resumed.connect(options);
                String replayed = received.poll(3, TimeUnit.SECONDS);
                if (manual) assertThat(replayed).isEqualTo(payload);
                else assertThat(replayed).isNull();
                resumed.disconnect();
            }
            System.out.printf("CRASH_BOUNDARY manual=%s iteration=%d publisherConfirmed=1 forwarded=1 kafkaCompleted=0 redelivered=%d%n",
                manual, iteration, manual ? 1 : 0);
        } finally {
            if (child.isAlive()) { child.destroyForcibly(); child.waitFor(10, TimeUnit.SECONDS); }
            System.out.println(Files.readString(state.resolve("worker.log")));
        }
    }
}
