package com.telemetry.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 재시도 예산이 소진돼 레코드가 DLQ로 갈 때 <b>로그에 흔적이 남는지</b>.
 *
 * <p>이 테스트는 실측에서 나왔다. InfluxDB를 12분 정지시킨 실험에서 6,158건이 DLQ로
 * 갔는데 <b>백엔드 로그에는 한 줄도 없었다</b>
 * ({@code load-test/long-outage/RESULT_20260906_influxdb.md}). 저장소는 실패를 카운터로만
 * 세고 예외를 다시 던지고, Spring Kafka의 재시도·복구 로그는 DEBUG인데 설정이 WARN이라
 * 전부 잘렸다. Runbook은 운영자에게 "DLQ를 조사하라"고 하는데, DLQ가 생겼다는 사실
 * 자체가 로그에 없으면 사고 후 되짚을 경로가 없다.
 *
 * <p>로그 문구를 테스트로 고정하는 것은 보통 과하지만, 여기서는 <b>그 문구가 없다는 것</b>이
 * 실제 관측 구멍이었으므로 사라지면 알아야 한다.
 */
class KafkaDlqLoggingTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(KafkaConfig.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
    }

    @Test
    @DisplayName("재시도 소진으로 DLQ에 보낼 때 WARN 로그와 카운터가 남는다")
    @SuppressWarnings("unchecked")
    void dlq로_보낼_때_로그와_카운터가_남는다() throws Exception {
        KafkaOperations<String, String> operations = mock(KafkaOperations.class);
        CompletableFuture<SendResult<String, String>> sent = new CompletableFuture<>();
        sent.complete(new SendResult<>(null, null));
        when(operations.send(any(ProducerRecord.class))).thenReturn(sent);

        MeterRegistry registry = new SimpleMeterRegistry();
        // 예산 0 = 재시도 없이 곧바로 recoverer로. 이 테스트가 보려는 것은 재시도 횟수가
        // 아니라 "recoverer까지 갔을 때 무엇이 남는가"다.
        DefaultErrorHandler handler = new KafkaConfig()
            .kafkaErrorHandler(operations, registry, 1L, 1.0, 1L, 0L);

        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("vehicle-telemetry", 2, 41L, "SIM-001", "{}");
        // 실제로 에러 핸들러가 받는 예외는 한 겹 싸여 있다. 그대로 찍으면 "Listener failed"만
        // 남고 정작 원인이 안 보이는데, 그 상황을 여기서 재현한다.
        Exception thrown = new ListenerExecutionFailedException(
            "Listener failed", new IllegalStateException("influxdb 연결 거부"));

        handler.handleOne(thrown, record, mock(Consumer.class), mock(MessageListenerContainer.class));

        assertThat(registry.counter("telemetry.kafka.dlq.published",
            "topic", "vehicle-telemetry-dlq").count()).isEqualTo(1.0);

        assertThat(appender.list)
            .as("DLQ 이동이 로그에 남아야 한다")
            .isNotEmpty();
        String message = appender.list.get(0).getFormattedMessage();
        assertThat(message).contains("[DLQ 이동]");
        assertThat(message).contains("vehicle-telemetry-2@41");
        assertThat(message).contains("vehicle-telemetry-dlq");
        // 껍데기 예외가 아니라 **원인**이 보여야 한다.
        assertThat(message).contains("influxdb 연결 거부");
    }
}
