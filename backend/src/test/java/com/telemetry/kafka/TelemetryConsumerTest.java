package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.write.Point;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.service.AnomalyService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
@DisplayName("TelemetryConsumer 단위 테스트")
class TelemetryConsumerTest {

    private static final String VALID_TELEMETRY_JSON =
        "{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T10:00:00Z\",\"speed\":80.0,\"rpm\":2000,"
            + "\"engine_temp\":90.0,\"throttle_position\":30.0,\"fuel_level\":50.0,\"battery_voltage\":13.5}";

    // toPoint()는 목이라 반환값의 내용은 검증 대상이 아니다 — 몇 건이 배치에 담겼는지만 본다.
    private static final Point DUMMY_POINT = Point.measurement("vehicle_telemetry");

    private static ConsumerRecord<String, String> telemetryRecord(long offset, String value) {
        return new ConsumerRecord<>("vehicle-telemetry", 0, offset, "SIM-001", value);
    }

    @Mock
    private TelemetryRepository telemetryRepository;

    @Mock
    private AnomalyService anomalyService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    @Mock
    private Acknowledgment acknowledgment;

    // 역직렬화 로직 자체를 검증해야 하므로 목이 아닌 실제 ObjectMapper를 사용한다.
    private final ObjectMapper objectMapper = new ObjectMapper();

    private TelemetryConsumer telemetryConsumer;

    @BeforeEach
    void setUp() {
        telemetryConsumer = new TelemetryConsumer(
            telemetryRepository, anomalyService, objectMapper, kafkaTemplate, messagingTemplate,
            new SimpleMeterRegistry());
    }

    @Test
    @DisplayName("정상 배치는 InfluxDB 쓰기 1건으로 묶여 저장되고 DLQ로 가지 않는다")
    void consumeForStorage_정상_배치저장() {
        given(telemetryRepository.toPoint(any())).willReturn(DUMMY_POINT);

        telemetryConsumer.consumeForStorage(
            List.of(telemetryRecord(0L, VALID_TELEMETRY_JSON),
                    telemetryRecord(1L, VALID_TELEMETRY_JSON),
                    telemetryRecord(2L, VALID_TELEMETRY_JSON)),
            acknowledgment);

        // 건당 writePoint()가 아니라 배치 1회로 묶여야 한다 — 이게 처리량 복구의 핵심이다.
        ArgumentCaptor<List<Point>> captor = ArgumentCaptor.forClass(List.class);
        verify(telemetryRepository).saveAll(captor.capture());
        assertThat(captor.getValue()).hasSize(3);
        verify(acknowledgment).acknowledge();
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("배치 안의 역직렬화 실패 1건만 DLQ로 가고 나머지는 정상 저장된다")
    void consumeForStorage_혼합배치_실패건만_DLQ이동() {
        givenDlqSendSucceeds();
        given(telemetryRepository.toPoint(any())).willReturn(DUMMY_POINT);
        String badJson = "{not-valid-json";

        telemetryConsumer.consumeForStorage(
            List.of(telemetryRecord(0L, VALID_TELEMETRY_JSON),
                    telemetryRecord(1L, badJson),
                    telemetryRecord(2L, VALID_TELEMETRY_JSON)),
            acknowledgment);

        ArgumentCaptor<List<Point>> captor = ArgumentCaptor.forClass(List.class);
        verify(telemetryRepository).saveAll(captor.capture());
        assertThat(captor.getValue()).hasSize(2);
        assertDlqRecord("vehicle-telemetry-dlq", "SIM-001", badJson, "JsonParseException");
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("포인트 변환 실패(잘못된 타임스탬프) 1건도 배치 전체를 막지 않고 그 건만 DLQ로 간다")
    void consumeForStorage_포인트변환실패_해당건만_DLQ이동() {
        givenDlqSendSucceeds();
        String badTimestampJson = VALID_TELEMETRY_JSON.replace("2026-05-09T10:00:00Z", "not-a-timestamp");
        // 역직렬화는 통과하고 toPoint()의 Instant.parse()에서만 터지는 경우를 재현한다.
        given(telemetryRepository.toPoint(any())).willAnswer(invocation -> {
            VehicleTelemetry t = invocation.getArgument(0);
            if (!"2026-05-09T10:00:00Z".equals(t.getTimestamp())) {
                throw new DateTimeParseException("Text could not be parsed", t.getTimestamp(), 0);
            }
            return DUMMY_POINT;
        });

        telemetryConsumer.consumeForStorage(
            List.of(telemetryRecord(0L, VALID_TELEMETRY_JSON),
                    telemetryRecord(1L, badTimestampJson)),
            acknowledgment);

        ArgumentCaptor<List<Point>> captor = ArgumentCaptor.forClass(List.class);
        verify(telemetryRepository).saveAll(captor.capture());
        assertThat(captor.getValue()).hasSize(1);
        assertDlqRecord("vehicle-telemetry-dlq", "SIM-001", badTimestampJson, "DateTimeParseException");
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("InfluxDB 저장 실패는 DLQ로 보내지 않고 offset도 커밋하지 않는다 — 재시도로 이어져야 한다")
    void consumeForStorage_저장실패_재시도유도() {
        given(telemetryRepository.toPoint(any())).willReturn(DUMMY_POINT);
        doThrow(new RuntimeException("InfluxDB 연결 실패")).when(telemetryRepository).saveAll(any());
        List<ConsumerRecord<String, String>> records = List.of(telemetryRecord(0L, VALID_TELEMETRY_JSON));

        // 예전엔 이 경로도 역직렬화 실패와 같은 catch에 묶여 DLQ+ack 됐다 — InfluxDB
        // 장애 중에도 Kafka lag은 낮게 유지된 채 텔레메트리가 조용히 유실되는 버그였다
        // (12시간 soak test로 발견). 이제는 예외가 그대로 전파돼 offset 미커밋 → 재시도로
        // 이어지고, 계속 실패하면 lag 상승으로 드러나야 한다.
        assertThatThrownBy(() -> telemetryConsumer.consumeForStorage(records, acknowledgment))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("InfluxDB 연결 실패");

        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        verify(acknowledgment, never()).acknowledge();
    }

    private ConsumerRecord<String, String> alertRecord(long offset, String json) {
        return new ConsumerRecord<>("vehicle-anomaly-alerts", 0, offset, "SIM-001", json);
    }

    private static final String ALERT_JSON =
        "{\"vehicle_id\":\"SIM-001\",\"anomaly_type\":\"엔진 과열\",\"severity\":\"HIGH\"}";

    private com.telemetry.entity.AnomalyAlert alertEntity() {
        com.telemetry.entity.AnomalyAlert saved = new com.telemetry.entity.AnomalyAlert();
        saved.setVehicleId("SIM-001");
        saved.setAnomalyType("엔진 과열");
        return saved;
    }

    @Test
    @DisplayName("정상 이상감지 배치는 한 번에 저장하고 커밋도 한 번만 한다")
    void consumeAnomalyAlerts_정상_저장() {
        // 배치당 트랜잭션 1건 + offset 커밋 1건이 배치화의 요점이다. 레코드마다 커밋하면
        // 알림 한 건당 PostgreSQL fsync와 브로커 왕복이 붙어 49 msg/s까지 떨어졌다
        // (load-test/anomaly-storage-throughput/).
        com.telemetry.entity.AnomalyAlert saved = alertEntity();
        given(anomalyService.toEntity(any())).willReturn(saved);
        given(anomalyService.saveAll(any())).willReturn(List.of(
            new com.telemetry.service.AnomalyService.SaveResult(saved, true),
            new com.telemetry.service.AnomalyService.SaveResult(saved, true)));

        telemetryConsumer.consumeAnomalyAlerts(
            List.of(alertRecord(0L, ALERT_JSON), alertRecord(1L, ALERT_JSON)), acknowledgment);

        verify(anomalyService, times(1)).saveAll(any());
        verify(acknowledgment, times(1)).acknowledge();
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        verify(messagingTemplate, times(2)).convertAndSend(
            org.mockito.ArgumentMatchers.eq("/topic/vehicle/SIM-001/anomalies"),
            org.mockito.ArgumentMatchers.any(Object.class));
    }

    @Test
    @DisplayName("변환 실패한 레코드만 DLQ로 가고 나머지는 정상 저장된다")
    void consumeAnomalyAlerts_혼합배치_그레코드만_DLQ() {
        // 배치를 한 트랜잭션으로 묶으므로, 잘못된 레코드 하나가 배치 전체를 롤백시키면
        // 안 된다. 그래서 파싱·변환을 트랜잭션 밖에서 레코드별로 격리한다.
        givenDlqSendSucceeds();
        com.telemetry.entity.AnomalyAlert saved = alertEntity();
        given(anomalyService.toEntity(any())).willReturn(saved);
        given(anomalyService.saveAll(any()))
            .willReturn(List.of(new com.telemetry.service.AnomalyService.SaveResult(saved, true)));

        telemetryConsumer.consumeAnomalyAlerts(
            List.of(alertRecord(0L, ALERT_JSON), alertRecord(1L, "not-json-at-all")),
            acknowledgment);

        assertDlqRecord("vehicle-anomaly-alerts-dlq", "SIM-001", "not-json-at-all",
            "JsonParseException");
        verify(acknowledgment, times(1)).acknowledge();
    }

    @Test
    @DisplayName("배치 저장이 실패하면 offset을 커밋하지 않고 예외를 던진다 — 재시도 유도")
    void consumeAnomalyAlerts_배치저장실패_offset미커밋() {
        // 레코드 단위였을 때는 DB 장애에도 곧바로 DLQ로 보내고 커밋했다. 배치에서
        // 그러면 수천 건이 한꺼번에 DLQ로 간다. 재시도(180초 예산)에 맡기는 편이 낫다.
        given(anomalyService.toEntity(any())).willReturn(alertEntity());
        given(anomalyService.saveAll(any()))
            .willThrow(new RuntimeException("PostgreSQL 연결 실패"));

        assertThatThrownBy(() -> telemetryConsumer.consumeAnomalyAlerts(
            List.of(alertRecord(0L, ALERT_JSON)), acknowledgment))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("PostgreSQL 연결 실패");

        verify(acknowledgment, never()).acknowledge();
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("이미 저장된 이상 이벤트는 알림을 다시 보내지 않는다 — 재처리·재전달 중복 방지")
    void consumeAnomalyAlerts_중복이면_브로드캐스트안함() {
        // DLQ 재처리와 리밸런싱 재전달 둘 다 이미 저장된 알림을 다시 넣는다. 행은
        // UNIQUE(event_id)가 막아주지만 브로드캐스트는 막을 것이 없어서, 예전 코드는
        // 같은 알림을 다시 밀어냈다(재처리 실측 9건 중 3건, 리밸런싱 실측 40건 —
        // load-test/anomaly-dlq-idempotency/, load-test/rebalance-redelivery/).
        com.telemetry.entity.AnomalyAlert saved = alertEntity();
        given(anomalyService.toEntity(any())).willReturn(saved);
        given(anomalyService.saveAll(any()))
            .willReturn(List.of(new com.telemetry.service.AnomalyService.SaveResult(saved, false)));

        telemetryConsumer.consumeAnomalyAlerts(List.of(alertRecord(0L, ALERT_JSON)), acknowledgment);

        // offset은 커밋해야 한다 — 중복은 실패가 아니라 정상적으로 처리된 것이다.
        verify(acknowledgment).acknowledge();
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        verify(messagingTemplate, never()).convertAndSend(
            org.mockito.ArgumentMatchers.startsWith("/topic/vehicle/"),
            org.mockito.ArgumentMatchers.any(Object.class));
    }

    @Test
    @DisplayName("역직렬화 실패한 이상감지 이벤트는 DLQ로 보낸다")
    void consumeAnomalyAlerts_역직렬화실패_DLQ이동() {
        givenDlqSendSucceeds();
        String badJson = "not-json-at-all";

        telemetryConsumer.consumeAnomalyAlerts(List.of(alertRecord(0L, badJson)), acknowledgment);

        verify(anomalyService, never()).toEntity(any());
        assertDlqRecord("vehicle-anomaly-alerts-dlq", "SIM-001", badJson, "JsonParseException");
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("DLQ 전송 실패 시 원본 offset을 커밋하지 않는다")
    void dlq전송실패_offset미커밋() {
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        given(kafkaTemplate.send(any(ProducerRecord.class))).willReturn(failed);
        List<ConsumerRecord<String, String>> records = List.of(telemetryRecord(0L, "not-json"));

        assertThatThrownBy(() -> telemetryConsumer.consumeForStorage(records, acknowledgment))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("DLQ 전송 실패");

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("재처리 이력(x-dlq-replay-count)은 DLQ 레코드로 이어져야 한다 — 무한 루프 방지")
    void sendToDlq_재처리이력_보존() {
        givenDlqSendSucceeds();
        // 재처리 도구가 DLQ 레코드를 원본 토픽으로 되돌릴 때 이 헤더를 올려서 보낸다.
        ConsumerRecord<String, String> replayed = telemetryRecord(0L, "{not-json");
        replayed.headers().add("x-dlq-replay-count", "2".getBytes(StandardCharsets.UTF_8));

        telemetryConsumer.consumeForStorage(List.of(replayed), acknowledgment);

        // 여기서 헤더를 이어받지 않으면 카운터가 매번 0으로 리셋돼, 영구 실패 메시지가
        // DLQ→원본→DLQ를 무한히 돈다. 실제로 재처리 4회에 2→4→8→16건으로 증식했다.
        ArgumentCaptor<ProducerRecord<String, String>> captor =
            ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(header(captor.getValue(), "x-dlq-replay-count")).isEqualTo("2");
    }

    private void givenDlqSendSucceeds() {
        given(kafkaTemplate.send(any(ProducerRecord.class)))
            .willReturn(CompletableFuture.completedFuture(null));
    }

    private static String header(ProducerRecord<String, String> record, String key) {
        Header found = record.headers().lastHeader(key);
        return found == null ? null : new String(found.value(), StandardCharsets.UTF_8);
    }

    /**
     * DLQ 레코드가 원본 payload뿐 아니라 <b>실패 원인</b>까지 싣고 있는지 본다.
     *
     * <p>재처리 도구(dlq-tools/dlq.py)가 이 헤더로 "다시 넣으면 되는 실패"와 "몇 번을
     * 넣어도 실패하는 메시지"를 가른다. 헤더가 조용히 빠지면 도구는 모든 걸
     * unknown으로 분류해 아무것도 재처리하지 않게 되므로, 계약으로 고정한다.
     */
    @SuppressWarnings("unchecked")
    private void assertDlqRecord(String expectedTopic, String expectedKey,
                                 String expectedValue, String expectedFailureClass) {
        ArgumentCaptor<ProducerRecord<String, String>> captor =
            ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> sent = captor.getValue();

        assertThat(sent.topic()).isEqualTo(expectedTopic);
        assertThat(sent.key()).isEqualTo(expectedKey);
        assertThat(sent.value()).isEqualTo(expectedValue);
        assertThat(header(sent, "x-dlq-origin-topic")).isNotBlank();
        assertThat(header(sent, "x-dlq-origin-partition")).isEqualTo("0");
        assertThat(header(sent, "x-dlq-origin-offset")).isNotBlank();
        assertThat(header(sent, "x-dlq-failed-at")).isNotBlank();
        assertThat(header(sent, "x-dlq-failure-class")).contains(expectedFailureClass);
        assertThat(header(sent, "x-dlq-failure-message")).isNotBlank();
    }
}
