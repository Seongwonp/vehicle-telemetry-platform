package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.write.Point;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.service.AnomalyService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("배치 안의 역직렬화 실패 1건만 DLQ로 가고 나머지는 정상 저장된다")
    void consumeForStorage_혼합배치_실패건만_DLQ이동() {
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(null));
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
        verify(kafkaTemplate).send("vehicle-telemetry-dlq", "SIM-001", badJson);
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("포인트 변환 실패(잘못된 타임스탬프) 1건도 배치 전체를 막지 않고 그 건만 DLQ로 간다")
    void consumeForStorage_포인트변환실패_해당건만_DLQ이동() {
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(null));
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
        verify(kafkaTemplate).send("vehicle-telemetry-dlq", "SIM-001", badTimestampJson);
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

        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("정상 이상감지 이벤트는 저장하고 DLQ로 보내지 않는다")
    void consumeAnomalyAlerts_정상_저장() {
        String json = "{\"vehicle_id\":\"SIM-001\",\"anomaly_type\":\"엔진 과열\",\"severity\":\"HIGH\"}";
        com.telemetry.entity.AnomalyAlert saved = new com.telemetry.entity.AnomalyAlert();
        saved.setVehicleId("SIM-001");
        saved.setAnomalyType("엔진 과열");
        given(anomalyService.save(any())).willReturn(saved);
        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("vehicle-anomaly-alerts", 0, 0L, "SIM-001", json);

        telemetryConsumer.consumeAnomalyAlerts(record, acknowledgment);

        verify(anomalyService).save(any());
        verify(acknowledgment).acknowledge();
        verify(kafkaTemplate, never()).send(eq("vehicle-anomaly-alerts-dlq"), anyString(), anyString());
    }

    @Test
    @DisplayName("역직렬화 실패한 이상감지 이벤트는 DLQ로 보낸다")
    void consumeAnomalyAlerts_역직렬화실패_DLQ이동() {
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(null));
        String badJson = "not-json-at-all";
        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("vehicle-anomaly-alerts", 0, 0L, "SIM-001", badJson);

        telemetryConsumer.consumeAnomalyAlerts(record, acknowledgment);

        verify(anomalyService, never()).save(any());
        verify(kafkaTemplate).send("vehicle-anomaly-alerts-dlq", "SIM-001", badJson);
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("DLQ 전송 실패 시 원본 offset을 커밋하지 않는다")
    void dlq전송실패_offset미커밋() {
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        given(kafkaTemplate.send(anyString(), anyString(), anyString())).willReturn(failed);
        List<ConsumerRecord<String, String>> records = List.of(telemetryRecord(0L, "not-json"));

        assertThatThrownBy(() -> telemetryConsumer.consumeForStorage(records, acknowledgment))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("DLQ 전송 실패");

        verify(acknowledgment, never()).acknowledge();
    }
}
