package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.service.AnomalyService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("TelemetryConsumer 단위 테스트")
class TelemetryConsumerTest {

    private static final String VALID_TELEMETRY_JSON =
        "{\"vehicle_id\":\"SIM-001\",\"timestamp\":\"2026-05-09T10:00:00Z\",\"speed\":80.0,\"rpm\":2000,"
            + "\"engine_temp\":90.0,\"throttle_position\":30.0,\"fuel_level\":50.0,\"battery_voltage\":13.5}";

    @Mock
    private TelemetryRepository telemetryRepository;

    @Mock
    private AnomalyService anomalyService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    // 역직렬화 로직 자체를 검증해야 하므로 목이 아닌 실제 ObjectMapper를 사용한다.
    private final ObjectMapper objectMapper = new ObjectMapper();

    private TelemetryConsumer telemetryConsumer;

    @BeforeEach
    void setUp() {
        telemetryConsumer = new TelemetryConsumer(telemetryRepository, anomalyService, objectMapper, kafkaTemplate);
    }

    @Test
    @DisplayName("정상 텔레메트리 메시지는 저장하고 DLQ로 보내지 않는다")
    void consumeForStorage_정상_저장() {
        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("vehicle-telemetry", 0, 0L, "SIM-001", VALID_TELEMETRY_JSON);

        telemetryConsumer.consumeForStorage(record);

        verify(telemetryRepository).save(any());
        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("역직렬화 실패한 메시지는 저장 대신 DLQ로 보낸다")
    void consumeForStorage_역직렬화실패_DLQ이동() {
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(null));
        String badJson = "{not-valid-json";
        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("vehicle-telemetry", 0, 0L, "SIM-001", badJson);

        telemetryConsumer.consumeForStorage(record);

        verify(telemetryRepository, never()).save(any());
        verify(kafkaTemplate).send("vehicle-telemetry-dlq", "SIM-001", badJson);
    }

    @Test
    @DisplayName("저장 중 예외가 나면 DLQ로 보낸다")
    void consumeForStorage_저장실패_DLQ이동() {
        given(kafkaTemplate.send(anyString(), anyString(), anyString()))
            .willReturn(CompletableFuture.completedFuture(null));
        doThrow(new RuntimeException("InfluxDB 연결 실패")).when(telemetryRepository).save(any());
        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("vehicle-telemetry", 0, 0L, "SIM-001", VALID_TELEMETRY_JSON);

        telemetryConsumer.consumeForStorage(record);

        verify(kafkaTemplate).send("vehicle-telemetry-dlq", "SIM-001", VALID_TELEMETRY_JSON);
    }

    @Test
    @DisplayName("정상 이상감지 이벤트는 저장하고 DLQ로 보내지 않는다")
    void consumeAnomalyAlerts_정상_저장() {
        String json = "{\"vehicle_id\":\"SIM-001\",\"anomaly_type\":\"엔진 과열\",\"severity\":\"HIGH\"}";
        ConsumerRecord<String, String> record =
            new ConsumerRecord<>("vehicle-anomaly-alerts", 0, 0L, "SIM-001", json);

        telemetryConsumer.consumeAnomalyAlerts(record);

        verify(anomalyService).save(any());
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

        telemetryConsumer.consumeAnomalyAlerts(record);

        verify(anomalyService, never()).save(any());
        verify(kafkaTemplate).send("vehicle-anomaly-alerts-dlq", "SIM-001", badJson);
    }
}
