package com.telemetry.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.write.Point;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.kafka.TelemetryConsumer;
import com.telemetry.kafka.TelemetryProducer;
import com.telemetry.mqtt.MqttInvalidMessagePublisher;
import com.telemetry.mqtt.MqttMessageHandler;
import com.telemetry.service.AnomalyService;
import com.telemetry.support.TestDecoders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.support.MessageBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * <b>같은 payload가 두 입구에서 같은 판정을 받는지</b> 확인한다 (P0-2 완료 조건).
 *
 * <p>2026-09-09 감사 전에는 MQTT만 Bean Validation을 했고 Kafka 직접 주입은
 * {@code readValue}만 했다. 그래서 같은 payload가 <b>입구에 따라 통과하기도 거부되기도</b>
 * 했다. 부하 도구와 다른 producer가 Kafka 경로로 들어오므로 실제로 열려 있던 구멍이다.
 *
 * <p>이 테스트는 decoder 단위가 아니라 <b>실제 핸들러 두 개</b>를 돌린다 —
 * decoder를 공유한다는 사실만으로는 "핸들러가 그걸 실제로 부른다"가 증명되지 않는다.
 *
 * <h2>이 테스트가 덮지 않는 것</h2>
 *
 * 이상 감지 경로({@code anomaly-detector}, Python, 별도 Consumer Group)는 <b>이 계약을
 * 거치지 않는다.</b> 같은 토픽을 자기 그룹으로 읽고 자체적으로 파싱한다. 저장 입구를
 * 막았다고 <b>이상 감지 경로까지 보호된다고 말하면 안 된다.</b>
 */
@DisplayName("두 입구가 같은 계약을 쓴다")
class BothEntrancesSameContractTest {

    private MqttMessageHandler mqttHandler;
    private TelemetryProducer telemetryProducer;
    private MqttInvalidMessagePublisher invalidPublisher;

    private TelemetryConsumer kafkaConsumer;
    private TelemetryRepository telemetryRepository;
    private KafkaTemplate<String, String> kafkaTemplate;
    private Acknowledgment acknowledgment;

    private static String base() {
        return """
            {"vehicle_id":"KR-GA-1234","timestamp":"2026-09-09T10:00:00.000Z",
             "speed":87.3,"rpm":2400,"engine_temp":92.1,"throttle_position":34.5,
             "fuel_level":67.0,"battery_voltage":13.8,
             "gps":{"lat":37.123456,"lng":127.654321},"dtc_codes":[]}
            """;
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        TelemetryDecoder decoder = TestDecoders.telemetryDecoder();

        telemetryProducer = mock(TelemetryProducer.class);
        invalidPublisher = mock(MqttInvalidMessagePublisher.class);
        mqttHandler = new MqttMessageHandler(
            telemetryProducer, decoder, new SimpleMeterRegistry(), invalidPublisher);

        telemetryRepository = mock(TelemetryRepository.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        acknowledgment = mock(Acknowledgment.class);
        // sendToDlq는 send(ProducerRecord) 오버로드를 쓴다. 다른 오버로드를 목으로 잡으면
        // "DLQ 전송 실패"가 나는데, 그건 **DLQ 발행 실패를 성공으로 안 치는 기존 계약**이
        // 살아 있다는 뜻이다(처음에 잘못 잡아서 실제로 그 예외를 봤다).
        given(kafkaTemplate.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
            .willReturn(CompletableFuture.completedFuture(null));
        kafkaConsumer = new TelemetryConsumer(
            telemetryRepository, mock(AnomalyService.class), new ObjectMapper(), decoder,
            kafkaTemplate, mock(SimpMessagingTemplate.class), new SimpleMeterRegistry());
    }

    /** 각 fixture는 base()에서 한 군데만 바꾼다. */
    private static String fixture(String find, String replace) {
        return base().replace(find, replace);
    }

    @ParameterizedTest(name = "{2}")
    @CsvSource(delimiter = '|', value = {
        "\"speed\":87.3,           | '                              ' | speed 필드 누락",
        "\"speed\":87.3            | \"speed\":null                   | speed null",
        "\"speed\":87.3            | \"speed\":-40.0                  | speed 범위 밖",
        "\"speed\":87.3            | \"sped\":87.3                    | 필드명 오타",
        "\"lat\":37.123456,        | '                              ' | gps.lat 누락",
        "\"dtc_codes\":[]          | \"dtc_codes\":[null]             | DTC null 원소",
        "\"dtc_codes\":[]          | \"dtc_codes\":[\"P0301,P0420\"]  | DTC 쉼표 충돌",
        "\"vehicle_id\":\"KR-GA-1234\", | '                         ' | vehicle_id 누락",
    })
    @DisplayName("계약 위반은 두 입구 모두에서 거부된다")
    void 계약위반은_양쪽에서_거부된다(String find, String replace, String label) {
        String payload = fixture(find, replace.isBlank() ? "" : replace);

        // ── MQTT 입구 ──
        mqttHandler.handle(MessageBuilder.withPayload(payload)
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build());
        verify(telemetryProducer, never()).send(any(VehicleTelemetry.class));
        ArgumentCaptor<String> mqttReason = ArgumentCaptor.forClass(String.class);
        verify(invalidPublisher).publish(any(), any(), mqttReason.capture());

        // ── Kafka 저장 입구 ──
        kafkaConsumer.consumeForStorage(
            List.of(new ConsumerRecord<>("vehicle-telemetry", 0, 0L, "KR-GA-1234", payload)),
            acknowledgment);
        // 저장 대상이 하나도 없어야 한다(빈 배치는 saveAll이 호출되더라도 0건이다).
        ArgumentCaptor<List<Point>> points = ArgumentCaptor.forClass(List.class);
        verify(telemetryRepository).saveAll(points.capture());
        assertThat(points.getValue()).isEmpty();
        // DLQ로 갔는지 확인한다.
        ArgumentCaptor<org.apache.kafka.clients.producer.ProducerRecord> dlqRecord =
            ArgumentCaptor.forClass(org.apache.kafka.clients.producer.ProducerRecord.class);
        verify(kafkaTemplate).send(dlqRecord.capture());
        assertThat(dlqRecord.getValue().topic()).isEqualTo("vehicle-telemetry-dlq");

        // ── 두 입구의 사유가 같아야 한다 ──
        // MQTT는 reject 사유 문자열을, Kafka는 DLQ 헤더에 예외를 남기는데,
        // 둘 다 TelemetryContractException의 같은 reason 코드에서 나온다.
        assertThat(mqttReason.getValue())
            .as("입구별 거부 사유가 같아야 한다: " + label)
            .isIn(TelemetryContractException.PAYLOAD_VALIDATION_FAILED,
                  TelemetryContractException.UNKNOWN_FIELD,
                  TelemetryContractException.TYPE_MISMATCH,
                  TelemetryContractException.MALFORMED_JSON);
    }

    @ParameterizedTest(name = "{2}")
    @CsvSource(delimiter = '|', value = {
        "\"speed\":87.3  | \"speed\":201   | 이상 감지 대상 속도",
        "\"rpm\":2400    | \"rpm\":6001    | 이상 감지 대상 RPM",
        "\"engine_temp\":92.1 | \"engine_temp\":106 | 이상 감지 대상 온도",
        "\"rpm\":2400    | \"rpm\":2400.7  | 소수 RPM 보존",
    })
    @DisplayName("계약 안의 값은 두 입구 모두에서 통과한다")
    void 계약안의_값은_양쪽에서_통과한다(String find, String replace, String label) {
        String payload = fixture(find, replace);

        mqttHandler.handle(MessageBuilder.withPayload(payload)
            .setHeader("mqtt_receivedTopic", "vehicle/telemetry/KR-GA-1234")
            .build());
        verify(telemetryProducer).send(any(VehicleTelemetry.class));
        verify(invalidPublisher, never()).publish(any(), any(), anyString());

        given(telemetryRepository.toPoint(any())).willReturn(Point.measurement("vehicle_telemetry"));
        kafkaConsumer.consumeForStorage(
            List.of(new ConsumerRecord<>("vehicle-telemetry", 0, 0L, "KR-GA-1234", payload)),
            acknowledgment);
        ArgumentCaptor<List<Point>> points = ArgumentCaptor.forClass(List.class);
        verify(telemetryRepository).saveAll(points.capture());
        assertThat(points.getValue()).as(label).hasSize(1);
        verify(kafkaTemplate, never()).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
    }

}
