package com.telemetry.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.VehicleTelemetry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class TelemetryProducer {

    private static final String TOPIC = "vehicle-telemetry";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * vehicle_id를 파티션 키로 사용한다.
     * 같은 차량의 메시지가 항상 동일 파티션에 쌓이기 때문에, Consumer가 순서를 보장한 채로 처리할 수 있다.
     * 키 없이 라운드로빈으로 보내면 시계열 순서가 뒤섞여 InfluxDB 저장 시 이상 탐지가 오동작할 수 있다.
     */
    public void send(VehicleTelemetry telemetry) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(telemetry);
        } catch (Exception e) {
            // 직렬화 실패는 도메인 객체 자체의 문제일 가능성이 높아 데이터 내용을 남긴다
            log.error("[Kafka] 직렬화 실패로 전송 불가 — vehicle={} timestamp={} speed={} rpm={}",
                telemetry.getVehicleId(),
                telemetry.getTimestamp(),
                telemetry.getSpeed(),
                telemetry.getRpm(),
                e);
            return;
        }

        CompletableFuture<SendResult<String, String>> future =
            kafkaTemplate.send(TOPIC, telemetry.getVehicleId(), payload);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[Kafka] 브로커 전송 실패 — vehicle={} topic={}",
                    telemetry.getVehicleId(), TOPIC, ex);
            } else {
                log.debug("[Kafka] 전송 완료 — vehicle={} partition={} offset={}",
                    telemetry.getVehicleId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });
    }
}
