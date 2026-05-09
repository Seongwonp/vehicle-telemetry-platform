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
     * vehicle_id를 파티션 키로 사용하여 같은 차량 데이터가 항상 같은 파티션에 저장되도록 보장.
     * 순서 보장 + Consumer 그룹의 파티션 분산 처리에 유리.
     */
    public void send(VehicleTelemetry telemetry) {
        try {
            String payload = objectMapper.writeValueAsString(telemetry);

            CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(TOPIC, telemetry.getVehicleId(), payload);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("[Kafka] 전송 실패 vehicle={}", telemetry.getVehicleId(), ex);
                } else {
                    log.debug("[Kafka] 전송 성공 vehicle={} partition={} offset={}",
                        telemetry.getVehicleId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset()
                    );
                }
            });

        } catch (Exception e) {
            log.error("[Kafka] 직렬화 실패 vehicle={}", telemetry.getVehicleId(), e);
        }
    }
}
