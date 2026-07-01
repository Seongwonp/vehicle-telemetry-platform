package com.telemetry.service;

import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.entity.AnomalyAlert;
import com.telemetry.repository.AnomalyAlertRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("AnomalyService 단위 테스트")
class AnomalyServiceTest {

    @Mock
    private AnomalyAlertRepository anomalyAlertRepository;

    @InjectMocks
    private AnomalyService anomalyService;

    @Test
    @DisplayName("Kafka 페이로드(snake_case)를 엔티티로 매핑해 저장")
    void save_페이로드_매핑() {
        // given — anomaly-detector(Python)가 발행하는 snake_case 키 형태
        Map<String, Object> payload = Map.of(
            "vehicle_id", "SIM-001",
            "anomaly_type", "엔진 과열",
            "field", "engine_temp",
            "value", 108.5,
            "threshold", "engine_temp > 105°C",
            "severity", "HIGH",
            "detector", "RULE",
            "timestamp", "2026-05-09T10:00:00Z",
            "detected_at", "2026-05-09T10:00:01Z"
        );
        ArgumentCaptor<AnomalyAlert> captor = ArgumentCaptor.forClass(AnomalyAlert.class);

        // when
        anomalyService.save(payload);

        // then
        verify(anomalyAlertRepository).save(captor.capture());
        AnomalyAlert saved = captor.getValue();
        assertThat(saved.getVehicleId()).isEqualTo("SIM-001");
        assertThat(saved.getAnomalyType()).isEqualTo("엔진 과열");
        assertThat(saved.getValue()).isEqualTo(108.5);
        assertThat(saved.getSeverity()).isEqualTo("HIGH");
        assertThat(saved.getVehicleTimestamp()).isNotNull();
        assertThat(saved.getDetectedAt()).isNotNull();
    }

    @Test
    @DisplayName("detected_at이 없으면 현재 시각으로 대체")
    void save_감지시각없으면_현재시각() {
        Map<String, Object> payload = Map.of(
            "vehicle_id", "SIM-001",
            "anomaly_type", "RPM 과부하",
            "severity", "HIGH"
        );
        ArgumentCaptor<AnomalyAlert> captor = ArgumentCaptor.forClass(AnomalyAlert.class);

        anomalyService.save(payload);

        verify(anomalyAlertRepository).save(captor.capture());
        assertThat(captor.getValue().getDetectedAt()).isNotNull();
        assertThat(captor.getValue().getVehicleTimestamp()).isNull();
    }

    @Test
    @DisplayName("최근 이상 이력 조회 시 응답 DTO 리스트로 변환")
    void getRecent_DTO변환() {
        AnomalyAlert alert = new AnomalyAlert();
        alert.setVehicleId("SIM-001");
        alert.setAnomalyType("엔진 과열");
        alert.setSeverity("HIGH");
        given(anomalyAlertRepository.findByVehicleIdOrderByDetectedAtDesc(any(), any()))
            .willReturn(List.of(alert));

        List<AnomalyResponse> result = anomalyService.getRecent("SIM-001", 20);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getVehicleId()).isEqualTo("SIM-001");
    }

    @Test
    @DisplayName("차량별 이상 건수 조회")
    void countByVehicleId_건수반환() {
        given(anomalyAlertRepository.countByVehicleId("SIM-001")).willReturn(3L);

        long count = anomalyService.countByVehicleId("SIM-001");

        assertThat(count).isEqualTo(3L);
    }
}
