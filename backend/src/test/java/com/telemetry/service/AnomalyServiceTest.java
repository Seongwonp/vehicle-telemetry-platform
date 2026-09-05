package com.telemetry.service;

import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.entity.AnomalyAlert;
import com.telemetry.repository.AnomalyAlertRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.time.Instant;
import org.springframework.data.domain.PageImpl;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
@DisplayName("AnomalyService 단위 테스트")
class AnomalyServiceTest {

    @Mock
    private AnomalyAlertRepository anomalyAlertRepository;

    // 지표를 세는 것 자체가 검증 대상은 아니라 mock 대신 실제 구현을 쓴다 —
    // mock이면 counter()가 null을 돌려줘 NPE가 난다.
    @org.mockito.Spy
    private io.micrometer.core.instrument.MeterRegistry meterRegistry =
        new io.micrometer.core.instrument.simple.SimpleMeterRegistry();

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
        AnomalyAlert persisted = new AnomalyAlert();
        persisted.setEventId("a".repeat(64));
        persisted.setVehicleId("SIM-001");
        persisted.setAnomalyType("엔진 과열");
        persisted.setValue(108.5);
        persisted.setSeverity("HIGH");
        given(anomalyAlertRepository.findByEventId(any())).willReturn(Optional.of(persisted));

        // when
        anomalyService.save(payload);

        // then
        verify(anomalyAlertRepository).insertIfAbsent(
            any(), org.mockito.ArgumentMatchers.eq("SIM-001"),
            org.mockito.ArgumentMatchers.eq("엔진 과열"),
            org.mockito.ArgumentMatchers.eq("engine_temp"),
            org.mockito.ArgumentMatchers.eq(108.5),
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("HIGH"),
            org.mockito.ArgumentMatchers.eq("RULE"),
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    @DisplayName("이미 저장된 이벤트면 inserted=false로 알려준다 — 재처리 시 중복 알림 방지")
    void save_중복이면_inserted_false() {
        // DLQ 재처리는 이미 저장된 알림을 반드시 다시 넣는다(PostgreSQL 장애 실측:
        // DLQ 9건 중 3건이 이미 저장돼 있었다). 행은 ON CONFLICT DO NOTHING이 막지만,
        // 호출자가 "새로 들어갔는지"를 모르면 WebSocket 알림이 그대로 다시 나간다.
        Map<String, Object> payload = Map.of(
            "vehicle_id", "SIM-001",
            "anomaly_type", "엔진 과열",
            "severity", "HIGH"
        );
        AnomalyAlert persisted = new AnomalyAlert();
        persisted.setEventId("d".repeat(64));
        persisted.setVehicleId("SIM-001");
        persisted.setDetectedAt(Instant.now());
        given(anomalyAlertRepository.findByEventId(any())).willReturn(Optional.of(persisted));

        // insertIfAbsent가 0 = 충돌로 아무것도 안 들어감
        given(anomalyAlertRepository.insertIfAbsent(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).willReturn(0);
        assertThat(anomalyService.save(payload).inserted()).isFalse();

        // 1 = 새로 들어감
        given(anomalyAlertRepository.insertIfAbsent(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).willReturn(1);
        assertThat(anomalyService.save(payload).inserted()).isTrue();
    }

    @Test
    @DisplayName("detected_at이 없으면 현재 시각으로 대체")
    void save_감지시각없으면_현재시각() {
        Map<String, Object> payload = Map.of(
            "vehicle_id", "SIM-001",
            "anomaly_type", "RPM 과부하",
            "severity", "HIGH"
        );
        AnomalyAlert persisted = new AnomalyAlert();
        persisted.setEventId("b".repeat(64));
        persisted.setVehicleId("SIM-001");
        persisted.setAnomalyType("RPM 과부하");
        persisted.setDetectedAt(java.time.Instant.now());
        given(anomalyAlertRepository.findByEventId(any())).willReturn(Optional.of(persisted));

        anomalyService.save(payload);

        verify(anomalyAlertRepository).insertIfAbsent(
            any(), org.mockito.ArgumentMatchers.eq("SIM-001"),
            org.mockito.ArgumentMatchers.eq("RPM 과부하"),
            org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.eq("HIGH"),
            org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.isNull(),
            org.mockito.ArgumentMatchers.any());
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

    @Test
    @DisplayName("심각도·기간 조건으로 페이지 조회")
    void search_필터_페이지조회() {
        AnomalyAlert alert = new AnomalyAlert();
        alert.setVehicleId("SIM-001");
        alert.setSeverity("HIGH");
        alert.setDetectedAt(Instant.parse("2026-08-04T10:00:00Z"));
        given(anomalyAlertRepository.findAll(
            org.mockito.ArgumentMatchers.<org.springframework.data.jpa.domain.Specification<AnomalyAlert>>any(),
            org.mockito.ArgumentMatchers.<org.springframework.data.domain.Pageable>any()))
            .willReturn(new PageImpl<>(List.of(alert)));

        var result = anomalyService.search(
            "SIM-001", "HIGH", Instant.parse("2026-08-03T00:00:00Z"), null, 0, 20);

        assertThat(result.getContent()).hasSize(1);
        assertThat(result.getContent().get(0).getSeverity()).isEqualTo("HIGH");
        verify(anomalyAlertRepository).findAll(
            org.mockito.ArgumentMatchers.<org.springframework.data.jpa.domain.Specification<AnomalyAlert>>any(),
            org.mockito.ArgumentMatchers.<org.springframework.data.domain.Pageable>any());
    }

    @Test
    @DisplayName("동일 원본 이벤트는 같은 결정적 eventId를 사용")
    void save_동일이벤트_eventId동일() {
        Map<String, Object> payload = Map.of(
            "vehicle_id", "SIM-001",
            "timestamp", "2026-05-09T10:00:00Z",
            "anomaly_type", "엔진 과열",
            "field", "engine_temp",
            "value", 108.0,
            "threshold", "engine_temp > 105",
            "severity", "HIGH",
            "detector", "RULE",
            "detected_at", "2026-05-09T10:00:01Z"
        );
        AnomalyAlert persisted = new AnomalyAlert();
        persisted.setEventId("c".repeat(64));
        persisted.setVehicleId("SIM-001");
        persisted.setAnomalyType("엔진 과열");
        persisted.setDetectedAt(java.time.Instant.now());
        given(anomalyAlertRepository.findByEventId(any())).willReturn(Optional.of(persisted));

        anomalyService.save(payload);
        anomalyService.save(payload);

        org.mockito.ArgumentCaptor<String> eventIds =
            org.mockito.ArgumentCaptor.forClass(String.class);
        verify(anomalyAlertRepository, times(2)).insertIfAbsent(
            eventIds.capture(), any(), any(), any(), any(), any(), any(), any(), any(), any());
        assertThat(eventIds.getAllValues()).containsOnly(eventIds.getAllValues().get(0));
        assertThat(eventIds.getValue()).hasSize(64);
    }
    @Test
    @DisplayName("빈 배치는 트랜잭션도 저장소도 건드리지 않는다")
    void saveAll_빈배치_무동작() {
        // 컨슈머는 배치의 모든 레코드가 변환에 실패해도 saveAll을 호출한다.
        // 여기서 트랜잭션을 열면 DB 장애 중 "저장할 것이 하나도 없는데 저장 실패"가
        // 나고, 그 예외로 offset이 커밋되지 않아 같은 배치가 계속 되돌아온다
        // (5분 장애 실측에서 "배치 저장 실패 0건" 로그로 발견).
        assertThat(anomalyService.saveAll(List.of())).isEmpty();
        org.mockito.Mockito.verifyNoInteractions(anomalyAlertRepository);
    }

    @Test
    @DisplayName("toEntity는 트랜잭션을 열지 않는다 — DB 장애 중 변환까지 실패하면 안 된다")
    void toEntity_트랜잭션_없음() throws NoSuchMethodException {
        // 이 클래스에는 클래스 레벨 @Transactional(readOnly = true)가 붙어 있어서,
        // 명시하지 않으면 DB를 안 건드리는 메서드도 트랜잭션을 연다. 그러면
        // PostgreSQL 장애 중 "변환 실패"로 오인돼 레코드가 하나씩 DLQ로 간다 —
        // 배치화로 없애려던 동작이다. 실측으로 겪고 고쳤다.
        //
        // 동작이 아니라 설정이라 단위 테스트로는 애너테이션을 직접 확인한다.
        Transactional annotation = AnomalyService.class
            .getMethod("toEntity", Map.class)
            .getAnnotation(Transactional.class);

        assertThat(annotation).isNotNull();
        assertThat(annotation.propagation()).isEqualTo(Propagation.NOT_SUPPORTED);
    }
}
