package com.telemetry.service;

import com.telemetry.dto.response.AnomalyResponse;
import com.telemetry.entity.AnomalyAlert;
import com.telemetry.repository.AnomalyAlertRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class AnomalyService {

    private final AnomalyAlertRepository anomalyAlertRepository;
    private final MeterRegistry meterRegistry;

    @Transactional
    public SaveResult save(Map<String, Object> payload) {
        return saveAll(List.of(toEntity(payload))).get(0);
    }

    /**
     * Kafka 페이로드를 엔티티로 옮긴다. <b>DB를 건드리지 않는다.</b>
     *
     * <p>파싱을 저장에서 떼어낸 이유는 {@link #saveAll}이 배치를 한 트랜잭션으로
     * 묶기 때문이다. 잘못된 타임스탬프({@link Instant#parse} 실패) 하나가 트랜잭션
     * 안에서 터지면 **정상 알림 수천 건까지 함께 롤백된다.** 파싱을 먼저 끝내고
     * 레코드별로 격리해야(호출자가 catch → DLQ) 배치화가 안전해진다 —
     * 텔레메트리 경로에서 {@code TelemetryRepository.toPoint()}를 따로 뺀 것과 같은 이유다.
     *
     * <p><b>{@code NOT_SUPPORTED}가 반드시 필요하다.</b> 이 클래스에는 클래스 레벨로
     * {@code @Transactional(readOnly = true)}가 붙어 있어서, 명시하지 않으면 DB를 전혀
     * 건드리지 않는 이 메서드도 트랜잭션을 연다. 그러면 PostgreSQL 장애 중에
     * {@code Could not open JPA EntityManager} 로 <b>변환 단계에서 실패</b>하고,
     * 호출자는 그것을 "이 메시지가 잘못됐다"로 오해해 레코드를 하나씩 DLQ로 보낸다 —
     * 배치화로 없애려던 바로 그 동작이다.
     *
     * <p>실제로 5분 장애를 주입해 발견했다: 저장할 것이 하나도 없는
     * "배치 저장 실패 <b>0건</b>" 로그가 반복해서 찍혔고, DLQ에는 8건의 알림이
     * 19개 레코드로 불어나 있었다
     * ({@code load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md}).
     */
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public AnomalyAlert toEntity(Map<String, Object> payload) {
        AnomalyAlert alert = new AnomalyAlert();
        alert.setEventId(resolveEventId(payload));
        alert.setVehicleId((String) payload.get("vehicle_id"));
        alert.setAnomalyType((String) payload.get("anomaly_type"));
        alert.setField((String) payload.get("field"));
        alert.setValue(toDouble(payload.get("value")));
        alert.setThreshold((String) payload.get("threshold"));
        alert.setSeverity((String) payload.get("severity"));
        alert.setDetector((String) payload.get("detector"));

        String vehicleTs = (String) payload.get("timestamp");
        if (vehicleTs != null) {
            alert.setVehicleTimestamp(Instant.parse(vehicleTs));
        }

        String detectedAt = (String) payload.get("detected_at");
        alert.setDetectedAt(detectedAt != null ? Instant.parse(detectedAt) : Instant.now());
        return alert;
    }

    /**
     * 배치를 <b>한 트랜잭션</b>으로 저장한다.
     *
     * <p>레코드마다 트랜잭션을 열면 알림 한 건당 PostgreSQL 커밋(=fsync)이 한 번씩 돈다.
     * 실측으로 이 경로의 처리량이 <b>49 msg/s</b>였다 — 100대 부하의 유입(200건/s 이상)을
     * 못 따라가서 lag이 계속 쌓였고, 부하를 끊은 뒤 따라잡는 데 13분이 걸렸다
     * ({@code load-test/anomaly-storage-throughput/}).
     *
     * <p>배치를 한 트랜잭션으로 묶으면 커밋이 배치당 1회가 된다. 저장 경로(InfluxDB)에서
     * 같은 이유로 배치화해 처리량을 회복시킨 적이 있다(ADR-011).
     */
    @Transactional
    public List<SaveResult> saveAll(List<AnomalyAlert> alerts) {
        // 빈 배치에 트랜잭션을 열지 않는다. 열면 DB가 죽어 있을 때 "저장할 것이
        // 하나도 없는데 저장 실패"가 나고, 그 예외 때문에 offset이 커밋되지 않아
        // 같은 배치가 계속 되돌아온다.
        if (alerts.isEmpty()) return List.of();

        List<SaveResult> results = new ArrayList<>(alerts.size());
        for (AnomalyAlert alert : alerts) {
            results.add(saveOne(alert));
        }
        return results;
    }

    private SaveResult saveOne(AnomalyAlert alert) {
        int inserted = anomalyAlertRepository.insertIfAbsent(
            alert.getEventId(), alert.getVehicleId(), alert.getAnomalyType(), alert.getField(),
            alert.getValue(), alert.getThreshold(), alert.getSeverity(), alert.getDetector(),
            alert.getVehicleTimestamp(), alert.getDetectedAt());
        AnomalyAlert saved = anomalyAlertRepository.findByEventId(alert.getEventId())
            .orElseThrow(() -> new IllegalStateException("이상 이벤트 저장 결과를 찾을 수 없습니다"));

        boolean isNew = inserted > 0;
        meterRegistry.counter("telemetry.anomaly.stored",
            "result", isNew ? "new" : "duplicate").increment();
        if (isNew) {
            log.info("[이상 저장] vehicle={} type={} severity={}",
                alert.getVehicleId(), alert.getAnomalyType(), alert.getSeverity());
        } else {
            // 재처리에서는 정상적인 결과다. 로그를 나누는 이유는 두 가지다 —
            // (1) 예전엔 중복도 "[이상 저장]"으로 찍혀서, 재처리 후 로그를 세면 실제
            //     저장된 건수보다 많이 나왔다(실측: 9건 재처리에 행 증가는 6건).
            // (2) 중복 비율이 높으면 재처리 범위가 필요 이상으로 넓다는 신호다.
            log.info("[이상 중복] 이미 저장된 이벤트라 건너뜀 vehicle={} type={} event={}",
                alert.getVehicleId(), alert.getAnomalyType(), alert.getEventId());
        }
        return new SaveResult(saved, isNew);
    }

    /**
     * 저장 결과. <b>새로 들어갔는지</b>가 호출자에게 필요하다.
     *
     * <p>DLQ 재처리는 이미 저장된 알림을 필연적으로 다시 넣는다 — PostgreSQL 60초 장애를
     * 주입해 재보니 DLQ 9건 중 <b>3건은 이미 저장돼 있었다</b>(서버에서는 커밋이 끝났는데
     * 연결이 끊겨 클라이언트만 실패로 본 경우). 행 중복은
     * {@code ON CONFLICT (event_id) DO NOTHING}이 막아주지만, 호출자가 이 구분을 모르면
     * <b>알림은 그대로 다시 나간다.</b>
     * ({@code load-test/anomaly-dlq-idempotency/})
     */
    public record SaveResult(AnomalyAlert alert, boolean inserted) {}

    public List<AnomalyResponse> getRecent(String vehicleId, int limit) {
        return anomalyAlertRepository
            .findByVehicleIdOrderByDetectedAtDesc(vehicleId, PageRequest.of(0, limit))
            .stream()
            .map(AnomalyResponse::new)
            .toList();
    }

    public long countByVehicleId(String vehicleId) {
        return anomalyAlertRepository.countByVehicleId(vehicleId);
    }

    public Page<AnomalyResponse> search(
        String vehicleId, String severity, Instant from, Instant to, int page, int size
    ) {
        Specification<AnomalyAlert> spec = (root, query, cb) ->
            cb.equal(root.get("vehicleId"), vehicleId);
        if (severity != null && !severity.isBlank()) {
            spec = spec.and((root, query, cb) -> cb.equal(root.get("severity"), severity));
        }
        if (from != null) {
            spec = spec.and((root, query, cb) ->
                cb.greaterThanOrEqualTo(root.get("detectedAt"), from));
        }
        if (to != null) {
            spec = spec.and((root, query, cb) ->
                cb.lessThanOrEqualTo(root.get("detectedAt"), to));
        }
        return anomalyAlertRepository.findAll(
            spec, PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "detectedAt")))
            .map(AnomalyResponse::new);
    }

    private Double toDouble(Object val) {
        if (val instanceof Number n) return n.doubleValue();
        return null;
    }

    private String resolveEventId(Map<String, Object> payload) {
        Object supplied = payload.get("event_id");
        if (supplied instanceof String value && value.matches("^[a-f0-9]{64}$")) {
            return value;
        }
        String key = String.join("|",
            stringValue(payload.get("vehicle_id")),
            stringValue(payload.get("timestamp")),
            stringValue(payload.get("anomaly_type")),
            stringValue(payload.get("field")),
            stringValue(payload.get("detector")));
        try {
            return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256을 사용할 수 없습니다", e);
        }
    }

    private String stringValue(Object value) {
        return value == null ? "" : value.toString();
    }
}
