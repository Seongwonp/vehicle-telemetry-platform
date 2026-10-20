#!/usr/bin/env python3
"""
Vehicle Anomaly Detector

Kafka Consumer → 룰 기반 + ML 이상 감지 → vehicle-anomaly-alerts 발행 + 웹훅 알림

데이터 흐름:
  [Kafka: vehicle-telemetry]
       ↓
  rules.py (즉시 판단)
  ml_detector.py (IsolationForest, 200개 샘플 수집 후 활성화)
       ↓
  [Kafka: vehicle-anomaly-alerts]  ← Spring Boot가 소비 → PostgreSQL 저장
       ↓
  notifier.py (Webhook)

실패한 메시지는 vehicle-telemetry-anomaly-dlq로 격리하고, 발행 또는 DLQ 성공 뒤에만
오프셋을 수동 커밋한다. auto-commit은 실패 메시지를 조용히 넘긴다(ADR-017).
"""
import os
import json
import signal
import socket
import sys
import time
import logging
import hashlib
from dataclasses import asdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from prometheus_client import REGISTRY, Counter, start_http_server
from dotenv import load_dotenv

import rules
import contract
import notifier
from ml_detector import MLAnomalyDetector, missing_features

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
# 여러 인스턴스를 띄웠을 때 로그에서 인스턴스를 구분하려고 hostname을 넣는다.
logger = logging.getLogger(f"anomaly-detector[{socket.gethostname()}]")

# ── 설정 ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC             = "vehicle-telemetry"
OUTPUT_TOPIC            = "vehicle-anomaly-alerts"
DLQ_TOPIC               = "vehicle-telemetry-anomaly-dlq"
METRICS_PORT            = int(os.getenv("METRICS_PORT", "9101"))
ML_ENABLED              = os.getenv("ML_ENABLED", "false").lower() == "true"
ML_MIN_SAMPLES          = int(os.getenv("ML_MIN_SAMPLES", "200"))
# 학습 윈도우(건수). 덮는 시간이 처리량에 따라 달라지므로 환경 변수로 뺐다(ADR-018).
ML_WINDOW_SIZE          = int(os.getenv("ML_WINDOW_SIZE", "2000"))
# 비우면 offset_을 써서 데이터와 무관하게 항상 contamination 비율만큼 알림이 뜬다.
# 값은 score_ml.py --sweep으로 정한다(ADR-018).
_threshold_raw          = os.getenv("ML_SCORE_THRESHOLD", "").strip()
ML_SCORE_THRESHOLD      = float(_threshold_raw) if _threshold_raw else None
# 측정 전용(score_ml.py --sweep 입력). 메시지마다 한 줄이라 처리량이 떨어진다.
ML_SCORE_DUMP           = os.getenv("ML_SCORE_DUMP", "false").lower() == "true"

# 메시지마다 동기 커밋하면 처리량이 떨어져 개수/시간 기준으로 배치 커밋한다.
COMMIT_INTERVAL_MESSAGES = 100
COMMIT_INTERVAL_SECONDS  = 5.0


def make_consumer() -> KafkaConsumer:
    # 원본 바이트로 받는다 — 파싱 실패도 직접 잡아 원본 그대로 DLQ에 보내야 한다.
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="anomaly-detector-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def make_dlq_producer() -> KafkaProducer:
    # DLQ는 원본 바이트(키·값)를 그대로 보존해야 하므로 별도 직렬화 없이 전달한다.
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        retries=3,
    )


def make_redis_client():
    """ML이 꺼져 있으면 Redis를 만들지 않는다 — 기본 경로에 실패 지점을 늘리지 않는다."""
    if not ML_ENABLED:
        return None
    import redis
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        password=os.getenv("REDIS_PASSWORD") or None,
    )


def ml_state_key(partition: int) -> str:
    return f"anomaly-detector:ml-model:{INPUT_TOPIC}:{partition}"


class PartitionedMLDetectors:
    """파티션마다 별도 MLAnomalyDetector를 둔다.

    vehicle_id가 파티션 키라 학습 상태를 인스턴스가 아닌 파티션 기준으로 Redis에 저장하면
    재시작·리밸런싱 뒤에도 이어받는다(ADR-018).
    한계: 파티션 반납 시점을 감지하지 않아 마지막 저장 이후 학습분은 잃을 수 있다.
    """

    def __init__(self, redis_client, min_samples: int, window_size: int,
                 score_threshold: float | None = None):
        self._redis = redis_client
        self._min_samples = min_samples
        self._window_size = window_size
        self._score_threshold = score_threshold
        self._detectors: dict[int, MLAnomalyDetector] = {}

    def get(self, partition: int) -> MLAnomalyDetector:
        detector = self._detectors.get(partition)
        if detector is None:
            detector = self._load(partition)
            self._detectors[partition] = detector
        return detector

    def _load(self, partition: int) -> MLAnomalyDetector:
        detector = MLAnomalyDetector(
            min_samples=self._min_samples, window_size=self._window_size,
            score_threshold=self._score_threshold,
        )
        if self._redis is None:
            return detector
        try:
            blob = self._redis.get(ml_state_key(partition))
            if blob:
                detector.load_state(blob)
                logger.info(f"[ML] 파티션 {partition} 학습 상태 복원 완료")
        except Exception:
            logger.warning(
                f"[ML] 파티션 {partition} 학습 상태 복원 실패 — 새로 학습합니다", exc_info=True
            )
        return detector

    def save_all(self) -> None:
        if self._redis is None:
            return
        for partition, detector in self._detectors.items():
            if not detector.is_trained:
                continue
            try:
                self._redis.set(ml_state_key(partition), detector.get_state())
            except Exception:
                logger.warning(f"[ML] 파티션 {partition} 학습 상태 저장 실패", exc_info=True)


def dump_scores(batch: list[dict], flags: list[bool], scores: list[float]) -> None:
    """배치의 ML 점수를 stdout에 한 번에 쓴다.

    채점 스크립트가 (vehicle_id, timestamp)로 조인한다. logger 대신 배치당 한 번 write해
    측정 오버헤드를 줄인다.
    """
    sys.stdout.writelines(
        f"[SCORE] vehicle={d.get('vehicle_id', 'UNKNOWN')} ts={d.get('timestamp', '')} "
        f"score={score:.6f} flag={int(flag)}\n"
        for d, flag, score in zip(batch, flags, scores)
    )


def process(data: dict, producer: KafkaProducer, ml_anomaly: bool = False) -> None:
    """한 메시지에 대해 룰 기반 탐지 + 이벤트 발행을 수행한다.

    ML 예측은 호출자가 파티션 배치 단위로 끝내고 결과만 넘긴다 — sklearn predict는
    호출 횟수가 비용을 좌우한다(ADR-018).
    """
    vehicle_id = data.get("vehicle_id", "UNKNOWN")

    detected = rules.detect(data)

    if ML_ENABLED:
        if ml_anomaly:
            from rules import AnomalyEvent
            detected.append(AnomalyEvent(
                vehicle_id=vehicle_id,
                timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
                anomaly_type="ML 복합 이상 패턴",
                field="multi_field",
                value=0.0,
                threshold="IsolationForest 이상 점수",
                severity="MEDIUM",
                detector="ML",
            ))

    if not detected:
        return

    futures = []
    for event in detected:
        payload = {
            **asdict(event),
            "detected_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        # detected_at을 키에 넣으면 안 된다 — 재처리 때 같은 event_id가 나와야
        # UNIQUE(event_id)가 중복을 막는다(ADR-020).
        event_key ="|".join(str(payload.get(field, "")) for field in (
            "vehicle_id", "timestamp", "anomaly_type", "field", "detector"
        ))
        payload["event_id"] = hashlib.sha256(event_key.encode("utf-8")).hexdigest()

        futures.append(producer.send(OUTPUT_TOPIC, key=vehicle_id, value=payload))

        # 추적 키 이름은 백엔드와 같아야 한다(ADR-028).
        logger.warning(
            f"[이상 감지] vehicle={vehicle_id} "
            f"ts={data.get('timestamp')} "
            f"event={payload['event_id']} "
            f"type={event.anomaly_type} "
            f"field={event.field} "
            f"value={event.value} "
            f"severity={event.severity} "
            f"detector={event.detector}"
        )

        notifier.send_webhook(payload)

    producer.flush()
    # flush()는 실패를 던지지 않는다. future를 확인해야 발행 실패가 DLQ로 간다.
    for future in futures:
        future.get(timeout=10)


def should_commit(handled_since_commit: int, elapsed_seconds: float, force: bool = False) -> bool:
    """개수/시간 중 하나라도 기준을 넘으면 True."""
    if handled_since_commit == 0:
        return False
    return (
        force
        or handled_since_commit >= COMMIT_INTERVAL_MESSAGES
        or elapsed_seconds >= COMMIT_INTERVAL_SECONDS
    )


# 계약 위반은 사유별로 센다 — 사유마다 대응이 다르다(docs/rejection-metrics-design.md).
CONTRACT_REJECTED = {reason: 0 for reason in contract.REASONS}
# 계약 위반이 아닌 실패(구현 버그일 수 있다)는 분리해서 센다.
PROCESSING_FAILED = 0

# 라벨에 차량 ID·payload·예외 메시지를 넣지 않는다 — 카디널리티와 개인정보 보존
# (docs/data-retention.md). 이름·라벨은 Java ContractMetrics와 같아야 한다.
# 재전달되면 다시 오르는 판정 횟수다(고유 메시지 수 아님).
CONTRACT_REJECTED_TOTAL = Counter(
    "telemetry_contract_rejected_attempts",
    "입력 계약 위반으로 거부한 **판정 횟수**(재전달 포함, 고유 메시지 수가 아니다)",
    ["entrance", "reason"],
)
ENTRANCE = "anomaly-detector"

# 시도/성공/실패는 단계가 달라 서로 빼서 다른 뜻을 만들면 안 된다.
DLQ_PUBLISH_ATTEMPTS = Counter(
    "telemetry_kafka_dlq_publish_attempts", "DLQ 발행 **시도** 횟수", ["topic"])
DLQ_PUBLISHED = Counter(
    "telemetry_kafka_dlq_published", "DLQ 발행 **성공을 확인한** 횟수", ["topic"])
DLQ_PUBLISH_FAILURES = Counter(
    "telemetry_kafka_dlq_publish_failures", "DLQ 발행 **실패·timeout을 관찰한** 횟수", ["topic"])
# failures 중 timeout으로 판별한 부분집합. 브로커가 받았는지 모르는 건이다.
# 0이어도 나머지 실패가 "미발행 확정"이라는 뜻은 아니다.
DLQ_PUBLISH_INDETERMINATE = Counter(
    "telemetry_kafka_dlq_publish_indeterminate",
    "DLQ 발행 실패 중 **발행 여부 불명**(timeout)", ["topic"])
PROCESSING_FAILED_TOTAL = Counter(
    "telemetry_anomaly_processing_failed_total",
    "계약을 통과했으나 처리(룰 판정·알림 발행)에서 실패한 레코드 수",
)
# 선택 필드(ADR-030)가 없어 ML만 건너뛴 건. 처리량과 같이 오르면 그 차량들은 ML이 꺼진 셈이다.
ML_SKIPPED_MISSING_FEATURES = Counter(
    "telemetry_anomaly_ml_skipped_missing_features_total",
    "ML 피처(연료량·전압 등)가 없어 ML 채점을 건너뛴 레코드 수 — 룰 판정은 수행됨",
)
PROCESSED_TOTAL = Counter(
    "telemetry_anomaly_processed_total",
    "계약을 통과해 룰 판정까지 끝낸 레코드 수",
)


def _metrics_snapshot() -> str:
    parts = [f"{r}={CONTRACT_REJECTED[r]}" for r in contract.REASONS
             if CONTRACT_REJECTED[r]]
    if PROCESSING_FAILED:
        parts.append(f"처리실패={PROCESSING_FAILED}")
    return " ".join(parts) if parts else "없음"


def dlq_headers(message, cause: Exception | None) -> list[tuple[str, bytes]]:
    """DLQ 레코드에 붙일 실패 원인 헤더.

    헤더 이름은 Java TelemetryConsumer.sendToDlq()와 같아야 한다 — dlq.py가 두 경로를
    같이 읽고, 이 헤더로 재처리 가능 여부를 가른다(없으면 DLQ→원본 무한 루프).
    """
    headers = [
        ("x-dlq-origin-topic", INPUT_TOPIC.encode("utf-8")),
        ("x-dlq-origin-partition", str(message.partition).encode("utf-8")),
        ("x-dlq-origin-offset", str(message.offset).encode("utf-8")),
        ("x-dlq-failed-at", datetime.now(timezone.utc).isoformat().encode("utf-8")),
    ]
    # replay-count는 이어받아야 한다 — 안 그러면 실패마다 0이 되어 --max-replays가 멈추지 못한다.
    # 나머지 x-dlq-* 헤더는 이번 실패 기준으로 새로 쓴다. 경위: load-test/dlq-replay-count.
    carried = None
    for key, value in (getattr(message, "headers", None) or []):
        if key == "x-dlq-replay-count":
            carried = value          # Java lastHeader처럼 마지막 값
    if carried is not None:
        headers.append(("x-dlq-replay-count", carried))
    if cause is not None:
        headers.append(("x-dlq-failure-class", type(cause).__name__.encode("utf-8")))
        # payload 조각이 섞일 수 있고 헤더도 메시지 크기 제한에 포함되어 자른다.
        text = str(cause)
        if len(text) > 512:
            text = text[:512] + "...(truncated)"
        headers.append(("x-dlq-failure-message", text.encode("utf-8")))
        # ContractViolation은 사유가 넷이라 예외 이름만으로는 재처리 판단이 안 된다.
        if isinstance(cause, contract.ContractViolation):
            headers.append(("x-dlq-contract-reason", cause.reason.encode("utf-8")))
    # 재처리 도구가 토픽 이름에 기대지 않도록 격리 경로를 헤더로도 남긴다.
    headers.append(("x-dlq-source-path", b"anomaly-detector"))
    return headers


def _is_indeterminate(error: BaseException) -> bool:
    """브로커 도달 여부가 불확실한 timeout인가.

    Java ContractMetrics.isIndeterminate와 같은 판단이어야 한다. 아는 예외 이름만 보므로
    False가 "미발행 확정"을 뜻하지는 않는다.
    """
    seen = set()
    cur: BaseException | None = error
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if type(cur).__name__ in ("KafkaTimeoutError", "TimeoutError", "RequestTimedOutError"):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def send_to_dlq(dlq_producer: KafkaProducer, message, cause: Exception | None = None) -> None:
    """원본 바이트 그대로 DLQ에 옮기고 실패 원인은 헤더로 남긴다."""
    try:
        DLQ_PUBLISH_ATTEMPTS.labels(topic=DLQ_TOPIC).inc()
        future = dlq_producer.send(DLQ_TOPIC, key=message.key, value=message.value,
                                   headers=dlq_headers(message, cause))
        dlq_producer.flush()
        future.get(timeout=10)
        DLQ_PUBLISHED.labels(topic=DLQ_TOPIC).inc()
    except Exception as send_error:
        DLQ_PUBLISH_FAILURES.labels(topic=DLQ_TOPIC).inc()
        if _is_indeterminate(send_error):
            DLQ_PUBLISH_INDETERMINATE.labels(topic=DLQ_TOPIC).inc()
        # 반드시 다시 던진다 — 삼키면 이 offset이 처리 완료로 커밋된다.
        logger.error(
            f"[DLQ] {DLQ_TOPIC} 전송 실패 — 원본 offset을 커밋하지 않음 "
            f"partition={message.partition} offset={message.offset}",
            exc_info=True,
        )
        raise


def main() -> None:
    logger.info("=" * 60)
    logger.info("  Vehicle Anomaly Detector 시작")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  ML 이상 감지: {'활성화' if ML_ENABLED else '비활성화'}")
    if ML_ENABLED:
        logger.info(f"  ML 최소 학습 샘플: {ML_MIN_SAMPLES}개")
        logger.info(f"  ML 학습 윈도우: {ML_WINDOW_SIZE}개")
        logger.info(
            "  ML 판정 기준: "
            + (f"점수 임계값 {ML_SCORE_THRESHOLD}" if ML_SCORE_THRESHOLD is not None
               else "모델 offset_ (= contamination 비율만큼 항상 알림)")
        )
        if ML_SCORE_DUMP:
            logger.warning(
                "  [ML] 점수 덤프 ON — 메시지마다 [SCORE] 줄을 남깁니다. "
                "측정 전용이며 처리량이 떨어집니다(ML_SCORE_DUMP=false로 끄세요)."
            )
    logger.info("=" * 60)

    running = True

    def handle_signal(signum, frame):
        nonlocal running
        logger.info("종료 신호 수신 — 종료 중...")
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # 지표 서버가 실패해도 감지는 계속한다 — 계측이 처리를 멈추게 하면 안 된다.
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"  지표: :{METRICS_PORT}/metrics")
    except Exception:
        logger.error("지표 서버 기동 실패 — 감지는 계속한다", exc_info=True)

    redis_client = make_redis_client()
    ml_detectors = PartitionedMLDetectors(
        redis_client, ML_MIN_SAMPLES, ML_WINDOW_SIZE, ML_SCORE_THRESHOLD
    )
    consumer = make_consumer()
    producer = make_producer()
    global PROCESSING_FAILED
    dlq_producer = make_dlq_producer()

    processed = 0
    dlq_count = 0
    handled_since_commit = 0
    last_commit_time = time.time()
    safe_offsets: dict[TopicPartition, OffsetAndMetadata] = {}

    def maybe_commit(force: bool = False) -> None:
        nonlocal handled_since_commit, last_commit_time
        elapsed = time.time() - last_commit_time
        if should_commit(handled_since_commit, elapsed, force=force):
            # 인자 없는 commit()은 DLQ 실패 건의 position까지 넘긴다 — 처리 완료로 표시한 offset만 커밋한다.
            consumer.commit(offsets=dict(safe_offsets))
            safe_offsets.clear()
            if ML_ENABLED:
                ml_detectors.save_all()
            handled_since_commit = 0
            last_commit_time = time.time()

    try:
        while running:
            # ML 예측을 파티션 배치당 1회로 묶는다. 학습 상태가 파티션별이라 배치도 파티션 단위다(ADR-018).
            records = consumer.poll(timeout_ms=1000)
            if not records:
                maybe_commit()
                continue

            for topic_partition, messages in records.items():
                if not running:
                    break

                # 1) 계약 검증. 저장 경로와 같은 계약으로 감지 전에 거부한다 — 필드가 빠지면
                # 해당 룰이 조용히 건너뛰어지기 때문이다. 경위: docs/anomaly-path-contract.md.
                parsed: list[tuple] = []
                for message in messages:
                    try:
                        parsed.append((message, contract.validate(message.value.decode("utf-8"))))
                    except contract.ContractViolation as e:
                        CONTRACT_REJECTED[e.reason] += 1
                        CONTRACT_REJECTED_TOTAL.labels(entrance=ENTRANCE, reason=e.reason).inc()
                        # 값은 남기지 않는다. SHA-256은 DLQ value와 대조용이다.
                        logger.warning(
                            f"[계약 위반] {e.reason} — DLQ로 이동 "
                            f"vehicle={message.key.decode('utf-8', 'replace') if message.key else '-'} "
                            f"partition={message.partition} offset={message.offset} "
                            f"payloadSha256={hashlib.sha256(message.value).hexdigest()} "
                            f"detail={e.detail}")
                        send_to_dlq(dlq_producer, message, e)
                        dlq_count += 1
                        safe_offsets[TopicPartition(message.topic, message.partition)] =                             OffsetAndMetadata(message.offset + 1, "")
                        handled_since_commit += 1
                    except Exception as e:
                        # 계약 위반이 아니면 구현 버그일 수 있다 — 분리해서 세고,
                        # dlq.py도 영구 실패로 분류하지 않는다(unknown).
                        PROCESSING_FAILED += 1
                        PROCESSING_FAILED_TOTAL.inc()
                        logger.error(
                            f"검증 중 예상 못 한 실패 — DLQ로 이동 "
                            f"partition={message.partition} offset={message.offset} "
                            f"payloadSha256={hashlib.sha256(message.value).hexdigest()}: {e}",
                            exc_info=True,
                        )
                        send_to_dlq(dlq_producer, message, e)
                        dlq_count += 1
                        safe_offsets[TopicPartition(message.topic, message.partition)] = \
                            OffsetAndMetadata(message.offset + 1, "")
                        handled_since_commit += 1

                # 2) ML 예측: 파티션 배치당 1회
                ml_flags = [False] * len(parsed)
                if ML_ENABLED and parsed:
                    try:
                        detector = ml_detectors.get(topic_partition.partition)
                        batch_data = [data for _, data in parsed]
                        # 피처가 빠진 레코드는 0으로 채우지 않고 건너뛴다(ADR-030).
                        skipped = sum(1 for d in batch_data if missing_features(d))
                        if skipped:
                            ML_SKIPPED_MISSING_FEATURES.inc(skipped)
                        if ML_SCORE_DUMP:
                            ml_flags, ml_scores = detector.update_batch_with_scores(batch_data)
                            dump_scores(batch_data, ml_flags, ml_scores)
                        else:
                            ml_flags = detector.update_batch(batch_data)
                    except Exception:
                        # ML은 보조 탐지다. 배치를 DLQ로 보내지 않고 ML 판정만 포기한다.
                        logger.error(
                            f"[ML] 배치 예측 실패 — partition={topic_partition.partition} "
                            f"{len(parsed)}건은 룰 기반으로만 판정합니다",
                            exc_info=True,
                        )
                        ml_flags = [False] * len(parsed)

                # 3) 룰 판정 + 발행. 실패는 메시지 단위로 DLQ
                for (message, data), ml_flag in zip(parsed, ml_flags):
                    try:
                        process(data, producer, ml_flag)
                        processed += 1
                        PROCESSED_TOTAL.inc()
                        if processed % 1000 == 0:
                            logger.info(f"처리 누적: {processed}건 "
                                        f"(계약 거부 사유별: {_metrics_snapshot()})")
                    except Exception as e:
                        # 계약을 통과한 payload다 — 발행 실패나 버그이므로 계약 위반과 섞지 않는다.
                        PROCESSING_FAILED += 1
                        PROCESSING_FAILED_TOTAL.inc()
                        logger.error(
                            f"메시지 처리 실패 — DLQ로 이동 "
                            f"vehicle={data.get('vehicle_id')} ts={data.get('timestamp')} "
                            f"partition={message.partition} offset={message.offset}: {e}",
                            exc_info=True,
                        )
                        send_to_dlq(dlq_producer, message, e)
                        dlq_count += 1

                    # 여기까지 오면 발행 또는 DLQ 발행이 성공한 것이라 커밋 대상이다.
                    # (send_to_dlq가 실패하면 예외로 이 줄에 오지 않는다.)
                    safe_offsets[TopicPartition(message.topic, message.partition)] = \
                        OffsetAndMetadata(message.offset + 1, "")
                    handled_since_commit += 1
                    maybe_commit()

            maybe_commit()
    finally:
        maybe_commit(force=True)
        consumer.close()
        producer.close()
        dlq_producer.close()
        logger.info(f"종료 완료 (총 처리: {processed}건, DLQ 격리: {dlq_count}건, "
                    f"사유별: {_metrics_snapshot()})")


if __name__ == "__main__":
    main()
