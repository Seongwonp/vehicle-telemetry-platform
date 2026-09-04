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

신뢰성: 메시지 처리(또는 발행)가 실패하면 원본을 vehicle-telemetry-anomaly-dlq로 격리하고,
성공/DLQ 어느 쪽이든 "처리 완료"로 취급한 뒤에만 오프셋을 커밋한다. auto-commit을 쓰면
처리 도중 예외가 나도 주기적으로 오프셋이 넘어가 실패한 메시지가 조용히 유실된다 —
이 문제를 막기 위해 수동 커밋으로 바꿨다 (자세한 이유는 ADR-017 참고).
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
from dotenv import load_dotenv

import rules
import notifier
from ml_detector import MLAnomalyDetector

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
# 컨테이너 hostname을 로거 이름에 포함 — 인스턴스를 여러 개(--scale) 띄웠을 때
# 로그에서 어느 인스턴스가 처리했는지 구분하기 위함.
logger = logging.getLogger(f"anomaly-detector[{socket.gethostname()}]")

# ── 설정 ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC             = "vehicle-telemetry"
OUTPUT_TOPIC            = "vehicle-anomaly-alerts"
DLQ_TOPIC               = "vehicle-telemetry-anomaly-dlq"
ML_ENABLED              = os.getenv("ML_ENABLED", "false").lower() == "true"
ML_MIN_SAMPLES          = int(os.getenv("ML_MIN_SAMPLES", "200"))
# 학습 윈도우 크기(건수). 이 값이 시간으로 몇 초를 덮는지는 처리량에 따라 달라진다 —
# 파티션당 약 500 msg/s에서 2000건은 약 4초치다. 재학습 간격(60초)보다 훨씬 짧으면
# "4초를 배워 60초를 채점"하는 셈이 되므로, 측정해서 정할 수 있게 밖으로 뺐다.
ML_WINDOW_SIZE          = int(os.getenv("ML_WINDOW_SIZE", "2000"))
# 이상 판정 점수 임계값. 비워두면 모델의 offset_을 쓰고, 그러면 데이터와 무관하게
# 항상 contamination 비율(5%)만큼 알림이 뜬다. 값을 주면 알림 수가 실제 데이터에
# 따라 움직인다 — 값은 정상 구간 데이터로 score_ml.py --sweep을 돌려 정한다(ADR-018).
_threshold_raw          = os.getenv("ML_SCORE_THRESHOLD", "").strip()
ML_SCORE_THRESHOLD      = float(_threshold_raw) if _threshold_raw else None
# 측정 전용: 모든 메시지의 ML 이상 점수를 stdout에 남긴다(기본 off).
# 켜면 메시지 1건당 한 줄이 나가므로 처리량이 떨어진다 — 탐지 품질을 재는 용도이지
# 상시 운영용이 아니다. score_ml.py --sweep이 이 줄을 읽어 임계값별 recall/오탐을 낸다.
ML_SCORE_DUMP           = os.getenv("ML_SCORE_DUMP", "false").lower() == "true"

# 커밋을 메시지마다 하면(동기 왕복) 부하 테스트로 이미 빠듯한 것을 확인한 처리량에
# 부담을 더한다 — 개수/시간 중 먼저 차는 조건으로 배치 커밋한다.
COMMIT_INTERVAL_MESSAGES = 100
COMMIT_INTERVAL_SECONDS  = 5.0


def make_consumer() -> KafkaConsumer:
    # value_deserializer를 두지 않고 원본 바이트를 그대로 받는다 — JSON 파싱 실패도
    # 우리가 직접 잡아서 DLQ로 보내기 위함(Java TelemetryConsumer와 동일한 방식:
    # 역직렬화는 컨슈머가 아니라 애플리케이션 코드가 하고, 실패하면 원본 바이트를 보존한다).
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
    """ML이 꺼져 있으면(기본값) Redis 연결 자체를 만들지 않는다 — 기본 동작에 새
    의존성/실패 지점을 추가하지 않기 위함."""
    if not ML_ENABLED:
        return None
    import redis  # 지연 임포트 — ML 비활성화 시 이 모듈을 아예 건드리지 않는다.
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        password=os.getenv("REDIS_PASSWORD") or None,
    )


def ml_state_key(partition: int) -> str:
    return f"anomaly-detector:ml-model:{INPUT_TOPIC}:{partition}"


class PartitionedMLDetectors:
    """파티션마다 별도 MLAnomalyDetector를 둔다.

    vehicle_id가 Kafka 파티션 키(ADR-003)라 파티션마다 담당하는 차량 집합이 사실상
    고정적이다 — 그래서 "어느 컨테이너/인스턴스가 떴는지"가 아니라 "어느 파티션인지"를
    기준으로 학습 상태를 나누는 게 훨씬 안정적이다. Redis에 파티션 ID로 저장해두면,
    인스턴스가 재시작되거나 리밸런싱으로 다른 인스턴스가 그 파티션을 넘겨받아도 학습
    상태를 이어받을 수 있다(ADR-018).

    한계: 파티션이 이 프로세스에서 빠져나가는(리밸런싱으로 다른 인스턴스에 넘어가는)
    시점을 별도로 감지하지 않는다 — 마지막 저장 이후 몇 초치 학습분은 넘겨받는 쪽이
    못 받을 수 있다(저장 주기는 커밋 주기와 같음, 기본 100건/5초). 완벽한 정합성이
    필요하면 Kafka Consumer Rebalance Listener로 파티션 반납 시점에 즉시 저장해야
    하는데, 이번 범위에서는 하지 않았다.
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
                continue  # 아직 학습 전이면 저장해봐야 복원할 때 다시 처음부터 모으므로 의미 없다.
            try:
                self._redis.set(ml_state_key(partition), detector.get_state())
            except Exception:
                logger.warning(f"[ML] 파티션 {partition} 학습 상태 저장 실패", exc_info=True)


def dump_scores(batch: list[dict], flags: list[bool], scores: list[float]) -> None:
    """ML_SCORE_DUMP가 켜졌을 때 배치 전체의 이상 점수를 한 번에 stdout으로 흘린다.

    채점 스크립트가 정답 로그와 같은 키 (vehicle_id, timestamp)로 조인할 수 있게
    그 두 값을 그대로 쓴다. 줄마다 logger를 부르면 포맷팅 비용이 메시지 수만큼 붙어
    측정 자체가 느려지므로, 배치당 한 번만 write한다.
    """
    sys.stdout.writelines(
        f"[SCORE] vehicle={d.get('vehicle_id', 'UNKNOWN')} ts={d.get('timestamp', '')} "
        f"score={score:.6f} flag={int(flag)}\n"
        for d, flag, score in zip(batch, flags, scores)
    )


def process(data: dict, producer: KafkaProducer, ml_anomaly: bool = False) -> None:
    """한 메시지에 대해 룰 기반 탐지 + 이벤트 발행을 수행한다.

    ML 예측 결과는 `ml_anomaly`로 받는다 — 예측 자체는 호출자가 파티션 배치 단위로
    한 번에 수행한다(ml_detector.update_batch). sklearn predict는 호출 횟수가 비용을
    지배해서, 메시지마다 부르면 처리량이 약 25배 떨어진다(ADR-018).
    """
    vehicle_id = data.get("vehicle_id", "UNKNOWN")

    # ── 1. 룰 기반 이상 감지 ────────────────────────────────────
    detected = rules.detect(data)

    # ── 2. ML 기반 이상 감지 (예측은 배치로 이미 끝났다) ─────────
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

    # ── 3. Kafka + Webhook 발행 ─────────────────────────────────
    futures = []
    for event in detected:
        payload = {
            **asdict(event),
            "detected_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        event_key = "|".join(str(payload.get(field, "")) for field in (
            "vehicle_id", "timestamp", "anomaly_type", "field", "detector"
        ))
        payload["event_id"] = hashlib.sha256(event_key.encode("utf-8")).hexdigest()

        futures.append(producer.send(OUTPUT_TOPIC, key=vehicle_id, value=payload))

        logger.warning(
            f"[이상 감지] vehicle={vehicle_id} "
            f"type={event.anomaly_type} "
            f"field={event.field} "
            f"value={event.value} "
            f"severity={event.severity} "
            f"detector={event.detector}"
        )

        notifier.send_webhook(payload)

    producer.flush()
    # flush()는 전송 완료까지 기다리지만 실패를 예외로 다시 던지진 않는다 — 각 future를
    # 확인해야 발행 실패(예: 브로커 다운)를 이 메시지의 처리 실패로 잡아 DLQ로 보낼 수 있다.
    for future in futures:
        future.get(timeout=10)


def should_commit(handled_since_commit: int, elapsed_seconds: float, force: bool = False) -> bool:
    """개수/시간 중 하나라도 기준을 넘으면 커밋할 시점이라고 판단한다(순수 함수, 테스트용으로 분리)."""
    if handled_since_commit == 0:
        return False
    return (
        force
        or handled_since_commit >= COMMIT_INTERVAL_MESSAGES
        or elapsed_seconds >= COMMIT_INTERVAL_SECONDS
    )


def send_to_dlq(dlq_producer: KafkaProducer, message) -> None:
    """처리 실패한 원본 메시지를 DLQ로 옮긴다. key/value를 원본 바이트 그대로 보존한다."""
    try:
        future = dlq_producer.send(DLQ_TOPIC, key=message.key, value=message.value)
        dlq_producer.flush()
        future.get(timeout=10)
    except Exception:
        # 이 예외를 메인 루프 밖으로 전파해야 현재 offset이 처리 완료로 집계되지 않는다.
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

    redis_client = make_redis_client()
    ml_detectors = PartitionedMLDetectors(
        redis_client, ML_MIN_SAMPLES, ML_WINDOW_SIZE, ML_SCORE_THRESHOLD
    )
    consumer = make_consumer()
    producer = make_producer()
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
            # consumer.commit()에 인자를 생략하면 DLQ 전송에 실패한 현재 position까지
            # 커밋될 수 있다. 성공/DLQ 성공으로 표시한 offset만 명시적으로 커밋한다.
            consumer.commit(offsets=dict(safe_offsets))
            safe_offsets.clear()
            if ML_ENABLED:
                ml_detectors.save_all()
            handled_since_commit = 0
            last_commit_time = time.time()

    try:
        while running:
            # poll()로 파티션별 묶음을 받는다. ML 예측을 파티션 배치당 1회로 묶기
            # 위해서다 — 파티션마다 학습 상태가 다르므로(ADR-018) 배치도 파티션 단위여야
            # 한다. 메시지별 DLQ 격리와 커밋 정책은 그대로 유지한다.
            records = consumer.poll(timeout_ms=1000)
            if not records:
                maybe_commit()
                continue

            for topic_partition, messages in records.items():
                if not running:
                    break

                # ── 1) 역직렬화: 실패는 메시지 단위로 DLQ 격리 ──────────────
                parsed: list[tuple] = []
                for message in messages:
                    try:
                        parsed.append((message, json.loads(message.value.decode("utf-8"))))
                    except Exception as e:
                        logger.error(
                            f"역직렬화 실패 — DLQ로 이동 "
                            f"partition={message.partition} offset={message.offset}: {e}",
                            exc_info=True,
                        )
                        send_to_dlq(dlq_producer, message)
                        dlq_count += 1
                        safe_offsets[TopicPartition(message.topic, message.partition)] = \
                            OffsetAndMetadata(message.offset + 1, "")
                        handled_since_commit += 1

                # ── 2) ML 예측: 이 파티션 배치에 대해 단 1회 ────────────────
                ml_flags = [False] * len(parsed)
                if ML_ENABLED and parsed:
                    try:
                        detector = ml_detectors.get(topic_partition.partition)
                        batch_data = [data for _, data in parsed]
                        if ML_SCORE_DUMP:
                            ml_flags, ml_scores = detector.update_batch_with_scores(batch_data)
                            dump_scores(batch_data, ml_flags, ml_scores)
                        else:
                            ml_flags = detector.update_batch(batch_data)
                    except Exception:
                        # ML은 룰 위에 얹는 보조 탐지다. 여기서 배치 전체를 DLQ로 보내면
                        # sklearn 문제 하나로 정상 메시지 수백 건이 격리된다. 룰 기반
                        # 탐지는 그대로 살리고 이 배치의 ML 판정만 포기하되,
                        # 조용히 넘어가지 않도록 ERROR로 남긴다.
                        logger.error(
                            f"[ML] 배치 예측 실패 — partition={topic_partition.partition} "
                            f"{len(parsed)}건은 룰 기반으로만 판정합니다",
                            exc_info=True,
                        )
                        ml_flags = [False] * len(parsed)

                # ── 3) 룰 판정 + 이벤트 발행: 실패는 메시지 단위로 DLQ ──────
                for (message, data), ml_flag in zip(parsed, ml_flags):
                    try:
                        process(data, producer, ml_flag)
                        processed += 1
                        if processed % 1000 == 0:
                            logger.info(f"처리 누적: {processed}건")
                    except Exception as e:
                        logger.error(
                            f"메시지 처리 실패 — DLQ로 이동 "
                            f"partition={message.partition} offset={message.offset}: {e}",
                            exc_info=True,
                        )
                        send_to_dlq(dlq_producer, message)
                        dlq_count += 1

                    # 성공이든 DLQ로 격리했든 "처리 완료"다 — 재시도 없이 넘어가되,
                    # 실패분은 원본이 DLQ에 남아 나중에 조사/재처리할 수 있다.
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
        logger.info(f"종료 완료 (총 처리: {processed}건, DLQ 격리: {dlq_count}건)")


if __name__ == "__main__":
    main()
