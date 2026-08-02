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
import time
import logging
from dataclasses import asdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
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


def process(data: dict, producer: KafkaProducer, ml: MLAnomalyDetector) -> None:
    vehicle_id = data.get("vehicle_id", "UNKNOWN")

    # ── 1. 룰 기반 이상 감지 ────────────────────────────────────
    detected = rules.detect(data)

    # ── 2. ML 기반 이상 감지 ────────────────────────────────────
    if ML_ENABLED:
        is_ml_anomaly = ml.update(data)
        if is_ml_anomaly:
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
        dlq_producer.send(DLQ_TOPIC, key=message.key, value=message.value)
        dlq_producer.flush()
    except Exception:
        # DLQ 전송조차 실패하면 이 메시지는 정말로 유실된다 — 다른 로그와 구분되게 남긴다.
        logger.error(
            f"[DLQ] {DLQ_TOPIC} 전송조차 실패 — 메시지 완전 유실 "
            f"partition={message.partition} offset={message.offset}",
            exc_info=True,
        )


def main() -> None:
    logger.info("=" * 60)
    logger.info("  Vehicle Anomaly Detector 시작")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  ML 이상 감지: {'활성화' if ML_ENABLED else '비활성화'}")
    if ML_ENABLED:
        logger.info(f"  ML 최소 학습 샘플: {ML_MIN_SAMPLES}개")
    logger.info("=" * 60)

    running = True

    def handle_signal(signum, frame):
        nonlocal running
        logger.info("종료 신호 수신 — 종료 중...")
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    ml = MLAnomalyDetector(min_samples=ML_MIN_SAMPLES)
    consumer = make_consumer()
    producer = make_producer()
    dlq_producer = make_dlq_producer()

    processed = 0
    dlq_count = 0
    handled_since_commit = 0
    last_commit_time = time.time()

    def maybe_commit(force: bool = False) -> None:
        nonlocal handled_since_commit, last_commit_time
        elapsed = time.time() - last_commit_time
        if should_commit(handled_since_commit, elapsed, force=force):
            consumer.commit()
            handled_since_commit = 0
            last_commit_time = time.time()

    try:
        while running:
            for message in consumer:
                if not running:
                    break
                try:
                    data = json.loads(message.value.decode("utf-8"))
                    process(data, producer, ml)
                    processed += 1
                    if processed % 100 == 0:
                        logger.info(f"처리 누적: {processed}건")
                except Exception as e:
                    logger.error(
                        f"메시지 처리 실패 — DLQ로 이동 "
                        f"partition={message.partition} offset={message.offset}: {e}",
                        exc_info=True,
                    )
                    send_to_dlq(dlq_producer, message)
                    dlq_count += 1

                # 성공이든 DLQ로 격리했든 이 메시지는 "처리 완료"다 — 재시도 없이 다음
                # 메시지로 넘어가되, 실패분은 원본이 DLQ에 남아 나중에 조사/재처리할 수 있다.
                handled_since_commit += 1
                maybe_commit()

            # consumer_timeout_ms로 idle 상태가 되어 for 루프를 빠져나온 시점에도 커밋한다.
            maybe_commit()
    finally:
        maybe_commit(force=True)
        consumer.close()
        producer.close()
        dlq_producer.close()
        logger.info(f"종료 완료 (총 처리: {processed}건, DLQ 격리: {dlq_count}건)")


if __name__ == "__main__":
    main()
