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
"""
import os
import json
import signal
import logging
from dataclasses import asdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
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
logger = logging.getLogger("anomaly-detector")

# ── 설정 ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC             = "vehicle-telemetry"
OUTPUT_TOPIC            = "vehicle-anomaly-alerts"
ML_ENABLED              = os.getenv("ML_ENABLED", "false").lower() == "true"
ML_MIN_SAMPLES          = int(os.getenv("ML_MIN_SAMPLES", "200"))


def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="anomaly-detector-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
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
    for event in detected:
        payload = {
            **asdict(event),
            "detected_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        producer.send(OUTPUT_TOPIC, key=vehicle_id, value=payload)

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

    processed = 0
    try:
        while running:
            for message in consumer:
                if not running:
                    break
                try:
                    process(message.value, producer, ml)
                    processed += 1
                    if processed % 100 == 0:
                        logger.info(f"처리 누적: {processed}건")
                except Exception as e:
                    logger.error(f"메시지 처리 오류: {e}", exc_info=True)
    finally:
        consumer.close()
        producer.close()
        logger.info(f"종료 완료 (총 처리: {processed}건)")


if __name__ == "__main__":
    main()
