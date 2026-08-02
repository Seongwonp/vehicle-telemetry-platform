"""
anomaly_detector.py 신뢰성 로직 테스트 — 배치 커밋 판단, DLQ 격리, 발행 실패 전파.

실제 Kafka 연결 없이 순수 함수(should_commit)와 KafkaProducer를 흉내 낸 가짜 객체로만
검증한다 (make_consumer/make_producer 자체는 브로커가 있어야 해서 테스트 대상이 아니다).
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from anomaly_detector import should_commit, send_to_dlq, process, DLQ_TOPIC
from ml_detector import MLAnomalyDetector


def make_data(**kwargs) -> dict:
    """정상 차량 데이터 기본값 — 필요한 필드만 오버라이드 (test_rules.py와 동일 패턴)"""
    base = {
        "vehicle_id": "TEST-001",
        "timestamp": "2026-03-01T10:00:00Z",
        "speed": 80.0,
        "rpm": 2000,
        "engine_temp": 90.0,
        "battery_voltage": 13.8,
        "fuel_level": 60.0,
        "dtc_codes": [],
    }
    base.update(kwargs)
    return base


class FakeFuture:
    def __init__(self, exc: Exception | None = None):
        self._exc = exc

    def get(self, timeout=None):
        if self._exc:
            raise self._exc
        return object()


class FakeProducer:
    """producer.send()가 반환하는 future를 미리 정해둔 것만 내보내는 가짜 프로듀서."""

    def __init__(self, send_exc=None, future_exc=None):
        self.sent = []
        self.flushed = False
        self._send_exc = send_exc
        self._future_exc = future_exc

    def send(self, topic, key=None, value=None):
        if self._send_exc:
            raise self._send_exc
        self.sent.append((topic, key, value))
        return FakeFuture(self._future_exc)

    def flush(self):
        self.flushed = True


class FakeMessage:
    def __init__(self, key=b"TEST-001", value=b'{"broken": true', partition=0, offset=42):
        self.key = key
        self.value = value
        self.partition = partition
        self.offset = offset


# ── should_commit: 배치 커밋 판단 (순수 함수) ─────────────────────

class TestShouldCommit:

    def test_처리된게_없으면_커밋안함(self):
        assert should_commit(handled_since_commit=0, elapsed_seconds=999) is False

    def test_개수_기준_미달_시간도_미달이면_커밋안함(self):
        assert should_commit(handled_since_commit=1, elapsed_seconds=0.1) is False

    def test_개수_기준_도달하면_커밋(self):
        assert should_commit(handled_since_commit=100, elapsed_seconds=0.1) is True

    def test_시간_기준_도달하면_커밋(self):
        assert should_commit(handled_since_commit=1, elapsed_seconds=5.0) is True

    def test_force면_처리된게_있으면_무조건_커밋(self):
        assert should_commit(handled_since_commit=1, elapsed_seconds=0.0, force=True) is True

    def test_force여도_처리된게_없으면_커밋안함(self):
        # force는 "지금까지 처리한 걸 커밋해라"는 뜻이지, 처리한 게 없는데 억지로 커밋하라는 뜻이 아니다.
        assert should_commit(handled_since_commit=0, elapsed_seconds=0.0, force=True) is False


# ── send_to_dlq: 실패 메시지 격리 ──────────────────────────────────

class TestSendToDlq:

    def test_원본_바이트를_그대로_DLQ로_전송(self):
        producer = FakeProducer()
        msg = FakeMessage(key=b"SIM-001", value=b'{"not":"valid json"')

        send_to_dlq(producer, msg)

        assert producer.sent == [(DLQ_TOPIC, b"SIM-001", b'{"not":"valid json"')]
        assert producer.flushed is True

    def test_DLQ_전송자체가_실패해도_예외를_밖으로_던지지_않음(self):
        producer = FakeProducer(send_exc=RuntimeError("broker down"))
        msg = FakeMessage()

        # 여기서 예외가 올라오면 메인 루프 전체가 죽는다 — 로그만 남기고 조용히 삼켜야 한다.
        send_to_dlq(producer, msg)


# ── process(): 발행 실패가 호출자에게 전파되는지 ───────────────────

class TestProcessPropagatesPublishFailure:

    def test_이상없으면_producer_호출안함(self):
        producer = FakeProducer()
        ml = MLAnomalyDetector(min_samples=1_000_000)  # ML 절대 안 켜지게

        process(make_data(), producer, ml)

        assert producer.sent == []

    def test_이상감지시_정상_발행되면_예외없음(self):
        producer = FakeProducer()
        ml = MLAnomalyDetector(min_samples=1_000_000)

        process(make_data(engine_temp=999.0), producer, ml)

        assert len(producer.sent) == 1
        assert producer.flushed is True

    def test_발행_future가_실패하면_process가_예외를_전파(self):
        # producer.send() 자체는 성공(버퍼에 큐잉)하지만, 나중에 브로커 응답에서 실패한
        # 경우를 흉내낸다 — future.get()이 예외를 던진다.
        producer = FakeProducer(future_exc=RuntimeError("send failed"))
        ml = MLAnomalyDetector(min_samples=1_000_000)

        with pytest.raises(RuntimeError):
            process(make_data(engine_temp=999.0), producer, ml)
