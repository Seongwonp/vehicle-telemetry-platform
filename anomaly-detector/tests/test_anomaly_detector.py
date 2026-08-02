"""
anomaly_detector.py 신뢰성 로직 테스트 — 배치 커밋 판단, DLQ 격리, 발행 실패 전파.

실제 Kafka 연결 없이 순수 함수(should_commit)와 KafkaProducer를 흉내 낸 가짜 객체로만
검증한다 (make_consumer/make_producer 자체는 브로커가 있어야 해서 테스트 대상이 아니다).
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from anomaly_detector import should_commit, send_to_dlq, process, DLQ_TOPIC, PartitionedMLDetectors, ml_state_key
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


class FakeRedis:
    """실제 redis-py 클라이언트 대신 dict로 흉내낸 가짜 — get/set만 흉내내면 충분하다."""

    def __init__(self, fail_on: set[str] = frozenset()):
        self.store: dict[str, bytes] = {}
        self._fail_on = fail_on  # 이 키로 get/set 호출 시 일부러 예외를 던져 장애를 흉내낸다.

    def get(self, key):
        if key in self._fail_on:
            raise ConnectionError("redis down")
        return self.store.get(key)

    def set(self, key, value):
        if key in self._fail_on:
            raise ConnectionError("redis down")
        self.store[key] = value


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


# ── PartitionedMLDetectors: 파티션별 ML 상태 저장/복원 (ADR-018) ──

def make_features(seed: int = 0) -> dict:
    import random
    random.seed(seed)
    return {
        "speed": random.uniform(60.0, 120.0),
        "rpm": random.randint(1500, 3500),
        "engine_temp": random.uniform(85.0, 98.0),
        "battery_voltage": random.uniform(13.5, 14.2),
        "fuel_level": random.uniform(30.0, 80.0),
    }


class TestPartitionedMLDetectors:

    def test_redis_없으면_파티션마다_새_감지기_생성(self):
        pool = PartitionedMLDetectors(redis_client=None, min_samples=30)

        d0 = pool.get(0)
        d1 = pool.get(1)

        assert d0 is not d1  # 파티션마다 독립된 인스턴스여야 한다.
        assert pool.get(0) is d0  # 같은 파티션은 같은 인스턴스를 재사용한다.

    def test_같은_파티션_재조회시_같은_인스턴스_반환(self):
        redis_client = FakeRedis()
        pool = PartitionedMLDetectors(redis_client, min_samples=30)

        first = pool.get(2)
        second = pool.get(2)

        assert first is second

    def test_save_all_이후_새_pool에서_복원됨(self):
        redis_client = FakeRedis()
        pool = PartitionedMLDetectors(redis_client, min_samples=30)

        detector = pool.get(0)
        for i in range(30):
            detector.update(make_features(seed=i))
        assert detector.is_trained is True

        pool.save_all()
        assert ml_state_key(0) in redis_client.store

        # 재시작(또는 리밸런싱으로 다른 인스턴스가 파티션을 넘겨받음)을 흉내낸다 —
        # 같은 Redis를 바라보는 새 pool이 저장된 학습 상태를 이어받아야 한다.
        restored_pool = PartitionedMLDetectors(redis_client, min_samples=30)
        restored = restored_pool.get(0)

        assert restored.is_trained is True
        sample = make_features(seed=999)
        assert restored.update(sample) == detector.update(sample)

    def test_미학습_감지기는_저장하지_않음(self):
        redis_client = FakeRedis()
        pool = PartitionedMLDetectors(redis_client, min_samples=1_000_000)

        detector = pool.get(0)
        detector.update(make_features())
        assert detector.is_trained is False

        pool.save_all()

        assert ml_state_key(0) not in redis_client.store

    def test_redis_조회_실패해도_예외없이_새_감지기로_대체(self):
        key = ml_state_key(0)
        redis_client = FakeRedis(fail_on={key})

        pool = PartitionedMLDetectors(redis_client, min_samples=30)
        detector = pool.get(0)  # 예외가 여기서 올라오면 컨슈머 루프 전체가 죽는다.

        assert detector.is_trained is False

    def test_redis_저장_실패해도_예외없이_넘어감(self):
        key = ml_state_key(0)
        redis_client = FakeRedis(fail_on={key})

        pool = PartitionedMLDetectors(redis_client, min_samples=30)
        detector = pool.get(0)
        for i in range(30):
            detector.update(make_features(seed=i))

        pool.save_all()  # 예외가 올라오면 안 된다 — 로그만 남기고 계속 처리해야 한다.
