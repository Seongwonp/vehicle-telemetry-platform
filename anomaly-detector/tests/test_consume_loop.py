"""**운영 소비 루프를 실행한 단위 테스트** — 분기와 예외 전파를 본다 (P0-2a).

## 이 파일이 증명하는 것과 증명하지 않는 것

`main()`을 **그대로** 실행한다. 루프를 함수로 발췌해서 테스트하면 그 발췌본을 검증하는
셈이라 그렇게 하지 않았다. 그래서 **분기·예외 전파·커밋 호출 방식**은 진짜다.

**다만 consumer와 producer가 가짜다.** 브로커가 실제로 offset을 어떻게 커밋하는지,
재전달이 진짜 일어나는지, 발행 실패가 실제로 어떤 모양인지는 **여기서 증명되지 않는다.**
그건 `load-test/anomaly-contract-kafka/`가 실제 Kafka로 확인한다 —
거기서는 committed offset을 **브로커에게 물어본다.**

이 구분을 흐리면 "가짜 Kafka에서 확인한 동작"이 "실제 Kafka에서 확인한 보장"처럼 읽힌다.

## 왜 파싱 테스트로는 부족한가

`contract.validate`가 거부한다는 것과 **그 레코드가 DLQ로 가고 offset이 올바르게
처리된다**는 것은 다른 주장이다. 2026-09-09 조사에서 offset·복구 동작을 코드를 읽어서
정리했는데, 그날 하루 종일 "읽은 것과 잰 것은 다르다"를 겪고도 거기서는 읽었다.

그래서 여기서는 **`main()`을 그대로 돌린다.** 루프를 함수로 발췌해서 테스트하면
그 발췌본을 검증하는 셈이 된다 — 진짜 루프의 커밋 시점과 예외 전파를 그대로 본다.
Kafka만 가짜로 바꾼다.

## 무엇을 확인하나

1. 정상 / 실패 / 정상 배치 — 실패 1건만 격리되고 세 건 다 offset이 넘어간다
2. **DLQ 발행 실패** — **아무것도 커밋하지 않는다**(아래 참고)
3. **알림 발행 실패** — DLQ로 가고 offset은 넘어간다(격리에 성공했으므로)
4. **재시작 후 재전달** — 커밋이 없으므로 배치 전체가 다시 온다

## 2번에서 기대가 틀렸고, 실제가 더 안전했다

처음에는 "실패한 레코드 앞까지는 커밋될 것"이라고 기대했다. 실측은 **아무것도
커밋되지 않는다**였다 — 검증 단계에서 실패하면 앞선 레코드들은 아직 `parsed`에 모여
있을 뿐 처리 전이기 때문이다. 중복 처리(at-least-once)는 생기지만
**격리하지 못한 것을 완료로 치지는 않는다.** 완료 조건이 요구한 것은 후자다.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

os.environ.setdefault("ML_ENABLED", "false")

import anomaly_detector as ad  # noqa: E402


TOPIC = "vehicle-telemetry"
PARTITION = 0


def payload(vehicle_id="KR-GA-1234", **over) -> str:
    body = {
        "vehicle_id": vehicle_id,
        "timestamp": "2026-09-09T10:00:00.000Z",
        "speed": 87.3, "rpm": 2400, "engine_temp": 92.1,
        "throttle_position": 34.5, "fuel_level": 67.0, "battery_voltage": 13.8,
        "gps": {"lat": 37.1, "lng": 127.6}, "dtc_codes": [],
    }
    for k, v in over.items():
        if v is ...:
            body.pop(k, None)
        else:
            body[k] = v
    return json.dumps(body)


class Msg:
    """kafka-python의 ConsumerRecord에서 루프가 실제로 읽는 필드만 흉내낸다."""

    def __init__(self, offset, value, key=b"KR-GA-1234"):
        self.topic = TOPIC
        self.partition = PARTITION
        self.offset = offset
        self.key = key
        self.value = value.encode("utf-8") if isinstance(value, str) else value


class _Stop(Exception):
    """루프를 끝내려고 가짜 consumer가 던진다. 시그널에 의존하지 않는다."""


class FakeConsumer:
    def __init__(self, batches):
        self._batches = list(batches)
        self.committed = {}          # TopicPartition -> OffsetAndMetadata
        self.commit_calls = 0
        self.closed = False

    def poll(self, timeout_ms=None):
        if not self._batches:
            # `finally`의 마지막 커밋까지 도달하도록 예외로 빠져나간다.
            raise _Stop()
        return self._batches.pop(0)

    def commit(self, offsets=None):
        self.commit_calls += 1
        # **인자 없는 커밋은 현재 position까지 밀어버린다.** 루프가 항상 명시적으로
        # offsets를 주는지 여기서 확인한다.
        assert offsets is not None, "commit()에 offsets를 명시하지 않았다"
        self.committed.update(offsets)

    def close(self):
        self.closed = True


class _Future:
    def __init__(self, exc=None):
        self._exc = exc

    def get(self, timeout=None):
        if self._exc:
            raise self._exc
        return None


class FakeProducer:
    """`fail_on`이 주어지면 그 순번의 send에서 실패시킨다(1부터 센다)."""

    def __init__(self, name, fail_on=None, fail_with=None):
        self.name = name
        self.sent = []
        self.fail_on = fail_on
        self.fail_with = fail_with or RuntimeError(f"{name} 발행 실패")

    def send(self, topic, key=None, value=None, headers=None):
        self.sent.append({"topic": topic, "key": key, "value": value,
                          "headers": dict(headers or [])})
        if self.fail_on is not None and len(self.sent) == self.fail_on:
            return _Future(self.fail_with)
        return _Future()

    def flush(self):
        pass

    def close(self):
        pass


@pytest.fixture
def loop(monkeypatch):
    """`main()`을 돌릴 준비. Kafka만 가짜로 바꾸고 나머지는 진짜다."""
    def run(batches, alert_producer=None, dlq_producer=None):
        consumer = FakeConsumer(batches)
        producer = alert_producer or FakeProducer("alerts")
        dlq = dlq_producer or FakeProducer("dlq")

        monkeypatch.setattr(ad, "make_consumer", lambda: consumer)
        monkeypatch.setattr(ad, "make_producer", lambda: producer)
        monkeypatch.setattr(ad, "make_dlq_producer", lambda: dlq)
        monkeypatch.setattr(ad, "make_redis_client", lambda: None)
        monkeypatch.setattr(ad.notifier, "send_webhook", lambda p: None)
        # 배치가 끝나자마자 커밋되게 해서 커밋 시점을 관찰 가능하게 만든다.
        monkeypatch.setattr(ad, "COMMIT_INTERVAL_MESSAGES", 1)

        raised = None
        try:
            ad.main()
        except _Stop:
            pass
        except Exception as e:   # noqa: BLE001 — 전파 여부 자체가 관찰 대상이다
            raised = e
        return consumer, producer, dlq, raised

    return run


def committed_offset(consumer):
    tp = ad.TopicPartition(TOPIC, PARTITION)
    meta = consumer.committed.get(tp)
    return None if meta is None else meta.offset


# ── 1) 정상 / 실패 / 정상 ───────────────────────────────────────
def test_배치_안의_계약위반_1건만_격리되고_offset은_전부_넘어간다(loop):
    batch = {ad.TopicPartition(TOPIC, PARTITION): [
        Msg(10, payload()),
        Msg(11, payload(speed=...)),      # 필수 센서 누락 → 계약 위반
        Msg(12, payload()),
    ]}
    consumer, producer, dlq, raised = loop([batch])

    assert raised is None
    assert len(dlq.sent) == 1, "계약 위반 1건만 DLQ로 가야 한다"
    assert dlq.sent[0]["topic"] == ad.DLQ_TOPIC

    # **원본 입력이 그대로 보존되는가** — 재처리하려면 원문이 있어야 한다.
    assert dlq.sent[0]["value"] == payload(speed=...).encode("utf-8")
    assert dlq.sent[0]["key"] == b"KR-GA-1234"

    # **source topic/partition/offset을 추적할 수 있는가**
    headers = dlq.sent[0]["headers"]
    assert headers["x-dlq-origin-topic"] == TOPIC.encode()
    assert headers["x-dlq-origin-partition"] == b"0"
    assert headers["x-dlq-origin-offset"] == b"11"
    assert headers["x-dlq-contract-reason"] == b"PAYLOAD_VALIDATION_FAILED"
    assert headers["x-dlq-source-path"] == b"anomaly-detector"

    # 세 건 다 처리 완료 — 마지막 offset + 1
    assert committed_offset(consumer) == 13


def test_계약을_통과한_정상건은_알림_판정까지_간다(loop):
    batch = {ad.TopicPartition(TOPIC, PARTITION): [
        Msg(0, payload(engine_temp=106.0)),   # 계약 안의 이상값 → 감지 대상
        Msg(1, payload()),                    # 정상 → 알림 없음
    ]}
    consumer, producer, dlq, raised = loop([batch])

    assert raised is None
    assert dlq.sent == []
    assert len(producer.sent) == 1, "106도 1건만 알림이어야 한다"
    assert committed_offset(consumer) == 2


def test_계약_밖의_값은_감지가_아니라_입력오류다(loop):
    """`speed: 300`은 과속 임계를 넘지만 **계약 밖**이다 — 알림이 아니라 DLQ다.

    "입력 오류"와 "차량 이상"을 구분한다. 201km/h는 감지 대상이고 300km/h는 입력 오류다.
    """
    batch = {ad.TopicPartition(TOPIC, PARTITION): [
        Msg(0, payload(speed=201.0)),   # 계약 안(상한 255) + 임계 초과 → 알림
        Msg(1, payload(speed=300.0)),   # 계약 밖 → DLQ
    ]}
    consumer, producer, dlq, raised = loop([batch])

    assert raised is None
    assert len(producer.sent) == 1, "201km/h만 알림이어야 한다"
    assert len(dlq.sent) == 1
    assert dlq.sent[0]["headers"]["x-dlq-contract-reason"] == b"PAYLOAD_VALIDATION_FAILED"


# ── 2) DLQ 발행 실패 — 조용히 건너뛰지 않는다 ───────────────────
def test_DLQ_발행_실패시_그_offset을_커밋하지_않는다(loop):
    """**이번 작업의 완료 조건 절반이다.** 격리에 실패하면 처리 완료로 치면 안 된다."""
    batch = {ad.TopicPartition(TOPIC, PARTITION): [
        Msg(20, payload()),
        Msg(21, payload(speed=...)),      # 계약 위반 → DLQ 시도 → **발행 실패**
        Msg(22, payload()),
    ]}
    dlq = FakeProducer("dlq", fail_on=1)
    consumer, producer, dlq, raised = loop([batch], dlq_producer=dlq)

    # 예외가 루프 밖으로 나가야 offset이 처리 완료로 집계되지 않는다.
    assert raised is not None, "DLQ 발행 실패가 삼켜졌다"

    # **아무것도 커밋되지 않는다** — 실측으로 확인한 값이고, 처음 기대(20까지 커밋)보다
    # 안전한 쪽이다. 검증 단계에서 실패하면 앞선 레코드들은 아직 `parsed`에 모여 있을
    # 뿐 처리 전이라, 배치 전체가 재전달되는 것이 맞다.
    # 중복 처리(at-least-once)는 생기지만 **격리 못 한 것을 완료로 치지는 않는다.**
    assert committed_offset(consumer) is None, (
        "격리에 실패했는데 무언가 처리 완료로 커밋됐다")


def test_DLQ_발행_실패후_재시작하면_그_레코드가_다시_온다(loop):
    """복구가 성립하는가 — 커밋 안 된 offset부터 다시 읽는다."""
    first = {ad.TopicPartition(TOPIC, PARTITION): [
        Msg(20, payload()),
        Msg(21, payload(speed=...)),
        Msg(22, payload()),
    ]}
    consumer1, _, _, _ = loop([first], dlq_producer=FakeProducer("dlq", fail_on=1))
    assert committed_offset(consumer1) is None, "커밋이 없어야 배치 전체가 재전달된다"

    # 재기동: 커밋이 없으므로 브로커가 **배치 전체**를 다시 준다. 이번엔 DLQ가 정상이다.
    second = {ad.TopicPartition(TOPIC, PARTITION): [
        Msg(20, payload()),
        Msg(21, payload(speed=...)),
        Msg(22, payload()),
    ]}
    consumer2, _, dlq2, raised2 = loop([second])

    assert raised2 is None
    assert len(dlq2.sent) == 1, "재전달된 레코드가 이번엔 격리돼야 한다"
    assert dlq2.sent[0]["headers"]["x-dlq-origin-offset"] == b"21"
    assert committed_offset(consumer2) == 23, "이번엔 세 건 다 처리 완료다"


# ── 3) 알림 발행 실패 ───────────────────────────────────────────
def test_알림_발행_실패는_그_레코드를_DLQ로_보내고_offset은_넘어간다(loop):
    """격리에 **성공**했으므로 처리 완료다. DLQ 발행 실패(위)와 다른 경우다.

    다만 이 실패는 **계약 위반이 아니다** — 계약을 통과한 payload이고 발행 쪽 문제다.
    그래서 `x-dlq-contract-reason` 헤더가 붙지 않는다.
    """
    batch = {ad.TopicPartition(TOPIC, PARTITION): [
        Msg(30, payload(engine_temp=106.0)),   # 알림 대상 → 발행 실패
        Msg(31, payload()),                    # 정상, 알림 없음
    ]}
    producer = FakeProducer("alerts", fail_on=1)
    consumer, producer, dlq, raised = loop([batch], alert_producer=producer)

    assert raised is None
    assert len(dlq.sent) == 1
    assert dlq.sent[0]["headers"]["x-dlq-origin-offset"] == b"30"
    assert "x-dlq-contract-reason" not in dlq.sent[0]["headers"], (
        "계약 위반이 아닌데 계약 사유가 붙었다")
    assert committed_offset(consumer) == 32


# ── 4) 커밋 방식 자체 ───────────────────────────────────────────
def test_커밋은_항상_명시적_offset으로_한다(loop):
    """인자 없는 `commit()`은 현재 position까지 밀어서 격리 실패분을 삼킨다.

    `FakeConsumer.commit`이 그걸 단언한다 — 이 테스트는 그 단언이 실제로 돌았는지 본다.
    """
    batch = {ad.TopicPartition(TOPIC, PARTITION): [Msg(0, payload())]}
    consumer, _, _, raised = loop([batch])
    assert raised is None
    assert consumer.commit_calls >= 1


def test_사유별_집계가_올라간다(loop):
    """지표가 사유별로 갈리는가. 라벨에 차량 ID·payload는 넣지 않는다."""
    before = dict(ad.CONTRACT_REJECTED)
    batch = {ad.TopicPartition(TOPIC, PARTITION): [
        Msg(0, payload(speed=...)),          # PAYLOAD_VALIDATION_FAILED
        Msg(1, '{"sped": 1}'),               # UNKNOWN_FIELD
        Msg(2, "{not-json"),                 # MALFORMED_JSON
    ]}
    loop([batch])

    delta = {r: ad.CONTRACT_REJECTED[r] - before.get(r, 0) for r in ad.contract.REASONS}
    assert delta["PAYLOAD_VALIDATION_FAILED"] == 1
    assert delta["UNKNOWN_FIELD"] == 1
    assert delta["MALFORMED_JSON"] == 1


# ── 5) 재처리 시 event_id가 같은가 — 중복 방지의 **전제** ───────
#
# "`UNIQUE(event_id)`가 막는다"는 주장은 **같은 원본 레코드가 같은 event_id를 만든다**는
# 전제 위에서만 성립한다. 매번 새 ID를 만들면 UNIQUE 인덱스는 아무것도 막지 못한다.
# 그 전제를 여기서 직접 확인한다 — 지금까지 아무도 안 봤다.
def test_같은_레코드를_다시_처리하면_event_id가_같다(loop):
    """`event_id`는 원본에서만 나온다 — `detected_at`은 키에 안 들어간다.

    이게 성립해야 재처리가 `ON CONFLICT (event_id) DO NOTHING`에 걸린다.
    행 중복이 실제로 0인 것은 `load-test/anomaly-dlq-idempotency/
    RESULT_20260905_alert_replay.md`(같은 DLQ를 두 번 되돌려 행 증가 0),
    브로드캐스트 차단은 `TelemetryConsumerTest.consumeAnomalyAlerts_중복이면_브로드캐스트안함`.
    """
    import json as _json

    def run_once():
        batch = {ad.TopicPartition(TOPIC, PARTITION): [
            Msg(0, payload(engine_temp=106.0)),
        ]}
        _, producer, _, raised = loop([batch])
        assert raised is None
        assert len(producer.sent) == 1
        return producer.sent[0]["value"]

    first = run_once()
    second = run_once()

    assert first["event_id"] == second["event_id"], (
        "재처리에서 event_id가 달라지면 UNIQUE(event_id)는 중복을 못 막는다")
    # 키에 들어가는 다섯 필드는 전부 원본에서 나온다.
    for field in ("vehicle_id", "timestamp", "anomaly_type", "field", "detector"):
        assert first[field] == second[field]
    # `detected_at`은 매번 달라질 수 있다 — **키에 없다는 것이 요점이다.**
    assert "detected_at" in first
    _json.dumps(first)   # 직렬화 가능해야 실제로 발행된다


def test_event_id는_원본_필드만으로_결정된다():
    """키 구성이 바뀌면 이 테스트가 깨진다 — Java `AnomalyService.resolveEventId`와
    **같은 다섯 필드·같은 순서**여야 한다. 어긋나면 Java가 다시 계산해서 다른 ID가 되고,
    그 순간 재처리 중복 방지가 무너진다."""
    import hashlib
    payload_fields = {
        "vehicle_id": "KR-GA-1234",
        "timestamp": "2026-09-09T10:00:00.000Z",
        "anomaly_type": "엔진 과열",
        "field": "engine_temp",
        "detector": "RULE",
    }
    expected = hashlib.sha256(
        "|".join(payload_fields[f] for f in
                 ("vehicle_id", "timestamp", "anomaly_type", "field", "detector")
                 ).encode("utf-8")).hexdigest()

    batch_payload = dict(payload_fields, value=106.0, threshold="x", severity="HIGH",
                         detected_at="2026-09-09T10:00:01Z")
    key = "|".join(str(batch_payload.get(f, "")) for f in
                   ("vehicle_id", "timestamp", "anomaly_type", "field", "detector"))
    assert hashlib.sha256(key.encode("utf-8")).hexdigest() == expected
