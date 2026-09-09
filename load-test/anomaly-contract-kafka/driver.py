#!/usr/bin/env python3
"""실제 Kafka로 감지기 소비 루프의 **offset·격리·복구**를 확인한다 (P0-2a 항목 2).

## 가짜 Kafka 테스트와 무엇이 다른가

`anomaly-detector/tests/test_consume_loop.py`는 **운영 소비 루프를 실행한 단위 테스트**다.
`main()`을 그대로 돌리지만 consumer/producer가 가짜라, **분기**는 보여도
브로커가 실제로 offset을 어떻게 커밋하는지, 재전달이 실제로 일어나는지는 못 본다.

여기서는 진짜 브로커에 붙는다. **committed offset을 Kafka에게 물어본다.**

## 격리

전용 토픽(`itc-*`)과 전용 Consumer Group을 쓴다. 운영 토픽과 그룹을 건드리지 않는다.

사용법(컨테이너 안에서):
    python driver.py <시나리오>
"""
from __future__ import annotations

import json
import os
import sys
import signal
import threading
import time

sys.path.insert(0, "/app")

RUN = os.environ["RUN_ID"]
# **시나리오마다 토픽과 Consumer Group을 나눈다.**
# 처음에 하나로 묶었더니 s1이 남긴 committed offset(3)을 s2가 물려받아, s2가
# "진행하지 않음"을 올바르게 지켰는데도 판정이 실패로 나왔다. 격리가 곧 판정이다.
#
# 다만 **s2와 s3은 일부러 같이 쓴다** — s3이 "s2의 장애를 제거하고 재시작"이라
# 상태를 이어받아야 의미가 있다.
SLOT = os.environ.get("SLOT", sys.argv[1] if len(sys.argv) > 1 else "s1")
if SLOT in ("s2", "s3"):
    SLOT = "s23"
IN = f"itc-{RUN}-{SLOT}-telemetry"
OUT = f"itc-{RUN}-{SLOT}-alerts"
DLQ = f"itc-{RUN}-{SLOT}-dlq"
GROUP = f"itc-{RUN}-{SLOT}-group"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
# **발행 실패를 어떻게 주입하나 — 그리고 그게 무엇을 검증하지 *않는지*.**
#
# `max_request_size=1`은 **클라이언트 쪽 크기 제한**이다. `send()`가 브로커에 닿기도 전에
# `MessageSizeTooLargeError`를 즉시 낸다. **발행이 확실히 실패한 상태**를 만든다.
#
# 그래서 이걸로 검증되는 것은 **"발행이 실패했을 때 offset이 어떻게 되는가"**뿐이다.
#
# **검증되지 않는 것**(이렇게 읽으면 안 된다):
#   - 브로커 장애·네트워크 단절
#   - timeout 후 복구
#   - **발행 성공 여부가 불확실한 상황**(브로커가 받았는지 모르는 경우) — 여기서는
#     안 받은 것이 확실하다. in-doubt 상태는 재시도·중복 판단이 전혀 다르다.
#
# 처음에는 도달 불가 브로커(`10.255.255.1:9092`)로 향하게 해봤는데 kafka-python의
# `KafkaProducer` **생성자가 부트스트랩에서 블록**해서 5분씩 멈췄다
# (`max_block_ms`는 `send()`에만 걸린다). 그래서 이 방식으로 바꿨다 —
# **더 약한 주입이라는 것을 알고 고른 것**이고, 그 대가는 위 세 줄이다.
TINY_REQUEST = 1

os.environ.setdefault("ML_ENABLED", "false")

import logging  # noqa: E402
logging.getLogger("kafka").setLevel(logging.WARNING)

import anomaly_detector as ad          # noqa: E402
from kafka import KafkaProducer, KafkaConsumer, TopicPartition  # noqa: E402

# 전용 토픽·그룹으로 갈아끼운다. 운영 상수를 건드리지 않는다.
ad.INPUT_TOPIC = IN
ad.OUTPUT_TOPIC = OUT
ad.DLQ_TOPIC = DLQ
ad.notifier.send_webhook = lambda p: None
# 배치가 끝나면 바로 커밋되게 해서 커밋 시점을 관찰 가능하게 한다.
ad.COMMIT_INTERVAL_MESSAGES = 1
# 지표 포트 충돌 방지 — 이 드라이버는 지표를 안 본다.
ad.start_http_server = lambda *a, **k: None

_real_make_consumer = ad.make_consumer


def make_consumer():
    return KafkaConsumer(
        IN,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=None,
    )


ad.make_consumer = make_consumer
ad.make_redis_client = lambda: None


def raw_producer(max_request_size=None):
    """DLQ용 — 원본 바이트를 그대로 보내므로 직렬화기가 없다(`make_dlq_producer`와 같다)."""
    kw = dict(bootstrap_servers=BOOTSTRAP, acks="all", retries=0,
              max_block_ms=5000, request_timeout_ms=5000)
    if max_request_size is not None:
        kw["max_request_size"] = max_request_size
    return KafkaProducer(**kw)


def alert_producer(max_request_size=None):
    """알림용 — **`make_producer()`와 같은 직렬화기**여야 한다.

    처음에 직렬화기 없이 만들었더니 dict를 보내다 `TypeError`가 났고, 그게 "알림 발행
    실패"로 잡혀서 시나리오가 엉뚱한 경로를 쟀다. 실패를 주입하려면 **나머지는 운영과
    같아야 한다** — 안 그러면 무엇이 실패한 건지 알 수 없다.
    """
    kw = dict(bootstrap_servers=BOOTSTRAP, acks="all", retries=0,
              max_block_ms=5000, request_timeout_ms=5000,
              value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
              key_serializer=lambda k: k.encode("utf-8"))
    if max_request_size is not None:
        kw["max_request_size"] = max_request_size
    return KafkaProducer(**kw)


def producer(max_request_size=None):
    """주입용(원본 bytes)."""
    return raw_producer(max_request_size)


def base(**over):
    b = {"vehicle_id": "KR-GA-1234", "timestamp": "2026-09-09T10:00:00.000Z",
         "speed": 87.3, "rpm": 2400, "engine_temp": 92.1, "throttle_position": 34.5,
         "fuel_level": 67.0, "battery_voltage": 13.8,
         "gps": {"lat": 37.1, "lng": 127.6}, "dtc_codes": []}
    for k, v in over.items():
        if v is ...:
            b.pop(k, None)
        else:
            b[k] = v
    return json.dumps(b)


def inject(payloads):
    p = producer()
    for raw in payloads:
        p.send(IN, key=b"KR-GA-1234", value=raw.encode()).get(timeout=30)
    p.flush(); p.close()


def run_detector(break_alerts=False, break_dlq=False, seconds=18):
    """진짜 브로커에 붙은 `main()`을 한 번 돌리고 **SIGTERM으로 끝낸다.**

    `consumer_timeout_ms`는 `for m in consumer` 반복에만 걸리고 `poll()`에는 안 걸린다 —
    처음에 그걸로 끝내려다 루프가 영원히 돌았다. SIGTERM은 `main()`이 직접 설치한
    핸들러가 받으므로 **실제 종료 경로**(마지막 `maybe_commit(force=True)`)를 그대로 탄다.
    """
    ad.make_producer = lambda: alert_producer(TINY_REQUEST if break_alerts else None)
    ad.make_dlq_producer = lambda: raw_producer(TINY_REQUEST if break_dlq else None)

    def watchdog():
        time.sleep(seconds)
        os.kill(os.getpid(), signal.SIGTERM)

    t = threading.Thread(target=watchdog, daemon=True)
    t.start()
    err = None
    try:
        ad.main()
    except Exception as e:                       # noqa: BLE001
        err = f"{type(e).__name__}: {e}"
    return err


def committed():
    """**Kafka에게 물어본다.** 우리가 기록한 값이 아니라 브로커가 가진 값이다."""
    c = KafkaConsumer(bootstrap_servers=BOOTSTRAP, group_id=GROUP,
                      enable_auto_commit=False)
    tps = [TopicPartition(IN, p) for p in (c.partitions_for_topic(IN) or {0})]
    out = {}
    for tp in tps:
        off = c.committed(tp)
        if off is not None:
            out[tp.partition] = off
    c.close()
    return out


def read_topic(topic, limit=20, want_headers=False):
    c = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP,
                      auto_offset_reset="earliest", consumer_timeout_ms=6000)
    rows = []
    for m in c:
        rows.append({"value": m.value.decode("utf-8", "replace"),
                     "headers": {k: v.decode("utf-8", "replace")
                                 for k, v in (m.headers or [])} if want_headers else None})
        if len(rows) >= limit:
            break
    c.close()
    return rows


# ══════════════════════════════════════════════════════════════
# 시나리오
# ══════════════════════════════════════════════════════════════

def s1_normal_violation_normal():
    """정상 / 계약 위반 / 정상 — 최종 committed offset이 3인가."""
    inject([base(), base(speed=...), base()])
    err = run_detector()
    off = committed()
    dlq = read_topic(DLQ, want_headers=True)
    alerts = read_topic(OUT)
    print(f"  루프 예외      : {err or '없음'}")
    print(f"  committed      : {off}  (기대 {{0: 3}})")
    print(f"  DLQ 건수       : {len(dlq)}  (기대 1)")
    print(f"  알림 건수      : {len(alerts)}  (기대 0 — 전부 정상값)")
    if dlq:
        h = dlq[0]["headers"]
        print(f"  DLQ 사유       : {h.get('x-dlq-contract-reason')}")
        print(f"  DLQ 원본 offset: {h.get('x-dlq-origin-offset')}  (기대 1)")
        print(f"  DLQ 원본 보존  : {'예' if dlq[0]['value'] == base(speed=...) else '아니오'}")
    ok = (off.get(0) == 3 and len(dlq) == 1 and err is None
          and dlq[0]["headers"].get("x-dlq-origin-offset") == "1")
    return ok


def s2_dlq_publish_failure():
    """**클라이언트 크기 제한으로 DLQ 발행 실패를 주입**하고, 실패 레코드를 넘어서는
    offset이 커밋되지 않는지 **실제 Kafka에서** 확인한다.

    브로커 장애나 timeout 복구를 검증하는 것이 아니다(TINY_REQUEST 주석 참고).

    이 시나리오는 s3과 토픽·그룹을 공유한다 — s3이 "발행 실패를 제거하고 재시작"이라
    여기서 만든 상태를 이어받아야 한다.
    """
    inject([base(), base(speed=...), base()])
    err = run_detector(break_dlq=True)
    off = committed()
    got = off.get(0)
    print(f"  루프 예외      : {(err or '없음')[:80]}  (예외가 나야 offset이 안 넘어간다)")
    print(f"  committed      : {off}  (기대: 없음 또는 1 이하 — 실패 레코드는 offset 1)")
    ok = err is not None and (got is None or got <= 1)
    print(f"  판정           : {'통과' if ok else '실패 — 실패 레코드를 넘어 커밋됐다'}")
    return ok


def s3_restart_after_recovery():
    """장애 제거 후 재시작 — 재전달되어 격리가 완료되는가.

    **s2가 남긴 상태에서 이어간다.** s2에서 DLQ 발행이 실패해 아무것도(또는 offset 1까지만)
    커밋되지 않았으므로, 재시작하면 그 레코드가 다시 온다.
    """
    before = committed()
    err = run_detector()          # DLQ 정상으로 복구
    off = committed()
    dlq = read_topic(DLQ, want_headers=True)
    print(f"  재시작 전 committed: {before}  (s2가 남긴 상태)")
    print(f"  재시작 후 committed: {off}  (기대 {{0: 3}} — 전부 처리 완료)")
    print(f"  루프 예외          : {(err or '없음')[:60]}")
    print(f"  DLQ 건수           : {len(dlq)}  (기대 1 — 재전달분이 이번엔 격리됨)")
    offsets = [d["headers"].get("x-dlq-origin-offset") for d in dlq]
    print(f"  DLQ 원본 offset    : {offsets}  (기대 ['1'])")
    ok = off.get(0) == 3 and len(dlq) == 1 and err is None and offsets == ["1"]
    return ok


def s4_alert_publish_failure():
    """**알림 발행 실패 → 원본 DLQ 격리 성공 → offset 진행 → 명시적 재처리**.

    순서를 정확히 읽어야 한다. 자동 재시도로 알림 발행이 복구되는 것이 **아니다** —
    감지기에는 재시도가 없다(`docs/anomaly-path-contract.md` 2절).
    알림 발행이 실패하면 그 레코드는 **원본이 DLQ로 격리**되고, 격리에 성공했으므로
    offset은 진행한다. 알림은 그 시점에 나가지 않는다.

    그 뒤 **사람이 원본을 다시 흘려보내야** 알림이 나간다. 이 시나리오에서는
    같은 원본을 다시 주입하는 것으로 그 재처리를 흉내낸다 —
    실제 절차는 `dlq-tools/dlq.py replay`이고 Runbook 2-2절에 있다.

    확인하는 것: 재처리에서 **같은 원본이 같은 event_id를 만드는가**. 그게 아니면
    `UNIQUE(event_id)`는 아무것도 막지 못한다.
    """
    inject([base(engine_temp=106.0)])
    err1 = run_detector(break_alerts=True)
    off1 = committed()
    dlq = read_topic(DLQ, want_headers=True)
    print(f"  1차 루프 예외  : {err1 or '없음'}")
    print(f"  1차 committed  : {off1}  (격리 성공이므로 넘어가야 한다)")
    print(f"  DLQ 건수       : {len(dlq)}")
    if dlq:
        h = dlq[-1]["headers"]
        print(f"  DLQ 실패 클래스: {h.get('x-dlq-failure-class')}  (기대 MessageSizeTooLargeError)")
        print(f"  계약 사유 헤더 : {h.get('x-dlq-contract-reason', '(없음 — 계약 위반이 아니다)')}")

    # 재처리: 같은 원본을 다시 넣고 정상 브로커로 처리한다.
    inject([base(engine_temp=106.0)])
    err2 = run_detector()
    alerts = read_topic(OUT)
    ids = [json.loads(a["value"])["event_id"] for a in alerts]
    print(f"  2차 루프 예외  : {err2 or '없음'}")
    print(f"  발행된 알림    : {len(alerts)}건, 고유 event_id {len(set(ids))}개")
    print(f"  → 같은 원본은 같은 event_id다: {'예' if len(set(ids)) == 1 else '아니오'}")
    ok = (err1 is None and len(set(ids)) == 1 and len(ids) >= 1
          and dlq and "x-dlq-contract-reason" not in dlq[-1]["headers"]
          and "MessageSizeTooLarge" in (dlq[-1]["headers"].get("x-dlq-failure-class") or ""))
    return bool(ok)


SCENARIOS = {
    "s1": ("정상/위반/정상 — committed offset", s1_normal_violation_normal),
    "s2": ("DLQ 발행 실패(클라이언트 크기 제한) — offset 미진행", s2_dlq_publish_failure),
    "s3": ("복구 후 재시작 — 재전달·격리", s3_restart_after_recovery),
    "s4": ("알림 발행 실패 → DLQ 격리 → offset 진행 → 명시적 재처리", s4_alert_publish_failure),
}

if __name__ == "__main__":
    name = sys.argv[1]
    title, fn = SCENARIOS[name]
    print(f"[{name}] {title}")
    ok = fn()
    print(f"[{name}] 판정: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
