"""dlq.py 분류 회귀 테스트.

## 왜 이 파일이 있는가

**2026-09-06에 한 번, 2026-09-09에 또 한 번 같은 실수를 했다** — 백엔드에 새 예외
타입을 만들고 `PERMANENT_MARKERS`/`TRANSIENT_MARKERS`에 넣는 것을 잊었다. 그러면
`classify()`가 `unknown`을 돌려주고, `unknown`은 **자동 재처리 대상에서 빠진다**.
증상이 조용하다 — DLQ에 쌓이는데 도구는 "재처리할 게 없다"고 말한다.

첫 번째(InfluxDB 90초 장애)는 76,878건이 전부 `unknown`으로 빠져서 알아챘다.
두 번째는 `TelemetryConsumerTest`가 예외 타입 단언으로 잡았다 — **운이 좋았다.
그 단언이 없었으면 못 봤다.** 그래서 분류 자체를 여기서 고정한다.

실행: `python -m pytest dlq-tools/test_dlq.py -q`
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from dlq import PERMANENT_MARKERS, TRANSIENT_MARKERS, classify, failure_class, replay_count

CONTRACT = "com.telemetry.domain.TelemetryContractException"


def h(fqcn: str, **extra) -> dict:
    return {"x-dlq-failure-class": fqcn, **extra}


# ── 입력 계약 위반(P0-2) ────────────────────────────────────────
# 네 사유 코드는 전부 같은 예외 타입으로 온다. 되돌려도 payload가 그대로라 **영구**다.
@pytest.mark.parametrize("reason", [
    "MALFORMED_JSON: Unexpected character",
    "UNKNOWN_FIELD: sped",
    "TYPE_MISMATCH: speed",
    "PAYLOAD_VALIDATION_FAILED: speed must be less than or equal to 255",
    "TYPE_MISMATCH: (최상위 null)",
])
def test_계약위반은_영구로_분류된다(reason):
    assert classify(h(CONTRACT, **{"x-dlq-failure-message": reason})) == "permanent"


def test_계약위반은_재처리_대상이_아니다():
    """`unknown`이 아니어야 한다 — unknown이면 도구가 조용히 건너뛴다."""
    assert classify(h(CONTRACT)) != "unknown"


# ── 실제로 겪은 장애 유형 ───────────────────────────────────────
@pytest.mark.parametrize("fqcn", [
    "com.influxdb.exceptions.InfluxException",
    "org.springframework.transaction.CannotCreateTransactionException",
    "java.net.ConnectException",
])
def test_의존성_장애는_일시로_분류된다(fqcn):
    assert classify(h(fqcn)) == "transient"


def test_wrapper보다_원인을_먼저_본다():
    """Spring이 감싼 wrapper 이름만 보면 전부 같은 값이라 분류가 무너진다.

    InfluxDB 90초 장애에서 76,878건이 전부 `unknown`이 됐던 실제 사고다.
    """
    headers = {
        "kafka_dlt-exception-fqcn":
            "org.springframework.kafka.listener.ListenerExecutionFailedException",
        "kafka_dlt-exception-cause-fqcn": "com.influxdb.exceptions.InfluxException",
    }
    assert failure_class(headers) == "com.influxdb.exceptions.InfluxException"
    assert classify(headers) == "transient"


# ── 모르는 것은 모른다고 해야 한다 ──────────────────────────────
def test_헤더가_없으면_unknown():
    assert classify({}) == "unknown"


def test_목록에_없는_예외는_unknown():
    """자동 재처리에 태우지 않고 사람이 보게 남긴다."""
    assert classify(h("com.example.SomethingNobodyAddedYet")) == "unknown"


# ── 분류 목록 자체의 무결성 ─────────────────────────────────────
def test_두_목록이_겹치지_않는다():
    """겹치면 PERMANENT가 먼저 걸려 transient가 조용히 죽는다.

    `classify()`가 PERMANENT를 먼저 훑기 때문에, 한쪽 문자열이 다른 쪽의
    부분 문자열이기만 해도 분류가 뒤집힌다.
    """
    충돌 = [(p, t) for p in PERMANENT_MARKERS for t in TRANSIENT_MARKERS
            if p in t or t in p]
    assert 충돌 == [], f"마커가 서로를 포함한다: {충돌}"


def test_재처리_횟수는_망가진_값에도_0을_돌려준다():
    assert replay_count({}) == 0
    assert replay_count({"x-dlq-replay-count": "3"}) == 3
    assert replay_count({"x-dlq-replay-count": "삼"}) == 0


# ── 감지 경로(Python)의 예외는 아직 분류되지 않는다 (P0-2a 조사, 2026-09-09) ──
#
# `anomaly-detector`가 계약 위반 payload에서 **실제로 내는** 예외를 fixture로 재봤더니
# (docs/anomaly-path-contract.md 2-1절) 대부분이 `unknown`이었다. 영구인 것도, 일시인
# 것도 다 unknown이라 자동 재처리 대상에서 빠진다.
#
# **고치지 않고 현재 상태를 고정한다.** ValueError·TypeError가 항상 영구인지는
# 감지기 코드가 바뀌면 달라지고, 그 판단은 정책(같은 문서 3절)과 같이 해야 한다.
# 여기 두는 이유는 **모르는 채로 넘어가지 않기 위해서**다 — 누가 목록에 넣으면
# 이 테스트가 깨지고, 그때 위 문서를 보게 된다.
@pytest.mark.parametrize("fqcn", [
    "KeyError",            # vehicle_id/timestamp 누락 + 이상 감지가 겹칠 때
    "TypeError",           # dtc_codes [null] -> ','.join
    "AttributeError",      # payload가 null 리터럴 -> None.get
    "ValueError",          # speed "fast" -> float()
    "KafkaTimeoutError",   # 이쪽은 **일시**인데도 unknown이다
    "NoBrokersAvailable",
])
def test_감지기_예외는_아직_분류_목록에_없다(fqcn):
    """현재 상태를 고정한다. **이게 옳다는 뜻이 아니다.**

    깨졌다면 누군가 목록에 넣은 것이다 — `docs/anomaly-path-contract.md` 3-2절
    5번 결정을 확인하고, 맞으면 이 테스트를 지워라.
    """
    assert classify(h(fqcn)) == "unknown"


def test_감지기가_내는_역직렬화_예외는_이미_영구다():
    """반대쪽 — 이 둘은 목록에 있다. 전부 빠진 게 아니라는 것을 같이 남긴다."""
    assert classify(h("JSONDecodeError")) == "permanent"
    assert classify(h("UnicodeDecodeError")) == "permanent"
