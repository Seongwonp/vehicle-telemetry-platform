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


# ── 감지 경로(Python) — 이름이 아니라 **출처**로 가른다 (P0-2a, 2026-09-09) ──
#
# 조사에서 감지기가 실제로 내는 예외가 전부 `unknown`인 것을 찾았다. 그런데 **전부
# 영구로 넣는 것이 답이 아니었다** — 정책 확정 때 그 지적을 받았다.
#
# 검증기가 **명시적으로 만든** 계약 오류(ContractViolation)는 payload의 성질이라 영구다.
# 반면 계약을 통과한 뒤 나오는 TypeError/KeyError는 **우리 코드의 버그일 수 있고**,
# 버그면 고친 뒤 재처리가 성공한다 — 영구로 박아두면 고친 다음에도 자동 재처리에서 빠진다.
# 근거: docs/anomaly-path-contract.md 2-1절.

CONTRACT_VIOLATION = "ContractViolation"


@pytest.mark.parametrize("reason", [
    "MALFORMED_JSON: JSONDecodeError",
    "UNKNOWN_FIELD: sped",
    "TYPE_MISMATCH: speed",
    "PAYLOAD_VALIDATION_FAILED: speed must not be null",
])
def test_감지경로_계약위반은_영구다(reason):
    """검증기가 만든 것이라 되돌려도 같은 자리에서 실패한다."""
    headers = h(CONTRACT_VIOLATION, **{
        "x-dlq-failure-message": reason,
        "x-dlq-contract-reason": reason.split(":")[0],
        "x-dlq-source-path": "anomaly-detector",
    })
    assert classify(headers) == "permanent"


@pytest.mark.parametrize("fqcn", [
    "TypeError",
    "KeyError",
    "AttributeError",
    "ValueError",
])
def test_일반_예외는_영구로_단정하지_않는다(fqcn):
    """**이게 옳다.** 계약을 통과한 뒤 나오는 것이라 구현 버그일 수 있다.

    영구로 분류하면 버그를 고친 뒤에도 자동 재처리에서 빠지고, 일시로 분류하면
    고치기 전에 무한 재시도한다. `unknown`이 정확하다 — 사람이 보고 판단한다.
    """
    assert classify(h(fqcn)) == "unknown"


@pytest.mark.parametrize("fqcn", ["KafkaTimeoutError", "NoBrokersAvailable"])
def test_kafka_타임아웃도_단정하지_않는다(fqcn):
    """발생 위치에 따라 뜻이 다르다.

    알림 발행 중 타임아웃이면 브로커가 이미 받았을 수 있어(at-least-once) 재처리가
    중복 알림을 만든다 — 다만 `UNIQUE(event_id)`로 행은 안 는다(2026-09-05 확인).
    DLQ 발행 중 타임아웃이면 애초에 DLQ에 레코드가 안 남는다(원본 offset 미커밋 → 재전달).
    "재시도해도 안전하다"가 자동으로 참이 아니라서 `unknown`으로 둔다.
    """
    assert classify(h(fqcn)) == "unknown"


def test_감지기가_내는_역직렬화_예외는_이미_영구다():
    """계약 도입 후에는 ContractViolation으로 감싸이지만, 옛 DLQ 레코드가 남아 있다."""
    assert classify(h("JSONDecodeError")) == "permanent"
    assert classify(h("UnicodeDecodeError")) == "permanent"
