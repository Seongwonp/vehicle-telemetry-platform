"""공유 fixture로 **감지 경로의 현재 동작을 고정**한다 (P0-2a).

## 이건 정책 테스트가 아니라 특성화 테스트다

`contract-fixtures/cases.json`의 `detector` 칸은 **2026-09-09에 실측한 현재 동작**이고,
그게 옳다는 뜻이 아니다. 실제로 그 중 8칸은 저장 경로와 갈리고, 5칸은
**조용히 눈이 머는** 동작이다(`docs/anomaly-path-contract.md` 1-1절).

그럼에도 고정하는 이유: 정책이 확정되기 전까지 **아무도 모르게 달라지는 것**을 막기
위해서다. 감지기를 고치면 이 테스트가 깨지고, 그때 fixture와 결정 문서를 같이 보게 된다.

**정책을 정해서 동작을 바꾸면 fixture의 기대값을 같이 고쳐라.** 테스트를 지우지 말고
기대값을 옮겨야, 무엇이 어떻게 바뀌었는지가 diff에 남는다.

## 왜 Java 계약을 여기 베끼지 않았나

두 벌을 손으로 맞추면 갈라지고, 갈라진 것은 **양쪽 테스트가 다 통과하므로** 측정에서
안 드러난다. 그래서 입력과 판정을 파일 하나에 두고 양쪽이 그 파일을 읽는다
(`backend/src/test/java/com/telemetry/domain/SharedFixtureContractTest.java`가 저장 칸을 읽는다).

실행: `pytest anomaly-detector/tests/test_shared_fixtures.py -q`
"""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import anomaly_detector as ad

FIXTURES = Path(__file__).resolve().parents[2] / "contract-fixtures" / "cases.json"


def load_cases():
    if not FIXTURES.exists():
        pytest.skip(f"공유 fixture가 없다: {FIXTURES}")
    with FIXTURES.open(encoding="utf-8") as f:
        return json.load(f)["cases"]


CASES = load_cases()


class _Future:
    def get(self, timeout=None):
        return None


class _Producer:
    """발행을 가로채 판정만 본다. 브로커에 붙지 않는다."""

    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))
        return _Future()

    def flush(self):
        pass


@pytest.fixture(autouse=True)
def _no_webhook(monkeypatch):
    # webhook은 네트워크를 탄다. 여기서 재는 것은 판정이지 알림 전송이 아니다.
    monkeypatch.setattr(ad.notifier, "send_webhook", lambda payload: None)


def classify(payload: str) -> tuple[str, str]:
    """소비 루프(anomaly_detector.main)의 판정 경로를 그대로 흉내낸다.

    루프는 (1) json.loads 실패를 DLQ로, (2) process() 예외를 DLQ로 격리한다.
    둘 다 아니면 발행된 알림이 있는지로 alert / no_alert가 갈린다.

    **소비 루프 자체를 돌리는 것은 아니다** — offset 커밋과 DLQ 발행 실패 처리는
    여기서 안 본다. 그건 test_anomaly_detector.py와 장애 주입의 몫이다.
    """
    try:
        data = json.loads(payload)
    except Exception as e:
        return "dlq", f"json.loads:{type(e).__name__}"

    producer = _Producer()
    try:
        ad.process(data, producer, False)
    except Exception as e:
        return "dlq", f"process:{type(e).__name__}"

    return ("alert" if producer.sent else "no_alert"), ""


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_감지_경로의_현재_동작(case):
    expected = case["detector"]["verdict"]
    if expected == "UNMEASURED":
        pytest.skip("아직 안 잰 칸이다 — 기대값이 아니라 미측정 표시다")

    actual, detail = classify(case["payload"])
    assert actual == expected, (
        f"[{case['id']}] 감지 경로 동작이 바뀌었다: 기대 {expected} / 실제 {actual} {detail}\n"
        f"의도한 변경이면 contract-fixtures/cases.json의 detector 칸을 같이 고쳐라. "
        f"결정은 docs/anomaly-path-contract.md 3-2절."
    )


def test_저장과_감지가_갈리는_칸이_아직_남아_있다():
    """**갈림 자체를 고정한다.** 줄어들면 정책이 적용된 것이고, 그때 이 수를 고쳐야 한다.

    이 테스트가 있는 이유: 위 파라미터 테스트는 칸별로만 보므로, 갈림이 **몇 개인지**는
    아무도 안 센다. 요약 수치가 문서(anomaly-path-contract.md 1절)와 어긋나면
    문서가 조용히 낡는다.
    """
    reject_no_alert = [c["id"] for c in CASES
                       if c["storage"]["verdict"] == "reject"
                       and c["detector"]["verdict"] == "no_alert"]
    reject_alert = [c["id"] for c in CASES
                    if c["storage"]["verdict"] == "reject"
                    and c["detector"]["verdict"] == "alert"]

    # 저장은 거부하는데 감지는 조용한 칸 — **제일 나쁜 부류**다.
    assert len(reject_no_alert) == 5, f"조용한 칸이 달라졌다: {reject_no_alert}"
    # 저장 안 된 데이터로 알림이 나가는 칸.
    assert len(reject_alert) == 3, f"알림 나가는 칸이 달라졌다: {reject_alert}"
