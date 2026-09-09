"""공유 fixture로 **두 경로가 같은 계약을 쓰는지** 검사한다 (P0-2a).

## 이 파일이 지키는 것

정책은 **공통 입력 계약**이다(`docs/anomaly-path-contract.md` 3절).
필수 센서 누락/null, 범위 밖, 타입 오류, unknown 필드는 **감지 이전에 거부**한다.
그래서 `contract-fixtures/cases.json`의 `storage`와 `detector` 두 칸은
**거부 여부가 일치해야 한다** — `storage: reject`면 `detector: dlq`다.

`detector_before` 칸이 있는 fixture는 **정책 적용 전** 동작이다. 지우지 않는다 —
무엇이 어떻게 바뀌었는지가 파일 안에 남아야 한다.

## 왜 Java 계약을 여기 베끼지 않았나

두 벌을 손으로 맞추면 갈라지고, 갈라진 것은 **양쪽 테스트가 다 통과하므로** 측정에서
안 드러난다. 그래서 입력과 판정을 파일 하나에 두고 양쪽이 그 파일을 읽는다
(`backend/src/test/java/com/telemetry/domain/SharedFixtureContractTest.java`가 `storage`를 읽는다).

실행: `pytest anomaly-detector/tests/test_shared_fixtures.py -q`
"""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import contract

FIXTURES = Path(__file__).resolve().parents[2] / "contract-fixtures" / "cases.json"


def _repo_root_present() -> bool:
    """저장소 루트가 곁에 있는가.

    `anomaly-detector/Dockerfile`처럼 컨텍스트가 잘린 빌드에서는 fixture를 못 읽는 것이
    **정상**이다. 그 한 경우만 건너뛰고, **나머지에서는 없으면 실패시킨다** —
    fixture를 지웠는데 테스트가 조용히 skip되면 검사가 사라진 줄도 모른다.
    Java 쪽 `SharedFixtureContractTest`도 같은 방식이다.
    """
    root = FIXTURES.parent.parent
    return (root / "docs").is_dir() and (root / "load-test").is_dir()


def load_cases():
    if not FIXTURES.exists():
        if _repo_root_present():
            raise AssertionError(
                f"저장소 루트는 있는데 공유 fixture가 없다: {FIXTURES}. "
                "옮겼거나 지운 것이다 — contract-fixtures/README.md 참고")
        pytest.skip(
            f"저장소 루트가 없는 빌드 컨텍스트다 — 여기서 공유 fixture를 못 읽는 것은 정상이다. "
            f"이 검사는 전체 저장소 CI가 담당한다 ({FIXTURES})")
    with FIXTURES.open(encoding="utf-8") as f:
        return json.load(f)["cases"]


CASES = load_cases()


# ── 1) 계약 판정이 fixture와 같은가 ─────────────────────────────
@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_계약_판정(case):
    """`contract.validate`가 fixture의 `storage` 칸과 같은 판정을 내는가.

    **`storage` 칸을 읽는 것이 핵심이다.** 감지 경로용 기대값을 따로 두면 두 경로가
    갈라져도 양쪽 테스트가 다 통과한다 — 그게 이 fixture를 만든 이유다.
    """
    expected = case["storage"]
    try:
        contract.validate(case["payload"])
        verdict, reason = "accept", None
    except contract.ContractViolation as e:
        verdict, reason = "reject", e.reason
    except Exception as e:  # noqa: BLE001 — 계약 예외 밖으로 새는 것 자체가 결함이다
        pytest.fail(
            f"[{case['id']}] 계약 예외가 아닌 것이 나왔다: {type(e).__name__}: {e}\n"
            "소비 루프는 이걸 '예상 못 한 실패'로 세고 dlq.py는 자동 재처리하지 않는다.")

    assert verdict == expected["verdict"], (
        f"[{case['id']}] 판정이 fixture와 다르다 — Java 쪽과 갈렸을 수 있다. "
        f"의도한 변경이면 contract-fixtures/cases.json을 같이 고쳐라.")
    if expected["reason"] is not None:
        assert reason == expected["reason"], (
            f"[{case['id']}] 사유 코드가 fixture와 다르다. "
            f"순서 정의는 docs/anomaly-path-contract.md 4절.")


# ── 2) 정책 자체를 고정한다 ─────────────────────────────────────
def test_거부는_두_경로가_일치한다():
    """**이번 작업의 완료 조건이다.** 저장이 거부하는 입력은 감지도 거부해야 한다.

    정책 적용 전에는 21칸 중 8칸이 갈렸다 — 5칸은 감지가 조용히 지나갔고
    3칸은 저장 안 된 데이터로 알림이 나갔다.
    """
    mismatched = []
    for c in CASES:
        try:
            contract.validate(c["payload"])
            detector_rejects = False
        except Exception:  # noqa: BLE001
            detector_rejects = True
        storage_rejects = c["storage"]["verdict"] == "reject"
        if storage_rejects != detector_rejects:
            mismatched.append(c["id"])
    assert mismatched == [], f"두 경로의 거부 여부가 갈린다: {mismatched}"


def test_정책_적용으로_바뀐_칸이_기록되어_있다():
    """`detector_before`가 남아 있는가 — 변경 전 측정을 지우지 않는다.

    이 테스트가 깨지면 누군가 이력을 지운 것이다. 결과 문서와 devlog가 참조한다.
    """
    changed = [c["id"] for c in CASES
               if "detector_before" in c
               and c["detector_before"]["verdict"] != c["detector"]["verdict"]]
    assert len(changed) == 8, f"정책으로 바뀐 칸이 8개여야 한다: {changed}"


# ── 3) 언어 차이 — 여기가 제일 미끄럽다 ─────────────────────────
@pytest.mark.parametrize("cid", [
    "speed_boolean_true",
    "speed_boolean_false",   # float(False)=0.0은 **범위 안**이라 더 위험하다
    "speed_nan_literal",     # Python json.loads는 NaN 리터럴을 기본으로 받는다
    "speed_infinity_literal",
    "timestamp_lowercase",   # Java Instant.parse는 받고 Python fromisoformat은 거부
    "timestamp_no_offset",   # 그 반대
    "timestamp_space",       # 그 반대
])
def test_언어_차이_칸이_fixture에_있다(cid):
    """언어 차이를 fixture에서 빼면 두 구현이 갈라져도 안 드러난다.

    이 목록이 줄면 커버리지가 준 것이다 — 지우기 전에 왜인지 적어라.
    """
    assert any(c["id"] == cid for c in CASES), f"{cid} 칸이 사라졌다"


def test_bool은_숫자로_취급되지_않는다():
    """Python에서 `isinstance(True, int)`는 참이고 `float(True)`는 1.0이다.

    그냥 `float()`로 변환하면 `{"speed": true}`가 **1.0으로 조용히 통과**하고
    `{"speed": false}`는 **0.0**이 된다 — 정지 상태로 저장될 뻔한 값이다.
    Java는 둘 다 `TYPE_MISMATCH`로 거부한다.
    """
    for literal in ("true", "false"):
        payload = ('{"vehicle_id":"KR-GA-1234","timestamp":"2026-09-09T10:00:00Z",'
                   f'"speed":{literal},"rpm":2400,"engine_temp":92.1,'
                   '"throttle_position":34.5,"fuel_level":67.0,"battery_voltage":13.8}')
        with pytest.raises(contract.ContractViolation) as ei:
            contract.validate(payload)
        assert ei.value.reason == contract.TYPE_MISMATCH


# ── 4) 신선도는 계약이 아니다 ───────────────────────────────────
def test_오래된_시각은_통과한다():
    """**형식만** 본다. 과거 데이터를 거부하면 DLQ 재처리와 백필이 불가능해진다.

    신선도가 필요하면 그건 별도 정책이지 입력 계약이 아니다(2026-09-09 결정).
    """
    old = next(c for c in CASES if c["id"] == "timestamp_old")
    assert old["storage"]["verdict"] == "accept"
    contract.validate(old["payload"])   # 예외가 나면 실패한다


# ── 5) 사유 선택 순서 ───────────────────────────────────────────
def test_사유는_payload_필드_순서에_흔들리지_않는다():
    """같은 오류 조합인데 필드 순서만 바꾸면 다른 사유가 나오면 안 된다.

    `json.loads`도 Jackson도 문서를 앞에서부터 읽으므로, 그대로 두면 순서 의존이 된다.
    """
    a = next(c for c in CASES if c["id"] == "order_unknown_before_type")
    b = next(c for c in CASES if c["id"] == "order_type_before_unknown")

    def reason(case):
        with pytest.raises(contract.ContractViolation) as ei:
            contract.validate(case["payload"])
        return ei.value.reason

    assert reason(a) == reason(b) == contract.UNKNOWN_FIELD


def test_잘린_json은_MALFORMED_JSON이다():
    """앞에 unknown 필드가 있어도. 예전에는 UNKNOWN_FIELD로 나와서 운영자가
    존재하지도 않는 필드명을 고치러 갔다."""
    case = next(c for c in CASES if c["id"] == "order_truncated_with_unknown")
    with pytest.raises(contract.ContractViolation) as ei:
        contract.validate(case["payload"])
    assert ei.value.reason == contract.MALFORMED_JSON
