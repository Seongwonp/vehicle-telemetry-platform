"""혼합 버전 — **배포 순서(감지기 → 백엔드 → 브리지)가 왜 필요한지**를 테스트로 고정한다 (ADR-030).

`docs/event-correlation-design.md` §10-5와 같은 원칙이다: 받는 쪽이 새 모양을 모르면 **메시지 전체**를
거부한다. 이번 변경에서 "새 모양"은 필드 추가가 아니라 **필드 생략**이다 —
`fuel_level`/`battery_voltage`가 없거나 `null`인 payload.

| 순서를 어기면 | 무엇이 일어나나 |
| --- | --- |
| 백엔드가 감지기보다 먼저 | 새 백엔드가 받은 payload를 재직렬화해 Kafka로 보낸다(`NON_NULL`이라 키 없음) → **구 감지기가 전부 DLQ** |
| 브리지가 백엔드보다 먼저 | 브리지가 키를 생략해 보낸다 → **구 백엔드 MQTT 입구가 거부**(MQTT DLQ) |

구 계약은 `tests/legacy/contract_v1.py`에 **얼려 둔** 사본이다 — 새 계약을 고친 뒤에는 현재 코드로
구 동작을 재현할 수 없으므로. Java 쪽 같은 고정은 `MixedVersionContractTest`가 한다.
"""
import importlib.util
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import contract

_V1_PATH = os.path.join(os.path.dirname(__file__), "legacy", "contract_v1.py")
_spec = importlib.util.spec_from_file_location("contract_v1_frozen", _V1_PATH)
contract_v1 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(contract_v1)

_FULL = {
    "vehicle_id": "KR-GA-1234", "timestamp": "2026-10-06T03:00:00.000Z",
    "speed": 50.0, "rpm": 2000.25, "engine_temp": 90.0, "throttle_position": 20.0,
    "fuel_level": 60.0, "battery_voltage": 13.9,
}


def _payload(drop=(), null=()):
    d = {k: v for k, v in _FULL.items() if k not in drop}
    for k in null:
        d[k] = None
    return json.dumps(d)


# 새 producer(브리지)·새 백엔드 재직렬화가 실제로 만들 수 있는 모양
NEW_SHAPES = {
    "fuel_absent": _payload(drop=("fuel_level",)),
    "voltage_absent": _payload(drop=("battery_voltage",)),
    "both_absent": _payload(drop=("fuel_level", "battery_voltage")),
    "fuel_null": _payload(null=("fuel_level",)),
    "voltage_null": _payload(null=("battery_voltage",)),
}


def test_동결본은_정말_구버전이다():
    """동결본이 현재 계약으로 바뀌어 있으면 아래 테스트가 아무것도 지키지 않는다."""
    assert not hasattr(contract_v1, "OPTIONAL_NUMERIC")
    assert hasattr(contract, "OPTIONAL_NUMERIC")
    assert contract.OPTIONAL_NUMERIC == {"fuel_level", "battery_voltage"}


@pytest.mark.parametrize("shape", sorted(NEW_SHAPES))
def test_구_계약은_새_payload를_거부한다(shape):
    """**배포 순서의 근거.** 구 수신자는 두 필드가 없는 메시지를 `PAYLOAD_VALIDATION_FAILED`로 거부한다.

    감지기를 먼저 올리지 않으면, 새 백엔드가 Kafka로 내보내는 이 모양을 구 감지기가 전부 DLQ로 보낸다.
    """
    with pytest.raises(contract_v1.ContractViolation) as ei:
        contract_v1.validate(NEW_SHAPES[shape])
    assert ei.value.reason == contract_v1.PAYLOAD_VALIDATION_FAILED


@pytest.mark.parametrize("shape", sorted(NEW_SHAPES))
def test_새_계약은_새_payload를_받는다(shape):
    data = contract.validate(NEW_SHAPES[shape])
    for f in ("fuel_level", "battery_voltage"):
        # 받되 **값을 지어내지 않는다** — 없으면 없음/None 그대로.
        assert data.get(f) in (None, _FULL[f])


def test_온전한_payload는_양쪽_다_받는다():
    """역방향 호환 — 새 감지기가 먼저 올라가도 구 백엔드·구 producer의 온전한 메시지는 그대로 통과한다.
    그래서 감지기를 **먼저** 올리는 것이 안전하다."""
    contract_v1.validate(_payload())
    contract.validate(_payload())


def test_필수_필드_누락은_양쪽_다_거부한다():
    """선택화 범위가 두 필드뿐인지 — 나머지 넷은 구·신 모두 거부."""
    for f in ("speed", "rpm", "engine_temp", "throttle_position"):
        for mod in (contract_v1, contract):
            with pytest.raises(mod.ContractViolation) as ei:
                mod.validate(_payload(drop=(f,)))
            assert ei.value.reason == mod.PAYLOAD_VALIDATION_FAILED, (mod.__name__, f)
