"""미지원·null PID를 0으로 채우지 않는다.

- **필수 PID**(속도·RPM·냉각수·스로틀)가 없으면 그 주기는 보내지 않는다.
- **선택 PID**(연료량 012F·제어 모듈 전압 0142, ADR-030)가 없으면 **키를 생략하고** 보낸다.
"""
import json
from datetime import datetime, timezone

import pytest

from conftest import ClientFactory, FakeObdConnection, fake_lookup
from obd_bridge.bridge import Bridge
from obd_bridge.mapping import OPTIONAL_FIELDS, PID_SPECS, REQUIRED_FIELDS, build_payload
from obd_bridge.publisher import SpoolPublisher, TlsConfig
from obd_bridge.reader import PidReader
from obd_bridge.spool import Spool

GOOD = {"SPEED": 50.0, "RPM": 2000.25, "COOLANT_TEMP": 90.0, "THROTTLE_POS": 20.0,
        "FUEL_LEVEL": 60.0, "CONTROL_MODULE_VOLTAGE": 13.9}
CMD_OF = {"speed": "SPEED", "rpm": "RPM", "engine_temp": "COOLANT_TEMP",
          "throttle_position": "THROTTLE_POS", "fuel_level": "FUEL_LEVEL",
          "battery_voltage": "CONTROL_MODULE_VOLTAGE"}
ALL_FIELDS = tuple(s.field for s in PID_SPECS)


def make_bridge(tmp_path, conn):
    spool = Spool(str(tmp_path / "spool"))
    pub = SpoolPublisher(spool, host="h", port=1, client_id="c",
                         tls=TlsConfig(insecure_plaintext=True), client_factory=ClientFactory())
    now = lambda: datetime(2026, 10, 6, 3, 0, 0, tzinfo=timezone.utc)
    return Bridge(PidReader(conn, command_lookup=fake_lookup), spool, pub,
                  "OBD-TEST-01", "vehicle/telemetry/OBD-TEST-01", now=now), spool


def test_scope_is_exactly_two_optional_fields():
    """선택화 범위가 연료량·전압 둘뿐이다 — 나머지 넷은 필수(ADR-030)."""
    assert set(OPTIONAL_FIELDS) == {"fuel_level", "battery_voltage"}
    assert set(REQUIRED_FIELDS) == {"speed", "rpm", "engine_temp", "throttle_position"}


@pytest.mark.parametrize("field", ALL_FIELDS)
def test_unsupported_pid_is_none_not_zero(field):
    conn = FakeObdConnection(GOOD, unsupported={CMD_OF[field]})
    reader = PidReader(conn, command_lookup=fake_lookup)
    r = reader.read()
    assert r[field] is None
    assert CMD_OF[field] not in conn.queries  # 미지원은 query조차 안 한다
    assert reader.unsupported == (field,)


@pytest.mark.parametrize("field", ALL_FIELDS)
def test_null_response_is_none_not_zero(field):
    values = dict(GOOD, **{CMD_OF[field]: None})
    r = PidReader(FakeObdConnection(values), command_lookup=fake_lookup).read()
    assert r[field] is None


# ── 필수 PID — 없으면 주기를 건너뛴다 ───────────────────────────
@pytest.mark.parametrize("field", REQUIRED_FIELDS)
def test_missing_required_field_skips_cycle_and_spools_nothing(tmp_path, field):
    bridge, spool = make_bridge(tmp_path, FakeObdConnection(GOOD, unsupported={CMD_OF[field]}))
    assert bridge.cycle() is None
    assert len(spool) == 0
    assert bridge.stats.skipped_missing == 1
    assert bridge.stats.missing_by_field == {field: 1}


def test_missing_required_is_not_rescued_by_optional_rules(tmp_path):
    """필수 하나 + 선택 둘이 모두 없을 때도 건너뛴다 — 선택 생략이 필수 누락을 덮지 않는다."""
    unsupported = {"SPEED", "FUEL_LEVEL", "CONTROL_MODULE_VOLTAGE"}
    bridge, spool = make_bridge(tmp_path, FakeObdConnection(GOOD, unsupported=unsupported))
    assert bridge.cycle() is None
    assert len(spool) == 0
    assert bridge.stats.missing_by_field == {"speed": 1}


# ── 선택 PID — 없으면 키를 생략하고 보낸다 ──────────────────────
@pytest.mark.parametrize("unsupported_fields", [
    ("fuel_level",), ("battery_voltage",), ("fuel_level", "battery_voltage")])
@pytest.mark.parametrize("how", ["unsupported", "null"])
def test_missing_optional_field_is_omitted_not_zero(tmp_path, contract, contract_v1,
                                                    unsupported_fields, how):
    """012F·0142가 미지원이거나 응답이 null이면 **키 자체가 없다**(0도 null도 아니다).

    - 새 계약(contract.py)은 받는다 → 그런 차량도 이제 데이터를 보낸다.
    - **구 계약은 거부한다** → 백엔드·감지기보다 브리지를 먼저 올리면 전부 MQTT DLQ로 간다.
      배포 순서(감지기 → 백엔드 → 브리지)의 근거다.
    """
    cmds = {CMD_OF[f] for f in unsupported_fields}
    if how == "unsupported":
        conn = FakeObdConnection(GOOD, unsupported=cmds)
    else:
        conn = FakeObdConnection(dict(GOOD, **{c: None for c in cmds}))
    bridge, spool = make_bridge(tmp_path, conn)

    p = bridge.cycle()
    assert p is not None
    for f in unsupported_fields:
        assert f not in p
    [rec] = spool.pending()
    raw = json.loads(rec.payload)
    for f in unsupported_fields:
        assert f not in raw, f"{f}가 payload에 있다: {rec.payload}"
    assert set(raw) == {"vehicle_id", "timestamp", *ALL_FIELDS} - set(unsupported_fields)
    assert bridge.stats.skipped_missing == 0
    assert bridge.stats.omitted_by_field == {f: 1 for f in unsupported_fields}

    contract.validate(rec.payload)
    with pytest.raises(contract_v1.ContractViolation) as ei:
        contract_v1.validate(rec.payload)
    assert ei.value.reason == contract_v1.PAYLOAD_VALIDATION_FAILED


def test_present_optional_value_still_range_checked(tmp_path):
    """선택이라도 **있으면** 범위 검사는 그대로다 — 잘라 넣지 않고 보내지 않는다."""
    bridge, spool = make_bridge(tmp_path, FakeObdConnection(dict(GOOD, FUEL_LEVEL=101.0)))
    assert bridge.cycle() is None
    assert len(spool) == 0
    assert bridge.stats.skipped_out_of_range == 1


def test_zero_reading_is_kept_as_real_zero(tmp_path, contract):
    """반대 방향 — 차량이 실제로 0을 보고하면 0을 보낸다. 0을 금지하는 게 아니라 지어내지 않는 것."""
    bridge, spool = make_bridge(tmp_path, FakeObdConnection(dict(GOOD, SPEED=0.0, FUEL_LEVEL=0.0)))
    p = bridge.cycle()
    assert p["speed"] == 0.0
    assert p["fuel_level"] == 0.0  # 실제 0%는 키가 있고 값이 0 — "없음"(키 생략)과 다르다
    contract.validate(spool.pending()[0].payload)


def test_no_payload_ever_contains_zero_for_a_missing_field(contract):
    """None이 섞인 모든 조합(2^6-1)에서 — 필수가 빠지면 payload 없음, 선택만 빠지면 그 키만 없음.
    어느 경우에도 빠진 필드가 0이나 null로 들어가지 않는다."""
    base = {f: 1.0 for f in ALL_FIELDS}
    for mask in range(1, 64):
        r = dict(base)
        dropped = {f for i, f in enumerate(ALL_FIELDS) if mask >> i & 1}
        for f in dropped:
            r[f] = None
        res = build_payload(r, "OBD-TEST-01", "2026-10-06T03:00:00.000Z")
        required_dropped = dropped & set(REQUIRED_FIELDS)
        if required_dropped:
            assert res.payload is None
            assert set(res.missing) == required_dropped
        else:
            assert res.payload is not None
            assert set(res.omitted) == dropped
            assert dropped.isdisjoint(res.payload)
            contract.validate(json.dumps(res.payload))


def test_full_cycle_payload_passes_contract(tmp_path, contract, contract_v1):
    bridge, spool = make_bridge(tmp_path, FakeObdConnection(GOOD))
    bridge.cycle()
    [rec] = spool.pending()
    assert rec.topic == "vehicle/telemetry/OBD-TEST-01"
    d = contract.validate(rec.payload)
    assert d["timestamp"] == "2026-10-06T03:00:00.000Z"
    assert json.loads(rec.payload)["rpm"] == 2000.25
    # 온전한 payload는 구 계약도 받는다 — 지원 차량만 있는 동안은 순서가 문제 되지 않는다.
    contract_v1.validate(rec.payload)
