"""미지원·null PID를 0으로 채우지 않는다. 계약상 필수라 그 주기는 보내지 않는다."""
import json
from datetime import datetime, timezone

import pytest

from conftest import ClientFactory, FakeObdConnection, fake_lookup
from obd_bridge.bridge import Bridge
from obd_bridge.mapping import REQUIRED_FIELDS, build_payload
from obd_bridge.publisher import SpoolPublisher, TlsConfig
from obd_bridge.reader import PidReader
from obd_bridge.spool import Spool

GOOD = {"SPEED": 50.0, "RPM": 2000.25, "COOLANT_TEMP": 90.0, "THROTTLE_POS": 20.0,
        "FUEL_LEVEL": 60.0, "CONTROL_MODULE_VOLTAGE": 13.9}
CMD_OF = {"speed": "SPEED", "rpm": "RPM", "engine_temp": "COOLANT_TEMP",
          "throttle_position": "THROTTLE_POS", "fuel_level": "FUEL_LEVEL",
          "battery_voltage": "CONTROL_MODULE_VOLTAGE"}


def make_bridge(tmp_path, conn):
    spool = Spool(str(tmp_path / "spool"))
    pub = SpoolPublisher(spool, host="h", port=1, client_id="c",
                         tls=TlsConfig(insecure_plaintext=True), client_factory=ClientFactory())
    now = lambda: datetime(2026, 10, 6, 3, 0, 0, tzinfo=timezone.utc)
    return Bridge(PidReader(conn, command_lookup=fake_lookup), spool, pub,
                  "OBD-TEST-01", "vehicle/telemetry/OBD-TEST-01", now=now), spool


@pytest.mark.parametrize("field", REQUIRED_FIELDS)
def test_unsupported_pid_is_none_not_zero(field):
    conn = FakeObdConnection(GOOD, unsupported={CMD_OF[field]})
    reader = PidReader(conn, command_lookup=fake_lookup)
    r = reader.read()
    assert r[field] is None
    assert CMD_OF[field] not in conn.queries  # 미지원은 query조차 안 한다
    assert reader.unsupported == (field,)


@pytest.mark.parametrize("field", REQUIRED_FIELDS)
def test_null_response_is_none_not_zero(field):
    values = dict(GOOD, **{CMD_OF[field]: None})
    r = PidReader(FakeObdConnection(values), command_lookup=fake_lookup).read()
    assert r[field] is None


@pytest.mark.parametrize("field", REQUIRED_FIELDS)
def test_missing_field_skips_cycle_and_spools_nothing(tmp_path, field):
    bridge, spool = make_bridge(tmp_path, FakeObdConnection(GOOD, unsupported={CMD_OF[field]}))
    assert bridge.cycle() is None
    assert len(spool) == 0
    assert bridge.stats.skipped_missing == 1
    assert bridge.stats.missing_by_field == {field: 1}


def test_zero_reading_is_kept_as_real_zero(tmp_path, contract):
    """반대 방향 — 차량이 실제로 0을 보고하면 0을 보낸다. 0을 금지하는 게 아니라 지어내지 않는 것."""
    bridge, spool = make_bridge(tmp_path, FakeObdConnection(dict(GOOD, SPEED=0.0)))
    p = bridge.cycle()
    assert p["speed"] == 0.0
    contract.validate(spool.pending()[0].payload)


def test_no_payload_ever_contains_zero_for_a_missing_field():
    """None이 섞인 모든 조합(2^6-1)에서 payload가 만들어지지 않는다."""
    base = {f: 1.0 for f in REQUIRED_FIELDS}
    for mask in range(1, 64):
        r = dict(base)
        for i, f in enumerate(REQUIRED_FIELDS):
            if mask >> i & 1:
                r[f] = None
        res = build_payload(r, "OBD-TEST-01", "2026-10-06T03:00:00.000Z")
        assert res.payload is None
        assert set(res.missing) == {f for i, f in enumerate(REQUIRED_FIELDS) if mask >> i & 1}


def test_full_cycle_payload_passes_contract(tmp_path, contract):
    bridge, spool = make_bridge(tmp_path, FakeObdConnection(GOOD))
    bridge.cycle()
    [rec] = spool.pending()
    assert rec.topic == "vehicle/telemetry/OBD-TEST-01"
    d = contract.validate(rec.payload)
    assert d["timestamp"] == "2026-10-06T03:00:00.000Z"
    assert json.loads(rec.payload)["rpm"] == 2000.25
