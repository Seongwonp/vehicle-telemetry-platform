"""브리지가 만드는 payload가 **실제 계약 모듈**(anomaly-detector/contract.py)을 통과하는가.

값은 손으로 고르지 않고 **python-OBD의 실제 decoder**에 PID 원시 바이트를 넣어 만든다.
그래야 "PID가 표현할 수 있는 모든 값이 계약 안에 드는가"를 잴 수 있다.
"""
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import obd
import pytest

from conftest import FakeObdConnection
from obd_bridge.mapping import (PID_SPECS, InvalidVehicleId, build_payload,
                                check_vehicle_id, format_timestamp, topic_for)
from obd_bridge.reader import PidReader

VID = "OBD-TEST-01"
TS = "2026-10-06T03:00:00.123Z"
PID_BYTE = {"SPEED": 0x0D, "RPM": 0x0C, "COOLANT_TEMP": 0x05, "THROTTLE_POS": 0x11,
            "FUEL_LEVEL": 0x2F, "CONTROL_MODULE_VOLTAGE": 0x42}


def decode(name, *data_bytes):
    """python-OBD decoder를 원시 응답 바이트로 직접 호출한다(ELM327 없이)."""
    msg = SimpleNamespace(data=bytearray([0x41, PID_BYTE[name], *data_bytes]))
    return obd.commands[name].decode([msg])


def readings_from_bytes(a, b):
    """1바이트 PID는 a, 2바이트 PID는 (a, b)."""
    conn = FakeObdConnection({
        "SPEED": decode("SPEED", a),
        "RPM": decode("RPM", a, b),
        "COOLANT_TEMP": decode("COOLANT_TEMP", a),
        "THROTTLE_POS": decode("THROTTLE_POS", a),
        "FUEL_LEVEL": decode("FUEL_LEVEL", a),
        "CONTROL_MODULE_VOLTAGE": decode("CONTROL_MODULE_VOLTAGE", a, b),
    })
    return PidReader(conn, command_lookup=lambda n: obd.commands[n]).read()


def test_pid_specs_match_contract_ranges(contract):
    """매핑 표의 범위가 계약 모듈의 숫자와 한 글자도 다르지 않다."""
    assert {s.field: (s.low, s.high) for s in PID_SPECS} == contract._NUMERIC


def test_optional_fields_match_contract(contract):
    """선택 PID 목록이 계약 모듈의 OPTIONAL_NUMERIC과 같다(ADR-030). 어긋나면 한쪽이 필수를 생략하거나
    선택을 필수처럼 버린다."""
    from obd_bridge.mapping import OPTIONAL_FIELDS, REQUIRED_FIELDS
    assert set(OPTIONAL_FIELDS) == contract.OPTIONAL_NUMERIC
    assert set(REQUIRED_FIELDS) == set(contract._NUMERIC) - contract.OPTIONAL_NUMERIC


def test_pid_commands_are_the_requested_pids():
    for s in PID_SPECS:
        assert obd.commands[s.command].command == s.pid.encode()


@pytest.mark.parametrize("a,b", [(0, 0), (255, 255), (0, 255), (255, 0), (1, 1),
                                 (40, 0), (128, 64), (7, 208)])
def test_edge_byte_values_pass_contract(contract, a, b):
    r = build_payload(readings_from_bytes(a, b), VID, TS)
    assert r.ok, r
    contract.validate(json.dumps(r.payload))


def test_every_expressible_pid_value_passes_contract(contract):
    """2바이트 PID의 전 범위(0..65535)를 훑는다. 1바이트 PID는 a 바이트로 함께 돈다."""
    checked = 0
    for raw in range(0, 65536):
        a, b = raw >> 8, raw & 0xFF
        r = build_payload(readings_from_bytes(a, b), VID, TS)
        assert r.ok, (raw, r)
        contract.validate(json.dumps(r.payload))  # 전수 — 표본이 아니다(65536회 약 0.7초)
        checked += 1
    assert checked == 65536


def test_values_keep_obd_resolution(contract):
    """rpm 0.25 단위, 전압 0.001 V를 반올림·절삭하지 않는다."""
    r = readings_from_bytes(0x1F, 0x41)  # rpm = (0x1F41)/4 = 2000.25
    assert r["rpm"] == 2000.25
    assert r["battery_voltage"] == pytest.approx(8.001)
    payload = build_payload(r, VID, TS).payload
    assert json.loads(json.dumps(payload))["rpm"] == 2000.25
    contract.validate(json.dumps(payload))


def test_payload_has_no_fabricated_optional_fields():
    """gps·dtc_codes는 OBD PID로 얻지 않는다 — 키 자체가 없어야 한다."""
    p = build_payload(readings_from_bytes(10, 10), VID, TS).payload
    assert "gps" not in p and "dtc_codes" not in p
    assert set(p) == {"vehicle_id", "timestamp", *(s.field for s in PID_SPECS)}


def test_units_are_converted_explicitly():
    """라이브러리가 mph·°F를 돌려줘도 km/h·°C로 바뀐다(조용한 단위 혼동 방지)."""
    u = obd.Unit
    conn = FakeObdConnection({
        "SPEED": u.Quantity(62.137119, u.mph),
        "RPM": 800.0 * u.rpm,
        "COOLANT_TEMP": u.Quantity(212.0, u.degF),
        "THROTTLE_POS": 10.0 * u.percent,
        "FUEL_LEVEL": 50.0 * u.percent,
        "CONTROL_MODULE_VOLTAGE": 13800 * u.millivolt,
    })
    r = PidReader(conn, command_lookup=lambda n: obd.commands[n]).read()
    assert r["speed"] == pytest.approx(100.0, abs=1e-4)
    assert r["engine_temp"] == pytest.approx(100.0)
    assert r["battery_voltage"] == pytest.approx(13.8)


def test_timestamp_format_matches_contract_and_simulator(contract):
    ts = format_timestamp(datetime(2026, 10, 6, 3, 4, 5, 678901, tzinfo=timezone.utc))
    assert ts == "2026-10-06T03:04:05.678Z"
    assert contract._TIMESTAMP.match(ts)


@pytest.mark.parametrize("vid", ["OBD1", "A" * 20, "KMH-123-AB"])
def test_vehicle_id_accepted(vid):
    assert check_vehicle_id(vid) == vid


@pytest.mark.parametrize("vid", ["", "ABC", "A" * 21, "obd-01", "OBD_01", "OBD 01"])
def test_vehicle_id_rejected_at_startup(vid):
    with pytest.raises(InvalidVehicleId):
        check_vehicle_id(vid)


def test_topic_matches_backend_pattern():
    assert topic_for("vehicle/telemetry/", VID) == f"vehicle/telemetry/{VID}"


@pytest.mark.parametrize("field,value", [("speed", 255.5), ("engine_temp", -41.0),
                                         ("battery_voltage", float("nan")),
                                         ("fuel_level", float("inf"))])
def test_out_of_range_is_not_clipped_or_sent(field, value):
    r = readings_from_bytes(10, 10)
    r[field] = value
    res = build_payload(r, VID, TS)
    assert res.payload is None and res.out_of_range == (field,)
