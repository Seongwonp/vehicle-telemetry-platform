"""
rules.py 이상 감지 룰 테스트

CLAUDE.md 기준 임계값:
- engine_temp > 105°C
- rpm > 6000
- battery_voltage < 11.5V 또는 > 15V
- speed > 200km/h
- dtc_codes 비어있지 않을 때
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from rules import detect, AnomalyEvent


def make_data(**kwargs) -> dict:
    """정상 차량 데이터 기본값 — 필요한 필드만 오버라이드"""
    base = {
        "vehicle_id": "TEST-001",
        "timestamp": "2026-03-01T10:00:00Z",
        "speed": 80.0,
        "rpm": 2000,
        "engine_temp": 90.0,
        "battery_voltage": 13.8,
        "fuel_level": 60.0,
        "dtc_codes": [],
    }
    base.update(kwargs)
    return base


# ── 정상 케이스 ──────────────────────────────────────────────────

class TestNormalData:

    def test_정상_데이터_이상없음(self):
        assert detect(make_data()) == []

    def test_경계값_엔진온도_105도_정상(self):
        anomalies = detect(make_data(engine_temp=105.0))
        engine = [a for a in anomalies if a.field == "engine_temp"]
        assert engine == []

    def test_경계값_RPM_6000_정상(self):
        anomalies = detect(make_data(rpm=6000))
        rpm = [a for a in anomalies if a.field == "rpm"]
        assert rpm == []

    def test_경계값_배터리_11점5V_정상(self):
        anomalies = detect(make_data(battery_voltage=11.5))
        bat = [a for a in anomalies if a.field == "battery_voltage"]
        assert bat == []

    def test_경계값_배터리_15V_정상(self):
        anomalies = detect(make_data(battery_voltage=15.0))
        bat = [a for a in anomalies if a.field == "battery_voltage"]
        assert bat == []

    def test_경계값_속도_200kmh_정상(self):
        anomalies = detect(make_data(speed=200.0))
        speed = [a for a in anomalies if a.field == "speed"]
        assert speed == []

    def test_빈_DTC_코드_정상(self):
        assert detect(make_data(dtc_codes=[])) == []


# ── 이상 감지 케이스 ─────────────────────────────────────────────

class TestAnomalyDetection:

    def test_엔진_과열_감지(self):
        anomalies = detect(make_data(engine_temp=106.0))
        engine = [a for a in anomalies if a.field == "engine_temp"]
        assert len(engine) == 1
        assert engine[0].severity == "HIGH"
        assert engine[0].detector == "RULE"

    def test_RPM_과부하_감지(self):
        anomalies = detect(make_data(rpm=6001))
        rpm = [a for a in anomalies if a.field == "rpm"]
        assert len(rpm) == 1
        assert rpm[0].severity == "HIGH"

    def test_배터리_저전압_감지(self):
        anomalies = detect(make_data(battery_voltage=11.4))
        bat = [a for a in anomalies if a.field == "battery_voltage"]
        assert len(bat) == 1
        assert bat[0].severity == "MEDIUM"  # 저전압은 MEDIUM

    def test_배터리_과전압_감지(self):
        anomalies = detect(make_data(battery_voltage=15.1))
        bat = [a for a in anomalies if a.field == "battery_voltage"]
        assert len(bat) == 1
        assert bat[0].severity == "HIGH"  # 과전압은 HIGH

    def test_과속_감지(self):
        anomalies = detect(make_data(speed=201.0))
        speed = [a for a in anomalies if a.field == "speed"]
        assert len(speed) == 1
        assert speed[0].severity == "HIGH"

    def test_DTC코드_감지(self):
        anomalies = detect(make_data(dtc_codes=["P0300"]))
        dtc = [a for a in anomalies if a.field == "dtc_codes"]
        assert len(dtc) == 1
        assert dtc[0].severity == "HIGH"

    def test_복수_DTC코드_1건만_생성(self):
        anomalies = detect(make_data(dtc_codes=["P0300", "P0171", "P0420"]))
        dtc = [a for a in anomalies if a.field == "dtc_codes"]
        assert len(dtc) == 1  # DTC는 여러 개여도 이벤트 1건

    def test_복수_이상_동시_감지(self):
        anomalies = detect(make_data(engine_temp=110.0, rpm=7000, speed=210.0))
        assert len(anomalies) >= 3

    def test_이상_이벤트_vehicle_id_포함(self):
        anomalies = detect(make_data(engine_temp=110.0, vehicle_id="KR-GA-1234"))
        assert all(a.vehicle_id == "KR-GA-1234" for a in anomalies)

    def test_이상_이벤트_timestamp_포함(self):
        ts = "2026-03-01T10:00:00Z"
        anomalies = detect(make_data(engine_temp=110.0, timestamp=ts))
        assert all(a.timestamp == ts for a in anomalies)

    def test_모든_감지기_유형이_RULE(self):
        anomalies = detect(make_data(engine_temp=110.0, rpm=7000))
        assert all(a.detector == "RULE" for a in anomalies)


# ── 극단값 케이스 ─────────────────────────────────────────────────

class TestEdgeCases:

    def test_매우_높은_엔진온도(self):
        anomalies = detect(make_data(engine_temp=200.0))
        engine = [a for a in anomalies if a.field == "engine_temp"]
        assert len(engine) == 1

    def test_배터리전압_0V(self):
        anomalies = detect(make_data(battery_voltage=0.0))
        bat = [a for a in anomalies if a.field == "battery_voltage"]
        assert len(bat) == 1

    def test_속도_0은_정상(self):
        anomalies = detect(make_data(speed=0.0))
        speed = [a for a in anomalies if a.field == "speed"]
        assert speed == []


# ── 선택 필드(ADR-030) — 값이 없으면 그 룰은 평가하지 않는다 ─────────

class TestOptionalFields:

    def _without(self, *fields) -> dict:
        data = make_data()
        for f in fields:
            data.pop(f, None)
        return data

    def test_전압_없음은_저전압_오탐이_아니다(self):
        # 0으로 보면 "0 < 11.5"로 매 메시지 저전압 알림이 나간다 — 지원하지 않는 차량 전체가 오탐.
        # 바로 위 test_배터리전압_0V가 "실제 0V는 알림"임을 고정한다 — 없음과 0은 다르다.
        assert detect(self._without("battery_voltage")) == []

    def test_전압_null도_평가하지_않는다(self):
        assert detect(make_data(battery_voltage=None)) == []

    def test_연료_없음은_알림이_없다(self):
        assert detect(self._without("fuel_level")) == []

    def test_전압이_없어도_다른_룰은_돈다(self):
        anomalies = detect(make_data(battery_voltage=None, rpm=6500, engine_temp=110.0))
        assert sorted(a.field for a in anomalies) == ["engine_temp", "rpm"]

    def test_전압이_있으면_룰은_그대로다(self):
        # 선택화가 "전압 룰 끔"이 아니다 — 값이 오면 기존 임계 그대로.
        assert [a.anomaly_type for a in detect(make_data(battery_voltage=11.0))] == ["배터리 저전압"]
        assert [a.anomaly_type for a in detect(make_data(battery_voltage=15.5))] == ["배터리 과전압"]
