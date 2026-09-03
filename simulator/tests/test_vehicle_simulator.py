"""
vehicle_simulator.py VehicleState 테스트
"""
import sys
import os
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import vehicle_simulator as vs
from vehicle_simulator import VehicleState

# 이상 감지 임계값 (CLAUDE.md 기준)
THRESHOLD_ENGINE_TEMP = 105.0
THRESHOLD_RPM = 6000
THRESHOLD_BATTERY_LOW = 11.5
THRESHOLD_BATTERY_HIGH = 15.0
THRESHOLD_SPEED = 200.0


class TestVehicleState초기화:

    def test_초기_속도_0(self):
        state = VehicleState(vehicle_id="TEST-001")
        assert state.speed == 0.0

    def test_초기_엔진온도_냉간(self):
        state = VehicleState(vehicle_id="TEST-001")
        assert state.engine_temp == 20.0  # 냉간 시동 상태

    def test_초기_RPM_공회전(self):
        state = VehicleState(vehicle_id="TEST-001")
        assert state.rpm == 800

    def test_연료잔량_랜덤_초기화(self):
        state = VehicleState(vehicle_id="TEST-001")
        assert 30.0 <= state.fuel_level <= 90.0


class TestVehicleStateNext:

    def test_페이로드_필수_필드_완전성(self):
        state = VehicleState(vehicle_id="TEST-001")
        payload = state.next()

        required = [
            "vehicle_id", "timestamp", "speed", "rpm",
            "engine_temp", "throttle_position", "fuel_level",
            "battery_voltage", "gps", "dtc_codes"
        ]
        for field in required:
            assert field in payload, f"필수 필드 누락: {field}"

    def test_GPS_위경도_포함(self):
        state = VehicleState(vehicle_id="TEST-001")
        payload = state.next()
        assert "lat" in payload["gps"]
        assert "lng" in payload["gps"]

    def test_정상주행_엔진온도_임계값_미만(self):
        state = VehicleState(vehicle_id="TEST-001")
        # 100틱 동안 정상 데이터 생성
        for _ in range(100):
            payload = state.next()
        assert payload["engine_temp"] < THRESHOLD_ENGINE_TEMP

    def test_워밍업_후_온도_상승(self):
        state = VehicleState(vehicle_id="TEST-001")
        initial_temp = state.engine_temp
        for _ in range(30):
            state.next()
        assert state.engine_temp > initial_temp

    def test_틱마다_연료_소모(self):
        state = VehicleState(vehicle_id="TEST-001")
        initial_fuel = state.fuel_level
        for _ in range(100):
            state.next()
        assert state.fuel_level < initial_fuel

    def test_정상_DTC코드_없음(self):
        state = VehicleState(vehicle_id="TEST-001")
        for _ in range(50):
            payload = state.next()
        assert payload["dtc_codes"] == []

    def test_vehicle_id_페이로드에_포함(self):
        state = VehicleState(vehicle_id="KR-GA-1234")
        payload = state.next()
        assert payload["vehicle_id"] == "KR-GA-1234"

    def test_타임스탬프_밀리초_정밀도(self):
        state = VehicleState(vehicle_id="TEST-001")
        timestamp = state.next()["timestamp"]

        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        assert len(timestamp) == 24
        assert parsed.microsecond % 1000 == 0


class TestVehicleStateAnomalyInjection:

    def test_이상값_주입_임계값_초과(self):
        """inject_anomaly는 반드시 하나 이상의 임계값을 초과해야 함"""
        state = VehicleState(vehicle_id="TEST-001")

        violations = 0
        for _ in range(30):
            payload = state.inject_anomaly()
            if (payload["engine_temp"] > THRESHOLD_ENGINE_TEMP
                    or payload["rpm"] > THRESHOLD_RPM
                    or payload["battery_voltage"] < THRESHOLD_BATTERY_LOW
                    or payload["battery_voltage"] > THRESHOLD_BATTERY_HIGH
                    or payload["speed"] > THRESHOLD_SPEED
                    or len(payload["dtc_codes"]) > 0):
                violations += 1

        assert violations == 30  # 매번 이상값이어야 함

    def test_이상값_vehicle_id_유지(self):
        state = VehicleState(vehicle_id="TEST-001")
        payload = state.inject_anomaly()
        assert payload["vehicle_id"] == "TEST-001"

    def test_이상값_타임스탬프_포함(self):
        state = VehicleState(vehicle_id="TEST-001")
        payload = state.inject_anomaly()
        assert "timestamp" in payload
        assert payload["timestamp"] != ""


# ── 복합 이상 / 드리프트 (ML 평가용 시나리오) ────────────────────────────

class TestCompositeAnomaly:
    """복합 이상의 핵심 전제: 개별 필드는 전부 룰 임계값 안이어야 한다.
    하나라도 룰에 걸리면 'ML만 잡을 수 있는 정답'이 아니게 되므로 측정이 무의미해진다."""

    def test_모든_복합_이상이_룰_임계값을_넘지_않는다(self):
        state = vs.VehicleState(vehicle_id="SIM-001")
        for _ in range(300):
            payload = state.inject_composite_anomaly()

            assert payload["engine_temp"] <= vs.THRESHOLD_ENGINE_TEMP_HIGH
            assert payload["rpm"] <= vs.THRESHOLD_RPM_HIGH
            assert vs.THRESHOLD_BATTERY_LOW <= payload["battery_voltage"] <= vs.THRESHOLD_BATTERY_HIGH
            assert payload["speed"] <= vs.THRESHOLD_SPEED_HIGH
            assert payload["dtc_codes"] == []

    def test_룰_이상_직후에도_룰_임계값을_넘지_않는다(self):
        """상태가 이월돼 오염되는 경우를 잡는다.

        복합 이상만 연속 호출하면 안 걸리는 버그가 실제로 있었다 — inject_anomaly()가
        dtc_code를 남기면 다음 복합 이상 페이로드에 그 DTC가 실려 룰이 잡아버렸다
        (실측에서 복합 19,908건 중 261건 오염). 두 주입을 섞어야 재현된다."""
        state = vs.VehicleState(vehicle_id="SIM-001")
        for _ in range(300):
            state.inject_anomaly()
            payload = state.inject_composite_anomaly()

            assert payload["dtc_codes"] == []
            assert payload["engine_temp"] <= vs.THRESHOLD_ENGINE_TEMP_HIGH
            assert payload["rpm"] <= vs.THRESHOLD_RPM_HIGH
            assert vs.THRESHOLD_BATTERY_LOW <= payload["battery_voltage"] <= vs.THRESHOLD_BATTERY_HIGH
            assert payload["speed"] <= vs.THRESHOLD_SPEED_HIGH

    def test_정답_로그를_남긴다(self, caplog):
        state = vs.VehicleState(vehicle_id="SIM-007")
        with caplog.at_level("WARNING"):
            payload = state.inject_composite_anomaly()

        gt = [r for r in caplog.messages if r.startswith(vs.GROUND_TRUTH_PREFIX)]
        assert len(gt) == 1
        assert f"vehicle={payload['vehicle_id']}" in gt[0]
        assert f"ts={payload['timestamp']}" in gt[0]
        assert "kind=composite" in gt[0]

    def test_룰_이상도_정답_로그를_남긴다(self, caplog):
        state = vs.VehicleState(vehicle_id="SIM-007")
        with caplog.at_level("WARNING"):
            payload = state.inject_anomaly()

        gt = [r for r in caplog.messages if r.startswith(vs.GROUND_TRUTH_PREFIX)]
        assert len(gt) == 1
        assert "kind=rule" in gt[0]
        assert f"ts={payload['timestamp']}" in gt[0]


class TestDrift:

    def test_기본값이면_드리프트가_없다(self):
        state = vs.VehicleState(vehicle_id="SIM-001")
        assert state._drift_offset() == 0.0

    def test_시작_시간_전에는_0이다(self, monkeypatch):
        monkeypatch.setattr(vs, "DRIFT_TEMP_DELTA", 10.0)
        monkeypatch.setattr(vs, "DRIFT_START_SECONDS", 100.0)
        state = vs.VehicleState(vehicle_id="SIM-001")
        state._started_at = vs.time.time()  # 방금 시작

        assert state._drift_offset() == 0.0

    def test_램프_구간을_지나면_최대치로_수렴한다(self, monkeypatch):
        monkeypatch.setattr(vs, "DRIFT_TEMP_DELTA", 10.0)
        monkeypatch.setattr(vs, "DRIFT_START_SECONDS", 0.0)
        monkeypatch.setattr(vs, "DRIFT_RAMP_SECONDS", 100.0)
        state = vs.VehicleState(vehicle_id="SIM-001")

        state._started_at = vs.time.time() - 50.0    # 램프 절반
        assert state._drift_offset() == pytest.approx(5.0, abs=0.2)

        state._started_at = vs.time.time() - 999.0   # 램프 종료 후
        assert state._drift_offset() == pytest.approx(10.0)
