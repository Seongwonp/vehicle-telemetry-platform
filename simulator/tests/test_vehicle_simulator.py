"""
vehicle_simulator.py VehicleState 테스트
"""
import sys
import os
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
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
