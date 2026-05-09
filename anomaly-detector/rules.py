"""
룰 기반 이상 감지 — CLAUDE.md 기준 임계값
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class AnomalyEvent:
    vehicle_id: str
    timestamp: str
    anomaly_type: str
    field: str
    value: float
    threshold: str
    severity: str      # HIGH | MEDIUM
    detector: str = "RULE"


# (필드명, 조건 함수, 이상 유형, 임계값 설명, 심각도)
_RULES = [
    ("engine_temp",      lambda v: v > 105.0,  "엔진 과열",     "engine_temp > 105°C",          "HIGH"),
    ("rpm",              lambda v: v > 6000,    "RPM 과부하",    "rpm > 6000",                   "HIGH"),
    ("battery_voltage",  lambda v: v < 11.5,   "배터리 저전압",  "battery_voltage < 11.5V",      "MEDIUM"),
    ("battery_voltage",  lambda v: v > 15.0,   "배터리 과전압",  "battery_voltage > 15.0V",      "HIGH"),
    ("speed",            lambda v: v > 200.0,  "과속",          "speed > 200km/h",              "HIGH"),
]


def detect(data: dict) -> list[AnomalyEvent]:
    anomalies = []

    for field, condition, anomaly_type, threshold, severity in _RULES:
        value = data.get(field)
        if value is not None and condition(float(value)):
            anomalies.append(AnomalyEvent(
                vehicle_id=data["vehicle_id"],
                timestamp=data["timestamp"],
                anomaly_type=anomaly_type,
                field=field,
                value=float(value),
                threshold=threshold,
                severity=severity,
                detector="RULE",
            ))

    dtc_codes = data.get("dtc_codes", [])
    if dtc_codes:
        anomalies.append(AnomalyEvent(
            vehicle_id=data["vehicle_id"],
            timestamp=data["timestamp"],
            anomaly_type="DTC 진단 코드 감지",
            field="dtc_codes",
            value=0.0,
            threshold=f"dtc_codes 비어있지 않음: {','.join(dtc_codes)}",
            severity="HIGH",
            detector="RULE",
        ))

    return anomalies
