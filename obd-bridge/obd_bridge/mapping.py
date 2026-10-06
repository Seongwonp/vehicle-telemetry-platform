"""OBD-II PID → 텔레메트리 입력 계약 매핑.

계약의 단일 기준은 `docs/telemetry-schema-decision-table.md`이고, 숫자는
`anomaly-detector/contract.py`와 같아야 한다(테스트가 그 모듈로 직접 검증한다).

불변식 — 지우면 안 되는 이유:
- **지원하지 않거나 null인 PID를 0으로 채우지 않는다.** 0.0은 "시속 0으로 주행 중"처럼
  그럴듯한 값이라 눈에 띄지 않는다 — 결정표 2절의 1·2·10·14번이 그 사례다.
  - **필수 PID**(속도·RPM·냉각수·스로틀)가 하나라도 없으면 payload를 만들지 않는다(보내지 않는다).
  - **선택 PID**(연료량 012F·제어 모듈 전압 0142, ADR-030)가 없으면 **그 키를 생략**하고 보낸다.
    `null`도 보내지 않는다 — 계약상 둘 다 허용이지만 "없음"을 한 모양으로 둔다.
- **gps·dtc_codes는 선택 필드라 생략한다.** OBD에서 나오지 않는 값을 지어내지 않는다.
- 단위는 python-OBD가 돌려주는 Pint 수량을 **명시 단위로 변환**해서 꺼낸다. 라이브러리
  기본 단위가 바뀌어도 mph·°F가 km/h·°C 자리에 조용히 들어가지 않게 한다.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

# (계약 필드, python-OBD 명령 이름, PID, 변환 단위, 최소, 최대)
@dataclass(frozen=True)
class PidSpec:
    field: str
    command: str
    pid: str
    unit: Optional[str]  # Pint 단위 이름. None이면 무차원(백분율 등 magnitude 그대로)
    low: float
    high: float


PID_SPECS: tuple[PidSpec, ...] = (
    PidSpec("speed", "SPEED", "010D", "kph", 0.0, 255.0),
    PidSpec("rpm", "RPM", "010C", "rpm", 0.0, 16383.75),
    PidSpec("engine_temp", "COOLANT_TEMP", "0105", "degC", -40.0, 215.0),
    PidSpec("throttle_position", "THROTTLE_POS", "0111", "percent", 0.0, 100.0),
    PidSpec("fuel_level", "FUEL_LEVEL", "012F", "percent", 0.0, 100.0),
    PidSpec("battery_voltage", "CONTROL_MODULE_VOLTAGE", "0142", "volt", 0.0, 65.535),
)

# 계약상 선택인 PID(ADR-030) — contract.py의 OPTIONAL_NUMERIC과 같아야 한다(테스트가 대조한다).
# 차종에 따라 지원하지 않을 수 있는 PID라서(어느 차가 그런지는 실차 미확인), 필수로 두면 그런 차량은 아무것도 보낼 수 없었다.
OPTIONAL_FIELDS = ("fuel_level", "battery_voltage")
REQUIRED_FIELDS = tuple(s.field for s in PID_SPECS if s.field not in OPTIONAL_FIELDS)

# contract.py의 _VEHICLE_ID와 같다. 백엔드 MQTT 입구의 topic 정규식도 같은 패턴이다.
VEHICLE_ID_PATTERN = re.compile(r"^[A-Z0-9-]{4,20}$")


class InvalidVehicleId(ValueError):
    pass


def check_vehicle_id(vehicle_id: str) -> str:
    if not VEHICLE_ID_PATTERN.match(vehicle_id or ""):
        raise InvalidVehicleId(
            f"vehicle_id {vehicle_id!r}는 계약 ^[A-Z0-9-]{{4,20}}$ 밖이다 — 보내면 전부 DLQ로 간다")
    return vehicle_id


def topic_for(prefix: str, vehicle_id: str) -> str:
    # 백엔드는 topic의 마지막 칸과 payload vehicle_id가 다르면 TOPIC_VEHICLE_MISMATCH로 거부한다.
    return f"{prefix.rstrip('/')}/{vehicle_id}"


def format_timestamp(dt: datetime) -> str:
    """시뮬레이터와 같은 형식: UTC, 밀리초, `Z`. 초 단위면 InfluxDB에서 덮어써진다(README 참고)."""
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def quantity_to_float(value: Any, unit: Optional[str]) -> Optional[float]:
    """python-OBD 응답 값을 float로. 값이 없거나 해석할 수 없으면 None(0이 아니다)."""
    if value is None:
        return None
    if hasattr(value, "to") and hasattr(value, "magnitude"):
        if unit is not None:
            value = value.to(unit)
        value = value.magnitude
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


@dataclass
class BuildResult:
    payload: Optional[dict]
    missing: tuple[str, ...] = ()        # 없는 **필수** PID — 있으면 payload 없음
    out_of_range: tuple[str, ...] = ()
    omitted: tuple[str, ...] = ()        # 없어서 키를 생략한 **선택** PID — payload는 있다

    @property
    def ok(self) -> bool:
        return self.payload is not None


def build_payload(readings: Mapping[str, Optional[float]], vehicle_id: str,
                  timestamp: str) -> BuildResult:
    """계약 필드 → 값(float 또는 None)에서 payload를 만든다.

    필수 PID가 하나라도 None이면 payload 없음(missing). 선택 PID가 None이면 그 키만 생략(omitted).
    **있는** 값이 범위 밖·비유한이면 payload 없음(out_of_range) — 선택 PID도 마찬가지다.
    범위 밖 값은 **잘라 넣지 않는다** — 자르면 값이 조용히 바뀐다. 이 경우 보내지 않을지
    보내서 DLQ로 격리할지는 결정 대기다(README).
    """
    missing = tuple(f for f in REQUIRED_FIELDS if readings.get(f) is None)
    if missing:
        return BuildResult(None, missing=missing)
    omitted = tuple(f for f in OPTIONAL_FIELDS if readings.get(f) is None)

    bad = []
    for spec in PID_SPECS:
        v = readings.get(spec.field)
        if v is None:
            continue  # 선택 PID 없음 — 위에서 omitted로 셌다
        if not math.isfinite(v) or v < spec.low or v > spec.high:
            bad.append(spec.field)
    if bad:
        return BuildResult(None, out_of_range=tuple(bad), omitted=omitted)

    payload = {"vehicle_id": vehicle_id, "timestamp": timestamp}
    for spec in PID_SPECS:
        if spec.field in omitted:
            continue  # 키 자체를 넣지 않는다 — 0도 null도 아니다
        payload[spec.field] = readings[spec.field]
    # gps, dtc_codes: 계약상 선택. OBD PID로 얻지 않으므로 키 자체를 넣지 않는다.
    return BuildResult(payload, omitted=omitted)
