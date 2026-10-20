"""텔레메트리 입력 계약 — 저장 경로(TelemetryDecoder)와 같은 계약을 감지 경로에도 적용한다.

필수 센서 누락/null, 범위 밖, 타입 오류, unknown 필드는 감지 전에 거부한다.
정책과 경위: docs/anomaly-path-contract.md.

계약이 아닌 것:
- 신선도. timestamp는 형식만 본다(과거 시각을 거부하면 DLQ 재처리·백필이 막힌다).
- fuel_level·battery_voltage는 선택 필드다(ADR-030). 없거나 null이면 통과, 0으로 바꾸지 않는다.
  빈 문자열은 거부한다(Java와 같게).
- dtc_codes는 선택이지만 있으면 원소 형식을 지켜야 한다.
- 숫자 문자열("87.3")은 허용한다(docs/telemetry-schema-decision-table.md 3절).

사유 선택 순서는 Java와 같아야 한다 — 안 그러면 사유가 payload 필드 순서에 따라 달라진다.

1. ``MALFORMED_JSON`` — 문서 전체가 JSON으로 읽히는가
2. ``TYPE_MISMATCH`` — 최상위가 객체인가 (배열·숫자·문자열·``null`` 거부)
3. ``UNKNOWN_FIELD`` — 계약에 없는 필드가 있는가 (**이름순** 전부 보고)
4. ``TYPE_MISMATCH`` — 각 필드가 계약 타입으로 해석되는가
5. ``PAYLOAD_VALIDATION_FAILED`` — 값이 계약 범위 안인가 (위반은 **정렬**해서 보고)

두 구현이 어긋나지 않는지는 contract-fixtures/cases.json을 양쪽이 읽어 검사한다.
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime
from typing import Any

# 사유 코드 문자열은 Java TelemetryContractException과 같아야 한다.
MALFORMED_JSON = "MALFORMED_JSON"
UNKNOWN_FIELD = "UNKNOWN_FIELD"
TYPE_MISMATCH = "TYPE_MISMATCH"
PAYLOAD_VALIDATION_FAILED = "PAYLOAD_VALIDATION_FAILED"

REASONS = (MALFORMED_JSON, UNKNOWN_FIELD, TYPE_MISMATCH, PAYLOAD_VALIDATION_FAILED)


class ContractViolation(Exception):
    """검증기가 명시적으로 낸 계약 위반 — 이것만 DLQ에서 영구 실패로 분류한다.

    일반 TypeError/KeyError는 구현 버그일 수 있어 dlq.py가 영구 처리하지 않는다.
    """

    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}" if detail else reason)


# OBD-II PID 표현 범위. 숫자 기준은 docs/telemetry-schema-decision-table.md다.
_NUMERIC = {
    "speed": (0.0, 255.0),              # PID 0D, 1바이트 부호 없음
    "rpm": (0.0, 16383.75),             # PID 0C, 2바이트/4 → 0.25 단위
    "engine_temp": (-40.0, 215.0),      # PID 05, 냉각수 온도
    "throttle_position": (0.0, 100.0),  # PID 11, 백분율
    "fuel_level": (0.0, 100.0),         # PID 2F, 백분율
    "battery_voltage": (0.0, 65.535),   # PID 42, 제어 모듈 전압
}
# ADR-030. VehicleTelemetry.java에서 @NotNull이 빠진 필드와 정확히 같아야 한다.
OPTIONAL_NUMERIC = frozenset({"fuel_level", "battery_voltage"})
_GPS_RANGE = {"lat": (-90.0, 90.0), "lng": (-180.0, 180.0)}

_VEHICLE_ID = re.compile(r"^[A-Z0-9-]{4,20}$")
_DTC_CODE = re.compile(r"^[PBCU][0-9]{4}$")

KNOWN_FIELDS = frozenset(
    {"vehicle_id", "timestamp", "gps", "dtc_codes"} | set(_NUMERIC))
_GPS_FIELDS = frozenset({"lat", "lng"})

# VehicleTelemetry.TIMESTAMP_PATTERN과 같아야 한다. Instant.parse와 fromisoformat의 허용
# 범위가 서로 달라 표준 라이브러리 대신 이 정규식이 계약이다(docs/anomaly-path-contract.md).
_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d{1,9})?([Zz]|[+-]\d{2}:\d{2})$")


def _is_real_instant(value: str) -> bool:
    """달력에 있는 시각인가(정규식은 `2026-13-45T99:00:00Z`도 통과시킨다).

    fromisoformat은 소문자 t/z와 긴 소수점을 버전에 따라 거부해서 정규화한 뒤 넘긴다.
    """
    v = value.replace("t", "T").replace("z", "Z")
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    # datetime 해상도가 마이크로초라 6자리로 자른다(계약은 9자리 허용 — 거부 아님).
    m = re.search(r"\.(\d+)", v)
    if m and len(m.group(1)) > 6:
        v = v[:m.start(1) + 6] + v[m.end(1):]
    try:
        datetime.fromisoformat(v)
        return True
    except ValueError:
        return False


def _reject_constant(name: str):
    """NaN/Infinity 리터럴 거부. Jackson은 거부하고 json.loads는 기본으로 받는다."""
    raise ContractViolation(MALFORMED_JSON, f"non-numeric literal: {name}")


def _as_number(field: str, value: Any) -> float:
    """계약 타입(숫자)으로 해석한다. 숫자 문자열은 허용하고 **bool은 거부**한다."""
    # bool은 int의 하위 타입이라 먼저 걸러야 한다(안 그러면 true가 1.0으로 통과).
    if isinstance(value, bool):
        raise ContractViolation(TYPE_MISMATCH, field)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # 빈 문자열은 Java에서 null이 되고 @NotNull에 걸린다 — 사유를 맞춘다.
        if value.strip() == "":
            raise ContractViolation(PAYLOAD_VALIDATION_FAILED, f"{field} must not be null")
        try:
            return float(value)
        except ValueError:
            raise ContractViolation(TYPE_MISMATCH, field) from None
    raise ContractViolation(TYPE_MISMATCH, field)


def validate(payload: str) -> dict:
    """payload를 계약에 맞게 해석한다.

    :returns: 검증을 통과한 원본 dict
    :raises ContractViolation: 계약 위반. ``reason``이 사유 코드다.
    """
    # 1) parse_constant가 없으면 NaN/Infinity 리터럴이 감지기만 통과한다.
    try:
        root = json.loads(payload, parse_constant=_reject_constant)
    except ContractViolation:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
        raise ContractViolation(MALFORMED_JSON, type(e).__name__) from None

    # 2) "null"은 예외 없이 None이 된다 — 여기서 막지 않으면 계약 예외가 아닌 AttributeError가 난다.
    if root is None:
        raise ContractViolation(TYPE_MISMATCH, "(최상위 null)")
    if not isinstance(root, dict):
        raise ContractViolation(
            TYPE_MISMATCH, f"(최상위가 객체가 아니다: {type(root).__name__})")

    # 3) unknown 필드는 이름순으로 전부 보고한다(문서 순서면 재현이 안 된다).
    unknown = sorted(set(root) - KNOWN_FIELDS)
    if unknown:
        raise ContractViolation(UNKNOWN_FIELD, ",".join(unknown))

    # 4) 타입. 문서 순서에 의존하지 않도록 이름순으로 훑는다.
    numbers: dict[str, float] = {}
    # 선택 필드의 빈 문자열은 위반이지만 Java처럼 5단계에서 함께 보고해야 사유가 같다.
    blank_optional: list[str] = []
    for field in sorted(_NUMERIC):
        if field in root and root[field] is not None:
            value = root[field]
            if field in OPTIONAL_NUMERIC and isinstance(value, str) and value.strip() == "":
                blank_optional.append(field)
                continue
            numbers[field] = _as_number(field, value)

    gps = root.get("gps")
    gps_numbers: dict[str, float] = {}
    if gps is not None:
        if not isinstance(gps, dict):
            raise ContractViolation(TYPE_MISMATCH, "gps")
        gps_unknown = sorted(set(gps) - _GPS_FIELDS)
        if gps_unknown:
            raise ContractViolation(UNKNOWN_FIELD, ",".join("gps." + f for f in gps_unknown))
        for field in sorted(_GPS_FIELDS):
            if field in gps and gps[field] is not None:
                gps_numbers[field] = _as_number("gps." + field, gps[field])

    dtc = root.get("dtc_codes")
    if dtc is not None and not isinstance(dtc, list):
        raise ContractViolation(TYPE_MISMATCH, "dtc_codes")

    for name in ("vehicle_id", "timestamp"):
        value = root.get(name)
        if value is not None and not isinstance(value, str):
            raise ContractViolation(TYPE_MISMATCH, name)

    # 5) 범위. 위반을 모아 정렬한다. Java와 맞추는 것은 문구가 아니라 사유 코드다.
    violations: list[str] = []

    vehicle_id = root.get("vehicle_id")
    if vehicle_id is None or vehicle_id == "":
        violations.append("vehicleId must not be blank")
    elif not _VEHICLE_ID.match(vehicle_id):
        violations.append("vehicleId must match pattern")

    timestamp = root.get("timestamp")
    if timestamp is None or timestamp == "":
        violations.append("timestamp must not be blank")
    elif not _TIMESTAMP.match(timestamp):
        violations.append("timestamp must be ISO-8601")
    elif not _is_real_instant(timestamp):
        violations.append("timestamp is not a real instant")

    for field in sorted(_NUMERIC):
        low, high = _NUMERIC[field]
        if field not in numbers:
            if field in blank_optional:
                violations.append(f"{field} must not be blank")
            elif field not in OPTIONAL_NUMERIC:
                violations.append(f"{field} must not be null")
            continue
        v = numbers[field]
        if math.isnan(v) or math.isinf(v):
            # Java @DecimalMin/@DecimalMax와 같게 NaN·Infinity를 거부한다.
            violations.append(f"{field} must be a finite number")
        elif v < low:
            violations.append(f"{field} must be greater than or equal to {low:g}")
        elif v > high:
            violations.append(f"{field} must be less than or equal to {high:g}")

    if gps is not None:
        # gps가 있으면 lat/lng 둘 다 있어야 한다.
        for field in sorted(_GPS_FIELDS):
            low, high = _GPS_RANGE[field]
            if field not in gps_numbers:
                violations.append(f"gps.{field} must not be null")
                continue
            v = gps_numbers[field]
            if math.isnan(v) or math.isinf(v):
                violations.append(f"gps.{field} must be a finite number")
            elif v < low or v > high:
                violations.append(f"gps.{field} out of range")

    if isinstance(dtc, list):
        for i, code in enumerate(dtc):
            if code is None:
                violations.append(f"dtcCodes[{i}] must not be null")
            elif not isinstance(code, str) or not _DTC_CODE.match(code):
                violations.append(f"dtcCodes[{i}] must match pattern")

    if violations:
        raise ContractViolation(PAYLOAD_VALIDATION_FAILED, "; ".join(sorted(violations)))

    return root
