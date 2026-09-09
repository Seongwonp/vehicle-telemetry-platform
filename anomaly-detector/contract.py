"""텔레메트리 입력 계약 — **저장 경로와 같은 계약**을 감지 경로에도 적용한다 (P0-2a).

## 왜 있는가

`anomaly-detector`는 `vehicle-telemetry`를 별도 Consumer Group으로 읽고 자체 파싱한다.
2026-09-09 측정에서 저장 경로와 **21칸 중 8칸이 갈렸다** — 다섯 칸은 저장이 거부하는데
감지는 알림도 로그도 지표도 없이 조용히 지나갔고, 세 칸은 저장 안 된 데이터로 알림이 나갔다.

정책은 **공통 입력 계약을 유지하는 것**으로 정했다(`docs/anomaly-path-contract.md` 3절).
필수 센서 누락/null, 범위 밖, 타입 오류, unknown 필드는 **감지 이전에 거부**한다.
정상 필드만 골라 부분 감지하지 않는다 — 그건 별개 설계이고 필요성이 확인되면 그때 한다.

## 무엇이 계약이 **아닌가**

- **신선도(freshness)가 아니다.** `timestamp`는 **형식만** 본다. 과거 데이터를 임의로
  거부하면 DLQ 재처리와 백필이 불가능해진다. 신선도가 필요하면 별도 정책으로 만든다.
- **`dtc_codes`는 선택 필드다.** 없어도 되고 비어도 된다. 다만 있으면 원소가 형식을 지켜야
  한다 — 저장 계약과 같다.
- **숫자 문자열은 허용한다.** `{"speed": "87.3"}`은 통과한다. Jackson의 강제 변환을
  끄지 않기로 한 결정이고(`docs/telemetry-schema-decision-table.md` 3절), 여기도 맞춘다.

## 사유 선택 순서 — Java와 같아야 한다

Jackson도 `json.loads`도 문서를 앞에서부터 읽다가 처음 만난 문제에서 멈추므로,
그대로 두면 **사유가 payload의 필드 순서에 따라 달라진다.** 그래서 단계를 나눈다.

1. ``MALFORMED_JSON`` — 문서 전체가 JSON으로 읽히는가
2. ``TYPE_MISMATCH`` — 최상위가 객체인가 (배열·숫자·문자열·``null`` 거부)
3. ``UNKNOWN_FIELD`` — 계약에 없는 필드가 있는가 (**이름순** 전부 보고)
4. ``TYPE_MISMATCH`` — 각 필드가 계약 타입으로 해석되는가
5. ``PAYLOAD_VALIDATION_FAILED`` — 값이 계약 범위 안인가 (위반은 **정렬**해서 보고)

같은 순서를 `backend/.../TelemetryDecoder.java`가 구현하고,
어긋나지 않는지는 `contract-fixtures/cases.json`을 양쪽이 읽어 검사한다.

## 언어 차이 — 여기가 제일 미끄럽다

**Python에서 `bool`은 `int`의 하위 타입이다.** `isinstance(True, int)`가 참이고
`float(True)`가 `1.0`이다. 그냥 `float()`로 변환하면 `{"speed": true}`가 **1.0으로
조용히 통과**한다 — Java는 `TYPE_MISMATCH`로 거부한다. 그래서 bool을 먼저 걸러낸다.

`float("nan")`·`float("inf")`도 성공한다. Java는 `@DecimalMin/@DecimalMax`가
NaN·Infinity를 거부하는 것을 실측으로 확인했으므로(2026-09-09), 여기서도 막는다.
"""
from __future__ import annotations

import json
import math
import re
from datetime import datetime
from typing import Any

# ── 사유 코드 — Java TelemetryContractException과 **문자열이 같아야 한다** ──────
MALFORMED_JSON = "MALFORMED_JSON"
UNKNOWN_FIELD = "UNKNOWN_FIELD"
TYPE_MISMATCH = "TYPE_MISMATCH"
PAYLOAD_VALIDATION_FAILED = "PAYLOAD_VALIDATION_FAILED"

REASONS = (MALFORMED_JSON, UNKNOWN_FIELD, TYPE_MISMATCH, PAYLOAD_VALIDATION_FAILED)


class ContractViolation(Exception):
    """계약 위반. **검증기가 명시적으로 만든 것**이라 영구 실패로 분류해도 된다.

    일반 `TypeError`/`KeyError`와 구분되는 것이 요점이다 — 그쪽은 구현 버그일 수도 있어서
    `dlq-tools/dlq.py`가 자동으로 영구 처리하지 않는다.
    """

    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}" if detail else reason)


# ── 계약 — docs/telemetry-schema-decision-table.md가 숫자의 단일 기준이다 ──────
# OBD-II PID의 **표현 범위**를 채택한 입력 계약이다. 물리적 한계가 아니다.
_NUMERIC = {
    # 필드명: (최소, 최대)  — PID 근거는 VehicleTelemetry.java의 필드 주석에 있다
    "speed": (0.0, 255.0),              # PID 0D, 1바이트 부호 없음
    "rpm": (0.0, 16383.75),             # PID 0C, 2바이트/4 → 0.25 단위
    "engine_temp": (-40.0, 215.0),      # PID 05, 냉각수 온도
    "throttle_position": (0.0, 100.0),  # PID 11, 백분율
    "fuel_level": (0.0, 100.0),         # PID 2F, 백분율
    "battery_voltage": (0.0, 65.535),   # PID 42, 제어 모듈 전압
}
_GPS_RANGE = {"lat": (-90.0, 90.0), "lng": (-180.0, 180.0)}

_VEHICLE_ID = re.compile(r"^[A-Z0-9-]{4,20}$")
_DTC_CODE = re.compile(r"^[PBCU][0-9]{4}$")

KNOWN_FIELDS = frozenset(
    {"vehicle_id", "timestamp", "gps", "dtc_codes"} | set(_NUMERIC))
_GPS_FIELDS = frozenset({"lat", "lng"})

# ISO-8601. **형식만 본다 — 시각이 과거인지 미래인지는 보지 않는다.**
# 재처리·백필이 가능해야 하므로 신선도는 계약이 아니다(모듈 docstring 참고).
#
# **VehicleTelemetry.TIMESTAMP_PATTERN과 같아야 한다.**
# 어느 한쪽 표준 라이브러리도 기준이 될 수 없다 — 2026-09-09 실측:
#
#   입력                          Java Instant.parse   Python fromisoformat
#   2026-09-09t10:00:00z (소문자)  OK                   거부
#   2026-09-09T10:00:00 (오프셋 X) 거부                 OK
#   2026-09-09 10:00:00Z (공백)    거부                 OK
#
# 그래서 **이 정규식이 계약**이고 양쪽이 그걸 구현한다. 오프셋은 필수다.
_TIMESTAMP = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d{1,9})?([Zz]|[+-]\d{2}:\d{2})$")


def _is_real_instant(value: str) -> bool:
    """달력에 있는 날인가. 정규식은 **모양**만 보므로 `2026-13-45T99:00:00Z`가 통과한다.

    Java는 `Instant.parse`가 이걸 잡는다. Python의 `fromisoformat`은 소문자 `t`/`z`를
    거부하고 초 소수점 자릿수 제한도 버전마다 달라서, **정규화한 뒤에** 넘긴다.
    """
    v = value.replace("t", "T").replace("z", "Z")
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    # 초 소수점을 6자리로 자른다 — 파이썬 datetime의 해상도가 마이크로초다.
    # 계약은 9자리까지 허용하므로 **자르는 것이지 거부하는 것이 아니다.**
    m = re.search(r"\.(\d+)", v)
    if m and len(m.group(1)) > 6:
        v = v[:m.start(1) + 6] + v[m.end(1):]
    try:
        datetime.fromisoformat(v)
        return True
    except ValueError:
        return False


def _reject_constant(name: str):
    """`NaN`/`Infinity`/`-Infinity` 리터럴을 거부한다 — JSON 표준에 없는 값이다.

    Jackson은 `ALLOW_NON_NUMERIC_NUMBERS`가 꺼져 있어 파싱 단계에서 거부한다(2026-09-08 실측).
    Python은 **기본으로 받아들이므로** 여기서 맞춰야 두 경로가 같아진다.
    """
    raise ContractViolation(MALFORMED_JSON, f"non-numeric literal: {name}")


def _as_number(field: str, value: Any) -> float:
    """계약 타입(숫자)으로 해석한다. 숫자 문자열은 허용하고 **bool은 거부**한다."""
    # bool을 먼저 본다 — Python에서 bool은 int의 하위 타입이라 아래 검사를 다 통과한다.
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
    # list, dict, 그 밖의 타입
    raise ContractViolation(TYPE_MISMATCH, field)


def validate(payload: str) -> dict:
    """payload를 계약에 맞게 해석한다.

    :returns: 검증을 통과한 dict (원본 그대로 — 룰이 원본 JSON을 보는 기존 동작을 유지한다)
    :raises ContractViolation: 계약 위반. ``reason``이 사유 코드다.
    """
    # ── 1) 문서 전체가 JSON인가 ────────────────────────────────
    # **`parse_constant`를 반드시 준다.** Python의 `json.loads`는 기본으로
    # `NaN`/`Infinity`/`-Infinity` **리터럴을 받아들인다** — JSON 표준에는 없는 값이고
    # Jackson(Boot 기본 설정)은 거부한다. 안 막으면 감지기만 조용히 통과시킨다.
    try:
        root = json.loads(payload, parse_constant=_reject_constant)
    except ContractViolation:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as e:
        raise ContractViolation(MALFORMED_JSON, type(e).__name__) from None

    # ── 2) 최상위가 객체인가 ───────────────────────────────────
    # `json.loads("null")`은 예외 없이 None을 돌려준다 — Java의 readTree도 같다.
    # 이걸 그냥 넘기면 아래에서 AttributeError가 나고, 그건 계약 예외가 아니다.
    if root is None:
        raise ContractViolation(TYPE_MISMATCH, "(최상위 null)")
    if not isinstance(root, dict):
        raise ContractViolation(
            TYPE_MISMATCH, f"(최상위가 객체가 아니다: {type(root).__name__})")

    # ── 3) 계약에 없는 필드 ────────────────────────────────────
    # **이름순으로 전부** 보고한다. 문서 순서로 하면 같은 오류가 payload 순서에 따라
    # 다른 필드를 가리켜서 사유가 재현되지 않는다.
    unknown = sorted(set(root) - KNOWN_FIELDS)
    if unknown:
        raise ContractViolation(UNKNOWN_FIELD, ",".join(unknown))

    # ── 4) 타입 ────────────────────────────────────────────────
    # 필드를 **이름순으로** 훑는다 — dict 순서(= 문서 순서)로 돌면 또 순서 의존이 된다.
    numbers: dict[str, float] = {}
    for field in sorted(_NUMERIC):
        if field in root and root[field] is not None:
            numbers[field] = _as_number(field, root[field])

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

    # ── 5) 값의 범위 ───────────────────────────────────────────
    # 위반을 **모아서 정렬**한다. 같은 입력이 항상 같은 문자열을 내야 재현이 된다.
    # 메시지 문구는 Java(Hibernate Validator)와 다르다 — 우리가 맞추는 것은 **사유 코드**다.
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
        # **형식만** 본다. 오래된 시각이라고 거부하지 않는다.
        violations.append("timestamp must be ISO-8601")
    elif not _is_real_instant(timestamp):
        violations.append("timestamp is not a real instant")

    for field in sorted(_NUMERIC):
        low, high = _NUMERIC[field]
        if field not in numbers:
            violations.append(f"{field} must not be null")
            continue
        v = numbers[field]
        if math.isnan(v) or math.isinf(v):
            # Java는 @DecimalMin/@DecimalMax가 NaN·Infinity를 둘 다 거부한다(실측).
            violations.append(f"{field} must be a finite number")
        elif v < low:
            violations.append(f"{field} must be greater than or equal to {low:g}")
        elif v > high:
            violations.append(f"{field} must be less than or equal to {high:g}")

    if gps is not None:
        # gps가 있으면 안이 완전해야 한다 — 한쪽만 있는 것은 계약 위반이다.
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
