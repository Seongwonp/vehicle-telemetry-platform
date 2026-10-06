"""python-OBD로 6개 PID를 읽는다. 연결 객체는 주입받는다(테스트는 가짜 연결을 쓴다)."""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from .mapping import PID_SPECS, quantity_to_float

log = logging.getLogger("obd_bridge.reader")


def default_command_lookup(name: str) -> Any:
    import obd  # 하드웨어 없는 테스트에서 import 비용·부작용을 피한다
    return obd.commands[name]


class PidReader:
    """한 주기에 PID 6개를 순서대로 query한다.

    `connection`은 python-OBD `obd.OBD`와 같은 모양이면 된다: ``supports(cmd)``, ``query(cmd)``.
    응답의 ``value``가 None이거나 ``is_null()``이면 그 필드는 **None**이다 — 0으로 채우지 않는다.
    """

    def __init__(self, connection: Any,
                 command_lookup: Callable[[str], Any] = default_command_lookup):
        self.connection = connection
        self.commands = {s.field: command_lookup(s.command) for s in PID_SPECS}
        self.unsupported = tuple(
            s.field for s in PID_SPECS if not connection.supports(self.commands[s.field]))
        for field in self.unsupported:
            log.warning("PID 미지원: %s — 이 필드가 없으면 계약상 payload를 만들 수 없다", field)

    def read(self) -> dict[str, Optional[float]]:
        out: dict[str, Optional[float]] = {}
        for spec in PID_SPECS:
            cmd = self.commands[spec.field]
            if spec.field in self.unsupported:
                out[spec.field] = None
                continue
            resp = self.connection.query(cmd)
            if resp is None or (hasattr(resp, "is_null") and resp.is_null()):
                out[spec.field] = None
                continue
            out[spec.field] = quantity_to_float(getattr(resp, "value", None), spec.unit)
        return out
