"""tools/measure_polling.py — note는 필수 인자(상수 아님), 어댑터 정보 수집은 실패해도 측정을 막지 않는다(동글 계획 D4)."""
from __future__ import annotations

import importlib.util
import os

import pytest

from conftest import FakeResponse, ROOT


@pytest.fixture(scope="module")
def tool():
    path = os.path.join(ROOT, "tools", "measure_polling.py")
    spec = importlib.util.spec_from_file_location("measure_polling", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_note_is_required(tool, capsys):
    with pytest.raises(SystemExit):
        tool.build_parser().parse_args(["--port", "socket://127.0.0.1:35000"])
    assert "--note" in capsys.readouterr().err


def test_note_is_passed_through_not_hardcoded(tool):
    a = tool.build_parser().parse_args(["--port", "COM5", "--note", "실차 — 코나 2017"])
    assert a.note == "실차 — 코나 2017"
    src = open(tool.__file__, encoding="utf-8").read()
    assert '"note": a.note' in src
    assert '"note": "실차 아님' not in src


class FakeConn:
    def __init__(self, responses, broken=()):
        self.responses = responses
        self.broken = set(broken)
        self.forced = []

    def _maybe(self, name, value):
        if name in self.broken:
            raise RuntimeError("boom")
        return value

    def port_name(self):
        return self._maybe("port_name", "COM5")

    def protocol_id(self):
        return self._maybe("protocol_id", "6")

    def protocol_name(self):
        return self._maybe("protocol_name", "ISO 15765-4 (CAN 11/500)")

    def status(self):
        return self._maybe("status", "Car Connected")

    def query(self, cmd, force=False):
        self.forced.append((cmd.name, force))
        if cmd.name in self.broken:
            raise RuntimeError("boom")
        return FakeResponse(self.responses.get(cmd.name))


def test_adapter_info_records_versions(tool):
    conn = FakeConn({"ELM_VERSION": "ELM327 v1.4b", "STI": "STN2255 v5.6.19"})
    info = tool.adapter_info(conn)
    assert info == {
        "port_name": "COM5", "protocol_id": "6", "protocol_name": "ISO 15765-4 (CAN 11/500)",
        "status": "Car Connected", "elm_version_ATI": "ELM327 v1.4b", "stn_version_STI": "STN2255 v5.6.19",
    }
    # ATI/STI는 supports() 표에 없으므로 force=True로 보내야 python-OBD가 거르지 않는다
    assert ("ELM_VERSION", True) in conn.forced and ("STI", True) in conn.forced


def test_adapter_info_failures_do_not_abort(tool):
    conn = FakeConn({"ELM_VERSION": None}, broken={"status", "STI"})
    info = tool.adapter_info(conn)
    assert info["status"] == "error: RuntimeError"
    assert info["stn_version_STI"] == "error: RuntimeError"
    assert info["elm_version_ATI"] is None  # 무응답은 None — 지어내지 않는다
    assert info["port_name"] == "COM5"
