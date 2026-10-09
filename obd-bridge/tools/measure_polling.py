"""PID 6개 한 주기를 연속으로 읽는 데 걸리는 시간을 잰다. 에뮬레이터·실동글 공용.

`--note`는 **필수**다 — 결과 JSON의 `note`에 그대로 들어가 "무엇을 쟀는지"를 남긴다
(예전에는 "실차 아님 — ELM327-emulator"가 상수로 박혀 실차 결과에도 찍혔다 — 동글 계획 D4).
어댑터 정보(python-OBD가 아는 포트·프로토콜, `ATI`/`STI` 응답)도 `adapter`에 남긴다.

사용(에뮬레이터를 먼저 띄운다 — README 참고):
    python tools/measure_polling.py --port socket://127.0.0.1:35000 --baudrate 38400 -n 300         --note "실차 아님 — ELM327-emulator"
실동글:
    python tools/measure_polling.py --port COM5 --baudrate <OBDwiz 값> -n 300         --note "실차 — 코나 2017 가솔린, OBDLink EX, 공회전" --out measure_polling_<run>.json

주의: ELM327-emulator 4.0.0(Windows, TCP)은 클라이언트가 끊은 뒤 오류 로그를 무한히 쏟아냈다.
그래서 한 번 연결해서 끝까지 재고, 측정마다 에뮬레이터를 새로 띄운다.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import obd  # noqa: E402

from obd_bridge.mapping import PID_SPECS, build_payload, format_timestamp  # noqa: E402
from obd_bridge.reader import PidReader  # noqa: E402


def pct(sorted_vals, p):
    # nearest-rank
    k = max(0, min(len(sorted_vals) - 1, int(round(p / 100 * len(sorted_vals) + 0.5)) - 1))
    return sorted_vals[k]


def summarize(vals_ms):
    s = sorted(vals_ms)
    return {"n": len(s), "median_ms": round(statistics.median(s), 2), "p95_ms": round(pct(s, 95), 2),
            "max_ms": round(s[-1], 2), "min_ms": round(s[0], 2), "mean_ms": round(statistics.fmean(s), 2)}


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", required=True)
    ap.add_argument("--baudrate", type=int, default=38400)
    ap.add_argument("-n", type=int, default=300)
    ap.add_argument("--fast", action="store_true", help="python-OBD fast 모드")
    ap.add_argument("--out", help="원시 측정값 JSON 저장 경로")
    ap.add_argument("--note", required=True,
                    help='결과 JSON의 note. 예: "실차 아님 — ELM327-emulator" / "실차 — <차종>, <동글>"')
    return ap


def _pkg_version(name):
    import importlib.metadata as md
    try:
        return md.version(name)
    except md.PackageNotFoundError:
        return None


# OBDLink(STN 칩) 펌웨어 문자열. ELM327 클론·에뮬레이터는 "?"나 빈 응답을 줄 수 있다 — 그대로 남긴다.
STI = obd.OBDCommand("STI", "STN firmware version", b"STI", 0, obd.decoders.raw_string, obd.ECU.UNKNOWN, False)


def adapter_info(conn) -> dict:
    """python-OBD 연결에서 어댑터 정보를 모은다. 하나가 실패해도 나머지는 남기고, 실패는 문자열로 적는다."""
    info = {}
    for key, fn in (("port_name", conn.port_name), ("protocol_id", conn.protocol_id),
                    ("protocol_name", conn.protocol_name), ("status", conn.status)):
        try:
            v = fn()
            info[key] = v if isinstance(v, (str, int, float, type(None))) else str(v)
        except Exception as e:  # noqa: BLE001 — 정보 수집 실패가 측정을 막으면 안 된다
            info[key] = f"error: {type(e).__name__}"
    for key, cmd in (("elm_version_ATI", obd.commands.ELM_VERSION), ("stn_version_STI", STI)):
        try:
            r = conn.query(cmd, force=True)
            info[key] = None if r.is_null() else str(r.value)
        except Exception as e:  # noqa: BLE001
            info[key] = f"error: {type(e).__name__}"
    return info


def main(argv=None):
    a = build_parser().parse_args(argv)

    t0 = time.perf_counter()
    conn = obd.OBD(a.port, baudrate=a.baudrate, fast=a.fast, timeout=5)
    connect_ms = (time.perf_counter() - t0) * 1000
    if not conn.is_connected():
        print(json.dumps({"error": f"not connected: {conn.status()}"}))
        return 1

    protocol = conn.protocol_name()
    adapter = adapter_info(conn)  # 측정 루프 전에 — 루프 시간에 섞이지 않게
    reader = PidReader(conn)
    per_pid = {s.field: [] for s in PID_SPECS}
    cycles, incomplete = [], 0
    # PidReader.read()와 같은 순서·같은 호출을 재되, PID별 시간도 같이 남긴다.
    for _ in range(a.n):
        c0 = time.perf_counter()
        readings = {}
        for s in PID_SPECS:
            q0 = time.perf_counter()
            r = conn.query(reader.commands[s.field])
            per_pid[s.field].append((time.perf_counter() - q0) * 1000)
            readings[s.field] = None if r.is_null() else r.value.to(s.unit).magnitude
        cycles.append((time.perf_counter() - c0) * 1000)
        if not build_payload(readings, "OBD-EMU-01", format_timestamp(datetime.now(timezone.utc))).ok:
            incomplete += 1
    conn.close()

    result = {
        "note": a.note,
        "when_utc": format_timestamp(datetime.now(timezone.utc)),
        "env": {"os": platform.platform(), "python": platform.python_version(),
                "obd": _pkg_version("obd"), "ELM327-emulator": _pkg_version("ELM327-emulator"),
                "pyserial": _pkg_version("pyserial")},
        "port": a.port, "baudrate": a.baudrate, "fast": a.fast,
        "protocol": protocol,
        "adapter": adapter,
        "connect_ms": round(connect_ms, 1),
        "unsupported": list(reader.unsupported),
        "incomplete_cycles": incomplete,
        "cycle": summarize(cycles),
        "per_pid": {k: summarize(v) for k, v in per_pid.items()},
    }
    # 원시값을 먼저 쓴다 — 콘솔 인코딩(cp949) 오류로 측정을 한 번 통째로 잃었다.
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump({**result, "raw_cycle_ms": cycles, "raw_per_pid_ms": per_pid}, f, ensure_ascii=False)
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
