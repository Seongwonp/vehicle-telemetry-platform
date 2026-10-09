"""리허설 실행 래퍼 — obd_bridge.bridge.main을 그대로 부르고, N초 뒤 SIGINT(Ctrl+C와 같은 경로)를 올린다.

- obd-bridge/.env를 os.environ에 올린다(값은 출력하지 않는다). CLI 인자가 우선.
- 종료 후 Bridge.stats / publisher.stats / spool 미전송 / 거부 seq / reason code 관찰을 JSON으로 쓴다.
- 브리지 코드는 바꾸지 않는다: 인스턴스를 잡으려고 생성자와 _is_failure를 감싼다(동작 동일).
usage: python run_bridge.py <env_file> <duration_s> <stats_json_out> -- <bridge args...>
"""
import collections, dataclasses, json, os, signal, sys, threading, time
from datetime import datetime, timezone

env_file, duration, out = sys.argv[1], float(sys.argv[2]), sys.argv[3]
args = sys.argv[sys.argv.index("--") + 1:]
with open(env_file, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

sys.path.insert(0, os.getcwd())
import obd_bridge.bridge as b
import obd_bridge.publisher as p

captured = {}
reasons = collections.Counter()
_orig_fail = p._is_failure
def _is_failure(rc):
    caller = sys._getframe(1).f_code.co_name
    try:
        val = int(rc.value) if hasattr(rc, "value") else (None if rc is None else int(rc))
    except Exception:
        val = repr(rc)
    reasons[f"{caller}:{val}:{rc}"] += 1
    return _orig_fail(rc)
p._is_failure = _is_failure

class B(b.Bridge):
    def __init__(self, *a, **k):
        super().__init__(*a, **k); captured["bridge"] = self
class P(b.SpoolPublisher):
    def __init__(self, *a, **k):
        super().__init__(*a, **k); captured["publisher"] = self
b.Bridge, b.SpoolPublisher = B, P

started = datetime.now(timezone.utc)
threading.Timer(duration, lambda: signal.raise_signal(signal.SIGINT)).start()
rc = b.main(args)
ended = datetime.now(timezone.utc)
br, pub = captured.get("bridge"), captured.get("publisher")
res = {
    "argv": args, "exit_code": rc,
    "wrapper_start_utc": started.isoformat(), "wrapper_end_utc": ended.isoformat(),
    "bridge_stats": dataclasses.asdict(br.stats) if br else None,
    "publisher_stats": dataclasses.asdict(pub.stats) if pub else None,
    "spool_unsent": len(pub.spool) if pub else None,
    "rejected_seqs": sorted(pub._rejected) if pub else None,
    "reason_codes_seen": dict(reasons),
}
with open(out, "w", encoding="utf-8", newline="\n") as f:
    json.dump(res, f, ensure_ascii=False, indent=2); f.write("\n")
print("STATS_JSON", json.dumps(res, ensure_ascii=False))
sys.exit(rc)
