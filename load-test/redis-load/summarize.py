#!/usr/bin/env python3
"""한 실행의 원본(gen_*.csv, live.csv, metrics.csv, dockerstats.csv, timeline.txt)을 구간별로 집계하고
PLAN_20260914.md §6의 판정 기준을 계산한다.

사용: python summarize.py <evidence 디렉터리>   → <dir>/summary.txt
"""
import csv
import glob
import math
import os
import re
import sys
from collections import defaultdict

D = sys.argv[1]


def timeline():
    t = {}
    with open(os.path.join(D, "timeline.txt")) as f:
        for line in f:
            if ":" in line:
                k, v = line.split(":", 1)
                try:
                    t[k.strip()] = float(v.strip())
                except ValueError:
                    pass
    return t


T = timeline()
# baseline 끝을 중지 명령 5초 전으로 둔다 — 중지 직전에 보낸 요청(최대 4초)이 장애에 걸리는 것을 기준에 섞지 않는다.
PHASES = [
    ("baseline", T["warm_end_ms"], T["stop_cmd_ms"] - 5000),
    ("outage", T["stop_done_ms"], T["start_cmd_ms"]),
    ("reconnect", T["start_cmd_ms"], T["start_cmd_ms"] + 60000),
    ("recovery_tail", T["start_cmd_ms"] + 90000, T["run_end_ms"]),
]


def phase_of(ms):
    for name, s, e in PHASES:
        if s <= ms < e:
            return name
    return None


def pct(vals, p):
    if not vals:
        return float("nan")
    v = sorted(vals)
    k = max(0, min(len(v) - 1, math.ceil(p / 100.0 * len(v)) - 1))
    return v[k]


def expected(phase, r):
    if r["outcome"] != "ok":
        return False
    http, code, hdr, path = r["http"], r["code"], r["ratelimit_hdr"], r["path"]
    if path == "liveness":
        return http == "200"
    if path == "query":
        if phase in ("baseline", "recovery_tail"):
            return http == "200" and hdr == "yes"
        if phase == "outage":
            return http == "200" and hdr == "no"
        return http == "200"
    if path == "refresh":
        if phase in ("baseline", "recovery_tail"):
            return http == "401"
        if phase == "outage":
            return http == "503" and code == "REDIS_UNAVAILABLE"
        return http == "401" or (http == "503" and code == "REDIS_UNAVAILABLE")
    return False


# ── 요청 ──────────────────────────────────────────────
rows = []
for p in sorted(glob.glob(os.path.join(D, "gen_*.csv"))) + [os.path.join(D, "live.csv")]:
    if os.path.exists(p):
        with open(p, newline="") as f:
            rows.extend(csv.DictReader(f))

req = defaultdict(list)
for r in rows:
    ph = phase_of(float(r["intended_ms"]))
    if ph:
        req[(ph, r["path"])].append(r)

gens = int(T.get("gens", 0))
intended_rate = {
    "query": gens * T.get("rate_per_gen", 0) / 2,
    "refresh": gens * T.get("rate_per_gen", 0) / 2,
    "liveness": T.get("live_rate", 0),
}

out = []
P = out.append
P("# Redis 장애 × 낮은 부하 — 구간별 요약")
P(f"# 단계 {T.get('level_n', '?')}: /api 생성기 {gens}개 × {T.get('rate_per_gen')} req/s, liveness {T.get('live_rate')} req/s")
P("# 구간(요청은 '예정 시각'으로 분류): " + " / ".join(
    f"{n} {(s - T['stop_done_ms']) / 1000:+.0f}~{(e - T['stop_done_ms']) / 1000:+.0f}s" for n, s, e in PHASES)
  + " (중지 완료 기준)")
P("")
P("## 요청")
P("phase,path,n,rate/s(예정),예상밖,timeout,conn_err,http분포,lat_p50_ms,lat_p95_ms,lat_p99_ms,lat_max_ms,lag_p95_ms,lag_max_ms")
stats = {}
for ph, s, e in PHASES:
    secs = (e - s) / 1000.0
    for path in ("query", "refresh", "liveness"):
        rs = req.get((ph, path), [])
        lat = [float(r["end_ms"]) - float(r["sent_ms"]) for r in rs]
        lag = [float(r["sent_ms"]) - float(r["intended_ms"]) for r in rs]
        unexpected = sum(1 for r in rs if not expected(ph, r))
        timeouts = sum(1 for r in rs if r["outcome"] == "timeout")
        conn = sum(1 for r in rs if r["outcome"].startswith(("conn_error", "http_error")))
        dist = defaultdict(int)
        for r in rs:
            key = r["http"] + ("/" + r["code"] if r["code"] != "-" else "") + ("+hdr" if r["ratelimit_hdr"] == "yes" else "")
            dist[key] += 1
        st = dict(n=len(rs), rate=len(rs) / secs if secs > 0 else 0, unexpected=unexpected, timeouts=timeouts,
                  conn=conn, p50=pct(lat, 50), p95=pct(lat, 95), p99=pct(lat, 99), max=max(lat) if lat else float("nan"),
                  lag95=pct(lag, 95), lagmax=max(lag) if lag else float("nan"))
        stats[(ph, path)] = st
        P(f"{ph},{path},{st['n']},{st['rate']:.2f}({intended_rate[path]:.2f}),{unexpected},{timeouts},{conn},"
          f"{' '.join(f'{k}:{v}' for k, v in sorted(dist.items()))},"
          f"{st['p50']:.0f},{st['p95']:.0f},{st['p99']:.0f},{st['max']:.0f},{st['lag95']:.0f},{st['lagmax']:.0f}")

# ── 서버 지표 ─────────────────────────────────────────
samples = defaultdict(lambda: defaultdict(float))  # ts -> key -> value
scrape_ms = {}
scrape_errors = defaultdict(int)
mpath = os.path.join(D, "metrics.csv")
if os.path.exists(mpath):
    with open(mpath, newline="") as f:
        for r in csv.DictReader(f):
            ts = int(r["ts_ms"])
            scrape_ms[ts] = int(r["scrape_ms"])
            s = r["series"]
            if s == "__scrape_error__":
                ph = phase_of(ts)
                if ph:
                    scrape_errors[ph] += 1
                continue
            try:
                v = float(r["value"])
            except ValueError:
                continue
            name = s.split("{", 1)[0]
            m = samples[ts]
            if name == "tomcat_threads_busy_threads":
                m["busy"] += v
            elif name == "tomcat_threads_current_threads":
                m["current"] += v
            elif name == "tomcat_threads_config_max_threads":
                m["max_cfg"] += v
            elif name == "http_server_requests_active_seconds_active_count":
                m["active"] += v
            elif name == "jvm_threads_live_threads":
                m["live"] += v
            elif name == "jvm_threads_states_threads":
                st_label = re.search(r'state="([a-z-]+)"', s)
                if st_label:
                    m["state_" + st_label.group(1)] += v
            elif name == "process_cpu_usage":
                m["cpu"] += v * 100
            elif name == "jvm_memory_used_bytes" and 'area="heap"' in s:
                m["heap_mb"] += v / 1048576

P("")
P("## 서버 지표 (actuator, 약 2초 간격)")
P("phase,표본,busy_mean,busy_max,current_max,max_threads,active_req_mean,active_req_max,live_threads_max,"
  "waiting_max,timed_waiting_max,cpu_mean%,cpu_max%,heap_max_MB,scrape_max_ms,scrape_errors")
mstats = {}
for ph, s, e in PHASES:
    ts_list = [ts for ts in samples if s <= ts < e]

    def col(k):
        return [samples[ts].get(k, 0.0) for ts in ts_list]

    def mean(v):
        return sum(v) / len(v) if v else float("nan")

    def mx(v):
        return max(v) if v else float("nan")

    ms = dict(n=len(ts_list), busy_mean=mean(col("busy")), busy_max=mx(col("busy")), current_max=mx(col("current")),
              max_cfg=mx(col("max_cfg")), active_mean=mean(col("active")), active_max=mx(col("active")),
              live_max=mx(col("live")), waiting_max=mx(col("state_waiting")),
              timed_max=mx(col("state_timed-waiting")), cpu_mean=mean(col("cpu")), cpu_max=mx(col("cpu")),
              heap_max=mx(col("heap_mb")), scrape_max=mx([scrape_ms[ts] for ts in ts_list]))
    mstats[ph] = ms
    P(f"{ph},{ms['n']},{ms['busy_mean']:.1f},{ms['busy_max']:.0f},{ms['current_max']:.0f},{ms['max_cfg']:.0f},"
      f"{ms['active_mean']:.1f},{ms['active_max']:.0f},{ms['live_max']:.0f},{ms['waiting_max']:.0f},{ms['timed_max']:.0f},"
      f"{ms['cpu_mean']:.1f},{ms['cpu_max']:.1f},{ms['heap_max']:.0f},{ms['scrape_max']:.0f},{scrape_errors.get(ph, 0)}")
P("주: active_req에는 지표를 긁는 요청 자신(1건)이 포함된다.")

# ── 컨테이너 자원 ──────────────────────────────────────
def mem_mib(text):
    m = re.match(r"\s*([\d.]+)\s*([KMG]i?B)", text)
    if not m:
        return float("nan")
    v, u = float(m.group(1)), m.group(2)
    return v / 1024 if u.startswith("K") else v * 1024 if u.startswith("G") else v


dstats = defaultdict(list)
dpath = os.path.join(D, "dockerstats.csv")
if os.path.exists(dpath):
    with open(dpath) as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4 or not parts[0].isdigit():
                continue
            ts, name, cpu, mem = int(parts[0]), parts[1], parts[2], parts[3]
            ph = phase_of(ts)
            if not ph:
                continue
            group = "generators" if name.startswith("redisload-gen-") else name
            try:
                dstats[(ph, group)].append((float(cpu.rstrip("%")), mem_mib(mem)))
            except ValueError:
                pass

P("")
P("## 컨테이너 자원 (docker stats, 생성기는 컨테이너별 표본을 모은 값)")
P("phase,container,표본,cpu_mean%,cpu_max%,mem_max_MiB")
for ph, _, _ in PHASES:
    for group in sorted({g for (p, g) in dstats if p == ph}):
        v = dstats[(ph, group)]
        cpus = [c for c, _ in v]
        mems = [m for _, m in v if not math.isnan(m)]
        P(f"{ph},{group},{len(v)},{sum(cpus) / len(cpus):.1f},{max(cpus):.1f},{max(mems) if mems else float('nan'):.0f}")

# ── 판정 (PLAN §6) ─────────────────────────────────────
P("")
P("## 판정 (PLAN_20260914.md §6)")
invalid, stop = [], []
for (ph, path), st in stats.items():
    if st["n"] and (st["lag95"] > 100 or st["lagmax"] > 1000):
        invalid.append(f"생성기 지연 초과 {ph}/{path} p95={st['lag95']:.0f} max={st['lagmax']:.0f}")
for path in ("query", "refresh", "liveness"):
    st = stats[("baseline", path)]
    want = intended_rate[path]
    if want and abs(st["rate"] - want) / want > 0.10:
        invalid.append(f"baseline 요청률 {path} {st['rate']:.2f} (예정 {want:.2f})")
gen_cpu = [c for (ph, g), v in dstats.items() if g == "generators" for c, _ in v]
if gen_cpu and sum(gen_cpu) / len(gen_cpu) > 50:
    invalid.append(f"생성기 CPU 평균 {sum(gen_cpu) / len(gen_cpu):.1f}%")

for (ph, path), st in stats.items():
    if st["timeouts"] or st["conn"]:
        stop.append(f"[1] {ph}/{path} timeout {st['timeouts']} · 연결 오류 {st['conn']}")
for ph in ("baseline", "recovery_tail"):
    for path in ("query", "refresh", "liveness"):
        st = stats[(ph, path)]
        if st["n"] and st["unexpected"] / st["n"] > 0.01:
            stop.append(f"[2] {ph}/{path} 예상 밖 {st['unexpected']}/{st['n']}")
for path in ("query", "refresh", "liveness"):
    b, t = stats[("baseline", path)], stats[("recovery_tail", path)]
    if b["n"] and t["n"] and t["p95"] > max(b["p95"] * 2, b["p95"] + 200):
        stop.append(f"[3] recovery_tail/{path} p95 {t['p95']:.0f}ms > 기준 {b['p95']:.0f}ms 대비 한도")
b, t = mstats.get("baseline", {}), mstats.get("recovery_tail", {})
if b.get("n") and t.get("n"):
    if t["busy_max"] > b["busy_max"] + 5:
        stop.append(f"[4] recovery_tail busy 스레드 최대 {t['busy_max']:.0f} > 기준 {b['busy_max']:.0f}+5")
    if t["active_max"] > b["active_max"] + 5:
        stop.append(f"[4] recovery_tail 처리 중 요청 최대 {t['active_max']:.0f} > 기준 {b['active_max']:.0f}+5")
for ph, ms in mstats.items():
    if ms.get("n") and ms["cpu_mean"] > 70:
        stop.append(f"[5] {ph} backend CPU 평균 {ms['cpu_mean']:.1f}%")
if not any(ms.get("n") for ms in mstats.values()):
    invalid.append("서버 지표 표본 없음")
st = stats[("outage", "liveness")]
if st["n"] and st["p95"] > 500:
    stop.append(f"[7] outage liveness p95 {st['p95']:.0f}ms")
if invalid:
    stop.append("[6] 실행 무효: " + "; ".join(invalid))

P("실행 유효성: " + ("무효 — " + "; ".join(invalid) if invalid else "유효"))
P("다음 단계로 올리지 않는 조건: " + ("해당 — " + " | ".join(stop) if stop else "해당 없음"))

text = "\n".join(out) + "\n"
with open(os.path.join(D, "summary.txt"), "w", encoding="utf-8") as f:
    f.write(text)
print(text)
