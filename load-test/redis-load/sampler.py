#!/usr/bin/env python3
"""actuator 지표를 일정 간격으로 긁어 필요한 줄만 남긴다 (PLAN_20260914.md §5).

한 줄 = 한 표본의 한 시계열: ts_ms,scrape_ms,series,value
긁기 자체가 실패하면 series=__scrape_error__ 로 남긴다 — 서버가 응답을 못 한 시각도 증거다.
"""
import argparse
import csv
import http.client
import time

PREFIXES = (
    "tomcat_threads_",
    "tomcat_connections_",
    "http_server_requests_active_seconds_active_count",
    "jvm_threads_live_threads",
    "jvm_threads_states_threads",
    "process_cpu_usage",
    "system_cpu_usage",
    "jvm_memory_used_bytes",
    "hikaricp_connections_active",
    "hikaricp_connections_pending",
)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="backend")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--until", type=float, required=True, help="epoch 초")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    with open(a.out, "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")  # 증거는 LF — gen.py 주석 참고
        w.writerow(["ts_ms", "scrape_ms", "series", "value"])
        while time.time() < a.until:
            t0 = time.time()
            ts = int(t0 * 1000)
            try:
                conn = http.client.HTTPConnection(a.host, a.port, timeout=10)
                conn.request("GET", "/actuator/prometheus")
                body = conn.getresponse().read().decode("utf-8", "replace")
                conn.close()
                scrape = int((time.time() - t0) * 1000)
                for line in body.splitlines():
                    if line.startswith(PREFIXES):
                        series, _, value = line.rpartition(" ")
                        w.writerow([ts, scrape, series, value])
            except Exception as e:  # noqa: BLE001 — 실패 종류를 그대로 남긴다
                w.writerow([ts, int((time.time() - t0) * 1000), "__scrape_error__", type(e).__name__])
            f.flush()
            time.sleep(max(0.0, a.interval - (time.time() - t0)))


if __name__ == "__main__":
    main()
