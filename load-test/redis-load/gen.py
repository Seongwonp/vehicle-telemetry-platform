#!/usr/bin/env python3
"""개방형(도착률 고정) 요청 생성기 — 표준 라이브러리만 쓴다 (PLAN_20260914.md §3).

- 한 컨테이너 = 한 IP = rate limit 키 하나. 한도(IP당 60/분) 아래로 보낸다.
- 정해진 시각표대로 보내고, **응답이 느려져도 보내는 속도를 줄이지 않는다.**
- 생성기 자신이 밀리면 `sent_ms - intended_ms`(생성기 지연)로 드러난다 — 서버 한계와 구분하는 근거.
- 요청마다 새 연결을 연다. 재연결 중 연결 재사용 오류가 결과를 섞지 않게.
"""
import argparse
import csv
import http.client
import json
import os
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

REFRESH_BODY = json.dumps({"refreshToken": "load-nonexistent-token"})


def now_ms():
    return time.time() * 1000.0


def call(host, port, method, path, headers, body, timeout):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, resp.getheader("X-RateLimit-Remaining"), data
    finally:
        conn.close()


def login(host, port):
    body = json.dumps({
        "username": os.environ["ADMIN_USERNAME"],
        "password": os.environ["ADMIN_PASSWORD"],
    })
    for _ in range(10):
        try:
            status, _, data = call(host, port, "POST", "/api/auth/login",
                                   {"Content-Type": "application/json"}, body, 10)
            if status == 200:
                return json.loads(data)["accessToken"]
        except (OSError, http.client.HTTPException, ValueError, KeyError):
            pass
        time.sleep(3)
    raise SystemExit("login failed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="backend")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--name", required=True)
    ap.add_argument("--rate", type=float, required=True, help="이 생성기의 총 요청/초")
    ap.add_argument("--paths", default="query,refresh", help="순서대로 번갈아 보낸다")
    ap.add_argument("--start-at", type=float, required=True, help="epoch 초")
    ap.add_argument("--duration", type=float, required=True, help="초")
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    paths = a.paths.split(",")
    token = login(a.host, a.port) if "query" in paths else None
    specs = {
        "query": ("GET", "/api/vehicles", {"Authorization": f"Bearer {token}"}, None),
        "refresh": ("POST", "/api/auth/refresh", {"Content-Type": "application/json"}, REFRESH_BODY),
        "liveness": ("GET", "/actuator/health/liveness", {}, None),
    }

    lock = threading.Lock()
    f = open(a.out, "w", newline="")
    # csv 모듈 기본 줄끝은 CRLF다. 증거는 LF로 커밋되므로 LF로 쓴다 — 아니면 manifest가 clone에서 깨진다
    # (2026-09-14 L1 증거가 그렇게 깨졌다).
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["gen", "seq", "path", "intended_ms", "sent_ms", "end_ms",
                "http", "code", "ratelimit_hdr", "outcome"])

    def work(seq, path, intended):
        method, url, headers, body = specs[path]
        sent = now_ms()
        status, rl, code, outcome = 0, None, "-", "ok"
        try:
            status, rl, data = call(a.host, a.port, method, url, headers, body, a.timeout)
            if status >= 400:
                try:
                    code = json.loads(data).get("code") or "-"
                except (ValueError, AttributeError):
                    code = "-"
        except TimeoutError:
            outcome = "timeout"
        except OSError as e:
            outcome = "conn_error:" + type(e).__name__
        except http.client.HTTPException as e:
            outcome = "http_error:" + type(e).__name__
        end = now_ms()
        with lock:
            w.writerow([a.name, seq, path, f"{intended:.0f}", f"{sent:.0f}", f"{end:.0f}",
                        status, code, "yes" if rl is not None else "no", outcome])
            f.flush()

    interval_ms = 1000.0 / a.rate
    # 생성기마다 위상을 흩는다 — 여러 생성기가 같은 밀리초에 몰리지 않게(이름으로 결정, 실행마다 같다)
    offset_ms = (zlib.crc32(a.name.encode()) % 1000) / 1000.0 * interval_ms
    start_ms = a.start_at * 1000.0
    total = int(a.duration * a.rate)
    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        for seq in range(total):
            intended = start_ms + offset_ms + seq * interval_ms
            delay = (intended - now_ms()) / 1000.0
            if delay > 0:
                time.sleep(delay)
            pool.submit(work, seq, paths[seq % len(paths)], intended)
    f.close()


if __name__ == "__main__":
    main()
