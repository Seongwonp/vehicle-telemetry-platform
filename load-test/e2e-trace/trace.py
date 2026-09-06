"""MQTT → Kafka → InfluxDB → REST → WebSocket 단계별 도착과 지연 (docs/roadmap.md P1-5).

## 왜 필요한가

지금까지의 정합성 대조는 **단계별 총량**을 맞췄다("발행 162,402 = 토픽 162,402 = 행
162,402"). 그건 유실을 잡지만, **어느 단계에서 얼마나 늦는지**는 말해주지 않는다.
그리고 총량이 맞아도 특정 레코드가 앱까지 못 갔을 수 있다 — 브로드캐스트 경로는
저장 경로와 갈라지므로 총량 대조에 안 잡힌다(2026-09-05에 이상 알림 쪽에서 실제로
이런 결함을 찾았다).

그래서 **한 대의 표식 차량**으로 소수의 레코드를 흘리고 그 개별 레코드를 끝까지 따라간다.

    발행 시각 ─┬─ Kafka 레코드
               ├─ InfluxDB 포인트
               ├─ REST 응답에 보이기까지 (0.5초 폴링)
               └─ WebSocket 프레임 도착까지

## 식별자

payload에 표식 필드를 넣을 수 없다(스키마가 고정이고 Bean Validation이 막는다).
대신 **차량 ID + 타임스탬프**를 식별자로 쓴다 — 저장 경로의 포인트 키와 같으므로
InfluxDB에서 그대로 조회된다. 마커는 서로 다른 ms 타임스탬프를 갖게 만든다
(같은 ms면 InfluxDB에서 덮어써진다 — 2026-09-05 ms 충돌 측정 참고).

## 지연의 의미

- WebSocket 지연은 **같은 프로세스**에서 발행 시각과 수신 시각을 재므로 시계 오차가 없다.
- REST 가시성 지연은 0.5초 폴링이라 **분해능이 0.5초**다. 그보다 작은 값은 못 잰다.
- Kafka/InfluxDB 도착은 시각이 아니라 **도착 여부**만 본다. 컨테이너 안에서 재는
  시각과 브로커 타임스탬프를 섞으면 시계 문제로 음수 지연이 나온다.

비밀은 출력하지 않는다(docs/evidence-policy.md). 자격 증명은 환경변수로만 받는다.
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import threading
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import requests
import websocket
from kafka import KafkaConsumer, TopicPartition

STOMP_NULL = "\x00"


def iso_ms(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") \
        + f"{int(ts * 1000) % 1000:03d}Z"


class WsCollector:
    """STOMP over WebSocket 구독자. 프레임 도착 시각을 마커별로 기록한다."""

    def __init__(self, url: str, token: str, vehicle_id: str):
        self.url = url
        self.token = token
        self.vehicle_id = vehicle_id
        self.arrivals: dict[str, float] = {}
        self.connected = threading.Event()
        self.error: str | None = None
        self._ws: websocket.WebSocket | None = None
        self._stop = threading.Event()

    def _frame(self, command: str, headers: dict[str, str], body: str = "") -> str:
        head = "\n".join(f"{k}:{v}" for k, v in headers.items())
        return f"{command}\n{head}\n\n{body}{STOMP_NULL}"

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        try:
            ws = websocket.create_connection(self.url, timeout=10,
                                             sslopt={"cert_reqs": ssl.CERT_NONE})
            self._ws = ws
            ws.send(self._frame("CONNECT", {
                "accept-version": "1.2",
                "host": "backend",
                "heart-beat": "0,0",
                # 인터셉터가 CONNECT의 native header "Authorization"만 본다
                # (WebSocketAuthChannelInterceptor.extractToken).
                "Authorization": f"Bearer {self.token}",
            }))
            first = ws.recv()
            if not str(first).startswith("CONNECTED"):
                self.error = f"CONNECT 실패: {str(first)[:120]}"
                return
            ws.send(self._frame("SUBSCRIBE", {
                "id": "sub-0",
                "destination": f"/topic/vehicle/{self.vehicle_id}/telemetry",
            }))
            self.connected.set()
            ws.settimeout(1.0)
            while not self._stop.is_set():
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                except Exception as e:  # 연결이 끊기면 그대로 남긴다
                    self.error = f"수신 중단: {e}"
                    return
                now = time.time()
                text = str(raw)
                if not text.startswith("MESSAGE"):
                    continue
                body = text.split("\n\n", 1)[-1].rstrip(STOMP_NULL)
                try:
                    payload = json.loads(body)
                except Exception:
                    continue
                ts = payload.get("timestamp")
                if ts:
                    self.arrivals.setdefault(ts, now)
        except Exception as e:
            self.error = f"연결 실패: {e}"

    def stop(self) -> None:
        self._stop.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass


def login(base: str, user: str, password: str) -> str:
    r = requests.post(f"{base}/api/auth/login",
                      json={"username": user, "password": password}, timeout=10)
    r.raise_for_status()
    return r.json()["accessToken"]


def ensure_vehicle(base: str, token: str, vehicle_id: str) -> str:
    h = {"Authorization": f"Bearer {token}"}
    got = requests.get(f"{base}/api/vehicles/{vehicle_id}", headers=h, timeout=10)
    if got.status_code == 200:
        return "already"
    r = requests.post(f"{base}/api/vehicles", headers=h, timeout=10,
                      json={"vehicleId": vehicle_id, "name": "E2E 추적용",
                            "owner": "e2e-trace"})
    if r.status_code in (200, 201):
        return "created"
    return f"등록 실패 {r.status_code}: {r.text[:120]}"


def publish_markers(host: str, port: int, vehicle_id: str, count: int,
                    gap_sec: float) -> list[tuple[str, float]]:
    """마커를 발행하고 (timestamp, publish 시각)을 돌려준다.

    QoS 1로 보내고 PUBACK까지 기다린다 — 발행 성공을 브로커가 확인한 것으로 정의해야
    "발행은 됐는데 안 왔다"와 "발행 자체가 안 됐다"를 구분할 수 있다
    (2026-09-05 MQTT 브로커 장애 측정에서 이 구분이 없어 한 번 헤맸다).
    """
    client = mqtt.Client(client_id=f"e2e-trace-{uuid.uuid4().hex[:8]}")
    client.connect(host, port, keepalive=30)
    client.loop_start()
    markers: list[tuple[str, float]] = []
    for i in range(count):
        now = time.time()
        ts = iso_ms(now)
        payload = {
            "vehicle_id": vehicle_id,
            "timestamp": ts,
            "speed": 60.0 + i,
            "rpm": 2000 + i,
            "engine_temp": 90.0,
            "throttle_position": 30.0,
            "fuel_level": 70.0,
            "battery_voltage": 13.8,
            "gps": {"lat": 37.5, "lng": 127.0},
            "dtc_codes": [],
        }
        info = client.publish(f"vehicle/telemetry/{vehicle_id}",
                              json.dumps(payload), qos=1)
        info.wait_for_publish(timeout=10)
        markers.append((ts, now))
        time.sleep(gap_sec)
    client.loop_stop()
    client.disconnect()
    return markers


def kafka_arrivals(bootstrap: str, topic: str, vehicle_id: str,
                   since_ms: int, timeout_ms: int) -> set[str]:
    """마커가 Kafka 토픽에 있는지.

    **끝을 미리 정해놓고 읽는다.** 처음에는 `consumer_timeout_ms`(무입력 20초)로
    끝내려 했는데, 배경 부하가 계속 쓰는 토픽에서는 조용해지는 순간이 오지 않아
    **영원히 안 끝났다**(실제로 4분 넘게 돌다 죽였다). 시작 지점도 마커 발행 직전으로
    잡는다 — 토픽 전체를 훑을 이유가 없다.
    """
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False,
                             consumer_timeout_ms=timeout_ms, group_id=None)
    partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
    consumer.assign(partitions)

    # 끝 지점을 **지금** 찍어둔다. 여기서 멈추지 않으면 배경 부하를 따라가며 끝나지 않는다.
    end_offsets = consumer.end_offsets(partitions)
    starts = consumer.offsets_for_times({tp: since_ms for tp in partitions})
    for tp in partitions:
        found = starts.get(tp)
        if found is None:
            consumer.seek_to_beginning(tp)
        else:
            consumer.seek(tp, found.offset)

    seen = set()
    remaining = {tp for tp in partitions if consumer.position(tp) < end_offsets[tp]}
    for msg in consumer:
        tp = TopicPartition(msg.topic, msg.partition)
        try:
            payload = json.loads(msg.value.decode("utf-8"))
            if payload.get("vehicle_id") == vehicle_id and payload.get("timestamp"):
                seen.add(payload["timestamp"])
        except Exception:
            pass
        if msg.offset >= end_offsets[tp] - 1:
            remaining.discard(tp)
            if not remaining:
                break
    consumer.close()
    return seen


def influx_arrivals(url: str, org: str, bucket: str, token: str,
                    vehicle_id: str) -> set[str]:
    """InfluxDB에 그 마커가 있는지.

    **조회가 실패하면 빈 집합으로 돌려준다.** 처음에는 예외를 그대로 던졌는데,
    InfluxDB를 세워놓고 추적하는 시나리오(장애 중 실패 지점 구분)에서 이름 해석이
    실패하면서 `requests.exceptions.ConnectionError`로 **도구가 통째로 죽었다** —
    측정하려던 바로 그 상황에서 못 쓰는 도구였다. "저장소에 못 물어봤다"는
    "저장 안 됐다"와 같은 결론(미도달)으로 처리하고, 실패 사실을 위로 알린다.
    """
    flux = (f'from(bucket: "{bucket}") |> range(start: -1h) '
            f'|> filter(fn: (r) => r._measurement == "vehicle_telemetry" '
            f'and r.vehicle_id == "{vehicle_id}" and r._field == "speed")')
    try:
        r = requests.post(f"{url}/api/v2/query", params={"org": org},
                          headers={"Authorization": f"Token {token}",
                                   "Content-Type": "application/vnd.flux",
                                   "Accept": "application/csv"},
                          data=flux, timeout=60)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  ** InfluxDB 조회 실패 — 미도달로 센다: {type(e).__name__}")
        return set()
    seen = set()
    header: list[str] = []
    for line in r.text.splitlines():
        if not line.strip():
            continue
        cols = line.split(",")
        if "_time" in cols:
            header = cols
            continue
        if header and line.startswith(","):
            try:
                seen.add(cols[header.index("_time")])
            except (ValueError, IndexError):
                continue
    return seen


def normalize(ts: str) -> str:
    """InfluxDB는 RFC3339 나노초(...:00.123Z 또는 ...:00Z)로 돌려준다.

    ms 3자리로 맞춰 비교한다. 문자열을 그대로 비교하면 같은 시각이 다른 값으로 보여서
    "InfluxDB에 없다"는 잘못된 결론이 난다.
    """
    ts = ts.strip().replace("+00:00", "Z")
    if "." in ts:
        base, frac = ts[:-1].split(".", 1)
        return f"{base}.{(frac + '000')[:3]}Z"
    return f"{ts[:-1]}.000Z"


class RestPoller:
    """REST 응답에 각 마커가 보이기 시작한 시각. 분해능은 `interval_sec`다.

    **발행보다 먼저 시작해야 한다.** 처음에는 발행이 다 끝난 뒤에 폴링을 시작했는데,
    그러면 첫 폴링 한 번에 20건이 한꺼번에 보이면서 마커 순서대로 정확히 500ms씩
    줄어드는 지연값이 나온다(9583, 9082, … 1072ms — 발행 간격 그대로다).
    시스템의 성질이 아니라 **측정 시작 시점의 그림자**였다. WebSocket 수집기처럼
    별도 스레드로 먼저 띄운다.
    """

    def __init__(self, base: str, token: str, vehicle_id: str, interval_sec: float):
        self.base = base
        self.token = token
        self.vehicle_id = vehicle_id
        self.interval = interval_sec
        self.seen: dict[str, float] = {}
        self._stop = threading.Event()

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        h = {"Authorization": f"Bearer {self.token}"}
        while not self._stop.is_set():
            try:
                r = requests.get(f"{self.base}/api/vehicles/{self.vehicle_id}/telemetry",
                                 params={"limit": 100}, headers=h, timeout=10)
                if r.status_code == 200:
                    now = time.time()
                    for item in r.json():
                        ts = item.get("timestamp")
                        if ts:
                            self.seen.setdefault(normalize(ts), now)
            except requests.RequestException:
                pass
            self._stop.wait(self.interval)

    def wait_for(self, expected: int, timeout_sec: float) -> None:
        deadline = time.time() + timeout_sec
        while time.time() < deadline and len(self.seen) < expected:
            time.sleep(0.5)

    def stop(self) -> None:
        self._stop.set()


def pct(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    idx = min(len(s) - 1, int(round((len(s) - 1) * p)))
    return s[idx]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicle-id", default="E2E-TRACE-01")
    ap.add_argument("--count", type=int, default=20)
    ap.add_argument("--gap-sec", type=float, default=0.5)
    ap.add_argument("--api", default="http://backend:8080")
    ap.add_argument("--ws", default="ws://backend:8080/ws")
    ap.add_argument("--mqtt-host", default="mosquitto")
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--topic", default="vehicle-telemetry")
    ap.add_argument("--influx", default="http://influxdb:8086")
    ap.add_argument("--settle-sec", type=float, default=15.0)
    args = ap.parse_args()

    user = os.environ["ADMIN_USERNAME"]
    password = os.environ["ADMIN_PASSWORD"]
    token = login(args.api, user, password)
    print(f"[1/6] 로그인 OK")
    print(f"[2/6] 차량 등록: {ensure_vehicle(args.api, token, args.vehicle_id)}")

    ws = WsCollector(args.ws, token, args.vehicle_id)
    ws.start()
    if not ws.connected.wait(timeout=20):
        print(f"[3/6] WebSocket 구독 실패 — {ws.error}")
    else:
        print("[3/6] WebSocket 구독 OK")

    # 발행보다 **먼저** 띄운다 — 뒤에 시작하면 첫 폴링에 전량이 한꺼번에 보여서
    # 지연이 발행 간격의 그림자가 된다(RestPoller 주석 참고).
    poller = RestPoller(args.api, token, args.vehicle_id, interval_sec=0.5)
    poller.start()

    markers = publish_markers(args.mqtt_host, args.mqtt_port, args.vehicle_id,
                              args.count, args.gap_sec)
    print(f"[4/6] 마커 {len(markers)}건 발행(PUBACK 확인)")

    poller.wait_for(len(markers), timeout_sec=args.settle_sec + 30)
    poller.stop()
    rest_seen = dict(poller.seen)
    print(f"[5/6] REST 가시성 폴링 종료 — {len(rest_seen)}건")

    time.sleep(2)
    ws.stop()
    # Kafka 조회도 같은 이유로 감싼다 — 어느 단계가 죽어 있든 **집계까지는 가야** 한다.
    # 이 도구의 존재 이유가 "어디서 끊겼나"라서, 끊긴 단계를 조회하다 죽으면 쓸모가 없다.
    try:
        kafka_seen = kafka_arrivals(args.bootstrap, args.topic, args.vehicle_id,
                                    int((markers[0][1] - 5) * 1000), 20000)
    except Exception as e:
        print(f"  ** Kafka 조회 실패 — 미도달로 센다: {type(e).__name__}")
        kafka_seen = set()
    influx_seen = {normalize(t) for t in
                   influx_arrivals(args.influx, os.environ["INFLUXDB_ORG"],
                                   os.environ["INFLUXDB_BUCKET"],
                                   os.environ["INFLUXDB_TOKEN"], args.vehicle_id)}
    ws_seen = {normalize(t): v for t, v in ws.arrivals.items()}
    print("[6/6] 집계")

    rows = []
    ws_lat, rest_lat = [], []
    for ts, sent in markers:
        key = normalize(ts)
        in_kafka = key in kafka_seen
        in_influx = key in influx_seen
        w = ws_seen.get(key)
        r = rest_seen.get(key)
        if w:
            ws_lat.append((w - sent) * 1000)
        if r:
            rest_lat.append((r - sent) * 1000)
        rows.append((key, in_kafka, in_influx, r is not None, w is not None))

    n = len(markers)
    print("")
    print("=== 단계별 도착 ===")
    print(f"MQTT 발행(PUBACK)      : {n} / {n}")
    print(f"Kafka {args.topic}     : {sum(1 for r in rows if r[1])} / {n}")
    print(f"InfluxDB               : {sum(1 for r in rows if r[2])} / {n}")
    print(f"REST 조회              : {sum(1 for r in rows if r[3])} / {n}")
    print(f"WebSocket 브로드캐스트  : {sum(1 for r in rows if r[4])} / {n}")
    if ws.error:
        print(f"  ** WebSocket 오류: {ws.error}")
    print("")
    print("=== 지연 (발행 시각 기준, ms) ===")
    if ws_lat:
        print(f"WebSocket  n={len(ws_lat)}  min={min(ws_lat):.0f}  "
              f"p50={pct(ws_lat, 0.5):.0f}  p95={pct(ws_lat, 0.95):.0f}  max={max(ws_lat):.0f}")
    else:
        print("WebSocket  측정 없음")
    if rest_lat:
        print(f"REST 가시성 n={len(rest_lat)}  min={min(rest_lat):.0f}  "
              f"p50={pct(rest_lat, 0.5):.0f}  p95={pct(rest_lat, 0.95):.0f}  "
              f"max={max(rest_lat):.0f}   (폴링 0.5초 — 분해능 500ms)")
    else:
        print("REST 가시성 측정 없음")

    missing = [r for r in rows if not (r[1] and r[2] and r[3] and r[4])]
    print("")
    if missing:
        print(f"=== 어느 단계에서 끊겼나 ({len(missing)}건) ===")
        for key, k, i, rr, w in missing[:20]:
            stage = "kafka" if not k else "influx" if not i else "rest" if not rr else "websocket"
            print(f"  {key}  최초 미도달 단계={stage}  kafka={k} influx={i} rest={rr} ws={w}")
    else:
        print("모든 마커가 5단계 전부 도달했다.")

    print("")
    print("CSV_BEGIN")
    print("timestamp,kafka,influx,rest,websocket,ws_latency_ms,rest_latency_ms")
    for (key, k, i, rr, w), (ts, sent) in zip(rows, markers):
        wl = (ws_seen[key] - sent) * 1000 if key in ws_seen else ""
        rl = (rest_seen[key] - sent) * 1000 if key in rest_seen else ""
        wl = f"{wl:.0f}" if wl != "" else ""
        rl = f"{rl:.0f}" if rl != "" else ""
        print(f"{key},{int(k)},{int(i)},{int(rr)},{int(w)},{wl},{rl}")
    print("CSV_END")
    return 0


if __name__ == "__main__":
    sys.exit(main())
