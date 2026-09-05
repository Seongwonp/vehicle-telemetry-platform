#!/usr/bin/env python3
"""같은 밀리초 키 충돌로 인한 InfluxDB 덮어쓰기 유실을 재현하고, 자연 발생률을 잰다.

## 왜

InfluxDB 포인트의 identity는 `(measurement, 태그, 타임스탬프)`다. 이 프로젝트에서는
`(vehicle_telemetry, vehicle_id, ms 타임스탬프)`이므로, **같은 차량이 같은 밀리초에
두 건을 보내면 뒤엣것이 앞엣것을 조용히 덮어쓴다.** 에러도 로그도 없다.

같은 구조의 버그를 이미 한 번 겪었다 — `WritePrecision.S`(초)였을 때 발행 주기가
1초 미만이면 그대로 유실됐고, 부하 테스트로 발견해 MS로 올렸다(ADR-014).
MS로 올린 뒤 재전달 실험에서는 충돌이 0건이었지만(`RESULT_20260904_kill_redelivery.md`),
그건 "그 부하에서 안 났다"는 뜻이지 "구조적으로 안 난다"는 뜻이 아니다.

## 무엇을 재나

1. **재현**: 같은 vehicle_id·같은 타임스탬프로 N건을 보내 실제로 1행만 남는지 확인한다.
   여기서 안 나면 이 위험 자체가 없는 것이므로, 먼저 확정해야 한다.
2. **자연 발생률**: 한 차량을 가능한 한 빠르게 발행시켜, 발행 속도 대비 같은 밀리초
   충돌이 얼마나 생기는지 잰다. 이게 "실제로 걱정할 일인가"의 근거다.

발행 측이 보낸 고유 밀리초 수를 스스로 세므로, InfluxDB 행 수와 직접 대조할 수 있다.

사용법:
    python ms_collision.py --mode collide --count 500
    python ms_collision.py --mode natural --seconds 20
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "vehicle/telemetry")


def payload(vehicle_id: str, ts: str, speed: float) -> dict:
    # speed를 매번 다르게 줘서, 덮어쓰기가 일어나면 "어느 값이 남았는지"로도 확인 가능하게 한다.
    return {
        "vehicle_id": vehicle_id,
        "timestamp": ts,
        "speed": speed,
        "rpm": 2000,
        "engine_temp": 90.0,
        "throttle_position": 30.0,
        "fuel_level": 50.0,
        "battery_voltage": 13.8,
        "gps": {"lat": 37.5, "lng": 127.0},
        "dtc_codes": [],
    }


def now_ms_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def connect(host: str, port: int, client_id: str) -> mqtt.Client:
    client = mqtt.Client(client_id=client_id)
    client.connect(host, port, keepalive=60)
    client.loop_start()
    return client


def flush_and_close(client: mqtt.Client, acked: dict, sent: int, timeout: float = 30.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and acked["n"] < sent:
        time.sleep(0.1)
    client.loop_stop()
    client.disconnect()
    return acked["n"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="mosquitto")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--vehicle", default="MSCOLLIDE-01")
    ap.add_argument("--mode", choices=["collide", "natural"], required=True)
    ap.add_argument("--count", type=int, default=500, help="collide 모드에서 보낼 건수")
    ap.add_argument("--seconds", type=float, default=20.0, help="natural 모드 발행 시간")
    ap.add_argument("--rate", type=float, default=0.0,
                    help="natural 모드 목표 발행 속도(msg/s). 0이면 최대 속도")
    args = ap.parse_args()

    acked = {"n": 0}
    client = connect(args.host, args.port, f"mscollision-{args.mode}")
    client.on_publish = lambda c, u, mid: acked.__setitem__("n", acked["n"] + 1)

    topic = f"{TOPIC_PREFIX}/{args.vehicle}"
    sent = 0
    timestamps = set()

    if args.mode == "collide":
        # 타임스탬프를 하나로 고정한다. 이론상 1행만 남아야 한다.
        fixed = now_ms_iso()
        timestamps.add(fixed)
        for i in range(args.count):
            client.publish(topic, json.dumps(payload(args.vehicle, fixed, float(i))), qos=1)
            sent += 1
    else:
        # `--rate`로 속도를 고정한다. 최대 속도로 돌리면 초당 10만 건까지 나가는데,
        # 그건 실차는 물론 시뮬레이터 부하 테스트에서도 안 나오는 값이라 "이 위험이
        # 현실에서 언제 시작되는가"라는 질문에 답을 못 준다(실제로 그렇게 재봤다가
        # paho 큐만 넘치고 측정이 오염됐다).
        start = time.monotonic()
        end = start + args.seconds
        interval = (1.0 / args.rate) if args.rate > 0 else 0.0
        while time.monotonic() < end:
            ts = now_ms_iso()
            timestamps.add(ts)
            client.publish(topic, json.dumps(payload(args.vehicle, ts, float(sent % 200))), qos=1)
            sent += 1
            if interval:
                # 누적 오차가 쌓이지 않도록 시작 시각 기준으로 다음 발행 시점을 잡는다.
                nxt = start + sent * interval
                delay = nxt - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

    confirmed = flush_and_close(client, acked, sent)

    print(f"모드                 : {args.mode}")
    print(f"vehicle_id           : {args.vehicle}")
    print(f"발행 건수            : {sent}")
    print(f"브로커 확인(PUBACK)  : {confirmed}")
    print(f"고유 밀리초 수       : {len(timestamps)}  ← InfluxDB에 남을 수 있는 최대 행 수")
    collided = sent - len(timestamps)
    rate = (collided / sent * 100) if sent else 0.0
    print(f"같은 밀리초 중복     : {collided}  ({rate:.2f}%)")
    if args.mode == "natural":
        print(f"실측 발행 속도       : {sent / args.seconds:,.0f} msg/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
