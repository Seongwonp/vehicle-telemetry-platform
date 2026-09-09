#!/usr/bin/env python3
"""입력 계약(P0-2)을 **두 입구로 각각** 주입한다.

MQTT 입구와 Kafka 직접 주입에 **같은 payload**를 넣고, 각각이 계약대로 판정되는지
파이프라인 끝까지(InfluxDB 저장 / DLQ) 확인하기 위한 도구다.

단위 테스트는 핸들러까지만 본다. 여기서는 브로커·Kafka·백엔드·InfluxDB를 실제로 거친다.

**차량 ID를 시나리오·입구별로 분리한다.** 같은 ID를 쓰면 어느 시나리오의 결과인지
저장 결과에서 가를 수 없다.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

TOPIC = "vehicle-telemetry"

# 계약이 정한 차량 ID 길이 상한. VehicleTelemetry의 @Pattern과 같아야 한다.
VEHICLE_ID_MAX = 20


def base(vehicle_id: str, ts: str) -> dict:
    return {
        "vehicle_id": vehicle_id,
        "timestamp": ts,
        "speed": 87.3,
        "rpm": 2400,
        "engine_temp": 92.1,
        "throttle_position": 34.5,
        "fuel_level": 67.0,
        "battery_voltage": 13.8,
        "gps": {"lat": 37.5, "lng": 127.0},
        "dtc_codes": [],
    }


# (번호, 이름, 변형 함수, 기대 결과)
#   stored   = InfluxDB에 저장되어야 한다
#   rejected = 저장되지 않고 해당 입구의 DLQ로 가야 한다
def cases():
    def ident(p):
        return p

    def rpm_fraction(p):
        p["rpm"] = 2000.7
        return p

    def anomaly(p):
        p["engine_temp"] = 106.0      # 감지 임계 105 초과, 계약 상한 215 이내
        return p

    def missing_number(p):
        del p["speed"]
        return p

    def null_number(p):
        p["speed"] = None
        return p

    def missing_lat(p):
        del p["gps"]["lat"]
        return p

    def unknown_field(p):
        p["sped"] = 87.3
        return p

    def out_of_range(p):
        p["speed"] = 300.0            # 계약 상한 255 초과
        return p

    def dtc_null(p):
        p["dtc_codes"] = [None]
        return p

    # **JSON 객체가 아닌 입력.** 문자열을 돌려주면 그대로 payload가 된다.
    # `null` 네 글자는 역직렬화가 예외 없이 성공하고 **null을 돌려주는** 유일한 입력이라
    # 다른 칸으로는 이 경로를 밟을 수 없다. 2026-09-09 최종 점검에서 여기가 계약 예외
    # 밖으로 새는 것을 찾았고(Hibernate Validator HV000116), decoder에 가드를 넣어 막았다.
    def null_literal(p):
        return "null"

    return [
        (1, "normal", ident, "stored"),
        (2, "rpm_fraction", rpm_fraction, "stored"),
        (3, "anomaly_in_contract", anomaly, "stored"),
        (4, "missing_number", missing_number, "rejected"),
        (5, "null_number", null_number, "rejected"),
        (6, "missing_gps_lat", missing_lat, "rejected"),
        (7, "unknown_field", unknown_field, "rejected"),
        (8, "out_of_range", out_of_range, "rejected"),
        (9, "dtc_null_element", dtc_null, "rejected"),
        (10, "null_literal", null_literal, "rejected"),
    ]


def check_id(vehicle_id: str) -> str:
    """차량 ID가 계약 길이를 넘지 않는지 확인한다.

    **이 가드가 없어서 2회차 실행이 통째로 무효였다.** 처음에 `-MQTT-01`/`-KAFKA-01`로
    만들었더니 Kafka 쪽만 21자가 되어 계약(4~20자)을 넘겼다. 그래서
    정상 payload가 거부되고, 거부되어야 할 payload도 **의도한 이유가 아니라 ID 길이
    때문에** 거부됐다 — 통과로 나온 칸까지 전부 못 믿는 상태가 됐다.
    """
    if not (4 <= len(vehicle_id) <= VEHICLE_ID_MAX):
        raise SystemExit(
            f"차량 ID가 계약 길이를 벗어난다: {vehicle_id} ({len(vehicle_id)}자, 상한 {VEHICLE_ID_MAX}). "
            "prefix를 줄여라 — 이걸 어기면 시나리오가 아니라 ID 때문에 거부된다.")
    return vehicle_id


def build(prefix: str, entrance: str, number: int, mutate) -> tuple[str, str]:
    """입구는 한 글자(M/K)로 줄여 ID 길이를 아낀다."""
    code = "M" if entrance == "MQTT" else "K"
    vehicle_id = check_id(f"{prefix}-{code}{number:02d}".upper())
    # 밀리초 충돌로 행이 덮어써지지 않게 시나리오마다 시각을 벌린다.
    ts = (datetime.now(timezone.utc) + timedelta(milliseconds=number * 7)) \
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    mutated = mutate(base(vehicle_id, ts))
    # 변형 함수가 문자열을 돌려주면 **그대로** 보낸다(JSON 객체가 아닌 입력).
    # 이 경우 payload에 차량 ID가 없으므로 저장 행 수로는 귀속되지 않는다 —
    # 판정은 "저장 0행"이고, 사유 귀속은 DLQ 쪽(Kafka는 레코드 키, MQTT는 토픽)에서 한다.
    payload = mutated if isinstance(mutated, str) else json.dumps(mutated)
    return vehicle_id, payload


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="SCHEMA")
    ap.add_argument("--mqtt-host", default="mosquitto")
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--mixed-batch", action="store_true",
                    help="정상 2 + 실패 1을 한 번에 Kafka로 넣어 배치 격리를 본다")
    args = ap.parse_args()

    manifest = []

    # ── MQTT 입구 ──────────────────────────────────────────────
    client = mqtt.Client(client_id=f"schema-contract-{int(time.time())}")
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=30)
    client.loop_start()
    for number, name, mutate, expect in cases():
        vid, payload = build(args.prefix, "MQTT", number, mutate)
        info = client.publish(f"vehicle/telemetry/{vid}", payload, qos=1)
        info.wait_for_publish(timeout=10)
        manifest.append({"entrance": "mqtt", "case": name, "vehicle_id": vid, "expect": expect})
    client.loop_stop()
    client.disconnect()

    # ── Kafka 직접 주입 ────────────────────────────────────────
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", retries=3)
    for number, name, mutate, expect in cases():
        vid, payload = build(args.prefix, "KAFKA", number, mutate)
        producer.send(TOPIC, key=vid.encode(), value=payload.encode()).get(timeout=30)
        manifest.append({"entrance": "kafka", "case": name, "vehicle_id": vid, "expect": expect})

    # ── 혼합 배치 ──────────────────────────────────────────────
    # 정상 2 + 계약 위반 1을 **같은 키**로 연속 발행해 한 배치에 담기게 한다.
    if args.mixed_batch:
        mixed_id = check_id(f"{args.prefix}-MIX".upper())
        good_ts = datetime.now(timezone.utc)
        for i, broken in enumerate([False, True, False]):
            ts = (good_ts + timedelta(milliseconds=i)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            p = base(mixed_id, ts)
            if broken:
                del p["speed"]
            producer.send(TOPIC, key=mixed_id.encode(),
                          value=json.dumps(p).encode()).get(timeout=30)
        manifest.append({"entrance": "kafka", "case": "mixed_batch",
                         "vehicle_id": mixed_id, "expect": "2_of_3_stored"})

    producer.flush()
    producer.close()

    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
