#!/usr/bin/env python3
"""독성 메시지를 주입해 격리 경계를 확인한다 (docs/roadmap.md P1-2).

## 무엇을 보려는가

한 건의 처리 불가능한 메시지가 **정상 메시지까지 막는가.**

저장 경로는 배치 리스너다. 배치 안에서 한 건이 실패할 때 그 한 건만 격리되는지,
아니면 배치 전체가 재시도·DLQ로 끌려가는지가 이 실험의 질문이다.
`TelemetryConsumer.consumeForStorage`는 역직렬화와 포인트 변환을 레코드별 try/catch로
감싸 놨지만, **`saveAll()` 단계에서 실패하는 독성**은 그 방어를 통과한다.

## 왜 Kafka에 직접 넣는가

MQTT 단계는 이미 JSON·Bean Validation·타임스탬프·토픽 일치를 검사해서 거른다
(`MqttMessageHandler`). 그 뒤 단계(Kafka → InfluxDB)의 격리를 보려면 그 검사를
우회해야 한다. 실제로도 프로듀서 쪽 버그나 스키마 변경으로 이런 레코드가 토픽에
들어올 수 있다.

## 대조군

독성 1건마다 **같은 파티션 키의 정상 레코드 N건**을 함께 넣는다. 나중에 InfluxDB에
그 N건이 몇 개 남았는지로 "정상 레코드가 함께 막혔는가"를 센다.
대조군이 없으면 "독성이 DLQ로 갔다"까지만 알 수 있고 그게 이 실험의 질문이 아니다.

사용법:
    python inject_poison.py --type bad_timestamp --controls 100
    python inject_poison.py --list
"""
import argparse
import json
import sys
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "vehicle-telemetry"


def base(vehicle_id: str, ts: str = None) -> dict:
    return {
        "vehicle_id": vehicle_id,
        "timestamp": ts or datetime.now(timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "speed": 60.0,
        "rpm": 2000,
        "engine_temp": 90.0,
        "throttle_position": 30.0,
        "fuel_level": 50.0,
        "battery_voltage": 13.8,
        "gps": {"lat": 37.5, "lng": 127.0},
        "dtc_codes": [],
    }


def poison_payloads(vehicle_id: str) -> dict:
    """이름 → (payload 문자열, 어디서 실패할 것으로 예상되는가)."""
    p = base(vehicle_id)

    malformed = "{this is not json"

    bad_ts = dict(p)
    bad_ts["timestamp"] = "어제쯤"          # Instant.parse 실패 → toPoint 단계

    wrong_schema = {"sensor": "temp", "reading": 12.3}   # 필수 필드가 아예 없다

    # Jackson은 지수가 double 범위를 넘는 리터럴(1e309)을 Double.POSITIVE_INFINITY로
    # 파싱한다. InfluxDB 라인 프로토콜에 Infinity는 쓸 수 없어서 **saveAll() 단계**에서
    # 터진다 — 레코드별 try/catch를 통과하는 유일한 유형이라 이 실험의 핵심이다.
    #
    # **JSON을 손으로 만든다.** `json.dumps`에 float('inf')를 주면 `Infinity`라는
    # 비표준 리터럴을 쓰는데, 그건 Jackson이 역직렬화 단계에서 거부해서
    # 배치 실패를 못 보고 레코드별 격리로 끝나버린다(처음에 그렇게 만들어서
    # 의도한 케이스를 테스트하지 못했다).
    infinity_json = json.dumps(p)
    infinity_json = infinity_json.replace('"speed": 60.0', '"speed": 1e309')

    # 큰 메시지. Kafka 기본 max.message.bytes(1MB) 아래로 잡아 토픽에는 들어가게 한다 —
    # 브로커가 거부하면 컨슈머 격리를 볼 수 없기 때문이다.
    huge = dict(p)
    huge["dtc_codes"] = ["P%04d" % i for i in range(40000)]   # 약 300KB

    return {
        "malformed_json": (malformed, "역직렬화(레코드별 격리 예상)"),
        "bad_timestamp": (json.dumps(bad_ts, ensure_ascii=False), "toPoint(레코드별 격리 예상)"),
        "wrong_schema": (json.dumps(wrong_schema), "toPoint 또는 저장(미지)"),
        "infinity": (infinity_json, "saveAll(배치 전체 실패 예상)"),
        "huge_payload": (json.dumps(huge), "저장 또는 브로커(미지)"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--type", help="주입할 독성 유형")
    ap.add_argument("--controls", type=int, default=100, help="함께 넣을 정상 레코드 수")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    types = poison_payloads("POISON-X")
    if args.list or not args.type:
        for name, (_, where) in types.items():
            print(f"{name:<16} {where}")
        return 0
    if args.type not in types:
        print(f"모르는 유형: {args.type}", file=sys.stderr)
        return 2

    # 대조군과 독성이 **같은 키**를 써야 같은 파티션에 들어가고, 같은 poll 배치에
    # 담길 가능성이 높아진다. 키가 다르면 파티션이 갈려 "함께 막혔는가"를 못 본다.
    vehicle_id = "POISON-" + args.type.upper().replace("_", "-")[:12]
    vehicle_id = vehicle_id[:20]
    payload, expected = types[args.type]
    payload = payload.replace("POISON-X", vehicle_id)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap, acks="all", retries=3,
        max_request_size=2 * 1024 * 1024)

    sent_controls = 0
    for i in range(args.controls):
        ctrl = base(vehicle_id)
        # 같은 밀리초 충돌로 행이 덮어써지면 대조군 수를 잘못 세게 된다.
        # 1ms씩 벌려 고유 키를 보장한다.
        ctrl["timestamp"] = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() + i * 0.001, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        producer.send(TOPIC, key=vehicle_id.encode(),
                      value=json.dumps(ctrl).encode()).get(timeout=30)
        sent_controls += 1
        # 독성을 대조군 한가운데 넣는다 — 앞뒤 모두가 같은 배치에 들어가도록.
        if i == args.controls // 2:
            producer.send(TOPIC, key=vehicle_id.encode(),
                          value=payload.encode()).get(timeout=30)

    producer.flush()
    producer.close()

    print(f"유형          : {args.type}")
    print(f"vehicle_id    : {vehicle_id}")
    print(f"예상 실패 지점: {expected}")
    print(f"대조군        : {sent_controls}건 (독성 1건을 가운데 삽입)")
    print(f"payload 크기  : {len(payload):,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
