#!/usr/bin/env python3
"""`vehicle-anomaly-alerts` 토픽의 **정답 기준**을 만든다.

이 측정에서 "정답"은 토픽의 메시지 수가 아니라 **서로 다른 event_id의 수**다.

PostgreSQL 쪽은 `anomaly_alerts.event_id`에 UNIQUE 인덱스가 걸려 있고
`INSERT ... ON CONFLICT (event_id) DO NOTHING`으로 저장한다. 즉 설계상
"같은 event_id는 한 행"이다. 그래서 대조해야 할 것은 두 가지다.

1. **복구**: 토픽의 고유 event_id 수 == 테이블 행 수인가.
   작으면 유실이다(재처리가 복구를 못 했다).
2. **중복**: 재처리를 두 번 돌렸을 때 행이 늘어나는가.
   늘면 멱등하지 않다.

여기서 같이 재는 게 하나 더 있다. **event_id가 서로 다른 이상을 뭉개지는 않는가.**
event_id는 `vehicle_id|timestamp|anomaly_type|field|detector`의 SHA-256이라,
같은 차량이 같은 밀리초에 같은 종류의 이상을 두 번 내면 두 이벤트가 한 행으로
합쳐진다 — 중복의 반대편 위험이고, 이쪽은 **알림 유실**이라 더 나쁘다.
그래서 payload 전체가 다른데 event_id만 같은 경우를 따로 센다.
"""
import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict

from kafka import KafkaConsumer

EVENT_KEY_FIELDS = ("vehicle_id", "timestamp", "anomaly_type", "field", "detector")


def derived_event_id(payload: dict) -> str:
    key = "|".join(str(payload.get(f, "")) for f in EVENT_KEY_FIELDS)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--topic", default="vehicle-anomaly-alerts")
    ap.add_argument("--timeout-ms", type=int, default=20000)
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # group_id 없이 읽는다 — 이 도구는 관측만 하고 재처리 상태를 건드리지 않는다.
        group_id=None,
        consumer_timeout_ms=args.timeout_ms,
    )

    total = 0
    undecodable = 0
    missing_event_id = 0
    mismatched_event_id = 0
    ids = Counter()
    # 같은 event_id 아래 서로 다른 "내용"이 몇 가지인지 — 뭉개짐 탐지용.
    # detected_at은 발행 시각이라 재처리와 무관하게 달라질 수 있어 비교에서 뺀다.
    payload_variants = defaultdict(set)

    for record in consumer:
        total += 1
        try:
            payload = json.loads(record.value.decode("utf-8"))
        except Exception:
            undecodable += 1
            continue

        supplied = payload.get("event_id")
        if not supplied:
            missing_event_id += 1
            eid = derived_event_id(payload)
        else:
            eid = supplied
            if supplied != derived_event_id(payload):
                mismatched_event_id += 1

        ids[eid] += 1
        body = {k: v for k, v in payload.items() if k not in ("event_id", "detected_at")}
        payload_variants[eid].add(json.dumps(body, sort_keys=True, ensure_ascii=False))

    consumer.close()

    collapsed = {e: v for e, v in payload_variants.items() if len(v) > 1}
    repeated = {e: c for e, c in ids.items() if c > 1}

    print(f"토픽                        : {args.topic}")
    print(f"메시지 총계                 : {total}")
    print(f"  JSON 파싱 실패            : {undecodable}")
    print(f"  event_id 없음(파생 사용)  : {missing_event_id}")
    print(f"  event_id가 파생값과 불일치: {mismatched_event_id}")
    print(f"고유 event_id (= 정답 기준) : {len(ids)}")
    print(f"  2회 이상 나타난 event_id  : {len(repeated)} "
          f"(중복 발행분 {sum(repeated.values()) - len(repeated)}건)")
    print(f"  내용이 다른데 event_id 같음: {len(collapsed)}  ← 뭉개짐(알림 유실) 후보")
    for eid, variants in list(collapsed.items())[:3]:
        print(f"    {eid[:16]}… 변형 {len(variants)}종")
        for v in list(variants)[:2]:
            print(f"      {v[:200]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
