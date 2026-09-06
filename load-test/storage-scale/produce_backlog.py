"""저장 경로 처리량 측정용 백로그 생성기 — Kafka에 직접 넣는다.

## 왜 MQTT를 안 쓰나

MQTT 경로로는 저장 경로의 한계를 넘길 수 없다. 수집(MQTT → Kafka)이 약 9,600 msg/s에서
먼저 막히고, 저장 경로도 배치화 후 비슷한 수준이라 **부하가 저장 경로를 넘지 못한다.**
그러면 인스턴스를 늘려도 차이가 안 보이고, 그건 "이득이 없다"가 아니라 "못 쟀다"다.

그래서 Kafka에 **직접** 넣어 백로그를 만들고, 그 백로그가 줄어드는 속도를 잰다.
드레인 속도는 유입과 무관한 저장 경로만의 처리량이다.

## 키 충돌을 피한다

InfluxDB 포인트 identity는 (measurement, vehicle_id 태그, ms 타임스탬프)다. 같은 키가
겹치면 덮어써져서 **행 수로 처리량을 검증할 수 없게 된다**(2026-09-05 ms 충돌 측정).
차량마다 자기 타임스탬프를 1ms씩 전진시켜 겹치지 않게 만든다.

사용법(도구 이미지 안에서):
    python produce_backlog.py --count 1500000 --vehicles 200 --shard 0 --shards 3
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--topic", default="vehicle-telemetry")
    ap.add_argument("--count", type=int, default=500_000, help="이 프로세스가 넣을 레코드 수")
    ap.add_argument("--vehicles", type=int, default=200)
    ap.add_argument("--shard", type=int, default=0, help="여러 프로세스로 나눠 넣을 때 이 프로세스 번호")
    ap.add_argument("--shards", type=int, default=1)
    args = ap.parse_args()

    # 샤드마다 차량 ID 범위를 갈라 같은 (vehicle_id, timestamp)가 두 프로세스에서
    # 나오지 않게 한다. 겹치면 InfluxDB에서 덮어써져 행 수가 줄어든다.
    base = args.shard * args.vehicles
    vehicles = [f"LOAD-{base + i:05d}" for i in range(args.vehicles)]

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        # 처리량이 목적이라 크게 묶는다. acks=1은 리더 확인만 — 이 실험은 유실 측정이
        # 아니라 드레인 속도 측정이고, 브로커가 1대라 acks=all과 차이도 없다.
        acks=1,
        linger_ms=50,
        batch_size=256 * 1024,
        # 압축은 쓰지 않는다 — 도구 이미지에 lz4/snappy/zstandard가 없어서
        # compression_type을 주면 발행 시점에 실패한다(설치 대신 확인해서 끈다).
        # gzip은 순수 파이썬 경로라 오히려 발행 속도를 떨어뜨린다.
        compression_type=None,
    )

    # 차량마다 자기 시작 시각을 다르게 두고 1ms씩 전진시킨다.
    start = datetime.now(timezone.utc) - timedelta(hours=1)
    offsets = {v: 0 for v in vehicles}

    t0 = time.time()
    for i in range(args.count):
        vid = vehicles[i % len(vehicles)]
        ts = start + timedelta(milliseconds=offsets[vid])
        offsets[vid] += 1
        payload = {
            "vehicle_id": vid,
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z",
            "speed": 60.0 + (i % 40),
            "rpm": 2000 + (i % 500),
            "engine_temp": 90.0,
            "throttle_position": 30.0,
            "fuel_level": 70.0,
            "battery_voltage": 13.8,
            "gps": {"lat": 37.5, "lng": 127.0},
            "dtc_codes": [],
        }
        producer.send(args.topic, key=vid, value=payload)
        if (i + 1) % 100_000 == 0:
            elapsed = time.time() - t0
            print(f"  shard {args.shard}: {i + 1:,}건 ({(i + 1) / elapsed:,.0f}/s)", flush=True)

    producer.flush()
    producer.close()
    elapsed = time.time() - t0
    print(f"shard {args.shard} 완료: {args.count:,}건 / {elapsed:.1f}초 "
          f"({args.count / elapsed:,.0f}/s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
