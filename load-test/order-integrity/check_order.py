#!/usr/bin/env python3
"""차량별 메시지 순서 역전을 센다 (docs/roadmap.md P1-1).

## 무엇을 재는가

`vehicle-telemetry` 토픽을 처음부터 읽어, **차량마다** 메시지가 타임스탬프 순서대로
들어 있는지 본다. 역전(inversion) = 앞에 온 메시지보다 타임스탬프가 이른 메시지.

## 왜 이걸 재야 하는가

`TelemetryProducer.retryPending()` 주석에 이미 이렇게 적혀 있다:

> 브로커 복구 직후 아주 짧은 구간에서는 새 메시지가 spool 드레인보다 먼저 Kafka에
> 닿아 같은 차량 메시지의 순서가 뒤집힐 수 있다(backlog 플래그가 best-effort라서).

**가설은 적혀 있는데 재본 적이 없다.** 얼마나, 어느 구간에서, 얼마나 과거로 되돌아가는지
모른 채 "영향 없다"고 적어둔 상태다. 성공 기준을 "역전 0"으로 미리 정하지 않는다 —
역전이 나오면 범위와 영향을 보고 허용할지 보정할지 정한다.

## 순서를 어떻게 정의하는가

Kafka의 순서는 **파티션 안에서만** 보장된다. 파티션 키가 `vehicle_id`라 한 차량은 한
파티션에 몰리므로, 차량별 순서 = 그 차량 레코드들의 offset 순서다. 그래서 각 차량의
레코드를 `(partition, offset)`으로 정렬한 뒤 타임스탬프 단조성을 본다.

**차량이 여러 파티션에 걸치면 그 자체가 발견이다** — 키 기반 파티셔닝이 깨진 것이고,
그러면 순서 보장의 전제가 무너진다. 그래서 따로 센다.

사용법:
    python check_order.py --bootstrap kafka:29092 [--json]
"""
import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer


def parse_ts(value: str) -> float:
    """ISO-8601(밀리초) → epoch ms. 형식이 다르면 None."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--topic", default="vehicle-telemetry")
    ap.add_argument("--timeout-ms", type=int, default=30000)
    ap.add_argument("--json", action="store_true", help="집계만 JSON으로 출력")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # group_id 없이 읽는다 — 관측이 실제 컨슈머의 offset을 건드리면 안 된다.
        group_id=None,
        consumer_timeout_ms=args.timeout_ms,
    )

    # vehicle_id -> [(partition, offset, ts_ms)]
    per_vehicle = defaultdict(list)
    total = 0
    undecodable = 0
    bad_timestamp = 0

    for record in consumer:
        total += 1
        try:
            payload = json.loads(record.value.decode("utf-8"))
        except Exception:
            undecodable += 1
            continue
        ts = parse_ts(payload.get("timestamp"))
        if ts is None:
            bad_timestamp += 1
            continue
        per_vehicle[payload.get("vehicle_id", "UNKNOWN")].append(
            (record.partition, record.offset, ts))
    consumer.close()

    multi_partition = 0
    inversions = 0
    vehicles_with_inversion = 0
    max_back_ms = 0.0
    worst = []          # (vehicle, offset, 되돌아간 ms)
    inversion_offsets = []

    for vehicle, rows in per_vehicle.items():
        if len({p for p, _, _ in rows}) > 1:
            multi_partition += 1
        # 파티션 안의 순서 = offset 순서. 파티션이 섞여 있어도 정렬 기준은 같다.
        rows.sort(key=lambda r: (r[0], r[1]))
        found = 0
        prev_ts = None
        for _, offset, ts in rows:
            if prev_ts is not None and ts < prev_ts:
                found += 1
                back = prev_ts - ts
                max_back_ms = max(max_back_ms, back)
                worst.append((vehicle, offset, back))
                inversion_offsets.append(offset)
            # 최대값을 기준으로 본다 — 한 번 뒤집힌 뒤 이어지는 정상 메시지까지
            # 역전으로 세면 한 건의 사고가 수백 건으로 부풀려진다.
            prev_ts = max(prev_ts, ts) if prev_ts is not None else ts
        if found:
            vehicles_with_inversion += 1
            inversions += found

    worst.sort(key=lambda w: -w[2])

    # 되돌아간 크기의 분포. 총 건수만으로는 "잠깐 뒤섞였다"와 "한참 묵은 게 늦게 왔다"를
    # 구분할 수 없는데, 사용자 영향은 그 둘이 완전히 다르다.
    buckets = [(1_000, "1초 이하"), (10_000, "10초 이하"), (60_000, "1분 이하"),
               (300_000, "5분 이하"), (float("inf"), "5분 초과")]
    hist = {label: 0 for _, label in buckets}
    for _, _, back in worst:
        for limit, label in buckets:
            if back <= limit:
                hist[label] += 1
                break

    # 차량당 역전 수 — 한두 대에 몰렸는지 전체에 퍼졌는지.
    per_vehicle_counts = defaultdict(int)
    for vehicle, _, _ in worst:
        per_vehicle_counts[vehicle] += 1
    counts = sorted(per_vehicle_counts.values())

    summary = {
        "total_messages": total,
        "undecodable": undecodable,
        "bad_timestamp": bad_timestamp,
        "vehicles": len(per_vehicle),
        "vehicles_multi_partition": multi_partition,
        "inversions": inversions,
        "vehicles_with_inversion": vehicles_with_inversion,
        "inversion_rate_pct": round(inversions / total * 100, 4) if total else 0,
        "max_backward_ms": round(max_back_ms),
        "inversion_offset_min": min(inversion_offsets) if inversion_offsets else None,
        "inversion_offset_max": max(inversion_offsets) if inversion_offsets else None,
        "backward_histogram": hist,
        "per_vehicle_min": counts[0] if counts else 0,
        "per_vehicle_median": counts[len(counts) // 2] if counts else 0,
        "per_vehicle_max": counts[-1] if counts else 0,
    }

    if args.json:
        print(json.dumps(summary, ensure_ascii=False))
        return 0

    print(f"토픽                      : {args.topic}")
    print(f"메시지 총계               : {total:,}")
    print(f"  JSON 파싱 실패          : {undecodable}")
    print(f"  타임스탬프 파싱 실패    : {bad_timestamp}")
    print(f"차량 수                   : {len(per_vehicle):,}")
    print(f"  여러 파티션에 걸친 차량 : {multi_partition}  ← 0이 아니면 키 파티셔닝이 깨진 것")
    print(f"역전 건수                 : {inversions:,}  ({summary['inversion_rate_pct']}%)")
    print(f"  역전이 난 차량          : {vehicles_with_inversion:,}")
    print(f"  최대 되돌아간 시간      : {summary['max_backward_ms']:,} ms")
    if inversion_offsets:
        print(f"  역전 발생 offset 범위   : {summary['inversion_offset_min']:,} ~ "
              f"{summary['inversion_offset_max']:,}")
        print("  되돌아간 크기 분포:")
        for _, label in [(1_000, "1초 이하"), (10_000, "10초 이하"), (60_000, "1분 이하"),
                         (300_000, "5분 이하"), (0, "5분 초과")]:
            print(f"    {label:<9} {hist[label]:>6,}")
        print(f"  차량당 역전 수            : 최소 {summary['per_vehicle_min']} / "
              f"중앙 {summary['per_vehicle_median']} / 최대 {summary['per_vehicle_max']}")
        print("  가장 큰 역전 5건:")
        for vehicle, offset, back in worst[:5]:
            print(f"    {vehicle} offset={offset:,}  {round(back):,}ms 과거")
    return 0


if __name__ == "__main__":
    sys.exit(main())
