"""DLQ 레코드에서 InfluxDB 포인트 키(vehicle_id, timestamp)를 뽑는다.

## 왜 필요한가

InfluxDB 12분 장애 측정에서 `토픽 258,726` < `저장 252,615 + DLQ 6,158`으로
**47건이 초과**했다. 산술로는 "47건이 양쪽에 다 있다"는 뜻이지만, 그건 뺄셈이
말해주는 것이지 확인된 사실이 아니다.

가설: `docker stop`으로 InfluxDB를 세울 때 **서버는 쓰기를 적용했는데 클라이언트는
연결이 끊겨 실패로 본** in-doubt 쓰기가 있었고, 그 배치가 재시도 끝에 DLQ로 갔다.
2026-09-05에 PostgreSQL에서 찾은 in-doubt 트랜잭션과 같은 종류다.

맞다면 **DLQ 앞부분(장애 시작 직후 간 배치)만** InfluxDB에 있고, 뒷부분(장애 한복판에
간 배치)은 없어야 한다. 그 분포까지 봐야 가설이 확인된다 — "몇 건 겹친다"는 총합만으로는
다른 설명(재전달 등)과 구분되지 않는다.

InfluxDB 조회는 여기서 하지 않는다. 이 스크립트는 kafka-python이 있는 도구 이미지에서
돌고, `influx` CLI는 InfluxDB 컨테이너에만 있다. 키만 뽑아 주고 조회는 호출한 쪽이 한다.

사용법(도구 이미지 안에서):
    python dlq_keys.py --topic vehicle-telemetry-dlq --sample 20
출력:
    STAT total=6158 decodable=6158 unique=6158 undecodable=0
    HEAD SIM-012 2026-09-06T00:13:22.104Z
    TAIL SIM-045 2026-09-06T00:24:01.882Z
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import OrderedDict

from kafka import KafkaConsumer


def key_of(raw: bytes):
    """저장 경로의 포인트 키는 (measurement, vehicle_id 태그, ms 타임스탬프)다
    (`TelemetryRepository.toPoint()`). 역직렬화가 안 되는 payload는 애초에
    저장될 수 없으므로 키가 없다."""
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        return None
    vid = payload.get("vehicle_id")
    ts = payload.get("timestamp")
    if not vid or not ts:
        return None
    return (vid, ts)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="vehicle-telemetry-dlq")
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--timeout-ms", type=int, default=20000)
    ap.add_argument("--sample", type=int, default=20)
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=args.timeout_ms,
        group_id=None,
    )
    keys, undecodable, total = [], 0, 0
    for msg in consumer:
        total += 1
        k = key_of(msg.value)
        if k is None:
            undecodable += 1
        else:
            keys.append(k)
    consumer.close()

    uniq = list(OrderedDict.fromkeys(keys))
    print(f"STAT total={total} decodable={len(keys)} unique={len(uniq)} "
          f"undecodable={undecodable} dup_in_dlq={len(keys) - len(uniq)}")

    n = min(args.sample, len(uniq))
    for vid, ts in uniq[:n]:
        print(f"HEAD {vid} {ts}")
    for vid, ts in uniq[-n:]:
        print(f"TAIL {vid} {ts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
