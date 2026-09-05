#!/usr/bin/env python3
"""DLQ에 들어간 알림들의 `event_id`만 한 줄씩 뽑는다.

재처리가 멱등한지 보려면 먼저 **재처리 대상 중 몇 건이 이미 저장돼 있는가**를
알아야 한다. 이미 저장된 것이 0건이면 재처리가 중복을 만들 기회 자체가 없어서,
"중복이 안 났다"는 결과가 아무것도 증명하지 못하기 때문이다.

실측에서는 PostgreSQL 60초 장애로 DLQ에 9건이 들어갔는데 그중 **3건은 이미
저장돼 있었다** — 서버에서는 커밋이 끝났는데 연결이 끊겨 클라이언트만 실패로 본
경우다(`DataAccessResourceFailureException: Unable to commit`). 즉 재처리는
"혹시 중복될 수도 있는" 게 아니라 **반드시 중복 INSERT를 시도한다.**

출력을 psql의 `WHERE event_id IN (...)`에 그대로 넣어 쓴다.
"""
import argparse
import json
import sys

from kafka import KafkaConsumer


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka:29092")
    ap.add_argument("--topic", default="vehicle-anomaly-alerts-dlq")
    ap.add_argument("--timeout-ms", type=int, default=15000)
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # 커서를 쓰지 않는다 — 관측이 재처리 상태를 건드리면 안 된다.
        group_id=None,
        consumer_timeout_ms=args.timeout_ms,
    )
    for record in consumer:
        try:
            payload = json.loads((record.value or b"").decode("utf-8"))
        except Exception:
            # payload가 원본 알림이 아닌 레코드(envelope 등)는 대조 대상이 아니다.
            continue
        event_id = payload.get("event_id")
        if event_id:
            print(event_id)
    consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
