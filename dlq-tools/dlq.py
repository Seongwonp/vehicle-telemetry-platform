#!/usr/bin/env python3
"""DLQ 조사·재처리 도구.

DLQ는 지금까지 "격리해서 유실을 막는" 데까지만 했다(ADR-017). 격리된 메시지를 실제로
어떻게 되돌릴지는 정해진 적이 없고, 그게 이 도구와 `docs/runbook/dlq-reprocessing.md`다.

## 왜 그냥 다 재처리하면 안 되는가

DLQ에는 성질이 정반대인 두 종류가 섞여 있다.

- **일시적 실패**: InfluxDB가 죽어 재시도가 소진된 경우. DB가 살아나면 다시 넣기만 하면
  성공한다. 재처리해야 데이터가 복구된다.
- **영구 실패**: JSON이 깨졌거나 타임스탬프 형식이 틀린 경우. 몇 번을 다시 넣어도
  같은 자리에서 실패한다. 재처리하면 **원본 토픽 → 컨슈머 실패 → DLQ**로 되돌아오는
  무한 루프가 되고, 그동안 정상 트래픽의 처리량까지 갉아먹는다.

둘을 가르는 근거는 payload가 아니라 **실패 원인**이다. 그래서 이 도구는 헤더를 본다:

- 레코드 단위 격리(`sendToDlq`) → `x-dlq-failure-class`, `x-dlq-failure-message`
- 재시도 소진(`DeadLetterPublishingRecoverer`) → Spring이 붙이는 `kafka_dlt-exception-fqcn` 등

두 경로가 같은 토픽에 섞이므로 둘 다 읽는다.

## 안전장치

1. `replay`는 기본적으로 **일시적 실패로 분류된 것만** 보낸다. 영구 실패를 보내려면
   `--include-permanent`를 명시해야 한다(정말 코드를 고쳐서 이제 처리 가능해진 경우).
2. 재처리할 때마다 `x-dlq-replay-count`를 올리고, `--max-replays`(기본 3)를 넘은
   레코드는 건너뛴다 — 분류가 틀렸어도 무한 루프가 되지 않는다.
3. `--dry-run`이 기본값이다. 실제로 보내려면 `--execute`를 붙여야 한다.
4. **`replay`는 커서(consumer group 커밋 offset)를 쓴다.** Kafka는 레코드를 지울 수
   없으므로 매번 DLQ를 처음부터 읽으면 이미 되돌린 것을 또 되돌리고, 그것들이 다시
   실패해 쌓인다 — 실행할 때마다 DLQ가 배로 늘어난다(실측: 4회에 2→4→8→16건).
   발행에 성공한 지점까지 커밋해서 다음 실행이 그 뒤부터 보게 한다.
   `inspect`는 커서를 쓰지 않는다 — 조사가 재처리 상태를 건드리면 안 되기 때문이다.

사용법:
    python dlq.py inspect --bootstrap kafka:29092 --topic vehicle-telemetry-dlq
    python dlq.py replay  --bootstrap kafka:29092 --topic vehicle-telemetry-dlq \\
        --target vehicle-telemetry --execute
"""
import argparse
import sys
from collections import Counter

# 일시적이라고 볼 예외 — 외부 의존성 장애나 타임아웃처럼 "나중에 다시 하면 되는" 것들.
# 이름 조각으로 매칭한다(Java FQCN과 Python 클래스명을 함께 다루기 위해).
TRANSIENT_MARKERS = (
    "InfluxException",
    "InfluxDBException",
    "SocketTimeout",
    "ConnectException",
    "ConnectionError",
    "TimeoutException",
    "IllegalStateException",   # DLQ 전송 실패 등 인프라 쪽 상태 문제
    "CannotCreateTransaction",
    "DataAccessResourceFailure",
    "OperationTimeout",
)

# 영구적이라고 볼 예외 — 메시지 내용 자체가 처리 불가능한 것들.
PERMANENT_MARKERS = (
    "JsonParseException",
    "JsonMappingException",
    "MismatchedInputException",
    "InvalidFormatException",
    "DateTimeParseException",
    "JSONDecodeError",
    "UnicodeDecodeError",
    "NumberFormatException",
    "NullPointerException",
)

REPLAY_COUNT_HEADER = "x-dlq-replay-count"


def header_map(record) -> dict[str, str]:
    """kafka-python의 headers(list of tuple)를 문자열 dict로."""
    out: dict[str, str] = {}
    for key, value in (record.headers or []):
        try:
            out[key] = value.decode("utf-8", errors="replace") if value is not None else ""
        except Exception:
            out[key] = repr(value)
    return out


def failure_class(headers: dict[str, str]) -> str:
    """두 발행 경로의 헤더 이름이 달라서 순서대로 찾는다.

    `kafka_dlt-exception-cause-fqcn`을 `-fqcn`보다 **먼저** 본다. Spring은 리스너에서
    나온 예외를 `ListenerExecutionFailedException`으로 감싸므로, `-fqcn`만 보면
    실제 원인이 InfluxDB 장애든 JSON 파싱 실패든 전부 같은 wrapper 이름으로 보인다 —
    InfluxDB 90초 장애를 주입했더니 DLQ 76,878건이 전부 `unknown`으로 분류돼
    (실제 원인은 `com.influxdb.exceptions.InfluxException`) 자동 재처리 대상이
    하나도 안 나왔다. 그때 발견해서 고쳤다.
    """
    for key in ("x-dlq-failure-class",
                "kafka_dlt-exception-cause-fqcn",
                "kafka_dlt-exception-fqcn"):
        if headers.get(key):
            return headers[key]
    return ""


def classify(headers: dict[str, str]) -> str:
    """transient / permanent / unknown.

    unknown은 헤더가 없는 옛 레코드이거나 우리가 목록에 넣지 않은 예외다.
    **unknown을 자동으로 재처리하지 않는다** — 모르는 것을 일시적이라고 가정하면
    영구 실패를 루프에 태우게 된다. 사람이 보고 판단하라는 뜻이다.
    """
    fqcn = failure_class(headers)
    if not fqcn:
        return "unknown"
    for marker in PERMANENT_MARKERS:
        if marker in fqcn:
            return "permanent"
    for marker in TRANSIENT_MARKERS:
        if marker in fqcn:
            return "transient"
    return "unknown"


def replay_count(headers: dict[str, str]) -> int:
    try:
        return int(headers.get(REPLAY_COUNT_HEADER, "0"))
    except ValueError:
        return 0


def read_dlq(bootstrap: str, topic: str, timeout_ms: int, limit: int) -> list:
    """조사용 — 커서를 쓰지 않고 항상 전체를 읽는다.

    inspect가 재처리 진행 상태를 건드리면 안 되기 때문에 group_id를 주지 않는다.
    """
    consumer = make_dlq_consumer(bootstrap, topic, timeout_ms, None)
    try:
        return drain(consumer, limit)
    finally:
        consumer.close()


def make_dlq_consumer(bootstrap: str, topic: str, timeout_ms: int, group_id):
    from kafka import KafkaConsumer  # 지연 임포트 — --help만 볼 때 의존성 없이 뜨게

    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        # group_id=None이면 커서 없이 매번 전체를 읽는다 — inspect 전용이다.
        # replay는 group_id를 줘서 진행 지점을 커밋한다. Kafka는 레코드를 지울 수 없어서,
        # 커서 없이 매번 처음부터 읽으면 이미 되돌린 레코드를 또 되돌린다 —
        # 실행할 때마다 DLQ가 배로 늘어난다(실측: 4회에 2→4→8→16건).
        group_id=group_id,
    )


def drain(consumer, limit: int) -> list:
    records = []
    for record in consumer:
        records.append(record)
        if limit and len(records) >= limit:
            break
    return records


def cmd_inspect(args) -> int:
    records = read_dlq(args.bootstrap, args.topic, args.timeout_ms, args.limit)
    if not records:
        print(f"{args.topic}: 비어 있다.")
        return 0

    by_class: Counter = Counter()
    by_verdict: Counter = Counter()
    by_origin: Counter = Counter()
    over_limit = 0
    samples: dict[str, object] = {}

    for record in records:
        headers = header_map(record)
        verdict = classify(headers)
        fqcn = failure_class(headers) or "(헤더 없음)"
        by_class[fqcn] += 1
        by_verdict[verdict] += 1
        by_origin[headers.get("x-dlq-origin-topic",
                              headers.get("kafka_dlt-original-topic", "(불명)"))] += 1
        if replay_count(headers) >= args.max_replays:
            over_limit += 1
        samples.setdefault(fqcn, record)

    total = len(records)
    print("=" * 70)
    print(f"{args.topic}: {total:,}건")
    print("-" * 70)
    print("판정")
    for verdict in ("transient", "permanent", "unknown"):
        count = by_verdict.get(verdict, 0)
        label = {"transient": "일시적 — 재처리 대상",
                 "permanent": "영구 — 재처리하면 루프",
                 "unknown": "불명 — 사람이 봐야 함"}[verdict]
        print(f"  {verdict:<10} {count:>7,}  ({count/total*100:5.1f}%)  {label}")
    if over_limit:
        print(f"  ** 재처리 {args.max_replays}회를 넘긴 레코드 {over_limit:,}건 — replay가 건너뛴다")
    print("-" * 70)
    print("원인별")
    for fqcn, count in by_class.most_common(15):
        print(f"  {count:>7,}  {fqcn}")
    print("-" * 70)
    print("원본 토픽별")
    for origin, count in by_origin.most_common():
        print(f"  {count:>7,}  {origin}")

    if args.show_samples:
        print("-" * 70)
        print("원인별 표본 1건")
        for fqcn, record in list(samples.items())[:5]:
            headers = header_map(record)
            value = (record.value or b"").decode("utf-8", errors="replace")
            print(f"\n  [{fqcn}]")
            print(f"    key      : {(record.key or b'').decode('utf-8', errors='replace')}")
            print(f"    원인 메시지: {headers.get('x-dlq-failure-message', headers.get('kafka_dlt-exception-message', '(없음)'))[:200]}")
            print(f"    payload  : {value[:200]}")
    print("=" * 70)
    return 0


def cmd_replay(args) -> int:
    """커서(consumer group)를 써서 이미 되돌린 레코드를 다시 되돌리지 않는다.

    커서가 없으면 실행할 때마다 DLQ 전체를 다시 되돌리고, 되돌린 것들이 다시
    실패해 DLQ에 쌓이므로 실행 횟수에 따라 레코드가 배로 늘어난다.
    `--max-replays`만으로는 이 증식을 막지 못한다(원본 레코드의 카운트는 늘 0이라
    매번 재처리 대상이 된다) — 실측으로 확인하고 커서를 넣었다.
    """
    group = args.group or f"dlq-replay-{args.topic}"
    consumer = make_dlq_consumer(args.bootstrap, args.topic, args.timeout_ms, group)
    try:
        return _replay(args, consumer, drain(consumer, args.limit), group)
    finally:
        consumer.close()


def _replay(args, consumer, records, group) -> int:
    from kafka import KafkaProducer

    print(f"커서 그룹: {group}")
    if not records:
        print(f"{args.topic}: 커서 이후 새 레코드가 없다 — 되돌릴 것이 없다.")
        print("전체를 처음부터 다시 보려면 --group 에 새 이름을 줘라.")
        return 0

    selected, skipped = [], Counter()
    for record in records:
        headers = header_map(record)
        verdict = classify(headers)
        if replay_count(headers) >= args.max_replays:
            skipped["재처리 횟수 초과"] += 1
            continue
        if verdict == "permanent" and not args.include_permanent:
            skipped["영구 실패 (--include-permanent 필요)"] += 1
            continue
        if verdict == "unknown" and not args.include_unknown:
            skipped["불명 (--include-unknown 필요)"] += 1
            continue
        selected.append((record, headers))

    print(f"{args.topic} → {args.target}")
    print(f"  읽음     : {len(records):,}건")
    print(f"  재처리 대상: {len(selected):,}건")
    for reason, count in skipped.items():
        print(f"  건너뜀   : {count:,}건 — {reason}")

    if not args.execute:
        print("\n--dry-run 상태다(기본값). 실제로 보내려면 --execute를 붙여라.")
        return 0
    if not selected:
        print("\n보낼 것이 없다.")
        return 0

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", retries=3)
    sent, failed = 0, 0
    for record, headers in selected:
        # 원본 헤더는 그대로 넘기고 재처리 횟수만 올린다 — 다음번 판단에 이력이 필요하다.
        new_headers = [(k, v) for k, v in (record.headers or [])
                       if k != REPLAY_COUNT_HEADER]
        new_headers.append((REPLAY_COUNT_HEADER,
                            str(replay_count(headers) + 1).encode("utf-8")))
        try:
            future = producer.send(args.target, key=record.key, value=record.value,
                                   headers=new_headers)
            future.get(timeout=30)
            sent += 1
        except Exception as e:
            failed += 1
            print(f"  발행 실패 offset={record.offset}: {e}", file=sys.stderr)
    producer.flush()
    producer.close()

    print(f"\n발행 완료: {sent:,}건 성공, {failed:,}건 실패")
    if failed:
        # 커밋하지 않는다 — 실패한 지점부터 다시 볼 수 있어야 한다.
        print("실패분은 DLQ에 그대로 남아 있고 커서도 전진시키지 않았다. "
              "원인을 고친 뒤 다시 돌려라.")
        return 1

    # 여기까지 왔으면 이번에 읽은 레코드는 모두 처리됐다(발행했거나 의도적으로 건너뛰었다).
    # 커서를 전진시켜야 다음 실행이 같은 것을 또 되돌리지 않는다 — 이게 없으면
    # 실행할 때마다 DLQ가 배로 늘어난다(실측: 2→4→8→16건).
    consumer.commit()
    print(f"커서 전진 완료 — 다음 실행은 이 지점 이후만 본다(그룹 {group}).")
    print("주의: 원본 DLQ 레코드 자체는 지워지지 않는다(Kafka는 임의 삭제가 안 된다).")
    print("      건너뛴 레코드도 커서를 넘겼으므로, 다시 보려면 --group에 새 이름을 줘라.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--topic", required=True, help="읽을 DLQ 토픽")
    parser.add_argument("--timeout-ms", type=int, default=15000)
    parser.add_argument("--limit", type=int, default=0, help="최대 몇 건까지 읽을지 (0=전부)")
    parser.add_argument("--max-replays", type=int, default=3,
                        help="이 횟수 이상 재처리된 레코드는 건너뛴다 (무한 루프 방지)")
    sub = parser.add_subparsers(dest="command", required=True)

    inspect = sub.add_parser("inspect", help="DLQ 내용을 원인별로 분류해 보여준다")
    inspect.add_argument("--show-samples", action="store_true",
                         help="원인별 표본 payload를 함께 출력")
    inspect.set_defaults(func=cmd_inspect)

    replay = sub.add_parser("replay", help="선택된 레코드를 원본 토픽으로 되돌린다")
    replay.add_argument("--target", required=True, help="되돌릴 원본 토픽")
    replay.add_argument("--execute", action="store_true",
                        help="실제로 발행한다 (없으면 dry-run)")
    replay.add_argument("--include-permanent", action="store_true",
                        help="영구 실패로 분류된 것도 보낸다 — 코드를 고쳐 이제 처리 가능할 때만")
    replay.add_argument("--include-unknown", action="store_true",
                        help="분류 불명(헤더 없는 옛 레코드 등)도 보낸다")
    replay.add_argument("--group", default="",
                        help="커서로 쓸 consumer group 이름 (기본 dlq-replay-<topic>). "
                             "전체를 처음부터 다시 보려면 새 이름을 줘라")
    replay.set_defaults(func=cmd_replay)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
