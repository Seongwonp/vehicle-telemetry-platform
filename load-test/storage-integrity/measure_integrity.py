#!/usr/bin/env python3
"""저장 경로 정합성 측정 — consumer 강제 종료·재전달 시 중복과 덮어쓰기를 수량화한다.

배경: Kafka는 at-least-once라 consumer가 offset을 커밋하기 전에 죽으면 같은 메시지가
다시 온다. 그때 InfluxDB에 무슨 일이 생기는지를 **추측하지 않고 세는** 것이 이 도구다.

InfluxDB 포인트의 identity는 (measurement, 태그 집합, 타임스탬프)다. 우리 코드에서는
`vehicle_telemetry` + `vehicle_id` 태그 + 밀리초 타임스탬프이므로(TelemetryRepository.toPoint):

- **같은 메시지가 재전달되면** identity가 같고 필드 값도 같아 덮어써진다 → 멱등, 행이 안 는다.
- **서로 다른 메시지가 같은 (vehicle_id, ms)를 가지면** 뒤엣것이 앞엣것을 덮어쓴다 → 조용한 유실.
  WritePrecision.S였을 때 실제로 50%가 사라졌던 버그가 이 경우다(ADR-014).

그래서 네 숫자를 모아 두 값을 뺀다:

    재전달 건수      = 저장 시도 건수 − 토픽 메시지 수
    덮어쓰기 유실    = 토픽 고유 키 수 − InfluxDB 행 수

`저장 시도 건수`는 Micrometer DistributionSummary `telemetry.influx.write.batch.size`의
합계다 — saveAll()에 넘어간 포인트 수라 재전달분이 그대로 포함된다.

사용법:
    python measure_integrity.py --bootstrap kafka:29092 \\
        --influx-url http://influxdb:8086 --influx-token ... \\
        --influx-org ... --influx-bucket ... \\
        --metrics-url http://backend:8080/actuator/prometheus
"""
import argparse
import json
import re
import sys
from collections import Counter

MEASUREMENT = "vehicle_telemetry"


def read_topic(bootstrap: str, topic: str, timeout_ms: int) -> tuple[int, Counter]:
    """토픽을 처음부터 끝까지 읽어 총 건수와 (vehicle_id, timestamp) 분포를 낸다.

    채점용이라 consumer group을 만들지 않는다(group_id=None) — 이 도구를 여러 번 돌려도
    실제 서비스의 offset에 영향을 주지 않기 위함이다.
    """
    from kafka import KafkaConsumer  # 지연 임포트 — --help만 볼 때 의존성 없이 뜨게

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        group_id=None,
    )
    total = 0
    keys: Counter = Counter()
    malformed = 0
    for message in consumer:
        total += 1
        try:
            data = json.loads(message.value.decode("utf-8"))
            keys[(data["vehicle_id"], data["timestamp"])] += 1
        except Exception:
            # 깨진 메시지는 애초에 DLQ로 가므로 InfluxDB 행 수와 비교할 대상이 아니다.
            malformed += 1
    consumer.close()
    return total, keys, malformed


def influx_row_count(url: str, token: str, org: str, bucket: str, minutes: int) -> int:
    """측정 구간의 포인트 수를 센다.

    `count()`는 field마다 세므로 그대로 쓰면 필드 수만큼 부풀려진다. 한 필드(speed)로
    좁혀서 "행 수 = 시계열 포인트 수"가 되게 한다.
    """
    import urllib.request

    flux = (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: -{minutes}m)\n'
        f'  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r._field == "speed")\n'
        f'  |> group()\n'
        f'  |> count()\n'
    )
    req = urllib.request.Request(
        f"{url.rstrip('/')}/api/v2/query?org={org}",
        data=flux.encode("utf-8"),
        headers={
            "Authorization": f"Token {token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        },
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read().decode("utf-8")

    # CSV 주석/헤더를 건너뛰고 _value 열을 집는다.
    header, value_row = None, None
    for line in body.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        cols = line.split(",")
        if header is None:
            header = cols
        else:
            value_row = cols
            break
    if header is None or value_row is None:
        return 0
    return int(value_row[header.index("_value")])


def metric_sum(metrics_text: str, name: str) -> float | None:
    """Prometheus 노출 텍스트에서 특정 메트릭의 값을 더한다(라벨 무시)."""
    total = None
    for line in metrics_text.splitlines():
        if line.startswith("#") or not line.startswith(name):
            continue
        # name{labels} value  또는  name value
        m = re.match(rf"{re.escape(name)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)$", line.strip())
        if m:
            total = (total or 0.0) + float(m.group(1))
    return total


def fetch_metrics(url: str) -> str:
    import urllib.request

    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.read().decode("utf-8")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="kafka:29092")
    p.add_argument("--topic", default="vehicle-telemetry")
    p.add_argument("--timeout-ms", type=int, default=20000)
    p.add_argument("--influx-url", required=True)
    p.add_argument("--influx-token", required=True)
    p.add_argument("--influx-org", required=True)
    p.add_argument("--influx-bucket", required=True)
    p.add_argument("--influx-window-min", type=int, default=60,
                   help="InfluxDB에서 최근 몇 분을 셀지 (측정 구간을 덮도록 넉넉히)")
    p.add_argument("--metrics-url", default="",
                   help="backend actuator prometheus URL (없으면 재전달 건수를 못 낸다)")
    # 재전달 건수는 "강제 종료 직후, 재시작 전"에 읽은 두 값으로 정확히 나온다.
    #   저장된 행 수  = 죽기 전까지 InfluxDB에 실제로 쓴 메시지 수
    #   커밋 offset  = 죽기 전까지 "처리 완료"로 표시한 메시지 수
    # 저장은 됐는데 커밋이 안 된 구간이 그대로 재전달된다.
    #
    # 메트릭(telemetry_influx_write_batch_size_sum)으로는 이걸 못 잰다 — 프로세스
    # 생명주기 카운터라 재시작하면 0으로 돌아가고, 종료 직전에 스냅샷을 찍어도
    # 찍는 순간과 죽는 순간 사이에 계속 처리돼서 값이 어긋난다(실제로 -270이 나왔다).
    p.add_argument("--pre-kill-rows", type=int, default=-1,
                   help="강제 종료 직후·재시작 전에 읽은 InfluxDB 행 수")
    p.add_argument("--pre-kill-committed", type=int, default=-1,
                   help="강제 종료 직후·재시작 전에 읽은 consumer group 커밋 offset 합계")
    args = p.parse_args()

    total, keys, malformed = read_topic(args.bootstrap, args.topic, args.timeout_ms)
    if total == 0:
        print("토픽이 비어 있다 — 부하가 실제로 들어갔는지 확인할 것.", file=sys.stderr)
        return 1

    unique = len(keys)
    collided_keys = sum(1 for c in keys.values() if c > 1)
    collided_extra = sum(c - 1 for c in keys.values() if c > 1)

    rows = influx_row_count(args.influx_url, args.influx_token, args.influx_org,
                            args.influx_bucket, args.influx_window_min)

    post_restart = None
    if args.metrics_url:
        try:
            post_restart = metric_sum(fetch_metrics(args.metrics_url),
                                      "telemetry_influx_write_batch_size_sum")
        except Exception as e:  # 메트릭은 부가 정보라 실패해도 나머지는 낸다.
            print(f"메트릭 수집 실패: {e}", file=sys.stderr)

    def pct(num, den):
        return f"{num / den * 100:.4f}%" if den else "n/a"

    print("=" * 66)
    print("입력 (Kafka 토픽에서 직접 셈)")
    print(f"  메시지 수             : {total:>10,}")
    print(f"  고유 (vehicle_id, ms) : {unique:>10,}")
    print(f"  깨진 메시지           : {malformed:>10,}  (DLQ 대상, 비교에서 제외)")
    if collided_keys:
        print(f"  ** 키 충돌 발생       : {collided_keys:,}개 키에 여분 {collided_extra:,}건")
    print("-" * 66)
    print("저장")
    if post_restart is not None:
        print(f"  재시작 후 saveAll     : {post_restart:>10,.0f}  (프로세스 생명주기 카운터)")
    print(f"  InfluxDB 행 수        : {rows:>10,}")
    print("=" * 66)

    redelivered = None
    if args.pre_kill_rows >= 0 and args.pre_kill_committed >= 0:
        redelivered = args.pre_kill_rows - args.pre_kill_committed
        print(f"강제 종료 시점 (재시작 전에 읽음)")
        print(f"  저장된 행 수          : {args.pre_kill_rows:>10,}")
        print(f"  커밋된 offset 합계    : {args.pre_kill_committed:>10,}")
        print(f"재전달 건수   = 저장된 행 − 커밋 offset = {redelivered:>8,} "
              f"({pct(redelivered, total)})")
        if redelivered < 0:
            print("  ** 음수다 — 저장보다 커밋이 앞섰다는 뜻이라 설계상 나올 수 없다. "
                  "두 값을 정말 '재시작 전'에 읽었는지 확인할 것.")

    lost = unique - rows
    print(f"덮어쓰기 유실 = 고유 키 − Influx 행 = {lost:>10,} ({pct(lost, unique)})")
    print()
    if redelivered is not None and redelivered > 0 and lost <= 0:
        print(f"판정: {redelivered:,}건이 재전달됐는데 InfluxDB 행 수가 고유 키 수와 같다 "
              "→ **재전달이 덮어쓰기로 흡수됐다(멱등)**.")
    elif lost > 0:
        print("판정: 고유 키보다 행이 적다 → 서로 다른 메시지가 같은 (vehicle_id, ms)로 "
              "겹쳐 덮어썼거나, 저장되지 않은 메시지가 있다. 둘을 가르려면 위 '키 충돌'을 볼 것.")
    else:
        print("판정: 고유 키 수와 InfluxDB 행 수가 일치한다(유실 없음). "
              "재전달 건수는 위 두 입력이 있어야 나온다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
