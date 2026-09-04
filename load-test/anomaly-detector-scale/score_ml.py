#!/usr/bin/env python3
"""ML 이상 감지 채점 — 시뮬레이터 정답 로그와 알림 토픽을 조인해 recall/FP를 낸다.

배경: 12차 측정에서 이 시뮬레이터로는 ML 탐지 품질을 잴 수 없다는 걸 확인했다
(주입한 이상값이 전부 룰 임계값과 대응해서 정답이 곧 룰이 잡는 것이었다).
그래서 시뮬레이터에 복합 이상(COMPOSITE_ANOMALY_RATE)과 분포 이동(DRIFT_*)을 추가했고,
이 스크립트가 그 정답으로 채점한다.

조인 키는 (vehicle_id, timestamp)다. 알림 페이로드의 timestamp가 원본 메시지 값을
그대로 물고 오므로(rules.py, anomaly_detector.py 모두 data["timestamp"] 사용)
정답 로그와 정확히 맞출 수 있다.

사용법:
    python score_ml.py --sim-containers telemetry-sim-0,telemetry-sim-1 \\
                       --bootstrap localhost:9092 [--timeout-ms 15000]
"""
import argparse
import json
import re
import subprocess
import sys
from collections import Counter

GT_PATTERN = re.compile(
    r"\[GT\] vehicle=(?P<vehicle>\S+) ts=(?P<ts>\S+) label=(?P<label>\S+) kind=(?P<kind>\S+)"
)

# anomaly-detector가 ML_SCORE_DUMP=true일 때 남기는 줄.
SCORE_PATTERN = re.compile(
    r"\[SCORE\] vehicle=(?P<vehicle>\S+) ts=(?P<ts>\S+) score=(?P<score>\S+) flag=(?P<flag>\d)"
)


def _collect(lines, truth: dict[tuple[str, str], dict]) -> None:
    for line in lines:
        m = GT_PATTERN.search(line)
        if m:
            truth[(m["vehicle"], m["ts"])] = {"label": m["label"], "kind": m["kind"]}


def load_ground_truth(containers: list[str], files: list[str]) -> dict[tuple[str, str], dict]:
    """[GT] 줄을 (vehicle, ts) → {label, kind} 로 만든다.

    컨테이너 로그와 저장된 파일 둘 다 받는다. 시뮬레이터를 `--rm`으로 띄우면 컨테이너를
    내리는 순간 로그가 사라지므로(실제로 한 번 날려먹었다), 측정 중에 파일로 받아두고
    그 파일로 채점하는 쪽이 안전하다:
        docker logs telemetry-sim-0 > gt0.log 2>&1
    """
    truth: dict[tuple[str, str], dict] = {}
    for name in containers:
        out = subprocess.run(
            ["docker", "logs", name],
            capture_output=True, text=True, errors="replace",
        )
        _collect((out.stdout + out.stderr).splitlines(), truth)
    for path in files:
        with open(path, encoding="utf-8", errors="replace") as fh:
            _collect(fh, truth)
    return truth


def load_alerts(bootstrap: str, topic: str, timeout_ms: int) -> list[dict]:
    """알림 토픽을 처음부터 끝까지 읽는다. 채점용이라 별도 그룹으로 매번 새로 읽는다."""
    from kafka import KafkaConsumer  # 지연 임포트 — 도움말만 볼 때 의존성 없이 뜨게

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        group_id=None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    alerts = [m.value for m in consumer]
    consumer.close()
    return alerts


def load_scores(containers: list[str], files: list[str]) -> dict[tuple[str, str], float]:
    """[SCORE] 줄을 (vehicle, ts) → score 로 만든다(ML_SCORE_DUMP=true로 받은 것).

    같은 키가 두 번 나오면(리밸런싱 후 재처리 등) 나중 값으로 덮는다 — 그때의 모델이
    더 많이 학습된 상태라 실제 판정에 가깝다.
    """
    scores: dict[tuple[str, str], float] = {}

    def collect(lines):
        for line in lines:
            m = SCORE_PATTERN.search(line)
            if m:
                try:
                    value = float(m["score"])
                except ValueError:
                    continue
                if value == value:  # nan 제외 — 학습 전이라 점수가 없는 구간
                    scores[(m["vehicle"], m["ts"])] = value

    for name in containers:
        out = subprocess.run(["docker", "logs", name],
                             capture_output=True, text=True, errors="replace")
        collect((out.stdout + out.stderr).splitlines())
    for path in files:
        with open(path, encoding="utf-8", errors="replace") as fh:
            collect(fh)
    return scores


def print_sweep(scores: dict[tuple[str, str], float],
                truth: dict[tuple[str, str], dict],
                steps: int) -> None:
    """임계값을 훑으며 복합 이상 recall과 오탐률을 낸다.

    지금 구조(contamination=0.05)는 "학습 분포의 하위 5%"를 무조건 찍는다. 그걸
    "점수가 임계값보다 낮으면 이상"으로 바꾸려면 임계값을 정해야 하는데, 값을 하나씩
    찍어 부하를 반복하면 측정 한 번에 점 하나밖에 못 얻는다. 점수를 전부 받아두면
    한 번의 측정으로 곡선 전체를 볼 수 있다 — 이 함수가 그 계산이다.

    점수는 낮을수록 이상이므로 `score < threshold`가 알림이다.
    """
    if not scores:
        print("점수 덤프가 없다 — ML_SCORE_DUMP=true로 돌렸는지, "
              "--score-files/--detector-containers 경로가 맞는지 확인할 것.", file=sys.stderr)
        return

    composite = {k for k, v in truth.items() if v["kind"] == "composite"}
    # 정답에 없는 메시지 = 정상. 룰 이상은 어차피 룰이 100% 잡으므로 ML 오탐 계산에서
    # 빼지 않고 "정답" 쪽에 둔다(알림이 떠도 헛알림은 아니다).
    scored_composite = [s for k, s in scores.items() if k in composite]
    scored_normal = [s for k, s in scores.items() if k not in truth]

    if not scored_composite:
        print("점수와 조인된 복합 이상이 없다 — 정답/점수 수집 구간이 어긋났을 수 있다.",
              file=sys.stderr)
        return

    lo = min(min(scored_composite), min(scored_normal or [0.0]))
    hi = max(max(scored_composite), max(scored_normal or [0.0]))

    print("-" * 62)
    print(f"임계값 스윕 (점수와 조인된 메시지: 복합 {len(scored_composite):,}건 / "
          f"정상 {len(scored_normal):,}건)")
    print(f"  {'임계값':>10} {'복합 recall':>12} {'정상 오탐률':>12} {'알림 비율':>10}")
    total = len(scored_composite) + len(scored_normal)
    for i in range(steps + 1):
        th = lo + (hi - lo) * i / steps
        rec = sum(1 for s in scored_composite if s < th)
        fp = sum(1 for s in scored_normal if s < th)
        print(f"  {th:>10.4f} {rec/len(scored_composite)*100:>11.1f}% "
              f"{(fp/len(scored_normal)*100 if scored_normal else 0):>11.1f}% "
              f"{(rec+fp)/total*100:>9.1f}%")
    print("-" * 62)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sim-containers", default="",
                        help="쉼표로 구분한 시뮬레이터 컨테이너 이름")
    parser.add_argument("--gt-files", default="",
                        help="쉼표로 구분한 정답 로그 파일 경로 (docker logs를 미리 받아둔 것)")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="vehicle-anomaly-alerts")
    parser.add_argument("--timeout-ms", type=int, default=15000,
                        help="알림 토픽에서 이 시간 동안 새 메시지가 없으면 읽기를 끝낸다")
    parser.add_argument("--sweep", action="store_true",
                        help="ML_SCORE_DUMP로 받은 점수로 임계값별 recall/오탐을 계산한다")
    parser.add_argument("--score-files", default="",
                        help="쉼표로 구분한 점수 덤프 파일 경로 (docker logs를 받아둔 것)")
    parser.add_argument("--detector-containers", default="",
                        help="쉼표로 구분한 anomaly-detector 컨테이너 이름")
    parser.add_argument("--sweep-steps", type=int, default=20,
                        help="스윕 구간을 몇 등분할지 (기본 20)")
    args = parser.parse_args()

    containers = [c.strip() for c in args.sim_containers.split(",") if c.strip()]
    files = [f.strip() for f in args.gt_files.split(",") if f.strip()]
    if not containers and not files:
        parser.error("--sim-containers 또는 --gt-files 중 하나는 필요하다")
    truth = load_ground_truth(containers, files)
    alerts = load_alerts(args.bootstrap, args.topic, args.timeout_ms)

    if not truth:
        print("정답이 없다 — COMPOSITE_ANOMALY_RATE/ANOMALY_RATE가 0이거나 "
              "컨테이너 이름이 틀렸을 수 있다.", file=sys.stderr)
        return 1

    # 같은 (vehicle, ts)에 룰·ML 알림이 동시에 뜰 수 있으므로 감지기별로 따로 모은다.
    ml_hits: set[tuple[str, str]] = set()
    rule_hits: set[tuple[str, str]] = set()
    for a in alerts:
        key = (a.get("vehicle_id"), a.get("timestamp"))
        if a.get("detector") == "ML":
            ml_hits.add(key)
        else:
            rule_hits.add(key)

    gt_rule = {k for k, v in truth.items() if v["kind"] == "rule"}
    gt_composite = {k for k, v in truth.items() if v["kind"] == "composite"}

    def pct(num: int, den: int) -> str:
        return f"{num/den*100:.1f}%" if den else "n/a"

    print("=" * 62)
    print("정답 (시뮬레이터 주입)")
    print(f"  룰 이상   : {len(gt_rule):>7,}건")
    print(f"  복합 이상 : {len(gt_composite):>7,}건")
    print(f"알림 (토픽에서 읽음)")
    print(f"  룰 알림   : {len(rule_hits):>7,}건 (고유 메시지 기준)")
    print(f"  ML 알림   : {len(ml_hits):>7,}건")
    print("=" * 62)

    # ── 룰 회귀 확인: 룰 이상은 여전히 전부 잡혀야 한다 ──────────────
    rule_caught = len(gt_rule & rule_hits)
    print(f"[룰] 룰 이상 recall        : {rule_caught:,}/{len(gt_rule):,} "
          f"({pct(rule_caught, len(gt_rule))})")

    # ── ML의 존재 이유: 룰이 못 잡는 복합 패턴을 잡는가 ───────────────
    comp_by_ml = len(gt_composite & ml_hits)
    comp_by_rule = len(gt_composite & rule_hits)
    print(f"[ML] 복합 이상 recall      : {comp_by_ml:,}/{len(gt_composite):,} "
          f"({pct(comp_by_ml, len(gt_composite))})")
    print(f"     └ 복합을 룰이 잡은 수 : {comp_by_rule:,} "
          f"(0이어야 정상 — 0이 아니면 복합 이상이 룰 임계값을 넘고 있다)")

    # ── ML 알림 중 실제로 정답이었던 비율 ─────────────────────────────
    ml_on_truth = len(ml_hits & set(truth))
    ml_on_normal = len(ml_hits) - ml_on_truth
    print(f"[ML] 알림 중 정답 적중     : {ml_on_truth:,}/{len(ml_hits):,} "
          f"({pct(ml_on_truth, len(ml_hits))})")
    print(f"     └ 정상 메시지에 뜬 알림: {ml_on_normal:,} "
          f"({pct(ml_on_normal, len(ml_hits))})")

    # ── 어떤 복합 패턴을 잘 잡고 못 잡는지 ────────────────────────────
    per_label_total = Counter(v["label"] for v in truth.values() if v["kind"] == "composite")
    per_label_hit = Counter(
        truth[k]["label"] for k in (gt_composite & ml_hits)
    )
    if per_label_total:
        print("-" * 62)
        print("복합 패턴별 ML recall")
        for label, total in sorted(per_label_total.items()):
            hit = per_label_hit.get(label, 0)
            print(f"  {label:<22} {hit:>6,}/{total:<6,} ({pct(hit, total)})")
    print("=" * 62)

    if args.sweep:
        print_sweep(
            load_scores(
                [c.strip() for c in args.detector_containers.split(",") if c.strip()],
                [f.strip() for f in args.score_files.split(",") if f.strip()],
            ),
            truth,
            args.sweep_steps,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
