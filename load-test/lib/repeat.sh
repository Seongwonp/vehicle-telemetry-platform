#!/bin/bash
# 같은 실험을 N회 반복한다 (docs/roadmap.md P0-2).
#
# 1회 실행은 "그 조건에서 관찰됨"까지만 말할 수 있다. 재전달량이나 유실량처럼
# 타이밍에 좌우되는 값은 **반복해야 변동 폭이 나온다.** 평균만 내지 않고 회차별
# 값을 그대로 남기는 것이 요점이라, 이 스크립트는 집계를 summarize.sh에 맡긴다.
#
# 사용법:
#   bash load-test/lib/repeat.sh <scenario-dir> <횟수> <실행할 명령...>
# 예:
#   bash load-test/lib/repeat.sh fault-injection 3 \
#     bash load-test/fault-injection/run_scenario.sh mosquitto 90
#
# **`set -e`를 쓰지 않는다.** 한 회차가 실패해도 나머지를 돌려야 한다 —
# 3회 중 1회 실패는 그 자체가 결과이고(정책의 "실패 횟수"), 거기서 멈추면
# 변동 폭을 영영 못 잰다.
set -uo pipefail
cd "$(dirname "$0")/../.."

SCENARIO="${1:?시나리오 디렉터리 이름}"
N="${2:?반복 횟수}"
shift 2

# 반복 시작 시점을 기억해 둔다 — summarize가 "이번 반복분"만 집계하도록.
START_MARK="$(date +%Y%m%d-%H%M%S)"
FAILED=0

echo "=== 반복 실행: $SCENARIO × $N ==="
echo "명령: $*"
echo "시작 마크: $START_MARK"

for i in $(seq 1 "$N"); do
  echo ""
  echo "─── $i/$N 시작 ($(date +%H:%M:%S)) ───"
  if "$@"; then
    echo "─── $i/$N 완료 ($(date +%H:%M:%S)) ───"
  else
    FAILED=$((FAILED + 1))
    echo "─── $i/$N **실패** (계속 진행) ───"
  fi
done

echo ""
echo "=== 반복 종료: 실패 $FAILED/$N ==="
bash load-test/lib/summarize.sh "$SCENARIO" "$START_MARK" "$FAILED"
