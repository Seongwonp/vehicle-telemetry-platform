#!/bin/bash
# 반복 실행의 회차별 결과를 표로 모은다 (docs/roadmap.md P0-2, docs/evidence-policy.md).
#
# **평균을 내지 않는다.** 회차별 값을 그대로 늘어놓고 최소·최대만 덧붙인다.
# 평균은 "3회 중 1회가 크게 튀었다"를 지워버려서, 안정성 판단에 쓸 수 없다.
#
# 사용법: bash load-test/lib/summarize.sh <scenario-dir> [시작마크] [실패횟수]
#   시작마크 이후에 만들어진 evidence/<run-id>/만 집계한다(같은 시나리오의 옛
#   실행이 섞이지 않도록). 생략하면 최근 실행 전부를 본다.
set -uo pipefail
cd "$(dirname "$0")/../.."

SCENARIO="${1:?시나리오 디렉터리 이름}"
SINCE="${2:-}"
FAILED="${3:-0}"
BASE="load-test/$SCENARIO/evidence"
OUT="load-test/$SCENARIO/REPEAT_$(date +%Y%m%d_%H%M%S).md"

[ -d "$BASE" ] || { echo "[summarize] $BASE 가 없다."; exit 1; }

# 대상 실행 디렉터리 목록 (run-id가 곧 시각이라 이름순 = 시간순)
RUNS=()
for d in "$BASE"/*/; do
  [ -f "$d/counts.csv" ] || continue
  id="$(basename "$d")"
  if [ -n "$SINCE" ] && [ "$id" \< "$SINCE" ]; then continue; fi
  RUNS+=("$d")
done

[ "${#RUNS[@]}" -gt 0 ] || { echo "[summarize] 집계할 실행이 없다."; exit 1; }

# 지표 키의 합집합 (회차마다 키가 다를 수 있다)
KEYS="$(for d in "${RUNS[@]}"; do awk -F, 'NR>1{print $1}' "$d/counts.csv"; done | sort -u)"

{
  echo "# $SCENARIO 반복 실행 요약"
  echo
  echo "| 항목 | 값 |"
  echo "| --- | --- |"
  echo "| 생성 시각 | $(date -Iseconds) |"
  echo "| 반복 횟수 | ${#RUNS[@]} |"
  echo "| 실패 횟수 | $FAILED |"
  echo "| **검증 상태** | $([ "${#RUNS[@]}" -ge 3 ] && echo '**검증 완료(반복 기준)** — 3회 이상' || echo '**부분 검증** — 3회 미만') |"
  echo
  echo "> 평균을 내지 않는다. 회차별 값을 그대로 두고 최소·최대만 덧붙인다 —"
  echo "> 평균은 \"3회 중 1회가 크게 튀었다\"를 지워버린다."
  echo
  echo "## 입력 조건"
  echo
  echo '```'
  cat "${RUNS[0]}/inputs.csv"
  echo '```'
  echo
  echo "## 회차별 결과"
  echo

  # 헤더
  printf "| 지표 |"
  n=1
  for _ in "${RUNS[@]}"; do printf " %d회 |" "$n"; n=$((n+1)); done
  printf " 최소 | 최대 | 편차 |\n"
  printf "| --- |"
  for _ in "${RUNS[@]}"; do printf " ---: |"; done
  printf " ---: | ---: | ---: |\n"

  # 지표별 행
  while IFS= read -r key; do
    [ -n "$key" ] || continue
    printf "| \`%s\` |" "$key"
    vals=""
    for d in "${RUNS[@]}"; do
      v="$(awk -F, -v k="$key" '$1==k{print $2}' "$d/counts.csv" | tail -1)"
      v="${v:--}"
      printf " %s |" "$v"
      [ "$v" = "-" ] || vals="$vals $v"
    done
    if [ -n "$vals" ]; then
      mn="$(echo $vals | tr ' ' '\n' | sort -n | head -1)"
      mx="$(echo $vals | tr ' ' '\n' | sort -n | tail -1)"
      printf " %s | %s | %s |\n" "$mn" "$mx" "$((mx - mn))"
    else
      printf " - | - | - |\n"
    fi
  done <<< "$KEYS"

  echo
  echo "## 회차별 판정"
  echo
  echo "| 회차 | run-id | git commit (dirty) | 판정 |"
  echo "| ---: | --- | --- | --- |"
  n=1
  for d in "${RUNS[@]}"; do
    id="$(basename "$d")"
    sha="$(awk -F': *' '/^git_commit/{print substr($2,1,7)}' "$d/metadata.txt")"
    dirty="$(awk -F': *' '/^git_dirty/{print $2}' "$d/metadata.txt")"
    verdict="$(awk -F': *' '/^verdict/{print $2}' "$d/metadata.txt")"
    printf "| %d | \`%s\` | %s (%s) | %s |\n" "$n" "$id" "${sha:-?}" "${dirty:-?}" "${verdict:-미기재}"
    n=$((n+1))
  done

  echo
  echo "## 원본"
  echo
  for d in "${RUNS[@]}"; do echo "- \`$d\`"; done
  echo
  echo "각 회차의 \`counts.csv\`에서 위 표의 모든 수치를 다시 계산할 수 있다."
} > "$OUT"

echo "[summarize] 작성됨: $OUT"
cat "$OUT"
