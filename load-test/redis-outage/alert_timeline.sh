#!/usr/bin/env bash
# Prometheus TSDB 원본(tsdb_alerts.json·tsdb_counters.json)에서 알림 지연을 단계별로 편다.
#
# 사용: bash load-test/redis-outage/alert_timeline.sh <tsdb_json_dir> <stop_done_ms> [from_s] [to_s]
#   from_s/to_s(epoch 초)는 한 파일에 여러 실행이 들어 있을 때 구간을 자른다.
#
# 출력 시각은 전부 **중지 완료(stop_done) 기준 초**다.
#   카운터: scrape마다 계열(fail-open / 거부)별 합, 시계열 수, 그리고
#           "보이는 증가"(같은 시계열의 직전 샘플보다 큼)와 "새 시계열"(직전 샘플이 없음)을 구분한다.
#           increase()는 새 시계열의 첫 값을 증가로 세지 않는다.
#   알림:   ALERTS 시계열의 pending·firing 첫/마지막 평가 시각. 비활성 전환 평가는 샘플이 남지 않으므로
#           "마지막 firing + 평가 주기(15s)"로 **추정**한다.
set -euo pipefail
DIR="${1:?tsdb json 디렉터리}"; STOP="${2:?stop_done_ms}"; FROM="${3:-0}"; TO="${4:-9999999999}"

flatten() {  # json → "이름|route|epoch|값" 줄
  sed 's/{"metric"/\n{"metric"/g' "$1" | grep '"__name__"\|"alertname"' | while read -r line; do
    name=$(echo "$line" | grep -o '"__name__":"[a-zA-Z_]*"' | sed 's/.*:"//;s/"//')
    route=$(echo "$line" | grep -o '"route":"[^"]*"' | sed 's/.*:"//;s/"//' || true)
    [ -z "$route" ] && route=$(echo "$line" | grep -o '"alertname":"[A-Za-z]*","alertstate":"[a-z]*"' | sed 's/"alertname":"//;s/","alertstate":"/:/;s/"$//' || true)
    echo "$line" | grep -o '\[[0-9.]*,"[0-9.]*"\]' | tr -d '[]"' | awk -F, -v n="$name" -v r="$route" '{print n"|"r"|"$1"|"$2}'
  done
}

echo "## 카운터 scrape (계열, 시각, 합, 시계열 수, 보이는 증가 시계열, 새 시계열)"
flatten "$DIR/tsdb_counters.json" | awk -F'|' -v s="$STOP" -v from="$FROM" -v to="$TO" '
  $3<from || $3>to {next}
  { fam = ($1 ~ /failopen/) ? "failopen" : "unavailable"
    if ($1 ~ /_all_total$/) fam = fam "_all"
    key=$1"|"$2; t=$3; v=$4+0
    rows[fam"|"t] += v; cnt[fam"|"t]++
    if (key in last) { if (v > last[key]) vis[fam"|"t]++ } else { newser[fam"|"t]++ }
    last[key]=v; seen[fam"|"t]=1 }
  END { for (k in seen) { split(k,a,"|"); printf "%s %+7.1f 합=%s 시계열=%d 보이는증가=%d 새시계열=%d\n", a[1], a[2]-s/1000, rows[k], cnt[k], vis[k]+0, newser[k]+0 } }' \
  | sort -k1,1 -k2,2n

echo
echo "## 알림 평가 (ALERTS 원본)"
flatten "$DIR/tsdb_alerts.json" | awk -F'|' -v s="$STOP" -v from="$FROM" -v to="$TO" '
  $3<from || $3>to {next}
  { k=$2; if (!(k in first) || $3<first[k]) first[k]=$3; if ($3>lastt[k]) lastt[k]=$3 }
  END { for (k in first) { split(k,a,":"); printf "%s %s 첫=%+.1f 마지막=%+.1f\n", a[1], a[2], first[k]-s/1000, lastt[k]-s/1000
          if (a[2]=="firing") printf "%s inactive(추정) %+.1f\n", a[1], lastt[k]+15-s/1000 } }' | sort
