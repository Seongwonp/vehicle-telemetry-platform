#!/usr/bin/env bash
# 원본 probes.csv에서 경로별 회복 구간을 계산한다 — 탐침 시각이 아니라 **응답 종료 시각** 기준.
#
# 왜 따로 있나: run_outage.sh의 recovery.csv는 "회복 탐침이 처음 성공한 시각"인데, 회복 탐침은
# 진행 중이던 장애 회차가 끝난 뒤에야 시작된다. 그 사이에 이미 회복했으면 첫 시도가 곧 성공이라
# recovery.csv 값은 **상한일 뿐**이다(2026-09-13 30초 실행에서 +10.7s로 기록됐지만 실제로는
# 장애 회차의 +4.3s 탐침이 이미 성공했다). 또 탐침 **시작** 시각을 쓰면 장애 중 2~4초 걸리는
# 요청이 섞여 경로 간 순서가 뒤바뀌어 보인다.
#
# 출력: path, 마지막 실패 응답 종료(기동 명령 기준 초), 첫 성공 응답 종료(같은 기준), 회복 판정
# 회복 판정: query=200+X-RateLimit-Remaining, diag=503 아님, login=200, refresh=401, health=200
#
# 사용: bash load-test/redis-outage/recovery_window.sh <evidence-dir>
set -euo pipefail
D="${1:?evidence 디렉터리}"
SC=$(awk '/start_cmd_ms/{print $3}' "$D/timeline.txt")
echo "path,last_failure_end_s,first_success_end_s"
awk -F, -v sc="$SC" '
  NR==1 {next}
  $2 < sc-10000 {next}                       # 기동 명령 10초 이전은 볼 필요 없다
  ($1!="outage" && $1!="recovery" && $1!="poll") {next}
  {
    p=$3; end=($2 + $5*1000 - sc)/1000
    ok = (p=="query"   && $4==200 && $7!="-") ||
         (p=="diag"    && $4!=503 && $4!="000") ||
         (p=="login"   && $4==200) ||
         (p=="refresh" && $4==401) ||
         (p=="health"  && $4==200)
    if (p!="query" && p!="diag" && p!="login" && p!="refresh" && p!="health") next
    if (ok) { if (!(p in fs) || end < fs[p]) fs[p]=end }
    else if (!(p in fs) || end < fs[p]) { if (!(p in lf) || end > lf[p]) lf[p]=end }
  }
  END {
    split("query diag login refresh health", ps, " ")
    for (i=1;i<=5;i++){ p=ps[i]
      printf "%s,%s,%s\n", p, (p in lf)?sprintf("%+.1f",lf[p]):"-", (p in fs)?sprintf("%+.1f",fs[p]):"미회복" }
  }' "$D/probes.csv"
