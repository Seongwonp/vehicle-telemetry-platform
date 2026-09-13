#!/usr/bin/env bash
# Redis 중단 → 복구 1회 관측 (docs/redis-failure-policy.md §9, §10).
#
# 사용:  bash load-test/redis-outage/run_outage.sh <중단초|blip>
#   예:  bash load-test/redis-outage/run_outage.sh 90
#        bash load-test/redis-outage/run_outage.sh blip   # 실패 요청 1건만 만드는 짧은 장애
#
# v2 (2026-09-13 오후) — v1(같은 날 오전 두 실행)과 다른 점:
#   - **경로별 폴러가 중지 직후부터 회복까지 끊기지 않고 돈다.** v1은 장애 회차가 끝난 뒤에야
#     회복 탐침을 시작해서, 그 사이에 회복한 경로는 상한만 남았다(30초 실행에서 +10.7s).
#   - **Redis 응답 확인을 기동 명령과 동시에 시작한다.** v1은 늦게 시작해 PONG 시각도 상한이었다.
#   - backend의 **Lettuce 재연결 로그**(ConnectionWatchdog)를 시각과 함께 남긴다.
#   - Prometheus TSDB에서 **알림 평가 원본**(ALERTS 시계열)과 **카운터 원본 샘플**을 받아 남긴다 —
#     v1의 alerts.csv는 20초 간격 폴링이라 pending·firing 시각이 거칠었다.
#   - 로컬 수신기(alert_sink.py)가 떠 있으면 **Alertmanager 발송 시각**을 남긴다.
#
# 남기는 것 (load-test/redis-outage/evidence/<run_id>/):
#   probes.csv            요청 1건당 한 줄 — phase,t_ms,path,http,time_s,code,ratelimit_remaining
#   redis_ping.csv        기동 명령 이후 redis-cli ping 결과
#   backend_reconnect.txt Lettuce 재연결 로그(시각 포함, 비밀 없음)
#   tsdb_alerts.json      ALERTS 시계열 원본 샘플
#   tsdb_counters.json    fail-open·거부 카운터 원본 샘플
#   alert_notifications.txt  로컬 수신기가 받은 webhook(있을 때)
#   alerts.csv            Prometheus 알림 상태·Alertmanager 활성 건수 폴링(보조)
#   timeline.txt          중지·기동 명령 시각(epoch ms)
#   summary.txt           위 원본의 집계
#
# 결과가 섞이지 않게 한 것은 v1과 같다 — 실행마다 새 진단 차량 ID, 맞는 비밀번호만 반복,
# 틀린 비밀번호는 기준 구간·장애 중 1회씩, refresh는 없는 토큰, 회복 판정 뒤 그 경로 중단,
# 모든 응답의 X-RateLimit-Remaining·429 기록. 직전 실행의 알림이 남아 있으면 시작하지 않는다.
set -uo pipefail
cd "$(dirname "$0")/../.."
. load-test/lib/evidence.sh

MODE="${1:?중단 초(예: 30, 90) 또는 blip}"
if [ "$MODE" = blip ]; then OUTAGE=0; else OUTAGE="$MODE"; fi
BASE=http://localhost:8080
PROM=http://localhost:9090
AM=http://localhost:9093
REDIS=telemetry-redis
BACKEND=telemetry-backend
SINK=redis-alert-sink-exp
ALERT_WAIT_MAX=${ALERT_WAIT_MAX:-1200}
RECOVERY_MAX=${RECOVERY_MAX:-300}
ALERT_NAMES="RateLimitFailingOpen RedisUnavailableRejections"

set -a; . ./.env; set +a
: "${ADMIN_USERNAME:?}" "${ADMIN_PASSWORD:?}" "${REDIS_PASSWORD:?}"

TMP="$(mktemp -d)"
trap 'docker start "$REDIS" >/dev/null 2>&1; rm -rf "$TMP"' EXIT

now_ms() { date +%s%3N; }
iso_of_ms() { date -u -d "@$(( $1 / 1000 ))" +%Y-%m-%dT%H:%M:%SZ; }

evidence_init redis-outage "bash load-test/redis-outage/run_outage.sh $MODE"
E="$EVIDENCE_DIR"
fail() { echo "[중단] $*" | tee -a "$E/metadata.txt" >&2; exit 1; }

echo "phase,t_ms,path,http,time_s,code,ratelimit_remaining" > "$E/probes.csv"
echo "phase,t_ms,failopen_alert,failopen_am_active,unavail_alert,unavail_am_active" > "$E/alerts.csv"
echo "t_ms,t_end_ms,result" > "$E/redis_ping.csv"
: > "$E/timeline.txt"

DIAG_ID="DG${OUTAGE}-$(date +%H%M%S)"
[ "$MODE" = blip ] && DIAG_ID="DGB-$(date +%H%M%S)"
SINK_UP=no
[ "$(docker inspect -f '{{.State.Running}}' "$SINK" 2>/dev/null)" = true ] && SINK_UP=yes
{
  echo "tool_version     : v2"
  echo "mode             : $MODE"
  echo "outage_seconds   : $OUTAGE"
  echo "diag_vehicle_id  : $DIAG_ID"
  echo "backend_image    : $(docker image inspect vehicle-telemetry-platform-backend:latest --format '{{.Id}}')"
  echo "backend_running  : $(docker inspect "$BACKEND" --format '{{.Image}}')"
  echo "backend_started  : $(docker inspect "$BACKEND" --format '{{.State.StartedAt}}')"
  echo "readiness_include: $(grep -A1 '^        readiness:' backend/src/main/resources/application.yml | tail -1 | sed 's/.*include: *//')"
  echo "alert_rules_sha  : $(sha256sum monitoring/prometheus/alerts.yml | cut -c1-16)"
  echo "alertmanager     : $(docker ps --format '{{.Names}}' | grep -E '^telemetry-alertmanager' | tr '\n' ' ')"
  echo "local_sink       : $SINK_UP (외부 전송 없음)"
} >> "$E/metadata.txt"

PHASE=preflight
probe() {  # $1=경로이름 $2=method $3=url $4=auth(yes/no) $5=body(선택)
  local name=$1 method=$2 url=$3 auth=$4 body=${5:-}
  local h="$TMP/$name.$BASHPID.h" b="$TMP/$name.$BASHPID.b"
  local args=(-s -D "$h" -o "$b" -w '%{http_code} %{time_total}' --max-time 20 -X "$method" "$url")
  [ "$auth" = yes ] && args+=(-H "Authorization: Bearer ${ACCESS:-}")
  [ -n "$body" ] && args+=(-H 'Content-Type: application/json' --data "$body")
  : > "$h"; : > "$b"
  local t out field rl
  t=$(now_ms)
  out=$(curl "${args[@]}" 2>/dev/null) || true
  [ -z "$out" ] && out="000 0"
  P_T=$t; P_HTTP=${out%% *}; P_SECS=${out##* }
  field=$(grep -o '"code" *: *"[A-Z_]*"' "$b" 2>/dev/null | head -1 | sed 's/.*"\([A-Z_]*\)"$/\1/')
  rl=$(grep -i '^X-RateLimit-Remaining:' "$h" 2>/dev/null | tr -d '\r' | awk '{print $2}')
  P_FIELD=${field:--}; P_RL=${rl:--}
  P_BODY="$b"
  echo "$PHASE,$t,$name,$P_HTTP,$P_SECS,$P_FIELD,$P_RL" >> "$E/probes.csv"
}

query()    { probe query        GET  "$BASE/api/vehicles" yes; }
noauth()   { probe query_noauth GET  "$BASE/api/vehicles" no; }
diag()     { probe diag         GET  "$BASE/api/vehicles/$DIAG_ID/diagnosis" yes; }
login()    { probe login        POST "$BASE/api/auth/login" no "{\"username\":\"$ADMIN_USERNAME\",\"password\":\"$ADMIN_PASSWORD\"}"; }
wrongpw()  { probe login_wrongpw POST "$BASE/api/auth/login" no "{\"username\":\"$ADMIN_USERNAME\",\"password\":\"wrong-on-purpose\"}"; }
refresh()  { probe refresh      POST "$BASE/api/auth/refresh" no '{"refreshToken":"probe-nonexistent-token"}'; }
liveness() { probe liveness     GET  "$BASE/actuator/health/liveness" no; }
readiness(){ probe readiness    GET  "$BASE/actuator/health/readiness" no; }
health()   { probe health       GET  "$BASE/actuator/health" no; }

take_tokens() {
  ACCESS=$(grep -o '"accessToken":"[^"]*"' "$P_BODY" | sed 's/.*:"//;s/"$//')
  REFRESH=$(grep -o '"refreshToken":"[^"]*"' "$P_BODY" | sed 's/.*:"//;s/"$//')
}

metrics() {
  curl -s --max-time 20 "$BASE/actuator/prometheus" \
    | grep -E '^telemetry_(ratelimit_failopen|redis_unavailable)' > "$E/metrics_$1.txt" || true
}

alert_state() {
  local s
  s=$(curl -s --max-time 10 "$PROM/api/v1/alerts" | sed 's/{"labels"/\n{"labels"/g' \
    | grep "\"alertname\":\"$1\"" | grep -o '"state":"[a-z]*"' | head -1 | sed 's/.*:"//;s/"//')
  echo "${s:-inactive}"
}
am_active() {
  local out
  out=$(curl -sf --max-time 10 "$AM/api/v2/alerts?active=true&silenced=false&inhibited=false") || { echo "-"; return; }
  echo "$out" | sed 's/{"annotations"/\n{"annotations"/g' | grep -c "\"alertname\":\"$1\""
}
alert_row() {
  echo "$1,$(now_ms),$(alert_state RateLimitFailingOpen),$(am_active RateLimitFailingOpen),$(alert_state RedisUnavailableRejections),$(am_active RedisUnavailableRejections)" >> "$E/alerts.csv"
}

# ── 사전 확인 ─────────────────────────────────────────────────
[ "$(docker inspect -f '{{.State.Running}}' "$REDIS" 2>/dev/null)" = true ] || fail "Redis가 실행 중이 아니다"
curl -sf --max-time 5 "$BASE/actuator/health/liveness" >/dev/null || fail "backend liveness 실패"
for a in $ALERT_NAMES; do
  st=$(alert_state "$a"); [ "$st" = inactive ] || fail "$a 가 이미 $st — 직전 실행의 영향이 남아 있다"
  amc=$(am_active "$a"); [ "$amc" = 0 ] || [ "$amc" = - ] || fail "Alertmanager에 $a 가 아직 활성 — 직전 실행의 영향이 남아 있다"
done
SINK_SINCE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# ── 기준 구간 ─────────────────────────────────────────────────
PHASE=baseline
alert_row baseline
login; take_tokens
[ -n "${ACCESS:-}" ] || fail "기준 로그인 실패 (http=$P_HTTP)"
query; noauth; diag; refresh; liveness; readiness; health
wrongpw
login; take_tokens
metrics baseline

# ── 중단 ──────────────────────────────────────────────────────
STOP_CMD=$(now_ms)
echo "stop_cmd_ms      : $STOP_CMD" >> "$E/timeline.txt"
docker stop "$REDIS" >/dev/null
STOP_DONE=$(now_ms)
echo "stop_done_ms     : $STOP_DONE" >> "$E/timeline.txt"

start_redis() {
  local s d
  s=$(now_ms); echo "start_cmd_ms     : $s" >> "$E/timeline.txt"
  # 응답 확인을 기동 명령과 **동시에** 시작한다(v1은 늦게 시작해 상한만 남았다).
  (
    local deadline=$(( $(now_ms) + RECOVERY_MAX * 1000 )) t r e
    while [ "$(now_ms)" -lt "$deadline" ]; do
      # 시작·종료를 둘 다 남긴다. 시작 시각만 쓰면 docker exec가 기동 완료를 기다린 시간이 빠져
      # "PONG +0.1s" 같은 하한만 남는다(2026-09-13 30초 반복 1회차).
      t=$(now_ms)
      r=$(docker exec "$REDIS" redis-cli --no-auth-warning -a "$REDIS_PASSWORD" ping 2>/dev/null | tr -d '\r')
      e=$(now_ms)
      echo "$t,$e,${r:-no-response}" >> "$E/redis_ping.csv"
      [ "$r" = PONG ] && break
      sleep 0.3
    done
  ) &
  PING_PID=$!
  docker start "$REDIS" >/dev/null
  d=$(now_ms); echo "start_done_ms    : $d" >> "$E/timeline.txt"
  touch "$TMP/started"
}

recovered() {  # $1=경로, 직전 probe 결과로 판정
  case $1 in
    query)   [ "$P_HTTP" = 200 ] && [ "$P_RL" != - ] ;;   # 제한이 다시 걸려야 회복이다
    diag)    [ "$P_HTTP" != 503 ] && [ "$P_HTTP" != 000 ] ;;
    login)   [ "$P_HTTP" = 200 ] ;;
    refresh) [ "$P_HTTP" = 401 ] ;;
    health)  [ "$P_HTTP" = 200 ] ;;
  esac
}

# 경로별 폴러: 중지 직후부터 **회복 판정까지 끊기지 않고** 찌른다.
poller() {  # $1=경로
  local deadline=$(( $(now_ms) + (OUTAGE + RECOVERY_MAX + 30) * 1000 ))
  while [ "$(now_ms)" -lt "$deadline" ]; do
    "$1"
    if [ -f "$TMP/started" ] && recovered "$1"; then return 0; fi
    sleep 1
  done
}

if [ "$MODE" = blip ]; then
  # 짧은 장애: **실패 요청 1건만** 만든다. 폴러를 돌리면 재연결 지연 동안 실패가 쌓여
  # "1건"이 아니게 되므로, 기동 뒤 충분히 기다렸다가 경로별 1회씩만 확인한다.
  PHASE=outage
  login
  start_redis
  wait "$PING_PID"
  sleep 20
  PHASE=recovery
  query; diag; login; refresh; health
else
  PHASE=poll
  for p in query diag login refresh health; do poller "$p" & done
  # 경로 폴러와 겹치지 않는 1회성 확인
  ( sleep 3; PHASE=outage; wrongpw; noauth ) &
  ( while [ ! -f "$TMP/started" ]; do PHASE=outage; liveness; readiness; alert_row outage; sleep 5; done ) &
  sleep "$OUTAGE"
  start_redis
  wait
fi

# ── 회복 후 확인 ──────────────────────────────────────────────
PHASE=post
readiness; liveness
probe refresh_pre_outage_token POST "$BASE/api/auth/refresh" no "{\"refreshToken\":\"${REFRESH:-}\"}"
metrics after_recovery

# ── 알림 해제(와 로컬 수신기의 resolved 수신)까지 관찰 ──────────────
fired_names() {  # 로컬 수신기가 firing으로 받은 알림 이름
  docker logs --since "$SINK_SINCE" "$SINK" 2>/dev/null | grep '"status": "firing"' \
    | grep -o '"name": "[A-Za-z]*"' | sed 's/.*: "//;s/"//' | sort -u
}
resolved_all() {
  local n
  [ "$SINK_UP" = yes ] || return 0
  for n in $(fired_names); do
    docker logs --since "$SINK_SINCE" "$SINK" 2>/dev/null | grep '"status": "resolved"' | grep -q "\"name\": \"$n\"" || return 1
  done
  return 0
}
clear_streak=0
deadline=$(( $(now_ms) + ALERT_WAIT_MAX * 1000 ))
while [ "$(now_ms)" -lt "$deadline" ]; do
  alert_row after_recovery
  row=$(tail -1 "$E/alerts.csv")
  if [ "$(echo "$row" | cut -d, -f3)" = inactive ] && [ "$(echo "$row" | cut -d, -f5)" = inactive ] \
     && [ "$(echo "$row" | cut -d, -f4)" != 1 ] && [ "$(echo "$row" | cut -d, -f6)" != 1 ] && resolved_all; then
    clear_streak=$((clear_streak + 1))
  else
    clear_streak=0
  fi
  [ "$clear_streak" -ge 2 ] && break
  sleep 15
done
END_MS=$(now_ms)

# ── 원본 수집 ─────────────────────────────────────────────────
docker logs -t --since "$(iso_of_ms $(( STOP_CMD - 5000 )))" "$BACKEND" 2>&1 \
  | grep -E 'ConnectionWatchdog|ReconnectionHandler|Reconnected' \
  | awk '{ts=$1; $1=""; sub(/^ [0-9:]+ \[[^]]*\] \[[^]]*\] /,""); print ts" "$0}' > "$E/backend_reconnect.txt" || true
RANGE_S=$(( (END_MS - STOP_CMD) / 1000 + 120 ))
curl -s --max-time 30 --get "$PROM/api/v1/query" --data-urlencode "query=ALERTS{alertname=~\"RateLimitFailingOpen|RedisUnavailableRejections\"}[${RANGE_S}s]" \
  --data-urlencode "time=$(( END_MS / 1000 ))" > "$E/tsdb_alerts.json" || true
curl -s --max-time 30 --get "$PROM/api/v1/query" --data-urlencode "query={__name__=~\"telemetry_(ratelimit_failopen|redis_unavailable).*\"}[${RANGE_S}s]" \
  --data-urlencode "time=$(( END_MS / 1000 ))" > "$E/tsdb_counters.json" || true
if [ "$SINK_UP" = yes ]; then
  docker logs --since "$SINK_SINCE" "$SINK" 2>/dev/null > "$E/alert_notifications.txt" || true
fi

# ── 집계 ──────────────────────────────────────────────────────
START_CMD=$(awk '/start_cmd_ms/{print $3}' "$E/timeline.txt")
{
  echo "# 요약 — Redis 중단 모드=$MODE 1회 (도구 v2)"
  echo "# 원본: probes.csv · redis_ping.csv · backend_reconnect.txt · tsdb_*.json · alert_notifications.txt"
  echo
  echo "## 시각"; cat "$E/timeline.txt"
  echo
  echo "## 429 발생 건수 (0이어야 제한 카운터가 결과를 오염시키지 않은 것)"
  awk -F, 'NR>1&&$4==429' "$E/probes.csv" | wc -l
  echo
  echo "## 장애 중 경로별 응답 (중지 완료 ~ 기동 명령 사이에 **끝난** 요청)"
  awk -F, -v s="$STOP_DONE" -v sc="$START_CMD" 'NR>1{e=$2+$5*1000; if($2>=s && e<sc){k=$3; n[k]++; v=$4"/"$6; if(index(c[k]," "v" ")==0)c[k]=c[k]" "v" ";
       t=$5+0; if(!(k in mn)||t<mn[k])mn[k]=t; if(t>mx[k])mx[k]=t}}
       END{for(k in n)printf "%s,%d,%s,%.3f,%.3f\n",k,n[k],c[k],mn[k],mx[k]}' "$E/probes.csv" | sort
  echo
  echo "## 회복 — 기동 명령 기준 초"
  echo "redis_ping (마지막 실패 응답 종료 / 첫 PONG 응답 종료): $(awk -F, -v sc="$START_CMD" 'NR>1{ if($3!="PONG"){lf=$2} else {printf "%s / %+.1f", (lf? sprintf("%+.1f",(lf-sc)/1000) : "-"), ($2-sc)/1000; exit} }' "$E/redis_ping.csv")"
  echo "docker start 완료: $(awk -v sc="$START_CMD" '/start_done_ms/{printf "%+.1f", ($3-sc)/1000}' "$E/timeline.txt")"
  bash load-test/redis-outage/recovery_window.sh "$E"
  echo
  echo "## 재연결 로그 (기동 명령 이후)"
  awk -v sc="$START_CMD" '{ print }' "$E/backend_reconnect.txt" | tail -n 12
  echo
  echo "## 알림 평가 원본 (ALERTS 시계열: 첫·마지막 샘플, 중지 완료 기준 초)"
  sed 's/{"metric"/\n{"metric"/g' "$E/tsdb_alerts.json" | grep '"alertname"' | while read -r line; do
    name=$(echo "$line" | grep -o '"alertname":"[A-Za-z]*"' | sed 's/.*:"//;s/"//')
    st=$(echo "$line" | grep -o '"alertstate":"[a-z]*"' | sed 's/.*:"//;s/"//')
    # 조회 범위가 직전 실행까지 걸칠 수 있다 — **이 실행의 중지 5초 전 이후 샘플만** 쓴다
    # (2026-09-13 30초 검증에서 직전 짧은 장애의 pending이 −110s로 섞였다).
    ts=$(echo "$line" | grep -o '\[[0-9.]*,"1"\]' | sed 's/\[//;s/,.*//' \
      | awk -v from="$(( STOP_CMD / 1000 - 5 ))" '$1 >= from')
    [ -z "$ts" ] && continue
    first=$(echo "$ts" | head -1); last=$(echo "$ts" | tail -1)
    awk -v n="$name" -v st="$st" -v f="$first" -v l="$last" -v s="$STOP_DONE" \
      'BEGIN{printf "%s %s 첫 %+.0fs 마지막 %+.0fs\n", n, st, f-s/1000, l-s/1000}'
  done
  echo
  echo "## 로컬 수신기가 받은 알림 (중지 완료 기준 초)"
  if [ -s "$E/alert_notifications.txt" ]; then
    grep -o '"received_ms": [0-9]*, "status": "[a-z]*", "alerts": \[{"name": "[A-Za-z]*"' "$E/alert_notifications.txt" \
      | awk -v s="$STOP_DONE" -F'[:,]' '{gsub(/[^0-9]/,"",$2); gsub(/[ "]/,"",$4); gsub(/[ "]/,"",$7); printf "%s %s %+.0fs\n",$7,$4,($2-s)/1000}'
  else
    echo "(수신기 없음 또는 수신 0건)"
  fi
} > "$E/summary.txt"

cat "$E/summary.txt"
evidence_finish "경로별 상태·지연, Redis 응답·재연결·경로 회복 구간, 알림 평가·발송 시각을 1회 관측" "관찰(1회)"
