#!/bin/bash
# PostgreSQL 장애 → 이상 알림 DLQ 격리 → 재처리 → **중복 알림 검증**.
#
# CLAUDE.md 우선순위 2번의 남은 갈래다. DLQ 재처리 도구와 Runbook을 만들면서
# `vehicle-anomaly-alerts-dlq` 재처리가 PostgreSQL에 알림을 중복으로 넣는지는
# 재보지 않았다. 텔레메트리 쪽은 InfluxDB가 같은 키를 덮어써서 멱등이 확인됐지만,
# 알림은 INSERT라 같은 보장이 없다 — 운영자에게 "이 명령으로 재처리하세요"라고
# 문서에 써둔 경로라서, 여기서 중복이 생기면 문서가 사고를 유도하는 셈이다.
#
# 정답 기준은 토픽의 **고유 event_id 수**다(count_events.py 주석 참고).
#
# 재처리를 두 번 돌린다. 두 번째는 일부러 **다른 커서 그룹**으로 돌려서 같은
# 레코드를 다시 읽게 만든다 — 운영자가 "아까 재처리가 먹었나?" 하고 한 번 더
# 실행하는 상황이고, 이때 안전하지 않으면 Runbook이 틀린 것이다.
#
# 사용법: bash run_scenario.sh [장애 지속 초]
set -euo pipefail
cd "$(dirname "$0")/../.."

OUTAGE_SEC="${1:-60}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/anomaly-dlq-idempotency/_result.txt"
NET="vehicle-telemetry-platform_telemetry-net"
IMG="vehicle-telemetry-platform-anomaly-detector"

PG_DB=$(grep '^POSTGRES_DB=' .env | cut -d= -f2-)
PG_USER=$(grep '^POSTGRES_USER=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }

psql_scalar() {  # $1 = SQL
  docker exec telemetry-postgres psql -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null | tr -d '\r' | head -1
}
rows()        { psql_scalar "SELECT count(*) FROM anomaly_alerts;"; }
distinct_ids(){ psql_scalar "SELECT count(DISTINCT event_id) FROM anomaly_alerts;"; }
topic_end_offsets() {
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'
}
alert_lag() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group anomaly-storage-group 2>/dev/null | awk 'NR>1{s+=$6} END{print s+0}'
}
drain() {  # 알림 컨슈머가 다 따라잡을 때까지
  # 넉넉하게 잡는다. PostgreSQL 장애 뒤에는 이 컨슈머가 아주 느리게 따라잡는다 —
  # 실측에서 200초로는 모자라 lag 3,693이 남은 채 측정이 오염됐다(그때 행 수와
  # 고유 event_id 수를 몇 초 간격으로 재서 13,102 < 13,134라는 불가능한 값이 나왔다).
  for _ in $(seq 1 240); do [ "$(alert_lag)" = "0" ] && break; sleep 5; done
  sleep 5
}
# Git Bash는 컨테이너 경로 인자(`-w /w`)를 `W:/`로 바꿔버린다. MSYS_NO_PATHCONV로 끄고,
# 볼륨 소스는 `pwd -W`(D:/... 형식)를 준다 — `/d/...`는 Docker Desktop이 못 받는다.
WINPWD=$(pwd -W 2>/dev/null || pwd)
in_tools() {  # $1 = 마운트할 호스트 하위 경로, 나머지 = 컨테이너에서 실행할 명령
  local dir="$1"; shift
  MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
    -v "$WINPWD/$dir:/w" -w /w "$IMG" "$@"
}
dlq_tool()     { in_tools dlq-tools python dlq.py "$@"; }
ground_truth() { in_tools load-test/anomaly-dlq-idempotency \
                   python count_events.py --topic vehicle-anomaly-alerts; }
dlq_event_ids(){ in_tools load-test/anomaly-dlq-idempotency \
                   python dlq_event_ids.py --topic vehicle-anomaly-alerts-dlq; }

# DLQ에 들어간 이벤트 중 **이미 저장돼 있는** 것이 몇 건인지.
# 이게 이 측정의 핵심이다 — 0이면 재처리가 중복을 만들 기회조차 없어서
# "중복이 안 났다"가 아무것도 증명하지 못한다.
dlq_already_stored() {
  local ids
  ids=$(dlq_event_ids 2>/dev/null | grep -E '^[a-f0-9]{64}$' | sed "s/^/'/;s/$/'/" | paste -sd,)
  [ -z "$ids" ] && { echo 0; return; }
  psql_scalar "SELECT count(*) FROM anomaly_alerts WHERE event_id IN ($ids);"
}

# 재처리로 "중복 저장"이 일어나면 행이 늘고, "중복 알림(WebSocket)"만 일어나면
# 행은 그대로인데 저장 로그만 늘어난다. 둘은 다른 문제라 따로 센다.
save_logs() { docker logs telemetry-backend 2>&1 | grep -c '\[이상 저장\]' || true; }
dup_logs()  { docker logs telemetry-backend 2>&1 | grep -c '\[이상 중복\]' || true; }

: > "$OUT"
log "=== 이상 알림 DLQ 재처리 멱등성 (PostgreSQL 장애 ${OUTAGE_SEC}초) ==="

# ── 1. 깨끗한 스택 ─────────────────────────────────────────────
$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis >/dev/null 2>&1 || true
until [ "$(docker inspect telemetry-postgres --format '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do sleep 10; done
$COMPOSE up -d backend anomaly-detector >/dev/null 2>&1
until curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/actuator/health 2>/dev/null | grep -q 200; do sleep 5; done
log "스택 기동 완료"

# ── 2. 부하 — 이상이 충분히 나오도록 ANOMALY_RATE를 올린다 ─────
$COMPOSE run -d --rm --name telemetry-sim-0 \
  -e VEHICLE_COUNT=50 -e PUBLISH_INTERVAL=0.2 -e ANOMALY_RATE=0.3 simulator >/dev/null 2>&1
log "시뮬레이터 기동 (50대/0.2초, 이상률 0.3) — 60초 정상 구간"
wait_sec 60
log "정상 구간 종료: 행=$(rows)"

# ── 3. 장애 주입 ───────────────────────────────────────────────
log "--- 장애 주입: telemetry-postgres 정지 ---"
docker stop telemetry-postgres >/dev/null
wait_sec "$OUTAGE_SEC"
log "장애 중 DLQ 토픽=$(topic_end_offsets vehicle-anomaly-alerts-dlq)"

# ── 4. 복구 ────────────────────────────────────────────────────
log "--- 복구: telemetry-postgres 재기동 ---"
docker start telemetry-postgres >/dev/null
until [ "$(docker inspect telemetry-postgres --format '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do sleep 5; done
log "PostgreSQL 복구 확인 — 30초 더"
wait_sec 30

# ── 5. 부하 정지 후 드레인 ──────────────────────────────────────
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
log "부하 정지 — 드레인 대기"
wait_sec 20
drain
log "드레인 완료 (알림 lag=$(alert_lag))"

TOPIC=$(topic_end_offsets vehicle-anomaly-alerts)
DLQ=$(topic_end_offsets vehicle-anomaly-alerts-dlq)
ROWS_BEFORE=$(rows)
LOGS_BEFORE=$(save_logs)
DUPS_BEFORE=$(dup_logs)
ALREADY=$(dlq_already_stored)
log ""
log "--- 재처리 전 ---"
log "Kafka vehicle-alerts     : $TOPIC   (DLQ $DLQ)"
log "PostgreSQL 행            : $ROWS_BEFORE  (고유 event_id $(distinct_ids))"
log "DLQ $DLQ건 중 이미 저장된 것: $ALREADY  ← 재처리가 중복 INSERT를 시도하는 건수"
log "  (0이면 중복이 날 기회가 없어 이 측정은 아무것도 증명하지 못한다)"
log "정답 기준 산출 중…"
ground_truth | tee -a "$OUT"

# ── 6. 재처리 1회차 ────────────────────────────────────────────
log ""
log "--- 재처리 1회차 (분류 확인) ---"
INSPECT=$(dlq_tool --topic vehicle-anomaly-alerts-dlq inspect 2>&1)
echo "$INSPECT" | tee -a "$OUT"

# 분류가 transient로 안 나오면 그것 자체가 발견이다 — dlq.py의 TRANSIENT_MARKERS가
# PostgreSQL 장애 예외를 모른다는 뜻이고, Runbook대로 하면 재처리 대상이 0건으로
# 나와 운영자가 막힌다. 측정은 계속해야 하므로 --include-unknown으로 넘기되 기록한다.
TRANSIENT=$(echo "$INSPECT" | awk '/^  transient/{gsub(/,/,"",$2); print $2; exit}')
REPLAY_EXTRA=""
if [ "${TRANSIENT:-0}" = "0" ]; then
  log "** transient 0건 — 자동 재처리 대상이 없다. --include-unknown으로 진행한다."
  REPLAY_EXTRA="--include-unknown"
fi

dlq_tool --topic vehicle-anomaly-alerts-dlq replay \
  --target vehicle-anomaly-alerts --group dlq-idem-run1 $REPLAY_EXTRA --execute 2>&1 | tee -a "$OUT"
drain
ROWS_AFTER1=$(rows)
LOGS_AFTER1=$(save_logs)
log "1회차 후 행=$ROWS_AFTER1 (증가 $((ROWS_AFTER1 - ROWS_BEFORE)))  저장로그 증가 $((LOGS_AFTER1 - LOGS_BEFORE))"

# ── 7. 재처리 2회차 — 같은 레코드를 일부러 다시 ────────────────
# 커서 그룹을 바꾸면 DLQ를 처음부터 다시 읽는다. 운영자가 커서를 모른 채
# 한 번 더 실행하거나, 그룹이 사라진 뒤 재실행하는 상황과 같다.
log ""
log "--- 재처리 2회차 (같은 레코드, 다른 커서 그룹) ---"
dlq_tool --topic vehicle-anomaly-alerts-dlq replay \
  --target vehicle-anomaly-alerts --group dlq-idem-run2 $REPLAY_EXTRA --execute 2>&1 | tee -a "$OUT"
drain
ROWS_AFTER2=$(rows)
LOGS_AFTER2=$(save_logs)
DUPS_AFTER2=$(dup_logs)

# ── 8. 집계 ────────────────────────────────────────────────────
log ""
log "=== 결과 ==="
log "재처리 대상 중 이미 저장돼 있던 것 : $ALREADY / $DLQ"
log "PostgreSQL 행 — 재처리 전 : $ROWS_BEFORE"
log "PostgreSQL 행 — 1회차 후  : $ROWS_AFTER1  (증가 $((ROWS_AFTER1 - ROWS_BEFORE)))"
log "PostgreSQL 행 — 2회차 후  : $ROWS_AFTER2  (증가 $((ROWS_AFTER2 - ROWS_AFTER1)))  ← 0이어야 멱등"
log "행 수 == 고유 event_id 수 : $ROWS_AFTER2 == $(distinct_ids)"
log "저장 로그 증가 (신규)     : $((LOGS_AFTER2 - LOGS_BEFORE))   ← 행 증가와 같아야 한다"
log "중복 로그 증가 (건너뜀)   : $((DUPS_AFTER2 - DUPS_BEFORE))"
log "  두 값을 나눠 세는 이유: 예전에는 중복도 '[이상 저장]'으로 찍혀서, 행은 안 느는데"
log "  로그만 늘었다. 그때는 WebSocket 알림도 함께 다시 나가고 있었다."
log "지표:"
curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
  | grep telemetry_anomaly_stored | tee -a "$OUT"
log "Kafka vehicle-alerts 최종 : $(topic_end_offsets vehicle-anomaly-alerts)"
log "Kafka alerts-dlq 최종     : $(topic_end_offsets vehicle-anomaly-alerts-dlq)"
log ""
log "최종 정답 기준 재산출:"
ground_truth | tee -a "$OUT"
log "DONE"
