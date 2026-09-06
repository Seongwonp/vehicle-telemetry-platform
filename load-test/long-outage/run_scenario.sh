#!/bin/bash
# 장기 의존성 장애에서 retry / rebalance / DLQ 경계 측정 (docs/roadmap.md P1-3).
#
# ## 왜 이 실험이 따로 필요한가
#
# 지금까지의 장애 실험은 전부 90~300초였고 전부 "유실 0"으로 끝났다. 그래서
# `telemetry.kafka.retry.budget-ms`(180초)가 실제로 소진되는 모습을 한 번도 못 봤다.
# 게다가 2026-09-05에 확인한 것처럼 이 예산은 **벽시계가 아니라 백오프로 쉰 시간의
# 합**이라, 의존성이 어떻게 실패하느냐에 따라 실효 내성이 달라진다.
#
#   influxdb : 컨테이너가 없으면 즉시 연결 거부된다. 실패에 시간이 안 드니
#              백오프 합이 곧 벽시계에 가깝고, 예산 180초가 약 3분 만에 찬다.
#   postgres : HikariCP connectionTimeout 30초를 시도마다 기다린다. 그 시간은
#              예산에 안 세므로 실효 내성이 약 8분까지 늘어난다(추정, 미측정).
#
# 즉 **같은 180초 설정이 의존성에 따라 3분과 8분이 된다**는 게 지금까지의 서술이고,
# 8분 너머는 아무도 안 봤다. 이 스크립트는 그 너머를 본다.
#
# ## 무엇을 재나 (하나의 질문)
#
# **장애가 재시도 예산을 넘겨 계속되면 무엇이 먼저 무너지는가.**
#
#   1. 재시도가 소진되면 레코드는 DLQ로 간다 — 언제부터, 얼마나?
#   2. 재시도하는 동안 리스너 스레드가 poll을 못 하면 max.poll.interval.ms(300초)를
#      넘겨 컨슈머가 그룹에서 쫓겨난다. 정적 멤버십(group.instance.id)이 이걸
#      막아준다는 것은 12시간 soak 사고 때의 추정이지 측정이 아니다.
#   3. 복구 후 얼마나 걸려 lag이 0으로 돌아오는가.
#   4. 최종 정합성 — DLQ에 격리된 것까지 세면 유실이 0인가.
#
# 30초마다 timeline.csv에 한 줄씩 남긴다. **이 실험의 결과물은 최종 수치가 아니라
# 그 시계열이다** — 질문이 "언제 무너졌나"이기 때문이다.
#
# 사용법: bash run_scenario.sh <influxdb|postgres> [장애 지속 초]
set -euo pipefail
cd "$(dirname "$0")/../.."

SCENARIO="${1:?시나리오를 지정해라: influxdb | postgres}"
OUTAGE_SEC="${2:-720}"
VEHICLES="${VEHICLES:-50}"
SAMPLE_SEC="${SAMPLE_SEC:-30}"
# 복구 후에도 부하를 얼마나 더 흘릴지. 드레인과 유입이 겹치는 구간을 봐야
# "복구됐다"가 정지 상태에서만 성립하는 이야기가 아니게 된다.
POST_RESTORE_LOAD_SEC="${POST_RESTORE_LOAD_SEC:-180}"

case "$SCENARIO" in
  influxdb) CONTAINER="telemetry-influxdb"; GROUP="telemetry-storage-group"
            SRC_TOPIC="vehicle-telemetry";      DLQ_TOPIC="vehicle-telemetry-dlq" ;;
  postgres) CONTAINER="telemetry-postgres"; GROUP="anomaly-storage-group"
            SRC_TOPIC="vehicle-anomaly-alerts"; DLQ_TOPIC="vehicle-anomaly-alerts-dlq" ;;
  *) echo "influxdb 또는 postgres만 된다"; exit 1 ;;
esac

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/long-outage/_result_${SCENARIO}.txt"

# 원본 증거 보존(docs/evidence-policy.md P0-1).
# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "long-outage" "bash load-test/long-outage/run_scenario.sh $SCENARIO $OUTAGE_SEC"
evidence_input scenario "$SCENARIO"
evidence_input dependency "$CONTAINER"
evidence_input consumer_group "$GROUP"
evidence_input vehicles "$VEHICLES"
evidence_input publish_interval_sec 0.2
evidence_input anomaly_rate 0.3
evidence_input outage_sec "$OUTAGE_SEC"
evidence_input sample_interval_sec "$SAMPLE_SEC"
evidence_input post_restore_load_sec "$POST_RESTORE_LOAD_SEC"
evidence_input retry_budget_ms 180000
evidence_input max_poll_interval_ms 300000

TOK=$(grep '^INFLUXDB_TOKEN=' .env | cut -d= -f2-)
ORG=$(grep '^INFLUXDB_ORG=' .env | cut -d= -f2-)
BKT=$(grep '^INFLUXDB_BUCKET=' .env | cut -d= -f2-)
PG_DB=$(grep '^POSTGRES_DB=' .env | cut -d= -f2-)
PG_USER=$(grep '^POSTGRES_USER=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }

metric() {
  curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
    | awk -v n="$1" '$1 ~ "^"n"([{]|$)" {gsub(/.* /,""); s+=$0} END{printf "%d", s+0}'
}
influx_rows() {
  docker exec telemetry-influxdb influx query \
    "from(bucket: \"$BKT\") |> range(start: -3h) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r._field == \"speed\") |> group() |> count()" \
    --org "$ORG" --token "$TOK" --raw 2>/dev/null \
    | tr -d '\r' | awk -F, '$0 ~ /^,/ && $NF ~ /^[0-9]+$/ {v=$NF} END{print v+0}'
}
psql_scalar() {
  docker exec telemetry-postgres psql -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null | tr -d '\r' | head -1
}
topic_end_offsets() {
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'
}

# 리밸런싱은 **그룹별로** 세야 한다. 백엔드 한 프로세스에 컨슈머가 둘이라 전체를
# 세면 다른 경로의 리밸런싱이 섞인다. 문구는 Java 클라이언트 기준이다
# (kafka-python 문구로 찾으면 0이 나온다 — 리밸런싱 측정에서 한 번 겪었다).
rebalance_count() {
  docker logs telemetry-backend 2>&1 \
    | grep -ciE "$GROUP.*(Revoke previously assigned|Attempt to heartbeat failed|leaving the group|poll timeout|sending LeaveGroup|already rebalancing)" || true
}
# 재시도 예산이 실제로 소진된 순간. 이게 처음 찍히는 시각이 이 실험의 핵심 값이다.
backoff_exhausted_count() {
  docker logs telemetry-backend 2>&1 | grep -ciE "Backoff .* exhausted|exhausted for" || true
}

# ── 시계열 한 줄 ────────────────────────────────────────────────
# Kafka 조회를 **한 번의 docker exec으로 묶는다.** 따로 부르면 회당 3~5초씩 들어
# 30초 간격 안에 안 들어오고, 그러면 시계열의 t가 실제 시각과 어긋난다.
TIMELINE="$EVIDENCE_DIR/timeline.csv"
echo "t_sec,phase,group_state,members,lag,dlq_offset,src_offset,influx_write_failures,dlq_published,rebalance_log,backoff_exhausted" > "$TIMELINE"

LAST_LAG=0; LAST_DLQ=0; LAST_STATE="?"; LAST_MEMBERS="?"

sample() {  # $1 = t_sec, $2 = phase
  local raw state members lag dlq src line
  raw=$(docker exec telemetry-kafka bash -c "
    kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group $GROUP --state 2>/dev/null
    echo @@S@@
    kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group $GROUP 2>/dev/null
    echo @@S@@
    kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic $DLQ_TOPIC 2>/dev/null
    echo @@S@@
    kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic $SRC_TOPIC 2>/dev/null
  " 2>/dev/null | tr -d '\r' || true)

  state=$(echo "$raw"   | awk '/@@S@@/{n++; next} n==0 && NR>1 && NF>=5 {print $(NF-1)}' | tail -1)
  members=$(echo "$raw" | awk '/@@S@@/{n++; next} n==0 && NR>1 && NF>=5 {print $NF}'     | tail -1)
  lag=$(echo "$raw"     | awk '/@@S@@/{n++; next} n==1 && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}')
  dlq=$(echo "$raw"     | awk -F: '/@@S@@/{n++; next} n==2 && NF==3 {s+=$3} END{print s+0}')
  src=$(echo "$raw"     | awk -F: '/@@S@@/{n++; next} n==3 && NF==3 {s+=$3} END{print s+0}')

  line="$1,$2,${state:-?},${members:-?},$lag,$dlq,$src,$(metric telemetry_influx_write_failures_total),$(metric telemetry_kafka_dlq_published_total),$(rebalance_count),$(backoff_exhausted_count)"
  echo "$line" >> "$TIMELINE"
  log "  ts $line"
  LAST_LAG="$lag"; LAST_DLQ="$dlq"; LAST_STATE="${state:-?}"; LAST_MEMBERS="${members:-?}"
}

sample_for() {  # $1 = 총 초, $2 = phase
  local total="$1" phase="$2" t0 now
  t0=$(date +%s)
  while :; do
    now=$(( $(date +%s) - t0 ))
    [ "$now" -ge "$total" ] && break
    sample "$(( $(date +%s) - OUTAGE_T0 ))" "$phase"
    sleep "$SAMPLE_SEC"
  done
}

: > "$OUT"
OUTAGE_T0=$(date +%s)
log "=== P1-3 장기 장애: $SCENARIO / $CONTAINER (${OUTAGE_SEC}초) ==="
log "그룹 $GROUP, 원본 $SRC_TOPIC, DLQ $DLQ_TOPIC"

# ── 1. 깨끗한 스택 ─────────────────────────────────────────────
$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis >/dev/null 2>&1 || true
wait_until 300 "PostgreSQL healthy" bash -c '[ "$(docker inspect telemetry-postgres --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
$COMPOSE up -d backend anomaly-detector >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "스택 기동 완료"

# ── 2. 부하 ────────────────────────────────────────────────────
# ANOMALY_RATE를 올려야 postgres 경로(이상 알림)에 의미 있는 유량이 생긴다.
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
$COMPOSE run -d --name telemetry-sim-0 \
  -e VEHICLE_COUNT=$VEHICLES -e PUBLISH_INTERVAL=0.2 -e ANOMALY_RATE=0.3 simulator >/dev/null 2>&1
log "시뮬레이터 기동 (${VEHICLES}대, 0.2초, 이상률 0.3) — 90초 정상 구간"
wait_sec 90

OUTAGE_T0=$(date +%s)
sample 0 warmup
log "정상 기준선: lag=$LAST_LAG dlq=$LAST_DLQ state=$LAST_STATE members=$LAST_MEMBERS"
DLQ_BEFORE="$LAST_DLQ"

# ── 3. 장애 ────────────────────────────────────────────────────
log "--- 장애 주입: $CONTAINER 정지 (${OUTAGE_SEC}초) ---"
docker stop "$CONTAINER" >/dev/null
OUTAGE_T0=$(date +%s)
sample_for "$OUTAGE_SEC" outage

# ── 4. 복구 ────────────────────────────────────────────────────
RESTORE_T=$(( $(date +%s) - OUTAGE_T0 ))
log "--- 복구: $CONTAINER 재기동 (t=${RESTORE_T}s) ---"
docker start "$CONTAINER" >/dev/null
wait_until 300 "$CONTAINER healthy" bash -c "[ \"\$(docker inspect $CONTAINER --format '{{.State.Health.Status}}' 2>/dev/null)\" = healthy ]"
log "의존성 healthy — 부하를 ${POST_RESTORE_LOAD_SEC}초 더 흘리며 관찰"
sample_for "$POST_RESTORE_LOAD_SEC" recovering

# ── 5. 부하 정지 후 드레인 ──────────────────────────────────────
# SIGKILL이 아니라 SIGTERM으로 세운다(fault-injection 스크립트와 같은 이유).
log "부하 정지 (SIGTERM) — 최종 집계 대기"
docker stop -t 90 telemetry-sim-0 >/dev/null 2>&1 || true
for _ in $(seq 1 30); do
  docker logs telemetry-sim-0 2>&1 | grep -q "전체 시뮬레이터 종료 완료" && break
  sleep 2
done
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
SIM_STOP_T=$(( $(date +%s) - OUTAGE_T0 ))

DRAIN_DONE_T=""
for _ in $(seq 1 120); do
  sample "$(( $(date +%s) - OUTAGE_T0 ))" draining
  if [ "$LAST_LAG" = "0" ]; then DRAIN_DONE_T=$(( $(date +%s) - OUTAGE_T0 )); break; fi
  sleep "$SAMPLE_SEC"
done
log "드레인 완료 t=${DRAIN_DONE_T:-미도달}s (lag=$LAST_LAG)"

# ── 6. 집계 ────────────────────────────────────────────────────
SRC_OFFSET=$(topic_end_offsets "$SRC_TOPIC")
DLQ_AFTER=$(topic_end_offsets "$DLQ_TOPIC")
DLQ_GROWTH=$(( DLQ_AFTER - DLQ_BEFORE ))
STORED_DISTINCT=""
if [ "$SCENARIO" = "influxdb" ]; then
  STORED=$(influx_rows); STORE_LABEL="InfluxDB 행"
else
  STORED=$(psql_scalar "SELECT count(*) FROM anomaly_alerts;"); STORE_LABEL="PostgreSQL 행"
  STORED_DISTINCT=$(psql_scalar "SELECT count(DISTINCT event_id) FROM anomaly_alerts;")
fi
REBALANCE=$(rebalance_count)
EXHAUSTED=$(backoff_exhausted_count)

# 재시도 예산이 언제 소진되기 시작했는지 — 시계열에서 backoff_exhausted가 처음
# 0이 아니게 된 t. 이 실험의 핵심 값이라 따로 뽑아 남긴다.
FIRST_EXHAUST_T=$(awk -F, 'NR>1 && $11+0 > 0 {print $1; exit}' "$TIMELINE")
FIRST_DLQ_T=$(awk -F, -v b="$DLQ_BEFORE" 'NR>1 && $6+0 > b+0 {print $1; exit}' "$TIMELINE")

evidence_capture_prometheus final
evidence_capture_kafka_groups "$GROUP" telemetry-storage-group anomaly-storage-group
evidence_capture_topic_offsets "$SRC_TOPIC" "$DLQ_TOPIC"
evidence_capture_log_lines telemetry-backend \
  "Revoke previously assigned|Attempt to heartbeat|leaving the group|poll timeout|exhausted|Rebalanc|재시도" backend-key-lines.txt

evidence_count dlq_before "$DLQ_BEFORE"
evidence_count dlq_after "$DLQ_AFTER"
evidence_count dlq_growth "$DLQ_GROWTH"
evidence_count src_topic_end_offset "$SRC_OFFSET"
evidence_count stored_rows "$STORED"
[ -n "$STORED_DISTINCT" ] && evidence_count stored_distinct_event_id "$STORED_DISTINCT"
evidence_count rebalance_log_lines "$REBALANCE"
evidence_count backoff_exhausted_log_lines "$EXHAUSTED"
evidence_count first_exhaust_t_sec "${FIRST_EXHAUST_T:-none}"
evidence_count first_dlq_growth_t_sec "${FIRST_DLQ_T:-none}"
evidence_count restore_t_sec "$RESTORE_T"
evidence_count sim_stop_t_sec "$SIM_STOP_T"
evidence_count drain_done_t_sec "${DRAIN_DONE_T:-none}"

log ""
log "=== 결과 ($SCENARIO, 장애 ${OUTAGE_SEC}초) ==="
log "원본 토픽 $SRC_TOPIC : $SRC_OFFSET"
log "$STORE_LABEL : $STORED${STORED_DISTINCT:+  (고유 event_id $STORED_DISTINCT)}"
log "DLQ $DLQ_TOPIC : $DLQ_BEFORE → $DLQ_AFTER  (증가 $DLQ_GROWTH)"
log "재시도 예산 소진 로그 : $EXHAUSTED  (처음 관측 t=${FIRST_EXHAUST_T:-없음}s)"
log "DLQ 증가 시작 : t=${FIRST_DLQ_T:-없음}s"
log "리밸런싱 로그($GROUP) : $REBALANCE"
log "복구 t=${RESTORE_T}s / 부하정지 t=${SIM_STOP_T}s / lag 0 도달 t=${DRAIN_DONE_T:-미도달}s"
log "시계열: $TIMELINE"

# 성공 기준을 실행 전에 정하고 판정까지 남긴다(docs/issue-guidelines.md).
CRIT="원본 토픽 == 저장 + DLQ 증가분 (유실 0) AND 리밸런싱 0"
LOSS=$(( SRC_OFFSET - STORED - DLQ_GROWTH ))
if [ "$LOSS" -le 0 ] && [ "$REBALANCE" -eq 0 ]; then
  VERDICT="PASS (차이 $LOSS, 리밸런싱 0)"
else
  VERDICT="관찰 (차이 $LOSS, 리밸런싱 $REBALANCE)"
fi
log "판정: $CRIT → $VERDICT"

evidence_capture_file "$OUT" console.log
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
