#!/bin/bash
# 부하가 걸린 상태에서 저장 인스턴스를 내리면 무슨 일이 나는가 (docs/roadmap.md P2).
#
# ## 왜 필요한가
#
# 2026-09-07~08에 스케일 다운이 **약 45초짜리**임을 3회 재서 확인했다. 그런데 그 셋은
# **전부 부하가 없는 상태**였다. Runbook에는 "부하 중에 내리면 그 파티션의 지연이 그만큼
# 튄다"고 적어뒀는데 그건 **추론이지 측정이 아니다.**
#
# 이 실험이 답하려는 것 셋:
#   1. 내려간 인스턴스가 맡던 파티션이 실제로 **소비를 멈추는가**(멈춘다면 얼마나)
#   2. 그동안 남은 인스턴스는 **영향을 받지 않는가**
#   3. 재할당 후 밀린 것을 **따라잡는가**, 그리고 **유실은 0인가**
#
# ## 어떻게 보는가 — 파티션별 lag
#
# 총 lag만 보면 "늘었다"까지밖에 모른다. **파티션별로** 봐야
# "6개는 정지하고 3개는 계속 돈다"가 보인다. 그래서 매 표본마다 파티션별 lag과
# 소유자(CONSUMER-ID)를 같이 남긴다.
#
# 사용법: bash load-test/storage-scale/run_scaledown_under_load.sh [파티션 수] [프로듀서 수]
set -euo pipefail
cd "$(dirname "$0")/../.."

PARTITIONS="${1:-9}"
SHARDS="${2:-2}"
PER_SHARD="${PER_SHARD:-20000000}"
WARMUP_SEC="${WARMUP_SEC:-90}"
OBSERVE_SEC="${OBSERVE_SEC:-180}"   # 스케일 다운 후 관찰 시간

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
SCALE_ENV="-f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/storage-scale/_result_scaledown_load.txt"
TOOLS="vehicle-telemetry-platform-anomaly-detector"
NET="vehicle-telemetry-platform_telemetry-net"
GROUP="telemetry-storage-group"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "storage-scale" "bash load-test/storage-scale/run_scaledown_under_load.sh $PARTITIONS $SHARDS"
evidence_input mode scaledown_under_load
evidence_input partitions "$PARTITIONS"
evidence_input producer_shards "$SHARDS"
evidence_input warmup_sec "$WARMUP_SEC"
evidence_input observe_sec "$OBSERVE_SEC"

TOK=$(grep '^INFLUXDB_TOKEN=' .env | cut -d= -f2-)
ORG=$(grep '^INFLUXDB_ORG=' .env | cut -d= -f2-)
BKT=$(grep '^INFLUXDB_BUCKET=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }
WINPWD=$(pwd -W 2>/dev/null || pwd)

# 파티션별 "partition current_offset lag consumer_id".
#
# **current_offset을 같이 받는 이유**: 정적 멤버십에서는 인스턴스를 내려도 그 파티션의
# 소유자가 "-"가 되지 않는다 — **죽은 멤버가 세션 만료까지 소유권을 유지한다.**
# 그래서 "무소유 파티션 수"로는 정지를 볼 수 없다(2026-09-08 1회차에서 그 지표가
# 끝까지 0이었다). 실제로 멈췄는지는 **current_offset이 안 움직이는 것**으로 봐야 한다.
partition_state() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group "$GROUP" 2>/dev/null | tr -d '\r' \
    | awk 'NR>1 && NF>=6 && $3 ~ /^[0-9]+$/ {
        cid = (NF >= 7 ? $7 : "-");
        cur = ($4 ~ /^[0-9]+$/ ? $4 : 0);
        lag = ($6 ~ /^[0-9]+$/ ? $6 : 0);
        printf "%s %s %s %s\n", $3, cur, lag, cid }'
}

# 멤버 수와 할당된 파티션 합.
#
# **둘 다 본다.** 2026-09-08에 "할당 >= 9"만 보고 기다렸다가, 직전 스케일 다운 뒤
# 남아 있던 기존 3개 멤버가 이미 9개를 쥔 상태에서 조건이 즉시 참이 돼
# **저장 인스턴스가 붙기도 전에** 다음 단계로 넘어간 적이 있다(그 회차를 버렸다).
member_state() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --members --group "$GROUP" 2>/dev/null | tr -d '\r' \
    | awk '$1=="GROUP" || NF<5 {next} {n++; a += $NF} END{printf "%d %d\n", n+0, a+0}'
}

TIMELINE="$EVIDENCE_DIR/scaledown_lag.csv"
echo "t_sec,phase,members,assigned,total_lag,stalled_partitions,max_partition_lag" > "$TIMELINE"
PARTFILE="$EVIDENCE_DIR/partition_lag.csv"
echo "t_sec,phase,partition,current_offset,lag,consumer_id" > "$PARTFILE"
T0=$(date +%s)

PREVOFF="$EVIDENCE_DIR/.prev_offsets"
: > "$PREVOFF"

sample() {  # $1 = phase
  local now ms members assigned total maxlag stalled
  now=$(( $(date +%s) - T0 ))
  ms=$(member_state); members=$(echo "$ms" | cut -d' ' -f1); assigned=$(echo "$ms" | cut -d' ' -f2)
  partition_state > /tmp/_pstate.$$ || true
  total=$(awk '{s+=$3} END{print s+0}' /tmp/_pstate.$$)
  maxlag=$(awk '{if($3>m) m=$3} END{print m+0}' /tmp/_pstate.$$)
  # **정지한 파티션**: 직전 표본 대비 current_offset이 하나도 안 움직인 파티션.
  # 소유자 유무가 아니라 이것이 "소비되고 있는가"의 지표다(위 partition_state 주석).
  stalled=$(awk 'NR==FNR {prev[$1]=$2; next}
                 ($1 in prev) && $2 == prev[$1] {n++}
                 END{print n+0}' "$PREVOFF" /tmp/_pstate.$$)
  awk -v t="$now" -v p="$1" '{printf "%s,%s,%s,%s,%s,%s\n", t, p, $1, $2, $3, $4}' /tmp/_pstate.$$ >> "$PARTFILE"
  awk '{print $1, $2}' /tmp/_pstate.$$ > "$PREVOFF"
  rm -f /tmp/_pstate.$$
  echo "$now,$1,$members,$assigned,$total,$stalled,$maxlag" >> "$TIMELINE"
  log "  t=${now}s $1 멤버=$members 할당=$assigned 총lag=$total 정지파티션=$stalled 최대파티션lag=$maxlag"
}

sample_for() {  # $1 = 초, $2 = phase
  local t0; t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$1" ]; do sample "$2"; sleep 5; done
}

: > "$OUT"
log "=== 부하 중 스케일 다운 (파티션 $PARTITIONS, 프로듀서 ${SHARDS}프로세스) ==="

# ── 1. 스택 ────────────────────────────────────────────────────
$COMPOSE --profile scale down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis kafka-init >/dev/null 2>&1 || true
wait_until 300 "Kafka healthy" bash -c '[ "$(docker inspect telemetry-kafka --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
wait_until 300 "토픽 생성" bash -c 'docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q vehicle-telemetry'

# ── 2. 파티션 확장은 backend 기동 **전에** ─────────────────────
# 2026-09-08 실측: 이미 떠 있는 컨슈머는 metadata.max.age.ms(기본 5분)가 지나야
# 새 파티션을 본다. 순서를 지키면 그 대기가 아예 없다.
docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
  --alter --topic vehicle-telemetry --partitions "$PARTITIONS" >/dev/null 2>&1 || true
ACTUAL=$(docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic vehicle-telemetry 2>/dev/null | grep -c "Partition:")
log "파티션: $ACTUAL개 (backend 기동 전에 확장)"
evidence_count partitions_actual "$ACTUAL"

$COMPOSE up -d backend >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "backend 기동"

# ── 3. 저장 인스턴스 2개 ───────────────────────────────────────
COMPOSE_FILES="$SCALE_ENV" bash scripts/scale-storage.sh up 2 >/dev/null 2>&1
wait_until 300 "멤버 9 · 할당 9" bash -c '
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --members --group telemetry-storage-group 2>/dev/null | tr -d "\r" \
  | awk "\$1==\"GROUP\" || NF<5 {next} {n++; a+=\$NF} END{exit !(n>=9 && a>=9)}"'
log "저장 인스턴스 2개 기동 — 멤버/할당 $(member_state)"

# ── 4. 부하 ────────────────────────────────────────────────────
log "발행 시작: ${SHARDS}개 프로세스"
for s in $(seq 0 $((SHARDS - 1))); do
  MSYS_NO_PATHCONV=1 docker run -d --rm --name "telemetry-producer-$s" --network "$NET" \
    -v "$WINPWD/load-test/storage-scale:/w" -w /w "$TOOLS" \
    python -u produce_backlog.py --count "$PER_SHARD" --vehicles 200 \
    --shard "$s" --shards "$SHARDS" >/dev/null
done
log "예열 ${WARMUP_SEC}초 — lag이 쌓여야 '소비가 멈췄다'가 보인다"
sample_for "$WARMUP_SEC" before

# ── 5. 스케일 다운 ─────────────────────────────────────────────
DOWN_AT=$(( $(date +%s) - T0 ))
log "--- t=${DOWN_AT}s 스케일 다운 (저장 인스턴스 2개 제거) ---"
evidence_count scaledown_at_sec "$DOWN_AT"
COMPOSE_FILES="$SCALE_ENV" bash scripts/scale-storage.sh down >/dev/null 2>&1
DOWN_RET=$(( $(date +%s) - T0 - DOWN_AT ))
log "down 반환: ${DOWN_RET}초"
evidence_count down_return_sec "$DOWN_RET"

sample_for "$OBSERVE_SEC" after

# ── 6. 발행 정지 후 드레인 ─────────────────────────────────────
log "발행 정지"
for s in $(seq 0 $((SHARDS - 1))); do docker rm -f "telemetry-producer-$s" >/dev/null 2>&1 || true; done
DRAIN_T0=$(date +%s)
wait_until 900 "lag 0" bash -c '
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group telemetry-storage-group 2>/dev/null | tr -d "\r" \
  | awk "NR>1 && NF>=6 && \$6 ~ /^[0-9]+\$/ {s+=\$6} END{exit !(s==0)}"' || true
DRAIN=$(( $(date +%s) - DRAIN_T0 ))
log "드레인 완료: ${DRAIN}초"
evidence_count drain_after_stop_sec "$DRAIN"
sample "drained"

# ── 7. 정합성 ──────────────────────────────────────────────────
TOPIC_TOTAL=$(docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:29092 --topic vehicle-telemetry 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
ROWS=$(docker exec telemetry-influxdb influx query \
  "from(bucket: \"$BKT\") |> range(start: -3h) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r._field == \"speed\") |> group() |> count()" \
  --org "$ORG" --token "$TOK" --raw 2>/dev/null \
  | tr -d '\r' | awk -F, '$0 ~ /^,/ && $NF ~ /^[0-9]+$/ {v=$NF} END{print v+0}')
log "토픽 총 $TOPIC_TOTAL / InfluxDB 행 $ROWS"
evidence_count topic_total "$TOPIC_TOTAL"
evidence_count influx_rows "$ROWS"

# ── 8. 집계 ────────────────────────────────────────────────────
# 정지한 파티션이 처음 나타난 시각과 마지막으로 보인 시각 → 소비가 멈춰 있던 구간.
# **소유자 유무가 아니라 offset 정지로 본다** — 정적 멤버십에서는 죽은 멤버가 소유권을
# 세션 만료까지 들고 있어서 "무소유 파티션"은 끝까지 0으로 나온다(1회차에서 확인).
STALL_FROM=$(awk -F, 'NR>1 && $2=="after" && $6>0 {print $1; exit}' "$TIMELINE")
STALL_TO=$(awk -F, 'NR>1 && $2=="after" && $6>0 {last=$1} END{print last}' "$TIMELINE")
MAX_STALLED=$(awk -F, 'NR>1 && $2=="after" {if($6>m) m=$6} END{print m+0}' "$TIMELINE")
MAX_LAG_BEFORE=$(awk -F, 'NR>1 && $2=="before" {if($7>m) m=$7} END{print m+0}' "$TIMELINE")
MAX_LAG_AFTER=$(awk -F, 'NR>1 && $2=="after"  {if($7>m) m=$7} END{print m+0}' "$TIMELINE")
evidence_count stall_first_sec "${STALL_FROM:-0}"
evidence_count stall_last_sec "${STALL_TO:-0}"
evidence_count max_stalled_partitions "${MAX_STALLED:-0}"
evidence_count max_partition_lag_before "$MAX_LAG_BEFORE"
evidence_count max_partition_lag_after "$MAX_LAG_AFTER"

log ""
log "=== 요약 ==="
log "스케일 다운 시각      : t=${DOWN_AT}s (down 반환 ${DOWN_RET}초)"
log "정지 파티션 구간      : t=${STALL_FROM:-없음}s ~ t=${STALL_TO:-없음}s (최대 ${MAX_STALLED:-0}개 동시 정지)"
log "파티션별 최대 lag     : 다운 전 $MAX_LAG_BEFORE → 다운 후 $MAX_LAG_AFTER"
log "발행 정지 후 드레인   : ${DRAIN}초"
log "정합성                : 토픽 $TOPIC_TOTAL vs 행 $ROWS"

VERDICT="관찰"
[ "$TOPIC_TOTAL" = "$ROWS" ] && VERDICT="PASS(유실 0)" || VERDICT="확인 필요(토픽≠행)"
log "판정: $VERDICT"

evidence_capture_prometheus final
evidence_capture_kafka_groups "$GROUP"
evidence_capture_topic_offsets vehicle-telemetry
evidence_capture_log_lines telemetry-backend "Revoke previously assigned|partitions assigned|FencedInstance" backend-key-lines.txt
evidence_capture_file "$OUT" console.log
evidence_finish "부하 중 스케일 다운에서 유실 0이고, 소비가 멈추는 구간이 파티션별로 보인다" "$VERDICT"
log "DONE"
