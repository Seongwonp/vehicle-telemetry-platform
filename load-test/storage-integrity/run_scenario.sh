#!/bin/bash
# 저장 consumer 강제 종료 → 재전달 → 정합성 대조 (docs/roadmap.md P0-2).
#
# 지금까지 이 실험은 README의 수동 절차였고 1회만 돌렸다. 반복해서 변동 폭을 내려면
# 사람이 매번 같은 순서로 치는 것에 의존할 수 없다 — 그러면 회차 간 차이가 실험 때문인지
# 조작 때문인지 구분이 안 된다.
#
# **`docker kill`을 쓴다**(`stop`이 아니라). `stop`은 SIGTERM이라 Spring이 정상 종료하며
# in-flight offset을 커밋해버려서 **재려는 현상 자체가 안 생긴다.**
#
# 사용법: bash run_scenario.sh [부하 지속 초] [kill 시점 offset]
set -euo pipefail
cd "$(dirname "$0")/../.."

WARMUP_SEC="${1:-60}"
KILL_AT_OFFSET="${2:-20000}"
VEHICLES="${VEHICLES:-200}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/storage-integrity/_result_kill.txt"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "storage-integrity" \
  "bash load-test/storage-integrity/run_scenario.sh $WARMUP_SEC $KILL_AT_OFFSET"
evidence_input vehicles "$VEHICLES"
evidence_input publish_interval_sec 0.2
evidence_input warmup_sec "$WARMUP_SEC"
evidence_input kill_at_committed_offset "$KILL_AT_OFFSET"
evidence_input kill_signal SIGKILL

TOK=$(grep '^INFLUXDB_TOKEN=' .env | cut -d= -f2-)
ORG=$(grep '^INFLUXDB_ORG=' .env | cut -d= -f2-)
BKT=$(grep '^INFLUXDB_BUCKET=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }

topic_end_offsets() {
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic vehicle-telemetry 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}'
}
committed_offset() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group telemetry-storage-group 2>/dev/null | awk 'NR>1{s+=$4} END{print s+0}'
}
storage_lag() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group telemetry-storage-group 2>/dev/null | awk 'NR>1{s+=$6} END{print s+0}'
}
batch_size_sum() {
  curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
    | awk '$1 ~ /^telemetry_influx_write_batch_size_sum/ {gsub(/.* /,""); s+=$0} END{printf "%d", s+0}'
}
influx_rows() {
  docker exec telemetry-influxdb influx query \
    "from(bucket: \"$BKT\") |> range(start: -60m) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r._field == \"speed\") |> group() |> count()" \
    --org "$ORG" --token "$TOK" --raw 2>/dev/null \
    | tr -d '\r' | awk -F, '$0 ~ /^,/ && $NF ~ /^[0-9]+$/ {v=$NF} END{print v+0}'
}

: > "$OUT"
log "=== 강제 종료 정합성 (${VEHICLES}대, offset ${KILL_AT_OFFSET}에서 kill) ==="

# ── 1. 깨끗한 스택 ─────────────────────────────────────────────
# anomaly-detector는 이 측정과 무관하므로 띄우지 않는다.
$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis backend >/dev/null 2>&1 || true
wait_until 300 "PostgreSQL healthy" bash -c '[ "$(docker inspect telemetry-postgres --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
$COMPOSE up -d backend >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "스택 기동 완료"

# ── 2. 부하 ────────────────────────────────────────────────────
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
$COMPOSE run -d --name telemetry-sim-0 \
  -e VEHICLE_COUNT=$VEHICLES -e PUBLISH_INTERVAL=0.2 simulator >/dev/null 2>&1
log "시뮬레이터 기동 — 커밋 offset ${KILL_AT_OFFSET} 도달 대기"

# ── 3. 강제 종료 ───────────────────────────────────────────────
# 커밋 offset이 목표를 넘긴 시점에 죽인다. 시간이 아니라 offset을 기준으로 삼아야
# 회차 간 조건이 같아진다(부하 기동 속도는 실행마다 다르다).
wait_until 600 "커밋 offset >= $KILL_AT_OFFSET" \
  bash -c "[ \"\$(docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group telemetry-storage-group 2>/dev/null | awk 'NR>1{s+=\$4} END{print s+0}')\" -ge $KILL_AT_OFFSET ]"

# 메트릭은 프로세스 생명주기 카운터라 재시작으로 리셋된다. 종료 직전 값을 받아둔다.
#
# **그래도 정확하지 않다.** 샘플과 kill 사이에 처리된 메시지는 어느 쪽에도 안 잡힌다
# — 죽은 프로세스의 카운터는 사라지기 때문이다. 그 틈을 줄이려고 느린 docker exec
# (offset 조회)를 먼저 하고 curl을 마지막에 둔다. 그래도 남는 오차는 아래 주석 참고.
OFFSET_BEFORE=$(committed_offset)
SAMPLE_T0=$(date +%s%3N 2>/dev/null || date +%s000)
BATCH_BEFORE=$(batch_size_sum)
log "--- 강제 종료(SIGKILL): 커밋 offset=$OFFSET_BEFORE, batch_size_sum=$BATCH_BEFORE ---"
docker kill telemetry-backend >/dev/null
SAMPLE_GAP_MS=$(( $(date +%s%3N 2>/dev/null || date +%s000) - SAMPLE_T0 ))

wait_sec 10
$COMPOSE up -d backend >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "backend 재기동 완료 — ${WARMUP_SEC}초 더"
wait_sec "$WARMUP_SEC"

# ── 4. 부하 정지 후 드레인 ──────────────────────────────────────
log "부하 정지 (SIGTERM)"
docker stop -t 90 telemetry-sim-0 >/dev/null 2>&1 || true
for _ in $(seq 1 30); do
  docker logs telemetry-sim-0 2>&1 | grep -q "전체 시뮬레이터 종료 완료" && break
  sleep 2
done
SIM_CONFIRMED=$(docker logs telemetry-sim-0 2>&1 | grep -o '\[STATS\].*' | tail -1 \
  | tr ' ' '\n' | awk -F= '$1=="confirmed"{print $2+0}' | tail -1)
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
wait_sec 30
for _ in $(seq 1 60); do [ "$(storage_lag)" = "0" ] && break; sleep 10; done
log "드레인 완료 (lag=$(storage_lag))"

# ── 5. 집계 ────────────────────────────────────────────────────
TOPIC=$(topic_end_offsets)
ROWS=$(influx_rows)
BATCH_AFTER=$(batch_size_sum)
# 재시작으로 리셋되므로 종료 직전 값과 합산해야 총 저장 시도량이 된다.
BATCH_TOTAL=$((BATCH_BEFORE + BATCH_AFTER))
REDELIVERED=$((BATCH_TOTAL - TOPIC))
OVERWRITE_LOSS=$((TOPIC - ROWS))

evidence_capture_prometheus final
evidence_capture_kafka_groups telemetry-storage-group
evidence_capture_topic_offsets vehicle-telemetry vehicle-telemetry-dlq
evidence_count sim_confirmed_puback "${SIM_CONFIRMED:-0}"
evidence_count kafka_topic_end_offset "$TOPIC"
evidence_count committed_offset_before_kill "$OFFSET_BEFORE"
evidence_count batch_size_sum_before_kill "$BATCH_BEFORE"
evidence_count batch_size_sum_after_restart "$BATCH_AFTER"
evidence_count batch_size_sum_total "$BATCH_TOTAL"
evidence_count batch_sum_minus_topic "$REDELIVERED"
evidence_count sample_to_kill_gap_ms "$SAMPLE_GAP_MS"
evidence_count influx_rows "$ROWS"
evidence_count overwrite_loss "$OVERWRITE_LOSS"

log ""
log "=== 결과 ==="
log "시뮬레이터 브로커 확인    : ${SIM_CONFIRMED:-?}"
log "Kafka vehicle-telemetry   : $TOPIC"
log "InfluxDB 행               : $ROWS"
log "덮어쓰기 유실             : $OVERWRITE_LOSS  ← 이 실험의 불변식"
log ""
log "저장 시도(batch_size_sum) : $BATCH_TOTAL  (종료 전 $BATCH_BEFORE + 재기동 후 $BATCH_AFTER)"
log "  batch_sum − 토픽        : $REDELIVERED  (샘플→kill 간격 ${SAMPLE_GAP_MS}ms)"
log "  ** 이 값을 '재전달 건수'로 읽지 마라.** 샘플과 kill 사이에 처리된 메시지는"
log "  죽은 프로세스와 함께 사라져 어느 쪽에도 안 잡힌다. 그래서 이 차이는"
log "  (재전달) − (샘플 이후 유실분)이고, 실측에서 체계적으로 음수가 나온다."
log "  정확한 재전달 수를 내려면 프로세스 밖에 처리 카운터가 있어야 한다(미구현)."

# 판정은 **측정 가능한 불변식**으로만 한다.
# 재전달 수는 위 이유로 이 도구가 잴 수 없으므로 판정 근거에서 뺀다.
CRIT="브로커 확인 == Kafka 토픽 == InfluxDB 행 (재전달이 덮어쓰기로 흡수)"
if [ "$OVERWRITE_LOSS" -ne 0 ]; then
  VERDICT="FAIL (덮어쓰기 유실 $OVERWRITE_LOSS)"
elif [ -n "${SIM_CONFIRMED:-}" ] && [ "${SIM_CONFIRMED:-0}" -ne "$TOPIC" ]; then
  VERDICT="FAIL (브로커 확인 $SIM_CONFIRMED != 토픽 $TOPIC)"
else
  VERDICT="PASS (행 손실 0, 전 구간 일치 $TOPIC)"
fi
log "판정: $CRIT → $VERDICT"
evidence_capture_file "$OUT" console.log
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
