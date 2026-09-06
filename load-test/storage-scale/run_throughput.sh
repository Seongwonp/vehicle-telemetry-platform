#!/bin/bash
# 저장 경로 수평 확장의 **처리량 이득** 측정 (docs/roadmap.md P2).
#
# ## 왜 별도 시나리오인가
#
# `run_scenario.sh split`은 "확장 가능한 구조가 됐다"까지만 보였다 — 컨슈머 멤버가
# 3에서 9로 늘었지만 **처리량 차이는 못 쟀다.** 이유가 둘이었다:
#
#   1. 파티션이 3개라 단일 인스턴스(concurrency 3)가 이미 다 잡고 있었다.
#      **파티션 수를 넘는 컨슈머는 유휴다** — 인스턴스를 늘려도 이득이 원천적으로 없다.
#   2. 부하가 MQTT 경로로 들어와서 수집(약 9,600 msg/s)이 먼저 막혔다.
#      저장 경로를 넘기는 부하를 만들 수가 없었다.
#
# 그래서 이 시나리오는 **파티션을 늘리고**, **Kafka에 직접 백로그를 쌓아** 드레인 속도를 잰다.
# 드레인 속도는 유입과 무관한 저장 경로만의 처리량이다.
#
# ## 왜 "백로그 한 번 쌓고 드레인"이 아니라 "계속 넣으면서 재나"
#
# 처음에는 백로그 1.5M을 쌓아두고 드레인 속도를 재려 했다. 그런데 **인스턴스 1개가
# 초당 3만 건을 처리해서** 40초 만에 백로그가 비었고, B 구간에는 드레인할 것이 남지
# 않았다(실측: lag 1,212,000 → 0, 처리 32,519 msg/s).
#
# 그래서 프로듀서를 **구간 내내 계속 돌린다.** 그러면 다음이 성립한다.
#
#     처리량(capacity) = 토픽 증가율 − lag 증가율
#
# 단, 이 식은 **lag > 0일 때만** 처리량이다. lag이 0이 되면 컨슈머가 놀고 있다는 뜻이라
# 그때 재는 건 처리 능력이 아니라 **발행 속도**다. 그래서 구간마다 lag이 0으로
# 떨어졌는지를 같이 기록하고, 떨어졌으면 그 구간 값을 "하한"으로만 읽는다.
#
# ## A/B/A로 잰다
#
#   A: 인스턴스 1개 — 처리량 측정
#   B: 3개로 늘려서 — 처리량 측정
#   A': 다시 1개로  — 부하나 캐시 상태가 흘러서 생긴 차이가 아님을 확인
#
# A'가 A와 비슷해야 B의 차이를 인스턴스 수 효과로 볼 수 있다.
#
# 사용법: bash run_throughput.sh [파티션 수] [프로듀서 프로세스 수]
set -euo pipefail
cd "$(dirname "$0")/../.."

PARTITIONS="${1:-12}"
SHARDS="${2:-6}"
# 프로세스당 발행 상한. 구간이 다 끝날 때까지 마르지 않을 만큼 크게 잡고, 끝나면 죽인다.
PER_SHARD="${PER_SHARD:-20000000}"
PHASE_SEC="${PHASE_SEC:-150}"
SAMPLE_SEC="${SAMPLE_SEC:-15}"

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/storage-scale/_result_throughput.txt"
IMG="vehicle-telemetry-platform-backend"
TOOLS="vehicle-telemetry-platform-anomaly-detector"
NET="vehicle-telemetry-platform_telemetry-net"
GROUP="telemetry-storage-group"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "storage-scale" "bash load-test/storage-scale/run_throughput.sh $PARTITIONS $SHARDS"
evidence_input mode throughput
evidence_input partitions "$PARTITIONS"
evidence_input producer_shards "$SHARDS"
evidence_input producer_mode "구간 내내 계속 발행"
evidence_input phase_sec "$PHASE_SEC"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }
WINPWD=$(pwd -W 2>/dev/null || pwd)

# "state members lag current_offset_sum topic_end_offset_sum"
# 토픽 끝 offset과 컨슈머 현재 offset을 **같은 호출에서** 읽는다. 따로 부르면 그 사이에
# 수만 건이 들어와 "처리량 = 토픽 증가율 − lag 증가율" 계산이 흔들린다.
group_state() {
  docker exec telemetry-kafka bash -c "
    kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group $GROUP --state 2>/dev/null
    echo @@S@@
    kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group $GROUP 2>/dev/null
    echo @@S@@
    kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:29092 --topic vehicle-telemetry 2>/dev/null
  " 2>/dev/null | tr -d '\r' | awk -F'[:\t ]+' '
    /@@S@@/{n++; next}
    n==0 && NR>1 && NF>=5 {state=$(NF-1); members=$NF}
    n==1 && $6 ~ /^[0-9]+$/ {lag+=$6; cur+=$4}
    n==2 && NF==3 && $3 ~ /^[0-9]+$/ {end+=$3}
    END{printf "%s %s %d %d %d", (state?state:"?"), (members?members:"?"), lag+0, cur+0, end+0}'
}

TIMELINE="$EVIDENCE_DIR/throughput.csv"
echo "t_sec,phase,instances,state,members,lag,current_offset,topic_end_offset,consume_msg_s,produce_msg_s" > "$TIMELINE"
T0=$(date +%s)
PREV_CUR=""; PREV_END=""; PREV_T=""

sample() {  # $1 = phase, $2 = 인스턴스 수
  local now st state members lag cur end crate="" prate=""
  now=$(( $(date +%s) - T0 ))
  st=$(group_state)
  state=$(echo "$st" | awk '{print $1}'); members=$(echo "$st" | awk '{print $2}')
  lag=$(echo "$st" | awk '{print $3}'); cur=$(echo "$st" | awk '{print $4}'); end=$(echo "$st" | awk '{print $5}')
  if [ -n "$PREV_CUR" ] && [ "$now" -gt "$PREV_T" ]; then
    crate=$(( (cur - PREV_CUR) / (now - PREV_T) ))
    prate=$(( (end - PREV_END) / (now - PREV_T) ))
  fi
  PREV_CUR="$cur"; PREV_END="$end"; PREV_T="$now"
  echo "$now,$1,$2,$state,$members,$lag,$cur,$end,$crate,$prate" >> "$TIMELINE"
  log "  ts ${now}s $1 inst=$2 members=$members lag=$lag 처리=${crate:-–} 발행=${prate:-–} msg/s"
  LAST_LAG="$lag"
}

sample_for() {  # $1 = 총 초, $2 = phase, $3 = 인스턴스 수
  local t0; t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$1" ]; do
    sample "$2" "$3"
    # lag이 0이 되면 컨슈머가 놀고 있다는 뜻이라 그때부터는 처리 능력이 아니라 발행
    # 속도를 재게 된다. 구간을 끊지는 않고(뒤 구간과 조건을 맞춰야 한다) 표시만 남긴다.
    [ "${LAST_LAG:-1}" = "0" ] && log "  ** lag=0 — 이 표본은 처리 능력이 아니라 발행 속도다"
    sleep "$SAMPLE_SEC"
  done
}

# 저장 전용 인스턴스(수집 끄고 정적 멤버 id 분리). run_scenario.sh split과 같은 방식이다.
start_storage() {  # $1 = 번호
  local env_args count
  env_args=$(docker inspect telemetry-backend --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | grep -vE '^(HOSTNAME|PATH|JAVA_|LANG)=' | grep -v '^$' \
    | while IFS= read -r kv; do printf ' -e %q' "$kv"; done)
  env_args="$env_args $(printf ' -e %q -e %q' "MQTT_INGEST_ENABLED=false" "GROUP_INSTANCE_ID_BASE=telemetry-storage-$1")"
  count=$(printf '%s' "$env_args" | grep -o ' -e ' | wc -l)
  if [ "$count" -lt 20 ]; then
    log "** 환경변수 복사 실패($count개) — 중단"; exit 1
  fi
  # shellcheck disable=SC2086
  eval docker run -d --name "telemetry-backend-storage-$1" --network "$NET" $env_args "$IMG" >/dev/null
  log "  telemetry-backend-storage-$1 기동 (환경변수 ${count}개)"
}

: > "$OUT"
log "=== 저장 경로 처리량 A/B/A (파티션 $PARTITIONS, 프로듀서 ${SHARDS}프로세스 계속 발행) ==="

# ── 1. 스택 ────────────────────────────────────────────────────
# anomaly-detector는 띄우지 않는다 — 같은 토픽을 다른 그룹으로 읽어 CPU를 나눠 쓰므로
# 저장 경로만 보려면 빼는 게 맞다.
$COMPOSE down -v >/dev/null 2>&1 || true
for i in 1 2; do docker rm -f "telemetry-backend-storage-$i" >/dev/null 2>&1 || true; done
# kafka-init을 **반드시 함께 띄운다.** 이 서비스가 init-topics.sh로 토픽을 만든다.
# 처음에 빼먹었더니 토픽을 만들 주체가 없는데 backend는 토픽 대기 뒤에 뜨도록 짜서
# 데드락이 됐다(다른 시나리오는 backend를 먼저 띄워 자동 생성에 기대고 있었다).
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis kafka-init >/dev/null 2>&1 || true
wait_until 300 "PostgreSQL healthy" bash -c '[ "$(docker inspect telemetry-postgres --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'

# ── 2. 파티션 확장 ─────────────────────────────────────────────
# **이게 이 실험의 전제다.** 파티션 3개로는 컨슈머를 늘려도 유휴만 는다.
wait_until 300 "토픽 생성" bash -c 'docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q vehicle-telemetry'
docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
  --alter --topic vehicle-telemetry --partitions "$PARTITIONS" >/dev/null 2>&1 || true
ACTUAL=$(docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic vehicle-telemetry 2>/dev/null | grep -c "Partition:")
log "파티션 확장: $ACTUAL개"
evidence_count partitions_actual "$ACTUAL"

$COMPOSE up -d backend >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "backend 기동 완료 (1개)"

# ── 3. 발행 시작 (구간 내내 계속 돈다) ─────────────────────────
# 기다리지 않는다. 컨슈머보다 빠르게 계속 넣어야 lag이 유지되고, lag이 유지돼야
# "처리량 = 토픽 증가율 − lag 증가율"이 처리 능력을 뜻한다.
log "발행 시작: ${SHARDS}개 프로세스 (계속 발행, 구간 종료 시 정지)"
for s in $(seq 0 $((SHARDS - 1))); do
  MSYS_NO_PATHCONV=1 docker run -d --rm --name "telemetry-producer-$s" --network "$NET" \
    -v "$WINPWD/load-test/storage-scale:/w" -w /w "$TOOLS" \
    python -u produce_backlog.py --count "$PER_SHARD" --vehicles 200 \
    --shard "$s" --shards "$SHARDS" >/dev/null
done
# 백로그가 쌓일 시간을 준다 — 처음부터 재면 아직 lag이 0이라 발행 속도를 재게 된다.
wait_sec 60
log "발행 안정화 — lag=$(group_state | awk '{print $3}')"

# ── 4. A: 인스턴스 1개 ─────────────────────────────────────────
log "--- A: 인스턴스 1개로 드레인 (${PHASE_SEC}초) ---"
sample_for "$PHASE_SEC" A-1inst 1

# ── 5. B: 인스턴스 3개 ─────────────────────────────────────────
log "--- B: 인스턴스 3개로 드레인 (${PHASE_SEC}초) ---"
start_storage 1; start_storage 2
wait_sec 40   # 리밸런싱이 끝나고 나서부터 재야 한다
sample_for "$PHASE_SEC" B-3inst 3

# ── 6. A': 다시 1개 ────────────────────────────────────────────
log "--- A': 다시 1개로 (${PHASE_SEC}초) — B의 차이가 조건 변화가 아님을 확인 ---"
for i in 1 2; do docker stop -t 30 "telemetry-backend-storage-$i" >/dev/null 2>&1 || true; done
wait_sec 40
sample_for "$PHASE_SEC" A2-1inst 1

# ── 7. 정리와 집계 ─────────────────────────────────────────────
log "발행 정지"
for s in $(seq 0 $((SHARDS - 1))); do docker rm -f "telemetry-producer-$s" >/dev/null 2>&1 || true; done

# 각 구간의 처리 속도는 **구간 내 표본의 중앙값**을 쓴다. 평균은 리밸런싱 직후의
# 0에 가까운 표본에 끌려간다.
phase_median() {  # $1 = phase, $2 = 열 번호(9=처리, 10=발행)
  awk -F, -v p="$1" -v c="$2" 'NR>1 && $2==p && $c != "" {print $c}' "$TIMELINE" | sort -n | awk '
    {a[NR]=$1} END{if(NR==0){print "-"} else if(NR%2){print a[(NR+1)/2]} else {print int((a[NR/2]+a[NR/2+1])/2)}}'
}
# lag이 0으로 떨어진 표본이 있으면 그 구간 값은 처리 능력이 아니라 발행 속도의 하한이다.
phase_idle() {  # $1 = phase
  awk -F, -v p="$1" 'NR>1 && $2==p && $6+0==0 {n++} END{print n+0}' "$TIMELINE"
}
RATE_A=$(phase_median A-1inst 9);  PROD_A=$(phase_median A-1inst 10);  IDLE_A=$(phase_idle A-1inst)
RATE_B=$(phase_median B-3inst 9);  PROD_B=$(phase_median B-3inst 10);  IDLE_B=$(phase_idle B-3inst)
RATE_A2=$(phase_median A2-1inst 9); PROD_A2=$(phase_median A2-1inst 10); IDLE_A2=$(phase_idle A2-1inst)
evidence_count lag_zero_samples_a "$IDLE_A"
evidence_count lag_zero_samples_b "$IDLE_B"
evidence_count lag_zero_samples_a2 "$IDLE_A2"
evidence_count produce_a_median "$PROD_A"
evidence_count produce_b_median "$PROD_B"
evidence_count produce_a2_median "$PROD_A2"

evidence_capture_prometheus final
evidence_capture_kafka_groups "$GROUP"
evidence_capture_topic_offsets vehicle-telemetry
evidence_capture_log_lines telemetry-backend "Revoke previously assigned|partitions assigned|FencedInstance" backend-key-lines.txt
evidence_count rate_a_1inst_median "$RATE_A"
evidence_count rate_b_3inst_median "$RATE_B"
evidence_count rate_a2_1inst_median "$RATE_A2"

log ""
log "=== 결과 (구간별 중앙값, msg/s) ==="
log "A  인스턴스 1개 : 처리 ${RATE_A} / 발행 ${PROD_A}   (lag=0 표본 ${IDLE_A}개)"
log "B  인스턴스 3개 : 처리 ${RATE_B} / 발행 ${PROD_B}   (lag=0 표본 ${IDLE_B}개)"
log "A' 인스턴스 1개 : 처리 ${RATE_A2} / 발행 ${PROD_A2}  (lag=0 표본 ${IDLE_A2}개)"
log ""
log "읽는 법: lag=0 표본이 있는 구간의 처리 값은 **처리 능력이 아니라 발행 속도**다."
if [ "$RATE_A" != "-" ] && [ "$RATE_B" != "-" ] && [ "$RATE_A" -gt 0 ] 2>/dev/null; then
  log "B/A 배수: $(awk -v b="$RATE_B" -v a="$RATE_A" 'BEGIN{printf "%.2f", b/a}')"
fi

CRIT="A'가 A와 같은 수준이고, B가 A보다 빠르면 확장 이득이 있다 (단 lag>0인 구간에서만 유효)"
VERDICT="관찰 — A=${RATE_A} B=${RATE_B} A'=${RATE_A2} msg/s (lag=0 표본 ${IDLE_A}/${IDLE_B}/${IDLE_A2})"
log "판정: $CRIT → $VERDICT"

evidence_capture_file "$OUT" console.log
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
