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
CPUFILE="$EVIDENCE_DIR/cpu.csv"
echo "t_sec,phase,instances,container,cpu_pct,mem_usage" > "$CPUFILE"
T0=$(date +%s)
PREV_CUR=""; PREV_END=""; PREV_T=""

# **처리량 상한이 어디인지 가르려고** 찍는다. 인스턴스를 늘려도 처리량이 안 오를 때,
# InfluxDB CPU가 포화면 저장이 상한이고, 호스트 전체가 포화면 측정 환경이 상한이다.
# 후자는 결론이 아니라 한계다 — 노트북 한 대에서 프로듀서 6 + 백엔드 N을 같이 돌린다.
#
# **반드시 offset을 읽은 뒤에 부른다.** `docker stats --no-stream`은 호출당 1~2초가
# 걸려서, offset 읽기 전에 부르면 그 사이에 수만 건이 흘러 처리량 계산이 흔들린다.
#
# **컨테이너를 골라 찍지 않고 전부 찍는다.** 2026-09-07 1차 스윕에서 telemetry-influxdb·
# kafka·backend만 찍었더니 "InfluxDB는 여유가 있다"까지는 말할 수 있어도 **호스트가
# 포화인지**를 판정할 근거가 없었다. 프로듀서 6개가 쓰는 CPU가 빠져 있었기 때문이다.
# 전부 찍고 합을 코어 수와 비교해야 "측정 환경이 상한"인지 갈린다.
sample_cpu() {  # $1 = t_sec, $2 = phase, $3 = 인스턴스 수
  docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null \
    | tr -d '\r' | while IFS= read -r line; do
        [ -n "$line" ] && echo "$1,$2,$3,$line" >> "$CPUFILE"
      done
}

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
  sample_cpu "$now" "$1" "$2"
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
for i in $(seq 1 7); do docker rm -f "telemetry-backend-storage-$i" >/dev/null 2>&1 || true; done
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

# ── 4. 인스턴스 스윕 ───────────────────────────────────────────
# 2026-09-06에는 1개와 3개만 재서 "3개가 더 빠르다"까지만 알았다. **왜 3배가 아닌지**를
# 가르려면 곡선이 어디서 꺾이는지 봐야 한다. 그래서 1→2→3→4로 훑는다.
#
# **왜 4에서 멈추나**: 리스너 `concurrency=3`(application.yml)이라 인스턴스 하나가
# 컨슈머 스레드 3개다. 파티션이 12개면 **4개(12스레드)에서 파티션이 다 찬다.**
# 5개째부터는 붙어도 파티션을 못 받아 유휴다 — 늘려도 이득이 없는 게 아니라
# **측정 자체가 무의미**하다. 파티션을 더 늘리면 그때 5개 이상을 볼 수 있다.
MAX_INST=$(( PARTITIONS / 3 ))
[ "$MAX_INST" -gt 8 ] && MAX_INST=8
[ "$MAX_INST" -lt 1 ] && MAX_INST=1

# 구간을 **기하급수로** 잡는다(1, 2, 4, 8). 2026-09-07 1차 스윕은 1→2→3→4로 훑었는데
# 인접 구간(2개와 3개)의 표본 범위가 겹쳐서 갈리지 않았다. 같은 시간에 더 넓은 범위를
# 보려면 배수로 벌리는 게 낫다. `STEPS`로 덮어쓸 수 있다.
if [ -n "${STEPS:-}" ]; then
  SWEEP="$STEPS"
else
  SWEEP=""
  n=2
  while [ "$n" -le "$MAX_INST" ]; do SWEEP="$SWEEP $n"; n=$(( n * 2 )); done
fi
log "스윕: 1${SWEEP} 인스턴스 (파티션 ${PARTITIONS} / concurrency 3 → 상한 ${MAX_INST})"
evidence_input sweep_steps "1$SWEEP"

# 호스트 코어 수. 컨테이너 CPU 합을 이 값 × 100%와 비교해야 포화를 판정할 수 있다.
NCPU=$(docker info --format '{{.NCPU}}' 2>/dev/null | tr -d '\r')
log "Docker가 보는 코어 수: ${NCPU:-?} (컨테이너 CPU 합의 상한은 ${NCPU:-?}00%)"
evidence_input docker_ncpu "${NCPU:-unknown}"

log "--- A: 인스턴스 1개로 드레인 (${PHASE_SEC}초) ---"
sample_for "$PHASE_SEC" "A-1inst" 1

PHASES="A-1inst"
PREV_N=1
for n in $SWEEP; do
  log "--- 인스턴스 ${n}개로 드레인 (${PHASE_SEC}초) ---"
  # 기하급수 스윕이라 한 번에 여러 개를 띄워야 한다(2→4면 2개 추가).
  # storage 번호는 1..n-1이고, 이미 뜬 것은 PREV_N-1개다.
  for i in $(seq "$PREV_N" $(( n - 1 ))); do start_storage "$i"; done
  PREV_N="$n"
  wait_sec 40   # 리밸런싱이 끝나고 나서부터 재야 한다
  sample_for "$PHASE_SEC" "B-${n}inst" "$n"
  PHASES="$PHASES B-${n}inst"
done

# ── 5. A': 다시 1개 ────────────────────────────────────────────
# **이 구간을 빼면 안 된다.** 2026-09-06 실행에서 A와 A'가 27% 차이 났다. 기준선이
# 그만큼 흔들린다는 걸 모르면 스윕 곡선의 기울기를 실제보다 정밀하게 믿게 된다.
log "--- A': 다시 1개로 (${PHASE_SEC}초) — 차이가 조건 변화가 아님을 확인 ---"
for i in $(seq 1 7); do
  docker stop -t 30 "telemetry-backend-storage-$i" >/dev/null 2>&1 || true
done
wait_sec 40
sample_for "$PHASE_SEC" A2-1inst 1
PHASES="$PHASES A2-1inst"

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
# 컨테이너별 CPU 중앙값. 상한이 InfluxDB인지 호스트인지 가르는 값이다.
cpu_median() {  # $1 = phase, $2 = 컨테이너 이름(정확히) 또는 prefix
  awk -F, -v p="$1" -v c="$2" 'NR>1 && $2==p && index($4,c)==1 {gsub(/%/,"",$5); print $5}' "$CPUFILE" \
    | sort -n | awk '{a[NR]=$1} END{if(NR==0){print "-"} else if(NR%2){print a[(NR+1)/2]} else {printf "%.1f",(a[NR/2]+a[NR/2+1])/2}}'
}
# 백엔드가 여러 개면 합이 그 구간의 저장 경로 총 CPU다.
cpu_backend_sum() {  # $1 = phase
  awk -F, -v p="$1" 'NR>1 && $2==p && index($4,"telemetry-backend")==1 {gsub(/%/,"",$5); s[$1]+=$5}
    END{n=0; for(t in s){v[n++]=s[t]}; if(n==0){print "-"; exit}
        for(i=0;i<n;i++)for(j=i+1;j<n;j++)if(v[i]>v[j]){x=v[i];v[i]=v[j];v[j]=x}
        if(n%2) printf "%.1f", v[(n-1)/2]; else printf "%.1f", (v[n/2-1]+v[n/2])/2}' "$CPUFILE"
}

# **모든 컨테이너의 CPU 합.** 이 값이 코어수×100%에 붙으면 상한은 시스템이 아니라
# 측정 환경이다 — 노트북 한 대에서 프로듀서와 백엔드 N개를 같이 돌리기 때문이다.
cpu_host_sum() {  # $1 = phase
  awk -F, -v p="$1" 'NR>1 && $2==p {gsub(/%/,"",$5); s[$1]+=$5}
    END{n=0; for(t in s){v[n++]=s[t]}; if(n==0){print "-"; exit}
        for(i=0;i<n;i++)for(j=i+1;j<n;j++)if(v[i]>v[j]){x=v[i];v[i]=v[j];v[j]=x}
        if(n%2) printf "%.1f", v[(n-1)/2]; else printf "%.1f", (v[n/2-1]+v[n/2])/2}' "$CPUFILE"
}

RESULT_ROWS=""
for p in $PHASES; do
  r=$(phase_median "$p" 9); pr=$(phase_median "$p" 10); idl=$(phase_idle "$p")
  ci=$(cpu_median "$p" telemetry-influxdb); ck=$(cpu_median "$p" telemetry-kafka); cb=$(cpu_backend_sum "$p")
  ch=$(cpu_host_sum "$p")
  evidence_count "cpu_host_sum_$(echo "$p" | tr 'A-Z-' 'a-z_')" "$ch"
  n=$(awk -F, -v ph="$p" 'NR>1 && $2==ph {print $3; exit}' "$TIMELINE")
  RESULT_ROWS="${RESULT_ROWS}${p}|${n}|${r}|${pr}|${idl}|${ci}|${ck}|${cb}|${ch}
"
  key=$(echo "$p" | tr 'A-Z-' 'a-z_')
  evidence_count "rate_${key}" "$r"
  evidence_count "produce_${key}" "$pr"
  evidence_count "lag_zero_${key}" "$idl"
  evidence_count "cpu_influxdb_${key}" "$ci"
  evidence_count "cpu_backend_sum_${key}" "$cb"
done

RATE_A=$(phase_median A-1inst 9);  PROD_A=$(phase_median A-1inst 10);  IDLE_A=$(phase_idle A-1inst)
RATE_B=$(phase_median "B-${MAX_INST}inst" 9)
RATE_A2=$(phase_median A2-1inst 9); IDLE_A2=$(phase_idle A2-1inst)

evidence_capture_prometheus final
evidence_capture_kafka_groups "$GROUP"
evidence_capture_topic_offsets vehicle-telemetry
evidence_capture_log_lines telemetry-backend "Revoke previously assigned|partitions assigned|FencedInstance" backend-key-lines.txt
evidence_capture_file "$CPUFILE" cpu.csv

log ""
log "=== 결과 (구간별 중앙값) ==="
log "구간         인스턴스  처리    발행    lag0  influxCPU%  kafkaCPU%  backendCPU합%  호스트CPU합%"
printf '%s' "$RESULT_ROWS" | while IFS='|' read -r p n r pr idl ci ck cb ch; do
  [ -z "$p" ] && continue
  log "$(printf '%-12s %5s %8s %8s %5s %10s %10s %13s %13s' "$p" "$n" "$r" "$pr" "$idl" "$ci" "$ck" "$cb" "$ch")"
done
[ -n "${NCPU:-}" ] && log "  (호스트CPU합의 포화 기준: ${NCPU}00% — 코어 ${NCPU}개)"
log ""
log "읽는 법:"
log "  - lag=0 표본이 있는 구간의 처리 값은 **처리 능력이 아니라 발행 속도**다."
log "  - 인스턴스를 늘려도 처리가 안 오르는데 influxCPU가 포화면 **InfluxDB가 상한**이다."
log "  - influxCPU에 여유가 있는데 안 오르면 호스트 CPU나 발행 속도를 봐야 한다."
log "  - A와 A'의 차이가 기준선의 흔들림이다. 그보다 작은 차이는 해석하지 마라."
if [ "$RATE_A" != "-" ] && [ "$RATE_B" != "-" ] && [ "$RATE_A" -gt 0 ] 2>/dev/null; then
  log "최대(${MAX_INST}개)/A 배수: $(awk -v b="$RATE_B" -v a="$RATE_A" 'BEGIN{printf "%.2f", b/a}')"
fi
if [ "$RATE_A" != "-" ] && [ "$RATE_A2" != "-" ] && [ "$RATE_A" -gt 0 ] 2>/dev/null; then
  log "기준선 흔들림 A'/A: $(awk -v b="$RATE_A2" -v a="$RATE_A" 'BEGIN{printf "%.2f", b/a}')"
fi

CRIT="인스턴스를 늘리며 처리량이 꺾이는 지점과 그때의 CPU를 함께 본다 (lag>0인 구간만 유효)"
VERDICT="관찰 — A=${RATE_A} 최대(${MAX_INST}개)=${RATE_B} A'=${RATE_A2} msg/s (lag=0 표본 A ${IDLE_A} / A' ${IDLE_A2})"
log "판정: $CRIT → $VERDICT"

evidence_capture_file "$OUT" console.log
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
