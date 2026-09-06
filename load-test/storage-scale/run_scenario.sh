#!/bin/bash
# 저장 경로(Java) 수평 확장 — 무엇이 막고 있고, 풀면 무슨 일이 나는가.
#
# ## 왜 이 실험이 남아 있었나
#
# 2026-09-05 리밸런싱 측정은 **Python 이상 감지만** 3 → 1 → 3으로 스케일했다. 저장 경로는
# `container_name`과 호스트 포트 8080이 고정돼 있어 `--scale`이 안 돼서 **미측정으로 남겼다.**
# 수집 파이프라인의 본류인데 여기만 비어 있다.
#
# ## 왜 compose 오버라이드를 안 쓰나
#
# `extends`는 `container_name`까지 복사해 이름이 충돌하고, 오버라이드로 그 값을 **지울 방법이
# 없다**(빈 값을 허용하지 않는다). 서비스 정의를 통째로 복사하면 환경변수 50줄이 두 벌이 되어
# 반드시 드리프트한다 — 이 저장소는 그 종류의 드리프트를 P0-3에서 이미 한 번 겪었다.
#
# 그래서 **실행 중인 `telemetry-backend`의 환경변수를 `docker inspect`로 읽어 그대로** 새
# 인스턴스에 넘긴다. 같은 이미지, 같은 설정이 보장된다.
#
# ## 두 모드
#
#   naive : 아무것도 안 바꾸고 인스턴스를 하나 더 띄운다.
#           백엔드는 **MQTT 수집과 Kafka→InfluxDB 저장을 겸하고** 있고 MQTT client-id와
#           Kafka group.instance.id가 둘 다 고정이다. 무엇이 먼저 깨지는지 본다.
#   split : MQTT 수집을 끄고(MQTT_INGEST_ENABLED=false) 정적 멤버 id를 인스턴스별로 갈라
#           **저장 역할만** 늘린다. 그 상태에서 리밸런싱·재전달·중복·유실을 측정한다.
#
# 사용법: bash run_scenario.sh <naive|split> [추가 인스턴스 수]
set -euo pipefail
cd "$(dirname "$0")/../.."

MODE="${1:?naive 또는 split}"
EXTRA="${2:-2}"
VEHICLES="${VEHICLES:-50}"
SAMPLE_SEC="${SAMPLE_SEC:-20}"
OBSERVE_SEC="${OBSERVE_SEC:-180}"

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/storage-scale/_result_${MODE}.txt"
IMG="vehicle-telemetry-platform-backend"
NET="vehicle-telemetry-platform_telemetry-net"
GROUP="telemetry-storage-group"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "storage-scale" "bash load-test/storage-scale/run_scenario.sh $MODE $EXTRA"
evidence_input mode "$MODE"
evidence_input extra_instances "$EXTRA"
evidence_input vehicles "$VEHICLES"
evidence_input publish_interval_sec 0.2
evidence_input observe_sec "$OBSERVE_SEC"

TOK=$(grep '^INFLUXDB_TOKEN=' .env | cut -d= -f2-)
ORG=$(grep '^INFLUXDB_ORG=' .env | cut -d= -f2-)
BKT=$(grep '^INFLUXDB_BUCKET=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }

topic_end_offsets() {
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'
}
influx_rows() {
  docker exec telemetry-influxdb influx query \
    "from(bucket: \"$BKT\") |> range(start: -3h) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r._field == \"speed\") |> group() |> count()" \
    --org "$ORG" --token "$TOK" --raw 2>/dev/null \
    | tr -d '\r' | awk -F, '$0 ~ /^,/ && $NF ~ /^[0-9]+$/ {v=$NF} END{print v+0}'
}
mqtt_received() {
  curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
    | awk '$1 ~ /^telemetry_mqtt_messages_received_total([{]|$)/ {gsub(/.* /,""); s+=$0} END{printf "%d", s+0}'
}

TIMELINE="$EVIDENCE_DIR/timeline.csv"
echo "t_sec,phase,group_state,members,lag,topic_offset,influx_rows_skipped,storage_instances" > "$TIMELINE"
T0=$(date +%s)
LAST_MEMBERS="?"; LAST_STATE="?"; LAST_LAG=0

sample() {  # $1 = phase, $2 = 인스턴스 수
  local raw state members lag off
  raw=$(docker exec telemetry-kafka bash -c "
    kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group $GROUP --state 2>/dev/null
    echo @@S@@
    kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group $GROUP 2>/dev/null
  " 2>/dev/null | tr -d '\r' || true)
  state=$(echo "$raw"   | awk '/@@S@@/{n++; next} n==0 && NR>1 && NF>=5 {print $(NF-1)}' | tail -1)
  members=$(echo "$raw" | awk '/@@S@@/{n++; next} n==0 && NR>1 && NF>=5 {print $NF}'     | tail -1)
  lag=$(echo "$raw"     | awk '/@@S@@/{n++; next} n==1 && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}')
  off=$(topic_end_offsets vehicle-telemetry)
  # InfluxDB 행 수는 매 표본마다 세지 않는다 — 3시간 range 집계라 한 번에 수 초가 걸려
  # 표본 간격을 넘긴다. 시작·끝에서만 센다.
  echo "$(( $(date +%s) - T0 )),$1,${state:-?},${members:-?},$lag,$off,,$2" | tee -a "$TIMELINE" \
    | sed 's/^/  ts /' >> "$OUT"
  log "  ts $(( $(date +%s) - T0 ))s $1 state=${state:-?} members=${members:-?} lag=$lag topic=$off inst=$2"
  LAST_MEMBERS="${members:-?}"; LAST_STATE="${state:-?}"; LAST_LAG="$lag"
}

sample_for() {  # $1 = 총 초, $2 = phase, $3 = 인스턴스 수
  local t0; t0=$(date +%s)
  while [ $(( $(date +%s) - t0 )) -lt "$1" ]; do sample "$2" "$3"; sleep "$SAMPLE_SEC"; done
}

# 실행 중인 backend의 환경변수를 그대로 복사한다. HOSTNAME은 컨테이너마다 달라야 하므로 뺀다.
#
# `{{end}}`를 빼먹으면 docker inspect가 "template parsing error"만 찍고 **빈 출력**을 낸다.
# 그러면 `-e` 인자가 하나도 안 붙은 채 컨테이너가 뜨고, 이미지 기본값으로 부팅해서
# `localhost:5432`에 붙으려다 죽는다 — 실제로 첫 실행에서 그렇게 됐다. 조용히 비는 종류의
# 실패라, 아래 검증을 붙여 **인자가 비면 즉시 멈춘다.**
storage_env_args() {  # $1 = 인스턴스 번호
  docker inspect telemetry-backend --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | grep -vE '^(HOSTNAME|PATH|JAVA_|LANG)=' | grep -v '^$' \
    | while IFS= read -r kv; do printf ' -e %q' "$kv"; done
  if [ "$MODE" = "split" ]; then
    # 수집을 끄고(이 인스턴스는 저장만 한다) 정적 멤버 id를 갈라준다.
    printf ' -e %q -e %q' "MQTT_INGEST_ENABLED=false" "GROUP_INSTANCE_ID_BASE=telemetry-storage-$1"
  fi
}

: > "$OUT"
log "=== 저장 경로 수평 확장: $MODE (추가 인스턴스 $EXTRA개) ==="

# ── 1. 스택 ────────────────────────────────────────────────────
$COMPOSE down -v >/dev/null 2>&1 || true
for i in $(seq 1 "$EXTRA"); do docker rm -f "telemetry-backend-storage-$i" >/dev/null 2>&1 || true; done
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis >/dev/null 2>&1 || true
wait_until 300 "PostgreSQL healthy" bash -c '[ "$(docker inspect telemetry-postgres --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
$COMPOSE up -d backend anomaly-detector >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "스택 기동 완료 (backend 1개)"

# ── 2. 부하 ────────────────────────────────────────────────────
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
$COMPOSE run -d --name telemetry-sim-0 \
  -e VEHICLE_COUNT=$VEHICLES -e PUBLISH_INTERVAL=0.2 simulator >/dev/null 2>&1
log "부하 기동 (${VEHICLES}대) — 90초 기준선"
wait_sec 90
MQTT_BEFORE=$(mqtt_received)
sample baseline 1
log "기준선: state=$LAST_STATE members=$LAST_MEMBERS lag=$LAST_LAG mqtt_received=$MQTT_BEFORE"

# ── 3. 스케일 아웃 ─────────────────────────────────────────────
log "--- 인스턴스 ${EXTRA}개 추가 (모드 $MODE) ---"
for i in $(seq 1 "$EXTRA"); do
  ENV_ARGS=$(storage_env_args "$i")
  # 환경변수 복사가 조용히 비는 것을 막는다. 20개는 임의 하한이 아니라, 정상 복사 시
  # 약 45개가 나오는 것을 보고 잡은 값이다.
  ENV_COUNT=$(printf '%s' "$ENV_ARGS" | grep -o ' -e ' | wc -l)
  if [ "$ENV_COUNT" -lt 20 ]; then
    log "** 환경변수 복사 실패($ENV_COUNT개) — 중단한다. docker inspect 템플릿을 확인하라"
    exit 1
  fi
  # shellcheck disable=SC2046,SC2086
  eval docker run -d --name "telemetry-backend-storage-$i" --network "$NET" \
    $ENV_ARGS "$IMG" >/dev/null
  log "  telemetry-backend-storage-$i 기동 (환경변수 ${ENV_COUNT}개 복사)"
done
# 새 인스턴스가 실제로 살아서 그룹에 붙는지 본다. naive 모드에서는 여기서 죽는 것이
# 결과의 일부이므로 **중단하지 않고 상태만 남긴다.**
sleep 30
for i in $(seq 1 "$EXTRA"); do
  st=$(docker inspect "telemetry-backend-storage-$i" --format '{{.State.Status}} exit={{.State.ExitCode}}' 2>/dev/null || echo "없음")
  log "  telemetry-backend-storage-$i 상태: $st"
done
sample_for "$OBSERVE_SEC" scaled-out $((1 + EXTRA))

# ── 4. 스케일 인 ───────────────────────────────────────────────
log "--- 추가 인스턴스 제거 ---"
for i in $(seq 1 "$EXTRA"); do docker stop -t 30 "telemetry-backend-storage-$i" >/dev/null 2>&1 || true; done
sample_for 120 scaled-in 1

# ── 5. 부하 정지 후 드레인 ──────────────────────────────────────
log "부하 정지 (SIGTERM)"
docker stop -t 90 telemetry-sim-0 >/dev/null 2>&1 || true
for _ in $(seq 1 30); do
  docker logs telemetry-sim-0 2>&1 | grep -q "전체 시뮬레이터 종료 완료" && break
  sleep 2
done
SIM_CONFIRMED=$(docker logs telemetry-sim-0 2>&1 | grep -o '\[STATS\].*' | tail -1 \
  | tr ' ' '\n' | awk -F= '$1=="confirmed" {print $2+0}' | tail -1)
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
for _ in $(seq 1 60); do sample draining 1; [ "$LAST_LAG" = "0" ] && break; sleep "$SAMPLE_SEC"; done

# ── 6. 집계 ────────────────────────────────────────────────────
TOPIC=$(topic_end_offsets vehicle-telemetry)
DLQ=$(topic_end_offsets vehicle-telemetry-dlq)
ROWS=$(influx_rows)
MQTT_AFTER=$(mqtt_received)
# 리밸런싱은 **저장 경로 그룹만** 센다.
REBALANCE=$(docker logs telemetry-backend 2>&1 \
  | grep -ciE "$GROUP.*(Revoke previously assigned|Attempt to heartbeat failed|leaving the group|sending LeaveGroup|already rebalancing)" || true)

evidence_capture_prometheus final
evidence_capture_kafka_groups "$GROUP"
evidence_capture_topic_offsets vehicle-telemetry vehicle-telemetry-dlq
evidence_capture_log_lines telemetry-backend \
  "Revoke previously assigned|leaving the group|already rebalancing|Lost connection|Connection lost|client id|FencedInstanceId|UnreleasedInstanceId" backend-key-lines.txt
for i in $(seq 1 "$EXTRA"); do
  docker logs "telemetry-backend-storage-$i" 2>&1 | tail -60 \
    > "$EVIDENCE_DIR/storage-$i-tail.txt" 2>/dev/null || true
done

evidence_count sim_confirmed_puback "${SIM_CONFIRMED:-0}"
evidence_count backend_mqtt_received_before "$MQTT_BEFORE"
evidence_count backend_mqtt_received_after "$MQTT_AFTER"
evidence_count kafka_topic_end_offset "$TOPIC"
evidence_count kafka_dlq_end_offset "$DLQ"
evidence_count influx_rows "$ROWS"
evidence_count rebalance_log_lines "$REBALANCE"

log ""
log "=== 결과 ($MODE) ==="
log "시뮬레이터 PUBACK      : ${SIM_CONFIRMED:-측정불가}"
log "Kafka vehicle-telemetry : $TOPIC"
log "InfluxDB 행            : $ROWS"
log "DLQ                    : $DLQ"
log "리밸런싱 로그($GROUP)  : $REBALANCE"
log "시계열: $TIMELINE"

# 성공 기준을 실행 전에 정한다. **토픽이 PUBACK보다 크면 수집이 이중화된 것**이고,
# 행이 토픽보다 작으면 유실이다(같은 키 덮어쓰기로 행 <= 토픽일 수는 있다).
CRIT="수집 이중화 없음(토픽 <= PUBACK) AND 유실 0(행 + DLQ >= 토픽)"
if [ -n "${SIM_CONFIRMED:-}" ] && [ "$TOPIC" -gt "$SIM_CONFIRMED" ]; then
  VERDICT="관찰 — 수집 이중화(토픽 $TOPIC > PUBACK $SIM_CONFIRMED, 차이 $((TOPIC - SIM_CONFIRMED)))"
elif [ $((ROWS + DLQ)) -lt "$TOPIC" ]; then
  VERDICT="관찰 — 유실 $((TOPIC - ROWS - DLQ))"
else
  VERDICT="PASS (토픽 $TOPIC, 행 $ROWS, DLQ $DLQ)"
fi
log "판정: $CRIT → $VERDICT"

evidence_capture_file "$OUT" console.log
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
