#!/bin/bash
# session.timeout.ms를 줄이면 무엇을 잃는가 (ADR-023 남은 과제).
#
# ## 왜 필요한가
#
# 2026-09-08에 `session.timeout.ms`를 45,000ms로 **명시**하면서 이렇게 적었다 —
# "줄이면 스케일 다운이 빨라지지만 GC 정지·순단을 죽음으로 오판해 리밸런싱이 는다.
#  **줄였을 때의 대가는 아직 안 쟀다.**"
#
# 그 대가를 잰다. 방법은 `docker pause`다 — 컨테이너를 SIGSTOP으로 얼리면 하트비트가
# 멈추므로 **긴 GC 정지와 같은 모양**이 된다. 얼린 시간이 session.timeout.ms보다 길면
# 브로커가 그 멤버를 죽은 것으로 보고 리밸런싱을 돈다.
#
# ## 무엇으로 세는가 — 세대(generation) 번호
#
# 리밸런싱이 몇 번 돌았는지는 컨슈머 로그의
# `Successfully joined group with generation Generation{generationId=N ...}`에서 N으로 본다.
# 로그 줄을 세는 것보다 정확하다 — 재시도나 부분 실패로 같은 세대가 여러 줄 찍힐 수 있다.
#
# ## 조건
#
#   A: session.timeout.ms = 45000 (현재 값), 20초 정지  → 타임아웃 안 넘김
#   B: session.timeout.ms = 10000 (줄인 값), 20초 정지  → 타임아웃 넘김
#
# **같은 20초 정지가 한쪽에서는 아무 일도 아니고 다른 쪽에서는 리밸런싱이 된다**는 것을
# 보이는 게 목적이다. 부하는 걸지 않는다 — 리밸런싱 발생 여부는 부하와 무관하고,
# 부하를 걸면 회차 간 비교가 흔들린다(2026-09-07 교훈).
#
# 사용법: bash load-test/storage-scale/run_session_timeout_cost.sh [정지 초]
set -euo pipefail
cd "$(dirname "$0")/../.."

PAUSE_SEC="${1:-20}"
PARTITIONS="${PARTITIONS:-9}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
SCALE_ENV="-f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/storage-scale/_result_session_timeout.txt"
GROUP="telemetry-storage-group"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "storage-scale" "bash load-test/storage-scale/run_session_timeout_cost.sh $PAUSE_SEC"
evidence_input mode session_timeout_cost
evidence_input pause_sec "$PAUSE_SEC"
evidence_input partitions "$PARTITIONS"
evidence_input conditions "A=45000ms(현재) / B=10000ms(줄인 값)"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }

# 모든 backend 컨테이너에서 관측된 **최대 generationId**. 리밸런싱마다 1씩 는다.
generation() {
  local g=0 c v
  for c in $(docker ps --filter 'name=telemetry-backend' --format '{{.Names}}'); do
    v=$(docker logs "$c" 2>&1 | grep -o 'generationId=[0-9]*' | sed 's/[^0-9]//g' \
        | sort -n | tail -1)
    [ -n "${v:-}" ] && [ "$v" -gt "$g" ] && g="$v"
  done
  echo "$g"
}

members() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --members --group "$GROUP" 2>/dev/null | tr -d '\r' \
    | awk '$1=="GROUP" || NF<5 {next} {n++} END{print n+0}'
}

TIMELINE="$EVIDENCE_DIR/session_timeout.csv"
echo "condition,session_timeout_ms,phase,t_sec,members,generation" > "$TIMELINE"

run_condition() {  # $1 = 라벨, $2 = session.timeout.ms
  local label="$1" timeout_ms="$2" t0 g_before g_after m_before m_during m_after
  log ""
  log "════ 조건 $label : session.timeout.ms=$timeout_ms, ${PAUSE_SEC}초 정지 ════"

  $COMPOSE --profile scale down -v >/dev/null 2>&1 || true
  KAFKA_SESSION_TIMEOUT_MS="$timeout_ms" $COMPOSE up -d \
    mosquitto zookeeper kafka influxdb postgres redis kafka-init >/dev/null 2>&1 || true
  wait_until 300 "Kafka healthy" bash -c '[ "$(docker inspect telemetry-kafka --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
  wait_until 300 "토픽 생성" bash -c 'docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q vehicle-telemetry'
  docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
    --alter --topic vehicle-telemetry --partitions "$PARTITIONS" >/dev/null 2>&1 || true

  KAFKA_SESSION_TIMEOUT_MS="$timeout_ms" $COMPOSE up -d backend >/dev/null 2>&1
  wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
  KAFKA_SESSION_TIMEOUT_MS="$timeout_ms" COMPOSE_FILES="$SCALE_ENV" \
    bash scripts/scale-storage.sh up 1 >/dev/null 2>&1
  wait_until 300 "멤버 6" bash -c '
    docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
      --describe --members --group telemetry-storage-group 2>/dev/null | tr -d "\r" \
    | awk "\$1==\"GROUP\" || NF<5 {next} {n++} END{exit !(n>=6)}"'

  # **설정이 실제로 먹었는지 확인한다.** 안 그러면 두 조건이 같은 값으로 돌아도 모른다.
  local actual
  actual=$(docker logs telemetry-backend 2>&1 | grep -o 'session.timeout.ms = [0-9]*' | tail -1)
  log "  실제 컨슈머 설정: $actual"
  case "$actual" in *"= $timeout_ms") ;; *) log "  ** 설정이 반영되지 않았다 — 중단"; exit 1;; esac

  # 안정화 — 기동 직후의 리밸런싱이 끝나야 정지 효과만 볼 수 있다.
  local st; st=$(date +%s)
  until [ $(( $(date +%s) - st )) -ge 30 ]; do sleep 5; done

  t0=$(date +%s)
  g_before=$(generation); m_before=$(members)
  log "  정지 전  : 멤버=$m_before generation=$g_before"
  echo "$label,$timeout_ms,before,0,$m_before,$g_before" >> "$TIMELINE"

  docker pause telemetry-backend-storage-1 >/dev/null
  log "  telemetry-backend-storage-1 정지(SIGSTOP) — ${PAUSE_SEC}초"
  st=$(date +%s); until [ $(( $(date +%s) - st )) -ge "$PAUSE_SEC" ]; do sleep 2; done
  m_during=$(members)
  log "  정지 중  : 멤버=$m_during"
  echo "$label,$timeout_ms,during,$(( $(date +%s) - t0 )),$m_during,$(generation)" >> "$TIMELINE"

  docker unpause telemetry-backend-storage-1 >/dev/null
  log "  재개"
  st=$(date +%s); until [ $(( $(date +%s) - st )) -ge 60 ]; do sleep 5; done

  g_after=$(generation); m_after=$(members)
  log "  재개 후  : 멤버=$m_after generation=$g_after (증가 $(( g_after - g_before )))"
  echo "$label,$timeout_ms,after,$(( $(date +%s) - t0 )),$m_after,$g_after" >> "$TIMELINE"

  evidence_count "${label}_generation_before" "$g_before"
  evidence_count "${label}_generation_after" "$g_after"
  evidence_count "${label}_rebalances" "$(( g_after - g_before ))"
  evidence_count "${label}_members_during_pause" "$m_during"
  evidence_capture_log_lines telemetry-backend \
    "Successfully joined group|Revoke previously assigned|Member .* sending LeaveGroup" \
    "backend-rebalance-${label}.txt"
}

: > "$OUT"
log "=== session.timeout.ms를 줄였을 때의 대가 (정지 ${PAUSE_SEC}초) ==="

run_condition A45000 45000
run_condition B10000 10000

A_REB=$(awk -F, '$1=="A45000_rebalances"{print $2}' "$EVIDENCE_DIR/counts.csv")
B_REB=$(awk -F, '$1=="B10000_rebalances"{print $2}' "$EVIDENCE_DIR/counts.csv")
A_MEM=$(awk -F, '$1=="A45000_members_during_pause"{print $2}' "$EVIDENCE_DIR/counts.csv")
B_MEM=$(awk -F, '$1=="B10000_members_during_pause"{print $2}' "$EVIDENCE_DIR/counts.csv")

log ""
log "=== 요약 (같은 ${PAUSE_SEC}초 정지) ==="
log "조건                     리밸런싱  정지 중 멤버"
log "A session.timeout=45000s   $A_REB        $A_MEM"
log "B session.timeout=10000s   $B_REB        $B_MEM"

VERDICT="관찰"
if [ "${B_REB:-0}" -gt "${A_REB:-0}" ]; then
  VERDICT="확인됨(줄이면 같은 정지가 리밸런싱이 된다)"
elif [ "${B_REB:-0}" = "${A_REB:-0}" ]; then
  VERDICT="차이 없음 — 정지 시간을 늘려 다시 봐야 한다"
fi
log "판정: $VERDICT"

# **정리한다.** `scale-storage.sh up`이 만든 Prometheus 대상 파일은 compose `down`으로는
# 안 지워진다 — 남겨두면 다음 기동 때 없는 인스턴스를 긁으려 하고, 그게 정확히
# `static_configs`를 피한 이유(장애처럼 보이는 정상)를 되살린다. 실제로 한 번 남겼다.
COMPOSE_FILES="$SCALE_ENV" bash scripts/scale-storage.sh down >/dev/null 2>&1 || true

evidence_capture_kafka_groups "$GROUP"
evidence_capture_file "$OUT" console.log
evidence_finish "같은 길이의 정지가 session.timeout.ms에 따라 리밸런싱이 되는지" "$VERDICT"
log "DONE"
