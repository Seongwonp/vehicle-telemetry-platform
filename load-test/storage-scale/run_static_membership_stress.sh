#!/bin/bash
# 정적 멤버십을 **끄면** 무엇을 잃는가 (ADR-024의 가장 큰 공백).
#
# ## 왜 필요한가
#
# 2026-09-09에 정적 멤버십을 끈 대조군을 돌려 **끄는 것의 이득**을 쟀다 —
# 스케일 다운 재할당이 46~54초에서 9초가 된다. 그런데 그 문서에 이렇게 적었다:
#
#   "이 대조군은 끄는 것의 이득만 봤다. 정적 멤버십을 켠 이유(리밸런싱 폭풍)는
#    재현하지 않았다. 따라서 '끄는 게 낫다'는 근거가 아니다."
#
# 그 반대편을 잰다.
#
# ## 무엇을 주입하는가 — max.poll.interval.ms 초과
#
# 정적 멤버십이 막아주는 것은 **poll 간격을 넘긴 컨슈머가 그룹을 완전히 떠났다가
# (member id 리셋) 재가입하는 경로**다. 12시간 soak test에서 여러 컨슈머가 동시에 이
# 경로를 타서 "MemberIdRequiredException → group is already rebalancing"에 갇혔다.
#
# `docker pause`로 `max.poll.interval.ms`(기본 300초)를 넘겨 정지시킨다.
# session.timeout.ms(45초)도 당연히 넘으므로 **둘 다 걸리는 상황**이다.
#
#   A: 정적 멤버십 ON  (현재 기본값)
#   B: 정적 멤버십 OFF
#
# ## 미리 밝혀두는 한계
#
# **이 실험은 "폭풍"을 재현하지 못할 수 있다.** soak test의 폭풍은 여러 컨슈머가
# 반복적으로 이 경로를 타면서 생긴 연쇄였고, 여기서는 인스턴스 하나를 한 번 정지시킨다.
# 차이가 안 나오면 "이 조건에서는 차이가 안 보인다"이지 "차이가 없다"가 아니다.
#
# 사용법: bash load-test/storage-scale/run_static_membership_stress.sh [정지 초]
set -euo pipefail
cd "$(dirname "$0")/../.."

PAUSE_SEC="${1:-330}"   # max.poll.interval.ms(300초)보다 길게
PARTITIONS="${PARTITIONS:-9}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
SCALE_ENV="-f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/storage-scale/_result_static_stress.txt"
GROUP="telemetry-storage-group"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "storage-scale" "bash load-test/storage-scale/run_static_membership_stress.sh $PAUSE_SEC"
evidence_input mode static_membership_stress
evidence_input pause_sec "$PAUSE_SEC"
evidence_input partitions "$PARTITIONS"
evidence_input conditions "A=정적 멤버십 ON / B=OFF"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }

generation() {
  local g=0 c v
  for c in $(docker ps --filter 'name=telemetry-backend' --format '{{.Names}}'); do
    v=$(docker logs "$c" 2>&1 | grep -o 'generationId=[0-9]*' | sed 's/[^0-9]//g' | sort -n | tail -1)
    [ -n "${v:-}" ] && [ "$v" -gt "$g" ] && g="$v"
  done
  echo "$g"
}
members() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --members --group "$GROUP" 2>/dev/null | tr -d '\r' \
    | awk '$1=="GROUP" || NF<5 {next} {n++} END{print n+0}'
}
# 폭풍의 표식. soak test에서 실제로 본 예외다.
#
# **정지시킨 컨테이너를 봐야 한다.** 처음엔 `telemetry-backend`만 셌는데, member id가
# 리셋되는 건 **정지됐다 돌아온 인스턴스**다. 그래서 집계는 "차이 없음"으로 나왔고
# 원본 로그를 열어보니 12건 대 0건으로 갈려 있었다(2026-09-09).
# **결과는 집계가 아니라 증거 파일에서 읽는다** — 이 프로젝트가 2026-09-06에 세운 규칙이다.
#
# `already rebalancing`은 세지 않는다. **정상 리밸런싱에서도 찍히는 줄**이라
# 폭풍의 지표가 아니다(처음에 넣었다가 양쪽 12건으로 똑같이 나와서 알았다).
storm_markers() {  # 모든 backend 컨테이너 합
  local n=0 c v
  for c in $(docker ps -a --filter 'name=telemetry-backend' --format '{{.Names}}'); do
    v=$(docker logs "$c" 2>&1 | grep -c 'MemberIdRequiredException' || true)
    n=$(( n + v ))
  done
  echo "$n"
}

TIMELINE="$EVIDENCE_DIR/static_stress.csv"
echo "condition,static_membership,phase,t_sec,members,generation,storm_markers" > "$TIMELINE"

run_condition() {  # $1 = 라벨, $2 = on|off
  local label="$1" mode="$2" t0 g0 g1 m0 m1 m_during s0 s1
  log ""
  log "════ 조건 $label : 정적 멤버십=$mode, ${PAUSE_SEC}초 정지 ════"

  if [ "$mode" = "off" ]; then
    export GROUP_INSTANCE_ID_BASE="" STORAGE_1_GROUP_INSTANCE_ID="" \
           STORAGE_2_GROUP_INSTANCE_ID="" STORAGE_3_GROUP_INSTANCE_ID=""
  else
    unset GROUP_INSTANCE_ID_BASE STORAGE_1_GROUP_INSTANCE_ID \
          STORAGE_2_GROUP_INSTANCE_ID STORAGE_3_GROUP_INSTANCE_ID || true
  fi

  $COMPOSE --profile scale down -v >/dev/null 2>&1 || true
  $COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis kafka-init >/dev/null 2>&1 || true
  wait_until 300 "Kafka healthy" bash -c '[ "$(docker inspect telemetry-kafka --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
  wait_until 300 "토픽 생성" bash -c 'docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q vehicle-telemetry'
  docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
    --alter --topic vehicle-telemetry --partitions "$PARTITIONS" >/dev/null 2>&1 || true

  $COMPOSE up -d backend >/dev/null 2>&1
  wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
  COMPOSE_FILES="$SCALE_ENV" bash scripts/scale-storage.sh up 1 >/dev/null 2>&1
  wait_until 300 "멤버 6" bash -c '
    docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
      --describe --members --group telemetry-storage-group 2>/dev/null | tr -d "\r" \
    | awk "\$1==\"GROUP\" || NF<5 {next} {n++} END{exit !(n>=6)}"'

  # **모드가 실제로 먹었는지 확인한다.** 두 조건이 같은 상태로 돌면 "차이 없음"이
  # 그럴듯하게 틀린 답으로 나온다 — session.timeout 실험에서 같은 이유로 대조를 넣었다.
  local off_warn
  off_warn=$(docker logs telemetry-backend 2>&1 | grep -c "정적 멤버십을 끕니다" || true)
  log "  정적 멤버십 끄기 경고 로그: ${off_warn}건 (모드=$mode)"
  if [ "$mode" = "off" ] && [ "$off_warn" -eq 0 ]; then log "  ** OFF인데 경고가 없다 — 중단"; exit 1; fi
  if [ "$mode" = "on" ] && [ "$off_warn" -ne 0 ]; then log "  ** ON인데 경고가 있다 — 중단"; exit 1; fi

  local st; st=$(date +%s); until [ $(( $(date +%s) - st )) -ge 30 ]; do sleep 5; done

  t0=$(date +%s); g0=$(generation); m0=$(members); s0=$(storm_markers)
  log "  정지 전  : 멤버=$m0 generation=$g0 폭풍표식=$s0"
  echo "$label,$mode,before,0,$m0,$g0,$s0" >> "$TIMELINE"

  docker pause telemetry-backend-storage-1 >/dev/null
  log "  telemetry-backend-storage-1 정지 — ${PAUSE_SEC}초 (max.poll.interval.ms=300초 초과)"
  st=$(date +%s)
  until [ $(( $(date +%s) - st )) -ge "$PAUSE_SEC" ]; do
    sleep 30
    log "    ... 정지 $(( $(date +%s) - st ))초 경과 멤버=$(members)"
  done
  m_during=$(members)
  echo "$label,$mode,during,$(( $(date +%s) - t0 )),$m_during,$(generation),$(storm_markers)" >> "$TIMELINE"

  docker unpause telemetry-backend-storage-1 >/dev/null
  log "  재개 — 재가입 관찰 120초"
  st=$(date +%s); until [ $(( $(date +%s) - st )) -ge 120 ]; do sleep 10; done

  g1=$(generation); m1=$(members); s1=$(storm_markers)
  log "  재개 후  : 멤버=$m1 generation=$g1 (리밸런싱 $(( g1 - g0 ))회) 폭풍표식=$s1"
  echo "$label,$mode,after,$(( $(date +%s) - t0 )),$m1,$g1,$s1" >> "$TIMELINE"

  evidence_count "${label}_rebalances" "$(( g1 - g0 ))"
  evidence_count "${label}_members_during_pause" "$m_during"
  evidence_count "${label}_members_after" "$m1"
  evidence_count "${label}_storm_markers" "$(( s1 - s0 ))"
  evidence_capture_log_lines telemetry-backend \
    "Successfully joined group|Revoke previously assigned|MemberIdRequired|already rebalancing" \
    "backend-rebalance-${label}.txt"
  evidence_capture_log_lines telemetry-backend-storage-1 \
    "Successfully joined group|Revoke previously assigned|MemberIdRequired|already rebalancing|poll" \
    "storage1-rebalance-${label}.txt"
}

: > "$OUT"
log "=== 정적 멤버십을 끄면 무엇을 잃는가 (정지 ${PAUSE_SEC}초 > max.poll.interval.ms 300초) ==="

run_condition Aon on
run_condition Boff off

A_R=$(awk -F, '$1=="Aon_rebalances"{print $2}' "$EVIDENCE_DIR/counts.csv")
B_R=$(awk -F, '$1=="Boff_rebalances"{print $2}' "$EVIDENCE_DIR/counts.csv")
A_S=$(awk -F, '$1=="Aon_storm_markers"{print $2}' "$EVIDENCE_DIR/counts.csv")
B_S=$(awk -F, '$1=="Boff_storm_markers"{print $2}' "$EVIDENCE_DIR/counts.csv")
A_M=$(awk -F, '$1=="Aon_members_after"{print $2}' "$EVIDENCE_DIR/counts.csv")
B_M=$(awk -F, '$1=="Boff_members_after"{print $2}' "$EVIDENCE_DIR/counts.csv")

log ""
log "=== 요약 (같은 ${PAUSE_SEC}초 정지) ==="
log "조건            리밸런싱  폭풍표식  재개 후 멤버"
log "A 정적 ON         $A_R         $A_S         $A_M"
log "B 정적 OFF        $B_R         $B_S         $B_M"

VERDICT="관찰"
if [ "${B_R:-0}" -gt "${A_R:-0}" ] || [ "${B_S:-0}" -gt "${A_S:-0}" ]; then
  VERDICT="차이 관측(끄면 리밸런싱·폭풍표식이 는다)"
else
  VERDICT="이 조건에서는 차이가 안 보인다 — 폭풍 재현 실패(차이가 없다는 뜻이 아니다)"
fi
log "판정: $VERDICT"

evidence_capture_kafka_groups "$GROUP"
evidence_capture_file "$OUT" console.log
evidence_finish "max.poll.interval.ms 초과 정지에서 정적 멤버십 on/off의 리밸런싱 차이" "$VERDICT"
log "DONE"

COMPOSE_FILES="$SCALE_ENV" bash scripts/scale-storage.sh down >/dev/null 2>&1 || true
