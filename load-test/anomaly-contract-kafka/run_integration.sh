#!/bin/bash
# 감지 경로의 offset·격리·복구를 **실제 Kafka**로 확인한다 (P0-2a 항목 2).
#
# ## 왜 필요한가
#
# `anomaly-detector/tests/test_consume_loop.py`는 **운영 소비 루프를 실행한 단위 테스트**다.
# `main()`을 그대로 돌리지만 consumer/producer가 가짜라 **분기**만 보여준다.
# 브로커가 실제로 offset을 어떻게 커밋하는지, 재전달이 진짜 일어나는지는 못 본다.
# 여기서는 진짜 브로커에 붙어 **committed offset을 Kafka에게 물어본다.**
#
# 발행 실패도 가짜가 아니다 — producer를 도달 불가 브로커로 향하게 해서 실제로 실패시킨다.
#
# ## 격리
#
# 전용 토픽(`itc-<run>-*`)과 전용 Consumer Group을 쓴다. 운영 토픽·그룹은 건드리지 않고
# **volume도 지우지 않는다**(`down -v`를 쓰지 않는다).
#
# 사용법: bash load-test/anomaly-contract-kafka/run_integration.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

RUN="${RUN:-$(date +%H%M%S)}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
NET="vehicle-telemetry-platform_telemetry-net"
IMG="vehicle-telemetry-platform-anomaly-detector"
OUT="load-test/anomaly-contract-kafka/_result.txt"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "anomaly-contract-kafka" "bash load-test/anomaly-contract-kafka/run_integration.sh"
evidence_input run_id "$RUN"
evidence_input topics "itc-$RUN-telemetry / -dlq / -alerts (전용)"
evidence_input group "itc-$RUN-group (전용)"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
: > "$OUT"
WINPWD=$(pwd -W 2>/dev/null || pwd)

log "=== 감지 경로 실제 Kafka 계약 검증 (run=$RUN) ==="

# 감지기 이미지는 이 스크립트가 쓴다 — 코드를 고쳤으면 반드시 다시 빌드해야 한다.
# 2026-09-09에 옛 이미지를 재서 E2E 한 회차를 통째로 버렸다.
log "감지기 이미지 빌드"
$COMPOSE build anomaly-detector >/dev/null 2>&1 || { log "** 빌드 실패"; exit 1; }

$COMPOSE up -d zookeeper kafka >/dev/null 2>&1
wait_until 300 "Kafka healthy" bash -c \
  '[ "$(docker inspect telemetry-kafka --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'

# **시나리오마다 토픽·그룹을 나눈다.** 처음에 하나로 묶었더니 앞 시나리오가 남긴
# committed offset을 뒤가 물려받아, 올바른 동작이 실패로 판정됐다.
# s2와 s3만 일부러 같이 쓴다 — s3이 s2의 장애 상태를 이어받아야 한다.
for slot in s1 s23 s4; do
  for t in telemetry dlq alerts; do
    docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092       --create --if-not-exists --topic "itc-$RUN-$slot-$t" --partitions 1 --replication-factor 1       >/dev/null 2>&1
  done
done
log "전용 토픽 생성 (슬롯 3 × 3, 파티션 1 — offset 판정을 단순하게)"

PASS=0; FAIL=0
for s in s1 s2 s3 s4; do
  log ""
  # `-v .../anomaly-detector:/app` 로 **작업 트리의 코드**를 쓴다 — 이미지에 구운 것과
  # 다를 수 있으므로, 지금 고친 코드를 재는 것이 맞다.
  if MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
       -e RUN_ID="$RUN" -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e ML_ENABLED=false \
       -v "$WINPWD/anomaly-detector":/app \
       -v "$WINPWD/load-test/anomaly-contract-kafka":/w \
       "$IMG" python /w/driver.py "$s" 2>&1 | tee -a "$OUT" | grep -q "판정: PASS"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
  fi
done

log ""
log "=== 요약: PASS $PASS / FAIL $FAIL ==="
evidence_count scenarios_pass "$PASS"
evidence_count scenarios_fail "$FAIL"

# 전용 토픽만 지운다. **운영 토픽과 volume은 건드리지 않는다.**
for slot in s1 s23 s4; do
  for t in telemetry dlq alerts; do
    docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092       --delete --topic "itc-$RUN-$slot-$t" >/dev/null 2>&1
  done
done

evidence_capture_file "$OUT" console.log
VERDICT=$([ "$FAIL" -eq 0 ] && echo PASS || echo FAIL)
evidence_finish "실제 Kafka에서 offset·격리·복구가 계약대로 동작한다" "$VERDICT"
log "판정: $VERDICT"
