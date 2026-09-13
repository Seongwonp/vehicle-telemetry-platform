#!/usr/bin/env bash
# 감지기 DLQ ↔ dlq.py replay 왕복에서 `x-dlq-replay-count`가 이어지는지 **실제 Kafka**로 본다
# (roadmap 4-b, docs/event-correlation-design.md §6-1).
#
# 사용: bash load-test/dlq-replay-count/run_repro.sh [라벨]
#   라벨은 evidence metadata에만 남는다(예: before-fix / after-fix).
#
# 격리: 실행마다 전용 토픽 `itc-<run>-s5-{telemetry,dlq,alerts}`와 전용 그룹
# `itc-<run>-s5-group`(감지기)·`itc-<run>-s5-replay`(dlq.py 커서)를 쓴다.
# 운영 토픽·그룹은 건드리지 않는다. **끝나면 이 실행이 만든 토픽·그룹만 지운다.** volume은 건드리지 않는다.
#
# 코드: 감지기와 dlq.py는 **작업 트리 코드를 마운트**해서 쓴다(이미지는 파이썬 의존성만 제공).
# 그래서 고치기 전·후를 같은 스크립트로 비교할 수 있다.
set -uo pipefail
cd "$(dirname "$0")/../.."
. load-test/lib/evidence.sh

LABEL="${1:-unlabeled}"
RUN="rc$(date +%H%M%S)"
NET=vehicle-telemetry-platform_telemetry-net
IMG=vehicle-telemetry-platform-anomaly-detector
WINPWD=$(pwd -W 2>/dev/null || pwd)
KT="docker exec telemetry-kafka"

evidence_init dlq-replay-count "bash load-test/dlq-replay-count/run_repro.sh $LABEL"
E="$EVIDENCE_DIR"
{
  echo "label            : $LABEL"
  echo "run_id           : $RUN"
  echo "topics           : itc-$RUN-s5-telemetry / -dlq / -alerts (전용, 끝나면 삭제)"
  echo "groups           : itc-$RUN-s5-group / itc-$RUN-s5-replay (전용, 끝나면 삭제)"
  echo "detector_image   : $(docker image inspect $IMG --format '{{.Id}}') (의존성만 — 코드는 작업 트리 마운트)"
  echo "detector_sha256  : $(sha256sum anomaly-detector/anomaly_detector.py | cut -c1-16)"
  echo "dlq_tool_sha256  : $(sha256sum dlq-tools/dlq.py | cut -c1-16)"
} >> "$E/metadata.txt"

[ "$(docker inspect -f '{{.State.Health.Status}}' telemetry-kafka 2>/dev/null)" = healthy ] \
  || { echo "[중단] Kafka가 healthy가 아니다" | tee -a "$E/metadata.txt"; exit 1; }

for t in telemetry dlq alerts; do
  $KT kafka-topics --bootstrap-server localhost:29092 --create --if-not-exists \
    --topic "itc-$RUN-s5-$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1
done

MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
  -e RUN_ID="$RUN" -e SLOT=s5 -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 -e ML_ENABLED=false \
  -v "$WINPWD/anomaly-detector":/app \
  -v "$WINPWD/load-test/anomaly-contract-kafka":/w \
  -v "$WINPWD/dlq-tools":/dlq \
  "$IMG" python /w/driver.py s5 > "$E/driver_output.txt" 2>&1
DRIVER_EXIT=$?

# 원본 레코드와 헤더를 브로커에서 직접 덤프한다 — 드라이버 출력만 믿지 않는다.
for t in telemetry dlq; do
  $KT kafka-console-consumer --bootstrap-server localhost:29092 --topic "itc-$RUN-s5-$t" \
    --from-beginning --timeout-ms 8000 \
    --property print.headers=true --property print.offset=true --property print.value=false \
    > "$E/topic_${t}_headers.txt" 2>/dev/null || true
done

cat "$E/driver_output.txt"
VERDICT=$(grep -o '판정: [A-Z]*' "$E/driver_output.txt" | tail -1)

# 이 실행이 만든 것만 지운다.
for t in telemetry dlq alerts; do
  $KT kafka-topics --bootstrap-server localhost:29092 --delete --topic "itc-$RUN-s5-$t" >/dev/null 2>&1 || true
done
for g in "itc-$RUN-s5-group" "itc-$RUN-s5-replay"; do
  $KT kafka-consumer-groups --bootstrap-server localhost:29092 --delete --group "$g" >/dev/null 2>&1 || true
done
echo "cleanup          : 전용 토픽 3개·그룹 2개 삭제 요청" >> "$E/metadata.txt"
echo "driver_exit      : $DRIVER_EXIT" >> "$E/metadata.txt"

evidence_finish "DLQ replay-count 순서가 (없음)→1→2이고 3회차 replay가 --max-replays 2로 차단" "${VERDICT:-판정 없음} ($LABEL)"
