#!/bin/bash
# 사용법: sample.sh <stage_label> <duration_seconds> <interval_seconds> [출력_디렉터리]
# anomaly-detector가 --scale/replicas로 여러 컨테이너로 뜨면서 container_name 고정을
# 없앴기 때문에(ADR-016), 컨테이너 이름을 매번 동적으로 찾는다(고정 이름이 없어졌음).
STAGE="$1"
DURATION="${2:-300}"
INTERVAL="${3:-30}"
OUTDIR="${4:-/private/tmp/claude-501/-Users-parkseong-won-IdeaProjects-vehicle-telemetry-platform/25c5d0c9-024f-4349-b174-0829576d65bf/scratchpad/loadtest2}"
LOG="$OUTDIR/${STAGE}.log"
TOKEN="localtoken1234567890localtoken1234567890abcdefgh"
ORG="vehicle-telemetry"

# macOS 기본 bash엔 coreutils의 timeout(1)이 없다. 12시간 soak처럼 사람이 지켜보지 않는
# 실행에서 docker exec 하나가 (호스트 절전 등으로) 응답 없이 멈추면 while 루프 전체가
# 무한정 멈춰버린다 — 실제로 이 문제로 soak 1회차가 6시간 넘게 조용히 멈춰 있었다.
# 그래서 새 의존성 설치 없이 백그라운드 job + watchdog kill로 타임아웃을 흉내낸다.
run_timeout() {
  local secs="$1"; shift
  "$@" &
  local pid=$!
  ( sleep "$secs"; kill -9 "$pid" 2>/dev/null ) &
  local watchdog=$!
  wait "$pid" 2>/dev/null
  local exit_code=$?
  kill "$watchdog" 2>/dev/null
  wait "$watchdog" 2>/dev/null
  return $exit_code
}

mkdir -p "$OUTDIR"
echo "=== stage=$STAGE duration=${DURATION}s interval=${INTERVAL}s start=$(date -u +%FT%TZ) ===" > "$LOG"

START_ISO=$(date -u +%FT%TZ)
END_TS=$((SECONDS + DURATION))

while [ $SECONDS -lt $END_TS ]; do
  TS=$(date -u +%FT%TZ)
  echo "--- sample @ $TS ---" >> "$LOG"

  echo "[kafka lag]" >> "$LOG"
  run_timeout 15 docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group telemetry-storage-group 2>/dev/null >> "$LOG"
  [ $? -ne 0 ] && echo "[TIMEOUT] telemetry-storage-group describe (15s 초과)" >> "$LOG"

  ANOMALY_DESC=$(run_timeout 15 docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group anomaly-detector-group 2>/dev/null)
  ANOMALY_STATUS=$?
  echo "$ANOMALY_DESC" >> "$LOG"
  if [ $ANOMALY_STATUS -ne 0 ]; then
    echo "[TIMEOUT] anomaly-detector-group describe (15s 초과) — 이번 샘플 lag_sum 생략" >> "$LOG"
  else
    # 원시 표는 그대로 남기되(재현/검증용), 12시간 soak처럼 나중에 눈으로 다 훑기 힘든
    # 로그를 위해 파티션 LAG 합계를 매 샘플마다 한 줄로 뽑아둔다 — 분석 시 grep 한 번으로 추이 확인.
    # 컬럼 순서: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ... → LAG은 6번째 필드.
    LAG_SUM=$(echo "$ANOMALY_DESC" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')
    echo "[anomaly_lag_sum] ts=$TS sum=$LAG_SUM" >> "$LOG"
  fi

  echo "[influx count since $START_ISO]" >> "$LOG"
  run_timeout 15 docker exec telemetry-influxdb influx query "
from(bucket:\"telemetry\")
  |> range(start: $START_ISO)
  |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\")
  |> filter(fn: (r) => r._field == \"speed\")
  |> count()
  |> group()
  |> sum()
" --org "$ORG" --token "$TOKEN" 2>/dev/null >> "$LOG"
  [ $? -ne 0 ] && echo "[TIMEOUT] influx query (15s 초과)" >> "$LOG"

  echo "[docker stats]" >> "$LOG"
  AD_CONTAINERS=$(run_timeout 10 docker ps --filter "name=anomaly-detector" --format "{{.Names}}")
  run_timeout 15 docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" telemetry-backend telemetry-kafka telemetry-influxdb $AD_CONTAINERS 2>/dev/null >> "$LOG"
  [ $? -ne 0 ] && echo "[TIMEOUT] docker stats (15s 초과)" >> "$LOG"

  sleep "$INTERVAL"
done

echo "=== stage=$STAGE end=$(date -u +%FT%TZ) ===" >> "$LOG"
