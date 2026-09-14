#!/usr/bin/env bash
# Redis 장애 × 낮은 부하 — 한 단계 1회 (load-test/redis-load/PLAN_20260914.md).
#
# 사용:  bash load-test/redis-load/run_load_outage.sh <L1|L2|L3>
#
# 전제: backend가 compose.observe.yml로 떠 있다(Tomcat 스레드 지표). Prometheus·Alertmanager는 띄우지 않는다.
#
# 남기는 것 (load-test/redis-load/evidence/<run_id>/):
#   gen_<i>.csv / live.csv   요청 1건당 한 줄 — 예정·실제 전송·종료 시각, HTTP, 오류 코드, timeout/연결 오류
#   metrics.csv              actuator 표본(약 2초) — Tomcat 스레드·처리 중 요청·JVM 스레드·CPU·힙
#   dockerstats.csv          docker stats 표본 — backend·redis·생성기 CPU·메모리
#   backend_reconnect.txt    Lettuce 재연결 로그
#   timeline.txt             구간 경계(epoch ms)와 부하 조건
#   summary.txt              위 원본의 구간별 집계와 PLAN §6 판정(summarize.py)
set -uo pipefail
cd "$(dirname "$0")/../.."
. load-test/lib/evidence.sh

LEVEL="${1:?L1|L2|L3}"
case "$LEVEL" in
  L1) GENS=4 ;;
  L2) GENS=8 ;;
  L3) GENS=12 ;;
  *) echo "L1|L2|L3 중 하나" >&2; exit 2 ;;
esac
RATE_PER_GEN=0.8
LIVE_RATE=1
WARM=20; BASE=90; OUTAGE=60; AFTER=150
DURATION=$(( WARM + BASE + OUTAGE + AFTER ))
NET=vehicle-telemetry-platform_telemetry-net
IMAGE=vehicle-telemetry-platform-anomaly-detector
REDIS=telemetry-redis
BACKEND=telemetry-backend
BASE_URL=http://localhost:8080

set -a; . ./.env; set +a
: "${ADMIN_USERNAME:?}" "${ADMIN_PASSWORD:?}"
export ADMIN_USERNAME ADMIN_PASSWORD

now_ms() { date +%s%3N; }
sleep_until() { local d=$(( $1 - $(now_ms) )); [ "$d" -gt 0 ] && sleep "$(awk -v d="$d" 'BEGIN{printf "%.3f", d/1000}')"; return 0; }
ctr_names() { docker ps -aq --filter "name=^redisload-" ; }

TMP="$(mktemp -d)"
cleanup() {
  touch "$TMP/stop_stats"
  docker start "$REDIS" >/dev/null 2>&1
  local ids; ids=$(ctr_names); [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1
  rm -rf "$TMP"
}
trap cleanup EXIT

# ── 사전 확인 (evidence를 만들기 전에) ─────────────────────────
[ -z "$(ctr_names)" ] || { echo "[중단] 이전 실행의 redisload-* 컨테이너가 남아 있다" >&2; exit 1; }
[ "$(docker inspect -f '{{.State.Running}}' "$REDIS" 2>/dev/null)" = true ] || { echo "[중단] Redis가 실행 중이 아니다" >&2; exit 1; }
curl -sf --max-time 5 "$BASE_URL/actuator/health/liveness" >/dev/null || { echo "[중단] backend liveness 실패" >&2; exit 1; }
# `curl | grep -q`로 쓰지 않는다 — grep -q가 먼저 끝나면 curl이 쓰기 오류로 죽고 pipefail이 실패로 본다
# (2026-09-14 첫 L1 시도가 지표가 있는데도 여기서 멈췄다).
curl -s --max-time 10 "$BASE_URL/actuator/prometheus" > "$TMP/prom.txt"
grep -q '^tomcat_threads_busy_threads' "$TMP/prom.txt" \
  || { echo "[중단] tomcat 스레드 지표가 없다 — compose.observe.yml로 backend를 띄웠나" >&2; exit 1; }
for a in prometheus alertmanager; do
  [ "$(docker inspect -f '{{.State.Running}}' "telemetry-$a" 2>/dev/null)" = true ] \
    && { echo "[중단] telemetry-$a 가 떠 있다 — 이 실험은 알림 경로를 띄우지 않는다(외부 발송 방지)" >&2; exit 1; }
done

evidence_init redis-load "bash load-test/redis-load/run_load_outage.sh $LEVEL"
E="$EVIDENCE_DIR"
{
  echo "level            : $LEVEL"
  echo "load             : /api 생성기 ${GENS}개 × ${RATE_PER_GEN} req/s(조회·refresh 번갈아) + liveness ${LIVE_RATE} req/s"
  echo "phases_s         : warm ${WARM} / baseline ${BASE} / outage ${OUTAGE} / after ${AFTER}"
  echo "plan_sha         : $(sha256sum load-test/redis-load/PLAN_20260914.md | cut -c1-16)"
  echo "tools_sha        : $(cat load-test/redis-load/gen.py load-test/redis-load/sampler.py load-test/redis-load/summarize.py load-test/redis-load/run_load_outage.sh | sha256sum | cut -c1-16)"
  echo "backend_image    : $(docker inspect "$BACKEND" --format '{{.Image}}')"
  echo "backend_started  : $(docker inspect "$BACKEND" --format '{{.State.StartedAt}}')"
  echo "backend_observe  : $(docker inspect "$BACKEND" --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -c '^SERVER_TOMCAT_MBEANREGISTRY_ENABLED=true') (SERVER_TOMCAT_MBEANREGISTRY_ENABLED=true 개수)"
  echo "generator_image  : $(docker image inspect "$IMAGE" --format '{{.Id}}')"
  echo "docker_host      : ncpu=$(docker info --format '{{.NCPU}}') mem=$(docker info --format '{{.MemTotal}}')"
  echo "other_containers : $(docker ps --format '{{.Names}}' | grep -v -E '^(telemetry-|redisload-)' | tr '\n' ' ')"
  echo "alerting         : prometheus·alertmanager 미기동(외부 발송 없음)"
} >> "$E/metadata.txt"
evidence_input level "$LEVEL"
evidence_input generators "$GENS"
evidence_input rate_per_generator "$RATE_PER_GEN"
evidence_input liveness_rate "$LIVE_RATE"

START_MS=$(( $(now_ms) + 20000 ))
START_S=$(awk -v m="$START_MS" 'BEGIN{printf "%.3f", m/1000}')
END_MS=$(( START_MS + DURATION * 1000 ))
{
  echo "level_n          : ${LEVEL#L}"
  echo "gens             : $GENS"
  echo "rate_per_gen     : $RATE_PER_GEN"
  echo "live_rate        : $LIVE_RATE"
  echo "run_start_ms     : $START_MS"
  echo "warm_end_ms      : $(( START_MS + WARM * 1000 ))"
} >> "$E/timeline.txt"

EW=$(cygpath -w "$PWD/$E")
TOOLW=$(cygpath -w "$PWD/load-test/redis-load")
run_ctr() {  # $1=컨테이너 이름, 나머지=python 인자
  local name=$1; shift
  MSYS_NO_PATHCONV=1 docker run -d --name "$name" --network "$NET" \
    -v "$TOOLW:/tool:ro" -v "$EW:/out" -e ADMIN_USERNAME -e ADMIN_PASSWORD \
    --entrypoint python "$IMAGE" -u "$@" >/dev/null
}

run_ctr redisload-sampler /tool/sampler.py --until "$(awk -v m="$END_MS" 'BEGIN{printf "%.3f", m/1000 + 5}')" --out /out/metrics.csv
for i in $(seq 1 "$GENS"); do
  run_ctr "redisload-gen-$i" /tool/gen.py --name "gen$i" --rate "$RATE_PER_GEN" --paths query,refresh \
    --start-at "$START_S" --duration "$DURATION" --out "/out/gen_$i.csv"
done
run_ctr redisload-live /tool/gen.py --name live --rate "$LIVE_RATE" --paths liveness \
  --start-at "$START_S" --duration "$DURATION" --out /out/live.csv

STAT_NAMES="$BACKEND $REDIS $(docker ps --format '{{.Names}}' | grep '^redisload-' | tr '\n' ' ')"
echo "ts_ms,name,cpu,mem" > "$E/dockerstats.csv"
(
  while [ ! -f "$TMP/stop_stats" ]; do
    t=$(now_ms)
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' $STAT_NAMES 2>/dev/null \
      | sed "s/^/$t,/" >> "$E/dockerstats.csv"
    sleep 1
  done
) &
STATS_PID=$!

# ── 구간 ──────────────────────────────────────────────────────
sleep_until $(( START_MS + (WARM + BASE) * 1000 ))
STOP_CMD=$(now_ms); docker stop "$REDIS" >/dev/null; STOP_DONE=$(now_ms)
echo "stop_cmd_ms      : $STOP_CMD" >> "$E/timeline.txt"
echo "stop_done_ms     : $STOP_DONE" >> "$E/timeline.txt"
sleep_until $(( STOP_DONE + OUTAGE * 1000 ))
START_CMD=$(now_ms); docker start "$REDIS" >/dev/null; START_DONE=$(now_ms)
echo "start_cmd_ms     : $START_CMD" >> "$E/timeline.txt"
echo "start_done_ms    : $START_DONE" >> "$E/timeline.txt"
echo "run_end_ms       : $END_MS" >> "$E/timeline.txt"

# 생성기·표본기가 끝날 때까지 (예정 종료 + 요청 timeout 여유)
for n in $(docker ps --format '{{.Names}}' | grep '^redisload-'); do
  docker wait "$n" >/dev/null 2>&1
done
touch "$TMP/stop_stats"; wait "$STATS_PID" 2>/dev/null

# ── 원본 수집 ─────────────────────────────────────────────────
for n in $(docker ps -a --format '{{.Names}}' | grep '^redisload-'); do
  echo "$n exit=$(docker inspect -f '{{.State.ExitCode}}' "$n")" >> "$E/generator_exit.txt"
  docker logs "$n" > "$E/$n.log" 2>&1
done
docker rm -f $(ctr_names) >/dev/null 2>&1
SINCE=$(date -u -d "@$(( STOP_CMD / 1000 - 5 ))" +%Y-%m-%dT%H:%M:%SZ)
docker logs -t --since "$SINCE" "$BACKEND" 2>&1 \
  | grep -E 'ConnectionWatchdog|ReconnectionHandler' \
  | awk '{ts=$1; $1=""; sub(/^ [0-9:]+ \[[^]]*\] \[[^]]*\] /,""); print ts" "$0}' > "$E/backend_reconnect.txt" || true

MSYS_NO_PATHCONV=1 docker run --rm --name redisload-summary -v "$TOOLW:/tool:ro" -v "$EW:/out" \
  --entrypoint python "$IMAGE" -u /tool/summarize.py /out > /dev/null 2> "$E/summarize.log"
cat "$E/summary.txt"

VERDICT="관찰(1회)"
grep -q '^다음 단계로 올리지 않는 조건: 해당 —' "$E/summary.txt" && VERDICT="관찰(1회) — 단계 증가 중단 조건 해당"
grep -q '^실행 유효성: 무효' "$E/summary.txt" && VERDICT="무효(생성기·표본 조건 미달) — summary.txt 참고"
evidence_finish "정상→Redis 중단→복구에서 경로별 응답·timeout과 서버 스레드·처리 중 요청·CPU·메모리의 회복을 1회 관측(PLAN_20260914.md §6)" "$VERDICT"
