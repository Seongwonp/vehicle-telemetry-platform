#!/bin/bash
# 기존 환경의 InfluxDB 버킷 보존 기간 변경 절차 검증 (docs/data-retention.md 5절).
#
# ## 왜 필요한가
#
# `DOCKER_INFLUXDB_INIT_RETENTION`은 **버킷을 처음 만들 때만** 적용된다. 즉 이미 돌고
# 있는 환경은 compose를 90일로 고쳐도 **여전히 무기한**이다. 그 환경을 바꾸려면
# `influx bucket update`를 직접 실행해야 하는데, 이 저장소 어디에도 그 절차가 없었다.
# GPS가 담긴 텔레메트리라 "언제 실제로 사라지는가"가 정책의 핵심이다.
#
# ## 이 스크립트가 답하는 질문 셋
#
#   1. 보존을 줄이면 초과분이 **즉시** 지워지는가, 아니면 나중에 지워지는가?
#   2. 삭제가 도는 동안 **쓰기가 막히는가**?
#   3. 되돌릴 수 있는가? (되돌리면 지워진 데이터가 돌아오는가?)
#
# 1번이 핵심이다. "90일로 설정했다"와 "90일 넘은 데이터가 없다"는 다른 말이고,
# 개인정보 처리 방침에는 후자가 필요하다.
#
# ## 방법
#
# 실제 기존 환경을 흉내내려고 **버킷을 무기한으로 되돌린 뒤** 과거 시각 데이터를
# 넣는다. 그리고 90일로 줄이면서 시각별로 행 수를 센다.
#
# 사용법: bash run_migration.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/retention-migration/_result.txt"
# 보존 검사 주기. InfluxDB 2.x 기본은 30분이라 그대로 두면 관측에 30분이 걸린다.
# 짧게 잡아 검사가 도는 순간을 볼 수 있게 한다 — 이 값 자체도 결과에 남긴다.
CHECK_INTERVAL="${CHECK_INTERVAL:-60s}"
OBSERVE_SEC="${OBSERVE_SEC:-420}"
SAMPLE_SEC="${SAMPLE_SEC:-20}"
# 주입할 line protocol을 담을 임시 파일. 호스트에만 있고 컨테이너로 복사하지 않는다.
SEED="${TMPDIR:-/tmp}/retention_seed_$$.lp"

. load-test/lib/evidence.sh
evidence_init "retention-migration" "bash load-test/retention-migration/run_migration.sh"
evidence_input retention_check_interval "$CHECK_INTERVAL"
evidence_input observe_sec "$OBSERVE_SEC"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }

# .env에서 InfluxDB 접속 정보를 읽는다. 토큰은 **증거에 남기지 않는다.**
ENVFILE=".env"
[ -f "$ENVFILE" ] || { echo ".env가 없다"; exit 1; }
TOKEN=$(grep -E '^INFLUXDB_TOKEN=' "$ENVFILE" | head -1 | cut -d= -f2-)
ORG=$(grep -E '^INFLUXDB_ORG=' "$ENVFILE" | head -1 | cut -d= -f2-)
BUCKET=$(grep -E '^INFLUXDB_BUCKET=' "$ENVFILE" | head -1 | cut -d= -f2-)
[ -n "$TOKEN" ] && [ -n "$ORG" ] && [ -n "$BUCKET" ] || { echo ".env에 INFLUXDB_TOKEN/ORG/BUCKET이 필요하다"; exit 1; }
evidence_input bucket "$BUCKET"

ix() { docker exec telemetry-influxdb influx "$@" --host http://localhost:8086 --token "$TOKEN" --org "$ORG" 2>&1 | tr -d '\r'; }

# **`bucket update`는 `--org`를 받지 않는다.** 그리고 `--name`은 대상을 고르는 게 아니라
# **새 이름을 설정하는** 옵션이라, 대상 지정은 `--id`뿐이다. 처음에 `ix bucket update
# --name ... --retention 0`으로 짰다가 `flag provided but not defined: -org`로 죽었고,
# `set -o pipefail` 때문에 **로그 한 줄 없이** 끝났다. 그래서 전용 함수로 분리한다.
ix_update_retention() {  # $1 = 보존 기간 (0 = 무기한)
  docker exec telemetry-influxdb influx bucket update \
    --id "$BUCKET_ID" --retention "$1" \
    --host http://localhost:8086 --token "$TOKEN" 2>&1 | tr -d '\r'
}

# 버킷 ID를 이름으로 찾는다. 위 이유로 ID가 없으면 보존을 못 바꾼다.
bucket_id_of() {  # $1 = 버킷 이름
  ix bucket list | awk -v n="$1" '$2==n {print $1; exit}'
}

# 현재 보존 기간. `bucket list`의 3번째 컬럼이다(4번째는 shard group duration이라
# 처음에 잘못 읽어 "24h"가 보존인 줄 알았다).
retention_of() {  # $1 = 버킷 이름
  ix bucket list | awk -v n="$1" '$2==n {print $3; exit}'
}

# 특정 시각대의 행 수. -90d보다 오래된 것과 최근 것을 나눠 센다.
count_rows() {  # $1 = start, $2 = stop
  docker exec telemetry-influxdb influx query --host http://localhost:8086 --token "$TOKEN" --org "$ORG" --raw \
    "from(bucket:\"$BUCKET\") |> range(start: $1, stop: $2) |> filter(fn:(r) => r._measurement == \"vehicle_telemetry\" and r._field == \"speed\") |> group() |> count()" 2>/dev/null \
    | tr -d '\r' | awk -F, '$0 ~ /^,/ && $NF ~ /^[0-9]+$/ {v=$NF} END{print v+0}'
}

: > "$OUT"
log "=== InfluxDB 보존 기간 마이그레이션 검증 ==="

# ── 1. 스택 (볼륨 유지하지 않고 새로 — 재현 가능해야 한다) ────
# **보존 검사 주기를 실험용으로 낮춘다.** InfluxDB 2.x 기본은 30분이라 그대로 두면
# 삭제되는 순간을 보는 데 30분을 기다려야 한다. 낮춰서 "즉시가 아니라 주기적으로
# 지워진다"는 구조를 확인하고, **운영 기본값이 30분이라는 사실은 결과에 그대로 남긴다.**
# 즉 이 실험이 재는 것은 "삭제까지 몇 초"가 아니라 "설정 변경과 삭제가 분리되어 있다"다.
OVERRIDE=/tmp/influx-retention-override.yml
cat > "$OVERRIDE" <<YML
services:
  influxdb:
    environment:
      INFLUXD_STORAGE_RETENTION_CHECK_INTERVAL: "${CHECK_INTERVAL}"
YML
COMPOSE="$COMPOSE -f $OVERRIDE"

$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d influxdb >/dev/null 2>&1
wait_until 300 "InfluxDB healthy" bash -c '[ "$(docker inspect telemetry-influxdb --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'

BUCKET_ID=$(bucket_id_of "$BUCKET")
[ -n "$BUCKET_ID" ] || { log "** 버킷 '$BUCKET'을 못 찾았다"; exit 1; }
INIT_RET=$(retention_of "$BUCKET")
log "compose가 만든 초기 보존: $INIT_RET (버킷 id $BUCKET_ID)"
evidence_count initial_retention_h "$INIT_RET"

# ── 2. 기존 환경 흉내: 보존을 무기한으로 되돌린다 ──────────────
# 실제 "이미 돌고 있던 환경"이 이 상태다. 여기서 출발해야 절차가 검증된다.
log "보존을 무기한(0)으로 되돌린다 — 기존 환경 재현"
ix_update_retention 0 >/dev/null
log "  현재: $(retention_of "$BUCKET")"

# ── 3. 90일보다 오래된 데이터를 넣는다 ─────────────────────────
# 100일 전, 95일 전, 어제. 앞 둘은 90일 보존이 걸리면 사라져야 한다.
now=$(date +%s)
old1=$(( (now - 100*86400) * 1000000000 ))
old2=$(( (now -  95*86400) * 1000000000 ))
recent=$(( (now - 86400) * 1000000000 ))
{
  for i in $(seq 1 200); do echo "vehicle_telemetry,vehicle_id=OLD-100D speed=1.0,lat=37.1,lng=127.1 $((old1 + i))"; done
  for i in $(seq 1 200); do echo "vehicle_telemetry,vehicle_id=OLD-95D speed=2.0,lat=37.2,lng=127.2 $((old2 + i))"; done
  for i in $(seq 1 200); do echo "vehicle_telemetry,vehicle_id=RECENT speed=3.0,lat=37.3,lng=127.3 $((recent + i))"; done
} > "$SEED"
# **파일을 컨테이너로 복사하지 않고 stdin으로 넣는다.** 처음엔 `docker cp` + `--file`로
# 짰는데 Git Bash의 경로 변환에 걸렸다 — `/tmp/seed.lp`가 컨테이너 안이 아니라
# `C:/Users/.../Temp/seed.lp`로 바뀌어 `failed to open`으로 죽는다. `MSYS_NO_PATHCONV=1`을
# 붙이면 이번엔 **호스트 쪽 경로**까지 변환이 꺼져서 `D:\tmp\...`를 찾는다.
# 한쪽만 끄는 방법이 없으니 파일 경로를 아예 주고받지 않는 쪽이 맞다.
# **`| head -N`을 붙이지 마라.** head가 먼저 끝나면 SIGPIPE가 나고 `set -o pipefail`이
# 그걸 실패로 읽어 스크립트가 죽는다. 실제로 그렇게 죽었는데, **쓰기는 이미 성공한
# 뒤라** 데이터는 들어가 있고 로그만 멈춰서 원인을 찾는 데 시간이 걸렸다.
WRITE_OUT=$(docker exec -i telemetry-influxdb influx write \
  --bucket "$BUCKET" --precision ns \
  --host http://localhost:8086 --token "$TOKEN" --org "$ORG" < "$SEED" 2>&1 | tr -d '\r') || true
[ -n "$WRITE_OUT" ] && log "  write 출력: $(echo "$WRITE_OUT" | tail -2)"
rm -f "$SEED"

OLD_BEFORE=$(count_rows -120d -91d)
NEW_BEFORE=$(count_rows -91d "now()")
log "주입 후 — 90일 초과: ${OLD_BEFORE}행 / 최근: ${NEW_BEFORE}행"
evidence_count old_rows_before "$OLD_BEFORE"
evidence_count recent_rows_before "$NEW_BEFORE"

if [ "$OLD_BEFORE" -eq 0 ]; then
  log "** 과거 데이터가 안 들어갔다 — 보존이 무기한이 아닐 수 있다. 중단"
  evidence_finish "과거 데이터 주입" "실패 — 90일 초과 행이 0"
  exit 1
fi

# ── 4. 보존을 90일로 줄인다 ────────────────────────────────────
log "--- influx bucket update --retention 90d 실행 ---"
T_UPDATE=$(date +%s)
UPDATE_OUT=$(ix_update_retention 90d)
log "  결과: $(echo "$UPDATE_OUT" | tail -1)"
log "  변경 직후 90일 초과 행: $(count_rows -120d -91d)  ← 즉시 지워지는지 여기서 갈린다"
evidence_count old_rows_immediately_after "$(count_rows -120d -91d)"

# ── 5. 언제 실제로 사라지는지 관측 ─────────────────────────────
# InfluxDB 2.x는 보존 정책을 **검사 주기마다** 적용한다(기본 30분). 즉 설정 변경과
# 실제 삭제 사이에 시차가 있다. 그 시차가 정책 문서에 필요한 값이다.
TL="$EVIDENCE_DIR/retention_timeline.csv"
echo "t_sec_since_update,old_rows,recent_rows,write_ok" > "$TL"
DELETED_AT=""
t0=$(date +%s)
while [ $(( $(date +%s) - t0 )) -lt "$OBSERVE_SEC" ]; do
  el=$(( $(date +%s) - T_UPDATE ))
  o=$(count_rows -120d -91d); r=$(count_rows -91d "now()")
  # 삭제가 도는 중에도 쓰기가 되는지 — 정책 적용이 서비스를 멈추는지 본다.
  wnow=$(( $(date +%s) * 1000000000 ))
  if ix write --bucket "$BUCKET" --precision ns "vehicle_telemetry,vehicle_id=PROBE speed=9.0 $wnow" >/dev/null 2>&1; then w=ok; else w=fail; fi
  echo "$el,$o,$r,$w" >> "$TL"
  log "  t+${el}s 90일초과=${o}행 최근=${r}행 쓰기=${w}"
  if [ -z "$DELETED_AT" ] && [ "$o" -eq 0 ]; then
    DELETED_AT="$el"
    log "  ** 90일 초과 데이터가 사라졌다: 설정 변경 후 ${el}초"
  fi
  sleep "$SAMPLE_SEC"
done

OLD_AFTER=$(count_rows -120d -91d)
NEW_AFTER=$(count_rows -91d "now()")
evidence_count old_rows_after "$OLD_AFTER"
evidence_count recent_rows_after "$NEW_AFTER"
evidence_count deleted_after_sec "${DELETED_AT:--1}"
WRITE_FAILS=$(awk -F, 'NR>1 && $4=="fail"{n++} END{print n+0}' "$TL")
evidence_count write_failures_during "$WRITE_FAILS"

# ── 6. 되돌리면 돌아오는가 ─────────────────────────────────────
log "--- 되돌리기: --retention 0 (무기한) ---"
ix_update_retention 0 >/dev/null
sleep 10
BACK=$(count_rows -120d -91d)
log "  되돌린 뒤 90일 초과 행: ${BACK}행"
evidence_count old_rows_after_revert "$BACK"

# ── 7. 정리 ────────────────────────────────────────────────────
log ""
log "=== 결과 ==="
log "90일 초과 행: 주입 ${OLD_BEFORE} → 변경 직후 관측 → ${OBSERVE_SEC}초 뒤 ${OLD_AFTER}"
log "최근 행:      주입 ${NEW_BEFORE} → ${NEW_AFTER} (지워지면 안 된다)"
log "실제 삭제 시점: ${DELETED_AT:-관측 창 안에서 안 지워짐}초"
log "삭제 중 쓰기 실패: ${WRITE_FAILS}건"
log "되돌린 뒤 복구: ${BACK}행 (0이면 복구 불가)"

evidence_capture_file "$OUT" console.log
CRIT="보존 축소가 언제 실제 삭제로 이어지는지, 그동안 쓰기가 되는지, 되돌릴 수 있는지"
VERDICT="관찰 — 삭제 ${DELETED_AT:-미관측}초 후, 쓰기 실패 ${WRITE_FAILS}건, 되돌린 뒤 ${BACK}행"
log "판정: $VERDICT"
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
