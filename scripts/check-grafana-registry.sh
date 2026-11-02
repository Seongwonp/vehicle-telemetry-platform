#!/usr/bin/env sh
# Grafana "등록 차량" 변수 경로 점검. 설정이 틀리면 대시보드가 조용히 "No data"로 비는 대신
# 여기서 0이 아닌 종료 코드와 이유를 낸다. 값(비밀번호·관리자 자격)은 출력하지 않는다.
#
#   sh scripts/check-grafana-registry.sh        # 통과 0 / 실패 1
#
# 1) GRAFANA_PG_READER_PASSWORD가 셸 환경 또는 .env에 비어 있지 않게 있다(셸 환경이 우선 — compose와 같다).
#    .env 값을 감싼 따옴표 한 쌍("…"/'…')은 compose처럼 벗긴다(`""`는 빈 값으로 FAIL [1])
# 2) 그 비밀번호로 grafana_reader가 PostgreSQL에 비밀번호 인증으로 로그인해 변수 쿼리를 실행하고 1행 이상 받는다
#    (컨테이너 안 127.0.0.1은 pg_hba가 trust라 비밀번호를 안 본다 → 컨테이너의 네트워크 IP로 접속한다)
# 3) 실행 중인 Grafana가 datasource(telemetry-pg-registry) health OK를 돌려준다
#    (Grafana가 .env 변경 전에 만들어져 옛 값을 들고 있으면 여기서 걸린다)
#
# 환경변수: ENV_FILE(기본 ./.env), POSTGRES_CONTAINER, GRAFANA_CONTAINER, GRAFANA_DS_UID
set -u
DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$DIR/.env}"
PG="${POSTGRES_CONTAINER:-telemetry-postgres}"
GF="${GRAFANA_CONTAINER:-telemetry-grafana}"
DS_UID="${GRAFANA_DS_UID:-telemetry-pg-registry}"
VAR_QUERY='SELECT vehicle_id FROM vehicles WHERE active ORDER BY vehicle_id'

fail() { echo "FAIL [$1] $2" >&2; echo "→ 대시보드 차량 변수가 비고 텔레메트리 패널 5개가 'No data'로 보인다. 절차: .env.example의 GRAFANA_PG_READER_PASSWORD 주석, docs/verification/2026-10-10-grafana-vehicle-filter.md" >&2; exit 1; }

# 1) 변수 존재(값은 읽기만 하고 출력하지 않는다)
if [ -z "${GRAFANA_PG_READER_PASSWORD:-}" ] && [ -f "$ENV_FILE" ]; then
  GRAFANA_PG_READER_PASSWORD="$(grep '^GRAFANA_PG_READER_PASSWORD=' "$ENV_FILE" | tail -n 1 | cut -d= -f2- | tr -d '\r')"
  # compose처럼 값을 감싼 따옴표 한 쌍은 벗긴다("…" 또는 '…'). 짝이 안 맞으면 그대로 두고 2)에서 인증 실패로 드러난다.
  case "$GRAFANA_PG_READER_PASSWORD" in
    \"*\") GRAFANA_PG_READER_PASSWORD="${GRAFANA_PG_READER_PASSWORD#\"}"; GRAFANA_PG_READER_PASSWORD="${GRAFANA_PG_READER_PASSWORD%\"}" ;;
    \'*\') GRAFANA_PG_READER_PASSWORD="${GRAFANA_PG_READER_PASSWORD#\'}"; GRAFANA_PG_READER_PASSWORD="${GRAFANA_PG_READER_PASSWORD%\'}" ;;
  esac
fi
[ -n "${GRAFANA_PG_READER_PASSWORD:-}" ] || fail 1 "GRAFANA_PG_READER_PASSWORD가 셸 환경과 $ENV_FILE 어디에도 없거나 비어 있다."
export GRAFANA_PG_READER_PASSWORD
echo "OK   [1] GRAFANA_PG_READER_PASSWORD 설정됨(값 미출력)"

# 2) 역할 로그인 + 변수 쿼리. 비밀번호는 docker exec -e 로만 넘긴다(명령줄 인자 아님).
out="$(docker exec -e GRAFANA_PG_READER_PASSWORD "$PG" sh -c \
  'PGPASSWORD="$GRAFANA_PG_READER_PASSWORD" PGCONNECT_TIMEOUT=5 psql -X -tA -h "$(hostname -i | cut -d" " -f1)" -U grafana_reader -d "$POSTGRES_DB" -c "$1"' \
  sh "$VAR_QUERY" 2>&1)"
rc=$?
if [ $rc -ne 0 ]; then
  case "$out" in
    *"password authentication failed"*) fail 2 "grafana_reader 비밀번호 인증 실패 — .env 값과 PostgreSQL 역할 비밀번호가 다르다. sh scripts/grafana-pg-reader.sh 로 맞춘다." ;;
    *"does not exist"*) fail 2 "grafana_reader 역할 또는 DB가 없다 — sh scripts/grafana-pg-reader.sh 를 실행한다. ($(echo "$out" | head -n 1))" ;;
    *) fail 2 "grafana_reader 접속/쿼리 실패: $(echo "$out" | head -n 1)" ;;
  esac
fi
n="$(printf '%s\n' "$out" | grep -c .)"
[ "$n" -ge 1 ] || fail 2 "변수 쿼리가 0행이다 — vehicles에 active=true 차량이 없다(대시보드가 비는 것이 정상인 상태)."
echo "OK   [2] grafana_reader 비밀번호 로그인 + 변수 쿼리 ${n}행: $(printf '%s' "$out" | tr '\n' ' ')"

# 3) Grafana datasource health. 관리자 자격은 Grafana 컨테이너 안 env에서만 쓴다.
#    curl -u 로 넘기면 자격이 curl의 argv(컨테이너 안 ps·/proc/<pid>/cmdline)에 보인다 →
#    셸 내장 printf로 curl 설정(user = "…")을 만들어 `-K -`(stdin)로 넘긴다. sed는 stdin으로만 받아 \ 와 " 를 이스케이프한다.
#    컨테이너 안 스크립트는 heredoc(stdin)으로 보낸다 — Windows에서 docker.exe 인자로 넘기면 백슬래시가 줄어 이스케이프가 깨졌다.
h="$(docker exec -i "$GF" sh -s "$DS_UID" 2>&1 <<'GFEOF'
{ printf 'user = "'; printf '%s:%s' "$GF_SECURITY_ADMIN_USER" "$GF_SECURITY_ADMIN_PASSWORD" | sed -e 's/[\\"]/\\&/g'; printf '"\n'; } \
  | curl -s -m 10 -K - "http://localhost:3000/api/datasources/uid/$1/health"
GFEOF
)" || fail 3 "Grafana 컨테이너($GF)에 접속하지 못했다."
case "$h" in
  *'"status":"OK"'*) echo "OK   [3] Grafana datasource $DS_UID health OK" ;;
  *) fail 3 "Grafana datasource $DS_UID health 실패: $(echo "$h" | tr -d '\n' | cut -c1-200) — .env를 바꿨다면 grafana를 다시 만든다(docker compose ... up -d --no-deps grafana)." ;;
esac
echo "PASS Grafana 등록 차량 변수 경로 정상"
