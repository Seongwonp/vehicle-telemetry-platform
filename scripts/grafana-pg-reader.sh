#!/usr/bin/env sh
# Grafana 읽기 전용 PostgreSQL 역할(grafana_reader)을 만들거나 비밀번호·권한을 맞춘다. 멱등.
# 기존 볼륨에도 동작한다(init 스크립트가 아니라 실행 중인 컨테이너에 접속한다).
#
#   GRAFANA_PG_READER_PASSWORD=... sh scripts/grafana-pg-reader.sh
#
# 관리자 접속은 컨테이너 안의 POSTGRES_USER/POSTGRES_DB를 쓴다(호스트에 비밀번호를 꺼내지 않는다).
# 비밀번호는 명령줄 인자가 아니라 환경변수로만 전달한다(docker exec -e NAME, psql \getenv).
set -eu
: "${GRAFANA_PG_READER_PASSWORD:?GRAFANA_PG_READER_PASSWORD 가 비어 있다 (.env 또는 셸 환경)}"
CONTAINER="${POSTGRES_CONTAINER:-telemetry-postgres}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
docker exec -i -e GRAFANA_PG_READER_PASSWORD "$CONTAINER" \
  sh -c 'psql -X -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f -' \
  < "$DIR/monitoring/postgres/grafana_reader.sql"
echo "grafana_reader: OK"
