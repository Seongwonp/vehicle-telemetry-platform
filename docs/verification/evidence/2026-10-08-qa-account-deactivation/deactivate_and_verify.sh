#!/usr/bin/env bash
# QA 계정 비활성화·refresh 폐기·401 확인. 비밀값은 변수에만 두고 출력하지 않는다.
# 입력: QA_ENV_FILE(계정 값 파일, 저장소 밖). 출력: 요청별 HTTP 상태 코드만.
set -u
API=http://localhost:8080
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
json() { py -3.11 -c "import sys,json; print(json.load(sys.stdin).get(sys.argv[1],''))" "$1"; }
set -a; . "$QA_ENV_FILE"; set +a
QA_USER_CURRENT="$QA_USER_PASSWORD"; [ "${QA_USER_PASSWORD_CHANGED:-0}" = 1 ] && QA_USER_CURRENT="$QA_USER_NEW_PASSWORD"
declare -A AT RT PW
PW[qa-admin]="$QA_ADMIN_PASSWORD"; PW[qa-user]="$QA_USER_CURRENT"
echo "== 1. 비활성화 전 로그인(토큰 확보)"
for u in qa-admin qa-user; do
  body=$(curl -s -X POST $API/api/auth/login -H 'Content-Type: application/json' -d "{\"username\":\"$u\",\"password\":\"${PW[$u]}\"}")
  AT[$u]=$(echo "$body" | json accessToken); RT[$u]=$(echo "$body" | json refreshToken)
  echo "$u login_before access_token_issued=$([ -n "${AT[$u]}" ] && echo yes || echo no) refresh_token_issued=$([ -n "${RT[$u]}" ] && echo yes || echo no)"
  echo "$u GET /api/auth/me (before) -> $(code $API/api/auth/me -H "Authorization: Bearer ${AT[$u]}")"
done
echo "== 2. DB 비활성화"
docker exec telemetry-postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "UPDATE users SET active = FALSE WHERE username IN ('"'"'qa-admin'"'"','"'"'qa-user'"'"') RETURNING username, role, active;"'
echo "== 3. Redis refresh token 폐기(사용자 값이 일치하는 refresh_token:* 키)"
RPW=$(docker exec telemetry-backend printenv REDIS_PASSWORD)
removed=0
for key in $(docker exec -e REDISCLI_AUTH="$RPW" telemetry-redis redis-cli --scan --pattern 'refresh_token:*'); do
  v=$(docker exec -e REDISCLI_AUTH="$RPW" telemetry-redis redis-cli GET "$key")
  if [ "$v" = qa-admin ] || [ "$v" = qa-user ]; then
    docker exec -e REDISCLI_AUTH="$RPW" telemetry-redis redis-cli DEL "$key" >/dev/null && removed=$((removed+1)) && echo "deleted key for $v"
  fi
done
echo "removed=$removed"
left=0
for key in $(docker exec -e REDISCLI_AUTH="$RPW" telemetry-redis redis-cli --scan --pattern 'refresh_token:*'); do
  v=$(docker exec -e REDISCLI_AUTH="$RPW" telemetry-redis redis-cli GET "$key"); { [ "$v" = qa-admin ] || [ "$v" = qa-user ]; } && left=$((left+1))
done
echo "remaining_qa_refresh_keys=$left"
echo "== 4. 확인"
for u in qa-admin qa-user; do
  echo "$u GET /api/auth/me (old access token) -> $(code $API/api/auth/me -H "Authorization: Bearer ${AT[$u]}")"
  echo "$u POST /api/auth/refresh (old refresh token) -> $(code -X POST $API/api/auth/refresh -H 'Content-Type: application/json' -d "{\"refreshToken\":\"${RT[$u]}\"}")"
  echo "$u POST /api/auth/login -> $(code -X POST $API/api/auth/login -H 'Content-Type: application/json' -d "{\"username\":\"$u\",\"password\":\"${PW[$u]}\"}")"
done
docker exec telemetry-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "SELECT u.username, u.role, u.active FROM users u ORDER BY u.username; SELECT v.vehicle_id, u.username AS owner, v.active FROM vehicles v LEFT JOIN users u ON u.id = v.owner_id WHERE v.vehicle_id = '"'"'SIM-002'"'"';"'
