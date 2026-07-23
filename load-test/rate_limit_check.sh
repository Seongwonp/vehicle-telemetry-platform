#!/bin/bash
# Rate Limit 정확성 시나리오 — "정확히 rate-limit.requests-per-minute(기본 60)에서 막히는가"
# 성능 테스트가 아니라 동작 정확성 확인이라 k6 대신 순차 curl로 확인한다.
#
# 사용법: RATE_LIMIT_RPM을 60(기본값)으로 되돌린 뒤 백엔드를 재기동하고 실행.
#   ./load-test/rate_limit_check.sh <JWT_TOKEN>

set -euo pipefail

TOKEN="${1:?사용법: rate_limit_check.sh <JWT_TOKEN>}"
BASE="${BASE_URL:-http://localhost:8080}"
VEHICLE_ID="${VEHICLE_ID:-SIM-001}"
REQUESTS="${REQUESTS:-65}"
EXPECTED_FIRST_429="${EXPECTED_FIRST_429:-61}"

echo "분당 ${REQUESTS}회 순차 요청 — ${BASE}/api/vehicles/${VEHICLE_ID}/telemetry/latest"
echo "요청# | HTTP status"

FIRST_429=""
for i in $(seq 1 "$REQUESTS"); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "Authorization: Bearer ${TOKEN}" \
    "${BASE}/api/vehicles/${VEHICLE_ID}/telemetry/latest")
  echo "$i | $STATUS"
  if [ "$STATUS" = "429" ] && [ -z "$FIRST_429" ]; then
    FIRST_429="$i"
  fi
done

echo "---"
if [ "$FIRST_429" = "$EXPECTED_FIRST_429" ]; then
  echo "결과: 예상대로 ${FIRST_429}번째 요청부터 429 시작"
  exit 0
fi

if [ -n "$FIRST_429" ]; then
  echo "실패: ${FIRST_429}번째 요청부터 429 시작 (예상: ${EXPECTED_FIRST_429}번째)" >&2
else
  echo "실패: ${REQUESTS}회 동안 429 없음 (예상: ${EXPECTED_FIRST_429}번째)" >&2
fi
exit 1
