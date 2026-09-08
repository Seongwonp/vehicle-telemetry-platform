#!/bin/bash
# 저장 전용 백엔드 인스턴스를 띄우고 내린다 (compose profile `scale`).
#
# ## 왜 스크립트가 필요한가
#
# compose만으로도 인스턴스는 뜬다. 스크립트가 하는 일은 **인스턴스와 관측을 같이
# 움직이는 것**이다 — Prometheus 스크레이프 대상 파일을 함께 만들고 지운다.
# 2026-09-06에 저장 경로를 복제해놓고 지표는 주 backend 것만 보고 있었는데,
# 그때 "멤버가 늘었는데 처리량이 그대로"인 이유를 한참 못 찾았다.
#
# ## 쓰는 법
#
#   bash scripts/scale-storage.sh up 2      # 저장 인스턴스 2개
#   bash scripts/scale-storage.sh status
#   bash scripts/scale-storage.sh down
#
# 평문(dev) 스택이면 COMPOSE_FILES를 넘긴다:
#   COMPOSE_FILES="-f docker-compose.yml -f docker-compose.dev.yml" bash scripts/scale-storage.sh up 2
#
# ## 먼저 알아야 할 것 — 파티션을 안 늘리면 이득이 없다
#
# `concurrency: 3`이라 backend 하나가 이미 파티션 3개를 다 가져간다. 늘어난 컨슈머는
# **유휴 멤버로만 붙는다.** 파티션을 먼저 늘려야 한다(늘린 파티션은 되돌릴 수 없다):
#
#   docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
#     --alter --topic vehicle-telemetry --partitions 12
#
# 그리고 늘려도 처리량이 비례하지 않는다 — 병목은 CPU가 아니라 InfluxDB 쓰기 지연이고,
# 이득의 배수는 부하 조건의 함수다(load-test/storage-scale/RESULT_20260907_repeat3.md).
set -uo pipefail
cd "$(dirname "$0")/.."

MAX=3   # docker-compose.yml에 선언된 backend-storage-* 개수
TARGETS_FILE="monitoring/prometheus/targets/backend-storage.json"
COMPOSE="docker compose ${COMPOSE_FILES:--f docker-compose.yml} --profile scale"

usage() { echo "사용법: bash scripts/scale-storage.sh {up <1-$MAX>|down|status}" >&2; exit 2; }

write_targets() {  # $1 = 개수
  local n="$1" i entries=""
  for i in $(seq 1 "$n"); do
    entries="$entries$([ -n "$entries" ] && echo ,)\"backend-storage-$i:8080\""
  done
  # Prometheus는 이 파일을 refresh_interval(30초)마다 다시 읽는다. 재시작 불필요.
  printf '[{"targets": [%s], "labels": {"app": "vehicle-telemetry-backend", "role": "storage"}}]\n' \
    "$entries" > "$TARGETS_FILE"
  echo "[scale] 스크레이프 대상 $n개 기록: $TARGETS_FILE"
}

case "${1:-}" in
  up)
    N="${2:-}"
    case "$N" in ''|*[!0-9]*) usage;; esac
    [ "$N" -ge 1 ] && [ "$N" -le "$MAX" ] || {
      echo "[scale] 1~$MAX 사이여야 한다. 더 필요하면 docker-compose.yml에 backend-storage-$((MAX+1))을 추가하라 (앵커를 쓰므로 8줄이다)." >&2
      exit 2; }
    SERVICES=""
    for i in $(seq 1 "$N"); do SERVICES="$SERVICES backend-storage-$i"; done
    # shellcheck disable=SC2086
    $COMPOSE up -d $SERVICES || exit 1
    write_targets "$N"
    echo "[scale] 리밸런싱이 끝날 때까지 기다렸다가 확인하라(보통 30~60초):"
    echo "        bash scripts/scale-storage.sh status"
    ;;
  down)
    SERVICES=""
    for i in $(seq 1 "$MAX"); do SERVICES="$SERVICES backend-storage-$i"; done
    # shellcheck disable=SC2086
    $COMPOSE rm -sf $SERVICES >/dev/null 2>&1
    rm -f "$TARGETS_FILE"
    echo "[scale] 저장 인스턴스 정리, 스크레이프 대상 파일 제거."
    ;;
  status)
    echo "=== 컨테이너 ==="
    docker ps --filter 'name=telemetry-backend' --format '{{.Names}}\t{{.Status}}'
    echo
    echo "=== telemetry-storage-group 멤버 (정적 id) ==="
    # `--describe --members`는 파티션을 못 받은 유휴 멤버까지 보여준다. 파티션만 세면
    # "멤버는 늘었는데 일은 안 는다"는 상태가 안 보인다 — 2026-09-06에 정확히 그 상태였다.
    docker exec telemetry-kafka kafka-consumer-groups \
      --bootstrap-server localhost:29092 --describe --members --group telemetry-storage-group 2>/dev/null \
      | awk '$1=="GROUP" || NF<5 {next} {
          # 컬럼: GROUP CONSUMER-ID GROUP-INSTANCE-ID HOST CLIENT-ID #PARTITIONS
          # 헤더 앞에 빈 줄이 하나 붙어 나온다 — NR>1로 거르면 헤더가 집계에 섞인다
          # (처음에 그렇게 짜서 멤버가 10개, 호스트가 10개로 나왔다).
          members++; assigned += $NF; ids[$3]=1; hosts[$4]=1
        } END {
          for (i in ids) n++; for (h in hosts) m++;
          printf "멤버(컨슈머 스레드): %d\n정적 id 종류      : %d  (멤버 수와 같아야 한다 — 겹치면 서로 fencing 한다)\n인스턴스(호스트)  : %d\n할당된 파티션 합  : %d\n", members, n+0, m+0, assigned
          if (members > 0 && assigned < members) printf "→ 유휴 멤버 %d개. 파티션이 모자란다 — 늘리지 않으면 인스턴스를 늘려도 이득이 없다.\n", members - assigned
        }'
    echo
    echo "=== 정적 멤버 충돌(fencing) / MQTT 세션 끊김 — 둘 다 0이어야 한다 ==="
    for c in $(docker ps --filter 'name=telemetry-backend' --format '{{.Names}}'); do
      printf '%s: fencing %s건, MQTT 연결 끊김 %s건\n' "$c" \
        "$(docker logs "$c" 2>&1 | grep -c 'FencedInstanceIdException')" \
        "$(docker logs "$c" 2>&1 | grep -c 'Connection lost')"
    done
    echo
    echo "=== Prometheus 스크레이프 대상 ==="
    if [ -f "$TARGETS_FILE" ]; then cat "$TARGETS_FILE"; else echo "(대상 파일 없음 — 저장 인스턴스 미기동)"; fi
    # **"응답 없음"과 "아직 대상이 없음"을 구분한다.** 처음엔 curl|grep 파이프라인에
    # `|| echo "(Prometheus 응답 없음)"`를 붙였는데, grep이 0건이면 파이프라인이 실패로
    # 끝나서 **Prometheus가 멀쩡한데도 "응답 없음"이 찍혔다**(2026-09-08에 실제로 그랬다).
    # 대상 파일을 쓴 직후에는 file_sd refresh_interval(30초)만큼 비어 있는 게 정상이다.
    # 정상을 장애처럼 보이게 하는 메시지는 알림 피로와 같은 종류의 실수다.
    if ! curl -s -o /dev/null --max-time 5 http://localhost:9090/-/ready 2>/dev/null; then
      echo "  (Prometheus에 못 붙었다 — 컨테이너가 떠 있는지 확인하라)"
    else
      up_lines=$(curl -s --get --max-time 5 'http://localhost:9090/api/v1/query' \
                   --data-urlencode 'query=up{job="telemetry-backend-storage"}' 2>/dev/null \
                 | tr '{},' '\n\n\n' | grep -E '"instance":"[^"]*"' -o | sed 's/.*:"//;s/"//')
      if [ -z "$up_lines" ]; then
        echo "  대상 0개 — file_sd 갱신 주기(30초)를 아직 안 지났을 수 있다. 잠시 뒤 다시 보라."
      else
        curl -s --get --max-time 5 'http://localhost:9090/api/v1/query' \
             --data-urlencode 'query=up{job="telemetry-backend-storage"}' 2>/dev/null \
          | tr '{},' '\n\n\n' | grep -E '"instance":|^"1"|^"0"' | sed 's/^/  /'
      fi
    fi
    ;;
  *) usage;;
esac
