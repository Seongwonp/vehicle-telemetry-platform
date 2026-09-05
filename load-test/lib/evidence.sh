#!/bin/bash
# 실험 원본 증거 보존 (docs/evidence-policy.md P0-1).
#
# ## 왜 필요한가
#
# 2026-09-05에 장애 실험을 여덟 번 돌리고 결과 문서를 썼는데, 커밋 직전마다
# 스크립트 출력(`_result_*.txt`)을 "임시 파일"로 보고 지웠다. 그래서 **그 문서들의
# 수치를 원본에서 다시 계산할 수 없다.** 판단은 남았는데 근거가 없다.
#
# 요약 문서는 "무엇을 결론지었나"를 남기고, 여기 저장하는 것은 "그 결론을 어떻게
# 계산했나"를 남긴다. 둘 중 하나만 있으면 재현이 안 된다.
#
# ## 쓰는 법
#
#   source load-test/lib/evidence.sh
#   evidence_init <scenario-dir> "<실행 명령 원문>"
#   evidence_input vehicles 200          # 입력 조건
#   evidence_count kafka_topic 162402    # 집계 결과
#   evidence_capture_prometheus
#   evidence_capture_kafka_groups telemetry-storage-group
#   evidence_capture_file "$OUT" console.log
#   evidence_finish "성공 기준 문장" "판정"
#
# 저장 위치: `load-test/<scenario>/evidence/<run-id>/`
#
# ## 담지 않는 것
#
# `.env` 값, 토큰, 인증서, 비밀번호는 절대 넣지 않는다. Prometheus 스냅샷도
# `telemetry_*`와 `kafka_*` 계열만 걸러서 저장한다 — 전체를 뜨면 시스템 정보와
# 환경변수가 섞여 들어올 수 있다.
#
# 대용량 로그도 넣지 않는다. 컨테이너 로그는 필요한 구간만 잘라서 저장한다.

# shellcheck disable=SC2034

EVIDENCE_DIR=""
EVIDENCE_STARTED_AT=""

# 조건이 참이 될 때까지 기다리되 **반드시 포기한다.**
#
# 예전 스크립트의 대기 루프는 `until <조건>; do sleep 5; done`이라 조건이 영원히
# 거짓이면 그대로 멈춰 있었다. 실제로 Docker Desktop이 죽은 채 20분을 대기하고도
# 로그에는 "스택 기동 완료" 이전 줄만 남아서, **왜 안 끝나는지 알 수 없었다.**
# 측정 도구가 조용히 멈추면 그날의 실험 시간을 통째로 잃는다.
#
# $1 = 최대 대기 초, $2 = 설명, $3.. = 판정 명령
wait_until() {
  local timeout="$1" what="$2"; shift 2
  local t0; t0=$(date +%s)
  until "$@" >/dev/null 2>&1; do
    if [ $(( $(date +%s) - t0 )) -ge "$timeout" ]; then
      echo "[FATAL] ${timeout}초 안에 '$what' 조건이 성립하지 않았다. 중단한다." >&2
      echo "        마지막 확인 명령: $*" >&2
      docker ps -a --format '{{.Names}}\t{{.Status}}' >&2 2>/dev/null \
        || echo "        docker 자체가 응답하지 않는다." >&2
      return 1
    fi
    sleep 5
  done
}

# $1 = 시나리오 디렉터리(load-test 아래 이름), $2 = 실행 명령 원문
evidence_init() {
  local scenario="$1" cmdline="${2:-}"
  local run_id
  run_id="$(date +%Y%m%d-%H%M%S)"
  EVIDENCE_DIR="load-test/${scenario}/evidence/${run_id}"
  EVIDENCE_STARTED_AT="$(date -Iseconds)"
  mkdir -p "$EVIDENCE_DIR"

  {
    echo "run_id           : $run_id"
    echo "scenario         : $scenario"
    # date -Iseconds가 이미 +09:00 같은 오프셋을 담는다. %Z는 Git Bash에서
    # 로케일 이름("대한민국 표준시")이 깨져 나와 쓰지 않는다.
    echo "started_at       : $EVIDENCE_STARTED_AT"
    echo "command          : $cmdline"
    echo "git_branch       : $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '(unknown)')"
    echo "git_commit       : $(git rev-parse HEAD 2>/dev/null || echo '(unknown)')"
    # 작업 트리가 더러우면 그 사실을 반드시 남긴다 — 커밋 SHA만 적으면
    # "그 커밋에서 실행했다"는 잘못된 인상을 준다. 실제로 이 프로젝트의 실험은
    # 대부분 커밋 직전 작업 트리에서 돌았다.
    if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
      echo "git_dirty        : yes ($(git status --porcelain | wc -l | tr -d ' ')개 변경)"
    else
      echo "git_dirty        : no"
    fi
    echo "docker           : $(docker --version 2>/dev/null | tr -d '\r')"
    echo "docker_compose   : $(docker compose version --short 2>/dev/null | tr -d '\r')"
    echo "host_os          : ${OS:-unknown} / $(uname -sr 2>/dev/null)"
    echo "machine_spec     : docs/verification/2026-09-05-environment.md 참고"
  } > "$EVIDENCE_DIR/metadata.txt"

  echo "key,value" > "$EVIDENCE_DIR/counts.csv"
  echo "key,value" > "$EVIDENCE_DIR/inputs.csv"
}

_evidence_ready() { [ -n "$EVIDENCE_DIR" ] && [ -d "$EVIDENCE_DIR" ]; }

# 수집이 실패해 사실상 빈 파일이면 지운다. 빈 파일을 남기면 "수집했는데 값이 0"과
# "대상이 안 떠서 못 받았다"를 구분할 수 없어, 나중에 증거를 잘못 읽게 된다.
_evidence_drop_if_empty() {
  local f="$EVIDENCE_DIR/$1"
  [ -f "$f" ] || return 0
  # 주석(#)과 빈 줄만 있으면 실제 내용이 없는 것으로 본다.
  #
  # `grep -c`는 0건일 때 "0"을 찍고 **종료 코드 1**을 낸다. `|| echo 0`을 붙이면
  # "0"이 두 줄이 되어 비교가 깨지고(처음에 그렇게 짜서 정리가 안 먹었다),
  # 그냥 두면 호출한 스크립트의 `set -e`가 걸린다. `|| true`가 둘 다 피한다.
  local lines
  lines=$(grep -cvE '^[[:space:]]*(#|$)' "$f" 2>/dev/null || true)
  if [ "${lines:-0}" -eq 0 ] 2>/dev/null; then
    rm -f "$f"
    echo "[evidence] 수집 실패로 제외: $1" >&2
  fi
}

# 입력 조건(차량 수, 장애 시간 등). 결과와 섞이면 안 돼서 파일을 나눈다.
evidence_input() {
  _evidence_ready || return 0
  echo "$1,$2" >> "$EVIDENCE_DIR/inputs.csv"
}

# 집계 결과. 결과 문서의 모든 수치는 여기에 한 줄씩 있어야 한다.
evidence_count() {
  _evidence_ready || return 0
  echo "$1,$2" >> "$EVIDENCE_DIR/counts.csv"
}

# Prometheus 스냅샷 — 판단에 실제로 쓰는 지표만. $1 = 파일 접미사(선택)
#
# 처음엔 `^(telemetry_|kafka_)`로 걸렀는데 920줄 250KB가 나왔다. 그중 우리 지표는
# 19줄이고 나머지는 `kafka_consumer_node_*` 같은 클라이언트 내부 지표가 노드·클라이언트별로
# 곱해진 것이다. 실행마다 250KB를 커밋하면 저장소가 증거가 아니라 쓰레기로 찬다.
# **판단 근거가 되는 계열만** 남긴다.
EVIDENCE_METRIC_FILTER='^(telemetry_|kafka_consumer_fetch_manager_records_lag|kafka_consumer_records_lag|kafka_producer_record_(send|error)_total)'

evidence_capture_prometheus() {
  _evidence_ready || return 0
  local name="prometheus${1:+_$1}.txt"
  curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
    | grep -E "$EVIDENCE_METRIC_FILTER" > "$EVIDENCE_DIR/$name" || true
  _evidence_drop_if_empty "$name"
}

# Consumer Group의 offset/lag 원문. $@ = 그룹 이름들
evidence_capture_kafka_groups() {
  _evidence_ready || return 0
  local g
  for g in "$@"; do
    {
      echo "# $g @ $(date -Iseconds)"
      docker exec telemetry-kafka kafka-consumer-groups \
        --bootstrap-server localhost:29092 --describe --group "$g" 2>/dev/null
      echo
    } >> "$EVIDENCE_DIR/kafka-consumer-groups.txt" || true
  done
  _evidence_drop_if_empty kafka-consumer-groups.txt
}

# 토픽별 end offset 원문. $@ = 토픽 이름들
evidence_capture_topic_offsets() {
  _evidence_ready || return 0
  local t
  for t in "$@"; do
    {
      echo "# $t @ $(date -Iseconds)"
      docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:29092 --topic "$t" 2>/dev/null
      echo
    } >> "$EVIDENCE_DIR/kafka-topic-offsets.txt" || true
  done
  _evidence_drop_if_empty kafka-topic-offsets.txt
}

# 컨테이너 로그에서 필요한 줄만. $1 = 컨테이너, $2 = grep 패턴, $3 = 저장 이름
evidence_capture_log_lines() {
  _evidence_ready || return 0
  docker logs "$1" 2>&1 | grep -E "$2" | tail -200 > "$EVIDENCE_DIR/$3" 2>/dev/null || true
  _evidence_drop_if_empty "$3"
}

# 임의 파일 복사(스크립트 콘솔 출력 등). $1 = 원본, $2 = 저장 이름
evidence_capture_file() {
  _evidence_ready || return 0
  [ -f "$1" ] && cp "$1" "$EVIDENCE_DIR/$2" || true
}

# $1 = 성공 기준 문장, $2 = 판정(예: PASS/FAIL/관찰)
evidence_finish() {
  _evidence_ready || return 0
  {
    echo "finished_at      : $(date -Iseconds)"
    echo "success_criteria : ${1:-(미기재)}"
    echo "verdict          : ${2:-(미기재)}"
    echo "verification     : 부분 검증 (1회 실행) — 반복은 docs/roadmap.md P0-2"
  } >> "$EVIDENCE_DIR/metadata.txt"

  # 파일이 나중에 바뀌지 않았음을 확인할 수 있게 한다.
  ( cd "$EVIDENCE_DIR" && sha256sum ./* > checksums.txt 2>/dev/null ) || true
  echo "[evidence] 저장됨: $EVIDENCE_DIR"
}
