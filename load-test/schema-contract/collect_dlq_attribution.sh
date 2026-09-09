#!/bin/bash
# DLQ 레코드를 **차량 ID와 사유에 귀속**시킨다 — 실행 후 분석 도구.
#
# ## 왜 별도인가
#
# 이건 실행 시점에 봉인한 **원본이 아니라 파생물**이다. 원본은 Kafka 토픽 자체이고
# 이 스크립트는 그걸 다시 읽어 표로 만든다. 그래서 `evidence/<run>/derived/`에 쓰고,
# 실행 매니페스트(`checksums.txt`는 `-maxdepth 1`)에 섞이지 않는다.
# 무결성은 `derived/checksums.txt`로 따로 확인한다.
#
# ## 왜 이게 필요한가
#
# 판정표의 "저장 0행"은 **거부됐다**는 뜻이지 **의도한 이유로 거부됐다**는 뜻이 아니다.
# 2026-09-09 2회차 실행에서 6칸이 시나리오가 아니라 **차량 ID 길이** 때문에 거부되고도
# PASS로 보였다. 사유를 레코드마다 맞춰봐야 그게 드러난다.
#
# 사용법: bash load-test/schema-contract/collect_dlq_attribution.sh <evidence_dir>
set -uo pipefail
cd "$(dirname "$0")/../.."

EV="${1:?evidence 디렉터리를 인자로 줘라 (예: load-test/schema-contract/evidence/20260909-145736)}"
[ -d "$EV" ] || { echo "없는 디렉터리: $EV"; exit 1; }
[ -f "$EV/manifest.json" ] || { echo "manifest.json이 없다 — E2E 실행 디렉터리가 맞나: $EV"; exit 1; }

OUT="$EV/derived"
mkdir -p "$OUT"

TOOLS="telemetrix-e2e-trace:latest"   # paho + kafka-python이 둘 다 있는 유일한 이미지
KAFKA_DLQ=vehicle-telemetry-dlq
MQTT_DLQ=vehicle-telemetry-mqtt-dlq
TIMEOUT_MS=20000

consume() {  # $1=topic, 나머지=추가 --property
  local topic="$1"; shift
  docker exec telemetry-kafka kafka-console-consumer \
    --bootstrap-server localhost:29092 --topic "$topic" \
    --from-beginning --timeout-ms "$TIMEOUT_MS" "$@" 2>/dev/null | tr -d '\r'
}

# ── Kafka DLQ: 레코드 키(차량 ID) ↔ 예외 타입·사유 ──────────────
# 키를 같이 뽑는 이유: 헤더만으로는 어느 시나리오인지 못 가른다
# ("speed must not be null"이 필드 누락과 명시적 null 둘 다에서 나온다).
#
# 헤더는 **이름으로 찾는다.** 위치로 박으면 헤더가 하나 늘어날 때 조용히 엉뚱한 값을
# 읽는다 — run_e2e.sh의 field_value가 첫 실행에서 정확히 그렇게 틀렸다.
{
  printf '%-22s %-32s %s\n' "vehicle_id" "exception" "reason"
  consume "$KAFKA_DLQ" --property print.key=true --property print.headers=true \
                       --property print.value=false \
    | awk -F'\t' '
        {
          hdr = $1; key = $2
          cls = ""; msg = ""
          n = split(hdr, parts, ",")
          for (i = 1; i <= n; i++) {
            if (parts[i] ~ /^x-dlq-failure-class:/) {
              cls = parts[i]; sub(/^x-dlq-failure-class:/, "", cls)
            }
            if (parts[i] ~ /^x-dlq-failure-message:/) {
              msg = parts[i]; sub(/^x-dlq-failure-message:/, "", msg)
              # 사유 문자열 자체에 쉼표가 들어갈 수 있어 뒤를 전부 붙인다.
              for (j = i + 1; j <= n; j++) msg = msg "," parts[j]
              break
            }
          }
          sub(/[ \t]+$/, "", msg)
          sub(/^.*[.]/, "", cls)          # FQCN에서 클래스 이름만
          printf "%-22s %-32s %s\n", key, cls, msg
        }' \
    | sort
} > "$OUT/dlq-kafka-attribution.txt"

# ── MQTT DLQ: envelope JSON ────────────────────────────────────
# payload가 원본이 아니라 envelope이라 헤더가 없다. 사유 코드만 있고 **상세가 없다**.
#
# **호스트의 python을 쓰지 않는다.** Git Bash에서 `python`은 Windows 스토어 스텁이라
# 아무 일도 안 하고 성공한다 — 처음에 그래서 이 파일이 **에러 없이 빈 채로** 만들어졌다.
# run_e2e.sh와 같은 도구 이미지 안에서 돌린다.
{
  printf '%-22s %s\n' "vehicle_id" "reason"
  consume "$MQTT_DLQ" \
    | MSYS_NO_PATHCONV=1 docker run --rm -i "$TOOLS" python -c '
import sys, json
rows = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except Exception:
        rows.append(("[envelope 파싱 불가]", line[:100])); continue
    rows.append((str(d.get("mqtt_topic", "?")).split("/")[-1], d.get("reason", "?")))
for vid, reason in sorted(rows):
    print("%-22s %s" % (vid, reason))
'
} > "$OUT/dlq-mqtt-attribution.txt"

# ── 사유별 집계 (Runbook 2-1절과 같은 방법) ────────────────────
{
  echo "# kafka-dlq (사유 코드별)"
  tail -n +2 "$OUT/dlq-kafka-attribution.txt" | awk '{print $3}' | sed 's/:$//' | sort | uniq -c
  echo "# mqtt-dlq (사유 코드별)"
  tail -n +2 "$OUT/dlq-mqtt-attribution.txt" | awk '{print $2}' | sort | uniq -c
} > "$OUT/reason-counts.txt"

# **빈 결과를 성공으로 넘기지 않는다.** 처음 판에서 MQTT 쪽이 조용히 비어 있었다.
for f in dlq-kafka-attribution.txt dlq-mqtt-attribution.txt; do
  [ "$(wc -l < "$OUT/$f")" -gt 1 ] || { echo "** $f 가 비었다 — 수집 실패"; exit 1; }
done

# ── 출처 기록 ──────────────────────────────────────────────────
cat > "$OUT/PROVENANCE.md" <<EOF
# 파생 증거 — 원본이 아니다

| | |
| --- | --- |
| 생성 스크립트 | \`load-test/schema-contract/collect_dlq_attribution.sh\` |
| 생성 명령 | \`bash load-test/schema-contract/collect_dlq_attribution.sh $EV\` |
| 생성 시각 | $(date -Iseconds) |
| 입력 (원본) | Kafka 토픽 \`$KAFKA_DLQ\`, \`$MQTT_DLQ\` — 실행이 남긴 상태 그대로 |
| 대조용 입력 | \`../manifest.json\` (주입한 차량 ID·시나리오·기대), \`../e2e_results.csv\` (판정) |
| 무결성 | 이 디렉터리의 \`checksums.txt\` — **상위의 실행 매니페스트와 별개다** |

**이 파일들은 실행이 끝난 뒤 토픽을 다시 읽어 만든 것이다.** 실행 시점에 봉인된 원본은
상위 디렉터리의 \`checksums.txt\`가 덮는 파일들뿐이다. 여기 있는 것을 원본인 것처럼
그 매니페스트에 섞지 않는다 — 2026-09-08에 자기검사 결과를 매니페스트 안의
\`metadata.txt\`에 붙이려다 같은 함정을 봤다.

**재현 조건**: DLQ 토픽의 보존 기간 안이고, 그 사이 다른 실행이 같은 토픽에 쓰지 않았다면
같은 결과가 나온다. 둘 중 하나라도 깨지면 재현되지 않는다 — 그때는 이 파일이 유일한 기록이다.
\`run_e2e.sh\`는 매 실행 시작에 \`down -v\`로 토픽을 비우므로, **다음 실행이 돌면 재현 불가**다.

## 파일

| 파일 | 내용 |
| --- | --- |
| \`dlq-kafka-attribution.txt\` | 차량 ID(레코드 키) / 예외 클래스 / 사유 문자열 |
| \`dlq-mqtt-attribution.txt\` | 차량 ID(토픽 마지막 마디) / 사유 코드 — **상세 없음** |
| \`reason-counts.txt\` | 사유 코드별 건수 (\`docs/runbook/dlq-reprocessing.md\` 2-1절과 같은 방법) |
EOF

( cd "$OUT" && find . -maxdepth 1 -type f ! -name 'checksums.txt' ! -name '.*' -print0 \
    | sort -z | xargs -0 -r sha256sum > checksums.txt )

echo "저장됨: $OUT"
awk '{print "  " $2}' "$OUT/checksums.txt"
