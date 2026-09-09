#!/bin/bash
# 보존된 증거의 checksum manifest를 전부 검사한다 (docs/evidence-policy.md P0-1).
#
# ## 왜 필요한가
#
# `evidence_finish`가 실행마다 `checksums.txt`를 남긴다. 그런데 **그 manifest가 실제로
# 맞는지 확인한 적이 없었다.** 2026-09-09 감사에서 Windows clean checkout의 세 실행이
# 불일치인 것이 드러났다 — 확장자 없는 `.prev_offsets`가 `.gitattributes`의 LF 규칙 밖에
# 있어서 `core.autocrlf=true`가 CRLF로 바꿔놓는다.
#
# 증거는 "남겼다"가 아니라 **"제3자가 다시 계산할 수 있다"**여야 의미가 있다.
# 그걸 사람이 아니라 스크립트가 확인하게 한다.
#
# ## 쓰는 법
#
#   bash load-test/lib/verify_evidence.sh              # 전부 검사
#   bash load-test/lib/verify_evidence.sh <evidence-dir>   # 하나만
#
# 실패하면 **파일 경로 / 기대 hash / 실제 hash**를 찍고 종료 코드 1로 끝난다.
# CI가 이 종료 코드를 본다.
set -uo pipefail
cd "$(dirname "$0")/../.."

TARGET="${1:-}"
# git이 없거나 저장소가 아니면 작업 트리 검사만 한다. **검사를 못 한 것과 통과는 다르므로**
# 아래 요약에 그 사실을 찍는다.
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then GIT_OK=1; else GIT_OK=0; fi
EMPTY_SHA=$(printf '' | sha256sum | cut -d' ' -f1)
FAIL=0
CHECKED=0
MISSING=0

# manifest 한 개를 검사한다. `sha256sum -c`를 그대로 쓰지 않는 이유는
# **어디가 왜 틀렸는지**를 찍어야 하기 때문이다 — "FAILED" 한 줄로는 CRLF 문제인지
# 파일이 바뀐 것인지 구분할 수 없다.
verify_one() {  # $1 = evidence 디렉터리
  local dir="$1" manifest="$1/checksums.txt" line expected file actual rel bad=0
  [ -f "$manifest" ] || return 0
  CHECKED=$((CHECKED + 1))

  while IFS= read -r line; do
    [ -n "$line" ] || continue
    expected="${line%% *}"
    # 형식: "<hash> *./<파일명>" 또는 "<hash>  ./<파일명>"
    file="${line#* }"; file="${file#\*}"; file="${file# }"
    rel="$dir/${file#./}"

    if [ ! -f "$rel" ]; then
      printf '  [없음] %s\n' "$rel"
      MISSING=$((MISSING + 1)); bad=1; continue
    fi

    actual=$(sha256sum "$rel" | cut -d' ' -f1)
    if [ "$actual" != "$expected" ]; then
      printf '  [불일치] %s\n    기대: %s\n    실제: %s\n' "$rel" "$expected" "$actual"
      # 줄바꿈이 원인인지 바로 알려준다. 이게 없으면 매번 손으로 확인하게 된다.
      if grep -qU $'\r' "$rel" 2>/dev/null; then
        printf '    → 이 파일에 CR(\\r)이 있다. .gitattributes의 LF 규칙 밖일 가능성이 높다.\n'
      fi
      bad=1
      continue
    fi

    # **여기부터가 clean checkout 검사다.** 디스크가 맞아도 커밋된 blob이 다르면
    # 남이 clone 했을 때 manifest가 깨진다 — P0-1이 정확히 그 방식으로 깨졌고,
    # 그때는 clean clone을 따로 떠서 잡았다. 이 스크립트는 그 부류를 못 잡고 있었다.
    if [ "$GIT_OK" = 1 ]; then
      blob=$(git show ":$rel" 2>/dev/null | sha256sum | cut -d' ' -f1)
      [ -z "$blob" ] && blob=$(git show "HEAD:$rel" 2>/dev/null | sha256sum | cut -d' ' -f1)
      # 빈 입력의 sha256 = 추적되지 않는 파일. 아직 커밋 대상이 아니므로 넘어간다.
      if [ -n "$blob" ] && [ "$blob" != "$EMPTY_SHA" ] && [ "$blob" != "$expected" ]; then
        printf '  [clean checkout 불일치] %s
    manifest: %s
    git blob: %s
'           "$rel" "$expected" "$blob"
        printf '    → 디스크는 맞는데 **커밋된 내용이 다르다.** clone 하면 깨진다.
'
        printf '    → .gitattributes의 eol 규칙과 이 파일의 줄바꿈을 맞춰라.
'
        bad=1
      fi
    fi
  done < "$manifest"

  if [ "$bad" -ne 0 ]; then
    FAIL=$((FAIL + 1))
    printf '실패: %s\n\n' "$dir"
  fi
}

echo "=== 증거 manifest 검사 ==="
if [ -n "$TARGET" ]; then
  verify_one "$TARGET"
else
  # `find`로 모은다. evidence 디렉터리 위치가 시나리오마다 달라도 따라간다.
  while IFS= read -r m; do
    verify_one "$(dirname "$m")"
  done < <(find load-test -name checksums.txt -path '*/evidence/*' | sort)
fi

echo "검사한 manifest : $CHECKED"
if [ "$GIT_OK" = 1 ]; then
  echo "clean checkout  : 대조함 (git blob)"
else
  echo "clean checkout  : **미검사** — git 저장소가 아니다. 작업 트리만 봤다"
fi
echo "실패한 manifest : $FAIL"
echo "없는 파일       : $MISSING"

if [ "$FAIL" -ne 0 ]; then
  echo
  echo "증거를 다시 계산할 수 없다. 원인을 고치기 전에는 그 실행의 수치를 인용하지 마라."
  exit 1
fi
echo "전부 일치."
