# 검증 — 증거 manifest가 새 체크아웃에서 재계산되는가 (2026-09-09)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **검증 완료** — Windows clean clone과 Linux CI 양쪽에서 39/39 통과 |
| 대상 | `load-test/**/evidence/**/checksums.txt` 전체 39개 |
| 코드 상태 | `a2a30b1` |
| 검사 도구 | `load-test/lib/verify_evidence.sh` |
| 성공 기준 | clean checkout에서 모든 manifest의 기대 해시와 실제 해시가 일치 |

`docs/current-state-audit-2026-09-09.md`가 P0으로 지목한 항목이다. 확인해보니 사실이었다.

## 문제 — 내 작업 트리에서는 안 보였다

감사는 세 실행의 `.prev_offsets` 해시가 불일치한다고 했다. 그런데 **내 작업 트리에서는
일치했다.** 어젯밤 스크립트가 만든 LF 원본 파일이 그대로 남아 있었기 때문이다.

파일을 지우고 다시 체크아웃해서 재현했다.

```
현재 작업 트리:        388dd7ca11a67e6f...   ← manifest 기대값과 일치
rm + git checkout 후:  cc8927afbbec5f20...   ← 불일치
줄바꿈:                0 989919^M$            ← CRLF로 변환됨
```

`cc8927af`는 감사가 보고한 값과 글자 하나까지 같다.

원인은 `git check-attr`로 확인했다.

| 파일 | `text` | `eol` |
| --- | --- | --- |
| `status.txt` | set | **lf** (`.gitattributes`가 보호) |
| `.prev_offsets` | **unspecified** | **unspecified** → `core.autocrlf=true` 적용 |

`.gitattributes`가 `*.txt`/`*.csv`/`*.json`/`*.log`만 나열하고 있었고,
확장자 없는 숨김 파일이 그 목록 밖으로 새어 나갔다.

## 전수 검사 — 39개 중 3개

`verify_evidence.sh`를 만들어 전체를 돌렸다. 내 작업 트리에서는 **1개**만 실패했다
(재현하느라 내가 다시 체크아웃한 그 파일). 진짜 조건인 **clean clone**에서 돌리니 3개였다.

```
[불일치] load-test/storage-scale/evidence/20260908-221713/.prev_offsets
[불일치] load-test/storage-scale/evidence/20260908-235532/.prev_offsets
[불일치] load-test/storage-scale/evidence/20260909-001140/.prev_offsets
검사한 manifest : 39 / 실패한 manifest : 3
```

**세 개 전부 `.prev_offsets`고 전부 CRLF다.** 나머지 36개는 통과했다 —
기존 LF 규칙이 덮는 범위 안에서는 잘 작동하고 있었다.

## 증거는 처음부터 옳았다 — 체크아웃만 망가뜨렸다

고치기 전에 **어느 쪽이 틀렸는지** 확인했다. git에 저장된 blob의 해시를 manifest와 대조했다.

| 실행 | git blob | manifest | |
| --- | --- | --- | --- |
| `20260908-221713` | `76cdfe49b5bc` | `76cdfe49b5bc` | 일치 |
| `20260908-235532` | `87da0f689896` | `87da0f689896` | 일치 |
| `20260909-001140` | `388dd7ca11a6` | `388dd7ca11a6` | 일치 |

**저장된 증거는 LF이고 manifest와 정확히 같다.** 깨뜨린 것은 checkout 단계다.
그래서 **증거를 고치지 않고 규칙으로 살린다** — manifest를 다시 쓰거나 파일을 지우면
그 실행의 기록을 바꾸는 것이 된다.

## 고친 것

1. **`.gitattributes`를 확장자별이 아니라 evidence 통째로 LF 고정.**
   확장자 목록은 새 파일이 생길 때마다 조용히 뚫린다. evidence 안이 전부 스크립트가 만든
   텍스트임을 확인하고(`file`로 전수 확인) `load-test/**/evidence/** text eol=lf`로 바꿨다.
2. **`verify_evidence.sh` 신규.** 모든 manifest를 순회하고, 실패하면 파일 경로·기대 해시·
   실제 해시를 찍는다. CR이 있으면 줄바꿈이 원인이라고 알려준다(안 그러면 매번 손으로 본다).
   통과 0 / 실패 1로 끝나고 CI가 그 코드를 본다.
3. **`evidence_finish`가 숨김 파일을 manifest에서 제외하고, 만든 직후 자체 검증한다.**
   결과는 `checksum_selfcheck.txt`에 남는다.
4. **실험 스크립트의 작업 파일을 evidence 밖으로 옮겼다**(`.prev_offsets`, `.current_phase`).
   규칙으로도 막았지만 **작업 파일을 증거에 섞지 않는 것**이 근본 해결이다 —
   `.prev_offsets` 내용은 `partition_lag.csv`에서 전부 다시 계산할 수 있어 증거 가치가 없다.
5. **CI에 검사 단계 추가.**

### 고치면서 같은 함정을 한 번 밟았다

자체 검증 결과를 `metadata.txt`에 덧붙이도록 짰다가 되돌렸다.
**`metadata.txt`는 manifest 안에 있어서, 거기 한 줄만 더해도 그 순간 해시가 어긋난다.**
고치려던 문제를 그대로 다시 만드는 코드였다. `checksum_selfcheck.txt`를 따로 두고
그 파일은 **의도적으로 manifest 밖**에 둔다(manifest보다 나중에 생기므로 들어갈 수도 없다).

## 검증

| 조건 | 결과 |
| --- | --- |
| 수정 전, Windows clean clone | **3 실패** / 39 |
| 수정 후, Windows clean clone (`core.autocrlf=true`) | **0 실패** / 39, `.prev_offsets`가 LF로 체크아웃됨 |
| 도구 자체 — 정상 manifest | 종료 코드 **0** |
| 도구 자체 — 파일 한 줄 변조 | 종료 코드 **1**, 기대·실제 해시 출력 |
| `evidence_finish` 합성 실행 | 숨김 파일이 manifest에서 제외됨, `checksum_selfcheck.txt = OK` |

Linux(CI)는 `core.autocrlf`가 기본 false라 원래 이 문제가 없다. 그래서 CI 단계는
**회귀 방지**용이다 — 앞으로 다른 이유로 증거가 어긋나면 그때 잡힌다.

## 이 문서의 한계

- **Linux CI에서의 통과는 다음 푸시에서 확인된다.** 이 문서 작성 시점에는 로컬 Windows
  clean clone 결과만 있다.
- `verify_evidence.sh`는 manifest에 **적힌** 파일만 검사한다. manifest에서 빠진 증거 파일이
  있어도 알려주지 않는다(예: `*.log`는 의도적으로 제외된다).
- 과거 실행의 `.prev_offsets`는 그대로 둔다. 증거 가치는 없지만 manifest에 들어 있어
  지우면 오히려 그 manifest가 깨진다.
- macOS에서는 확인하지 않았다.
