# load-test/lib — 실험 스크립트 공용

## `evidence.sh` — 원본 증거 보존

`docs/evidence-policy.md`의 P0-1을 코드로 옮긴 것이다.

### 왜 만들었나

2026-09-05에 장애 실험을 여덟 번 돌리고 결과 문서를 썼는데, **커밋 직전마다 스크립트
출력(`_result_*.txt`)을 "임시 파일"로 보고 지웠다.** 그래서 그 문서들의 수치를 원본에서
다시 계산할 수 없다. 판단은 남았는데 근거가 없다.

요약 문서는 "무엇을 결론지었나"를, 여기 저장하는 것은 "그 결론을 어떻게 계산했나"를
남긴다. 둘 중 하나만 있으면 재현이 안 된다.

### 저장되는 것

```text
load-test/<scenario>/evidence/<YYYYMMDD-HHMMSS>/
├── metadata.txt              # 실행 시각, git SHA + dirty 여부, docker 버전, 명령 원문,
│                             #   성공 기준과 판정, 검증 상태
├── inputs.csv                # 입력 조건 (차량 수, 장애 시간, 이상률 …)
├── counts.csv                # 집계 결과 — 결과 문서의 모든 수치가 여기 한 줄씩 있어야 한다
├── console.log               # 스크립트 출력 원문
├── prometheus_final.txt      # telemetry_*/kafka_* 지표 스냅샷
├── kafka-consumer-groups.txt # offset/lag 원문
├── kafka-topic-offsets.txt   # end offset 원문
├── *-key-lines.txt           # 컨테이너 로그 중 판단에 쓴 줄만
└── checksums.txt             # 위 파일들의 sha256
```

### 설계에서 신경 쓴 것

- **`git_dirty`를 반드시 남긴다.** SHA만 적으면 "그 커밋에서 실행했다"는 잘못된
  인상을 준다. 이 프로젝트의 실험은 대부분 커밋 직전 작업 트리에서 돌았다.
- **수집 실패한 파일은 지운다.** 빈 파일을 남기면 "값이 0"과 "대상이 안 떠서 못 받았다"를
  구분할 수 없어, 나중에 증거를 잘못 읽게 된다.
- **입력과 결과를 다른 파일에 둔다.** 섞이면 "무엇을 넣었더니 무엇이 나왔나"가 흐려진다.
- **성공 기준을 스크립트가 판정한다.** 사람이 결과를 보고 기준을 정하면 사후 합리화가 된다.
- **비밀정보를 담지 않는다.** `.env` 값, 토큰, 인증서는 넣지 않고, Prometheus도
  `telemetry_*`/`kafka_*`만 걸러 저장한다.

### 쓰는 법

```bash
. load-test/lib/evidence.sh
evidence_init "<scenario-dir>" "<실행 명령 원문>"
evidence_input vehicles 200
# … 실험 …
evidence_count kafka_topic_end_offset "$TOPIC"
evidence_capture_prometheus final
evidence_capture_kafka_groups telemetry-storage-group
evidence_capture_file "$OUT" console.log
evidence_finish "성공 기준" "PASS"
```

`evidence_*` 함수는 실패해도 0을 반환한다 — **증거 수집이 실험을 죽이면 안 된다.**
대상이 안 떠 있으면 그 파일만 빠지고 실험은 계속된다.

### 크기

한 실행당 수십 KB다. 커밋해도 되는 크기로 유지하려고 컨테이너 로그는 필요한 줄만
(`grep` 후 최대 200줄) 저장한다. 그보다 큰 원본이 필요하면 압축해 외부에 두고
`RESULT_*.md`에 위치와 checksum을 적는다(`docs/evidence-policy.md`).
