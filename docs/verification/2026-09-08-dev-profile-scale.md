# 검증 — dev(평문) 프로파일에서 `scale` 실기동 (2026-09-08)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **부분 검증(1회)** — 구조 검증이다. 처리량은 재지 않았다 |
| 적용 범위 | `docker-compose.yml` + `docker-compose.dev.yml`(평문 MQTT 1883), 파티션 **9**, 저장 인스턴스 2개 |
| 코드 상태 | `464785c` + 작업 트리 변경(`scripts/scale-storage.sh` 메시지 수정) |
| 실행 명령 | 아래 "절차" 그대로 |
| 원본 | 이 문서에 인용한 출력이 전부다(실험 스크립트가 아니라 수동 절차라 `evidence/`를 만들지 않았다) |

2026-09-07에 compose 프로파일 `scale`을 만들면서 **dev(평문) 프로파일은 렌더링만
확인하고 기동은 못 했다**(그 시점에 Docker Desktop이 내려가 있었다).
[`2026-09-07-compose-scale-profile.md`](2026-09-07-compose-scale-profile.md)의 미검증 항목이었다.

## 절차

```bash
DC="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
$DC up -d mosquitto zookeeper kafka influxdb postgres redis kafka-init prometheus
$DC up -d backend
docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
  --alter --topic vehicle-telemetry --partitions 9
COMPOSE_FILES="-f docker-compose.yml -f docker-compose.dev.yml" bash scripts/scale-storage.sh up 2
COMPOSE_FILES="-f docker-compose.yml -f docker-compose.dev.yml" bash scripts/scale-storage.sh status
```

## 결과 — 구조는 mTLS 프로파일과 같다

```
멤버(컨슈머 스레드): 9
정적 id 종류      : 9
인스턴스(호스트)  : 3
할당된 파티션 합  : 9
fencing 0건 / MQTT 연결 끊김 0건 (세 컨테이너 모두)
```

**dev 오버레이가 저장 인스턴스에도 먹는다**(이번에 확인하려던 것):

| 컨테이너 | 확인한 환경변수 |
| --- | --- |
| `telemetry-backend-storage-1` | `MQTT_PORT=1883`, `MQTT_TLS_ENABLED=false`, `MQTT_INGEST_ENABLED=false`, `GROUP_INSTANCE_ID_BASE=telemetry-storage-1` |
| `telemetry-backend-storage-2` | 같음(`GROUP_INSTANCE_ID_BASE=telemetry-storage-2`) |

브로커에 붙은 클라이언트는 둘뿐이고 **둘 다 주 backend(172.18.0.10)** 다.
저장 인스턴스에는 `mqttInbound` 어댑터가 **0개**다(주 backend는 1개).

Prometheus는 `up{job="telemetry-backend-storage"}`로 두 인스턴스 모두 **1**.

전날 명시한 `session.timeout.ms`도 실제 컨슈머 설정에 찍힌다 —
`session.timeout.ms = 45000`(backend 로그), 저장 인스턴스 환경변수 `KAFKA_SESSION_TIMEOUT_MS=45000`.

## 새로 알게 된 것 — 파티션을 늘려도 컨슈머는 최대 5분간 못 본다

**어제는 backend 기동 *전에* 파티션을 늘려서 안 드러났다.** 오늘은 순서를 바꿔
backend를 먼저 띄우고 파티션을 3 → 9로 늘렸더니 이렇게 됐다.

| 시각(UTC) | 사건 | 할당된 파티션 |
| --- | --- | ---: |
| 11:20:33 | backend 기동 | — |
| 11:21:38 | 최초 할당 | 3 |
| ~11:22 | 파티션 3 → 9로 확장, 저장 인스턴스 2개 기동 → 리밸런싱 | **3** |
| 11:26:05~08 | 메타데이터 갱신에 따른 재리밸런싱 | **9** |

**리밸런싱은 즉시 돌았는데 할당은 3개 그대로였다.** 컨슈머가 보고 있는 토픽
메타데이터가 아직 "파티션 3개"였기 때문이다. `metadata.max.age.ms`(기본 **300,000ms**,
백엔드 로그에서 확인)가 지나야 새 파티션이 보인다.

그동안 새로 띄운 6개 스레드는 **유휴 멤버로만 붙어 있다.** 겉으로는
"인스턴스를 늘렸는데 처리량이 안 는다"로 보이고, 원인이 인스턴스가 아니라 **메타데이터
나이**라는 건 `--describe --members`의 `#PARTITIONS`를 봐야 드러난다.

→ Runbook의 "파티션을 **먼저** 늘려라"에 이유와 숫자가 생겼다. 순서를 지키면 이 대기가
아예 없고, 이미 떠 있는 스택에서 늘렸다면 **최대 5분 기다린 뒤에** 판정해야 한다.

## 스케일 다운 — dev 프로파일 3회를 채웠다

| 회차 | 프로파일 | `down` 반환 | 멤버 3 복귀 |
| ---: | --- | ---: | ---: |
| 1 (2026-09-07) | 기본(mTLS) | 3초 | 44초 |
| 2 (2026-09-08) | dev(평문) | 4초 | **46초** |
| 3 (2026-09-08) | dev(평문) | 5초 | **48초** |
| 4 (2026-09-08) | dev(평문) | 4초 | **47초** |

**dev 프로파일 동일 조건 3회: 46 / 48 / 47초 (최소 46, 최대 48, 폭 2초 = 4.3%).**
이 프로젝트 기준(3회 이상)을 채웠으므로 "**스케일 다운은 약 45~48초가 걸린다**"까지는
말할 수 있다. mTLS 1회(44초)를 합쳐도 44~48초 안이다.

`down` 명령 자체가 반환하는 데 4~5초가 들고, 위 시간은 **명령을 낸 시점부터** 잰 것이다.
컨테이너가 실제로 멈춘 뒤부터의 순수 세션 만료는 그만큼 짧다(약 42~43초) —
`session.timeout.ms`(45,000ms)와 같은 자리다.

### 4회차 하나를 버렸다 — 대기 조건이 잘못돼 있었다

처음 잰 4회차는 **8초**가 나왔다. 값이 앞의 것들과 너무 달라서 다시 봤더니,
스케일 업 후 대기 조건을 `할당된 파티션 >= 9`로만 걸어둔 게 문제였다.
직전 스케일 다운 뒤 남아 있던 **기존 3개 멤버가 이미 9개 파티션을 전부 쥐고 있어서**
조건이 즉시 참이 됐고, **저장 인스턴스가 그룹에 붙기도 전에 내린 것**이다.

`멤버 >= 9`와 `할당 >= 9`를 **둘 다** 봐야 한다. 하나만 보면 "준비됐다"가 거짓이 되는
상태가 있다. 그 8초는 버리고 다시 쟀다(위 표의 4회차 47초).
**값이 예상 범위 밖일 때 문서에 적기 전에 왜 그런지 보는 것** 말고는 이걸 잡을 방법이 없었다.
## 도구를 하나 고쳤다 — 정상을 장애처럼 보여주고 있었다

`scale-storage.sh status`의 Prometheus 확인이 이렇게 돼 있었다.

```bash
curl ... | tr ... | grep -E '...' || echo "  (Prometheus 응답 없음)"
```

`grep`이 0건이면 파이프라인이 실패로 끝나서 **Prometheus가 멀쩡한데도 "응답 없음"이
찍힌다.** 실제로 오늘 `up 2` 직후 상태 확인에서 그 메시지가 나왔고, 직접 질의해보니
Prometheus는 200에 대상 2개가 정상이었다 — file_sd 갱신 주기(30초)를 아직 안 지났을 뿐이다.

**대상 파일을 막 쓴 직후에 대상이 비어 있는 것은 정상이다.** 그걸 "응답 없음"이라고
쓰면 `static_configs`를 피한 이유(=장애처럼 보이는 정상을 만들지 않기)를 스크립트가
스스로 어기는 셈이다. 도달성 확인과 대상 수 확인을 분리했다.

## 이 문서의 한계

- **처리량을 재지 않았다.** 부하를 안 걸었다(파티션 할당까지만 확인).
- 스케일 다운은 dev 프로파일에서 **3회를 채웠다**(46/48/47초). mTLS 프로파일은 여전히 1회다.
  네 회차 모두 **부하가 없는 상태**에서 쟀다 — 처리 중인 배치가 있을 때도 같은지는 안 봤다.
- 메타데이터 지연은 **1회 관찰**이고, `metadata.max.age.ms`를 줄였을 때의 영향은 안 봤다.
  기본값 그대로 두는 것이 맞는지도 판단하지 않았다(줄이면 브로커 메타데이터 요청이 는다).
- 파티션 9는 이 검증을 위해 올린 값이다. **기본값은 3이다.**
- 평문 브로커라 ACL이 없다. mTLS 프로파일에만 있는 ACL 관련 문제는 여기서 안 드러난다
  (2026-09-05에 실제로 그런 버그가 있었다).
