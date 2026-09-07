# 검증 — 수집/저장 분리를 compose로 옮겼다 (2026-09-07)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **부분 검증(1회)** — 구조 검증이다. 처리량은 재지 않았다 |
| 적용 범위 | 단일 Docker Compose(dev 아님, 기본 mTLS 프로파일), 파티션 **9**, 저장 인스턴스 2개 |
| 코드 상태 | 작업 트리(이 커밋 직전) |
| 실행 명령 | 아래 "절차" 그대로 |
| 성공 기준 | 멤버 = 인스턴스×3, 정적 id 중복 0, fencing 0, MQTT 세션 1개, Prometheus 대상 up |

2026-09-06에 만든 `mqtt.ingest.enabled` 분리는 **실험 스크립트의 `docker run`에만**
있었다. 그래서 "운영에서 어떻게 켜느냐"에 답이 없었다. compose 프로파일 `scale`로 옮기고
실제로 켜봤다.

## 절차

```bash
docker compose up -d mosquitto zookeeper kafka influxdb postgres redis kafka-init prometheus
docker compose up -d backend
docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
  --alter --topic vehicle-telemetry --partitions 9
bash scripts/scale-storage.sh up 2
bash scripts/scale-storage.sh status
```

## 결과

### 1. 컨슈머 그룹 — 인스턴스 3개가 파티션 9개를 정확히 나눠 가졌다

```
멤버(컨슈머 스레드): 9
정적 id 종류      : 9   (멤버 수와 같다 = 겹치지 않는다)
인스턴스(호스트)  : 3
할당된 파티션 합  : 9
```

`GROUP-INSTANCE-ID`가 호스트별로 갈린다 — 이게 2026-09-06에 안 되던 것이다.

| GROUP-INSTANCE-ID | HOST | #PARTITIONS |
| --- | --- | ---: |
| `telemetry-backend-consumer-0/1/2` | 172.18.0.10 | 각 1 |
| `telemetry-storage-1-0/1/2` | 172.18.0.11 | 각 1 |
| `telemetry-storage-2-0/1/2` | 172.18.0.9 | 각 1 |

`FencedInstanceIdException` **0건**(세 컨테이너 모두), MQTT `Connection lost` **0건**.

### 2. MQTT 수집은 주 backend 하나에서만 일어난다

브로커에 붙은 클라이언트는 둘뿐이고 **둘 다 같은 IP(172.18.0.10 = 주 backend)** 다.

```
New client connected from 172.18.0.10:43978 as telemetry-backend
New client connected from 172.18.0.10:43994 as telemetry-backend-sys
```

저장 인스턴스 로그에는 `mqttInbound` / `mqttBrokerMetricsInbound` 어댑터가 아예 없다
(`@ConditionalOnProperty`가 빈을 안 만든다). Spring Integration 채널과 service activator는
만들어지지만 **브로커에 붙는 어댑터가 없어서** 연결이 생기지 않는다.

### 3. Prometheus가 저장 인스턴스를 자동으로 잡는다

`file_sd_configs`가 `scripts/scale-storage.sh`가 쓴 파일을 읽는다. 재시작 없이 반영됐다.

```
up{job="telemetry-backend-storage", instance="backend-storage-1:8080"} = 1
up{job="telemetry-backend-storage", instance="backend-storage-2:8080"} = 1
```

내리면 대상이 **0개**로 돌아간다(`/api/v1/targets?state=active`에서 사라진다).

## 새로 알게 된 것 — 스케일 다운은 즉시 반영되지 않는다

`scale-storage.sh down`은 **3초 만에 반환**하는데, 컨슈머 그룹의 멤버가 3으로 돌아오는 데는
**44초**가 걸렸다(1회 관찰). **정적 멤버십의 대가다** — `group.instance.id`를 쓰면 컨슈머가
종료해도 그룹을 즉시 떠나지 않고 `session.timeout.ms`(클라이언트 기본 45초)를 기다린다.

그동안 **그 인스턴스가 맡던 파티션은 아무도 소비하지 않는다.** 유실은 아니다(Kafka가 들고
있고 lag으로 쌓였다가 재할당 후 처리된다). 하지만 **부하 중에 인스턴스를 내리면 그 파티션의
지연이 최소 45초 튄다**는 뜻이고, 이건 원래 정적 멤버십을 켠 이유(리밸런싱 폭풍 회피)와
맞바꾼 것이다.

`docker compose up`으로 늘릴 때는 반대다 — 새 멤버가 즉시 조인해 리밸런싱이 돈다.
**늘리는 건 싸고 줄이는 건 45초짜리다.**

## 이 문서의 한계

- **처리량을 재지 않았다.** 부하를 안 걸었다(파티션만 나눠 가진 것을 확인했다).
  같은 날 3회 반복에서 **회차 간 비교가 성립하지 않는다**는 결론이 나와, 지금 이 환경에서
  처리량 숫자를 새로 내는 것은 값이 없다(`load-test/storage-scale/RESULT_20260907_repeat3.md`).
- 스케일 다운 44초는 **1회 관찰**이다. `session.timeout.ms`를 명시적으로 설정한 적이 없으므로
  이 값은 클라이언트 기본값에 의존한다 — **설정으로 고정돼 있지 않다.**
- 파티션 9는 이 검증을 위해 올린 값이다. **기본값은 3이고, 그때는 저장 인스턴스를 띄워도
  유휴 멤버만 는다.**
- 저장 인스턴스에 InfluxDB 쓰기가 실제로 분산되는지는 **부하 없이는 확인할 수 없다** —
  이번엔 할당만 봤다.
- dev(평문) 프로파일에서는 실행하지 않았다. compose config 렌더링만 확인했다.
