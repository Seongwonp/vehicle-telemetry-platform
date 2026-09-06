# 장애 중 엔드투엔드 추적 — 실패 지점이 실제로 구분되는가 (2026-09-06)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **부분 검증** — 1회 실행, 마커 20건 |
| 적용 범위 | 단일 Docker Compose(dev), 배경 부하 50대, **InfluxDB를 추적 구간 내내 정지** |
| 코드 상태 | `6138aef` + 작업 트리 변경 |
| 실행 명령 | `FAULT=telemetry-influxdb bash load-test/e2e-trace/run_scenario.sh 20 0.5` |
| 환경 | [`docs/verification/2026-09-05-environment.md`](../../docs/verification/2026-09-05-environment.md) |
| 원본 증거 | `evidence/20260906-110110/`(성공 실행), `evidence/20260906-105523/`(도구가 죽은 실행) |

`docs/roadmap.md` P1.5-3. 정상 경로 측정([`RESULT_20260906_e2e.md`](RESULT_20260906_e2e.md))은
전 단계 20/20이라, 도구의 **"최초 미도달 단계" 출력이 한 번도 실행되지 않았다.**
전 단계가 도달하면 그 코드는 죽은 코드다. 끊고 봐야 확인된다.

---

## 결과 — 구분된다

InfluxDB를 세운 채 같은 마커 20건을 흘렸다.

| 단계 | 도착 |
| --- | ---: |
| MQTT 발행(PUBACK) | 20 / 20 |
| Kafka `vehicle-telemetry` | **20 / 20** |
| InfluxDB | **0 / 20** |
| REST 조회 | **0 / 20** |
| WebSocket | **0 / 20** |

```
최초 미도달 단계=influx  kafka=True influx=False rest=False ws=False
```

**수집은 정상이고 저장부터 끊긴다**가 그대로 나온다. 총량 대조였다면 "토픽 = N,
행 = 0"까지만 알 수 있는데, 여기서는 **마커별로** 어디까지 갔는지가 남는다.

## WebSocket이 0인 것은 설계대로다

브로드캐스트는 `saveAll()`이 성공한 뒤에만 일어난다(`TelemetryConsumer`). 그래서 저장이
막히면 앱 실시간 화면은 **조용해진다** — 오래된 값이 새 값인 척 올라오지 않는다.
2026-09-05 순서 측정에서 "묵은 값이 현재 값으로 표시된다"를 앱에서 막았는데, 저장 장애
쪽은 애초에 파이프라인이 막아준다는 것이 여기서 확인된다.

REST가 0인 것도 같은 이유다 — 조회 대상이 InfluxDB다.

## 도구가 측정하려던 상황에서 죽었다

첫 실행은 결과를 못 냈다. `influx_arrivals()`가 InfluxDB에 질의하다
`requests.exceptions.ConnectionError`(이름 해석 실패)로 **프로세스를 통째로 죽였다.**

```
urllib3.exceptions.NameResolutionError: HTTPConnection(host='influxdb', port=8086):
  Failed to resolve 'influxdb'
```

즉 **"어디서 끊겼나"를 재는 도구가, 끊긴 단계를 조회하다 죽는** 구조였다. 정상 경로에서만
돌려봤으니 드러날 수 없었다. 조회 실패를 **미도달로 세고 그 사실을 출력**하도록 고쳤다.

```
** InfluxDB 조회 실패 — 미도달로 센다: ConnectionError
```

Kafka 조회도 같은 이유로 감쌌다. 죽은 실행의 증거(`evidence/20260906-105523/`)는 지우지
않고 남긴다.

## 이 문서의 한계

- **1회 실행이다.**
- 끊는 지점이 **InfluxDB 하나뿐이다.** Kafka·MQTT를 끊었을 때도 같은 방식으로 구분되는지는
  미측정이다(도구는 지원한다 — `FAULT=telemetry-kafka` 등).
- 복구 후 마커가 뒤늦게 도착하는지(재시도로 결국 저장되는지)는 보지 않았다. 이 실험은
  추적 구간 내내 장애를 유지한다.
- 이상 알림 경로는 범위 밖이다.
