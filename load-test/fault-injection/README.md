# 의존성 장애 주입 → 복구 → 정합성 대조

CLAUDE.md 우선순위 3번. `run_scenario.sh <influxdb|kafka|mosquitto> [장애 지속 초]`.

## 무엇이 "정답"인지가 시나리오마다 다르다

이게 이 측정의 핵심 설계다. 의존성이 죽으면 평소 기준으로 쓰던 것이 더 이상 기준이 아니다.

| 장애 | 정답 기준 | 이유 | 검증 대상 |
| --- | --- | --- | --- |
| **InfluxDB** | Kafka 토픽 | 메시지는 Kafka까지 정상 도착한다 | 재시도 → DLQ → 재처리로 전부 복구되는가 |
| **Kafka** | backend의 `telemetry_mqtt_messages_received_total` | 토픽 자체가 못 받으므로 토픽을 기준으로 쓸 수 없다 | 로컬 spool이 유실을 막는가 |
| **MQTT 브로커** | 시뮬레이터의 `confirmed`(PUBACK 수신) | 브로커가 죽으면 그 아래가 전부 비어 기준이 될 수 없다 | 브로커가 받았다고 확인한 것이 backend까지 오는가 |

### 브로커 장애의 정답은 `publish()`의 반환값이 **아니다**

이 시나리오를 오래 미뤄둔 이유가 여기 있다. 발행 측에 기준이 필요한데,
`client.publish()`가 돌려주는 `MQTT_ERR_SUCCESS`는 **"paho 클라이언트의 송신 큐에
넣었다"**는 뜻이지 브로커가 받았다는 뜻이 아니다. 브로커가 죽어도 paho가 그것을
알아채기 전까지는 계속 성공을 반환한다.

QoS 1에서 브로커가 받았다는 유일한 증거는 PUBACK이고, 그게 `on_publish` 콜백이다.
그래서 시뮬레이터가 두 단계를 따로 센다(`PublishStats`):

```
[STATS] attempted=… queued=… rejected=… confirmed=…
```

- `queued` : publish()가 성공 반환 — 클라이언트가 받아들였다
- `confirmed` : PUBACK 도착 — **브로커가 받았다. 이게 정답 기준이다**
- `rejected` : publish()가 실패(대부분 `NO_CONN`)를 반환한 건수
- `attempted - confirmed` : 발행 측에서 브로커까지 끝내 못 간 건수

**`rejected`는 유실이 아니다.** 처음엔 유실로 적었다가 측정에서 `confirmed`가
`queued`보다 크게 나와서(46,142 > 39,440) 틀린 걸 알았다. paho 1.6.1의
`_messages_reconnect_reset_out()`은 QoS 1 메시지를 `clean_session`과 무관하게
DUP로 표시해 **다시 큐에 넣는다** — 즉 `NO_CONN`으로 거부된 건도 재연결하면 나간다.

PUBACK이 유실되면 paho가 DUP로 재전송하지만 `confirmed`는 1만 오른다. 즉 `confirmed`는
브로커가 실제로 받은 건수의 **하한**이다 — 유실 판정 기준으로는 이쪽이 안전하다.

### 시뮬레이터를 SIGKILL로 죽이면 안 된다

정답 기준이 시뮬레이터 안에 있으므로 **정상 종료시켜 최종 집계를 남겨야 한다.**
`docker rm -f`(SIGKILL)로 죽였더니 최대 `STATS_INTERVAL`(5초)만큼 묵은 주기 로그를
읽게 됐고, 초당 1,000건 부하에서 그건 5,000건이라 **정답 기준이 backend 수신량보다
작게** 나왔다. 그래서 시뮬레이터를 `--rm` 없이 띄우고 `docker stop`으로 세운다.

종료 시 paho 큐에 남은 미확인 메시지도 최대 `SHUTDOWN_FLUSH_SECONDS`(45초)까지
flush를 기다린다 — 안 기다리면 그 건들이 PUBACK 전에 버려져 기준이 작아진다.

## 절차

`docker stop`(SIGTERM)을 쓴다. 의존성이 정상 종료한 상태를 흉내내는 것이고,
여기서 보려는 건 **우리 쪽의 복구 동작**이지 의존성의 크래시 복구가 아니다.

1. `down -v`로 완전히 비운 뒤 스택 기동
2. 시뮬레이터 200대 / 0.2초, 60초 정상 구간
3. 대상 의존성 `docker stop`, 지정 시간 대기
4. `docker start` 후 healthy 확인, 60초 더
5. 시뮬레이터 `docker stop`(SIGTERM) → 최종 집계 읽기 → lag 0까지 드레인
6. 시뮬레이터 집계 / MQTT 수신 / Kafka 토픽 / DLQ / InfluxDB 행을 한 번에 대조

## 결과

- InfluxDB·Kafka (2026-09-04): `RESULT_20260904_fault_injection.md` — 둘 다 유실 0.
- **MQTT 브로커 (2026-09-05): `RESULT_20260905_mqtt_broker.md` — 유실 129,445건(72.1%)을
  찾아 0으로 고쳤다.** 원인은 Paho 재연결 백오프 상한이 기본 128초라, 브로커가 살아난
  뒤에도 백엔드가 한참 붙지 않는 동안 브로커가 큐를 넘겨 버리고 있었던 것.

## 주의

- 스크립트가 시작할 때 **`down -v`로 볼륨을 지운다.** 남겨야 할 데이터가 있으면 돌리지 마라.
- InfluxDB 시나리오는 DLQ에 수만 건이 쌓인다(설계상 그렇다 — 아래 결과 문서 참고).
  이어서 `dlq-tools/dlq.py replay`로 복구까지 확인하는 것이 이 측정의 후반부다.
- 각 시나리오 1회 실행이다. 장애 지속 시간·부하에 따라 DLQ 비율이 크게 달라진다.
