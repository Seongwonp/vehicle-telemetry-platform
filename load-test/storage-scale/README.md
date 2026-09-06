# 저장 경로 수평 확장

```bash
bash load-test/storage-scale/run_scenario.sh naive 2   # 그대로 복제하면 뭐가 깨지나
bash load-test/storage-scale/run_scenario.sh split 2   # 수집 1 + 저장 N으로 나누면
```

## 무엇을 재나

**저장 경로(Java)를 여러 인스턴스로 늘릴 수 있는가, 늘리면 정합성이 유지되는가.**

2026-09-05 리밸런싱 측정은 Python 이상 감지만 스케일했다. 저장 경로는 `container_name`과
호스트 포트 8080이 고정돼 있어 스케일이 안 돼서 **미측정으로 남아 있었다.**

## 두 모드가 곧 결론이다

백엔드는 **MQTT 수집**과 **Kafka→InfluxDB 저장**을 겸한다. 그래서 통째로 복제하면
두 식별자가 동시에 부딪힌다 — MQTT `client-id`(`cleanSession=false`)와
Kafka `group.instance.id`가 둘 다 고정이다.

| 모드 | 무엇을 하나 | 결과(2026-09-06) |
| --- | --- | --- |
| `naive` | 아무것도 안 바꾸고 복제 | 멤버 3 그대로(용량 증가 0), fencing 45, MQTT 끊김 44 |
| `split` | 수집 끄고 정적 멤버 id 분리 | 멤버 **9**, fencing 0, 끊김 0, PUBACK=토픽=행 |

## 왜 compose가 아니라 `docker run`인가

`extends`는 `container_name`까지 복사해 충돌하고, 오버라이드로 **그 값을 지울 방법이
없다**. 서비스 정의를 복사하면 환경변수 50줄이 두 벌이 되어 반드시 드리프트한다 —
이 저장소는 그 종류의 드리프트를 P0-3에서 이미 겪었다.

그래서 **실행 중인 `telemetry-backend`의 환경변수를 `docker inspect`로 읽어 그대로**
새 인스턴스에 넘긴다. 같은 이미지·같은 설정이 보장된다.

## 주의

- `docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}'`에서 **`{{end}}`를
  빼먹으면 빈 출력**이 나온다(에러는 stderr로만). 그러면 환경변수 없이 컨테이너가 떠서
  `localhost:5432`에 붙으려다 죽는다. 스크립트가 인자 수를 세어 20개 미만이면 멈춘다.
- **naive의 중복·유실 수치는 무효다.** 연결 경합이 QoS 1 재전송을 유발해 브로커 수신이
  PUBACK보다 커지고, 그러면 중복과 유실이 총량에서 상쇄돼 분리할 수 없다.
  실패 양상만 유효하다.
- 파티션이 3개라 인스턴스를 3개 넘겨도 **유휴 멤버만 는다.**
- `down -v`로 시작한다.
