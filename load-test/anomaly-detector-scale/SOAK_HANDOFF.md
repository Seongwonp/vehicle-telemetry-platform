# 데스크탑용 12시간 soak test 핸드오프 프롬프트

노트북에서 두 번 시도했지만 둘 다 중간에 끊겼다(v1: macOS 절전으로 1h46m만에 중단,
v2: caffeinate로 절전은 막았지만 6프로세스+3인스턴스+전체 스택의 지속 CPU 600-700%대
부하로 Docker Desktop 자체가 약 2시간 만에 응답 불능. 자세한 내용은
`load-test/anomaly-detector-scale/README.md` 참고). 데스크탑(더 여유 있는 호스트)에서
재시도하기 위한 핸드오프.

아래 내용을 **그대로 복사해서 데스크탑의 새 Claude Code 세션에 붙여넣으면** 된다.

---

## 프롬프트 (여기부터 복사)

vehicle-telemetry-platform 저장소에서 anomaly-detector 3인스턴스 12시간 soak test를
진행해줘. 배경:

- 목표: `docker-compose.yml`의 `anomaly-detector` 서비스가 `deploy.replicas: 3`으로
  이미 다중화되어 있는데(commit 525037f까지 반영됨), ~2,300-2,700 msg/s급 실부하를
  12시간 동안 유지해도 Kafka consumer lag(`anomaly-detector-group`)이 발산하지 않고
  버티는지 확인하는 것. 1시간 A/B 비교(`AB1_1instance_1h_samehost.txt` vs
  `AB2_3instances_1h_samehost.txt`)는 이미 끝났고 3인스턴스가 확실히 우세했지만, 장시간
  안정성은 아직 검증 안 됨.
- 노트북에서 두 번 시도했지만 둘 다 호스트 문제로 중단됐다(README.md의 "4차 측정" 절
  참고). 데스크탑이 더 여유 있다는 전제로 재시도한다.
- 절차와 스크립트는 전부 `load-test/anomaly-detector-scale/`에 이미 있다
  (`sample.sh`, `README.md`, 이전 로그들). **그대로 재사용**하면 된다.

### 시작 전 체크리스트
1. `git pull origin main` — 최신 커밋 확인 (525037f 이후)
2. `.env` 파일 확인 — gitignore라 git엔 없음. 노트북 `.env`를 복사해오거나
   `.env.example` 기준으로 새로 작성(최소: POSTGRES_*, REDIS_PASSWORD, INFLUXDB_*,
   JWT_SECRET, ADMIN_PASSWORD, MQTT_TLS_STORE_PASSWORD)
3. macOS 절전 방지: `caffeinate -dimsu &`로 백그라운드 실행 + 시스템 설정에서도
   "디스플레이 꺼짐 후 자동 절전 방지" 켜두기 (v1이 이걸 안 해서 실패했었음)
4. Docker Desktop 리소스 설정(CPU/메모리 할당량)을 최대한 넉넉하게 — v2는 6프로세스+
   3인스턴스+전체 스택의 지속 CPU 600-700%로 Docker Desktop 자체가 죽었다. 데스크탑
   코어 수에 맞춰 시뮬레이터 프로세스 수(아래 6개)를 줄이는 것도 고려할 것

### 실행 절차 (이번 소크 테스트는 mTLS 인증서 없이 plaintext 개발 오버레이로 진행 —
### 최근에 추가된 차량별 mTLS 인증서는 SIM-001~003 3대용이라 이번에 합성으로 띄우는
### 1,200개 vehicle_id와는 안 맞음. `docker-compose.dev.yml` 오버레이로 우회)

```bash
cd <데스크탑의 저장소 경로>

# 1. 코어 스택 기동 — 기본 simulator 프로파일은 올리지 말 것(offset 충돌 방지),
#    anomaly-detector는 deploy.replicas:3이 이미 있어 --scale 없이 3개가 뜬다
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d \
  mosquitto zookeeper kafka influxdb postgres redis backend anomaly-detector prometheus grafana

docker compose ps anomaly-detector   # 3개 Up 확인

# 2. 시뮬레이터 6프로세스 기동 (AB1/AB2와 동일 조건: 200대/0.05초 × 6 ≈ 2,300-2,700 msg/s)
for i in 0 1 2 3 4 5; do
  docker compose -f docker-compose.yml -f docker-compose.dev.yml run -d --rm \
    --name telemetry-sim-$i \
    -e VEHICLE_ID_OFFSET=$((i*200)) \
    -e VEHICLE_COUNT=200 \
    -e PUBLISH_INTERVAL=0.05 \
    simulator
done

# 3. anomaly-detector-group lag이 500 밑으로 드레인될 때까지 대기 (AB2와 동일 절차)
watch -n 10 'docker exec telemetry-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group anomaly-detector-group'

# 4. 드레인 확인되면 sample.sh로 12시간(43200초) 관찰 시작, 60초 간격
cd load-test/anomaly-detector-scale
nohup ./sample.sh AB4_soak_desktop_12h 43200 60 \
  "$(pwd)" > soak_run.out 2>&1 &
echo "sample.sh PID: $!"
disown
```

`sample.sh`는 이미 자체 타임아웃(15초)이 있어서 docker exec 하나가 멈춰도 전체 루프가
같이 멈추지 않는다(v1의 실패 원인은 이미 고쳐져 있음). `AB4_soak_desktop_12h.txt` 파일에
60초 간격으로 kafka lag, `[anomaly_lag_sum]`, influx 저장 건수, docker stats가 쌓인다.

### 종료 후 (또는 다시 끊기면)
- `grep anomaly_lag_sum AB4_soak_desktop_12h.txt`로 lag 추이만 뽑아서 발산/진동/안정
  여부 판단
- `docs/load-test-plan.md`의 "Track A — 이상 감지 서비스 다중화" 절, ADR-016,
  `load-test/anomaly-detector-scale/README.md`를 이번 결과로 갱신 — **완주했든
  또 중단됐든 정직하게 기록** (이 세션의 원칙: 과장 없이, 안 되면 안 됐다고 씀)
- 원시 로그(`AB4_soak_desktop_12h.txt`)는 반드시 `load-test/anomaly-detector-scale/`
  안에 커밋할 것 — 스크래치패드에만 남기면 세션 종료 시 날아간다(1차 측정 원시 로그가
  이렇게 유실된 전례가 있음)
- 커밋 메시지는 직접 만들지 말고 나(사용자)에게 먼저 요약해서 보여줄 것

---

## 프롬프트 끝
