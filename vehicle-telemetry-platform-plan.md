# 차량 텔레메트리 데이터 수집 & 모니터링 플랫폼

> 백엔드/서버 엔지니어 포트폴리오 프로젝트
> 목표: 현대오토에버 등 모빌리티 IT 기업 지원용 대표 프로젝트

---

## 프로젝트 개요

차량(또는 시뮬레이터)에서 발생하는 실시간 센서 데이터를 수집하고, 이상 감지 및 시각화까지 처리하는 서버 플랫폼.

- **핵심 키워드**: 실시간 스트리밍, 대용량 처리, 보안, IoT 백엔드
- **어필 포인트**: 현대오토에버 도메인(커넥티드카) + 서버 엔지니어링 깊이를 동시에 보여줌

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 데이터 수신 | MQTT (Eclipse Mosquitto 브로커) |
| 메시지 큐 | Apache Kafka |
| 백엔드 API | Java (Spring Boot) |
| 이상 감지 모듈 | Python (scikit-learn 또는 룰 기반) |
| 데이터베이스 | PostgreSQL (이력), InfluxDB (시계열) |
| 캐시 | Redis |
| 모니터링 대시보드 | Grafana |
| 차량 시뮬레이터 | Python 또는 C |
| 보안 | JWT 인증, TLS/SSL, Rate Limiting, 이상 트래픽 감지 |
| 인프라 | Docker Compose, (선택) AWS EC2 배포 |

---

## 시스템 아키텍처

```
[차량 / 시뮬레이터]
       │
       │  MQTT (TLS 암호화)
       ▼
[MQTT 브로커 - Mosquitto]
       │
       │  메시지 수신
       ▼
[Kafka Producer - Java]
       │
       ├──────────────────────────┐
       ▼                          ▼
[Kafka Consumer A]         [Kafka Consumer B]
  데이터 저장                이상 감지 트리거
  (InfluxDB/PostgreSQL)       (Python 모듈 호출)
       │                          │
       └──────────┬───────────────┘
                  ▼
         [Spring Boot REST API]
                  │
                  ▼
         [Grafana 대시보드]
         [웹 프론트엔드 (선택)]
```

---

## 수집 데이터 항목 (OBD-II 기준)

```json
{
  "vehicle_id": "KR-GA-1234",
  "timestamp": "2026-05-09T10:00:00Z",
  "speed": 87.3,
  "rpm": 2400,
  "engine_temp": 92.1,
  "throttle_position": 34.5,
  "fuel_level": 67.0,
  "battery_voltage": 13.8,
  "gps": {
    "lat": 37.123456,
    "lng": 127.654321
  },
  "dtc_codes": []
}
```

---

## 핵심 기능

### Phase 1 - 데이터 수집 파이프라인 (필수)
- [ ] Python/C 차량 시뮬레이터 구현 (1초 간격 데이터 발행)
- [ ] MQTT 브로커 구축 (TLS 적용)
- [ ] Kafka 클러스터 구성 (단일 노드로 시작)
- [ ] Spring Boot Consumer 구현 → InfluxDB 저장

### Phase 2 - API 서버 (필수)
- [ ] 차량 등록/관리 REST API
- [ ] 실시간 데이터 조회 API
- [ ] JWT 기반 인증/인가
- [ ] Rate Limiting (Bucket4j 또는 Redis 기반)

### Phase 3 - 이상 감지 (핵심 차별화)
- [ ] 룰 기반 이상 감지 (엔진 온도 임계값, RPM 급등 등)
- [ ] 이상 감지 시 알림 이벤트 발행 (Kafka → Webhook/이메일)
- [ ] (선택) 머신러닝 이상 감지 (Isolation Forest)

### Phase 4 - 보안 강화 (차별화 포인트)
- [ ] MQTT 클라이언트 인증 (X.509 인증서)
- [ ] API 요청 로깅 및 이상 접근 감지
- [ ] SQL Injection, IDOR 방어 처리
- [ ] 보안 취약점 자체 스캔 및 보고서 작성

### Phase 5 - 모니터링 & 배포 (선택)
- [ ] Grafana 대시보드 (실시간 차트)
- [ ] Prometheus + Spring Actuator 메트릭
- [ ] Docker Compose로 전체 스택 컨테이너화
- [ ] AWS EC2 배포 (선택)

---

## OBD-II 실제 연결 방법 (ELM327 동글)

```
1. ELM327 Bluetooth/WiFi 동글 구매 (2~3만원, 쿠팡)
2. 차량 OBD-II 포트에 꽂기 (보통 운전석 아래)
3. Python 라이브러리로 데이터 읽기:

   pip install obd

   import obd
   connection = obd.OBD()
   cmd = obd.commands.SPEED
   response = connection.query(cmd)
   print(response.value)  # 87 kph

4. 읽은 데이터를 MQTT로 서버에 전송
```

> 신차 걱정 NO: OBD-II 동글은 읽기 전용이라 차량 제어 불가, 데이터만 읽음

---

## 시뮬레이터 방식 (동글 없이 개발 가능)

```python
# vehicle_simulator.py
import paho.mqtt.client as mqtt
import json, time, random

client = mqtt.Client()
client.connect("localhost", 1883)

while True:
    data = {
        "vehicle_id": "SIM-001",
        "timestamp": time.time(),
        "speed": random.uniform(0, 120),
        "rpm": random.randint(800, 4000),
        "engine_temp": random.uniform(80, 100),
    }
    client.publish("vehicle/telemetry", json.dumps(data))
    time.sleep(1)
```

---

## 면접 어필 포인트

| 질문 | 답변 근거 |
|------|----------|
| "대용량 트래픽 처리 경험?" | Kafka로 차량 N대 동시 스트림 처리 |
| "실시간 시스템 경험?" | MQTT + Kafka + InfluxDB 파이프라인 |
| "보안 신경 써봤어요?" | TLS, 인증서, Rate Limiting, 이상 접근 감지 |
| "우리 도메인 알아요?" | 커넥티드카 데이터 흐름 직접 구현해봄 |
| "모니터링 해봤어요?" | Grafana + Prometheus 구성 경험 |

---

## 예상 개발 일정

| 기간 | 내용 |
|------|------|
| 1~2주 | 환경 구성, 시뮬레이터, MQTT + Kafka 파이프라인 |
| 3~4주 | Spring Boot API 서버, DB 연동 |
| 5~6주 | 이상 감지 모듈, 알림 시스템 |
| 7~8주 | 보안 강화, Grafana 대시보드 |
| 9~10주 | Docker화, 배포, README 및 아키텍처 문서 작성 |

---

## 레포지토리 권장 구조

```
vehicle-telemetry-platform/
├── simulator/          # Python/C 차량 시뮬레이터
├── broker/             # Mosquitto 설정
├── kafka/              # Kafka 설정
├── backend/            # Spring Boot API 서버
├── anomaly-detector/   # Python 이상 감지 모듈
├── monitoring/         # Grafana + Prometheus 설정
├── docker-compose.yml
└── README.md
```

---

## 참고 자료

- MQTT 프로토콜: https://mqtt.org
- Apache Kafka 공식 문서: https://kafka.apache.org/documentation
- python-OBD: https://python-obd.readthedocs.io
- UN R155 (자동차 사이버보안 규제): ISO/SAE 21434 참고
- InfluxDB 시작하기: https://docs.influxdata.com

---

> 이 프로젝트 하나로: **Java 백엔드 + Python 분석 + C 시뮬레이터 + 보안 + IoT + 분산처리** 전부 커버 가능
