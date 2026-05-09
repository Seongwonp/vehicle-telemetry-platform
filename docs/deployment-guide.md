# AWS EC2 배포 가이드

---

## 1. EC2 인스턴스 준비

### 권장 스펙
| 항목 | 권장 |
|------|------|
| 인스턴스 타입 | t3.medium (2 vCPU, 4GB RAM) |
| OS | Ubuntu 22.04 LTS |
| 스토리지 | 30GB gp3 |
| 보안 그룹 포트 | 아래 표 참고 |

### 보안 그룹 인바운드 규칙
| 포트 | 프로토콜 | 허용 대상 | 용도 |
|------|----------|----------|------|
| 22 | TCP | 내 IP만 | SSH |
| 8080 | TCP | 0.0.0.0/0 | Spring Boot API |
| 3000 | TCP | 0.0.0.0/0 | Grafana |
| 1883 | TCP | 차량/시뮬레이터 IP | MQTT (개발) |
| 8883 | TCP | 차량/시뮬레이터 IP | MQTT TLS (운영) |

> 운영 배포 시 1883 포트는 닫고 8883만 열기

---

## 2. EC2 서버 초기 설정

```bash
# SSH 접속
ssh -i your-key.pem ubuntu@<EC2-PUBLIC-IP>

# 시스템 업데이트
sudo apt-get update && sudo apt-get upgrade -y

# Docker 설치
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker ubuntu
newgrp docker

# Docker Compose 설치
sudo apt-get install -y docker-compose-plugin

# 확인
docker --version
docker compose version
```

---

## 3. 프로젝트 배포

```bash
# 저장소 클론
git clone https://github.com/<your-username>/vehicle-telemetry-platform.git
cd vehicle-telemetry-platform

# 환경변수 설정 (실제 비밀번호로 변경!)
cp .env.example .env
nano .env

# (선택) MQTT TLS 인증서 생성
cd broker/certs && ./generate-certs.sh && cd ../..

# 전체 스택 실행
docker compose up -d

# 상태 확인
docker compose ps
```

---

## 4. 접속 확인

| 서비스 | URL |
|--------|-----|
| Swagger UI | `http://<EC2-IP>:8080/swagger-ui.html` |
| Grafana | `http://<EC2-IP>:3000` (admin / .env 비밀번호) |
| Prometheus | `http://<EC2-IP>:9090` |
| InfluxDB | `http://<EC2-IP>:8086` |

---

## 5. 시뮬레이터 실행 (로컬 → EC2 연결)

```bash
# 로컬 PC에서
cd simulator
pip install -r requirements.txt

MQTT_HOST=<EC2-PUBLIC-IP> \
VEHICLE_COUNT=5 \
ANOMALY_RATE=0.05 \
python vehicle_simulator.py
```

---

## 6. 유용한 운영 명령어

```bash
# 로그 확인
docker compose logs -f backend
docker compose logs -f anomaly-detector

# 특정 서비스 재시작
docker compose restart backend

# 전체 중지
docker compose down

# 볼륨까지 삭제 (데이터 초기화)
docker compose down -v

# 이미지 재빌드 후 배포
docker compose build backend
docker compose up -d backend
```

---

## 7. .env 필수 변경 항목 체크리스트

```
[ ] INFLUXDB_PASSWORD      — 강력한 비밀번호로 변경
[ ] INFLUXDB_TOKEN         — 최소 32자 랜덤 문자열
[ ] POSTGRES_PASSWORD      — 강력한 비밀번호로 변경
[ ] REDIS_PASSWORD         — 강력한 비밀번호로 변경
[ ] JWT_SECRET             — 최소 32자 랜덤 문자열
[ ] ADMIN_PASSWORD         — 기본값 changeme 반드시 변경
[ ] GRAFANA_PASSWORD       — 기본값 changeme 반드시 변경
```

> 랜덤 문자열 생성: `openssl rand -base64 32`
