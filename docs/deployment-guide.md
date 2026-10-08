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
| 8883 | TCP | 차량/시뮬레이터 IP | MQTT TLS (운영) |

> 기본 Compose는 8883/mTLS만 노출한다. 1883 평문은 `docker-compose.dev.yml`을 명시한
> 로컬 개발 프로파일에서만 `127.0.0.1`에 바인딩된다.
>
> **2026-10-08부터 8883도 기본은 `127.0.0.1`이다.** EC2에서 차량·시뮬레이터가 밖에서 붙으려면
> 서버의 `.env`에 `MQTT_TLS_BIND=0.0.0.0`을 넣어야 한다(보안 그룹은 그대로 허용 대상 IP만).
> 인증서는 서버에서 `broker/certs/generate-certs.sh`로 새로 만들고, 각 서비스에는 필요한 파일만
> 마운트된다(`ca.key`는 어떤 컨테이너에도 안 들어간다) — `docs/verification/2026-10-08-cert-access-scope.md`.

> `/actuator/prometheus`는 애플리케이션 레벨에선 인증 없이 열려있다(Prometheus 스크레이핑용, `SecurityConfig.java`).
> 운영 배포 시엔 보안그룹/리버스프록시로 `/actuator/**` 전체를 내부망(모니터링 서버)에서만 접근 가능하도록 제한할 것.

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

# MQTT TLS 인증서 생성 (기본 프로파일 필수)
cd broker/certs && ./generate-certs.sh && cd ../..

# 전체 스택 실행
docker compose up -d

# 인증서 없는 로컬 평문 개발만 필요한 경우
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

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

## 8. Windows에서 8883 포트가 바인딩되지 않을 때 (로컬 개발 PC)

`MQTT_TLS_PORT`의 기본값은 **8883 그대로**다. 다만 일부 Windows PC는 Hyper-V/WSL이 잡는 **동적 제외 포트 범위**에 8883이 들어가
`docker compose up`이 mosquitto 포트 게시에서 실패한다(이 저장소 개발 PC에서 2026-10-07 관찰 — 범위 8875–8974).

1. 확인(관리자 권한 불필요):
   ```
   netsh int ipv4 show excludedportrange protocol=tcp
   ```
   출력의 시작·끝 포트 사이에 8883이 있으면 충돌이다.
2. **그 PC에서만** 포트를 바꾼다 — 저장소 기본값·compose 파일은 건드리지 않고, 그 PC의 `.env`에 한 줄:
   ```
   MQTT_TLS_PORT=18883
   ```
   (제외 범위 밖의 아무 포트). 바뀌는 것은 **호스트에 게시되는 포트**뿐이다 — 컨테이너 안 리스너와 compose 네트워크 안의
   backend·simulator(`mosquitto:8883`)는 영향이 없다.
3. 이 PC 밖의 클라이언트(브리지 등)는 바꾼 포트로 접속해야 한다(`obd-bridge/README.md` 참고).

시스템의 제외 범위 자체(`netsh int ipv4 add excludedportrange` 등)는 바꾸지 않는다 — 다른 프로그램에 영향이 있다.

## 9. VM 배포 체크리스트 — 인증서·CRL

```
[ ] broker/certs/generate-certs.sh로 이 VM 전용 CA·인증서 생성(로컬 개발 인증서 복사 금지)
[ ] CRL nextUpdate 확인: openssl crl -in broker/certs/crl.pem -noout -nextupdate
    — 현재 개발 CRL은 2027-10-07. 그 전에 bash broker/certs/generate-crl.sh 로 갱신 후 docker compose restart mosquitto.
      만료되면 모든 mTLS 클라이언트(백엔드·차량)가 거부될 수 있다.
[ ] 폐기 대상은 broker/certs/revoked-certs.txt에만 추가(CA 지문이 맞는 줄만 반영된다)
```
