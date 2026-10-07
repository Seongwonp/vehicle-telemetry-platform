#!/bin/bash
# ================================================================
# MQTT TLS + X.509 인증서 생성 스크립트
#
# 생성 파일:
#   ca.crt / ca.key         — 인증기관 (CA)
#   server.crt / server.key — Mosquitto 서버 인증서
#   backend.crt/.key/.p12   — Spring Boot 구독자 전용 인증서
#   vehicles/<ID>.crt/.key  — 차량별 발행자 인증서(CN=vehicle_id)
#   truststore.p12          — CA 인증서만 담은 트러스트스토어 (PKCS12, Spring Boot용)
#   crl.pem                 — 폐기 목록(generate-crl.sh). 새 CA면 빈 CRL — mosquitto crlfile용
#
# **이 스크립트는 CA부터 새로 만든다.** 기존 차량 하나만 폐기하려면 이걸 돌리지 말고
# revoked-certs.txt + generate-crl.sh를 쓴다.
#
# Windows(Git Bash)에서 돌릴 때 두 가지가 걸린다:
#   1) `-subj "/C=KR/..."`를 Git Bash가 Windows 경로로 바꿔버린다.
#      → `MSYS_NO_PATHCONV=1`을 앞에 붙여 실행할 것.
#   2) `keytool`이 PATH에 없을 수 있다(JDK는 있는데 bin이 안 잡힌 경우).
#      → `PATH="/c/Program Files/Java/jdk-17/bin:$PATH"` 같은 식으로 넣고 실행할 것.
#   예: MSYS_NO_PATHCONV=1 MQTT_VEHICLE_IDS="SIM-001,SIM-002" bash generate-certs.sh
#
# Phase 4: MQTT 연결 시 서버/클라이언트 상호 인증 (mTLS)
# Phase 10: Spring Boot 백엔드에서 mTLS로 접속하려면 PKCS12 형식이 필요하다.
#           openssl req가 만드는 backend.key는 PKCS#1 형식인데 Java는 이를 직접 못 읽는다 —
#           PKCS12로 한 번 감싸면 표준 javax.net.ssl API(KeyStore.getInstance("PKCS12"))로 바로 로드 가능하다.
# ================================================================
set -e

MQTT_TLS_STORE_PASSWORD="${MQTT_TLS_STORE_PASSWORD:-changeit}"
MQTT_VEHICLE_IDS="${MQTT_VEHICLE_IDS:-SIM-001,SIM-002,SIM-003}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# docker-compose.yml은 서비스별로 필요한 파일만 파일 단위로 마운트한다(ca.key는 어디에도 안 감).
# 인증서를 만들기 전에 `docker compose up`을 하면 Docker가 없는 파일 자리에 **빈 디렉터리**를
# 만들어 두고, 그러면 아래 openssl이 "Is a directory"로 실패한다. 비어 있는 디렉터리만 지운다
# (rmdir은 내용이 있으면 실패하므로 실제 파일·데이터는 건드리지 않는다).
for mounted in ca.crt server.crt server.key crl.pem backend.p12 truststore.p12; do
  if [ -d "$mounted" ]; then
    rmdir "$mounted" && echo "      빈 디렉터리 $mounted 제거(인증서 생성 전 compose up이 남긴 것)"
  fi
done

echo "================================================================"
echo "  MQTT TLS 인증서 생성 시작"
echo "================================================================"

# ── 1. CA (Certificate Authority) ───────────────────────────────
echo "[1/5] CA 인증서 생성 중..."
openssl req -new -x509 -days 3650 \
  -keyout ca.key -out ca.crt \
  -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=TelemetryCA" \
  -nodes

echo "      ca.key / ca.crt 생성 완료"

# ── 2. Mosquitto 서버 인증서 ─────────────────────────────────────
echo "[2/5] 서버 인증서 생성 중..."
openssl req -new \
  -keyout server.key -out server.csr \
  -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=mosquitto" \
  -nodes

openssl x509 -req -days 3650 \
  -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt

rm -f server.csr
echo "      server.key / server.crt 생성 완료"

# ── 3. Backend 구독자 인증서 ──────────────────────────────────
echo "[3/4] Backend 인증서 생성 중..."
openssl req -new \
  -keyout backend.key -out backend.csr \
  -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=telemetry-backend" \
  -nodes

openssl x509 -req -days 3650 \
  -in backend.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out backend.crt

rm -f backend.csr
echo "      backend.key / backend.crt 생성 완료"

# ── 4. 차량별 발행자 인증서 ────────────────────────────────────
echo "[4/5] 차량별 인증서 생성 중..."
mkdir -p vehicles
IFS=',' read -ra VEHICLE_IDS <<< "$MQTT_VEHICLE_IDS"
for vehicle_id in "${VEHICLE_IDS[@]}"; do
  if [[ ! "$vehicle_id" =~ ^[A-Z0-9-]{4,20}$ ]]; then
    echo "유효하지 않은 MQTT vehicle_id: $vehicle_id" >&2
    exit 1
  fi
  openssl req -new \
    -keyout "vehicles/$vehicle_id.key" -out "vehicles/$vehicle_id.csr" \
    -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=$vehicle_id" \
    -nodes
  openssl x509 -req -days 3650 \
    -in "vehicles/$vehicle_id.csr" -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out "vehicles/$vehicle_id.crt"
  rm -f "vehicles/$vehicle_id.csr"
done

# ── 5. Spring Boot용 PKCS12 변환 ────────────────────────────────
echo "[5/5] Spring Boot용 PKCS12 키/트러스트스토어 생성 중..."
openssl pkcs12 -export \
  -in backend.crt -inkey backend.key -certfile ca.crt \
  -out backend.p12 -name telemetry-backend \
  -passout pass:"$MQTT_TLS_STORE_PASSWORD"

# CA 인증서만 담은 트러스트스토어는 keytool로 만든다.
# openssl pkcs12 -export -nokeys로 만들면 인증서가 bag에는 들어가지만
# trustedCertEntry 속성이 없어서 Java KeyStore(PKCS12)가 항목을 0개로 인식한다 — keytool은 이 속성을 올바르게 채운다.
rm -f truststore.p12
keytool -importcert -alias telemetry-ca -file ca.crt \
  -keystore truststore.p12 -storetype PKCS12 \
  -storepass "$MQTT_TLS_STORE_PASSWORD" -noprompt

echo "      backend.p12 / truststore.p12 생성 완료 (비밀번호: MQTT_TLS_STORE_PASSWORD 환경변수 또는 기본값 'changeit')"

# ── CRL ─────────────────────────────────────────────────────────
# mosquitto.conf의 crlfile이 읽을 파일. revoked-certs.txt의 줄은 CA 지문이 같을 때만 들어가므로
# 방금 만든 새 CA에서는 폐기 0건(빈 CRL)이다.
bash "$SCRIPT_DIR/generate-crl.sh"

# ── 권한 설정 ────────────────────────────────────────────────────
chmod 600 ./*.key ./*.p12 vehicles/*.key
chmod 644 ./*.crt vehicles/*.crt

echo ""
echo "================================================================"
echo "  생성 완료!"
ls -lh ./*.crt ./*.key ./*.p12 vehicles/*.crt vehicles/*.key 2>/dev/null
echo ""
echo "  다음 단계: docker compose up -d (기본 프로파일은 mTLS 강제)"
echo "================================================================"
