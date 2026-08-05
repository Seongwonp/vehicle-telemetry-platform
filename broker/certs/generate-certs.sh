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

echo "================================================================"
echo "  MQTT TLS 인증서 생성 시작"
echo "================================================================"

# ── 1. CA (Certificate Authority) ───────────────────────────────
echo "[1/5] CA 인증서 생성 중..."
openssl req -new -x509 -days 3650 \
  -keyout ca.key -out ca.crt \
  -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=TelemetryCA" \
  -nodes -quiet

echo "      ca.key / ca.crt 생성 완료"

# ── 2. Mosquitto 서버 인증서 ─────────────────────────────────────
echo "[2/5] 서버 인증서 생성 중..."
openssl req -new \
  -keyout server.key -out server.csr \
  -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=mosquitto" \
  -nodes -quiet

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
  -nodes -quiet

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
    -nodes -quiet
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
