#!/bin/bash
# ================================================================
# MQTT TLS + X.509 인증서 생성 스크립트
#
# 생성 파일:
#   ca.crt / ca.key         — 인증기관 (CA)
#   server.crt / server.key — Mosquitto 서버 인증서
#   client.crt / client.key — 차량/시뮬레이터 클라이언트 인증서 (PEM, Python 시뮬레이터용)
#   client.p12              — 차량 클라이언트 인증서+키 (PKCS12, Spring Boot용)
#   truststore.p12          — CA 인증서만 담은 트러스트스토어 (PKCS12, Spring Boot용)
#
# Phase 4: MQTT 연결 시 서버/클라이언트 상호 인증 (mTLS)
# Phase 10: Spring Boot 백엔드에서 mTLS로 접속하려면 PKCS12 형식이 필요하다.
#           openssl req가 만드는 client.key는 PKCS#1 형식인데 Java는 이를 직접 못 읽는다 —
#           PKCS12로 한 번 감싸면 표준 javax.net.ssl API(KeyStore.getInstance("PKCS12"))로 바로 로드 가능하다.
# ================================================================
set -e

MQTT_TLS_STORE_PASSWORD="${MQTT_TLS_STORE_PASSWORD:-changeit}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "================================================================"
echo "  MQTT TLS 인증서 생성 시작"
echo "================================================================"

# ── 1. CA (Certificate Authority) ───────────────────────────────
echo "[1/3] CA 인증서 생성 중..."
openssl req -new -x509 -days 3650 \
  -keyout ca.key -out ca.crt \
  -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=TelemetryCA" \
  -nodes -quiet

echo "      ca.key / ca.crt 생성 완료"

# ── 2. Mosquitto 서버 인증서 ─────────────────────────────────────
echo "[2/3] 서버 인증서 생성 중..."
openssl req -new \
  -keyout server.key -out server.csr \
  -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=mosquitto" \
  -nodes -quiet

openssl x509 -req -days 3650 \
  -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt

rm -f server.csr
echo "      server.key / server.crt 생성 완료"

# ── 3. 클라이언트 인증서 (차량/시뮬레이터) ──────────────────────
echo "[3/3] 클라이언트 인증서 생성 중..."
openssl req -new \
  -keyout client.key -out client.csr \
  -subj "/C=KR/ST=Seoul/O=VehicleTelemetry/CN=vehicle-client" \
  -nodes -quiet

openssl x509 -req -days 3650 \
  -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt

rm -f client.csr
echo "      client.key / client.crt 생성 완료"

# ── 4. Spring Boot용 PKCS12 변환 (Phase 10) ─────────────────────
echo "[4/4] Spring Boot용 PKCS12 키/트러스트스토어 생성 중..."
openssl pkcs12 -export \
  -in client.crt -inkey client.key -certfile ca.crt \
  -out client.p12 -name mqtt-client \
  -passout pass:"$MQTT_TLS_STORE_PASSWORD"

# CA 인증서만 담은 트러스트스토어는 keytool로 만든다.
# openssl pkcs12 -export -nokeys로 만들면 인증서가 bag에는 들어가지만
# trustedCertEntry 속성이 없어서 Java KeyStore(PKCS12)가 항목을 0개로 인식한다 — keytool은 이 속성을 올바르게 채운다.
rm -f truststore.p12
keytool -importcert -alias telemetry-ca -file ca.crt \
  -keystore truststore.p12 -storetype PKCS12 \
  -storepass "$MQTT_TLS_STORE_PASSWORD" -noprompt

echo "      client.p12 / truststore.p12 생성 완료 (비밀번호: MQTT_TLS_STORE_PASSWORD 환경변수 또는 기본값 'changeit')"

# ── 권한 설정 ────────────────────────────────────────────────────
chmod 600 *.key *.p12
chmod 644 *.crt

echo ""
echo "================================================================"
echo "  생성 완료!"
ls -lh *.crt *.key *.p12 2>/dev/null
echo ""
echo "  다음 단계: mosquitto.conf 의 TLS 섹션 주석 해제"
echo "  (broker/config/mosquitto.conf 참고)"
echo "================================================================"
