#!/bin/bash
# ================================================================
# MQTT TLS + X.509 인증서 생성 스크립트
#
# 생성 파일:
#   ca.crt / ca.key         — 인증기관 (CA)
#   server.crt / server.key — Mosquitto 서버 인증서
#   client.crt / client.key — 차량/시뮬레이터 클라이언트 인증서
#
# Phase 4: MQTT 연결 시 서버/클라이언트 상호 인증 (mTLS)
# ================================================================
set -e

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
  -CAcreateserial -out server.crt -quiet

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
  -CAcreateserial -out client.crt -quiet

rm -f client.csr
echo "      client.key / client.crt 생성 완료"

# ── 권한 설정 ────────────────────────────────────────────────────
chmod 600 *.key
chmod 644 *.crt

echo ""
echo "================================================================"
echo "  생성 완료!"
ls -lh *.crt *.key 2>/dev/null
echo ""
echo "  다음 단계: mosquitto.conf 의 TLS 섹션 주석 해제"
echo "  (broker/config/mosquitto.conf 참고)"
echo "================================================================"
