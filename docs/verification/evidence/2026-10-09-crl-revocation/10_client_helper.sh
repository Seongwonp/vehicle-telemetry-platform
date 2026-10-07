#!/bin/bash
# usage: crlpub.sh <VEHICLE_ID|none>  — 일회용 mosquitto 클라이언트로 8883에 1회 발행. 키 내용은 출력하지 않는다.
V="$1"; R=D:/vehicle-telemetry-platform/broker/certs
TS=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
P="{\"vehicle_id\":\"$V\",\"timestamp\":\"$TS\",\"speed\":42.0,\"rpm\":1500.0,\"engine_temp\":85.0,\"throttle_position\":20.0,\"fuel_level\":60.0,\"battery_voltage\":13.8,\"gps\":{\"lat\":37.5665,\"lng\":126.978},\"dtc_codes\":[]}"
MOUNTS=(-v "$R/ca.crt:/c/ca.crt:ro")
ARGS=(--cafile /c/ca.crt)
if [ "$V" != "none" ]; then
  MOUNTS+=(-v "$R/vehicles/$V.crt:/c/v.crt:ro" -v "$R/vehicles/$V.key:/c/v.key:ro")
  ARGS+=(--cert /c/v.crt --key /c/v.key)
fi
echo "# $(date '+%F %T %Z') client=$V ts=$TS"
MSYS_NO_PATHCONV=1 docker run --rm --network vehicle-telemetry-platform_telemetry-net "${MOUNTS[@]}" eclipse-mosquitto:2.0 \
  mosquitto_pub -h mosquitto -p 8883 "${ARGS[@]}" -i "crltest-$V" -q 1 -t "vehicle/telemetry/$V" -m "$P" 2>&1
echo "mosquitto_pub exit=$?"
