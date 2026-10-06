#!/bin/bash
# usage: ctl.sh '<json commands array body>'  -> 응답 토픽 출력
MSYS_NO_PATHCONV=1 docker run --rm --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 sh -c "mosquitto_sub -h mosquitto -t '\$CONTROL/dynamic-security/v1/response' -W 4 & sleep 1; mosquitto_pub -h mosquitto -t '\$CONTROL/dynamic-security/v1' -m '{\"commands\":[$1]}'; wait"
