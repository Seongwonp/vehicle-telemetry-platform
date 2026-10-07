#!/usr/bin/env bash
# Discard experiment-G persistent MQTT sessions (user-approved 2026-10-07, M1-M4 of docs/experiment-data-inventory.md §6-2).
#
# HOW: connect once with the given client id and clean_session=true (mosquitto_sub default when -c is NOT given),
#      subscribe to a no-op topic and exit right after SUBACK (-E). On a clean-session CONNECT the broker discards that
#      id's stored session (subscriptions + queued QoS1 messages) without delivering them. The connect itself is the delete.
# WHERE: throwaway eclipse-mosquitto:2.0 container on the compose network -> mosquitto:1883 (dev listener, allow_anonymous true).
#
# HARD ALLOWLIST: anything not exactly one of the four ids below is refused before any connection is made.
# NEVER the production ids telemetry-backend / telemetry-backend-sys (would discard un-PUBACKed QoS1 queue -> real loss).
# -h2 / -h2-sys are NOT in scope (not requested).
set -u

ALLOW=("telemetry-backend-g1" "telemetry-backend-g2" "telemetry-backend-g1-sys" "telemetry-backend-g2-sys")
NET="vehicle-telemetry-platform_telemetry-net"

is_allowed() {
  local id="$1" a
  for a in "${ALLOW[@]}"; do [[ "$id" == "$a" ]] && return 0; done
  return 1
}

if [[ $# -eq 0 ]]; then echo "usage: $0 <client-id>..." >&2; exit 2; fi

# Validate ALL args first; refuse the whole run if any is outside the allowlist.
for id in "$@"; do
  if ! is_allowed "$id"; then echo "REFUSED (not in allowlist): '$id' — nothing executed" >&2; exit 3; fi
done

rc_all=0
for id in "$@"; do
  echo "== $(date -u '+%F %T') UTC clean-session connect as '$id'"
  MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" eclipse-mosquitto:2.0 \
    mosquitto_sub -h mosquitto -p 1883 -i "$id" -t 'telemetrix/session-cleanup/noop' -q 0 -E -d
  rc=$?; echo "   exit=$rc"; [[ $rc -ne 0 ]] && rc_all=$rc
done
exit $rc_all
