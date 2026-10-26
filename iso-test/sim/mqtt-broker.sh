#!/usr/bin/env bash
# neolink and its MQTT broker:
#   - a broker CA that cannot be read stops neolink from connecting, rather
#     than connecting without TLS
# Needs mosquitto on PATH.
#
# Usage: iso-test/sim/mqtt-broker.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18564}
MQTT_PORT=${MQTT_PORT:-18836}

build

# A neolink with a CA it cannot read, and a broker of its own, so that any
# connection the broker sees is from it
CA_PORT=$((MQTT_PORT + 1))
printf 'listener %s 127.0.0.1\nallow_anonymous true\n' "$CA_PORT" > "$WORK/mosquitto-ca.conf"
mosquitto -c "$WORK/mosquitto-ca.conf" > "$WORK/mosquitto-ca.log" 2>&1 &
PIDS+=($!)
{
  echo 'cameras = []'
  neolink_toml $((RTSP_PORT + 100)) "$CA_PORT"
  echo 'ca = "/nonexistent/ca.pem"'
} > "$WORK/no-ca.toml"
"$NEOLINK_BIN" mqtt --config "$WORK/no-ca.toml" > "$WORK/no-ca.log" 2>&1 &
PIDS+=($!)
sleep 5
if ! grep -q "New client connected" "$WORK/mosquitto-ca.log" \
  && grep -q "Could not read the MQTT ca" "$WORK/no-ca.log"; then
  echo "ok: a CA that cannot be read stops neolink from connecting"
else
  flunk "neolink connected without the CA it could not read: $(tail -n 3 "$WORK/no-ca.log")"
fi

finish
