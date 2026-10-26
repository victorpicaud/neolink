#!/usr/bin/env bash
# A camera that refuses neolink's login is tried again, not given up on:
#   - neolink says why, and keeps the camera
#   - once the camera takes the password, and the config changes, the camera
#     comes back without restarting neolink
# Needs ffprobe, python3 and mosquitto on PATH.
#
# Usage: iso-test/sim/login.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18562}
MQTT_PORT=${MQTT_PORT:-18834}
URL=rtsp://127.0.0.1:$RTSP_PORT

build
start_mosquitto "$MQTT_PORT"
# Not the password neolink has
start_fakecam refusing 20000 20010 --password other
CAMERA_PID=${PIDS[-1]}

neolink_toml "$RTSP_PORT" "$MQTT_PORT" refusing=20000 > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml" mqtt-rtsp

start=$SECONDS
refused=false
while [ $((SECONDS - start)) -lt 30 ]; do
  if grep -q "refusing: The camera refused the login\|refusing: Login credentials were not accepted" \
    "$WORK/neolink.log"; then
    refused=true
    break
  fi
  sleep 0.5
done
if ! $refused; then
  fail "neolink never tried to log in"
fi
if grep -q "refusing: The camera refused the login with code" "$WORK/neolink.log"; then
  echo "ok: neolink said that the camera refused the login, and why"
else
  flunk "neolink did not say why the camera refused the login:" \
    "$(grep -m1 "refusing:.*\(login\|Login\)" "$WORK/neolink.log")"
fi

# The camera now takes neolink's password, and the config changes
kill "$CAMERA_PID"
wait "$CAMERA_PID" 2> /dev/null || true
start_fakecam refusing 20000 20010
{
  neolink_toml "$RTSP_PORT" "$MQTT_PORT"
  camera_toml refusing 20000 "max_reconnect_backoff = 30"
} | mosquitto_pub -h 127.0.0.1 -p "$MQTT_PORT" -t neolink/config -s

if wait_for_answer "$URL/refusing/main" "^h264$" 60; then
  echo "ok: the camera came back once it took the password"
else
  flunk "the camera did not come back once it took the password"
fi

finish
