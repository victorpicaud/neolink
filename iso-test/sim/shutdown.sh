#!/usr/bin/env bash
# Stopping neolink with SIGTERM, as docker stop does, while a client watches:
#   - neolink stops by itself, soon, and exits with 0
#   - it logs out of the camera
#   - MQTT shows it offline, and its camera disconnected
# Needs ffmpeg, python3 and mosquitto on PATH.
#
# Usage: iso-test/sim/shutdown.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18568}
MQTT_PORT=${MQTT_PORT:-18842}
URL=rtsp://127.0.0.1:$RTSP_PORT
MQTT=(-h 127.0.0.1 -p "$MQTT_PORT")

build
start_mosquitto "$MQTT_PORT"
start_fakecam s 20400 20410
neolink_toml "$RTSP_PORT" "$MQTT_PORT" s=20400 > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml" mqtt-rtsp
wait_for_codec "$URL/s/main" h264
for _ in $(seq 1 30); do
  if [ "$(mosquitto_sub "${MQTT[@]}" -t neolink/s/status -C 1 -W 2 2> /dev/null)" = connected ]; then
    break
  fi
  sleep 1
done

timeout -k 2 60 ffmpeg -v error -rtsp_transport tcp -i "$URL/s/main" -f null - \
  > "$WORK/client.log" 2>&1 &
PIDS+=($!)
sleep 3

kill -TERM "$NEO_PID"
started=$SECONDS
# Docker gives up after 10s, and kills it
(sleep 15 && kill -KILL "$NEO_PID" 2> /dev/null) &
PIDS+=($!)
code=0
wait "$NEO_PID" || code=$?
took=$((SECONDS - started))
if [ "$code" = 0 ] && [ "$took" -le 10 ]; then
  echo "ok: neolink stopped by itself after ${took}s, with 0"
else
  flunk "neolink stopped after ${took}s, with $code"
fi

if grep -q "logout" "$WORK/fakecam-s.log"; then
  echo "ok: neolink logged out of the camera"
else
  flunk "neolink did not log out of the camera"
fi

status=$(mosquitto_sub "${MQTT[@]}" -t neolink/status -C 1 -W 2 2> /dev/null || true)
camera=$(mosquitto_sub "${MQTT[@]}" -t neolink/s/status -C 1 -W 2 2> /dev/null || true)
if [ "$status" = offline ] && [ "$camera" = disconnected ]; then
  echo "ok: MQTT shows neolink offline, and its camera disconnected"
else
  flunk "MQTT shows neolink as ${status:-nothing}, and its camera as ${camera:-nothing}"
fi

finish
