#!/usr/bin/env bash
# Cameras come and go with a config published over MQTT, as Home Assistant or
# a user would send it:
#   - a camera removed from the config is no longer served
#   - the other camera keeps streaming meanwhile, without reconnecting
#   - a camera added back is served again
#   - `neolink mqtt` and `neolink mqtt-rtsp` without an [mqtt] section stop
#     with an error at once
# Needs ffprobe, python3 and mosquitto on PATH.
#
# Usage: iso-test/sim/config-reload.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18556}
MQTT_PORT=${MQTT_PORT:-18830}
URL=rtsp://127.0.0.1:$RTSP_PORT

build
start_mosquitto "$MQTT_PORT"
start_fakecam a 19200 19210
start_fakecam b 19201 19211

# config [--no-mqtt] CAMERA...: a config serving these cameras
config() {
  local mqtt=yes camera
  if [ "$1" = --no-mqtt ]; then
    mqtt=no
    shift
  fi
  echo 'bind = "127.0.0.1"'
  echo "bind_port = $RTSP_PORT"
  if [ "$mqtt" = yes ]; then
    printf '\n[mqtt]\nbroker_addr = "127.0.0.1"\nport = %s\n' "$MQTT_PORT"
  fi
  for camera in "$@"; do
    case $camera in
      a) camera_toml a 19200 ;;
      b) camera_toml b 19201 ;;
    esac
  done
}

# publish_config CAMERA...: sends neolink a new config over MQTT
publish_config() {
  config "$@" | mosquitto_pub -h 127.0.0.1 -p "$MQTT_PORT" -t neolink/config -s
}

config a b > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml" mqtt-rtsp
wait_for_codec "$URL/a/main" h264
wait_for_codec "$URL/b/main" h264
b_connections=$(grep -c "b: Connecting to camera" "$WORK/neolink.log")

publish_config b
if wait_for_answer "$URL/a/main" "404 Not Found" 30; then
  echo "ok: a camera removed from the config is no longer served"
else
  flunk "a camera removed from the config is still served"
fi
if out=$(probe "$URL/b/main" 15) && echo "$out" | grep -qx h264; then
  echo "ok: the other camera keeps streaming"
else
  flunk "the other camera stopped streaming: $out"
fi

publish_config a b
if wait_for_answer "$URL/a/main" "^h264$" 60; then
  echo "ok: a camera added back is served again"
else
  flunk "a camera added back is not served again"
fi
if [ "$(grep -c "b: Connecting to camera" "$WORK/neolink.log")" = "$b_connections" ]; then
  echo "ok: the other camera never reconnected"
else
  flunk "the other camera reconnected although its config did not change"
fi

# On a port of its own, so that only the missing section can stop it
RTSP_PORT=$((RTSP_PORT + 100)) config --no-mqtt a > "$WORK/no-mqtt.toml"
for command in mqtt mqtt-rtsp; do
  start=$SECONDS
  rc=0
  timeout 30 "$NEOLINK_BIN" "$command" --config "$WORK/no-mqtt.toml" > "$WORK/$command.log" 2>&1 || rc=$?
  if [ "$rc" = 0 ] || [ "$rc" = 124 ] || ! grep -q "\[mqtt\] section" "$WORK/$command.log"; then
    flunk "neolink $command without an [mqtt] section did not stop with an error (rc=$rc)"
  else
    echo "ok: neolink $command without an [mqtt] section stops with an error after $((SECONDS - start))s"
  fi
done

finish
