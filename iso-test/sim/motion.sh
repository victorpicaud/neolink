#!/usr/bin/env bash
# Motion follows the camera, over MQTT:
#   - motion the camera reports is published
#   - a camera that goes away during motion stops showing motion, instead of
#     showing it for good
# Needs python3 and mosquitto on PATH.
#
# Usage: iso-test/sim/motion.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18563}
MQTT_PORT=${MQTT_PORT:-18835}
MQTT=(-h 127.0.0.1 -p "$MQTT_PORT")
CONTROL=127.0.0.1:20110

build
start_mosquitto "$MQTT_PORT"
start_fakecam m 20100 20110
neolink_toml "$RTSP_PORT" "$MQTT_PORT" m=20100 > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml" mqtt-rtsp

control() {
  python3 "$CTL" "$CONTROL" "$1" > /dev/null 2>&1 || fail "could not tell the camera to $1"
}
# retained TOPIC: the message the broker keeps on TOPIC
retained() {
  mosquitto_sub "${MQTT[@]}" -t "$1" -C 1 -W 2 2> /dev/null || true
}
# wait_for_retained TOPIC MESSAGE SECONDS
wait_for_retained() {
  local deadline=$((SECONDS + $3))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$(retained "$1")" = "$2" ]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_retained neolink/m/status connected 60 || fail "camera m never connected"

control motion
if wait_for_retained neolink/m/status/motion on 30; then
  echo "ok: the camera's motion was published"
else
  flunk "the camera's motion was not published: $(retained neolink/m/status/motion)"
fi

# The camera goes away during the motion, and does not come back
control hang
control die
if wait_for_retained neolink/m/status/motion off 30; then
  echo "ok: a camera that went away during motion stopped showing motion"
else
  flunk "a camera that went away during motion still shows: $(retained neolink/m/status/motion)"
fi

finish
