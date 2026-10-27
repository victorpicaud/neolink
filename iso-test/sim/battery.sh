#!/usr/bin/env bash
# neolink and a battery camera over MQTT, with idle_disconnect:
#   - once nothing needs it, the camera sleeps, and says so, rather than
#     disconnected: Home Assistant keeps its entities available
#   - a command wakes it, and it sleeps again after
#   - a camera that cannot be reached when needed is disconnected, once it
#     had its minute to wake
#   - discovery gives Home Assistant a button to wake it, and a switch for
#     its PIR
# Needs mosquitto on PATH.
#
# Usage: iso-test/sim/battery.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18594}
MQTT_PORT=${MQTT_PORT:-18856}
MQTT=(-h 127.0.0.1 -p "$MQTT_PORT")

build
start_mosquitto "$MQTT_PORT"
start_fakecam b 20900 20910
FAKECAM_PID=${PIDS[-1]}
{
  neolink_toml "$RTSP_PORT" "$MQTT_PORT" b=20900
  echo 'idle_disconnect = true'
  echo '[cameras.mqtt.discovery]'
  echo 'topic = "homeassistant"'
  echo 'features = ["reboot", "wakeup", "pir_switch"]'
} > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml" mqtt

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

wait_for_retained neolink/b/status connected 60 || fail "camera b never connected"
# Nothing needs it once it started: 30s later, it sleeps
if wait_for_retained neolink/b/status sleeping 90; then
  echo "ok: a camera that nothing needs sleeps"
else
  flunk "a camera that nothing needs is $(retained neolink/b/status), not sleeping"
fi
if retained homeassistant/button/neolink_b_reboot/b/config | grep -qF "if value == 'disconnected'"; then
  echo "ok: in Home Assistant, a sleeping camera's entities are available"
else
  flunk "discovery does not keep a sleeping camera's entities available"
fi
if retained homeassistant/button/neolink_b_wakeup/b/config | grep -qF '"command_topic":"neolink/b/control/wakeup","payload_press":"5"' \
  && retained homeassistant/switch/neolink_b_pir/b/config | grep -qF '"command_topic":"neolink/b/control/pir"'; then
  echo "ok: discovery gives Home Assistant a button to wake the camera, and a switch for its PIR"
else
  flunk "discovery gives no button to wake the camera, or no switch for its PIR"
fi

mosquitto_pub "${MQTT[@]}" -t neolink/b/control/wakeup -m 0
if wait_for_retained neolink/b/status connected 30; then
  echo "ok: a command wakes a sleeping camera"
else
  flunk "a command did not wake the camera: it is $(retained neolink/b/status)"
fi
if wait_for_retained neolink/b/status sleeping 60; then
  echo "ok: the camera sleeps again after"
else
  flunk "the camera did not sleep again: it is $(retained neolink/b/status)"
fi

# Out of reach: asked to wake, it gets its minute, then is disconnected
kill "$FAKECAM_PID"
mosquitto_pub "${MQTT[@]}" -t neolink/b/control/wakeup -m 0
sleep 10
waking=$(retained neolink/b/status)
if [ "$waking" = sleeping ]; then
  echo "ok: a camera waking is still sleeping, rather than disconnected"
else
  flunk "a camera waking is $waking"
fi
if wait_for_retained neolink/b/status disconnected 80; then
  echo "ok: a camera that does not wake is disconnected"
else
  flunk "a camera that does not wake is $(retained neolink/b/status)"
fi

finish
