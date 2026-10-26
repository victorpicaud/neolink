#!/usr/bin/env bash
# A burst of MQTT messages, faster than neolink can read them, must not stop
# neolink listening:
#   - a camera's MQTT handler keeps running, without a restart
#   - it still answers commands
#   - a config published after the burst is still applied
# Needs ffprobe, python3 and mosquitto on PATH.
#
# Usage: iso-test/sim/mqtt-burst.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18558}
MQTT_PORT=${MQTT_PORT:-18832}
URL=rtsp://127.0.0.1:$RTSP_PORT
MQTT=(-h 127.0.0.1 -p "$MQTT_PORT")

build
start_mosquitto "$MQTT_PORT"
start_fakecam a 19400 19410
start_fakecam b 19401 19411
neolink_toml "$RTSP_PORT" "$MQTT_PORT" a=19400 b=19401 > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml" mqtt-rtsp
wait_for_codec "$URL/a/main" h264
wait_for_codec "$URL/b/main" h264
for _ in $(seq 1 30); do
  if [ "$(mosquitto_sub "${MQTT[@]}" -t neolink/a/status -C 1 -W 2 2> /dev/null)" = connected ]; then
    break
  fi
  sleep 1
done

# Far more than the 100 messages neolink buffers for its readers
seq 1 5000 | mosquitto_pub "${MQTT[@]}" -t neolink/a/burst -l
sleep 2

if grep -qE "a: MQTT (stopped|failed|crashed), restarting" "$WORK/neolink.log"; then
  flunk "the burst restarted camera a's MQTT handler"
else
  echo "ok: camera a's MQTT handler kept running through the burst"
fi

# The handler answers on the command's own topic: OK, or FAIL here since the
# fake camera has no LED
mosquitto_sub "${MQTT[@]}" -t neolink/a/control/led -C 2 -W 20 > "$WORK/led.out" 2>&1 &
reader=$!
sleep 1
mosquitto_pub "${MQTT[@]}" -t neolink/a/control/led -m on
wait "$reader" || true
if grep -qE "^(OK|FAIL)" "$WORK/led.out"; then
  echo "ok: camera a still answers commands"
else
  flunk "camera a did not answer a command: $(tr '\n' ' ' < "$WORK/led.out")"
fi

neolink_toml "$RTSP_PORT" "$MQTT_PORT" b=19401 \
  | mosquitto_pub "${MQTT[@]}" -t neolink/config -s
if wait_for_answer "$URL/a/main" "404 Not Found" 30; then
  echo "ok: a config published after the burst is still applied"
else
  flunk "a config published after the burst was ignored"
fi

finish
