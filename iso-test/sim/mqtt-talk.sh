#!/usr/bin/env bash
# neolink over MQTT, talking through a camera:
#   - control/say speaks the text through the camera, with the say command
#   - control/talk plays the sound file at a URL through the camera, and
#     nothing else: not a file of the host of neolink
#   - a say command in a config sent over MQTT does not replace the one of
#     the config file, nor runs
#   - a few sounds wait their turn, and the camera refuses more
#   - discovery gives Home Assistant a text to say and a notify entity
# Needs ffmpeg, python3 and mosquitto on PATH.
#
# Usage: iso-test/sim/mqtt-talk.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18614}
MQTT_PORT=${MQTT_PORT:-18876}
HTTP_PORT=${HTTP_PORT:-18880}
MQTT=(-h 127.0.0.1 -p "$MQTT_PORT")

build
start_mosquitto "$MQTT_PORT"
start_fakecam v 21000 21010
ffmpeg -v error -f lavfi -i "sine=frequency=440:duration=2" "$WORK/tone.wav"
python3 -m http.server "$HTTP_PORT" --bind 127.0.0.1 --directory "$WORK" > "$WORK/http.log" 2>&1 &
PIDS+=($!)

# The say command: every text sounds like the tone, and is kept
cat > "$WORK/say.sh" <<EOF
#!/bin/sh
cat > "$WORK/said.txt"
exec cat "$WORK/tone.wav"
EOF
chmod +x "$WORK/say.sh"
{
  neolink_toml "$RTSP_PORT" "$MQTT_PORT" v=21000
  echo '[cameras.mqtt]'
  echo "say_command = [\"$WORK/say.sh\"]"
  echo '[cameras.mqtt.discovery]'
  echo 'topic = "homeassistant"'
  echo 'features = ["talk"]'
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
# command TOPIC MESSAGE: publishes MESSAGE on neolink/v/TOPIC, and prints the
# reply that comes back there
command() {
  local out sub
  out=$(mktemp -p "$WORK")
  mosquitto_sub "${MQTT[@]}" -t "neolink/v/$1" -C 2 -W 90 > "$out" 2> /dev/null &
  sub=$!
  sleep 0.5
  mosquitto_pub "${MQTT[@]}" -t "neolink/v/$1" -m "$2"
  wait "$sub" || true
  # After the command itself
  sed -n 2p "$out"
}
# talks: how many times the camera talked
talks() {
  grep -c "talk stop:" "$WORK/fakecam-v.log" || true
}
# last_sound: the seconds of sound the camera got the last time it talked
last_sound() {
  grep "talk stop:" "$WORK/fakecam-v.log" | tail -n 1 | sed -E 's/.*: ([0-9.]+)s of sound.*/\1/'
}

wait_for_retained neolink/v/status connected 60 || fail "camera v never connected"

before=$(talks)
reply=$(command control/say "Bonjour")
if [ "$reply" = OK ] && [ "$(cat "$WORK/said.txt")" = Bonjour ] && [ "$(talks)" -gt "$before" ] \
  && python3 -c "import sys; sys.exit(not $(last_sound) >= 1.9)"; then
  echo "ok: control/say spoke the text through the camera, $(last_sound)s of it"
else
  flunk "control/say replied '$reply', said '$(cat "$WORK/said.txt" 2> /dev/null)', the camera got $(last_sound)s"
fi
reply=$(command control/say "   ")
if [ "$reply" = "FAIL: There is nothing to say" ]; then
  echo "ok: there is nothing to say for an empty text"
else
  flunk "control/say with an empty text replied '$reply'"
fi

before=$(talks)
reply=$(command control/talk "http://127.0.0.1:$HTTP_PORT/tone.wav")
if [ "$reply" = OK ] && [ "$(talks)" -gt "$before" ] \
  && python3 -c "import sys; sys.exit(not $(last_sound) >= 1.9)"; then
  echo "ok: control/talk played the sound at the URL through the camera, $(last_sound)s of it"
else
  flunk "control/talk replied '$reply', the camera got $(last_sound)s"
fi
reply=$(command control/talk "file://$WORK/tone.wav")
if [ "$reply" = "FAIL: Only an http or https URL plays" ]; then
  echo "ok: control/talk does not play a file of the host"
else
  flunk "control/talk with a file replied '$reply'"
fi

# Sent with a say command of its own, and a change, so that it applies
config=$(retained neolink/config)
evil=$(echo "$config" | sed \
  -e 's|^\[cameras\.mqtt\]$|&\nsay_command = ["touch", "'"$WORK"'/pwned"]|' \
  -e 's/^battery_update = 2000$/battery_update = 3000/')
mosquitto_sub "${MQTT[@]}" -t neolink/config/status -C 1 -W 30 > "$WORK/config-status.txt" 2> /dev/null &
sub=$!
sleep 0.5
mosquitto_pub "${MQTT[@]}" -t neolink/config -m "$evil"
wait "$sub" || true
# The camera's MQTT handler starts again with the new config: a command sent
# meanwhile is missed
sleep 3
reply=$(command control/say "again")
if [ "$(cat "$WORK/config-status.txt")" = "Ok(())" ] && [ "$reply" = OK ] \
  && [ "$(cat "$WORK/said.txt")" = again ] && [ ! -e "$WORK/pwned" ]; then
  echo "ok: a say command sent over MQTT neither replaces the file's nor runs"
else
  flunk "after a config with a say command of its own ($(cat "$WORK/config-status.txt")), control/say replied '$reply', $([ -e "$WORK/pwned" ] && echo "and that command ran")"
fi

# Six at once: four play in turn, two are refused
mosquitto_sub "${MQTT[@]}" -t neolink/v/control/say -C 12 -W 90 > "$WORK/queue.txt" 2> /dev/null &
sub=$!
sleep 0.5
for i in 1 2 3 4 5 6; do
  mosquitto_pub "${MQTT[@]}" -t neolink/v/control/say -m "number $i"
done
wait "$sub" || true
played=$(grep -cx OK "$WORK/queue.txt" || true)
refused=$(grep -c "^FAIL: The camera has 4 sounds to play already" "$WORK/queue.txt" || true)
if [ "$played" = 4 ] && [ "$refused" = 2 ]; then
  echo "ok: four sounds wait their turn, and two more are refused"
else
  flunk "of six sounds at once, $played played and $refused were refused"
fi

if retained homeassistant/text/neolink_v_say/v/config | grep -qF '"command_topic":"neolink/v/control/say"' \
  && retained homeassistant/notify/neolink_v_speaker/v/config | grep -qF '"command_topic":"neolink/v/control/say"'; then
  echo "ok: discovery gives Home Assistant a text to say and a notify entity"
else
  flunk "discovery gives no text to say or notify entity"
fi

finish
