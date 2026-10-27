#!/usr/bin/env bash
# neolink and its MQTT broker:
#   - discovery tells Home Assistant when an entity is unavailable
#   - after a network failure that the broker only notices later, neolink and
#     its camera still show as connected
#   - a broker that restarts, and forgets everything, gets the state back,
#     and commands still get answers
#   - a config the broker kept from before neolink connected is not applied
#   - two configs sent at once are applied once each, rather than each undoing
#     the other endlessly
#   - a config sent over MQTT that points a camera elsewhere does not send
#     its password there
#   - a broker CA that cannot be read stops neolink from connecting, rather
#     than connecting without TLS
# Needs python3 and mosquitto on PATH.
#
# Usage: iso-test/sim/mqtt-broker.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18564}
MQTT_PORT=${MQTT_PORT:-18836}
MQTT=(-h 127.0.0.1 -p "$MQTT_PORT")

build
start_mosquitto "$MQTT_PORT"
BROKER_PID=${PIDS[-1]}
# neolink reaches the broker through a proxy, which can fail as a network does
PROXY_PORT=$((MQTT_PORT + 2))
python3 "$ROOT/iso-test/sim/tcp_proxy.py" "$PROXY_PORT" "$MQTT_PORT" > "$WORK/proxy.log" 2>&1 &
PROXY_PID=$!
PIDS+=("$PROXY_PID")
start_fakecam m 20200 20210
# Where a config sent over MQTT points the camera, and should not send its
# password
start_fakecam elsewhere 20201 20211
{
  neolink_toml "$RTSP_PORT" "$PROXY_PORT" m=20200
  echo '[cameras.mqtt.discovery]'
  echo 'topic = "homeassistant"'
  echo 'features = ["motion"]'
} > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml" mqtt-rtsp

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

discovery=$(mosquitto_sub "${MQTT[@]}" -t 'homeassistant/#' -C 1 -W 5 2> /dev/null || true)
if echo "$discovery" | grep -q '"payload_not_available":"disconnected"' \
  && echo "$discovery" | grep -q '"topic":"neolink/status"'; then
  echo "ok: discovery tells Home Assistant when an entity is unavailable"
else
  flunk "discovery does not tell Home Assistant when an entity is unavailable: $discovery"
fi

# The network to the broker fails. The broker notices only once the old
# connection times out, and publishes its last will then, after neolink
# connected again.
kill -USR1 "$PROXY_PID"
sleep 15
status=$(retained neolink/status)
camera=$(retained neolink/m/status)
if [ "$status" = connected ] && [ "$camera" = connected ]; then
  echo "ok: after a network failure, neolink and its camera still show as connected"
else
  flunk "after a network failure, neolink shows as $status, and its camera as $camera"
fi

# The broker restarts, and has forgotten everything. Before neolink is
# back, the broker is given a config without the camera to keep, as one
# from an earlier run.
kill -USR2 "$PROXY_PID"
kill "$BROKER_PID"
wait "$BROKER_PID" 2> /dev/null || true
start_mosquitto "$MQTT_PORT"
{
  echo 'cameras = []'
  neolink_toml "$RTSP_PORT" "$PROXY_PORT"
} | mosquitto_pub "${MQTT[@]}" -t neolink/config -r -s
kill -USR2 "$PROXY_PID"
if wait_for_retained neolink/m/status connected 90; then
  echo "ok: the state came back after the broker restarted"
else
  flunk "the state did not come back after the broker restarted: $(retained neolink/m/status)"
fi
sleep 2
applied=$(grep -c "Updated config" "$WORK/neolink.log" || true)
if [ "$applied" = 0 ]; then
  echo "ok: a config the broker kept from before neolink connected was not applied"
else
  flunk "a config the broker kept from before neolink connected was applied, $applied times"
fi

# The handler answers on the command's own topic: OK, or FAIL here since the
# fake camera has no LED
mosquitto_sub "${MQTT[@]}" -t neolink/m/control/led -C 2 -W 20 > "$WORK/led.out" 2>&1 &
reader=$!
sleep 1
mosquitto_pub "${MQTT[@]}" -t neolink/m/control/led -m on
wait "$reader" || true
if grep -qE "^(OK|FAIL)" "$WORK/led.out"; then
  echo "ok: commands still get answers after the broker restarted"
else
  flunk "a command got no answer after the broker restarted: $(tr '\n' ' ' < "$WORK/led.out")"
fi

# Two configs sent at the same moment: neolink hears its own when it
# publishes them back, and should not take those for new ones
for backoff in 5 7; do
  {
    neolink_toml "$RTSP_PORT" "$PROXY_PORT" m=20200
    echo "max_reconnect_backoff = $backoff"
  } > "$WORK/config-$backoff.toml"
done
mosquitto_pub "${MQTT[@]}" -t neolink/config -f "$WORK/config-5.toml" &
first=$!
mosquitto_pub "${MQTT[@]}" -t neolink/config -f "$WORK/config-7.toml"
wait "$first" || true
sleep 5
applied=$(grep -c "Updated config" "$WORK/neolink.log" || true)
if [ "$applied" -le 2 ]; then
  echo "ok: two configs sent at once were applied once each"
else
  flunk "two configs sent at once were applied $applied times"
fi

# A config sent over MQTT that points the camera elsewhere, and leaves its
# password out
{
  neolink_toml "$RTSP_PORT" "$PROXY_PORT"
  camera_toml m 20201 | grep -v '^password'
} | mosquitto_pub "${MQTT[@]}" -t neolink/config -s
for _ in $(seq 1 30); do
  if grep -q "login \(accepted\|rejected\)" "$WORK/fakecam-elsewhere.log"; then
    break
  fi
  sleep 1
done
if grep -q "login accepted" "$WORK/fakecam-elsewhere.log"; then
  flunk "a config sent over MQTT sent the camera's password elsewhere"
elif grep -q "login rejected" "$WORK/fakecam-elsewhere.log"; then
  echo "ok: a camera pointed elsewhere over MQTT did not get its password"
else
  flunk "the camera pointed elsewhere over MQTT never went there"
fi

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
