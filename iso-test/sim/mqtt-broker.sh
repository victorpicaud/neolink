#!/usr/bin/env bash
# neolink and its MQTT broker:
#   - discovery tells Home Assistant when an entity is unavailable
#   - after a network failure that the broker only notices later, neolink and
#     its camera still show as connected
#   - a broker that restarts, and forgets everything, gets the state back,
#     and commands still get answers
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

# The broker restarts, and has forgotten everything
kill "$BROKER_PID"
wait "$BROKER_PID" 2> /dev/null || true
start_mosquitto "$MQTT_PORT"
if wait_for_retained neolink/m/status connected 90; then
  echo "ok: the state came back after the broker restarted"
else
  flunk "the state did not come back after the broker restarted: $(retained neolink/m/status)"
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
