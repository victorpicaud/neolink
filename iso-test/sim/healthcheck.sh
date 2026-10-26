#!/usr/bin/env bash
# neolink healthcheck, as a container's HEALTHCHECK runs it:
#   - healthy while neolink serves, with heartbeats from neolink, its RTSP
#     server and its camera
#   - still healthy once the camera is gone, which restarting neolink would
#     not bring back
#   - unhealthy while neolink is frozen, saying what froze, and healthy again
#     once it runs again
# Needs ffprobe and python3 on PATH.
#
# Usage: iso-test/sim/healthcheck.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18572}
URL=rtsp://127.0.0.1:$RTSP_PORT
export NEOLINK_HEALTH_DIR=$WORK/health

build
start_fakecam h 20600 20610
FAKECAM_PID=${PIDS[-1]}
neolink_toml "$RTSP_PORT" none h=20600 > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml" rtsp
wait_for_codec "$URL/h/main" h264
sleep 2

# healthcheck [ARGS...]: prints what the healthcheck says, and fails as it does
healthcheck() {
  "$NEOLINK_BIN" healthcheck "$@" 2>&1
}

beats=$(ls "$NEOLINK_HEALTH_DIR" 2> /dev/null | tr '\n' ' ')
if out=$(healthcheck) && [ "$beats" = "camera-h neolink rtsp " ]; then
  echo "ok: healthy while neolink serves, with heartbeats from $beats"
else
  flunk "not healthy while neolink serves: $out (heartbeats: ${beats:-none})"
fi

kill "$FAKECAM_PID"
sleep 15
if out=$(healthcheck h); then
  echo "ok: still healthy once the camera is gone"
else
  flunk "unhealthy once the camera is gone: $out"
fi

kill -STOP "$NEO_PID"
sleep 12
if out=$(healthcheck); then
  flunk "healthy while neolink was frozen: $out"
elif echo "$out" | grep -q "^neolink: no sign of life"; then
  echo "ok: unhealthy while neolink was frozen: $(echo "$out" | head -n 3 | tr '\n' ';')"
else
  flunk "unhealthy while neolink was frozen, without saying so: $out"
fi
kill -CONT "$NEO_PID"
sleep 3
if out=$(healthcheck); then
  echo "ok: healthy again once neolink runs again"
else
  flunk "still unhealthy once neolink runs again: $out"
fi

finish
