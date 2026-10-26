#!/usr/bin/env bash
# The clients of a stream share one camera stream:
#   - clients watching at the same time start the camera's stream once
#   - the camera stops streaming once nobody watches
#   - a camera paused until motion still shows a new client a picture, and
#     then stops streaming
# Needs ffmpeg and ffprobe on PATH.
#
# Usage: iso-test/sim/shared-stream.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18557}
URL=rtsp://127.0.0.1:$RTSP_PORT

build
start_fakecam shared 19300 19310
start_fakecam paused 19301 19311
{
  echo 'bind = "127.0.0.1"'
  echo "bind_port = $RTSP_PORT"
  camera_toml shared 19300
  # The fake camera never sees motion
  camera_toml paused 19301 "pause = { on_motion = true }"
} > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml"

# started CAMERA / stopped CAMERA: how many video streams the camera started,
# and stopped
started() {
  grep -c "start_video" "$WORK/fakecam-$1.log" || true
}
stopped() {
  grep -c "stop_video" "$WORK/fakecam-$1.log" || true
}

# check_idle CAMERA: checks that the camera stops every stream it started
# within 30s
check_idle() {
  local start=$SECONDS
  while [ $((SECONDS - start)) -lt 30 ]; do
    if [ "$(stopped "$1")" -ge "$(started "$1")" ]; then
      echo "ok: $1 stopped streaming $((SECONDS - start))s after its last client left"
      return 0
    fi
    sleep 0.5
  done
  flunk "$1 still streams 30s after its last client left" \
    "($(started "$1") streams started, $(stopped "$1") stopped)"
}

wait_for_codec "$URL/shared/main" h264
check_idle shared

before=$(started shared)
CLIENTS=()
for client in 1 2; do
  timeout 30 ffmpeg -v error -rtsp_transport tcp -i "$URL/shared/main" \
    -frames:v 60 -c copy -f null - > "$WORK/client$client.log" 2>&1 &
  CLIENTS+=($!)
done
served=0
for pid in "${CLIENTS[@]}"; do
  if wait "$pid"; then
    served=$((served + 1))
  fi
done
if [ "$served" = 2 ]; then
  echo "ok: both clients got their frames"
else
  flunk "$((2 - served)) of 2 clients did not get their frames"
fi
streams=$(($(started shared) - before))
if [ "$streams" = 1 ]; then
  echo "ok: two clients at once started the camera's stream once"
else
  flunk "two clients at once started the camera's stream $streams times"
fi
check_idle shared

if out=$(probe "$URL/paused/main" 30) && echo "$out" | grep -qx h264; then
  echo "ok: the paused camera showed a new client a picture"
else
  flunk "the paused camera showed a new client no picture: $out"
fi
check_idle paused

finish
