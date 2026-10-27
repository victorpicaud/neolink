#!/usr/bin/env bash
# Video keeps time with the clock, whatever frame rate the camera claims:
#   - a camera that sends fewer frames than it claims, 10 a second for 25 as
#     in the dark: its clients used to fall further and further behind
#   - a camera whose frame rate neolink misreads, 10 a second read as 4: its
#     video used to run ahead of time
# Needs ffmpeg on PATH.
#
# Usage: iso-test/sim/stream-timing.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18559}
URL=rtsp://127.0.0.1:$RTSP_PORT

build
start_fakecam slow 19500 19510 --fps 10 --claimed-fps 25
start_fakecam misread 19501 19511 --fps 10
neolink_toml "$RTSP_PORT" none slow=19500 misread=19501 > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml"

# check_timing CAMERA: checks that 10s of the camera's video take about 10s
# to arrive
check_timing() {
  local start end elapsed rc=0
  start=$(date +%s.%N)
  timeout -k 2 60 ffmpeg -nostdin -v error -rtsp_transport tcp -i "$URL/$1/main" \
    -t 10 -c copy -f null - > "$WORK/timing-$1.log" 2>&1 || rc=$?
  end=$(date +%s.%N)
  elapsed=$(awk -v s="$start" -v e="$end" 'BEGIN { printf "%.1f", e - s }')
  if [ "$rc" != 0 ]; then
    flunk "$1: ffmpeg failed (rc=$rc): $(tail -n 3 "$WORK/timing-$1.log")"
  elif awk -v t="$elapsed" 'BEGIN { exit !(t >= 9 && t <= 14) }'; then
    echo "ok: 10s of $1's video took ${elapsed}s to arrive"
  else
    flunk "10s of $1's video took ${elapsed}s to arrive"
  fi
}

for camera in slow misread; do
  wait_for_codec "$URL/$camera/main" h264
  check_timing "$camera"
done

finish
