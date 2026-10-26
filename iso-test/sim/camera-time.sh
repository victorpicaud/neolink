#!/usr/bin/env bash
# Setting a camera's time keeps its time zone: it used to be reset to UTC.
# Needs ffprobe on PATH.
#
# Usage: iso-test/sim/camera-time.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18565}

build
# UTC+1, as Reolink counts it
start_fakecam clock 20300 20310 --time-zone -3600
{
  echo 'bind = "127.0.0.1"'
  echo "bind_port = $RTSP_PORT"
  camera_toml clock 20300 "update_time = true"
} > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml"

start=$SECONDS
while [ $((SECONDS - start)) -lt 30 ] && ! grep -q "set_time:" "$WORK/fakecam-clock.log"; do
  sleep 0.5
done
set_time=$(grep -m1 "set_time:" "$WORK/fakecam-clock.log" || true)
if echo "$set_time" | grep -q "time_zone Some(-3600)"; then
  echo "ok: the camera's time was set in its own time zone"
elif [ -z "$set_time" ]; then
  flunk "neolink never set the camera's time"
else
  flunk "the camera's time zone was changed: ${set_time#*set_time: }"
fi

finish
