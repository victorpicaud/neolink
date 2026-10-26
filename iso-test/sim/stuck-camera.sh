#!/usr/bin/env bash
# A camera that stops sending video must not hold up anyone else, and its own
# clients must get an answer within the bounded wait:
#   - a client of a healthy camera is served while the stuck camera's clients
#     wait
#   - the stuck camera's clients get the splash screen, or a refusal with
#     use_splash = false, within about 20s
#   - a camera that never connects refuses clients at once, without GLib
#     CRITICALs
#   - a second neolink on the same port exits with an error
# Needs ffprobe and python3 on PATH.
#
# Usage: iso-test/sim/stuck-camera.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18555}
URL=rtsp://127.0.0.1:$RTSP_PORT

build
start_fakecam healthy 19100 19110
start_fakecam stuck 19101 19111
start_fakecam nosplash 19102 19112
{
  echo 'bind = "127.0.0.1"'
  echo "bind_port = $RTSP_PORT"
  camera_toml healthy 19100
  camera_toml stuck 19101
  camera_toml nosplash 19102 "use_splash = false"
  # Nothing listens on 19103, so this camera never connects
  camera_toml offline 19103 "use_splash = false"
} > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml"

for camera in healthy stuck nosplash; do
  wait_for_codec "$URL/$camera/main" h264
done

for control_port in 19111 19112; do
  python3 "$CTL" "127.0.0.1:$control_port" freeze > /dev/null 2>&1 \
    || fail "could not freeze the camera on control port $control_port"
done

PROBES=()
# timed_probe CAMERA: probes the camera's main stream in the background,
# recording the answer and how long it took
timed_probe() {
  (
    start=$SECONDS
    rc=0
    probe "$URL/$1/main" 60 > "$WORK/$1.out" || rc=$?
    echo "$rc $((SECONDS - start))" > "$WORK/$1.result"
  ) &
  PROBES+=($!)
}
timed_probe stuck
timed_probe nosplash
timed_probe offline
sleep 2

if out=$(probe "$URL/healthy/main" 10) && echo "$out" | grep -qx h264; then
  echo "ok: the healthy camera is served while the stuck camera's clients wait"
else
  flunk "the healthy camera was not served while the stuck camera's clients waited: $out"
fi

wait "${PROBES[@]}"

# check_probe CAMERA EXPECTED MAX_SECONDS: EXPECTED is a codec, or "refused"
check_probe() {
  local rc elapsed out
  read -r rc elapsed < "$WORK/$1.result"
  out=$(cat "$WORK/$1.out")
  if [ "$elapsed" -gt "$3" ]; then
    flunk "$1: answered after ${elapsed}s, more than $3s (rc=$rc: $out)"
  elif [ "$2" = refused ]; then
    if [ "$rc" = 0 ] || [ "$rc" = 124 ]; then
      flunk "$1: expected a refusal, got rc=$rc: $out"
    else
      echo "ok: $1 refused the client after ${elapsed}s"
    fi
  elif [ "$rc" != 0 ] || ! echo "$out" | grep -qx "$2"; then
    flunk "$1: expected $2, got rc=$rc: $out"
  else
    echo "ok: $1 served $2 after ${elapsed}s"
  fi
}
check_probe stuck mjpeg 35
check_probe nosplash refused 35
check_probe offline refused 5

if grep -q CRITICAL "$WORK/neolink.log"; then
  flunk "GLib CRITICALs in the log: $(grep -m1 CRITICAL "$WORK/neolink.log")"
else
  echo "ok: no GLib CRITICALs"
fi

start=$SECONDS
rc=0
timeout 30 "$NEOLINK_BIN" rtsp --config "$WORK/neolink.toml" > "$WORK/second.log" 2>&1 || rc=$?
if [ "$rc" = 0 ] || [ "$rc" = 124 ] || ! grep -q "Could not listen for RTSP" "$WORK/second.log"; then
  flunk "a second neolink on port $RTSP_PORT did not stop with an error (rc=$rc)"
else
  echo "ok: a second neolink on the same port exits with an error after $((SECONDS - start))s"
fi

finish
