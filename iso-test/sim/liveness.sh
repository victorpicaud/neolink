#!/usr/bin/env bash
# neolink tells a camera that is there from one that went away:
#   - a camera that refuses pings, as some do, stays connected
#   - a camera that stops answering altogether is reconnected
# Needs ffprobe and python3 on PATH.
#
# Usage: iso-test/sim/liveness.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18561}
URL=rtsp://127.0.0.1:$RTSP_PORT

build
start_fakecam refusing 19900 19910 --refuse-pings
start_fakecam hanging 19901 19911
neolink_toml "$RTSP_PORT" none refusing=19900 hanging=19901 > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml"

# logins CAMERA: how many times the camera let neolink in
logins() {
  grep -c "login accepted" "$WORK/fakecam-$1.log" || true
}

wait_for_codec "$URL/refusing/main" h264
wait_for_codec "$URL/hanging/main" h264

# A dozen pings' worth
sleep 60
if [ "$(logins refusing)" = 1 ]; then
  echo "ok: a camera that refuses pings stayed connected"
else
  flunk "a camera that refuses pings was connected $(logins refusing) times in a minute"
fi

python3 "$CTL" 127.0.0.1:19911 hang > /dev/null 2>&1 || fail "could not hang the camera"
start=$SECONDS
lost=false
while [ $((SECONDS - start)) -lt 90 ]; do
  if grep -q "hanging: Connection Lost" "$WORK/neolink.log"; then
    lost=true
    break
  fi
  sleep 1
done
if $lost; then
  echo "ok: a camera that stopped answering was dropped after $((SECONDS - start))s"
else
  flunk "a camera that stopped answering was still thought connected after 90s"
fi

finish
