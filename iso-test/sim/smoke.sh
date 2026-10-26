#!/usr/bin/env bash
# End-to-end smoke test: `neolink rtsp` streaming from the fake camera, no
# hardware needed. Needs ffprobe on PATH.
#
# Usage: iso-test/sim/smoke.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

BC_PORT=${BC_PORT:-19000}
CTL_PORT=${CTL_PORT:-19010}
RTSP_PORT=${RTSP_PORT:-18554}

build
start_fakecam testcam "$BC_PORT" "$CTL_PORT"
sed -e "s/127.0.0.1:9000/127.0.0.1:$BC_PORT/" -e "s/bind_port = 8554/bind_port = $RTSP_PORT/" \
  "$ROOT/iso-test/sim/fakecam/testcam.toml" > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml"

wait_for_codec "rtsp://127.0.0.1:$RTSP_PORT/testcam/main" h264
echo "PASS"
