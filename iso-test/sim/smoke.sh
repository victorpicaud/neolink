#!/usr/bin/env bash
# End-to-end smoke test: `neolink rtsp` streaming from the fake camera, no
# hardware needed. Needs ffprobe on PATH.
#
# Usage: iso-test/sim/smoke.sh [--release]
#
# CARGO_TARGET_DIR is honoured; the fake camera builds into
# $CARGO_TARGET_DIR/fakecam (or target/fakecam).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PROFILE=debug
CARGO_FLAGS=()
if [ "${1:-}" = "--release" ]; then
  PROFILE=release
  CARGO_FLAGS=(--release)
fi
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
FAKE_TARGET=${FAKECAM_TARGET_DIR:-$TARGET/fakecam}

BC_PORT=${BC_PORT:-19000}
CTL_PORT=${CTL_PORT:-19010}
RTSP_PORT=${RTSP_PORT:-18554}

cargo build --manifest-path "$ROOT/Cargo.toml" "${CARGO_FLAGS[@]}"
CARGO_TARGET_DIR="$FAKE_TARGET" cargo build \
  --manifest-path "$ROOT/iso-test/sim/fakecam/Cargo.toml" "${CARGO_FLAGS[@]}"

WORK=$(mktemp -d)
cleanup() {
  kill "${NEO_PID:-}" "${CAM_PID:-}" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

"$FAKE_TARGET/$PROFILE/fakecam" --bind "127.0.0.1:$BC_PORT" --control "127.0.0.1:$CTL_PORT" \
  > "$WORK/fakecam.log" 2>&1 &
CAM_PID=$!

sed -e "s/127.0.0.1:9000/127.0.0.1:$BC_PORT/" -e "s/bind_port = 8554/bind_port = $RTSP_PORT/" \
  "$ROOT/iso-test/sim/fakecam/testcam.toml" > "$WORK/neolink.toml"
"$TARGET/$PROFILE/neolink" rtsp --config "$WORK/neolink.toml" > "$WORK/neolink.log" 2>&1 &
NEO_PID=$!

URL="rtsp://127.0.0.1:$RTSP_PORT/testcam/main"
for _ in $(seq 1 60); do
  if out=$(timeout 15 ffprobe -v error -rtsp_transport tcp -select_streams v:0 \
      -show_entries stream=codec_name,width,height -of csv=p=0 "$URL" 2>&1) \
      && echo "$out" | grep -q h264; then
    echo "PASS: $URL -> $out"
    exit 0
  fi
  sleep 1
done

echo "FAIL: no H.264 stream at $URL"
echo "--- neolink.log"; tail -n 60 "$WORK/neolink.log"
echo "--- fakecam.log"; tail -n 20 "$WORK/fakecam.log"
exit 1
