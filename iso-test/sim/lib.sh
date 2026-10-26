# Helpers shared by the fake-camera end-to-end tests. Source it from a test
# script, passing the script's arguments:
#
#   . "$(dirname "$0")/lib.sh" "$@"
#
# Arguments: --release builds and tests release binaries.
#
# Environment:
#   CARGO_TARGET_DIR    honoured
#   FAKECAM_TARGET_DIR  where fakecam builds (default $CARGO_TARGET_DIR/fakecam)
#   NEOLINK_BIN         test this neolink binary instead of building one, e.g.
#                       to compare against another branch
#
# Logs go to $WORK, which is removed on exit along with every process started
# here.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
PROFILE=debug
CARGO_FLAGS=()
if [ "${1:-}" = "--release" ]; then
  PROFILE=release
  CARGO_FLAGS=(--release)
fi
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
FAKE_TARGET=${FAKECAM_TARGET_DIR:-$TARGET/fakecam}
CTL="$ROOT/iso-test/sim/fakecam/ctl.py"

WORK=$(mktemp -d)
PIDS=()
cleanup() {
  kill "${PIDS[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# Builds neolink (unless NEOLINK_BIN is set) and fakecam
build() {
  if [ -z "${NEOLINK_BIN:-}" ]; then
    cargo build --manifest-path "$ROOT/Cargo.toml" "${CARGO_FLAGS[@]}"
    NEOLINK_BIN=$TARGET/$PROFILE/neolink
  fi
  CARGO_TARGET_DIR="$FAKE_TARGET" cargo build \
    --manifest-path "$ROOT/iso-test/sim/fakecam/Cargo.toml" "${CARGO_FLAGS[@]}"
  FAKECAM_BIN=$FAKE_TARGET/$PROFILE/fakecam
}

# start_fakecam NAME BC_PORT CONTROL_PORT [FAKECAM_ARGS...]
start_fakecam() {
  local name=$1 bc_port=$2 control_port=$3
  shift 3
  "$FAKECAM_BIN" --bind "127.0.0.1:$bc_port" --control "127.0.0.1:$control_port" "$@" \
    > "$WORK/fakecam-$name.log" 2>&1 &
  PIDS+=($!)
}

# start_neolink CONFIG [SUBCOMMAND]: runs neolink in the background
start_neolink() {
  "$NEOLINK_BIN" "${2:-rtsp}" --config "$1" > "$WORK/neolink.log" 2>&1 &
  NEO_PID=$!
  PIDS+=("$NEO_PID")
}

# camera_toml NAME BC_PORT [EXTRA_LINE...]: a [[cameras]] entry for a fakecam
camera_toml() {
  local name=$1 bc_port=$2
  shift 2
  cat <<EOF

[[cameras]]
name = "$name"
address = "127.0.0.1:$bc_port"
username = "admin"
password = "password123"
stream = "main"
discovery = "none"
max_encryption = "bcencrypt"
EOF
  local line
  for line in "$@"; do
    echo "$line"
  done
}

# probe URL SECONDS: prints the codecs of the streams at URL, or ffprobe's
# error; fails if ffprobe does
probe() {
  timeout "$2" ffprobe -v error -rtsp_transport tcp \
    -show_entries stream=codec_name -of csv=p=0 "$1" 2>&1
}

# wait_for_codec URL CODEC: waits up to a minute for URL to serve CODEC
wait_for_codec() {
  local out
  for _ in $(seq 1 60); do
    if out=$(probe "$1" 15) && echo "$out" | grep -qx "$2"; then
      echo "ok: $1 serves $2"
      return 0
    fi
    sleep 1
  done
  fail "$1 never served $2 (last answer: ${out:-none})"
}

# fail MESSAGE: prints the message and the logs, then exits
fail() {
  echo "FAIL: $*"
  local log
  for log in "$WORK"/*.log; do
    echo "--- $(basename "$log")"
    tail -n 60 "$log"
  done
  exit 1
}
