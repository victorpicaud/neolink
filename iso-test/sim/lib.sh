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
#   KEEP_LOGS           a directory to copy the logs to on exit
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
  if [ -n "${KEEP_LOGS:-}" ]; then
    mkdir -p "$KEEP_LOGS"
    cp "$WORK"/*.log "$KEEP_LOGS"/ 2>/dev/null || true
  fi
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

# start_mosquitto PORT: runs an MQTT broker on loopback
start_mosquitto() {
  printf 'listener %s 127.0.0.1\nallow_anonymous true\n' "$1" > "$WORK/mosquitto.conf"
  mosquitto -c "$WORK/mosquitto.conf" > "$WORK/mosquitto.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 50); do
    if mosquitto_pub -h 127.0.0.1 -p "$1" -t neolink-test/ping -m ping 2> /dev/null; then
      return 0
    fi
    sleep 0.1
  done
  fail "mosquitto did not start"
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

# neolink_toml RTSP_PORT MQTT_PORT [NAME=BC_PORT...]: a config serving these
# fakecams over RTSP, and over MQTT unless MQTT_PORT is "none"
neolink_toml() {
  local rtsp_port=$1 mqtt_port=$2 camera
  shift 2
  echo 'bind = "127.0.0.1"'
  echo "bind_port = $rtsp_port"
  if [ "$mqtt_port" != none ]; then
    printf '\n[mqtt]\nbroker_addr = "127.0.0.1"\nport = %s\n' "$mqtt_port"
  fi
  for camera in "$@"; do
    camera_toml "${camera%=*}" "${camera#*=}"
  done
}

# probe URL SECONDS: prints the codecs of the streams at URL, or ffprobe's
# error; fails if ffprobe does
probe() {
  timeout -k 2 "$2" ffprobe -v error -rtsp_transport tcp \
    -show_entries stream=codec_name -of csv=p=0 "$1" 2>&1
}

# wait_for_codec URL CODEC: waits up to a minute for URL to serve CODEC
wait_for_codec() {
  # By the clock: a probe can take its 15s every time, and 60 of them took
  # a quarter of an hour
  local out deadline=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if out=$(probe "$1" 15) && echo "$out" | grep -qx "$2"; then
      echo "ok: $1 serves $2"
      return 0
    fi
    sleep 1
  done
  fail "$1 never served $2 within a minute (last answer: ${out:-none})"
}

# wait_for_answer URL PATTERN SECONDS: waits up to SECONDS for ffprobe's
# answer from URL to match the grep pattern
wait_for_answer() {
  local out deadline=$((SECONDS + $3))
  while [ "$SECONDS" -lt "$deadline" ]; do
    out=$(probe "$1" 15) || true
    if echo "$out" | grep -q -- "$2"; then
      return 0
    fi
    sleep 1
  done
  echo "  last answer from $1: ${out:-none}"
  return 1
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

FAILURES=0
# flunk MESSAGE: records a failed check and carries on, so that one run shows
# everything that is broken
flunk() {
  echo "FAIL: $*"
  FAILURES=$((FAILURES + 1))
}

# finish: passes, or fails with the logs if any check flunked
finish() {
  if [ "$FAILURES" -gt 0 ]; then
    fail "$FAILURES check(s) failed"
  fi
  echo "PASS"
}
