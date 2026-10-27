#!/usr/bin/env bash
# A camera that stops sending video while it answers everything else:
#   - its stream is restarted, and its client gets video again
#   - when restarting the stream brings no video back, the camera is
#     reconnected, and the client gets video again once the camera recovers
# Needs ffmpeg and python3 on PATH.
#
# Usage: iso-test/sim/stalled-stream.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

RTSP_PORT=${RTSP_PORT:-18560}
URL=rtsp://127.0.0.1:$RTSP_PORT/stalled/main
CONTROL=127.0.0.1:19710

build
start_fakecam stalled 19700 19710
neolink_toml "$RTSP_PORT" none stalled=19700 > "$WORK/neolink.toml"
start_neolink "$WORK/neolink.toml"

started() {
  grep -c "start_video" "$WORK/fakecam-stalled.log" || true
}
logins() {
  grep -c "login accepted" "$WORK/fakecam-stalled.log" || true
}
# Whether the camera started more streams, or logins, than $before
more_streams() {
  [ "$(started)" -gt "$before" ]
}
more_logins() {
  [ "$(logins)" -gt "$before" ]
}
control() {
  python3 "$CTL" "$CONTROL" "$1" > /dev/null 2>&1 || fail "could not tell the camera to $1"
}
# wait_until SECONDS COMMAND...: waits up to SECONDS for COMMAND to succeed
wait_until() {
  local deadline=$((SECONDS + $1))
  shift
  until "$@"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      return 1
    fi
    sleep 0.5
  done
}
# start_client FRAMES: a client in the background that watches until it got FRAMES
start_client() {
  timeout -k 2 150 ffmpeg -nostdin -v error -rtsp_transport tcp -i "$URL" \
    -frames:v "$1" -c copy -f null - > "$WORK/client.log" 2>&1 &
  CLIENT=$!
}

wait_for_codec "$URL" h264

# 40s of video at 15 fps, with the camera silent for a while in between
start_client 600
sleep 5
before=$(started)
control drop
start=$SECONDS
if wait_until 30 more_streams; then
  echo "ok: the stream was restarted $((SECONDS - start))s after its video stopped"
else
  flunk "the stream was not restarted within 30s of its video stopping"
fi
rc=0
wait "$CLIENT" || rc=$?
if [ "$rc" = 0 ]; then
  echo "ok: the client got video again"
else
  flunk "the client did not get video again (ffmpeg exited with $rc, 124 for a timeout)"
fi

start_client 300
sleep 5
before=$(logins)
control freeze
start=$SECONDS
if wait_until 60 more_logins; then
  echo "ok: the camera was reconnected $((SECONDS - start))s after restarts brought no video back"
else
  flunk "the camera was not reconnected within 60s of its video stopping for good"
fi
control normal
rc=0
wait "$CLIENT" || rc=$?
if [ "$rc" = 0 ]; then
  echo "ok: the client got video again once the camera recovered"
else
  flunk "the client did not get video again once the camera recovered" \
    "(ffmpeg exited with $rc, 124 for a timeout)"
fi

finish
