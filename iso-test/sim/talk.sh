#!/usr/bin/env bash
# neolink talk, sending a sound file for a camera to play, over a network
# that acknowledges talk 150ms late:
#   - a camera busy talking to another client is taken over
#   - the camera gets all of the sound
#   - the sound comes as fast as it plays, so that the camera never waits
#     for more
#   - talk stops once the sound has played
#   - a file whose path has spaces in it plays
#   - a file that is not sound ends talk with an error, at once
#   - talk without a file or the microphone says it needs one
# Needs ffmpeg and python3 on PATH.
#
# Usage: iso-test/sim/talk.sh [--release]
. "$(dirname "$0")/lib.sh" "$@"

build
start_fakecam t 20700 20710 --talk-busy --talk-ack-delay 150
neolink_toml 18574 none t=20700 > "$WORK/neolink.toml"
ffmpeg -v error -f lavfi -i "sine=frequency=440:duration=3" "$WORK/tone.wav"

# talk: sends the tone for camera t to play, and reads what the camera says of
# it into CODE, SOUND, STOPPED, AHEAD and SILENCE
talk() {
  local before stop
  before=$(grep -c "talk stop:" "$WORK/fakecam-t.log" || true)
  CODE=0
  timeout -k 2 60 "$NEOLINK_BIN" talk --config "$WORK/neolink.toml" -f "$WORK/tone.wav" t \
    > "$WORK/talk.log" 2>&1 || CODE=$?
  stop=$(grep "talk stop:" "$WORK/fakecam-t.log" | tail -n +$((before + 1)) | head -n 1 || true)
  # talk stop: 2.95s of sound in 46 messages, stopped after 3.05s, at most
  # 0.06s ahead, 0.00s of silence
  read -r SOUND STOPPED AHEAD SILENCE <<< "$(echo "$stop" | sed -E \
    's/.*: ([0-9.]+)s of sound .* after ([0-9.]+)s, at most ([0-9.]+)s ahead, ([0-9.]+)s of silence/\1 \2 \3 \4/')"
}

# is EXPRESSION: whether a python expression holds
is() {
  python3 -c "import sys; sys.exit(not ($1))"
}

# Busy: the camera refuses the first talk config
talk
if [ "$CODE" = 0 ] && grep -q "talk config refused: busy" "$WORK/fakecam-t.log" \
  && grep -q "talk config accepted" "$WORK/fakecam-t.log"; then
  echo "ok: a camera busy talking to another client was taken over"
else
  flunk "talk to a busy camera ended with $CODE: $(tail -n 2 "$WORK/talk.log" | tr '\n' ' ')"
fi

talk
if [ -z "$SOUND" ]; then
  flunk "the camera got no sound: talk ended with $CODE: $(tail -n 2 "$WORK/talk.log" | tr '\n' ' ')"
else
  if is "$SOUND >= 2.9"; then
    echo "ok: the camera got all ${SOUND}s of the sound"
  else
    flunk "the camera got ${SOUND}s of the 3s of sound"
  fi
  if is "$SILENCE <= 0.2 and $AHEAD <= 0.7"; then
    echo "ok: the sound came as fast as it played, with ${SILENCE}s of silence, at most ${AHEAD}s ahead"
  else
    flunk "the sound came with ${SILENCE}s of silence, at most ${AHEAD}s ahead"
  fi
  if is "$STOPPED >= $SOUND"; then
    echo "ok: talk stopped once the sound had played, after ${STOPPED}s"
  else
    flunk "talk stopped after ${STOPPED}s, with ${SOUND}s of sound"
  fi
fi

cp "$WORK/tone.wav" "$WORK/a tone.wav"
code=0
timeout -k 2 60 "$NEOLINK_BIN" talk --config "$WORK/neolink.toml" -f "$WORK/a tone.wav" t \
  > "$WORK/talk-spaces.log" 2>&1 || code=$?
if [ "$code" = 0 ]; then
  echo "ok: a file whose path has spaces in it plays"
else
  flunk "a file whose path has spaces in it ended talk with $code: $(tail -n 2 "$WORK/talk-spaces.log" | tr '\n' ' ')"
fi

echo "not a sound" > "$WORK/not-sound.wav"
code=0
timeout -k 2 30 "$NEOLINK_BIN" talk --config "$WORK/neolink.toml" -f "$WORK/not-sound.wav" t \
  > "$WORK/talk-not-sound.log" 2>&1 || code=$?
if [ "$code" = 1 ] && grep -q "Could not play the sound" "$WORK/talk-not-sound.log"; then
  echo "ok: a file that is not sound ends talk with an error"
else
  flunk "a file that is not sound ended talk with $code: $(tail -n 2 "$WORK/talk-not-sound.log" | tr '\n' ' ')"
fi

code=0
timeout -k 2 30 "$NEOLINK_BIN" talk --config "$WORK/neolink.toml" t \
  > "$WORK/talk-nothing.log" 2>&1 || code=$?
if [ "$code" = 2 ] && grep -q "required" "$WORK/talk-nothing.log"; then
  echo "ok: talk without a file or the microphone says it needs one"
else
  flunk "talk without a file or the microphone ended with $code: $(tail -n 2 "$WORK/talk-nothing.log" | tr '\n' ' ')"
fi

finish
