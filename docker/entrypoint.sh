#!/bin/sh

ulimit -n 65535 2> /dev/null \
    || echo "Could not raise the limit of open files, which stays at $(ulimit -n)"

# The default command, built here so that NEO_LINK_MODE still picks it
if [ "$#" -eq 0 ]; then
    set -- /usr/local/bin/neolink "${NEO_LINK_MODE:-rtsp}" --config /etc/neolink.toml
fi

echo "Running: ${*}"
# neolink takes this shell's place as process 1, and so gets the signal to
# stop. This shell held it until docker gave up and killed them both.
exec "$@"
