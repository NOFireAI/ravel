#!/bin/bash
# The coverage variant's ravel-server entrypoint (README.md, "Coverage
# variant"). Runs ravel-server with this script's arguments as its child.
#
# The driver asks for a graceful drain by writing a nonce to
# $RAVEL_CONTROL_DIR/drain-<hostname>. The supervisor then sends the child
# SIGTERM, writes "seq=<nonce> code=<exit code>" to
# $RAVEL_CONTROL_DIR/drained-<hostname> once it exits, and starts a new child.
# A drain cut short by a stop of the container is written as
# "seq=<nonce> interrupted", and so is a request found at startup, left behind
# by a container that was killed before it acted on it.
#
# Every exit the supervisor did not ask for ends the supervisor with the
# child's exit code, and a SIGTERM or SIGINT it receives is passed on to the
# child, so a container stop or kill behaves as it does in the base variant,
# where ravel-server is the entrypoint.
set -u
CONTROL="${RAVEL_CONTROL_DIR:-/control}"
HOST=$(hostname)
REQUEST="$CONTROL/drain-$HOST"
RESULT="$CONTROL/drained-$HOST"
READYZ="${RAVEL_SUPERVISE_READYZ:-http://127.0.0.1:4318/readyz}"
# ravel-server installs its SIGTERM handler after its startup reads, just after
# it starts reporting ready. A SIGTERM before that kills it with status 143,
# which is not a drain, so a request waits this long past the first ready.
SETTLE_SECS=2

child=0
stopping=0
code=0

on_signal() {
    stopping=1
    if [ "$child" -gt 0 ]; then
        kill -TERM "$child" 2> /dev/null
    fi
}
trap on_signal TERM INT

report() {
    local tmp="$CONTROL/.drained-$HOST.$$"
    if printf '%s\n' "$1" > "$tmp" && mv -f "$tmp" "$RESULT"; then
        echo "supervise: reported $1"
    else
        echo "supervise: could not write $RESULT ($1)"
    fi
}

# Wait for the child to exit and set `code` to its exit status. A trapped
# signal makes `wait` return early with a status above 128 while the child
# still runs, so wait again until it is gone. When that early return races the
# child's exit, bash still holds the real status, and one more `wait` returns
# it; once the status has been collected, that `wait` answers 127 instead.
reap() {
    local again
    while :; do
        wait "$child"
        code=$?
        if ! kill -0 "$child" 2> /dev/null; then
            wait "$child" 2> /dev/null
            again=$?
            if [ "$again" -ne 127 ]; then
                code=$again
            fi
            return
        fi
    done
}

mkdir -p "$CONTROL"
if [ -e "$REQUEST" ]; then
    report "seq=$(cat "$REQUEST" 2> /dev/null) interrupted"
    rm -f "$REQUEST"
fi

while :; do
    if [ "$stopping" = 1 ]; then
        exit 143
    fi
    /usr/local/bin/ravel-server "$@" &
    child=$!
    echo "supervise: started ravel-server, pid $child"
    ready_at=""
    drained=""
    while kill -0 "$child" 2> /dev/null; do
        if [ "$stopping" = 1 ]; then
            kill -TERM "$child" 2> /dev/null
            break
        fi
        if [ -z "$ready_at" ]; then
            if curl -sf -o /dev/null --max-time 1 "$READYZ"; then
                ready_at=$(date +%s)
            fi
        elif [ -e "$REQUEST" ] && [ $(($(date +%s) - ready_at)) -ge "$SETTLE_SECS" ]; then
            seq=$(cat "$REQUEST" 2> /dev/null)
            rm -f "$REQUEST"
            echo "supervise: drain $seq requested, sending SIGTERM"
            kill -TERM "$child" 2> /dev/null
            reap
            if [ "$stopping" = 1 ]; then
                report "seq=$seq interrupted"
                exit "$code"
            fi
            report "seq=$seq code=$code"
            drained=1
            break
        fi
        sleep 0.2
    done
    if [ -z "$drained" ]; then
        reap
        echo "supervise: ravel-server exited with status $code"
        exit "$code"
    fi
done
