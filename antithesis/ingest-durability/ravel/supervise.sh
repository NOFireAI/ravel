#!/bin/bash
# The coverage variant's ravel-server entrypoint (README.md, "Coverage
# variant"). Runs ravel-server with this script's arguments as its child.
#
# The driver asks for a graceful drain by writing a nonce to
# $RAVEL_CONTROL_DIR/drain-<hostname>. The supervisor moves the nonce to
# $RAVEL_CONTROL_DIR/draining-<hostname>, sends the child SIGTERM, writes
# "seq=<nonce> code=<exit code>" to $RAVEL_CONTROL_DIR/drained-<hostname> once
# it exits, and starts a new child. A drain cut short by a stop of the
# container is written as "seq=<nonce> interrupted", and so is a request or an
# unfinished drain found at startup, left behind by a container that was
# killed, so every request the supervisor saw gets an answer.
#
# Every exit the supervisor did not ask for ends the supervisor with the
# child's exit code, and a SIGTERM or SIGINT it receives is passed on to the
# child, so a container stop or kill behaves as it does in the base variant,
# where ravel-server is the entrypoint.
#
# RAVEL_SUPERVISE_SELF_CHECK=1 is the local loop's self-check: the first drain
# kills the child with SIGKILL instead, and every later one is taken and never
# answered, so the driver's drain properties must fail.
set -u
CONTROL="${RAVEL_CONTROL_DIR:-/control}"
HOST=$(hostname)
REQUEST="$CONTROL/drain-$HOST"
DRAINING="$CONTROL/draining-$HOST"
RESULT="$CONTROL/drained-$HOST"
READYZ="${RAVEL_SUPERVISE_READYZ:-http://127.0.0.1:4318/readyz}"
SELF_CHECK="${RAVEL_SUPERVISE_SELF_CHECK:-}"
# ravel-server reports ready a little before it installs its SIGTERM handler
# (#4493); a SIGTERM in between kills it with status 143, which is not a drain.
# A request waits this many polls of 0.2 s past the first ready. Counting
# sleeps keeps the wait whole when the wall clock jumps.
SETTLE_POLLS=25

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

# Write `content` to `path` through a temporary file and a rename, then sync
# it, so a reader never sees a partial file and a crash keeps what was written.
put() {
    local path="$1" content="$2" tmp
    tmp="$(dirname "$path")/.$(basename "$path").$$"
    printf '%s\n' "$content" > "$tmp" && mv -f "$tmp" "$path" && sync "$path" "$CONTROL"
}

report() {
    if put "$RESULT" "$1"; then
        rm -f "$DRAINING"
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
for left in "$DRAINING" "$REQUEST"; do
    if [ -e "$left" ]; then
        report "seq=$(cat "$left" 2> /dev/null) interrupted"
        rm -f "$left"
    fi
done

hung=""
while :; do
    if [ "$stopping" = 1 ]; then
        exit 143
    fi
    # The fault-switch profile: ravel-server reads RAVEL_ANTITHESIS_FAULTS
    # once at startup, so the driver turns the switches off for the
    # eventually by writing faults-off and asking for a drain; every start
    # after that, including one after a container restart, runs without them.
    if [ -n "${RAVEL_ANTITHESIS_FAULTS:-}" ] && [ -e "$CONTROL/faults-off" ]; then
        echo "supervise: $CONTROL/faults-off exists, starting without RAVEL_ANTITHESIS_FAULTS"
        unset RAVEL_ANTITHESIS_FAULTS
    fi
    echo "supervise: RAVEL_ANTITHESIS_FAULTS='${RAVEL_ANTITHESIS_FAULTS:-}'"
    /usr/local/bin/ravel-server "$@" &
    child=$!
    echo "supervise: started ravel-server, pid $child"
    ready_polls=-1
    drained=""
    while kill -0 "$child" 2> /dev/null; do
        if [ "$stopping" = 1 ]; then
            kill -TERM "$child" 2> /dev/null
            break
        fi
        if [ "$ready_polls" -lt 0 ]; then
            if curl -sf -o /dev/null --max-time 1 "$READYZ"; then
                ready_polls=0
            fi
        elif [ "$ready_polls" -lt "$SETTLE_POLLS" ]; then
            ready_polls=$((ready_polls + 1))
        elif [ -z "$hung" ] && [ -e "$REQUEST" ]; then
            seq=$(cat "$REQUEST" 2> /dev/null)
            put "$DRAINING" "$seq"
            rm -f "$REQUEST"
            if [ "$SELF_CHECK" = 1 ] && [ "$seq" != 1 ]; then
                echo "supervise: self-check, leaving drain $seq unanswered"
                hung=1
            else
                signal=TERM
                if [ "$SELF_CHECK" = 1 ]; then
                    signal=KILL
                fi
                echo "supervise: drain $seq requested, sending SIG$signal"
                kill "-$signal" "$child" 2> /dev/null
                reap
                if [ "$stopping" = 1 ]; then
                    report "seq=$seq interrupted"
                    exit "$code"
                fi
                report "seq=$seq code=$code"
                drained=1
                break
            fi
        fi
        sleep 0.2
    done
    if [ -z "$drained" ]; then
        reap
        echo "supervise: ravel-server exited with status $code"
        exit "$code"
    fi
done
