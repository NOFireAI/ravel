#!/bin/bash
# The s3proxy open-file gate (issue #4633), which local-run.sh runs on the
# s3proxy container's log: every start of the proxy logs its open-file limit
# beside its connection cap, and no line reports "Too many open files". A
# proxy out of descriptors fails the servers' store requests, which they see
# as a store outage that no rule drew.
#
# Usage: scripts/proxy-log-check.sh <s3proxy-log>
#
# Exit 0: the log holds at least one startup line and no "Too many open
# files"; one summary line on stdout. Exit 1: it does not, or the log cannot
# be read; the reason on stdout.
set -uo pipefail
LOG="${1:?s3proxy log}"
LIMIT='open-file limit [0-9a-z]* ([a-z0-9 ]*, [0-9]* needed)'
STARTUP="s3-proxy: .* connections, $LIMIT"
EMFILE='Too many open files'

[ -s "$LOG" ] || { echo "the s3proxy log $LOG is empty or unreadable"; exit 1; }
starts=$(grep -c -- "$STARTUP" "$LOG")
emfile=$(grep -cF -- "$EMFILE" "$LOG")
for n in "$starts" "$emfile"; do
    case "$n" in
        '' | *[!0-9]*) echo "could not count lines in the s3proxy log $LOG"; exit 1 ;;
    esac
done
[ "$starts" -gt 0 ] || { echo "the s3proxy log $LOG holds no startup line stating the open-file limit"; exit 1; }
limits=$(grep -o -- "$LIMIT" "$LOG" | sort -u | tr '\n' ' ')
if [ "$emfile" -gt 0 ]; then
    echo "s3proxy logged '$EMFILE' $emfile times (${limits% })"
    exit 1
fi
echo "s3proxy: $starts start(s), ${limits% }, 0 lines with '$EMFILE'"
