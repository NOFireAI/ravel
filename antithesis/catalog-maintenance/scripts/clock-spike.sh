#!/bin/bash
# The clock spike (README.md, "Clock spike"): run the local loop with every
# container under libfaketime, move every clock forward by one offset partway
# through, and report whether a fold sealed an hour that was ingested live.
#
# Usage: scripts/clock-spike.sh <out-dir> [seconds-before] [seconds-after] [offset]
#   seconds-before  workload time before the spike (default 120)
#   seconds-after   workload time after it (default 300)
#   offset          the libfaketime offset written to every clock (default +4h)
#
# Exit 0: the run passed and "clock: an hour ingested live was sealed by a
# fold" was true at least once. Exit 2: some clock did not move, so the rows
# that need the spike are recorded `none: needs clock control`. Exit 1: any
# other failure, including the sealing assertion never holding.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${1:?out dir}"
BEFORE="${2:-120}"
AFTER="${3:-300}"
OFFSET="${4:-+4h}"

CLOCK_SPIKE_AFTER_SECS="$BEFORE" CLOCK_SPIKE_OFFSET="$OFFSET" "$HERE/local-run.sh" "$OUT" "$AFTER"
rc=$?
if [ "$rc" -ne 0 ]; then
    exit "$rc"
fi
sealed=$(awk -F '\t' '$4 == "clock: an hour ingested live was sealed by a fold" { print $3 }' "$OUT/assertions.tsv")
if [ -n "$sealed" ] && [ "$sealed" -gt 0 ]; then
    echo "clock spike OK: a fold sealed a live hour ($sealed times)"
    exit 0
fi
echo "FAIL: the clocks moved, but no fold sealed a live hour"
exit 1
