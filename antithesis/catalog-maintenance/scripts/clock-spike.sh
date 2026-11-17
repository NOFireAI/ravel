#!/bin/bash
# The clock-jump check (README.md, "The clock jump"): run the local loop,
# whose compose file is the run's (every container under libfaketime, the
# driver jumping every clock once), and report on the jump alone: that it
# happened, that every probed clock moved by it, that a fold sealed an hour
# ingested live, and that a compaction record was published. The verdict is
# written to <out-dir>/clock-spike-result.txt as well as printed.
#
# Usage: scripts/clock-spike.sh <out-dir> [seconds]
#   seconds   passed to local-run.sh (workload time before the eventually)
#
# Exit 0: local-run.sh passed and every clock check held. Exit 1: anything
# else, including a clock probe that read nothing.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="${1:?out dir}"
SECS="${2:-240}"
mkdir -p "$OUT"
RESULT="$OUT/clock-spike-result.txt"
: > "$RESULT"
bad=0
note() {
    echo "$*" | tee -a "$RESULT"
}
check() {
    if "${@:2}"; then
        note "PASS $1"
    else
        note "FAIL $1"
        bad=1
    fi
}

"$HERE/local-run.sh" "$OUT" "$SECS"
rc=$?
check "local-run.sh exited 0 (exit $rc)" test "$rc" -eq 0

held() {
    local hits
    [ -r "$OUT/assertions.tsv" ] || return 1
    hits=$(awk -F '\t' -v m="$1" '$4 == m { print $3 }' "$OUT/assertions.tsv")
    [ -n "$hits" ] && [ "$hits" -gt 0 ]
}
for m in "clock: the driver jumped every clock forward" \
    "clock: an hour ingested live was sealed by a fold" \
    "catalog: a compaction record was published"; do
    check "held at least once: $m" held "$m"
done

# Every line of the probe local-run.sh wrote: the clock read, and ahead of the
# host by the jump less 60 s.
probe_ok() {
    local name reading jump host n=0
    read -r _ host _ jump < <(grep '^host ' "$OUT/clock-probe.txt" 2>/dev/null) || return 1
    case "$host$jump" in '' | *[!0-9]*) return 1 ;; esac
    [ "$jump" -gt 0 ] || return 1
    while read -r name reading; do
        [ "$name" = host ] && continue
        case "$reading" in '' | *[!0-9]*) return 1 ;; esac
        [ $((reading - host)) -ge $((jump - 60)) ] || return 1
        n=$((n + 1))
    done < "$OUT/clock-probe.txt"
    # rustfs twice and the four ravel-server containers.
    [ "$n" -eq 6 ]
}
check "every probed clock read and moved by the jump ($OUT/clock-probe.txt)" probe_ok

if [ "$bad" -ne 0 ]; then
    note "clock jump: FAIL"
    exit 1
fi
note "clock jump: PASS"
