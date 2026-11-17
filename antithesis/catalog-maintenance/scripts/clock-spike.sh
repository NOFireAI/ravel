#!/bin/bash
# The clock-jump check (README.md, "The clock jump"): run the local loop,
# whose compose file is the run's (every container under libfaketime, the
# driver jumping every clock to seal the live hours and then twice more for
# the sweep), and report on the jumps alone: that they happened, that every
# probed clock moved by their total, that a fold sealed an hour ingested
# live, that a compaction record was published, and that a sweep past the
# jumps deleted one of its inputs. The verdict is
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
check "local-run.sh exit code is 0 (got $rc)" test "$rc" -eq 0

held() {
    local hits
    [ -r "$OUT/assertions.tsv" ] || return 1
    hits=$(awk -F '\t' -v m="$1" '$4 == m { print $3 }' "$OUT/assertions.tsv")
    [ -n "$hits" ] && [ "$hits" -gt 0 ]
}
for m in "clock: the driver's clock jumped forward" \
    "clock: an hour ingested live was sealed by a fold" \
    "catalog: a compaction record was published" \
    "clock: the clocks jumped past the sweep's protection horizon and pinned-query window" \
    "catalog: sweep deleted a superseded compaction input"; do
    check "held at least once: $m" held "$m"
done

# Every line of the probe local-run.sh wrote: the clock read, and ahead of the
# host by the jumps' total less 60 s.
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
