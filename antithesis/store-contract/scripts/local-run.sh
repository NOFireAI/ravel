#!/bin/bash
# The local loop (Antithesis skill, workload/local-loop.md), on the Linux
# build host: start the stack from scratch, play the driver's first_ command,
# let the workload run, then collect the driver's SDK output and logs and
# list the assertions that would fail in a run.
#
# Usage: scripts/local-run.sh <out-dir> [seconds]
#   RAVEL_ANTITHESIS_MUTANT=common/<stem>  mutant control: the run passes only
#                                          if the property that mutant names in
#                                          formal/tla/common/antithesis-mutants.tsv
#                                          was violated.
#   WORKLOAD_MAX_ACTIONS=N                 the driver ends the timeline after N
#                                          actions; the run then waits up to
#                                          [seconds] for it to exit 0 instead of
#                                          sleeping, and fails if it does not.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
TSV="$WL/../../formal/tla/common/antithesis-mutants.tsv"
OUT="${1:?out dir}"
SECS="${2:-300}"
MUTANT="${RAVEL_ANTITHESIS_MUTANT:-}"
mkdir -p "$OUT"
export COMPOSE_FILE="$WL/config/docker-compose.yaml:$WL/local/docker-compose.local.yaml"
export COMPOSE_PROJECT_NAME=ravel-antithesis-store-contract

expected=""
if [ -n "$MUTANT" ]; then
    expected=$(awk -F'\t' -v m="$MUTANT" '$2 == m { print $3 }' "$TSV")
    if [ -z "$expected" ]; then
        echo "refused: $MUTANT is not listed in $TSV"
        exit 1
    fi
    echo "mutant $MUTANT: expecting \"$expected\" to be violated"
fi

docker compose down -v > /dev/null 2>&1
docker compose up -d || { echo "compose up failed"; exit 1; }
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload \
    || { echo "first_start_workload failed"; exit 1; }
start=$(date +%s)
until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl; do
    if [ $(( $(date +%s) - start )) -gt 600 ]; then
        echo "setup complete never arrived"; docker compose logs driver | tail -40; exit 1
    fi
    sleep 5
done
echo "setup complete after $(( $(date +%s) - start ))s"

# Why the driver's run is invalid, gated below; empty when it is valid. The
# state is read before `docker compose stop`, which would set a signal code.
# Bounded (WORKLOAD_MAX_ACTIONS): the driver must have exited 0 by the
# deadline. Sleep mode: a driver still running at the end is expected, and one
# that is no longer running must have exited 0.
driver_fail=""
driver_state() { docker inspect -f '{{.State.Running}}' driver 2>/dev/null; }
check_exited_zero() {
    local code
    code=$(docker inspect -f '{{.State.ExitCode}}' driver 2>/dev/null)
    echo "driver exit code ${code:-unknown}"
    [ "$code" = 0 ] || driver_fail="driver exited ${code:-with an unknown code}$1"
}
if [ -n "${WORKLOAD_MAX_ACTIONS:-}" ]; then
    deadline=$(( $(date +%s) + SECS ))
    while [ "$(driver_state)" = true ] && [ "$(date +%s)" -lt "$deadline" ]; do
        sleep 5
    done
else
    sleep "$SECS"
fi
running=$(driver_state)
case "$running" in
    true)
        if [ -n "${WORKLOAD_MAX_ACTIONS:-}" ]; then
            driver_fail="driver still running after ${SECS}s with WORKLOAD_MAX_ACTIONS=$WORKLOAD_MAX_ACTIONS"
        fi
        ;;
    false)
        if [ -n "${WORKLOAD_MAX_ACTIONS:-}" ]; then
            check_exited_zero ""
        else
            check_exited_zero " before the ${SECS}s run ended"
        fi
        ;;
    *) driver_fail="could not read the driver container's state" ;;
esac
[ -z "$driver_fail" ] || echo "$driver_fail"

docker compose exec -T driver sh -c 'curl --version | head -1; jq --version' \
    > "$OUT/debug-tools.txt" 2>&1
docker compose cp "driver:/tmp/sdk.jsonl" "$OUT/sdk-driver.jsonl" > /dev/null 2>&1 \
    || echo "no sdk output from driver" >> "$OUT/missing-sdk.txt"
docker compose logs --no-color > "$OUT/compose.log" 2>&1
docker compose ps -a > "$OUT/ps.txt" 2>&1
docker compose stop > /dev/null 2>&1

# Every assertion with its hit and pass counts, then the ones that would fail.
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | "\(.[0].display_type)\t\([.[] | select(.hit)] | length)\t\([.[] | select(.hit and .condition)] | length)\t\(.[0].message)"' \
    "$OUT"/sdk-*.jsonl | sort -k4 > "$OUT/assertions.tsv"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(if .[0].assert_type == "sometimes" then any(.hit and .condition) | not
             else any(.hit and (.condition | not)) or (.[0].must_hit and (any(.hit) | not)) end)
    | "\(.[0].display_type)\t\(.[0].message)"' "$OUT"/sdk-*.jsonl | sort > "$OUT/would-fail.tsv"
echo "assertions: $(wc -l < "$OUT/assertions.tsv"), would fail: $(wc -l < "$OUT/would-fail.tsv")"
cat "$OUT/would-fail.tsv"

# The gate. A fault-free local run cannot reach the assertions that need
# faults (an assert_always_or_unreachable or an assert_reachable), so those
# are reported above and not gated. What is gated: no SDK output at all, and
# any always-type assertion that was hit with its condition false.
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type != "sometimes" and any(.hit and (.condition | not)))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/violated.tsv"
# An assert_always (assert_type "always" with must_hit) that the run never hit.
# In a fault-free plain run every always property is reached, so a never-hit
# assert_always is a dead rule, not a fault-gated one. must_hit is false for an
# assert_always_or_unreachable and the type is "reachability" for an
# assert_reachable, so both of those, which legitimately go unhit without a
# fault, are excluded.
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type == "always" and .[0].must_hit and (any(.hit) | not))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/unhit-always.tsv"
if [ ! -s "$OUT/assertions.tsv" ]; then
    echo "FAIL: no assertions collected (see $OUT/missing-sdk.txt)"
    exit 1
fi
# A driver that crashed, misconfigured, or overran its bounded run invalidates
# the run whatever assertions were collected; gate it for every run kind.
if [ -n "$driver_fail" ]; then
    echo "FAIL: $driver_fail"
    exit 1
fi
if [ -n "$MUTANT" ]; then
    if grep -qxF "$expected" "$OUT/violated.tsv"; then
        echo "mutant OK: $MUTANT violated \"$expected\""
        exit 0
    fi
    echo "FAIL: mutant $MUTANT ran, but \"$expected\" was not violated"
    exit 1
fi
if [ -s "$OUT/violated.tsv" ]; then
    echo "FAIL: always-type assertions violated:"
    cat "$OUT/violated.tsv"
    exit 1
fi
if [ -s "$OUT/unhit-always.tsv" ]; then
    echo "FAIL: assert_always assertions never hit in a fault-free run:"
    cat "$OUT/unhit-always.tsv"
    exit 1
fi
echo "OK: no always-type assertion violated"
