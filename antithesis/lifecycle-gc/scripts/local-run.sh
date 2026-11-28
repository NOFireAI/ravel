#!/bin/bash
# The local loop on the Linux build host: start the stack from scratch under
# the run's compose file, play the driver's first_ command, let the workload
# run until the driver's forced eventually ends the timeline, then collect
# every container's SDK output and gate on what a run would report.
#
# Usage: scripts/local-run.sh <out-dir> [seconds]
#   seconds  workload time before the forced eventually (default 240)
# Environment:
#   PROFILE=claims-off        maint2 runs with --maintain-claims off (#4449)
#   WORKLOAD_PLACEMENTS=...   logs subject placements (record,resource,scope)
#   WORKLOAD_INJECT_SERVED_ERASED=1
#                             the self-test: the driver's erasure property
#                             must be violated, and nothing else may be
#   EXPECT_VIOLATED=<file>    one assertion message per line that must be
#                             violated (a negative control); nothing else may
#   RAVEL_ANTITHESIS_MUTANT=lifecycle/<stem>
#                             a mutant control run (ADR-4326 decision 3): the
#                             stack runs the config copy
#                             scripts/mutant-config.sh writes, over the
#                             lifecycle-gc-mutants images, with that mutant
#                             selected on every ravel-server container. The
#                             property it must make false is the one
#                             formal/tla/lifecycle/antithesis-mutants.tsv
#                             names for it.
#   RSS_LIMIT_MB=500          the driver's peak memory bound
#
# Exit 0: every gate passed (in a mutant run: the mutant's property was
# evaluated false at least once). Exit 1: a gate failed or could not be
# checked; the FAIL lines say which. In a mutant run every other gate, and
# every other always-type assertion that went false, is reported and does not
# decide the exit code. The last line is PASS or FAIL.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
OUT="${1:?out dir}"
SECS="${2:-240}"
MUTANT="${RAVEL_ANTITHESIS_MUTANT:-}"
TSV="$(cd "$WL/../.." && pwd)/formal/tla/lifecycle/antithesis-mutants.tsv"
COMPOSE_BASE="$WL/config/docker-compose.yaml"
TAG=lifecycle-gc
mkdir -p "$OUT"
if [ -n "$MUTANT" ]; then
    if [ "${WORKLOAD_INJECT_SERVED_ERASED:-}" = 1 ] || [ -n "${EXPECT_VIOLATED:-}" ]; then
        echo "refused: a self-check or negative control and a mutant run are separate runs"
        exit 1
    fi
    rm -rf "$OUT/mutant-config"
    # Refuses a name that is not one of the lifecycle mutants.
    "$HERE/mutant-config.sh" "$MUTANT" "$OUT/mutant-config" || exit 1
    COMPOSE_BASE="$OUT/mutant-config/docker-compose.yaml"
    TAG=lifecycle-gc-mutants
    MUTANT_PROPERTY=$(awk -F '\t' -v m="$MUTANT" '$2 == m { print $3 }' "$TSV")
    case "$MUTANT_PROPERTY" in
        '' | *$'\n'*) echo "refused: $TSV names not exactly one property for $MUTANT"; exit 1 ;;
    esac
    # A ravel-server built without antithesis-mutants ignores
    # RAVEL_ANTITHESIS_MUTANT, and the run would test no mutant at all.
    features=$(docker image inspect -f '{{ index .Config.Labels "org.ravel.antithesis.features" }}' \
        "ravel-antithesis-server:$TAG" 2>/dev/null) || features=""
    case ",$features," in
        *,antithesis-mutants,*) ;;
        *)
            echo "refused: ravel-antithesis-server:$TAG was built with features '$features', not antithesis-mutants; build it with MUTANTS=1 scripts/build.sh"
            exit 1
            ;;
    esac
    echo "mutant $MUTANT: expecting \"$MUTANT_PROPERTY\" to be violated"
fi
SETUP_LIMIT=900
# The eventually waits for recovery, then up to eleven clock jumps with their
# settle and observation waits; 5400 s bounds all of it.
END_LIMIT=$((SECS + 5400))
RSS_LIMIT_MB="${RSS_LIMIT_MB:-500}"
SERVERS="ravel1 ravel2 maint1 maint2"
CONTAINERS="faketime rustfs init $SERVERS driver"
P1="erasure: an erased subject is never returned after its request"
# Every run must reach these: the driver's progress through the erasure, hold
# and clock phases and the situations it forces, the five SUT
# always-assertions the lifecycle smoke run must reach and pass, the driver's
# erasure property, and the SUT reachables that show the forced hold-read
# failure skipped a tick and a retention delete happened.
REQUIRED=(
    "lifecycle: the workload starts"
    "erasure: an erasure request was acked"
    "erasure: a rewrite record was observed"
    "erasure: a .done was observed"
    "erasure: a .dreq removal was observed"
    "erasure: a read after a .dreq removal ran"
    "gc: a hold was placed"
    "gc: the hold reads were made to fail"
    "clock: the clocks passed the protection horizon"
    "clock: the retention jumps ran"
    "$P1"
    "gc: no object is deleted inside its protection window"
    "gc: a held object is never deleted"
    "gc: a failed hold refresh sweeps nothing"
    "erasure: a completion is written only when no pre-rewrite object is still served"
    "erasure: a completion is written only once every bucket open at its acknowledgement is judged"
    "gc: a failed hold refresh skips the maintain tick"
    "gc: retention physical sweep delete"
)
export COMPOSE_PROJECT_NAME=ravel-antithesis-lifecycle
COMPOSE_FILE="$COMPOSE_BASE:$WL/local/docker-compose.local.yaml"
if [ "${PROFILE:-default}" = claims-off ]; then
    COMPOSE_FILE="$COMPOSE_FILE:$WL/local/docker-compose.claims-off.yaml"
fi
export COMPOSE_FILE
export WORKLOAD_EVENTUALLY_AFTER_SECS="$SECS"
failures=()
fail() {
    echo "FAIL: $*"
    failures+=("$*")
}
finish() {
    if [ "${#failures[@]}" -gt 0 ]; then
        echo "${#failures[@]} gate(s) failed; see $OUT"
        echo FAIL
        exit 1
    fi
    echo PASS
    exit 0
}
collect_logs() {
    docker compose logs --no-color > "$OUT/compose.log" 2>&1
    docker compose ps -a > "$OUT/ps.txt" 2>&1
}
abort() {
    fail "$*"
    collect_logs
    docker compose stop > /dev/null 2>&1
    kill "$sampler" 2> /dev/null
    finish
}
container_state() {
    docker inspect -f '{{.Name}} {{.State.Status}} {{.State.ExitCode}}' "$1" 2>/dev/null | sed 's#^/##'
}

# Container names are global on this host: refuse to start over another
# project's containers rather than removing them.
for c in $CONTAINERS; do
    proj=$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "$c" 2>/dev/null) || continue
    if [ "$proj" != "$COMPOSE_PROJECT_NAME" ]; then
        echo "refused: container $c belongs to compose project '$proj'" >&2
        echo FAIL
        exit 1
    fi
done

{
    echo "profile ${PROFILE:-default}"
    echo "placements ${WORKLOAD_PLACEMENTS:-record,resource,scope}"
    echo "inject ${WORKLOAD_INJECT_SERVED_ERASED:-0}"
    echo "mutant ${MUTANT:-none}"
    docker image inspect -f 'server {{.Id}} ref {{ index .Config.Labels "org.ravel.antithesis.ref" }} features {{ index .Config.Labels "org.ravel.antithesis.features" }}' "ravel-antithesis-server:$TAG"
    docker image inspect -f 'driver {{.Id}} ref {{ index .Config.Labels "org.ravel.antithesis.ref" }}' "ravel-antithesis-driver:$TAG"
} > "$OUT/run.txt" 2>&1
cat "$OUT/run.txt"

sampler=""
docker compose down -v > /dev/null 2>&1
docker compose up -d || abort "compose up failed (faketime or init did not exit 0?)"

# Driver memory, sampled every 5 s until the driver exits.
(
    while [ "$(docker inspect -f '{{.State.Status}}' driver 2>/dev/null)" = running ]; do
        docker stats --no-stream --format '{{.MemUsage}}' driver 2>/dev/null
        sleep 5
    done
) > "$OUT/driver-mem.txt" &
sampler=$!

start=$(date +%s)
until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl; do
    read -r _ st code <<< "$(container_state driver)"
    [ "${st:-}" = running ] || abort "the driver stopped before setup complete (status ${st:-unknown}, exit ${code:-unknown})"
    [ $(($(date +%s) - start)) -le "$SETUP_LIMIT" ] || abort "setup complete never arrived within ${SETUP_LIMIT}s"
    sleep 5
done
echo "setup complete after $(($(date +%s) - start))s"
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload \
    || abort "first_start_workload failed"

start=$(date +%s)
while read -r _ st code <<< "$(container_state driver)" && [ "${st:-}" = running ]; do
    if [ $(($(date +%s) - start)) -gt "$END_LIMIT" ]; then
        fail "the driver did not end the timeline within ${END_LIMIT}s"
        break
    fi
    sleep 10
done
kill "$sampler" 2> /dev/null
wait "$sampler" 2> /dev/null
docker compose logs --no-color driver > "$OUT/driver.log" 2>&1
read -r _ st code <<< "$(container_state driver)"
if [ "${st:-}" = exited ]; then
    [ "${code:-}" = 0 ] || fail "the driver exited ${code:-unknown}"
    grep -q 'eventually: ending the timeline' "$OUT/driver.log" \
        || fail "the driver exited without ending the timeline through the eventually"
else
    fail "the driver is ${st:-unreadable}, not exited"
fi

# Peak driver memory, from docker stats' "123.4MiB / 62.8GiB".
peak=$(awk '{ v = $1; u = v; gsub(/[0-9.]/, "", u); gsub(/[A-Za-z]/, "", v);
              m = (u == "GiB") ? v * 1024 : (u == "KiB") ? v / 1024 : (u == "B") ? 0 : v + 0;
              if (m > max) max = m } END { printf "%d", max }' "$OUT/driver-mem.txt")
echo "driver peak memory ${peak:-?} MiB over $(wc -l < "$OUT/driver-mem.txt") samples"
case "$peak" in
    '' | *[!0-9]*) fail "could not read the driver's memory samples" ;;
    *) [ "$peak" -le "$RSS_LIMIT_MB" ] || fail "driver peak memory ${peak} MiB above ${RSS_LIMIT_MB} MiB" ;;
esac

: > "$OUT/containers.txt"
for svc in $CONTAINERS; do
    line=$(container_state "$svc")
    echo "${line:-$svc unreadable}" >> "$OUT/containers.txt"
    read -r _ st code <<< "$line"
    case "${st:-}" in
        running) ;;
        exited) [ "${code:-}" = 0 ] || fail "container $svc exited ${code:-unknown}" ;;
        *) fail "container $svc is ${st:-unreadable}" ;;
    esac
done
docker compose exec -T ravel1 cat /faketime/offset > "$OUT/clock-offset.txt" 2>&1

: > "$OUT/missing-sdk.txt"
for svc in driver $SERVERS; do
    docker compose cp "$svc:/tmp/sdk.jsonl" "$OUT/sdk-$svc.jsonl" > /dev/null 2>&1 \
        || echo "no sdk output from $svc" >> "$OUT/missing-sdk.txt"
done
collect_logs
docker compose stop > /dev/null 2>&1
[ -s "$OUT/missing-sdk.txt" ] && fail "SDK output missing: $(tr '\n' ' ' < "$OUT/missing-sdk.txt")"

# Every assertion: type, hits, passes, message.
if ! jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | "\(.[0].display_type)\t\([.[] | select(.hit)] | length)\t\([.[] | select(.hit and .condition)] | length)\t\(.[0].message)"' \
    "$OUT"/sdk-*.jsonl > "$OUT/assertions.unsorted"; then
    abort "could not parse the SDK output"
fi
sort -t "$(printf '\t')" -k4 "$OUT/assertions.unsorted" > "$OUT/assertions.tsv"
rm -f "$OUT/assertions.unsorted"
[ -s "$OUT/assertions.tsv" ] || abort "no assertions collected"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(if .[0].assert_type == "sometimes" then any(.hit and .condition) | not
             else any(.hit and (.condition | not)) or (.[0].must_hit and (any(.hit) | not)) end)
    | "\(.[0].display_type)\t\(.[0].message)"' "$OUT"/sdk-*.jsonl | sort > "$OUT/would-fail.tsv"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type != "sometimes" and any(.hit and (.condition | not)))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/violated.tsv"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type == "always" and .[0].must_hit and (any(.hit) | not))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/never-hit.tsv"
grep -E '^(Always|AlwaysOrUnreachable|Sometimes|Reachable|Unreachable)' "$OUT/assertions.tsv" \
    | grep -E 'erasure|lifecycle|gc:|clock:' || true
echo "would fail in a run:"
cat "$OUT/would-fail.tsv"

# Only the workload's own always-type assertions must be hit here. The
# server's (catalog:, commit:, ...) belong to the paths other workloads drive,
# and are listed in never-hit.tsv for reading, not gated.
while IFS= read -r m; do
    case "$m" in
        erasure:* | lifecycle:* | gc:* | clock:*) fail "always-type assertion never hit: $m" ;;
    esac
done < "$OUT/never-hit.tsv"
for m in "${REQUIRED[@]}"; do
    hits=$(awk -F '\t' -v m="$m" '$4 == m { print $3 }' "$OUT/assertions.tsv")
    [ -n "$hits" ] && [ "$hits" -gt 0 ] || fail "required assertion never held: $m"
done

if [ -n "$MUTANT" ]; then
    # The mutant's property decides; every other violation and gate is
    # reported.
    hit=""
    while IFS= read -r m; do
        if [ "$m" = "$MUTANT_PROPERTY" ]; then
            hit=1
        else
            echo "REPORT (mutant run, not gated): always-type assertion violated besides the mutant's: $m"
        fi
    done < "$OUT/violated.tsv"
    for f in "${failures[@]}"; do
        echo "REPORT (mutant run, not gated): $f"
    done
    if [ -n "$hit" ]; then
        echo "mutant OK: $MUTANT violated \"$MUTANT_PROPERTY\""
        echo PASS
        exit 0
    fi
    echo "FAIL: mutant $MUTANT ran, but \"$MUTANT_PROPERTY\" was never violated"
    echo FAIL
    exit 1
fi

expected="$OUT/expected-violated.txt"
: > "$expected"
[ "${WORKLOAD_INJECT_SERVED_ERASED:-}" = 1 ] && echo "$P1" >> "$expected"
[ -n "${EXPECT_VIOLATED:-}" ] && cat "$EXPECT_VIOLATED" >> "$expected"
sort -u -o "$expected" "$expected"
while IFS= read -r m; do
    [ -z "$m" ] && continue
    grep -qxF "$m" "$OUT/violated.tsv" || fail "expected violation did not happen: $m"
done < "$expected"
while IFS= read -r m; do
    grep -qxF "$m" "$expected" || fail "always-type assertion violated: $m"
done < "$OUT/violated.tsv"
finish
