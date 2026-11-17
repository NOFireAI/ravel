#!/bin/bash
# The local loop (Antithesis skill, workload/local-loop.md), on the Linux
# build host: start the stack from scratch under the same compose file a run
# uses (libfaketime and the clock jump included), play the driver's first_
# command, let the workload run until the driver's forced eventually ends the
# timeline, probe every clock, then collect every container's SDK output and
# logs and gate on what a run would report as failing.
#
# Usage: scripts/local-run.sh <out-dir> [seconds]
#   seconds   workload time before the driver's eventually runs
#             (WORKLOAD_EVENTUALLY_AFTER_SECS, default 240)
#   WORKLOAD_INJECT_SWAPPED_RECORD=1  the self-check: the model expects one
#             seeded record moved to another write, so the multiset assertion
#             must fail, and nothing else may.
#
# Exit 0: every gate passed (or, in the self-check, exactly the injected
# failure appeared). Exit 1: any gate failed or could not be checked; the
# FAIL lines say which.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
OUT="${1:?out dir}"
SECS="${2:-240}"
# The driver's own setup budget is 600 s (SETUP_BUDGET in driver/src/main.rs);
# this bound and launch.sh's --setup-timeout add room for the containers to
# start around it.
SETUP_LIMIT=900
# The eventually waits up to 300 s for recovery, then jumps the clocks, folds,
# compacts and audits, takes the two sweep jumps (each with its reread and
# serving waits), sweeps, folds, audits again and reads; 2400 s bounds all of
# it.
END_LIMIT=$((SECS + 2400))
SERVERS="ravel1 ravel2 maint1 maint2"
MULTISET="catalog: compaction preserves the acked multiset"
UPPER="catalog: no scan shows a phantom or an excess copy"
# Reachable in every plain local run: the clock jump, the sealing it exists
# for, the compaction the sealing enables (ADR-4326 decision 5), and the sweep
# jumps, sweep and deletion of a superseded input the compaction enables.
REQUIRED=(
    "catalog: the workload starts"
    "clock: the driver's clock jumped forward"
    "catalog: a sealed seeded hour was folded into HEAD"
    "clock: an hour ingested live was sealed by a fold"
    "catalog: a compaction record was published"
    "clock: the clocks jumped past the sweep's protection horizon and pinned-query window"
    "catalog: an on-demand sweep succeeded"
    "catalog: sweep deleted a superseded compaction input"
)
mkdir -p "$OUT"
export COMPOSE_FILE="$WL/config/docker-compose.yaml:$WL/local/docker-compose.local.yaml"
export COMPOSE_PROJECT_NAME=ravel-antithesis-catalog
export WORKLOAD_EVENTUALLY_AFTER_SECS="$SECS"
failures=()
fail() {
    echo "FAIL: $*"
    failures+=("$*")
}

collect_logs() {
    docker compose logs --no-color > "$OUT/compose.log" 2>&1
    docker compose ps -a > "$OUT/ps.txt" 2>&1
}

abort() {
    echo "FAIL: $*"
    collect_logs
    docker compose stop > /dev/null 2>&1
    exit 1
}

# One line per container: name, status, exit code.
container_state() {
    docker inspect -f '{{.Name}} {{.State.Status}} {{.State.ExitCode}}' "$1" 2>/dev/null | sed 's#^/##'
}

docker compose down -v > /dev/null 2>&1
# `up` waits for faketime and init to exit 0 (service_completed_successfully)
# and fails if either does not.
docker compose up -d || abort "compose up failed (faketime or init did not exit 0?)"

start=$(date +%s)
until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl; do
    read -r _ st code <<< "$(container_state driver)"
    if [ "${st:-}" != running ]; then
        abort "the driver stopped before setup complete (status ${st:-unknown}, exit ${code:-unknown})"
    fi
    if [ $(($(date +%s) - start)) -gt "$SETUP_LIMIT" ]; then
        abort "setup complete never arrived within ${SETUP_LIMIT}s"
    fi
    sleep 5
done
echo "setup complete after $(($(date +%s) - start))s"
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload \
    || abort "first_start_workload failed"

# The driver ends the timeline itself, exiting 0 after the eventually.
start=$(date +%s)
while read -r _ st code <<< "$(container_state driver)" && [ "${st:-}" = running ]; do
    if [ $(($(date +%s) - start)) -gt "$END_LIMIT" ]; then
        fail "the driver did not end the timeline within ${END_LIMIT}s"
        break
    fi
    sleep 10
done
read -r _ st code <<< "$(container_state driver)"
if [ "${st:-}" = exited ]; then
    if [ "${code:-}" != 0 ]; then
        fail "the driver died after setup (exit ${code:-unknown})"
    elif ! docker compose logs --no-color driver > "$OUT/driver.log" 2>&1; then
        fail "could not read the driver's logs"
    elif ! grep -q 'eventually: ending the timeline' "$OUT/driver.log"; then
        fail "the driver exited 0 without ending the timeline through the eventually"
    fi
elif [ "${st:-}" != running ]; then
    fail "could not read the driver's state (${st:-nothing})"
fi

# Every container: the setup ones exited 0, the rest still running or exited 0.
: > "$OUT/containers.txt"
for svc in faketime init rustfs $SERVERS driver; do
    line=$(container_state "$svc")
    echo "${line:-$svc unreadable}" >> "$OUT/containers.txt"
    read -r _ st code <<< "$line"
    case "${st:-}" in
        running) ;;
        exited) [ "${code:-}" = 0 ] || fail "container $svc exited ${code:-unknown}" ;;
        *) fail "container $svc is ${st:-unreadable}" ;;
    esac
done
for svc in faketime init; do
    grep -q "^$svc exited 0$" "$OUT/containers.txt" || fail "setup container $svc did not exit 0"
done

# The clock probe: each running container's wall clock, against the host's,
# must be ahead by the total offset the three jumps add up to (the sealing
# jump, then the sweep's horizon and window jumps), less the driver's allowed
# shortfall (60 s). The offset file must hold that total. rustfs is read
# twice, by `date` in its container and by the Last-Modified the rustfs
# process stamps on a PUT. A reading that is empty or not a number is a probe
# failure.
driver_env=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' driver)
jump=0
for var in CLOCK_JUMP_SECS CLOCK_SWEEP_HORIZON_JUMP_SECS CLOCK_SWEEP_WINDOW_JUMP_SECS; do
    secs=$(printf '%s\n' "$driver_env" | sed -n "s/^$var=//p")
    case "$secs" in
        '' | *[!0-9]*) fail "could not read $var from the driver container ('$secs')" ;;
        *) jump=$((jump + secs)) ;;
    esac
done
offset=$(docker compose exec -T ravel1 cat /faketime/offset 2>/dev/null | tr -d '[:space:]')
[ "$offset" = "+$jump" ] || fail "the offset file holds '$offset', not the +${jump} the three jumps add up to"
host_now=$(date +%s)
{
    for svc in rustfs $SERVERS; do
        echo "$svc $(docker compose exec -T "$svc" date +%s 2>/dev/null)"
    done
    # shellcheck disable=SC2016 # expanded inside the container
    echo "rustfs-last-modified $(docker compose exec -T ravel1 sh -c '
        sig="aws:amz:$RAVEL_S3_REGION:s3"; creds="$RAVEL_S3_ACCESS_KEY:$RAVEL_S3_SECRET_KEY"
        url="$RAVEL_S3_ENDPOINT/$RAVEL_S3_BUCKET/antithesis/catalog-maintenance/clock-probe"
        printf x | curl -s --max-time 10 -o /dev/null -X PUT --data-binary @- --aws-sigv4 "$sig" --user "$creds" "$url"
        lm=$(curl -s --max-time 10 -I --aws-sigv4 "$sig" --user "$creds" "$url" | tr -d "\r" | sed -n "s/^[Ll]ast-[Mm]odified: //p")
        [ -n "$lm" ] && env -u LD_PRELOAD date -d "$lm" +%s' 2>/dev/null)"
} > "$OUT/clock-probe.txt"
echo "host $host_now jump $jump" >> "$OUT/clock-probe.txt"
cat "$OUT/clock-probe.txt"
while read -r name reading; do
    case "$name" in host) continue ;; esac
    case "$reading" in
        '' | *[!0-9]*) fail "clock probe of $name read '${reading}'" ;;
        *) [ $((reading - host_now)) -ge $((jump - 60)) ] \
            || fail "clock of $name is $((reading - host_now))s ahead of the host, not the ${jump}s jump" ;;
    esac
done < "$OUT/clock-probe.txt"

# Debug tools in each image.
{
    docker compose exec -T ravel1 sh -c 'curl --version | head -1; jq --version; ravel-cli --help | head -1'
} > "$OUT/debug-tools.txt" 2>&1

# Graceful shutdown: a clean stop exits 0, a kill after the timeout 137.
for svc in ravel2 maint2; do
    docker stop -t 30 "$svc" > /dev/null
    docker inspect -f "$svc exit code after docker stop: {{.State.ExitCode}}" "$svc" >> "$OUT/shutdown.txt"
done

: > "$OUT/missing-sdk.txt"
for svc in driver $SERVERS; do
    docker compose cp "$svc:/tmp/sdk.jsonl" "$OUT/sdk-$svc.jsonl" > /dev/null 2>&1 \
        || echo "no sdk output from $svc" >> "$OUT/missing-sdk.txt"
done
collect_logs
docker compose stop > /dev/null 2>&1
[ -s "$OUT/missing-sdk.txt" ] && fail "SDK output missing: $(tr '\n' ' ' < "$OUT/missing-sdk.txt")"

# Every assertion with its hit and pass counts, then the ones that would fail.
if ! jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | "\(.[0].display_type)\t\([.[] | select(.hit)] | length)\t\([.[] | select(.hit and .condition)] | length)\t\(.[0].message)"' \
    "$OUT"/sdk-*.jsonl > "$OUT/assertions.unsorted"; then
    abort "could not parse the SDK output"
fi
sort -t "$(printf '\t')" -k4 "$OUT/assertions.unsorted" > "$OUT/assertions.tsv"
rm -f "$OUT/assertions.unsorted"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(if .[0].assert_type == "sometimes" then any(.hit and .condition) | not
             else any(.hit and (.condition | not)) or (.[0].must_hit and (any(.hit) | not)) end)
    | "\(.[0].display_type)\t\(.[0].message)"' "$OUT"/sdk-*.jsonl | sort > "$OUT/would-fail.tsv"
echo "assertions: $(wc -l < "$OUT/assertions.tsv"), would fail: $(wc -l < "$OUT/would-fail.tsv")"
cat "$OUT/would-fail.tsv"
[ -s "$OUT/assertions.tsv" ] || abort "no assertions collected"

# Always-type assertions hit with a false condition, and those a run must hit
# (must_hit, as Antithesis counts them) that were never hit.
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type != "sometimes" and any(.hit and (.condition | not)))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/violated.tsv"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type == "always" and .[0].must_hit and (any(.hit) | not))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/never-hit.tsv"
while IFS= read -r m; do fail "always-type assertion never hit: $m"; done < "$OUT/never-hit.tsv"
for m in "${REQUIRED[@]}"; do
    hits=$(awk -F '\t' -v m="$m" '$4 == m { print $3 }' "$OUT/assertions.tsv")
    [ -n "$hits" ] && [ "$hits" -gt 0 ] || fail "required assertion never held: $m"
done

if [ "${WORKLOAD_INJECT_SWAPPED_RECORD:-}" = 1 ]; then
    # The self-check: the swapped expectation must make the multiset fail,
    # and may also break the upper bound it implies; any other violation is
    # a real one.
    grep -qxF "$MULTISET" "$OUT/violated.tsv" \
        || fail "self-check run, but the multiset assertion did not fail"
    while IFS= read -r m; do
        [ "$m" = "$MULTISET" ] || [ "$m" = "$UPPER" ] || fail "always-type assertion violated besides the injected one: $m"
    done < "$OUT/violated.tsv"
else
    while IFS= read -r m; do fail "always-type assertion violated: $m"; done < "$OUT/violated.tsv"
fi

if [ "${#failures[@]}" -gt 0 ]; then
    echo "${#failures[@]} gate(s) failed; see $OUT"
    exit 1
fi
if [ "${WORKLOAD_INJECT_SWAPPED_RECORD:-}" = 1 ]; then
    echo "self-check OK: the multiset assertion failed as injected, and nothing else did"
else
    echo "OK: no always-type assertion violated or unhit, every required assertion held, every container healthy, every clock jumped"
fi
