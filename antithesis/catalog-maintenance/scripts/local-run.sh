#!/bin/bash
# The local loop (Antithesis skill, workload/local-loop.md), on the Linux
# build host: start the stack from scratch, play the driver's first_ command,
# let the workload run, then collect every container's SDK output and logs and
# list the assertions that would fail in a run.
#
# Usage: scripts/local-run.sh <out-dir> [seconds]
#   WORKLOAD_INJECT_SWAPPED_RECORD=1  the self-check: the model expects one
#                                     seeded record moved to another write, so
#                                     the multiset assertion must fail.
#   CLOCK_SPIKE_AFTER_SECS=N          run under libfaketime
#                                     (local/docker-compose.faketime.yaml) and
#                                     move every clock by CLOCK_SPIKE_OFFSET
#                                     (default +4h) N seconds into the
#                                     workload; scripts/clock-spike.sh sets it.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
OUT="${1:?out dir}"
SECS="${2:-300}"
SPIKE_AFTER="${CLOCK_SPIKE_AFTER_SECS:-}"
OFFSET="${CLOCK_SPIKE_OFFSET:-+4h}"
SERVERS="ravel1 ravel2 maint1 maint2"
mkdir -p "$OUT"
export COMPOSE_FILE="$WL/config/docker-compose.yaml:$WL/local/docker-compose.local.yaml"
if [ -n "$SPIKE_AFTER" ]; then
    COMPOSE_FILE="$COMPOSE_FILE:$WL/local/docker-compose.faketime.yaml"
fi
export COMPOSE_PROJECT_NAME=ravel-antithesis-catalog

docker compose down -v > /dev/null 2>&1
docker compose up -d || { echo "compose up failed"; exit 1; }
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload \
    || { echo "first_start_workload failed"; exit 1; }
start=$(date +%s)
until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl; do
    if [ $(( $(date +%s) - start )) -gt 900 ]; then
        echo "setup complete never arrived"; docker compose logs driver | tail -40; exit 1
    fi
    sleep 5
done
echo "setup complete after $(( $(date +%s) - start ))s"

if [ -n "$SPIKE_AFTER" ]; then
    sleep "$SPIKE_AFTER"
    docker compose exec -T driver sh -c "printf '%s\n' '$OFFSET' > /faketime/offset" \
        || { echo "could not write the clock offset"; exit 1; }
    # Each libfaketime cache lasts FAKETIME_CACHE_DURATION (2 s).
    sleep 5
    # Every container's wall clock must have moved by the offset. rustfs is
    # checked through the Last-Modified it stamps on a PUT, the clock that
    # matters to Ravel (SigV4 skew, object ages); its image may not honour
    # LD_PRELOAD.
    host_now=$(date +%s)
    {
        for svc in $SERVERS driver; do
            echo "$svc $(docker compose exec -T "$svc" date +%s)"
        done
        echo "rustfs $(docker compose exec -T driver sh -c '
            sig="aws:amz:$RAVEL_S3_REGION:s3"; creds="$RAVEL_S3_ACCESS_KEY:$RAVEL_S3_SECRET_KEY"
            url="$RAVEL_S3_ENDPOINT/$RAVEL_S3_BUCKET/antithesis/catalog-maintenance/clock-probe"
            printf x | curl -s -o /dev/null -X PUT --data-binary @- --aws-sigv4 "$sig" --user "$creds" "$url"
            lm=$(curl -s -I --aws-sigv4 "$sig" --user "$creds" "$url" | tr -d "\r" | sed -n "s/^[Ll]ast-[Mm]odified: //p")
            date -d "$lm" +%s 2>/dev/null')"
    } > "$OUT/clock-spike.txt"
    echo "host $host_now" >> "$OUT/clock-spike.txt"
    cat "$OUT/clock-spike.txt"
    # The shift must be at least two hours for every clock, whatever the
    # offset's spelling; the check is that each clock moved, not by how much.
    lagging=$(awk -v host="$host_now" '$1 != "host" && ($2 == "" || $2 - host < 7200) { print $1 }' "$OUT/clock-spike.txt")
    if [ -n "$lagging" ]; then
        echo "FAIL: none: needs clock control; these clocks did not move by $OFFSET: $lagging"
        docker compose logs --no-color > "$OUT/compose.log" 2>&1
        docker compose stop > /dev/null 2>&1
        exit 2
    fi
    echo "clock spike: every clock moved by $OFFSET"
fi

sleep "$SECS"

# Debug tools in each image.
{
    docker compose exec -T driver sh -c 'curl --version | head -1; jq --version; ravel-cli --help | head -1'
    docker compose exec -T ravel1 sh -c 'curl --version | head -1; jq --version; ravel-cli --help | head -1'
} > "$OUT/debug-tools.txt" 2>&1

# Graceful shutdown: a clean stop exits 0, a kill after the timeout 137.
for svc in ravel2 maint2; do
    docker stop -t 30 "$svc" > /dev/null
    docker inspect -f "$svc exit code after docker stop: {{.State.ExitCode}}" "$svc" >> "$OUT/shutdown.txt"
done

for svc in driver $SERVERS; do
    docker compose cp "$svc:/tmp/sdk.jsonl" "$OUT/sdk-$svc.jsonl" > /dev/null 2>&1 \
        || echo "no sdk output from $svc" >> "$OUT/missing-sdk.txt"
done
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

# The gate: no SDK output at all, or any always-type assertion hit with its
# condition false. Sometimes-type assertions a short fault-free run cannot
# reach are reported above and not gated.
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type != "sometimes" and any(.hit and (.condition | not)))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/violated.tsv"
if [ ! -s "$OUT/assertions.tsv" ]; then
    echo "FAIL: no assertions collected (see $OUT/missing-sdk.txt)"
    exit 1
fi
if [ "${WORKLOAD_INJECT_SWAPPED_RECORD:-}" = 1 ]; then
    # The self-check: the swapped expectation must make the multiset fail.
    if grep -qx "catalog: compaction preserves the acked multiset" "$OUT/violated.tsv"; then
        echo "self-check OK: the multiset assertion failed as injected"
        exit 0
    fi
    echo "FAIL: self-check run, but the multiset assertion did not fail"
    exit 1
fi
if [ -s "$OUT/violated.tsv" ]; then
    echo "FAIL: always-type assertions violated:"
    cat "$OUT/violated.tsv"
    exit 1
fi
echo "OK: no always-type assertion violated"
