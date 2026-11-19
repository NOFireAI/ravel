#!/bin/bash
# The local loop (Antithesis skill, workload/local-loop.md), on the Linux
# build host: start the stack from scratch, play the driver's first_ command,
# let the workload run, then collect every container's SDK output and logs and
# list the assertions that would fail in a run.
#
# Usage: scripts/local-run.sh <out-dir> [seconds]
#   WORKLOAD_EVENTUALLY_AFTER_SECS=N  also run the eventually after N seconds
#                                     of workload; the run then waits for the
#                                     driver to exit instead of sleeping.
#   RAVEL_ANTITHESIS_MUTANT=commit/<stem>  mutant control on the
#                                     ingest-durability-mutants images: the run
#                                     passes only if the property that mutant
#                                     names in
#                                     formal/tla/commit/antithesis-mutants.tsv
#                                     was evaluated false at least once.
#   CONFIG=config-coverage            the coverage variant (README.md,
#                                     "Coverage variant"): the run also fails
#                                     unless every "required" assertion in
#                                     local/coverage-required.tsv was hit with
#                                     its condition true.
#   WORKLOAD_COVERAGE_SELF_CHECK=1    with CONFIG=config-coverage: the
#                                     variant's self-check, which breaks each
#                                     of its properties on purpose; the run
#                                     passes only if every "self-check" line
#                                     of local/coverage-required.tsv was
#                                     violated. Needs
#                                     WORKLOAD_EVENTUALLY_AFTER_SECS.
#   LOCAL_COMPOSE_OVERRIDE=<file>     one more compose file layered last, such
#                                     as one naming other image tags on a
#                                     shared build host.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
OUT="${1:?out dir}"
SECS="${2:-300}"
TSV="$WL/../../formal/tla/commit/antithesis-mutants.tsv"
MUTANT="${RAVEL_ANTITHESIS_MUTANT:-}"
VARIANT_CONFIG="${CONFIG:-config}"
case "$VARIANT_CONFIG" in
    config) VARIANT=base ;;
    config-coverage) VARIANT=coverage ;;
    *) echo "refused: CONFIG must be config or config-coverage, not '$VARIANT_CONFIG'"; exit 1 ;;
esac
if [ "$VARIANT" = coverage ] && [ -n "$MUTANT" ]; then
    echo "refused: mutant runs use the base config; unset CONFIG or RAVEL_ANTITHESIS_MUTANT"
    exit 1
fi
export COMPOSE_FILE="$WL/$VARIANT_CONFIG/docker-compose.yaml:$WL/local/docker-compose.local.yaml"
export COMPOSE_PROJECT_NAME=ravel-antithesis
if [ -n "$MUTANT" ]; then
    expected=$(awk -F'\t' -v m="$MUTANT" '!/^#/ && $2 == m && m ~ /^commit\// { print $3 }' "$TSV")
    if [ -z "$expected" ]; then
        echo "refused: $MUTANT is not a mutant listed in $TSV"
        exit 1
    fi
    if [ -n "${WORKLOAD_INJECT_MISSING_RECORD:-}" ]; then
        echo "refused: WORKLOAD_INJECT_MISSING_RECORD is the plain build's self-check; run it without a mutant"
        exit 1
    fi
    # A server built without antithesis-mutants ignores the selector, and the
    # run would report a plain run's result.
    features=$(docker image inspect -f '{{ index .Config.Labels "org.ravel.antithesis.server-features" }}' \
        ravel-antithesis-server:ingest-durability-mutants 2>/dev/null) || features=""
    case ",$features," in
        *,antithesis-mutants,*) ;;
        *)
            echo "refused: ravel-antithesis-server:ingest-durability-mutants was not built with antithesis-mutants (features '$features'); run MUTANTS=1 scripts/build.sh"
            exit 1 ;;
    esac
    export RAVEL_ANTITHESIS_MUTANT="$MUTANT"
    COMPOSE_FILE="$COMPOSE_FILE:$WL/local/docker-compose.mutant.yaml"
    echo "mutant $MUTANT: expecting \"$expected\" to be violated"
fi
COVERAGE_SELF_CHECK="${WORKLOAD_COVERAGE_SELF_CHECK:-}"
if [ -n "$COVERAGE_SELF_CHECK" ]; then
    if [ "$COVERAGE_SELF_CHECK" != 1 ] || [ "$VARIANT" != coverage ] || [ -z "${WORKLOAD_EVENTUALLY_AFTER_SECS:-}" ]; then
        echo "refused: WORKLOAD_COVERAGE_SELF_CHECK=1 needs CONFIG=config-coverage and WORKLOAD_EVENTUALLY_AFTER_SECS"
        exit 1
    fi
    COMPOSE_FILE="$COMPOSE_FILE:$WL/local/docker-compose.coverage-self-check.yaml"
fi
if [ -n "${LOCAL_COMPOSE_OVERRIDE:-}" ]; then
    [ -f "$LOCAL_COMPOSE_OVERRIDE" ] || { echo "refused: no compose file at $LOCAL_COMPOSE_OVERRIDE"; exit 1; }
    COMPOSE_FILE="$COMPOSE_FILE:$LOCAL_COMPOSE_OVERRIDE"
fi
mkdir -p "$OUT"
if [ -n "${WORKLOAD_EVENTUALLY_AFTER_SECS:-}" ]; then
    export WORKLOAD_EVENTUALLY=on
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

if [ -n "${WORKLOAD_EVENTUALLY_AFTER_SECS:-}" ]; then
    deadline=$(( $(date +%s) + SECS ))
    while [ "$(docker inspect -f '{{.State.Running}}' driver)" = true ]; do
        [ "$(date +%s)" -lt "$deadline" ] || { echo "driver still running after ${SECS}s"; break; }
        sleep 5
    done
    docker inspect -f 'driver exit code {{.State.ExitCode}}' driver
else
    sleep "$SECS"
fi

# Debug tools in each image.
{
    docker compose exec -T driver sh -c 'curl --version | head -1; jq --version'
    docker compose exec -T ravel1 sh -c 'curl --version | head -1; jq --version; ravel-cli --help | head -1'
} > "$OUT/debug-tools.txt" 2>&1

# Graceful shutdown: a clean stop exits 0, a kill after the timeout 137.
docker stop -t 30 ravel2 > /dev/null
docker inspect -f 'ravel2 exit code after docker stop: {{.State.ExitCode}}' ravel2 > "$OUT/shutdown.txt"

for svc in driver ravel1 ravel2; do
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

# The gate. A fault-free local run cannot reach the assertions that need
# faults, so an unreached or never-true assertion is reported above and not
# gated. What is gated: no SDK output at all, and any always-type assertion
# that was hit with its condition false.
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type != "sometimes" and any(.hit and (.condition | not)))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/violated.tsv"
if [ ! -s "$OUT/assertions.tsv" ]; then
    echo "FAIL: no assertions collected (see $OUT/missing-sdk.txt)"
    exit 1
fi
if [ -n "$MUTANT" ]; then
    # A broken commit path can also fail other always-properties, such as
    # the driver's read-backs; those are reported, not gated.
    others=$(grep -vxF -- "$expected" "$OUT/violated.tsv")
    if [ -n "$others" ]; then
        echo "also violated under $MUTANT:"
        printf '%s\n' "$others"
    fi
    if grep -qxF -- "$expected" "$OUT/violated.tsv"; then
        echo "mutant OK: $MUTANT violated \"$expected\""
        exit 0
    fi
    echo "FAIL: mutant $MUTANT ran, but \"$expected\" was not violated"
    exit 1
fi
if [ "${WORKLOAD_INJECT_MISSING_RECORD:-}" = 1 ]; then
    # The self-check: the injected record must make the presence assertion fail.
    if grep -q "returns every record of that write" "$OUT/violated.tsv"; then
        echo "self-check OK: the presence assertion failed as injected"
        exit 0
    fi
    echo "FAIL: self-check run, but the presence assertion did not fail"
    exit 1
fi
if [ -n "$COVERAGE_SELF_CHECK" ]; then
    awk -F'\t' '$1 == "self-check" { print $2 }' "$WL/local/coverage-required.tsv" > "$OUT/self-check-expected.tsv"
    if [ ! -s "$OUT/self-check-expected.tsv" ]; then
        echo "FAIL: no self-check line read from local/coverage-required.tsv"
        exit 1
    fi
    grep -vxF -f "$OUT/violated.tsv" "$OUT/self-check-expected.tsv" > "$OUT/self-check-unmet.tsv"
    if [ -s "$OUT/self-check-unmet.tsv" ]; then
        echo "FAIL: coverage self-check ran, but these properties were not violated:"
        cat "$OUT/self-check-unmet.tsv"
        exit 1
    fi
    echo "coverage self-check OK: all $(wc -l < "$OUT/self-check-expected.tsv") properties violated"
    exit 0
fi
if [ -s "$OUT/violated.tsv" ]; then
    echo "FAIL: always-type assertions violated:"
    cat "$OUT/violated.tsv"
    exit 1
fi
if [ "$VARIANT" = coverage ]; then
    # Each listed assertion with its hits and its passes (hits with the
    # condition true) from assertions.tsv; a message absent there is 0 0.
    awk -F'\t' '
        NR == FNR { hits[$4] = $2; passes[$4] = $3; next }
        /^#/ || NF < 2 { next }
        { printf "%s\t%d\t%d\t%s\n", $1, hits[$2], passes[$2], $2 }
    ' "$OUT/assertions.tsv" "$WL/local/coverage-required.tsv" > "$OUT/coverage.tsv"
    echo "coverage variant (class, hits, passes, message):"
    cat "$OUT/coverage.tsv"
    required=$(awk -F'\t' '$1 == "required"' "$OUT/coverage.tsv" | wc -l)
    awk -F'\t' '$1 == "required" && $3 < 1 { print $4 }' "$OUT/coverage.tsv" > "$OUT/coverage-unmet.tsv"
    if [ "$required" -eq 0 ]; then
        echo "FAIL: no required assertion read from local/coverage-required.tsv"
        exit 1
    fi
    if [ -s "$OUT/coverage-unmet.tsv" ]; then
        echo "FAIL: required coverage assertions never hit with their condition true:"
        cat "$OUT/coverage-unmet.tsv"
        exit 1
    fi
    echo "OK: all $required required coverage assertions met"
fi
echo "OK: no always-type assertion violated"
