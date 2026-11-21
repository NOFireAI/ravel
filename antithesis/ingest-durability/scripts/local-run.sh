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
#   CONFIG=config-faults              the fault-switch variant (README.md,
#                                     "Fault-switch variant"): the coverage
#                                     variant with Ravel's fault switches on,
#                                     gated on local/faults-required.tsv.
#   CONFIG=config-perf                the perf variant (README.md, "Perf
#                                     variant"). Needs
#                                     WORKLOAD_EVENTUALLY_AFTER_SECS; not with
#                                     a mutant. The run also fails unless the
#                                     driver printed exactly one perf summary
#                                     line, two perf ttfa lines and 20 perf
#                                     ack lines, every perf line is inside its
#                                     band, and every "required" assertion in
#                                     local/perf-required.tsv was hit with its
#                                     condition true.
#   PERF_LEVER=slow-ack|no-ack        with CONFIG=config-perf: layer
#                                     local/docker-compose.perf-<lever>.yaml
#                                     and gate on that lever's bands and its
#                                     lines of local/perf-required.tsv instead.
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
    config-coverage) VARIANT=coverage; REQUIRED_TSV="$WL/local/coverage-required.tsv" ;;
    config-faults) VARIANT=coverage; REQUIRED_TSV="$WL/local/faults-required.tsv" ;;
    config-perf) VARIANT=perf; REQUIRED_TSV="$WL/local/perf-required.tsv" ;;
    *) echo "refused: CONFIG must be config, config-coverage, config-faults or config-perf, not '$VARIANT_CONFIG'"; exit 1 ;;
esac
if [ "$VARIANT" != base ] && [ -n "$MUTANT" ]; then
    echo "refused: mutant runs use the base config; unset CONFIG or RAVEL_ANTITHESIS_MUTANT"
    exit 1
fi
PERF_LEVER="${PERF_LEVER:-}"
if [ -n "$PERF_LEVER" ] && [ "$VARIANT" != perf ]; then
    echo "refused: PERF_LEVER needs CONFIG=config-perf"
    exit 1
fi
case "$PERF_LEVER" in
    ''|slow-ack|no-ack) ;;
    *) echo "refused: PERF_LEVER must be slow-ack or no-ack, not '$PERF_LEVER'"; exit 1 ;;
esac
if [ "$VARIANT" = perf ] && [ -z "${WORKLOAD_EVENTUALLY_AFTER_SECS:-}" ]; then
    echo "refused: CONFIG=config-perf needs WORKLOAD_EVENTUALLY_AFTER_SECS: the perf probes run in the eventually"
    exit 1
fi
export COMPOSE_FILE="$WL/$VARIANT_CONFIG/docker-compose.yaml:$WL/local/docker-compose.local.yaml"
if [ -n "$PERF_LEVER" ]; then
    COMPOSE_FILE="$COMPOSE_FILE:$WL/local/docker-compose.perf-$PERF_LEVER.yaml"
fi
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
if [ "$VARIANT" = perf ]; then
    # The pre-registered local bands of #4559. Each figure is printed with its
    # band; any miss fails the run.
    perf_missed=()
    perf_count() {
        local name="$1" got="$2" want="$3"
        if [ "$got" -eq "$want" ]; then
            echo "perf gate: $name lines $got (band: exactly $want): held"
        else
            echo "perf gate: $name lines $got (band: exactly $want): MISSED"
            perf_missed+=("$name lines $got, want $want")
        fi
    }
    # Lines of compose.log holding $1 whose outcome is not $2, whose mono_ms
    # is outside [$3, $4] when $3 is set, or whose probes is not 1 or 2 when
    # $5 is set.
    perf_out_of_band() {
        awk -v pat="$1" -v want="$2" -v lo="$3" -v hi="$4" -v probes="$5" '
            index($0, pat) {
                split("", f)
                for (i = 1; i <= NF; i++) {
                    n = index($i, "=")
                    if (n) f[substr($i, 1, n - 1)] = substr($i, n + 1)
                }
                bad = f["outcome"] != want
                if (lo != "" && (f["mono_ms"] !~ /^[0-9]+$/ || f["mono_ms"] + 0 < lo + 0 || f["mono_ms"] + 0 > hi + 0)) bad = 1
                if (probes != "" && f["probes"] != "1" && f["probes"] != "2") bad = 1
                if (bad) print
            }' "$OUT/compose.log"
    }
    perf_band() {
        local name="$1" band="$2" file="$3" n
        n=$(wc -l < "$file")
        if [ "$n" -eq 0 ]; then
            echo "perf gate: $name lines outside $band: 0 (band: 0): held"
        else
            echo "perf gate: $name lines outside $band: $n (band: 0): MISSED"
            cat "$file"
            perf_missed+=("$name lines outside $band: $n")
        fi
    }
    summary=$(grep -cF "perf summary " "$OUT/compose.log") || summary=0
    ttfa=$(grep -cF "perf ttfa " "$OUT/compose.log") || ttfa=0
    acks=$(grep -cF "perf ack " "$OUT/compose.log") || acks=0
    skipped=$(grep -cF "perf skipped " "$OUT/compose.log") || skipped=0
    perf_count summary "$summary" 1
    perf_count ttfa "$ttfa" 2
    perf_count skipped "$skipped" 0
    case "$PERF_LEVER" in
        '')
            perf_count ack "$acks" 20
            perf_out_of_band "perf ack " acked 500 1500 "" > "$OUT/perf-ack-out-of-band.txt"
            perf_band "perf ack" "outcome=acked, 500 <= mono_ms <= 1500" "$OUT/perf-ack-out-of-band.txt"
            perf_out_of_band "perf ttfa " acked 500 1500 probes > "$OUT/perf-ttfa-out-of-band.txt"
            perf_band "perf ttfa" "outcome=acked, 500 <= mono_ms <= 1500, probes 1 or 2" "$OUT/perf-ttfa-out-of-band.txt"
            ;;
        slow-ack)
            perf_count ack "$acks" 20
            perf_out_of_band "perf ack " acked 3500 6000 "" > "$OUT/perf-ack-out-of-band.txt"
            perf_band "perf ack" "outcome=acked, 3500 <= mono_ms <= 6000" "$OUT/perf-ack-out-of-band.txt"
            ;;
        no-ack)
            perf_count ack "$acks" 0
            perf_out_of_band "perf ttfa " none "" "" "" > "$OUT/perf-ttfa-out-of-band.txt"
            perf_band "perf ttfa" "outcome=none" "$OUT/perf-ttfa-out-of-band.txt"
            ;;
    esac
    # Each listed assertion with its hits and its passes (hits with the
    # condition true) from assertions.tsv; a message absent there is 0 0.
    awk -F'\t' '
        NR == FNR { hits[$4] = $2; passes[$4] = $3; next }
        /^#/ || NF < 2 { next }
        { printf "%s\t%d\t%d\t%s\n", $1, hits[$2], passes[$2], $2 }
    ' "$OUT/assertions.tsv" "$REQUIRED_TSV" > "$OUT/perf.tsv"
    echo "perf variant (class, hits, passes, message):"
    cat "$OUT/perf.tsv"
    case "$PERF_LEVER" in
        '') must=required; never="" ;;
        slow-ack) must=slow-ack-required; never=slow-ack-never ;;
        no-ack) must=""; never=no-ack-never ;;
    esac
    for class in $must $never; do
        listed=$(awk -F'\t' -v c="$class" '$1 == c' "$OUT/perf.tsv" | wc -l)
        if [ "$listed" -eq 0 ]; then
            echo "perf gate: no $class line read from $REQUIRED_TSV: MISSED"
            perf_missed+=("no $class line in $REQUIRED_TSV")
        fi
    done
    if [ -n "$must" ]; then
        awk -F'\t' -v c="$must" '$1 == c && $3 < 1 { print $4 }' "$OUT/perf.tsv" > "$OUT/perf-unmet.tsv"
        if [ -s "$OUT/perf-unmet.tsv" ]; then
            echo "perf gate: $must assertions never hit with their condition true: MISSED"
            cat "$OUT/perf-unmet.tsv"
            perf_missed+=("$must assertions unmet: $(wc -l < "$OUT/perf-unmet.tsv")")
        else
            echo "perf gate: every $must assertion hit with its condition true: held"
        fi
    fi
    if [ -n "$never" ]; then
        awk -F'\t' -v c="$never" '$1 == c && $3 > 0 { print $4 }' "$OUT/perf.tsv" > "$OUT/perf-hit-true.tsv"
        if [ -s "$OUT/perf-hit-true.tsv" ]; then
            echo "perf gate: $never assertions hit with their condition true: MISSED"
            cat "$OUT/perf-hit-true.tsv"
            perf_missed+=("$never assertions hit true: $(wc -l < "$OUT/perf-hit-true.tsv")")
        else
            echo "perf gate: no $never assertion hit with its condition true: held"
        fi
    fi
    if [ "${#perf_missed[@]}" -gt 0 ]; then
        echo "FAIL: perf gate${PERF_LEVER:+ ($PERF_LEVER)} missed:"
        printf '  %s\n' "${perf_missed[@]}"
        exit 1
    fi
    echo "OK: perf gate${PERF_LEVER:+ ($PERF_LEVER)} held"
fi
if [ "$VARIANT" = coverage ]; then
    # Each listed assertion with its hits and its passes (hits with the
    # condition true) from assertions.tsv; a message absent there is 0 0.
    awk -F'\t' '
        NR == FNR { hits[$4] = $2; passes[$4] = $3; next }
        /^#/ || NF < 2 { next }
        { printf "%s\t%d\t%d\t%s\n", $1, hits[$2], passes[$2], $2 }
    ' "$OUT/assertions.tsv" "$REQUIRED_TSV" > "$OUT/coverage.tsv"
    echo "coverage variant (class, hits, passes, message):"
    cat "$OUT/coverage.tsv"
    required=$(awk -F'\t' '$1 == "required"' "$OUT/coverage.tsv" | wc -l)
    awk -F'\t' '$1 == "required" && $3 < 1 { print $4 }' "$OUT/coverage.tsv" > "$OUT/coverage-unmet.tsv"
    if [ "$required" -eq 0 ]; then
        echo "FAIL: no required assertion read from $REQUIRED_TSV"
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
