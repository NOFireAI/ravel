#!/bin/bash
# Validate and launch one Antithesis run of the ingest-durability workload.
# Every image the compose file names must already be in the local image store
# (scripts/import-images.sh). Check the task's compute budget before running.
#
# Usage: scripts/launch.sh <max-compute-hours> [description]
#   RAVEL_ANTITHESIS_MUTANT=commit/<stem>  launch a mutant control run: the
#       ingest-durability-mutants images (build.sh and import-images.sh with
#       MUTANTS=1), with the named mutant switched on in ravel1 and ravel2. The
#       name must be a mutant in formal/tla/commit/antithesis-mutants.tsv; the
#       run should make that line's property fail (README.md, Mutants).
#   CONFIG=config-coverage  launch the coverage variant (README.md, Coverage
#       variant) from config-coverage/ on the same images, as test
#       ravel-ingest-durability-coverage with attrs.variant=coverage. Not
#       with a mutant.
#   CONFIG=config-faults  the fault-switch variant (README.md, Fault-switch
#       variant), as test ravel-ingest-durability-faults with
#       attrs.variant=faults. Not with a mutant.
#   CONFIG=config-perf  the perf variant (README.md, Perf variant), as test
#       ravel-ingest-durability-perf with attrs.variant=perf. Not with a
#       mutant.
set -euo pipefail
HOURS="${1:?max compute hours}"
DESC="${2:-ravel ingest durability}"
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
VARIANT_CONFIG="${CONFIG:-config}"
CHECKOUT=$(git -C "$HERE" rev-parse HEAD)
MUTANT="${RAVEL_ANTITHESIS_MUTANT:-}"
TSV="$WL/../../formal/tla/commit/antithesis-mutants.tsv"
TAG=ingest-durability
TEST_NAME=ravel-ingest-durability
VARIANT_PARAMS=()
case "$VARIANT_CONFIG" in
    config) ;;
    config-coverage)
        if [ -n "$MUTANT" ]; then
            echo "refused: mutant runs use the base config; unset CONFIG or RAVEL_ANTITHESIS_MUTANT" >&2
            exit 1
        fi
        TEST_NAME=ravel-ingest-durability-coverage
        VARIANT_PARAMS=(--param attrs.variant=coverage)
        DESC="$DESC (coverage variant)"
        ;;
    config-faults)
        if [ -n "$MUTANT" ]; then
            echo "refused: mutant runs use the base config; unset CONFIG or RAVEL_ANTITHESIS_MUTANT" >&2
            exit 1
        fi
        TEST_NAME=ravel-ingest-durability-faults
        VARIANT_PARAMS=(--param attrs.variant=faults)
        DESC="$DESC (fault-switch variant)"
        ;;
    config-perf)
        if [ -n "$MUTANT" ]; then
            echo "refused: mutant runs use the base config; unset CONFIG or RAVEL_ANTITHESIS_MUTANT" >&2
            exit 1
        fi
        TEST_NAME=ravel-ingest-durability-perf
        VARIANT_PARAMS=(--param attrs.variant=perf)
        DESC="$DESC (perf variant)"
        ;;
    *)
        echo "refused: CONFIG must be config, config-coverage, config-faults or config-perf, not '$VARIANT_CONFIG'" >&2
        exit 1 ;;
esac
CONFIG="$WL/$VARIANT_CONFIG"

if [ -n "$MUTANT" ]; then
    # Only a commit mutant listed in the TSV. ravel-server refuses an unknown
    # name at startup (ravel_invariant::init), so a typo would launch a run
    # whose every node exits at once; refusing here costs no compute hours.
    if ! awk -F'\t' -v m="$MUTANT" '!/^#/ && $2 == m && m ~ /^commit\// { found = 1 } END { exit !found }' "$TSV"; then
        echo "refused: $MUTANT is not a mutant listed in formal/tla/commit/antithesis-mutants.tsv" >&2
        exit 1
    fi
    TAG=ingest-durability-mutants
fi
SERVER_IMAGE="ravel-antithesis-server:$TAG"
DRIVER_IMAGE="ravel-antithesis-driver:$TAG"

# The run records the commit the images were built from (build.sh stamps it
# as a label), and the config comes from this checkout, so the two must agree.
image_label() {
    docker image inspect -f "{{ index .Config.Labels \"$2\" }}" "$1" 2>/dev/null
}
SERVER_REF=$(image_label "$SERVER_IMAGE" org.ravel.antithesis.ref) || SERVER_REF=""
DRIVER_REF=$(image_label "$DRIVER_IMAGE" org.ravel.antithesis.ref) || DRIVER_REF=""
if [ -z "$SERVER_REF" ] || [ -z "$DRIVER_REF" ] || [ "$SERVER_REF" = "<no value>" ] || [ "$DRIVER_REF" = "<no value>" ]; then
    echo "refused: $SERVER_IMAGE or $DRIVER_IMAGE has no org.ravel.antithesis.ref label; rebuild it with scripts/build.sh" >&2
    exit 1
fi
if [ "$SERVER_REF" != "$DRIVER_REF" ]; then
    echo "refused: the server image is from $SERVER_REF and the driver image from $DRIVER_REF" >&2
    exit 1
fi
if [ "$SERVER_REF" != "$CHECKOUT" ]; then
    echo "refused: the images are from $SERVER_REF but this checkout is at $CHECKOUT" >&2
    exit 1
fi
REF="$SERVER_REF"

if [ -n "$MUTANT" ]; then
    # A server built without antithesis-mutants ignores the variable, and the
    # run would pass as a plain run.
    FEATURES=$(image_label "$SERVER_IMAGE" org.ravel.antithesis.server-features) || FEATURES=""
    case ",$FEATURES," in
        *,antithesis-mutants,*) ;;
        *)
            echo "refused: $SERVER_IMAGE was not built with antithesis-mutants (features '$FEATURES'); rebuild it with MUTANTS=1 scripts/build.sh" >&2
            exit 1 ;;
    esac
    # The config copy names the -mutants images and sets the selector on the
    # x-ravel-server anchor, which only ravel1 and ravel2 use. The checked-in
    # config stays the plain run's.
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    cp -R "$CONFIG/." "$tmp/"
    awk -v m="$MUTANT" '
        { sub(/:ingest-durability$/, ":ingest-durability-mutants") }
        { print }
        /^    RUST_LOG: info$/ { print "    RAVEL_ANTITHESIS_MUTANT: \"" m "\"" }
    ' "$CONFIG/docker-compose.yaml" > "$tmp/docker-compose.yaml"
    selected=$(grep -cxF "    RAVEL_ANTITHESIS_MUTANT: \"$MUTANT\"" "$tmp/docker-compose.yaml") || selected=0
    plain=$(grep -c ':ingest-durability$' "$tmp/docker-compose.yaml") || plain=0
    if [ "$selected" -ne 1 ] || [ "$plain" -ne 0 ]; then
        echo "refused: could not select $MUTANT in the copied config ($selected selector lines, $plain plain image tags)" >&2
        exit 1
    fi
    CONFIG="$tmp"
    TEST_NAME="$TEST_NAME-mutant"
    DESC="$DESC (mutant $MUTANT)"
fi

# SKIP_VALIDATE=1 skips the local validation on a host that cannot run the
# amd64 images (an Apple Silicon Mac without Rosetta emulation), once the same
# config was validated on an amd64 host.
if [ "${SKIP_VALIDATE:-}" != 1 ]; then
    snouty validate "$CONFIG" --timeout 10m
fi
snouty launch --launcher basic_test --config "$CONFIG" \
    --test-name "$TEST_NAME" \
    --description "$DESC" \
    --max-compute-hours "$HOURS" \
    --fault-tolerance high_availability \
    --fault-exclusions '^driver$' \
    --setup-timeout 10m \
    --param attrs.workload=ingest-durability \
    --param attrs.ravel_ref="$REF" \
    --param attrs.mutant="${MUTANT:-none}" \
    --param attrs.plugin_version=0.1.1 \
    ${VARIANT_PARAMS[@]+"${VARIANT_PARAMS[@]}"}
