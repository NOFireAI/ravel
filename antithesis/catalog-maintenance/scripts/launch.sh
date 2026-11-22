#!/bin/bash
# Validate and launch one Antithesis run of the catalog-maintenance workload.
# Every image the compose file names must already be in the local image store
# (scripts/import-images.sh). Check the task's compute budget before running.
#
# Usage: scripts/launch.sh <max-compute-hours> [description]
#   RAVEL_ANTITHESIS_MUTANT=<area>/<stem>  launch a mutant control run of a
#             catalog, resharding or maintenance mutant: a copy of the config
#             (scripts/mutant-config.sh) selects that mutant on every
#             ravel-server container, over the catalog-maintenance-mutants
#             images (MUTANTS=1 scripts/build.sh). Unset or empty launches
#             the plain run.
set -euo pipefail
HOURS="${1:?max compute hours}"
DESC="${2:-ravel catalog maintenance}"
HERE="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$HERE/../config"
CHECKOUT=$(git -C "$HERE" rev-parse HEAD)
MUTANT="${RAVEL_ANTITHESIS_MUTANT:-}"
TAG=catalog-maintenance
TEST_NAME=ravel-catalog-maintenance
if [ -n "$MUTANT" ]; then
    TAG=catalog-maintenance-mutants
    TEST_NAME=ravel-catalog-maintenance-mutant
    # Refuses a name that is not one of the mutants mutant-tsv.sh lists,
    # before anything else is checked.
    tmp=$(mktemp -d)
    trap 'rm -rf "${tmp:-}"' EXIT
    "$HERE/mutant-config.sh" "$MUTANT" "$tmp/config" || exit 1
    CONFIG="$tmp/config"
    DESC="$DESC (mutant $MUTANT)"
fi

# The run records the commit the images were built from (build.sh stamps it
# as a label), and the config comes from this checkout, so the two must agree.
image_label() {
    docker image inspect -f "{{ index .Config.Labels \"$2\" }}" "$1" 2>/dev/null
}
SERVER_REF=$(image_label "ravel-antithesis-server:$TAG" org.ravel.antithesis.ref) || SERVER_REF=""
DRIVER_REF=$(image_label "ravel-antithesis-driver:$TAG" org.ravel.antithesis.ref) || DRIVER_REF=""
if [ -z "$SERVER_REF" ] || [ -z "$DRIVER_REF" ] || [ "$SERVER_REF" = "<no value>" ] || [ "$DRIVER_REF" = "<no value>" ]; then
    echo "refused: an image tagged $TAG has no org.ravel.antithesis.ref label; rebuild it with scripts/build.sh" >&2
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
    # A ravel-server built without antithesis-mutants ignores
    # RAVEL_ANTITHESIS_MUTANT, so the run would test no mutant at all.
    FEATURES=$(image_label "ravel-antithesis-server:$TAG" org.ravel.antithesis.features) || FEATURES=""
    case ",$FEATURES," in
        *,antithesis-mutants,*) ;;
        *)
            echo "refused: ravel-antithesis-server:$TAG was built with features '$FEATURES', not antithesis-mutants; rebuild it with MUTANTS=1 scripts/build.sh" >&2
            exit 1
            ;;
    esac
fi

# SKIP_VALIDATE=1 skips the local validation on a host that cannot run the
# amd64 images (an Apple Silicon Mac without Rosetta emulation), once the same
# config was validated on an amd64 host.
#
# Both setup bounds are 15 minutes, local-run.sh's 900 s: the driver's own
# setup budget is 600 s, and the containers start around it.
if [ "${SKIP_VALIDATE:-}" != 1 ]; then
    snouty validate "$CONFIG" --timeout 15m
fi
snouty launch --launcher basic_test --config "$CONFIG" \
    --test-name "$TEST_NAME" \
    --description "$DESC" \
    --max-compute-hours "$HOURS" \
    --fault-tolerance high_availability \
    --fault-exclusions '^driver$' \
    --setup-timeout 15m \
    --param attrs.workload=catalog-maintenance \
    --param attrs.mutant="${MUTANT:-none}" \
    --param attrs.ravel_ref="$REF" \
    --param attrs.plugin_version=0.1.1
