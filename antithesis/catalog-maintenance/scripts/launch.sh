#!/bin/bash
# Validate and launch one Antithesis run of the catalog-maintenance workload.
# Every image the compose file names must already be in the local image store
# (scripts/import-images.sh). Check the task's compute budget before running.
#
# Usage: scripts/launch.sh <max-compute-hours> [description]
set -euo pipefail
HOURS="${1:?max compute hours}"
DESC="${2:-ravel catalog maintenance}"
HERE="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$HERE/../config"
CHECKOUT=$(git -C "$HERE" rev-parse HEAD)

# The run records the commit the images were built from (build.sh stamps it
# as a label), and the config comes from this checkout, so the two must agree.
image_ref() {
    docker image inspect -f '{{ index .Config.Labels "org.ravel.antithesis.ref" }}' "$1" 2>/dev/null
}
SERVER_REF=$(image_ref ravel-antithesis-server:catalog-maintenance) || SERVER_REF=""
DRIVER_REF=$(image_ref ravel-antithesis-driver:catalog-maintenance) || DRIVER_REF=""
if [ -z "$SERVER_REF" ] || [ -z "$DRIVER_REF" ] || [ "$SERVER_REF" = "<no value>" ] || [ "$DRIVER_REF" = "<no value>" ]; then
    echo "refused: an image has no org.ravel.antithesis.ref label; rebuild it with scripts/build.sh" >&2
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
    --test-name ravel-catalog-maintenance \
    --description "$DESC" \
    --max-compute-hours "$HOURS" \
    --fault-tolerance high_availability \
    --fault-exclusions '^driver$' \
    --setup-timeout 15m \
    --param attrs.workload=catalog-maintenance \
    --param attrs.ravel_ref="$REF" \
    --param attrs.plugin_version=0.1.1
