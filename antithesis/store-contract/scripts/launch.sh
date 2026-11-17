#!/bin/bash
# Validate and launch one Antithesis run of the store-contract workload.
# Every image the compose file names must already be in the local image store
# (scripts/import-images.sh). Check the task's compute budget before running.
#
# Usage: scripts/launch.sh <max-compute-hours> [description]
#   RAVEL_ANTITHESIS_MUTANT=common/<stem>  launch a mutant control run: the
#                                          config is copied with that one
#                                          broken switch selected.
set -euo pipefail
HOURS="${1:?max compute hours}"
DESC="${2:-ravel store contract}"
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
CONFIG="$WL/config"
CHECKOUT=$(git -C "$HERE" rev-parse HEAD)
MUTANT="${RAVEL_ANTITHESIS_MUTANT:-}"

# The run records the commit the image was built from (build.sh stamps it as
# a label), and the config comes from this checkout, so the two must agree.
DRIVER_REF=$(docker image inspect -f '{{ index .Config.Labels "org.ravel.antithesis.ref" }}' \
    ravel-antithesis-driver:store-contract 2>/dev/null) || DRIVER_REF=""
if [ -z "$DRIVER_REF" ] || [ "$DRIVER_REF" = "<no value>" ]; then
    echo "refused: the driver image has no org.ravel.antithesis.ref label; rebuild it with scripts/build.sh" >&2
    exit 1
fi
if [ "$DRIVER_REF" != "$CHECKOUT" ]; then
    echo "refused: the driver image is from $DRIVER_REF but this checkout is at $CHECKOUT" >&2
    exit 1
fi
REF="$DRIVER_REF"

TEST_NAME=ravel-store-contract
if [ -n "$MUTANT" ]; then
    # Only a name listed in formal/tla/common/antithesis-mutants.tsv; the
    # driver refuses any other at startup, which would only show as a failed
    # setup on the platform.
    if ! cut -f2 "$WL/../../formal/tla/common/antithesis-mutants.tsv" | grep -qxF "$MUTANT"; then
        echo "refused: $MUTANT is not listed in formal/tla/common/antithesis-mutants.tsv" >&2
        exit 1
    fi
    tmp=$(mktemp -d)
    cp -R "$CONFIG/." "$tmp/"
    sed -i "s|\${RAVEL_ANTITHESIS_MUTANT:-}|\"$MUTANT\"|" "$tmp/docker-compose.yaml"
    if ! grep -qF "RAVEL_ANTITHESIS_MUTANT: \"$MUTANT\"" "$tmp/docker-compose.yaml"; then
        echo "refused: could not select $MUTANT in the copied config" >&2
        exit 1
    fi
    CONFIG="$tmp"
    TEST_NAME="ravel-store-contract-mutant"
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
    --param attrs.workload=store-contract \
    --param attrs.mutant="${MUTANT:-none}" \
    --param attrs.ravel_ref="$REF" \
    --param attrs.plugin_version=0.1.1
