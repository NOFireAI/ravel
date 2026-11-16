#!/bin/bash
# Validate and launch one Antithesis run of the ingest-durability workload.
# Every image the compose file names must already be in the local image store
# (scripts/import-images.sh). Check the task's compute budget before running.
#
# Usage: scripts/launch.sh <max-compute-hours> [description]
set -euo pipefail
HOURS="${1:?max compute hours}"
DESC="${2:-ravel ingest durability}"
HERE="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$HERE/../config"
REF=$(git -C "$HERE" rev-parse HEAD)

snouty validate "$CONFIG" --timeout 10m
snouty launch --launcher basic_test --config "$CONFIG" \
    --test-name ravel-ingest-durability \
    --description "$DESC" \
    --max-compute-hours "$HOURS" \
    --fault-tolerance high_availability \
    --fault-exclusions '^driver$' \
    --setup-timeout 10m \
    --param attrs.workload=ingest-durability \
    --param attrs.ravel_ref="$REF" \
    --param attrs.plugin_version=0.1.1
