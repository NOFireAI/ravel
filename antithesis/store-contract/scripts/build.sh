#!/bin/bash
# Build the workload's one image (the driver, which also carries the init
# script) and check its instrumentation. No mutant image exists: a mutant is
# a driver-side switch selected at run time by RAVEL_ANTITHESIS_MUTANT.
# Run on the Linux build host, from a checkout of the commit to build.
# Usage: scripts/build.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$WL/../.." && pwd)"
REG="${REGISTRY:-819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis}"
TAG="${TAG:-store-contract}"
# The compose file names the local-only tag; the registry tag is only for
# moving the image between machines (export-images.sh).
DRIVER="ravel-antithesis-driver:$TAG"

# The commit the image is built from, stamped into it as a label that
# launch.sh reads back, so a run records the source that is in its image.
# A tree with modified tracked files is refused: its image would match no
# commit. Untracked files (a CI job's downloaded tools) do not count.
if [ -n "$(git -C "$ROOT" status --porcelain --untracked-files=no)" ]; then
    echo "refused: the checkout at $ROOT has uncommitted changes to tracked files" >&2
    exit 1
fi
REF=$(git -C "$ROOT" rev-parse HEAD)
LABEL="org.ravel.antithesis.ref=$REF"

# The context is the repository root (the driver depends on
# crates/ravel-object-store by path); driver/Dockerfile.dockerignore narrows it.
DOCKER_BUILDKIT=1 docker build --progress=plain -f "$WL/driver/Dockerfile" --label "$LABEL" \
    -t "$DRIVER" -t "$REG/driver:$TAG" "$ROOT"

# Instrumentation checks (snouty docs: /docs/reference/sdk/rust/instrumentation/):
# the runtime shim is linked in, and the sancov sections exist.
dir=$(mktemp -d)
cid=$(docker create "$DRIVER")
docker cp "$cid:/symbols/driver" "$dir/driver" > /dev/null
docker rm "$cid" > /dev/null
# Written to files first: `grep -q` closing a pipe early would make nm or
# readelf die of SIGPIPE, which pipefail reports as a failed check.
nm "$dir/driver" > "$dir/nm.txt" || { echo "FAIL $DRIVER: nm failed"; exit 1; }
readelf -S "$dir/driver" > "$dir/sections.txt" || { echo "FAIL $DRIVER: readelf failed"; exit 1; }
if ! grep -q ' T antithesis_load_libvoidstar$' "$dir/nm.txt"; then
    echo "FAIL $DRIVER: antithesis_load_libvoidstar is not a text symbol of /symbols/driver"
    exit 1
fi
if ! grep -q __sancov_guards "$dir/sections.txt"; then
    echo "FAIL $DRIVER: /symbols/driver has no __sancov_guards section"
    exit 1
fi
echo "ok $DRIVER: /symbols/driver links the shim and carries sancov sections"
rm -rf "$dir"
