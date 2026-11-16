#!/bin/bash
# Build the workload's two images and check their instrumentation.
# Run on the Linux build host, from a checkout of the commit to build.
# Usage: scripts/build.sh [server|driver|all]   (default all)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$WL/../.." && pwd)"
REG="${REGISTRY:-819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis}"
TAG="${TAG:-ingest-durability}"
WHAT="${1:-all}"
# The compose file names the local-only tags (see its header); the registry
# tags are only for moving the images between machines (export-images.sh).
SERVER="ravel-antithesis-server:$TAG"
DRIVER="ravel-antithesis-driver:$TAG"

# The commit the images are built from, stamped into each as a label that
# launch.sh reads back, so a run records the source that is in its images.
# A tree with modified tracked files is refused: its images would match no
# commit. Untracked files (a CI job's downloaded tools) do not count.
if [ -n "$(git -C "$ROOT" status --porcelain --untracked-files=no)" ]; then
    echo "refused: the checkout at $ROOT has uncommitted changes to tracked files" >&2
    exit 1
fi
REF=$(git -C "$ROOT" rev-parse HEAD)
LABEL="org.ravel.antithesis.ref=$REF"

if [ "$WHAT" = all ] || [ "$WHAT" = server ]; then
    docker build --progress=plain -f "$WL/ravel/Dockerfile" --label "$LABEL" \
        -t "$SERVER" -t "$REG/ravel-server:$TAG" "$ROOT"
fi
if [ "$WHAT" = all ] || [ "$WHAT" = driver ]; then
    docker build --progress=plain -f "$WL/driver/Dockerfile" --label "$LABEL" \
        -t "$DRIVER" -t "$REG/driver:$TAG" "$WL"
fi

# Instrumentation checks (snouty docs: /docs/reference/sdk/rust/instrumentation/):
# the runtime shim is linked in, and the sancov sections exist.
check() {
    local image="$1" bin="$2" dir
    dir=$(mktemp -d)
    local cid
    cid=$(docker create "$image")
    docker cp "$cid:/symbols/$bin" "$dir/$bin" > /dev/null
    docker rm "$cid" > /dev/null
    # Written to files first: `grep -q` closing a pipe early would make nm or
    # readelf die of SIGPIPE, which pipefail reports as a failed check.
    nm "$dir/$bin" > "$dir/nm.txt" || { echo "FAIL $image: nm failed"; return 1; }
    readelf -S "$dir/$bin" > "$dir/sections.txt" || { echo "FAIL $image: readelf failed"; return 1; }
    if ! grep -q ' T antithesis_load_libvoidstar$' "$dir/nm.txt"; then
        echo "FAIL $image: antithesis_load_libvoidstar is not a text symbol of /symbols/$bin"; return 1
    fi
    if ! grep -q __sancov_guards "$dir/sections.txt"; then
        echo "FAIL $image: /symbols/$bin has no __sancov_guards section"; return 1
    fi
    echo "ok $image: /symbols/$bin links the shim and carries sancov sections"
    rm -rf "$dir"
}
rc=0
if [ "$WHAT" = all ] || [ "$WHAT" = server ]; then check "$SERVER" ravel-server || rc=1; fi
if [ "$WHAT" = all ] || [ "$WHAT" = driver ]; then check "$DRIVER" driver || rc=1; fi
exit $rc
