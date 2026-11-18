#!/bin/bash
# Build the workload's two images and check their instrumentation.
# Run on the Linux build host, from a checkout of the commit to build.
# Usage: scripts/build.sh [server|driver|all]   (default all)
#   MUTANTS=1  build the ingest-durability-mutants images instead: ravel-server
#              also gets the antithesis-mutants feature (ADR-4326 decision 3),
#              so RAVEL_ANTITHESIS_MUTANT can select a commit-area mutant.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$WL/../.." && pwd)"
REG="${REGISTRY:-819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis}"
WHAT="${1:-all}"
# The mutant names the server binary must hold when the feature is on, and
# must not hold when it is off: formal/tla/commit/antithesis-mutants.tsv.
MUTANT_NAMES=$(awk -F'\t' '!/^#/ && $2 ~ /^commit\// { print $2 }' \
    "$ROOT/formal/tla/commit/antithesis-mutants.tsv")
# shellcheck source=antithesis/ingest-durability/scripts/image-tag.sh
. "$HERE/image-tag.sh"
# The features label lets launch.sh and local-run.sh refuse a mutant run on
# images that would ignore RAVEL_ANTITHESIS_MUTANT.
if [ "$MUTANTS" = 1 ]; then
    SERVER_FEATURES=sql,antithesis,antithesis-mutants
else
    SERVER_FEATURES=sql,antithesis
fi
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
        --build-arg "SERVER_FEATURES=$SERVER_FEATURES" \
        --label "org.ravel.antithesis.server-features=$SERVER_FEATURES" \
        -t "$SERVER" -t "$REG/ravel-server:$TAG" "$ROOT"
fi
if [ "$WHAT" = all ] || [ "$WHAT" = driver ]; then
    docker build --progress=plain -f "$WL/driver/Dockerfile" --label "$LABEL" \
        -t "$DRIVER" -t "$REG/driver:$TAG" "$WL"
fi

# Instrumentation checks (snouty docs: /docs/reference/sdk/rust/instrumentation/):
# the runtime shim is linked in, and the sancov sections exist, in each
# instrumented binary: ravel-server and ravel-cli in the server image, and the
# driver.
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
# The mutant switch: with antithesis-mutants, inv_mutant! passes each name to
# the selector at run time, so every name is a string in ravel-server; without
# it the macro is the constant false and no name is.
check_mutants() {
    local image="$1" dir cid name found=0 missing=0
    if [ -z "$MUTANT_NAMES" ]; then
        echo "FAIL $image: no commit mutant listed in formal/tla/commit/antithesis-mutants.tsv"; return 1
    fi
    dir=$(mktemp -d) || { echo "FAIL $image: no temporary directory"; return 1; }
    # This function runs inside `check_mutants ... || rc=1`, where set -e is
    # off, so each step that must succeed says so: otherwise a failed
    # extraction leaves no file, every grep misses, and the plain build's
    # "no mutant names" check passes without having read the binary.
    cid=$(docker create "$image") || { echo "FAIL $image: docker create failed"; rm -rf "$dir"; return 1; }
    if ! docker cp "$cid:/symbols/ravel-server" "$dir/ravel-server" > /dev/null; then
        docker rm "$cid" > /dev/null 2>&1
        echo "FAIL $image: could not extract /symbols/ravel-server"; rm -rf "$dir"; return 1
    fi
    docker rm "$cid" > /dev/null
    [ -s "$dir/ravel-server" ] || { echo "FAIL $image: extracted /symbols/ravel-server is empty"; rm -rf "$dir"; return 1; }
    for name in $MUTANT_NAMES; do
        if grep -qaF -- "$name" "$dir/ravel-server"; then
            found=$((found + 1))
        else
            missing=$((missing + 1))
        fi
    done
    rm -rf "$dir"
    if [ "$MUTANTS" = 1 ] && [ "$missing" -ne 0 ]; then
        echo "FAIL $image: /symbols/ravel-server lacks $missing of the commit mutant names"; return 1
    fi
    if [ "$MUTANTS" != 1 ] && [ "$found" -ne 0 ]; then
        echo "FAIL $image: /symbols/ravel-server holds $found commit mutant names without MUTANTS=1"; return 1
    fi
    echo "ok $image: /symbols/ravel-server holds $found of $((found + missing)) commit mutant names"
}
rc=0
if [ "$WHAT" = all ] || [ "$WHAT" = server ]; then
    check "$SERVER" ravel-server || rc=1
    check "$SERVER" ravel-cli || rc=1
    check_mutants "$SERVER" || rc=1
fi
if [ "$WHAT" = all ] || [ "$WHAT" = driver ]; then check "$DRIVER" driver || rc=1; fi
exit $rc
