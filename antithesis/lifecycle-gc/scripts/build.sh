#!/bin/bash
# Build the lifecycle-gc workload's two images on the Linux build host.
# Usage: scripts/build.sh [server|driver|all]   (default all)
#
# server: ravel-antithesis-server:lifecycle-gc, built from this checkout with
#   the catalog-maintenance server Dockerfile (ravel-server with
#   `sql,antithesis`, libfaketime and its scripts, the maintain key). With
#   SERVER_FROM=<image>, retag that existing image instead of building (a
#   local loop against an image built elsewhere, such as a fix branch's).
# driver: ravel-antithesis-driver:lifecycle-gc, which copies ravel-cli from
#   the server image, so build or retag the server first.
#
#   MUTANTS=1  build the mutant images instead (ADR-4326 decision 3):
#              ravel-server with `sql,antithesis,antithesis-mutants`, tagged
#              lifecycle-gc-mutants, and the driver over it under the same
#              tag. The build checks that the binary registers the four
#              lifecycle mutants formal/tla/lifecycle/antithesis-mutants.tsv
#              names. SERVER_FROM cannot be combined with it.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$WL/../.." && pwd)"
WHAT="${1:-all}"
case "${MUTANTS:-}" in
    '' | 0)
        TAG=lifecycle-gc
        FEATURES=sql,antithesis
        ;;
    1)
        if [ -n "${SERVER_FROM:-}" ]; then
            echo "refused: SERVER_FROM cannot be set with MUTANTS=1" >&2
            exit 1
        fi
        TAG=lifecycle-gc-mutants
        FEATURES=sql,antithesis,antithesis-mutants
        ;;
    *)
        echo "refused: MUTANTS must be 1 or unset, not '$MUTANTS'" >&2
        exit 1
        ;;
esac
SERVER="ravel-antithesis-server:$TAG"
DRIVER="ravel-antithesis-driver:$TAG"
TSV="$ROOT/formal/tla/lifecycle/antithesis-mutants.tsv"

# The commit the images are built from, stamped as a label launch.sh and
# local-run.sh read back. A tree with modified tracked files matches no
# commit, so it is refused.
if [ -n "$(git -C "$ROOT" status --porcelain --untracked-files=no)" ]; then
    echo "refused: the checkout at $ROOT has uncommitted changes to tracked files" >&2
    exit 1
fi
REF=$(git -C "$ROOT" rev-parse HEAD)

# The binary checks (snouty docs /docs/reference/sdk/rust/instrumentation/):
# the runtime shim is linked in and the sancov sections exist; for the
# mutant server, every lifecycle mutant name is registered.
check() {
    local image="$1" bin="$2" dir cid name n=0
    dir=$(mktemp -d)
    cid=$(docker create "$image")
    docker cp "$cid:/symbols/$bin" "$dir/$bin" > /dev/null
    docker rm "$cid" > /dev/null
    nm "$dir/$bin" > "$dir/nm.txt"
    readelf -S "$dir/$bin" > "$dir/sections.txt"
    grep -q ' T antithesis_load_libvoidstar$' "$dir/nm.txt" \
        || { echo "FAIL $image: antithesis_load_libvoidstar is not a text symbol of /symbols/$bin"; return 1; }
    grep -q __sancov_guards "$dir/sections.txt" \
        || { echo "FAIL $image: /symbols/$bin has no __sancov_guards section"; return 1; }
    echo "ok $image: /symbols/$bin links the shim and carries sancov sections (ref $REF)"
    if [ "${3:-}" = mutants ]; then
        while IFS= read -r name; do
            grep -qaF "$name" "$dir/$bin" \
                || { echo "FAIL $image: /symbols/$bin does not register the mutant $name"; return 1; }
            n=$((n + 1))
        done < <(awk -F '\t' '$2 ~ /^lifecycle\// { print $2 }' "$TSV")
        [ "$n" -eq 4 ] || { echo "FAIL $image: expected the 4 lifecycle mutants in $TSV, read $n"; return 1; }
        echo "ok $image: /symbols/$bin registers the $n lifecycle mutants"
    fi
    rm -rf "$dir"
}

if [ "$WHAT" = all ] || [ "$WHAT" = server ]; then
    if [ -n "${SERVER_FROM:-}" ]; then
        docker image inspect "$SERVER_FROM" > /dev/null
        docker tag "$SERVER_FROM" "$SERVER"
        echo "tagged $SERVER_FROM as $SERVER"
    else
        docker build --progress=plain -f "$ROOT/antithesis/catalog-maintenance/ravel/Dockerfile" \
            --label "org.ravel.antithesis.ref=$REF" \
            --label "org.ravel.antithesis.features=$FEATURES" \
            --build-arg "SERVER_FEATURES=$FEATURES" \
            -t "$SERVER" "$ROOT"
        if [ "$FEATURES" = sql,antithesis ]; then
            check "$SERVER" ravel-server
        else
            check "$SERVER" ravel-server mutants
        fi
    fi
    docker image inspect -f "$SERVER built from {{ index .Config.Labels \"org.ravel.antithesis.ref\" }}" "$SERVER"
fi

if [ "$WHAT" = all ] || [ "$WHAT" = driver ]; then
    docker build --progress=plain -f "$WL/driver/Dockerfile" \
        --label "org.ravel.antithesis.ref=$REF" \
        --build-arg "SERVER_IMAGE=$SERVER" \
        -t "$DRIVER" "$WL"
    check "$DRIVER" driver
fi
