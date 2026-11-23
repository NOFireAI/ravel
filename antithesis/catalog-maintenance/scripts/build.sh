#!/bin/bash
# Build the workload's two images and check their instrumentation.
# Run on the Linux build host, from a checkout of the commit to build.
# Usage: scripts/build.sh [server|driver|all]   (default all)
#   MUTANTS=1  build the mutant images instead (ADR-4326 decision 3):
#              ravel-server with the antithesis-mutants feature, tagged
#              catalog-maintenance-mutants, and the driver over it under the
#              same tag. The default build is unchanged.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$WL/../.." && pwd)"
# shellcheck source=mutant-tsv.sh
. "$HERE/mutant-tsv.sh"
REG="${REGISTRY:-819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis}"
case "${MUTANTS:-}" in
    '' | 0)
        TAG="${TAG:-catalog-maintenance}"
        FEATURES=sql,antithesis
        ;;
    1)
        # launch.sh, local-run.sh and mutant-config.sh all name this tag.
        if [ -n "${TAG:-}" ]; then
            echo "refused: TAG cannot be set with MUTANTS=1; the mutant images are always tagged catalog-maintenance-mutants" >&2
            exit 1
        fi
        TAG=catalog-maintenance-mutants
        FEATURES=sql,antithesis,antithesis-mutants
        ;;
    *)
        echo "refused: MUTANTS must be 1 or unset, not '$MUTANTS'" >&2
        exit 1
        ;;
esac
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
# launch.sh and local-run.sh read this back: a mutant run refuses a server
# image whose ravel-server lacks antithesis-mutants, because that build
# ignores RAVEL_ANTITHESIS_MUTANT and the run would only look like a pass.
FEATURES_LABEL="org.ravel.antithesis.features=$FEATURES"

if [ "$WHAT" = all ] || [ "$WHAT" = server ]; then
    docker build --progress=plain -f "$WL/ravel/Dockerfile" --label "$LABEL" \
        --label "$FEATURES_LABEL" --build-arg "SERVER_FEATURES=$FEATURES" \
        -t "$SERVER" -t "$REG/ravel-server:$TAG" "$ROOT"
fi
# The driver's build context is antithesis/, because the driver depends on
# the ingest-durability driver's library by path, and it copies ravel-cli out
# of the server image, so the server image is built first.
if [ "$WHAT" = all ] || [ "$WHAT" = driver ]; then
    docker build --progress=plain -f "$WL/driver/Dockerfile" --label "$LABEL" \
        --build-arg "SERVER_IMAGE=$SERVER" \
        -t "$DRIVER" -t "$REG/driver:$TAG" "$WL/.."
fi

# The binary must register every mutant name mutant-tsv.sh lists, and the
# per-area counts must be exactly the mutants ADR-4326 decision 3 ships
# (EXPECTED_MUTANTS, in mutant-tsv.sh), so a tsv that lost or gained a mutant
# line, or a reader that skips an area or counts none: lines, fails here
# rather than in a run. mutant_names itself refuses an area of MUTANT_AREAS
# with no expected count, and the loop below fails an expected area the tsv
# files give no mutant.
check_registered_mutants() {
    local image="$1" binary="$2" shown="${3:-$2}" names name pair area want got total=0 bad="" summary=""
    names=$(mutant_names) || { echo "FAIL $image: could not read the mutant names"; return 1; }
    while IFS= read -r name; do
        [ -n "$name" ] || continue
        if ! grep -qaF "$name" "$binary"; then
            echo "FAIL $image: $shown does not register the mutant $name"; return 1
        fi
    done <<< "$names"
    for pair in $EXPECTED_MUTANTS; do
        area="${pair%%=*}"
        want="${pair#*=}"
        got=$(printf '%s\n' "$names" | grep -c "^$area/") || got=0
        echo "$area: $got mutants (expected $want)"
        [ "$got" -eq "$want" ] || bad="$bad $area=$got"
        total=$((total + got))
        summary="${summary:+$summary, }$area $got"
    done
    if [ -n "$bad" ]; then
        echo "FAIL $image: expected the mutants $EXPECTED_MUTANTS, read$bad"; return 1
    fi
    echo "ok $image: $shown registers the $total mutants ($summary)"
}

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
    # The registered mutant names exist in the binary only when
    # antithesis-mutants compiled ravel_invariant::mutant in.
    if [ "${3:-}" = mutants ]; then
        check_registered_mutants "$image" "$dir/$bin" "/symbols/$bin" || return 1
    fi
    rm -rf "$dir"
}
MODE=plain
[ "$FEATURES" = sql,antithesis ] || MODE=mutants
rc=0
if [ "$WHAT" = all ] || [ "$WHAT" = server ]; then check "$SERVER" ravel-server "$MODE" || rc=1; fi
if [ "$WHAT" = all ] || [ "$WHAT" = driver ]; then
    check "$DRIVER" driver || rc=1
    check "$DRIVER" driver-reshard || rc=1
    check "$DRIVER" s3-proxy || rc=1
    check "$DRIVER" ravel-cli-ant || rc=1
fi
exit $rc
