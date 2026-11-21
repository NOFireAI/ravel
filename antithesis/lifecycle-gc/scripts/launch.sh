#!/bin/bash
# Push the lifecycle-gc images and an OCI config image, or launch a run from
# a pushed config image.
#
#   scripts/launch.sh push [--profile default|claims-off] [--mutant lifecycle/<stem>]
#       On the build host, as the user whose docker holds the images built by
#       scripts/build.sh. Pushes both images to the tenant registry, renders
#       config/docker-compose.yaml with every workload image pinned by digest
#       (and, for claims-off, maint2 on --maintain-claims off, #4449), builds
#       that into a config image with buildx (oci-mediatypes=true), pushes it,
#       and prints the config image reference pinned by digest and the launch
#       command. Registry credentials come from DOCKER_CONFIG (default
#       /root/.antithesis/docker-config). With --mutant, the config is the
#       copy scripts/mutant-config.sh writes for that mutant, over the
#       lifecycle-gc-mutants images (MUTANTS=1 scripts/build.sh).
#
#   scripts/launch.sh launch <config-image@digest> <max-compute-hours> [profile] [description]
#       Anywhere snouty is logged in (no docker needed). Check the compute
#       budget first. RAVEL_ANTITHESIS_MUTANT=lifecycle/<stem> names the
#       mutant the config image was pushed with: the run is then launched as
#       ravel-lifecycle-gc-mutant with attrs.mutant set.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
REG="${REGISTRY:-us-central1-docker.pkg.dev/molten-verve-216720/taffy-psheep-repository}"

label() {
    docker image inspect -f "{{ index .Config.Labels \"${2:-org.ravel.antithesis.ref}\" }}" "$1"
}

# Push one local image under the registry name and print its digest reference.
push_pinned() {
    local local_ref="$1" name="$2" tag="$3" remote digest
    remote="$REG/$name:$tag"
    docker tag "$local_ref" "$remote"
    docker push -q "$remote" > /dev/null
    digest=$(docker image inspect -f '{{range .RepoDigests}}{{println .}}{{end}}' "$remote" \
        | grep -F "$REG/$name@sha256:" | head -1)
    [ -n "$digest" ] || { echo "refused: no registry digest for $remote" >&2; return 1; }
    echo "$digest"
}

case "${1:-}" in
push)
    shift
    PROFILE=default
    MUTANT=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --profile) PROFILE="${2:?profile}"; shift 2 ;;
            --mutant) MUTANT="${2:?mutant}"; shift 2 ;;
            *) echo "unknown argument $1" >&2; exit 64 ;;
        esac
    done
    case "$PROFILE" in default | claims-off) ;; *) echo "unknown profile $PROFILE" >&2; exit 64 ;; esac
    export DOCKER_CONFIG="${DOCKER_CONFIG:-/root/.antithesis/docker-config}"
    CHECKOUT=$(git -C "$WL" rev-parse HEAD)
    LOCAL_TAG=lifecycle-gc
    SOURCE="$WL/config/docker-compose.yaml"
    stage=$(mktemp -d)
    if [ -n "$MUTANT" ]; then
        LOCAL_TAG=lifecycle-gc-mutants
        "$HERE/mutant-config.sh" "$MUTANT" "$stage/mutant-config" || exit 1
        SOURCE="$stage/mutant-config/docker-compose.yaml"
    fi
    SERVER="ravel-antithesis-server:$LOCAL_TAG"
    DRIVER="ravel-antithesis-driver:$LOCAL_TAG"
    DRIVER_REF=$(label "$DRIVER")
    SERVER_REF=$(label "$SERVER")
    if [ "$DRIVER_REF" != "$CHECKOUT" ]; then
        echo "refused: the driver image is from $DRIVER_REF but this checkout is at $CHECKOUT" >&2
        exit 1
    fi
    if [ -n "$MUTANT" ]; then
        # A ravel-server built without antithesis-mutants ignores
        # RAVEL_ANTITHESIS_MUTANT, so the run would test no mutant at all.
        case ",$(label "$SERVER" org.ravel.antithesis.features)," in
            *,antithesis-mutants,*) ;;
            *) echo "refused: $SERVER was not built with antithesis-mutants; build it with MUTANTS=1 scripts/build.sh" >&2; exit 1 ;;
        esac
    fi
    TAG="$LOCAL_TAG-${CHECKOUT:0:12}"
    server_pin=$(push_pinned "$SERVER" ravel-server "$TAG")
    driver_pin=$(push_pinned "$DRIVER" driver "$TAG")
    sed -e "s#image: $SERVER\$#image: $server_pin#" \
        -e "s#image: $DRIVER\$#image: $driver_pin#" \
        "$SOURCE" > "$stage/docker-compose.yaml"
    if [ "$PROFILE" = claims-off ]; then
        # maint2's command only: the first `command: *maintain-command` after
        # its service key.
        awk '/^  maint2:$/ { in2 = 1 }
             in2 && /command: \*maintain-command$/ { sub(/\*maintain-command$/, "*maintain-claims-off-command"); in2 = 0 }
             { print }' "$stage/docker-compose.yaml" > "$stage/rendered.yaml"
        mv "$stage/rendered.yaml" "$stage/docker-compose.yaml"
        [ "$(grep -c 'command: \*maintain-claims-off-command$' "$stage/docker-compose.yaml")" = 1 ] \
            || { echo "refused: the claims-off render did not change exactly maint2" >&2; exit 1; }
    fi
    if grep -q 'image: ravel-antithesis-' "$stage/docker-compose.yaml"; then
        echo "refused: an unpinned workload image is left in the rendered compose file" >&2
        exit 1
    fi
    printf 'FROM scratch\nCOPY docker-compose.yaml /docker-compose.yaml\n' > "$stage/Dockerfile"
    config="$REG/lifecycle-gc-config:$TAG-$PROFILE"
    docker buildx build --platform linux/amd64 \
        --output "type=image,name=$config,push=true,oci-mediatypes=true" \
        --metadata-file "$stage/meta.json" "$stage"
    digest=$(jq -r '."containerimage.digest" // empty' "$stage/meta.json")
    [ -n "$digest" ] || { echo "refused: buildx reported no config image digest" >&2; exit 1; }
    mkdir -p "$WL/out"
    rendered="$WL/out/config-rendered-$LOCAL_TAG-$PROFILE.yaml"
    cp "$stage/docker-compose.yaml" "$rendered"
    echo "server image: $server_pin (built from $SERVER_REF)"
    echo "driver image: $driver_pin (built from $DRIVER_REF)"
    echo "rendered compose: $rendered"
    echo "CONFIG_IMAGE=$REG/lifecycle-gc-config@$digest"
    if [ -n "$MUTANT" ]; then
        echo "launch with: RAVEL_ANTITHESIS_MUTANT=$MUTANT scripts/launch.sh launch $REG/lifecycle-gc-config@$digest <hours> $PROFILE"
    else
        echo "launch with: scripts/launch.sh launch $REG/lifecycle-gc-config@$digest <hours> $PROFILE"
    fi
    rm -rf "$stage"
    ;;
launch)
    shift
    CONFIG_IMAGE="${1:?config image reference}"
    HOURS="${2:?max compute hours}"
    PROFILE="${3:-default}"
    MUTANT="${RAVEL_ANTITHESIS_MUTANT:-}"
    DESC="${4:-ravel lifecycle-gc ($PROFILE${MUTANT:+, mutant $MUTANT})}"
    case "$CONFIG_IMAGE" in *@sha256:*) ;; *) echo "refused: pin the config image by digest" >&2; exit 64 ;; esac
    NAME=ravel-lifecycle-gc
    [ "$PROFILE" = claims-off ] && NAME=ravel-lifecycle-gc-claims-off
    [ -n "$MUTANT" ] && NAME=ravel-lifecycle-gc-mutant
    snouty launch --launcher basic_test --config-image "$CONFIG_IMAGE" \
        --test-name "$NAME" \
        --description "$DESC" \
        --max-compute-hours "$HOURS" \
        --fault-tolerance high_availability \
        --fault-exclusions '^driver$' \
        --setup-timeout 15m \
        --param attrs.workload=lifecycle-gc \
        --param attrs.profile="$PROFILE" \
        --param attrs.mutant="${MUTANT:-none}" \
        --param attrs.ravel_ref="$(git -C "$WL" rev-parse HEAD)" \
        --param attrs.plugin_version=0.1.1
    ;;
*)
    echo "usage: $0 push [--profile default|claims-off] [--mutant lifecycle/<stem>] | launch <config-image@digest> <hours> [profile] [description]" >&2
    exit 64
    ;;
esac
