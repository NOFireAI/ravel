#!/bin/bash
# On the machine that launches runs: load the images export-images.sh saved,
# push them to ECR, pull them back, and pull the store image, so every image
# the compose file names is in the local image store for `snouty launch`.
# Usage: scripts/import-images.sh s3://<bucket>/<prefix>
#   MUTANTS=1  load the ingest-durability-mutants archives
#              (export-images.sh MUTANTS=1) and tag them for launch.sh's
#              mutant runs.
set -euo pipefail
SRC="${1:?s3 source prefix}"
export AWS_PROFILE="${AWS_PROFILE:-nf-sandbox}" AWS_REGION="${AWS_REGION:-us-east-1}" AWS_PAGER=''
ACCOUNT_REG="819709950457.dkr.ecr.us-east-1.amazonaws.com"
REG="$ACCOUNT_REG/ravel-antithesis"
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=antithesis/ingest-durability/scripts/image-tag.sh
. "$HERE/image-tag.sh"
COMPOSE="$HERE/../config/docker-compose.yaml"

aws ecr get-login-password | docker login --username AWS --password-stdin "$ACCOUNT_REG"
for name in ravel-server driver; do
    aws ecr describe-repositories --repository-names "ravel-antithesis/$name" > /dev/null 2>&1 \
        || aws ecr create-repository --repository-name "ravel-antithesis/$name" > /dev/null
    aws s3 cp "$SRC/$name$ARCHIVE_SUFFIX.tar.zst" - --only-show-errors | zstd -q -d | docker load
    docker push -q "$REG/$name:$TAG"
    docker pull -q --platform linux/amd64 "$REG/$name:$TAG"
    # The compose file names the local-only tag, which snouty pushes into the
    # tenant registry (config/docker-compose.yaml header).
    case "$name" in
        ravel-server) local_name="ravel-antithesis-server" ;;
        driver) local_name="ravel-antithesis-driver" ;;
    esac
    docker tag "$REG/$name:$TAG" "$local_name:$TAG"
done
store=$(sed -n 's/^ *image: \(ghcr.io\/rustfs\/[^ ]*\)$/\1/p' "$COMPOSE")
docker pull -q --platform linux/amd64 "$store"
