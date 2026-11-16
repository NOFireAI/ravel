#!/bin/bash
# On the machine that launches runs: load the images export-images.sh saved,
# push them to ECR, pull them back, and pull the store image, so every image
# the compose file names is in the local image store for `snouty launch`.
# Usage: scripts/import-images.sh s3://<bucket>/<prefix>
set -euo pipefail
SRC="${1:?s3 source prefix}"
export AWS_PROFILE="${AWS_PROFILE:-nf-sandbox}" AWS_REGION="${AWS_REGION:-us-east-1}" AWS_PAGER=''
ACCOUNT_REG="819709950457.dkr.ecr.us-east-1.amazonaws.com"
REG="$ACCOUNT_REG/ravel-antithesis"
TAG="${TAG:-ingest-durability}"
HERE="$(cd "$(dirname "$0")" && pwd)"
COMPOSE="$HERE/../config/docker-compose.yaml"

aws ecr get-login-password | docker login --username AWS --password-stdin "$ACCOUNT_REG"
for name in ravel-server driver; do
    aws ecr describe-repositories --repository-names "ravel-antithesis/$name" > /dev/null 2>&1 \
        || aws ecr create-repository --repository-name "ravel-antithesis/$name" > /dev/null
    aws s3 cp "$SRC/$name.tar.zst" - --only-show-errors | zstd -q -d | docker load
    docker push -q "$REG/$name:$TAG"
    docker pull -q --platform linux/amd64 "$REG/$name:$TAG"
done
store=$(sed -n 's/^ *image: \(ghcr.io\/rustfs\/[^ ]*\)$/\1/p' "$COMPOSE")
docker pull -q --platform linux/amd64 "$store"
