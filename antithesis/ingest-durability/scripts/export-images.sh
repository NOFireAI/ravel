#!/bin/bash
# On the build host: save the workload images to S3, for import-images.sh to
# load and push from a machine that holds registry credentials.
# Usage: scripts/export-images.sh s3://<bucket>/<prefix>
set -euo pipefail
DEST="${1:?s3 destination prefix}"
REG="${REGISTRY:-819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis}"
TAG="${TAG:-ingest-durability}"
for name in ravel-server driver; do
    image="$REG/$name:$TAG"
    docker image inspect -f "$name {{.Id}}" "$image"
    docker save "$image" | zstd -q -T0 -3 | aws s3 cp - "$DEST/$name.tar.zst" --only-show-errors
    echo "saved $image to $DEST/$name.tar.zst"
done
