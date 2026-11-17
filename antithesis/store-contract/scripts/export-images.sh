#!/bin/bash
# On the build host: save the workload image to S3, for import-images.sh to
# load and push from a machine that holds registry credentials.
# Usage: scripts/export-images.sh s3://<bucket>/<prefix>
set -euo pipefail
DEST="${1:?s3 destination prefix}"
REG="${REGISTRY:-819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis}"
TAG="${TAG:-store-contract}"
image="$REG/driver:$TAG"
docker image inspect -f "driver {{.Id}}" "$image"
docker save "$image" | zstd -q -T0 -3 | aws s3 cp - "$DEST/driver.tar.zst" --only-show-errors
echo "saved $image to $DEST/driver.tar.zst"
