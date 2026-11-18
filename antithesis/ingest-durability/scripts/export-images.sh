#!/bin/bash
# On the build host: save the workload images to S3, for import-images.sh to
# load and push from a machine that holds registry credentials.
# Usage: scripts/export-images.sh s3://<bucket>/<prefix>
#   MUTANTS=1  save the ingest-durability-mutants images (build.sh MUTANTS=1)
#              as <name>-mutants.tar.zst, beside the plain archives.
set -euo pipefail
DEST="${1:?s3 destination prefix}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REG="${REGISTRY:-819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis}"
# shellcheck source=antithesis/ingest-durability/scripts/image-tag.sh
. "$HERE/image-tag.sh"
for name in ravel-server driver; do
    image="$REG/$name:$TAG"
    archive="$DEST/$name$ARCHIVE_SUFFIX.tar.zst"
    docker image inspect -f "$name {{.Id}}" "$image"
    docker save "$image" | zstd -q -T0 -3 | aws s3 cp - "$archive" --only-show-errors
    echo "saved $image to $archive"
done
