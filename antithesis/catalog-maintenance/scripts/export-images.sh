#!/bin/bash
# On the build host: save the workload images to S3, for import-images.sh to
# load and push from a machine that holds registry credentials.
# Usage: scripts/export-images.sh s3://<bucket>/<prefix>
#   MUTANTS=1  save the catalog-maintenance-mutants images instead, as
#              <name>.mutants.tar.zst beside the plain ones
set -euo pipefail
DEST="${1:?s3 destination prefix}"
REG="${REGISTRY:-819709950457.dkr.ecr.us-east-1.amazonaws.com/ravel-antithesis}"
case "${MUTANTS:-}" in
    '' | 0) TAG="${TAG:-catalog-maintenance}" SUFFIX="" ;;
    1)
        # build.sh, launch.sh, local-run.sh and mutant-config.sh all name this tag.
        if [ -n "${TAG:-}" ]; then
            echo "refused: TAG cannot be set with MUTANTS=1; the mutant images are always tagged catalog-maintenance-mutants" >&2
            exit 1
        fi
        TAG=catalog-maintenance-mutants SUFFIX=".mutants"
        ;;
    *) echo "refused: MUTANTS must be 1 or unset, not '$MUTANTS'" >&2; exit 1 ;;
esac
for name in ravel-server driver; do
    image="$REG/$name:$TAG"
    docker image inspect -f "$name {{.Id}}" "$image"
    docker save "$image" | zstd -q -T0 -3 | aws s3 cp - "$DEST/$name$SUFFIX.tar.zst" --only-show-errors
    echo "saved $image to $DEST/$name$SUFFIX.tar.zst"
done
