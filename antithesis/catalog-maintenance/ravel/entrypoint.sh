#!/bin/sh
# Every ravel-server container's entrypoint: wait until the store answers and
# the driver has written the seed marker, then exec ravel-server as the
# container's main process.
#
# The store wait is ingest-durability's: ravel-server exits within seconds when
# the store does not answer its startup read, and nothing would start it again.
# The marker wait is this workload's: `ravel-cli load --ingest-hour-stride`
# refuses a tenant that already has a HEAD or a commit, and stops if a fold
# runs while it loads, so no server, and so no fold, ingest or maintenance
# pass, may start before the seeded load has finished. A restarted container
# finds the marker at once.
trap 'exit 143' TERM INT
sig="aws:amz:${RAVEL_S3_REGION:?}:s3"
creds="${RAVEL_S3_ACCESS_KEY:?}:${RAVEL_S3_SECRET_KEY:?}"
base="${RAVEL_S3_ENDPOINT:?}/${RAVEL_S3_BUCKET:?}"
marker="${SEED_MARKER:-antithesis/catalog-maintenance/seeded}"

answer() {
    curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 "$sig" --user "$creds" "$1"
}

until [ "$(answer "$base?max-keys=1")" = 200 ]; do
    sleep 1
done
announced=""
until [ "$(answer "$base/$marker")" = 200 ]; do
    if [ -z "$announced" ]; then
        echo "entrypoint: waiting for the seed marker $marker"
        announced=1
    fi
    sleep 1
done
exec /usr/local/bin/ravel-server "$@"
