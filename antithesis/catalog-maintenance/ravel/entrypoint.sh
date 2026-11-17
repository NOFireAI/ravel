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
#
# Both waits together give up after RAVEL_ENTRYPOINT_WAIT_SECS (900 s, the
# local loop's setup bound; the driver's own setup budget is 600 s), measured
# on /proc/uptime because the wall clock jumps under libfaketime.
trap 'exit 143' TERM INT
sig="aws:amz:${RAVEL_S3_REGION:?}:s3"
creds="${RAVEL_S3_ACCESS_KEY:?}:${RAVEL_S3_SECRET_KEY:?}"
base="${RAVEL_S3_ENDPOINT:?}/${RAVEL_S3_BUCKET:?}"
marker="${SEED_MARKER:-antithesis/catalog-maintenance/seeded}"
limit="${RAVEL_ENTRYPOINT_WAIT_SECS:-900}"

uptime_s() {
    read -r up _ < /proc/uptime
    echo "${up%.*}"
}
deadline=$(($(uptime_s) + limit))

answer() {
    curl -s --max-time 10 -o /dev/null -w '%{http_code}' --aws-sigv4 "$sig" --user "$creds" "$1"
}

wait_for() {
    what="$1"
    url="$2"
    announced=""
    until [ "$(answer "$url")" = 200 ]; do
        if [ "$(uptime_s)" -ge "$deadline" ]; then
            echo "entrypoint: gave up after ${limit}s waiting for $what" >&2
            exit 1
        fi
        if [ -z "$announced" ]; then
            echo "entrypoint: waiting for $what"
            announced=1
        fi
        sleep 1
    done
}

wait_for "the object store" "$base?max-keys=1"
wait_for "the seed marker $marker" "$base/$marker"
exec /usr/local/bin/ravel-server "$@"
