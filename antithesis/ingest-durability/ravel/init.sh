#!/bin/sh
# One-shot setup before the ravel-server nodes start: create the bucket and
# write the store qualification record that ravel-server requires before it
# serves (ADR-0050 section 6). Both steps are idempotent.
set -u
endpoint="${RAVEL_S3_ENDPOINT:?}"
bucket="${RAVEL_S3_BUCKET:?}"
sig="aws:amz:${RAVEL_S3_REGION:?}:s3"
creds="${RAVEL_S3_ACCESS_KEY:?}:${RAVEL_S3_SECRET_KEY:?}"

last=""
until code=$(curl -s -o /dev/null -w '%{http_code}' --aws-sigv4 "$sig" --user "$creds" "$endpoint/") \
    && [ "$code" = 200 ]; do
    if [ "$code" != "$last" ]; then
        echo "init: waiting for the object store at $endpoint (answered ${code:-nothing})"
        last="$code"
    fi
    sleep 1
done

code=$(curl -s -o /tmp/create.out -w '%{http_code}' -X PUT --aws-sigv4 "$sig" --user "$creds" "$endpoint/$bucket")
case "$code" in
    200) echo "init: created bucket $bucket" ;;
    409) echo "init: bucket $bucket already exists" ;;
    *) echo "init: creating bucket $bucket answered $code"; cat /tmp/create.out; exit 1 ;;
esac

exec /usr/local/bin/ravel-cli --store s3 store qualify
