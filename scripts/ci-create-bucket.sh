#!/usr/bin/env bash
# Create an S3 bucket on a local S3-compatible endpoint (RustFS in CI) with the
# aws CLI installed on the host, so no job pulls the AWS CLI container image.
# ECR Public caps anonymous pulls per source IP, and a throttled pull used to
# fail the job before any Ravel code ran (issue #2036). GitHub's ubuntu runner
# images ship the aws CLI v2.
#
# Usage: scripts/ci-create-bucket.sh <endpoint-url> <bucket>
#
# Credentials and region come from the standard AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION variables. The create-bucket
# call is retried with backoff only when it failed transiently: exit 255 (the
# CLI got no usable response: connection refused, reset or timed out) or a
# 5xx-class S3 error code. Any other failure (bad credentials, an invalid
# bucket name) is permanent and fails at once. A bucket already owned by these
# credentials counts as success on any attempt, the first included: an earlier
# attempt may have created it before its response was lost, and a rerun
# against a populated endpoint should not fail. CI_CREATE_BUCKET_BACKOFF_SECONDS
# (default 5) is the backoff unit; attempt n waits n units before the next.
#
# Cases: scripts/ci-create-bucket.test.sh.
set -uo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <endpoint-url> <bucket>" >&2
  exit 64
fi
endpoint=$1
bucket=$2

if ! command -v aws >/dev/null 2>&1; then
  echo "::error::aws CLI not found on PATH" >&2
  exit 69
fi

# No IMDS on a CI runner; without this the CLI spends its metadata timeout
# before using the static credentials.
export AWS_EC2_METADATA_DISABLED=true
export AWS_PAGER=""

attempts=5
backoff=${CI_CREATE_BUCKET_BACKOFF_SECONDS:-5}
err=$(mktemp)
trap 'rm -f "$err"' EXIT

for attempt in $(seq 1 "$attempts"); do
  rc=0
  aws --endpoint-url "$endpoint" s3api create-bucket --bucket "$bucket" 2>"$err" || rc=$?
  cat "$err" >&2
  if [ "$rc" -eq 0 ]; then
    echo "created bucket $bucket at $endpoint"
    exit 0
  fi
  if grep -q '(BucketAlreadyOwnedByYou)' "$err"; then
    echo "bucket $bucket already exists and is owned by these credentials"
    exit 0
  fi
  if [ "$rc" -ne 255 ] &&
    ! grep -qE '\((InternalError|ServiceUnavailable|SlowDown|RequestTimeout|50[0-9])\)' "$err"; then
    echo "create-bucket failed with a non-transient error (exit $rc)" >&2
    exit "$rc"
  fi
  echo "create-bucket failed transiently on attempt $attempt (exit $rc)" >&2
  if [ "$attempt" -lt "$attempts" ]; then sleep $((attempt * backoff)); fi
done
echo "::error::create-bucket still failing after $attempts attempts" >&2
exit 1
