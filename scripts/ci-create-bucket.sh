#!/usr/bin/env bash
# Create an S3 bucket on a local S3-compatible endpoint (RustFS in CI) with the
# aws CLI installed on the host, so no job pulls the AWS CLI container image.
# ECR Public caps anonymous pulls per source IP, and a throttled pull used to
# fail the job before any Ravel code ran (issue #2036). GitHub's ubuntu runner
# images ship the aws CLI v2.
#
# Usage: scripts/ci-create-bucket.sh <endpoint-url> <bucket>
#
# The bucket is created the way a --require-bucket-protection server needs it
# (docs/object-store-contract.md "Required bucket configuration"), with the
# same settings and read-back as deploy/k8s/rustfs.yaml: Object Lock enabled
# at creation, versioning on, and one whole-bucket lifecycle rule carrying the
# sanctioned actions. crates/ravel-object-store's
# `launcher_lifecycle_documents_pass_every_in_process_condition` test reads the
# lifecycle document from this file.
#
# Credentials and region come from the standard AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION variables. The create-bucket
# call is retried with backoff only when it failed transiently: exit 255 (the
# CLI got no usable response: connection refused, reset or timed out) or a
# 5xx-class S3 error code. Any other failure (bad credentials, an invalid
# bucket name) is permanent and fails at once. A bucket already owned by these
# credentials counts as created on any attempt, the first included: an earlier
# attempt may have created it before its response was lost, and a rerun
# against a populated endpoint should not fail. CI_CREATE_BUCKET_BACKOFF_SECONDS
# (default 5) is the backoff unit; attempt n waits n units before the next.
#
# Versioning and the lifecycle rule are then put, which replaces the whole
# configuration, so a rerun is a no-op. Every setting is read back and checked;
# an existing bucket created without Object Lock fails the read-back, since
# Object Lock can only be enabled at creation.
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

# NoncurrentDays 1 keeps a CI bucket's noncurrent versions briefly; a
# production bucket sets its own E_v (docs/guides/disaster-recovery.md).
LIFECYCLE='{"Rules":[{"ID":"ravel","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true},"NoncurrentVersionExpiration":{"NoncurrentDays":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}'

attempts=5
backoff=${CI_CREATE_BUCKET_BACKOFF_SECONDS:-5}
err=$(mktemp)
trap 'rm -f "$err"' EXIT

created=no
for attempt in $(seq 1 "$attempts"); do
  rc=0
  aws --endpoint-url "$endpoint" s3api create-bucket --bucket "$bucket" \
    --object-lock-enabled-for-bucket 2>"$err" || rc=$?
  cat "$err" >&2
  if [ "$rc" -eq 0 ]; then
    echo "created bucket $bucket at $endpoint"
    created=yes
    break
  fi
  if grep -q '(BucketAlreadyOwnedByYou)' "$err"; then
    echo "bucket $bucket already exists and is owned by these credentials"
    created=yes
    break
  fi
  if [ "$rc" -ne 255 ] &&
    ! grep -qE '\((InternalError|ServiceUnavailable|SlowDown|RequestTimeout|50[0-9])\)' "$err"; then
    echo "create-bucket failed with a non-transient error (exit $rc)" >&2
    exit "$rc"
  fi
  echo "create-bucket failed transiently on attempt $attempt (exit $rc)" >&2
  if [ "$attempt" -lt "$attempts" ]; then sleep $((attempt * backoff)); fi
done
if [ "$created" != yes ]; then
  echo "::error::create-bucket still failing after $attempts attempts" >&2
  exit 1
fi

rc=0
aws --endpoint-url "$endpoint" s3api put-bucket-versioning --bucket "$bucket" \
  --versioning-configuration Status=Enabled || rc=$?
if [ "$rc" -ne 0 ]; then
  echo "::error::put-bucket-versioning on $bucket failed (exit $rc)" >&2
  exit "$rc"
fi
aws --endpoint-url "$endpoint" s3api put-bucket-lifecycle-configuration \
  --bucket "$bucket" --lifecycle-configuration "$LIFECYCLE" || rc=$?
if [ "$rc" -ne 0 ]; then
  echo "::error::put-bucket-lifecycle-configuration on $bucket failed (exit $rc)" >&2
  exit "$rc"
fi

# readback <setting> <want> <get-subcommand> <query>: read one value of the
# bucket's configuration and fail unless it is <want>.
readback() {
  local setting=$1 want=$2 subcommand=$3 query=$4 got rc=0
  got=$(aws --endpoint-url "$endpoint" s3api "$subcommand" --bucket "$bucket" \
    --query "$query" --output text) || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "::error::$subcommand on $bucket failed (exit $rc); cannot confirm $setting" >&2
    exit 1
  fi
  if [ "$got" != "$want" ]; then
    echo "::error::bucket $bucket does not carry $setting (want $want, read $got)" >&2
    exit 1
  fi
}

readback "Object Lock" Enabled get-object-lock-configuration \
  ObjectLockConfiguration.ObjectLockEnabled
readback versioning Enabled get-bucket-versioning Status
readback "the multipart-abort rule" 7 get-bucket-lifecycle-configuration \
  'Rules[0].AbortIncompleteMultipartUpload.DaysAfterInitiation'
readback "the noncurrent-expiration rule" 1 get-bucket-lifecycle-configuration \
  'Rules[0].NoncurrentVersionExpiration.NoncurrentDays'
readback "the expired-delete-marker rule" true get-bucket-lifecycle-configuration \
  'to_string(Rules[0].Expiration.ExpiredObjectDeleteMarker)'
echo "bucket $bucket ready: Object Lock, versioning and lifecycle rules set"
