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
# AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION variables. Every s3api call (the
# create, each put and each read) is retried with backoff only when
# it failed transiently: exit 255 (the CLI got no usable response: connection
# refused, reset or timed out) or a 5xx-class S3 error code. RustFS can answer
# the create and still be settling, so a put or a read can fail transiently
# right after a create that succeeded. Any other failure (bad credentials, an
# invalid bucket name) is permanent and fails at once. A bucket
# already owned by these credentials counts as created on any attempt, the
# first included: an earlier attempt may have created it before its response
# was lost, and a rerun against a populated endpoint should not fail.
# CI_CREATE_BUCKET_BACKOFF_SECONDS (default 5) is the backoff unit; attempt n
# waits n units before the next.
#
# Object Lock is read first: an existing bucket created without it fails before
# anything is changed, since Object Lock can only be enabled at creation.
# Versioning and the lifecycle rule are each read before they are put and put
# only when the read does not already match, then read back, so a rerun
# against a bucket that is already set up issues no put and cannot fail on a
# backend whose metadata writes are failing. The
# lifecycle configuration is read and checked whole: exactly one rule, enabled,
# scoped to the whole bucket (ADR-1727 rule-scope and no-foreign-rule),
# carrying each action.
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

# retry <accept-pattern> <s3api subcommand> <arg>...: run one s3api call,
# retrying it while it fails transiently. A failure whose stderr matches
# <accept-pattern> (skipped when empty) counts as success. Exits the script with
# the call's own status on a non-transient failure, and with 1 once every
# attempt failed transiently. The last attempt's stderr is left in "$err".
retry() {
  local accept=$1 subcommand=$2 attempt rc
  shift
  for attempt in $(seq 1 "$attempts"); do
    rc=0
    aws --endpoint-url "$endpoint" s3api "$@" 2>"$err" || rc=$?
    cat "$err" >&2
    if [ "$rc" -eq 0 ]; then return 0; fi
    if [ -n "$accept" ] && grep -q "$accept" "$err"; then return 0; fi
    if [ "$rc" -ne 255 ] &&
      ! grep -qE '\((InternalError|ServiceUnavailable|SlowDown|RequestTimeout|50[0-9])\)' "$err"; then
      echo "::error::$subcommand on $bucket failed with a non-transient error (exit $rc)" >&2
      exit "$rc"
    fi
    echo "$subcommand failed transiently on attempt $attempt (exit $rc)" >&2
    if [ "$attempt" -lt "$attempts" ]; then sleep $((attempt * backoff)); fi
  done
  echo "::error::$subcommand on $bucket still failing after $attempts attempts" >&2
  exit 1
}

retry '(BucketAlreadyOwnedByYou)' create-bucket --bucket "$bucket" \
  --object-lock-enabled-for-bucket
if grep -q '(BucketAlreadyOwnedByYou)' "$err"; then
  echo "bucket $bucket already exists and is owned by these credentials"
else
  echo "created bucket $bucket at $endpoint"
fi
# read_setting <setting> <get-subcommand> <query>: print one value of the
# bucket's configuration. The read goes through retry, since a backend still
# settling can answer a GET transiently too.
read_setting() {
  local setting=$1 subcommand=$2 query=$3 got rc=0
  got=$(retry '' "$subcommand" --bucket "$bucket" \
    --query "$query" --output text) || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "::error::$subcommand on $bucket failed (exit $rc); cannot confirm $setting" >&2
    exit 1
  fi
  printf '%s\n' "$got"
}

# readback <setting> <want> <get-subcommand> <query>: read one value and fail
# unless it is <want>.
readback() {
  local got
  got=$(read_setting "$1" "$3" "$4") || exit 1
  if [ "$got" != "$2" ]; then
    echo "::error::bucket $bucket does not carry $1 (want $2, read $got)" >&2
    exit 1
  fi
}

# lifecycle_matches: one read of the whole lifecycle configuration, printed as
# tab-separated fields: rule count, Status, scope, then the three actions.
# Every field goes through to_string, so none is empty and the split cannot
# shift. The scope is the rule's Filter and legacy rule-level Prefix as one JSON
# object. Returns 1 with the mismatch in $why when the configuration is absent
# or differs; exits on a read that fails any other way.
lifecycle_matches() {
  local got rc=0 rules rule_status scope markers noncurrent abort
  got=$(retry '(NoSuchLifecycleConfiguration)' get-bucket-lifecycle-configuration \
    --bucket "$bucket" --output text --query '[to_string(length(Rules)), to_string(Rules[0].Status), to_string({f: Rules[0].Filter, p: Rules[0].Prefix}), to_string(Rules[0].Expiration.ExpiredObjectDeleteMarker), to_string(Rules[0].NoncurrentVersionExpiration.NoncurrentDays), to_string(Rules[0].AbortIncompleteMultipartUpload.DaysAfterInitiation)]') || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "::error::get-bucket-lifecycle-configuration on $bucket failed (exit $rc)" >&2
    exit 1
  fi
  if grep -q '(NoSuchLifecycleConfiguration)' "$err"; then
    why="bucket $bucket carries no lifecycle configuration"
    return 1
  fi
  IFS=$'\t' read -r rules rule_status scope markers noncurrent abort <<<"$got"
  lifecycle_want "exactly one lifecycle rule" 1 "$rules" || return 1
  lifecycle_want "an enabled lifecycle rule" Enabled "$rule_status" || return 1
  # The whole-bucket spellings the server's rule parser reads as an empty
  # prefix. This reads through botocore, which drops unknown elements and text
  # inside Filter and keeps only the first of two Filters. Text beside a
  # Filter's one Prefix is harmless: the server ignores it too and reads the
  # whole bucket. An unknown element inside Filter, text in a Filter with no
  # child, or a second Filter reads back as an accepted spelling here, while the
  # server reads it as an unrecognized filter, reports the lifecycle conditions
  # unknown, and starts with a warning rather than refusing.
  case "$scope" in
    '{"f":{"Prefix":""},"p":null}' | '{"f":{},"p":null}' | \
      '{"f":{"And":{"Prefix":""}},"p":null}' | '{"f":null,"p":""}') ;;
    *)
      why="bucket $bucket lifecycle rule is not scoped to the whole bucket (read $scope)"
      return 1
      ;;
  esac
  lifecycle_want "the expired-delete-marker rule" true "$markers" || return 1
  lifecycle_want "the noncurrent-expiration rule" 1 "$noncurrent" || return 1
  lifecycle_want "the multipart-abort rule" 7 "$abort"
}
lifecycle_want() {
  if [ "$3" != "$2" ]; then
    why="bucket $bucket does not carry $1 (want $2, read $3)"
    return 1
  fi
}

readback "Object Lock" Enabled get-object-lock-configuration \
  ObjectLockConfiguration.ObjectLockEnabled

versioning=$(read_setting versioning get-bucket-versioning Status) || exit 1
if [ "$versioning" = Enabled ]; then
  echo "bucket $bucket already has versioning Enabled; not putting it"
else
  retry '' put-bucket-versioning --bucket "$bucket" \
    --versioning-configuration Status=Enabled
  readback versioning Enabled get-bucket-versioning Status
fi

why=
if lifecycle_matches; then
  echo "bucket $bucket already carries the lifecycle rule; not putting it"
else
  echo "putting the lifecycle rule: $why"
  retry '' put-bucket-lifecycle-configuration \
    --bucket "$bucket" --lifecycle-configuration "$LIFECYCLE"
  if ! lifecycle_matches; then
    echo "::error::$why" >&2
    exit 1
  fi
fi
echo "bucket $bucket ready: Object Lock, versioning and lifecycle rules set"
