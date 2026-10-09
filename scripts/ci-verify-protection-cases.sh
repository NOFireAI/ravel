#!/usr/bin/env bash
# The negative path of `ravel-cli store verify-protection` against a real
# S3-compatible endpoint (ADR-1727 decision 6, issue #2672). CI runs it in the
# object-store-contract job against RustFS, on the bucket
# scripts/ci-create-bucket.sh provisioned.
#
# Usage: scripts/ci-verify-protection-cases.sh [<endpoint-url> [<bucket>]]
#
# The endpoint and bucket come from the arguments, else from RAVEL_S3_ENDPOINT
# and RAVEL_S3_BUCKET. Credentials and region come from AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION, which the aws CLI reads and
# which are handed to ravel-cli as its RAVEL_S3_* variables. Nothing has a
# default: the first one missing exits 64 and names it. `aws` and `ravel-cli`
# must be on PATH (exit 69 otherwise).
#
# Cases, in order. The control case runs the subcommand on the bucket as given
# and expects exit 0. Each breaking case then changes one setting with the aws
# CLI, runs the subcommand, and expects exit 1 with exactly the listed
# conditions failed, both on the per-condition lines and in the summary, and
# nothing reported as could-not-verify. It then puts the compliant setting back
# and expects exit 0 again before the next case.
#
#   versioning-suspended     versioning Suspended          versioning
#   no-noncurrent-expiration rule without NoncurrentVersionExpiration
#                                                          noncurrent-expiration, rule-scope
#   no-expired-delete-marker rule without ExpiredObjectDeleteMarker
#                                                          expired-delete-marker, rule-scope
#   no-abort-multipart       rule without AbortIncompleteMultipartUpload
#                                                          abort-multipart, rule-scope
#   foreign-rule             a second rule expiring sys/ after 30 days
#                                                          no-foreign-rule
#
# Removing an action from the one covering rule also fails rule-scope, since no
# rule covering t/ then carries it; the subcommand names both. object-lock
# cannot be broken (S3 has no call that disables Object Lock on a bucket),
# delete-marker-replication is not expected without --expect-replication, and
# object-retention is not checked by the subcommand.
#
# A case whose change the store rejects with a non-transient error, or whose
# subcommand run exits 2 reporting the case's first condition as could not
# verify, is SKIPPED with the reason: the store cannot express that broken
# state, which proves nothing either way. A SKIPPED case still runs the
# subcommand afterwards and expects exit 0.
#
# Prints one line per case. Exits 0 when every case behaved and at least one
# breaking case was not skipped; 1 naming the first case that did not behave,
# or when every breaking case was skipped; 64 on usage. A case that leaves the
# bucket broken (the restore put failed, or the subcommand does not exit 0
# after it) says so and exits 1.
#
# Every s3api call is retried the way scripts/ci-create-bucket.sh retries:
# again with backoff on exit 255 or a 5xx-class error code, at once otherwise.
# CI_VERIFY_PROTECTION_BACKOFF_SECONDS (default 5) is the backoff unit.
#
# Cases: scripts/ci-verify-protection-cases.test.sh.
set -uo pipefail

usage() {
  echo "usage: $0 [<endpoint-url> [<bucket>]] (or RAVEL_S3_ENDPOINT, RAVEL_S3_BUCKET; credentials from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION)" >&2
}

if [ "$#" -gt 2 ]; then
  usage
  exit 64
fi
endpoint=${1:-${RAVEL_S3_ENDPOINT:-}}
bucket=${2:-${RAVEL_S3_BUCKET:-}}
for pair in "endpoint:$endpoint" "bucket:$bucket" \
  "AWS_ACCESS_KEY_ID:${AWS_ACCESS_KEY_ID:-}" \
  "AWS_SECRET_ACCESS_KEY:${AWS_SECRET_ACCESS_KEY:-}" \
  "AWS_DEFAULT_REGION:${AWS_DEFAULT_REGION:-}"; do
  if [ -z "${pair#*:}" ]; then
    echo "missing: ${pair%%:*}" >&2
    usage
    exit 64
  fi
done

for tool in aws ravel-cli; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "::error::$tool not found on PATH" >&2
    exit 69
  fi
done

export AWS_EC2_METADATA_DISABLED=true
export AWS_PAGER=""

# The bucket ci-create-bucket.sh provisions: one whole-bucket rule with the
# three sanctioned actions, NoncurrentDays 1.
EXPECTED_NONCURRENT_DAYS=1
LIFECYCLE_COMPLIANT='{"Rules":[{"ID":"ravel","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true},"NoncurrentVersionExpiration":{"NoncurrentDays":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}'
LIFECYCLE_NO_NONCURRENT='{"Rules":[{"ID":"ravel","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}'
LIFECYCLE_NO_MARKER='{"Rules":[{"ID":"ravel","Filter":{"Prefix":""},"Status":"Enabled","NoncurrentVersionExpiration":{"NoncurrentDays":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}'
LIFECYCLE_NO_ABORT='{"Rules":[{"ID":"ravel","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true},"NoncurrentVersionExpiration":{"NoncurrentDays":1}}]}'
LIFECYCLE_FOREIGN='{"Rules":[{"ID":"ravel","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true},"NoncurrentVersionExpiration":{"NoncurrentDays":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}},{"ID":"foreign","Filter":{"Prefix":"sys/"},"Status":"Enabled","Expiration":{"Days":30}}]}'

attempts=5
backoff=${CI_VERIFY_PROTECTION_BACKOFF_SECONDS:-5}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
err="$work/aws.err"
out="$work/verify.out"

# s3api <subcommand> <arg>...: one s3api call, retried while it fails
# transiently. Returns 0 on success, 75 once every attempt failed transiently,
# and the CLI's own status on a non-transient failure. The last attempt's
# stderr is left in "$err".
s3api() {
  local subcommand=$1 attempt rc
  for attempt in $(seq 1 "$attempts"); do
    rc=0
    aws --endpoint-url "$endpoint" s3api "$@" 2>"$err" || rc=$?
    cat "$err" >&2
    if [ "$rc" -eq 0 ]; then return 0; fi
    if [ "$rc" -ne 255 ] &&
      ! grep -qE '\((InternalError|ServiceUnavailable|SlowDown|RequestTimeout|50[0-9])\)' "$err"; then
      return "$rc"
    fi
    echo "$subcommand failed transiently on attempt $attempt (exit $rc)" >&2
    if [ "$attempt" -lt "$attempts" ]; then sleep $((attempt * backoff)); fi
  done
  return 75
}

# apply <setting>: put one bucket setting. <setting> is versioning-enabled,
# versioning-suspended, or the name of a LIFECYCLE_* variable.
apply() {
  case "$1" in
    versioning-enabled | versioning-suspended)
      local want=Enabled
      [ "$1" = versioning-suspended ] && want=Suspended
      s3api put-bucket-versioning --bucket "$bucket" \
        --versioning-configuration "Status=$want"
      ;;
    *)
      s3api put-bucket-lifecycle-configuration --bucket "$bucket" \
        --lifecycle-configuration "${!1}"
      ;;
  esac
}

# verify: run the subcommand, its stdout into "$out", and echo both streams
# indented; sets $verify_rc.
verify() {
  verify_rc=0
  RAVEL_S3_ENDPOINT="$endpoint" RAVEL_S3_BUCKET="$bucket" \
    RAVEL_S3_REGION="$AWS_DEFAULT_REGION" \
    RAVEL_S3_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
    RAVEL_S3_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" \
    ravel-cli --store s3 store verify-protection \
    --expected-noncurrent-days "$EXPECTED_NONCURRENT_DAYS" >"$out" 2>"$work/verify.err" ||
    verify_rc=$?
  sed 's/^/    /' "$out"
  sed 's/^/    stderr: /' "$work/verify.err"
}

# The conditions the per-condition lines read as fail, sorted, comma-joined.
failed_lines() {
  awk '$1 !~ /^verify-protection:/ && $2 == "fail" { print $1 }' "$out" |
    LC_ALL=C sort | paste -sd, -
}
# The summary's failed list and could-not-verify list, sorted, comma-joined.
summary_list() {
  local which=$1
  grep '^verify-protection: ' "$out" | tail -n 1 |
    sed -n "s/.*${which}: \([^;]*\).*/\1/p" | tr -d ' ' | tr ',' '\n' |
    sed '/^$/d' | LC_ALL=C sort | paste -sd, -
}
condition_line() {
  awk -v id="$1" '$1 == id' "$out"
}

fail_case() {
  echo "FAIL $1: $2"
  echo "::error::verify-protection case $1 did not behave: $2" >&2
}

# expect_compliant: run the subcommand; fails unless it exited 0.
expect_compliant() {
  verify
  [ "$verify_rc" -eq 0 ]
}

# restore <case> <setting>: put the compliant setting back and prove it with an
# exit 0. Exits 1 when the bucket was left broken.
restore() {
  local rc=0
  apply "$2" || rc=$?
  if [ "$rc" -ne 0 ]; then
    fail_case "$1" "restoring $2 failed (exit $rc); the bucket was left broken"
    exit 1
  fi
  if ! expect_compliant; then
    fail_case "$1" "verify-protection exited $verify_rc after restoring $2, not 0; the bucket was left broken"
    exit 1
  fi
}

if ! expect_compliant; then
  fail_case control "verify-protection exited $verify_rc on the compliant bucket, not 0"
  exit 1
fi
echo "PASS control: exit 0 on the compliant bucket"

ran=0
skipped=0
# run_case <name> <break-setting> <restore-setting> <want>: <want> is the
# expected failed conditions, sorted and comma-joined; its first entry is the
# condition the case targets.
run_case() {
  local name=$1 breaking=$2 restoring=$3 want=$4 rc=0 primary got_lines got_summary
  local got_unknown
  primary=${want%%,*}
  apply "$breaking" || rc=$?
  if [ "$rc" -eq 75 ]; then
    fail_case "$name" "putting $breaking still failed transiently after $attempts attempts"
    exit 1
  fi
  if [ "$rc" -ne 0 ]; then
    echo "SKIPPED $name: the store rejected $breaking (exit $rc: $(tail -n 1 "$err"))"
    skipped=$((skipped + 1))
    if ! expect_compliant; then
      fail_case "$name" "verify-protection exited $verify_rc after the rejected change, not 0; the bucket was left broken"
      exit 1
    fi
    return 0
  fi
  verify
  got_lines=$(failed_lines)
  got_summary=$(summary_list failed)
  got_unknown=$(summary_list "could not verify")
  if [ "$verify_rc" -eq 2 ] && [ -z "$got_lines" ] &&
    [ "$(condition_line "$primary" | awk '{ print $2 }')" = unknown ]; then
    echo "SKIPPED $name: the store's broken state reads unknown, not fail: $(condition_line "$primary")"
    skipped=$((skipped + 1))
    restore "$name" "$restoring"
    return 0
  fi
  local why=
  if [ "$verify_rc" -ne 1 ]; then
    why="verify-protection exited $verify_rc, want 1"
  elif [ "$got_lines" != "$want" ]; then
    why="condition lines read fail for [$got_lines], want [$want]"
  elif [ "$got_summary" != "$want" ]; then
    why="summary names [$got_summary] as failed, want [$want]"
  elif [ -n "$got_unknown" ]; then
    why="summary could not verify [$got_unknown], want none"
  fi
  if [ -n "$why" ]; then
    fail_case "$name" "$why"
    restore "$name" "$restoring"
    exit 1
  fi
  restore "$name" "$restoring"
  echo "PASS $name: exit 1 naming exactly [$want], exit 0 after restore"
  ran=$((ran + 1))
}

run_case versioning-suspended versioning-suspended versioning-enabled \
  versioning
run_case no-noncurrent-expiration LIFECYCLE_NO_NONCURRENT LIFECYCLE_COMPLIANT \
  noncurrent-expiration,rule-scope
run_case no-expired-delete-marker LIFECYCLE_NO_MARKER LIFECYCLE_COMPLIANT \
  expired-delete-marker,rule-scope
run_case no-abort-multipart LIFECYCLE_NO_ABORT LIFECYCLE_COMPLIANT \
  abort-multipart,rule-scope
run_case foreign-rule LIFECYCLE_FOREIGN LIFECYCLE_COMPLIANT \
  no-foreign-rule

if [ "$ran" -eq 0 ]; then
  echo "FAIL: every breaking case was SKIPPED ($skipped); this run proved nothing"
  echo "::error::verify-protection: every breaking case was skipped" >&2
  exit 1
fi
echo "verify-protection cases: $ran passed, $skipped skipped"
