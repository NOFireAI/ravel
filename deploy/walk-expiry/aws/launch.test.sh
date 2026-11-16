#!/bin/bash
# Cases for launch.sh --dry-run: the rendered user data arms the 10-hour
# shutdown before anything else, the launch terminates on shutdown, and the
# argument checks refuse what they say they refuse. Runs no aws command.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
launch="$here/launch.sh"
fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1"; fails=$((fails + 1)); }

sha=$(printf '%040d' 0)
env_ok=(WE_RESULTS_BUCKET=example-results WE_INSTANCE_PROFILE=example-profile
  WE_SUBNET_ID=subnet-0example WE_SECURITY_GROUP_ID=sg-0example AWS_REGION=us-east-1)

out=$(env "${env_ok[@]}" bash "$launch" --dry-run "$sha" example-store test 2>&1)
code=$?
[ "$code" -eq 0 ] && pass "dry run exits 0" || fail "dry run exits 0 (got $code): $out"

user_data=$(printf '%s\n' "$out" | sed -n '/^--- rendered user data ---$/,/^--- end user data ---$/p')
first_command=$(printf '%s\n' "$user_data" | sed '1d' | grep -v '^#' | grep -v '^$' | head -n 1)
[ "$first_command" = "shutdown -h +600" ] \
  && pass "the first command of the user data is shutdown -h +600" \
  || fail "the first command of the user data is shutdown -h +600 (got '$first_command')"
printf '%s\n' "$out" | grep -q -- '--instance-initiated-shutdown-behavior terminate' \
  && pass "the launch terminates on shutdown" \
  || fail "the launch terminates on shutdown"
printf '%s\n' "$out" | grep -q -- '--instance-type c6a.4xlarge' \
  && pass "one c6a.4xlarge" || fail "one c6a.4xlarge"
printf '%s\n' "$user_data" | grep -q "^COMMIT=${sha}\$" \
  && pass "the commit reaches the box" || fail "the commit reaches the box"
printf '%s\n' "$user_data" | grep -q '^STORE_BUCKET=example-store$' \
  && pass "the store bucket reaches the box" || fail "the store bucket reaches the box"
printf '%s\n' "$out" | grep -q '^+ aws s3 cp' \
  && pass "the uploads are printed" || fail "the uploads are printed"

refused() {
  local name=$1
  shift
  local got
  got=$("$@" >/dev/null 2>&1; echo $?)
  [ "$got" -ne 0 ] && pass "refuses $name" || fail "refuses $name"
}
refused "a 39-character commit" env "${env_ok[@]}" bash "$launch" --dry-run "$(printf '%039d' 0)" example-store test
refused "a 41-character commit" env "${env_ok[@]}" bash "$launch" --dry-run "$(printf '%041d' 0)" example-store test
refused "an uppercase commit" env "${env_ok[@]}" bash "$launch" --dry-run "$(printf 'A%039d' 0)" example-store test
refused "a branch name" env "${env_ok[@]}" bash "$launch" --dry-run main example-store test
refused "an s3:// store" env "${env_ok[@]}" bash "$launch" --dry-run "$sha" s3://example-store test
refused "the results bucket as the store" env "${env_ok[@]}" bash "$launch" --dry-run "$sha" example-results test
refused "a missing results bucket" env WE_INSTANCE_PROFILE=p WE_SUBNET_ID=s WE_SECURITY_GROUP_ID=g AWS_REGION=us-east-1 \
  bash "$launch" --dry-run "$sha" example-store test
refused "a missing instance profile" env WE_RESULTS_BUCKET=r WE_SUBNET_ID=s WE_SECURITY_GROUP_ID=g AWS_REGION=us-east-1 \
  bash "$launch" --dry-run "$sha" example-store test

for f in "$launch" "$here/box-run.sh"; do
  bash -n "$f" && pass "$(basename "$f") parses" || fail "$(basename "$f") parses"
done
grep -q 'shutdown -h +1' "$here/box-run.sh" && grep -q 'FAILED' "$here/box-run.sh" \
  && pass "box-run uploads a FAILED marker and shuts down" \
  || fail "box-run uploads a FAILED marker and shuts down"

[ "$fails" -eq 0 ] || { echo "$fails case(s) failed"; exit 1; }
echo "all cases passed"
