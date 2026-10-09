#!/usr/bin/env bash
# Cases for scripts/ci-verify-protection-cases.sh (issue #2672). The helper runs
# the aws CLI and ravel-cli, so each case puts a stub of each first on PATH.
# The stub aws keeps the bucket's versioning status and lifecycle document in
# files, as a store would. The stub ravel-cli reads them and prints the
# subcommand's report the way services/ravel-cli/src/store.rs renders it, with
# FAKE_CLI_MODE bending it: correct, exit 0 on a broken bucket, naming a
# different condition, naming a second one, exiting 1 naming nothing, or
# reporting the broken condition unknown. CI's green run takes only the
# behaved path against RustFS, so every refusal is covered here or nowhere.
#
# Run: bash scripts/ci-verify-protection-cases.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/ci-verify-protection-cases.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/ci-verify-protection-cases-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

STUB_BIN="${TMP}/stub-bin"
mkdir -p "${STUB_BIN}"

# Stub aws. FAKE_AWS_FAIL_PUT: comma list of put numbers (1-based, counting
# every put call) that fail non-transiently with AccessDenied.
# FAKE_AWS_IGNORE_PUT: the same, for puts that answer success and change
# nothing. FAKE_AWS_REJECT_BREAKS=1 rejects every put that would break the bucket.
# FAKE_AWS_TRANSIENT_PUTS: how many put calls fail transiently (exit 255)
# before puts start answering.
cat >"${STUB_BIN}/aws" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"${FAKE_STATE}/aws.log"
sub= prev= value=
for arg in "$@"; do
  [ "${prev}" = s3api ] && sub=${arg}
  case "${prev}" in
    --versioning-configuration | --lifecycle-configuration) value=${arg} ;;
  esac
  prev=${arg}
done
case "${sub}" in
  put-bucket-versioning | put-bucket-lifecycle-configuration) ;;
  *) echo "stub aws: unexpected ${sub}" >&2; exit 2 ;;
esac
t=$(( $(cat "${FAKE_STATE}/transient" 2>/dev/null || echo 0) + 1 ))
echo "${t}" >"${FAKE_STATE}/transient"
if [ "${t}" -le "${FAKE_AWS_TRANSIENT_PUTS:-0}" ]; then
  echo "Could not connect to the endpoint URL" >&2
  exit 255
fi
n=$(( $(cat "${FAKE_STATE}/puts" 2>/dev/null || echo 0) + 1 ))
echo "${n}" >"${FAKE_STATE}/puts"
if [[ ",${FAKE_AWS_FAIL_PUT:-}," == *",${n},"* ]]; then
  echo "An error occurred (AccessDenied) when calling the operation: denied" >&2
  exit 254
fi
if [[ ",${FAKE_AWS_IGNORE_PUT:-}," == *",${n},"* ]]; then
  exit 0
fi
breaking=0
case "${value}" in
  Status=Suspended) breaking=1 ;;
  Status=Enabled) ;;
  *) [ "${value}" != "$(cat "${FAKE_STATE}/compliant")" ] && breaking=1 ;;
esac
if [ "${breaking}" = 1 ] && [ "${FAKE_AWS_REJECT_BREAKS:-0}" = 1 ]; then
  echo "An error occurred (InvalidBucketState) when calling the operation: rejected" >&2
  exit 254
fi
if [ "${sub}" = put-bucket-versioning ]; then
  echo "${value#Status=}" >"${FAKE_STATE}/versioning"
else
  echo "${value}" >"${FAKE_STATE}/lifecycle"
fi
STUB

# Stub ravel-cli: the subcommand's report over the stub aws's state.
cat >"${STUB_BIN}/ravel-cli" <<'STUB'
#!/usr/bin/env bash
echo "$* endpoint=${RAVEL_S3_ENDPOINT} bucket=${RAVEL_S3_BUCKET}" >>"${FAKE_STATE}/cli.log"
versioning=$(cat "${FAKE_STATE}/versioning")
lifecycle=$(cat "${FAKE_STATE}/lifecycle")
failed=()
[ "${versioning}" = Enabled ] || failed+=(versioning)
missing=0
[[ "${lifecycle}" == *NoncurrentVersionExpiration* ]] || { failed+=(noncurrent-expiration); missing=1; }
[[ "${lifecycle}" == *ExpiredObjectDeleteMarker* ]] || { failed+=(expired-delete-marker); missing=1; }
[[ "${lifecycle}" == *AbortIncompleteMultipartUpload* ]] || { failed+=(abort-multipart); missing=1; }
[ "${missing}" = 1 ] && failed+=(rule-scope)
[[ "${lifecycle}" == *'"Days":'* ]] && failed+=(no-foreign-rule)
unknown=()
case "${FAKE_CLI_MODE:-correct}" in
  correct) ;;
  exit0) failed=() ;;
  wrong) [ "${#failed[@]}" -gt 0 ] && failed=(object-lock) ;;
  extra) [ "${#failed[@]}" -gt 0 ] && failed+=(object-lock) ;;
  silent)
    if [ "${#failed[@]}" -gt 0 ]; then
      echo "error: something went wrong"
      exit 1
    fi
    ;;
  unknown) unknown=("${failed[@]}"); failed=() ;;
esac
verdict() {
  local id=$1 f
  for f in "${failed[@]}"; do [ "${f}" = "${id}" ] && { echo fail; return; }; done
  for f in "${unknown[@]}"; do [ "${f}" = "${id}" ] && { echo unknown; return; }; done
  echo pass
}
for id in versioning noncurrent-expiration expired-delete-marker abort-multipart \
  rule-scope no-foreign-rule; do
  printf '%-26s %s\n' "${id}" "$(verdict "${id}")"
done
printf '%-26s %s\n' delete-marker-replication "unknown not expected, does not affect the exit code"
printf '%-26s %s\n' object-lock "$(verdict object-lock)"
printf '%-26s %s\n' object-retention "unknown not checked by this command, does not affect the exit code"
join() { local IFS=,; echo "$*" | sed 's/,/, /g'; }
if [ "${#failed[@]}" -gt 0 ]; then
  echo "verify-protection: FAIL: failed: $(join "${failed[@]}")"
  exit 1
elif [ "${#unknown[@]}" -gt 0 ]; then
  echo "verify-protection: UNKNOWN: could not verify: $(join "${unknown[@]}")"
  exit 2
fi
echo "verify-protection: PASS: every expected condition passed"
STUB
chmod +x "${STUB_BIN}/aws" "${STUB_BIN}/ravel-cli"

COMPLIANT=$(sed -n "s/^LIFECYCLE_COMPLIANT='\(.*\)'$/\1/p" "${SCRIPT}")
if [ -z "${COMPLIANT}" ]; then
  echo "FAIL: could not read LIFECYCLE_COMPLIANT from ${SCRIPT}"
  exit 1
fi

# run_helper <name> <env assignment>...: fresh compliant state, run the helper
# with the stubs, output in ${TMP}/<name>.out, exit code in $rc.
run_helper() {
  local name=$1
  shift
  export FAKE_STATE="${TMP}/${name}"
  mkdir -p "${FAKE_STATE}"
  echo Enabled >"${FAKE_STATE}/versioning"
  echo "${COMPLIANT}" >"${FAKE_STATE}/lifecycle"
  echo "${COMPLIANT}" >"${FAKE_STATE}/compliant"
  rc=0
  env PATH="${STUB_BIN}:${PATH}" CI_VERIFY_PROTECTION_BACKOFF_SECONDS=0 \
    AWS_ACCESS_KEY_ID=ak AWS_SECRET_ACCESS_KEY=sk AWS_DEFAULT_REGION=us-east-1 \
    "$@" bash "${SCRIPT}" http://localhost:9000 test-bucket \
    >"${TMP}/${name}.out" 2>&1 || rc=$?
}

check() {
  local name=$1 want_rc=$2
  shift 2
  local ok=1 pattern
  if [ "${want_rc}" = nonzero ]; then
    [ "${rc}" -ne 0 ] || ok=0
  else
    [ "${rc}" -eq "${want_rc}" ] || ok=0
  fi
  for pattern in "$@"; do
    if [ "${pattern#!}" != "${pattern}" ]; then
      grep -qF -- "${pattern#!}" "${TMP}/${name}.out" && ok=0
    else
      grep -qF -- "${pattern}" "${TMP}/${name}.out" || ok=0
    fi
  done
  if [ "${ok}" = 1 ]; then
    echo "PASS: ${name}"
    passes=$((passes + 1))
  else
    echo "FAIL: ${name} (exit ${rc}, want ${want_rc}; patterns: $*)"
    sed 's/^/  | /' "${TMP}/${name}.out"
    fails=$((fails + 1))
  fi
}

run_helper behaved
check behaved 0 \
  "PASS control:" \
  "PASS versioning-suspended: exit 1 naming exactly [versioning]" \
  "PASS no-noncurrent-expiration: exit 1 naming exactly [noncurrent-expiration,rule-scope]" \
  "PASS no-expired-delete-marker: exit 1 naming exactly [expired-delete-marker,rule-scope]" \
  "PASS no-abort-multipart: exit 1 naming exactly [abort-multipart,rule-scope]" \
  "PASS foreign-rule: exit 1 naming exactly [no-foreign-rule]" \
  "verify-protection cases: 5 passed, 0 skipped"
# The helper hands the store selection and endpoint to ravel-cli.
if grep -q -- '--store s3 store verify-protection --expected-noncurrent-days 1 endpoint=http://localhost:9000 bucket=test-bucket' \
  "${TMP}/behaved/cli.log"; then
  echo "PASS: behaved-cli-arguments"
  passes=$((passes + 1))
else
  echo "FAIL: behaved-cli-arguments"
  sed 's/^/  | /' "${TMP}/behaved/cli.log"
  fails=$((fails + 1))
fi

run_helper exit0-on-broken FAKE_CLI_MODE=exit0
check exit0-on-broken 1 "FAIL versioning-suspended: verify-protection exited 0, want 1" \
  "!PASS versioning-suspended"

run_helper names-other FAKE_CLI_MODE=wrong
check names-other 1 \
  "FAIL versioning-suspended: condition lines read fail for [object-lock], want [versioning]"

run_helper names-extra FAKE_CLI_MODE=extra
check names-extra 1 \
  "FAIL versioning-suspended: condition lines read fail for [object-lock,versioning], want [versioning]"

run_helper names-none FAKE_CLI_MODE=silent
check names-none 1 \
  "FAIL versioning-suspended: condition lines read fail for [], want [versioning]"

# Put 1 breaks versioning, put 2 restores it.
run_helper restore-fails FAKE_AWS_FAIL_PUT=2
check restore-fails nonzero \
  "FAIL versioning-suspended: restoring versioning-enabled failed (exit 254); the bucket was left broken" \
  "!PASS no-noncurrent-expiration"

# Put 4 restores the lifecycle after the first lifecycle case.
run_helper restore-lifecycle-fails FAKE_AWS_FAIL_PUT=4
check restore-lifecycle-fails nonzero \
  "FAIL no-noncurrent-expiration: restoring LIFECYCLE_COMPLIANT failed (exit 254); the bucket was left broken"

# The restore put answers success but changes nothing.
run_helper restore-still-broken FAKE_AWS_IGNORE_PUT=2
check restore-still-broken nonzero \
  "FAIL versioning-suspended: verify-protection exited 1 after restoring versioning-enabled, not 0; the bucket was left broken"

run_helper all-rejected FAKE_AWS_REJECT_BREAKS=1
check all-rejected nonzero \
  "SKIPPED versioning-suspended: the store rejected versioning-suspended" \
  "SKIPPED foreign-rule: the store rejected LIFECYCLE_FOREIGN" \
  "FAIL: every breaking case was SKIPPED (5); this run proved nothing"

run_helper all-unknown FAKE_CLI_MODE=unknown
check all-unknown nonzero \
  "SKIPPED versioning-suspended: the store's broken state reads unknown, not fail" \
  "FAIL: every breaking case was SKIPPED (5); this run proved nothing"

# The first put (suspending versioning) rejected, the rest behave.
run_helper one-rejected FAKE_AWS_FAIL_PUT=1
check one-rejected 0 \
  "SKIPPED versioning-suspended: the store rejected versioning-suspended (exit 254" \
  "PASS foreign-rule:" \
  "verify-protection cases: 4 passed, 1 skipped"

run_helper transient-retried FAKE_AWS_TRANSIENT_PUTS=2
check transient-retried 0 \
  "failed transiently on attempt 2" \
  "verify-protection cases: 5 passed, 0 skipped"

run_helper transient-exhausted FAKE_AWS_TRANSIENT_PUTS=99
check transient-exhausted 1 \
  "FAIL versioning-suspended: putting versioning-suspended still failed transiently after 5 attempts"

# Usage: no defaults, the first missing input is named.
usage_case() {
  local name=$1 want=$2
  shift 2
  rc=0
  env -i PATH="${STUB_BIN}:${PATH}" "$@" bash "${SCRIPT}" >"${TMP}/${name}.out" 2>&1 || rc=$?
  check "${name}" 64 "missing: ${want}"
}
usage_case usage-endpoint endpoint
usage_case usage-bucket bucket RAVEL_S3_ENDPOINT=http://localhost:9000
usage_case usage-access-key AWS_ACCESS_KEY_ID RAVEL_S3_ENDPOINT=e RAVEL_S3_BUCKET=b
usage_case usage-secret-key AWS_SECRET_ACCESS_KEY RAVEL_S3_ENDPOINT=e RAVEL_S3_BUCKET=b \
  AWS_ACCESS_KEY_ID=ak
usage_case usage-region AWS_DEFAULT_REGION RAVEL_S3_ENDPOINT=e RAVEL_S3_BUCKET=b \
  AWS_ACCESS_KEY_ID=ak AWS_SECRET_ACCESS_KEY=sk
rc=0
bash "${SCRIPT}" a b c >"${TMP}/usage-extra.out" 2>&1 || rc=$?
check usage-extra 64 "usage:"

echo
echo "${passes} passed, ${fails} failed"
[ "${fails}" -eq 0 ]
