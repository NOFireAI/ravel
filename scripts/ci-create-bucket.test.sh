#!/usr/bin/env bash
# Cases for scripts/ci-create-bucket.sh (issues #2036 and #2257). The script
# runs the aws CLI, so each case puts a stub `aws` first on PATH that logs every
# call. create-bucket answers one scripted response per call (exit status and
# stderr); every other call answers what a compliant bucket would, unless the
# case overrides it. A green CI run only ever takes the clean-create path with
# every read-back passing, so the retry classification and each read-back
# failure are covered here or nowhere.
#
# Run: bash scripts/ci-create-bucket.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/ci-create-bucket.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/ci-create-bucket-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

STUB_BIN="${TMP}/stub-bin"
mkdir -p "${STUB_BIN}"
# create-bucket response file: one line per call, "<exit status><TAB><stderr
# text>". A call past the last line repeats the last line.
#
# Every other call is keyed by its --query value, or by its subcommand when it
# has none, and answers the compliant value for that key. Override file: one
# line per override, "<key substring><TAB><exit status><TAB><stdout>".
cat >"${STUB_BIN}/aws" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"${FAKE_AWS_LOG}"
sub=
query=
prev=
for arg in "$@"; do
  [ "${prev}" = s3api ] && sub=${arg}
  [ "${prev}" = --query ] && query=${arg}
  prev=${arg}
done
if [ "${sub}" = create-bucket ]; then
  n=$(( $(cat "${FAKE_AWS_CALLS}") + 1 ))
  echo "${n}" >"${FAKE_AWS_CALLS}"
  line=$(sed -n "${n}p" "${FAKE_AWS_RESPONSES}")
  [ -z "${line}" ] && line=$(tail -n 1 "${FAKE_AWS_RESPONSES}")
  rc=${line%%$'\t'*}
  msg=${line#*$'\t'}
  [ -n "${msg}" ] && echo "${msg}" >&2
  exit "${rc}"
fi
key=${query:-${sub}}
case "${key}" in
  ObjectLockConfiguration.ObjectLockEnabled | Status) out=Enabled ;;
  *.DaysAfterInitiation) out=7 ;;
  *.NoncurrentDays) out=1 ;;
  *ExpiredObjectDeleteMarker*) out=true ;;
  *) out= ;;
esac
rc=0
while IFS=$'\t' read -r match orc oout; do
  if [ -n "${match}" ] && [[ "${key}" == *"${match}"* ]]; then
    rc=${orc}
    out=${oout}
  fi
done <"${FAKE_AWS_OVERRIDES}"
[ -n "${out}" ] && echo "${out}"
[ "${rc}" -ne 0 ] && echo "An error occurred (StubbedFailure) when calling ${sub}" >&2
exit "${rc}"
STUB
chmod +x "${STUB_BIN}/aws"

CONN=$'255\tCould not connect to the endpoint URL: "http://127.0.0.1:9000/b"'
OWNED=$'254\tAn error occurred (BucketAlreadyOwnedByYou) when calling the CreateBucket operation: owned'
BADKEY=$'254\tAn error occurred (InvalidAccessKeyId) when calling the CreateBucket operation: bad key'
SLOW=$'254\tAn error occurred (SlowDown) when calling the CreateBucket operation: slow down'
UNAVAIL=$'254\tAn error occurred (ServiceUnavailable) when calling the CreateBucket operation: busy'
OK=$'0\t'

# A full run after the create: two puts and five read-backs.
AFTER_CREATE=7

# Overrides for the next case_ call, one "<key>\t<exit>\t<stdout>" per line.
override=

# case_ <name> <want exit> <want create calls> <want total calls> <response line>...
case_() {
  local name=$1 want_rc=$2 want_creates=$3 want_total=$4
  shift 4
  local dir="${TMP}/${name}"
  mkdir -p "${dir}"
  printf '%s\n' "$@" >"${dir}/responses"
  printf '%s\n' "${override}" >"${dir}/overrides"
  override=
  echo 0 >"${dir}/calls"
  : >"${dir}/log"
  local rc=0
  PATH="${STUB_BIN}:${PATH}" \
    FAKE_AWS_RESPONSES="${dir}/responses" FAKE_AWS_CALLS="${dir}/calls" \
    FAKE_AWS_OVERRIDES="${dir}/overrides" FAKE_AWS_LOG="${dir}/log" \
    CI_CREATE_BUCKET_BACKOFF_SECONDS=0 \
    bash "${SCRIPT}" http://127.0.0.1:9000 b >"${dir}/out" 2>&1 || rc=$?
  local creates total unlocked
  creates=$(cat "${dir}/calls")
  total=$(wc -l <"${dir}/log" | tr -d ' ')
  unlocked=$(grep ' create-bucket ' "${dir}/log" | grep -vc -- '--object-lock-enabled-for-bucket')
  if [ "${rc}" = "${want_rc}" ] && [ "${creates}" = "${want_creates}" ] &&
    [ "${total}" = "${want_total}" ] && [ "${unlocked}" = 0 ]; then
    passes=$((passes + 1))
  else
    fails=$((fails + 1))
    echo "FAIL ${name}: want exit ${want_rc} after ${want_creates} create(s) and ${want_total} call(s)," \
      "got exit ${rc} after ${creates} and ${total}, ${unlocked} create(s) without Object Lock"
    sed 's/^/    /' "${dir}/out"
  fi
}

# log_has <case name> <fixed string>: the case's aws call log has a line
# containing the string.
log_has() {
  if grep -qF -- "$2" "${TMP}/$1/log"; then passes=$((passes + 1)); else
    fails=$((fails + 1))
    echo "FAIL $1: no aws call containing: $2"
    sed 's/^/    /' "${TMP}/$1/log"
  fi
}

case_ clean-create 0 1 $((1 + AFTER_CREATE)) "${OK}"
case_ bad-credentials-fail-at-once 254 1 1 "${BADKEY}"
case_ already-owned-on-first-call-succeeds 0 1 $((1 + AFTER_CREATE)) "${OWNED}"
case_ connection-error-then-created 0 2 $((2 + AFTER_CREATE)) "${CONN}" "${OK}"
case_ connection-error-then-already-owned 0 2 $((2 + AFTER_CREATE)) "${CONN}" "${OWNED}"
case_ slowdown-is-retried 0 2 $((2 + AFTER_CREATE)) "${SLOW}" "${OK}"
case_ service-unavailable-is-retried 0 3 $((3 + AFTER_CREATE)) "${UNAVAIL}" "${UNAVAIL}" "${OK}"
case_ persistent-connection-error-gives-up 1 5 5 "${CONN}"
case_ transient-then-permanent-fails-with-its-code 254 2 2 "${CONN}" "${BADKEY}"

# The configuration the clean create sends.
log_has clean-create 'put-bucket-versioning --bucket b --versioning-configuration Status=Enabled'
log_has clean-create 'put-bucket-lifecycle-configuration --bucket b --lifecycle-configuration {"Rules":[{"ID":"ravel","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true},"NoncurrentVersionExpiration":{"NoncurrentDays":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}'

# A put that fails stops the run with its exit status.
override=$'put-bucket-versioning\t254\t'
case_ versioning-put-fails 254 1 2 "${OK}"
override=$'put-bucket-lifecycle-configuration\t254\t'
case_ lifecycle-put-fails 254 1 3 "${OK}"

# Each read-back that does not confirm its setting fails the run, and the run
# stops at the first one.
override=$'ObjectLockEnabled\t254\t'
case_ existing-bucket-without-object-lock-fails 1 1 4 "${OWNED}"
override=$'ObjectLockEnabled\t0\tNone'
case_ object-lock-not-enabled-fails 1 1 4 "${OK}"
override=$'Status\t0\tSuspended'
case_ versioning-suspended-fails 1 1 5 "${OK}"
override=$'Status\t254\t'
case_ versioning-read-error-fails 1 1 5 "${OK}"
override=$'DaysAfterInitiation\t0\tNone'
case_ missing-multipart-abort-fails 1 1 6 "${OK}"
override=$'NoncurrentDays\t0\t30'
case_ wrong-noncurrent-days-fails 1 1 7 "${OK}"
override=$'ExpiredObjectDeleteMarker\t0\tnull'
case_ missing-expired-delete-marker-fails 1 1 8 "${OK}"
override=$'ExpiredObjectDeleteMarker\t254\t'
case_ lifecycle-read-error-fails 1 1 8 "${OK}"

# Usage errors never call aws.
rc=0
PATH="${STUB_BIN}:${PATH}" bash "${SCRIPT}" only-one-arg >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 64 ]; then passes=$((passes + 1)); else
  fails=$((fails + 1)); echo "FAIL usage: want exit 64, got ${rc}"
fi

echo "${passes} passed, ${fails} failed"
[ "${fails}" -eq 0 ]
