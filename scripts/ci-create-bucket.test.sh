#!/usr/bin/env bash
# Cases for scripts/ci-create-bucket.sh (issue #2036). The script runs the aws
# CLI, so each case puts a stub `aws` first on PATH that answers one scripted
# response per call (exit status and stderr) and counts its calls. A green CI
# run only ever takes the clean-create path, so the retry classification is
# covered here or nowhere.
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
# Response file: one line per call, "<exit status><TAB><stderr text>". A call
# past the last line repeats the last line.
cat >"${STUB_BIN}/aws" <<'STUB'
#!/usr/bin/env bash
n=$(( $(cat "${FAKE_AWS_CALLS}") + 1 ))
echo "${n}" >"${FAKE_AWS_CALLS}"
line=$(sed -n "${n}p" "${FAKE_AWS_RESPONSES}")
[ -z "${line}" ] && line=$(tail -n 1 "${FAKE_AWS_RESPONSES}")
rc=${line%%$'\t'*}
msg=${line#*$'\t'}
[ -n "${msg}" ] && echo "${msg}" >&2
exit "${rc}"
STUB
chmod +x "${STUB_BIN}/aws"

CONN=$'255\tCould not connect to the endpoint URL: "http://127.0.0.1:9000/b"'
OWNED=$'254\tAn error occurred (BucketAlreadyOwnedByYou) when calling the CreateBucket operation: owned'
BADKEY=$'254\tAn error occurred (InvalidAccessKeyId) when calling the CreateBucket operation: bad key'
SLOW=$'254\tAn error occurred (SlowDown) when calling the CreateBucket operation: slow down'
UNAVAIL=$'254\tAn error occurred (ServiceUnavailable) when calling the CreateBucket operation: busy'
OK=$'0\t'

# case <name> <want exit> <want calls> <response line>...
case_() {
  local name=$1 want_rc=$2 want_calls=$3
  shift 3
  local dir="${TMP}/${name}"
  mkdir -p "${dir}"
  printf '%s\n' "$@" >"${dir}/responses"
  echo 0 >"${dir}/calls"
  local rc=0
  PATH="${STUB_BIN}:${PATH}" \
    FAKE_AWS_RESPONSES="${dir}/responses" FAKE_AWS_CALLS="${dir}/calls" \
    CI_CREATE_BUCKET_BACKOFF_SECONDS=0 \
    bash "${SCRIPT}" http://127.0.0.1:9000 b >"${dir}/out" 2>&1 || rc=$?
  local calls
  calls=$(cat "${dir}/calls")
  if [ "${rc}" = "${want_rc}" ] && [ "${calls}" = "${want_calls}" ]; then
    passes=$((passes + 1))
  else
    fails=$((fails + 1))
    echo "FAIL ${name}: want exit ${want_rc} after ${want_calls} call(s), got exit ${rc} after ${calls}"
    sed 's/^/    /' "${dir}/out"
  fi
}

case_ clean-create 0 1 "${OK}"
case_ bad-credentials-fail-at-once 254 1 "${BADKEY}"
case_ already-owned-on-first-call-succeeds 0 1 "${OWNED}"
case_ connection-error-then-created 0 2 "${CONN}" "${OK}"
case_ connection-error-then-already-owned 0 2 "${CONN}" "${OWNED}"
case_ slowdown-is-retried 0 2 "${SLOW}" "${OK}"
case_ service-unavailable-is-retried 0 3 "${UNAVAIL}" "${UNAVAIL}" "${OK}"
case_ persistent-connection-error-gives-up 1 5 "${CONN}"
case_ transient-then-permanent-fails-with-its-code 254 2 "${CONN}" "${BADKEY}"

# Usage errors never call aws.
rc=0
PATH="${STUB_BIN}:${PATH}" bash "${SCRIPT}" only-one-arg >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 64 ]; then passes=$((passes + 1)); else
  fails=$((fails + 1)); echo "FAIL usage: want exit 64, got ${rc}"
fi

echo "${passes} passed, ${fails} failed"
[ "${fails}" -eq 0 ]
