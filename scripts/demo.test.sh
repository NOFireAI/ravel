#!/usr/bin/env bash
# Cases for createbucket_exit_code in scripts/demo.sh (issue #2308).
#
# The function is extracted from demo.sh and run against a stub `docker` on
# PATH that prints a listing each case chooses, one `Name State ExitCode` line
# per container, as demo.sh's `docker compose ps --format` string produces. A
# leftover `docker compose run` container (<project>-createbucket-run-<id>)
# must never be read, and a filter that stopped matching the service container
# would surface as a timeout instead of the real exit code, so every case
# asserts both the printed exit code and the return status.
#
# DEMO_CREATEBUCKET_POLLS bounds the poll count, and `sleep` is replaced by a
# shell function that records each call instead of waiting.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEMO="${ROOT_DIR}/scripts/demo.sh"

PASSED=0
FAILED=0

check() {
  local description="$1" expected="$2" actual="$3"
  if [[ "${expected}" == "${actual}" ]]; then
    PASSED=$((PASSED + 1))
    printf 'ok   %s\n' "${description}"
  else
    FAILED=$((FAILED + 1))
    printf 'FAIL %s\n     expected: %q\n     actual:   %q\n' \
      "${description}" "${expected}" "${actual}"
  fi
}

TMP="$(mktemp -d "${TMPDIR:-/tmp}/demo-test.XXXXXX")" || {
  echo "demo.test.sh: could not create a scratch directory" >&2
  exit 2
}
trap 'rm -rf "${TMP}"' EXIT

FUNCTION_SRC="$(sed -n '/^createbucket_exit_code() {$/,/^}$/p' "${DEMO}")"
if [[ -z "${FUNCTION_SRC}" ]]; then
  echo "demo.test.sh: createbucket_exit_code() not found in ${DEMO}" >&2
  exit 2
fi
eval "${FUNCTION_SRC}"
if ! declare -F createbucket_exit_code >/dev/null; then
  echo "demo.test.sh: extracting createbucket_exit_code from ${DEMO} defined no function" >&2
  exit 2
fi

log() {
  echo "[demo] $*" >&2
}

sleep() {
  echo "$*" >>"${STUB_DIR}/sleeps"
}

# The stub answers call N with listing.N when that file exists, else with
# listing; a `fail` file makes every call exit 1 with nothing on stdout. Each
# call's arguments are appended to `calls`.
BIN="${TMP}/bin"
mkdir -p "${BIN}"
printf '%s\n' \
  '#!/usr/bin/env bash' \
  'echo "$*" >>"${STUB_DIR}/calls"' \
  'n=$(wc -l <"${STUB_DIR}/calls")' \
  'n=$((n))' \
  'if [[ -e "${STUB_DIR}/fail" ]]; then echo "stub docker: failing" >&2; exit 1; fi' \
  'if [[ -e "${STUB_DIR}/listing.${n}" ]]; then cat "${STUB_DIR}/listing.${n}"; exit 0; fi' \
  'if [[ -e "${STUB_DIR}/listing" ]]; then cat "${STUB_DIR}/listing"; fi' \
  'exit 0' >"${BIN}/docker"
chmod +x "${BIN}/docker"
PATH="${BIN}:${PATH}"
RUSTFS_COMPOSE="deploy/docker-compose/rustfs.yml"
export DEMO_CREATEBUCKET_POLLS=4

# new_case NAME: a fresh stub directory for one case.
new_case() {
  STUB_DIR="${TMP}/$1"
  mkdir -p "${STUB_DIR}"
  : >"${STUB_DIR}/calls"
  : >"${STUB_DIR}/sleeps"
  export STUB_DIR
}

# run_case: runs the function once, leaving stdout in out, stderr in err and
# the return status in rc.
run_case() {
  local code=0
  createbucket_exit_code >"${STUB_DIR}/out" 2>"${STUB_DIR}/err" || code=$?
  printf '%s\n' "${code}" >"${STUB_DIR}/rc"
}

out() { cat "${STUB_DIR}/out"; }
rc() { cat "${STUB_DIR}/rc"; }
polls() { wc -l <"${STUB_DIR}/calls" | tr -d ' '; }
err_has() {
  if grep -qF -- "$1" "${STUB_DIR}/err"; then echo yes; else echo no; fi
}

TIMEOUT_PREFIX="[demo] timed out waiting for the createbucket service to exit (last state: "

# --- the service container is read, the run container is not --------------

new_case service-0-run-1
printf '%s\n' \
  'docker-compose-createbucket-run-4f2a9c1b7e3d exited 1' \
  'docker-compose-createbucket-1 exited 0' >"${STUB_DIR}/listing"
run_case
check "service exited 0 beside a run container exited 1: prints 0" "0" "$(out)"
check "service exited 0 beside a run container exited 1: returns 0" "0" "$(rc)"
check "service exited 0 beside a run container exited 1: one poll" "1" "$(polls)"

new_case service-3-run-0
printf '%s\n' \
  'docker-compose-createbucket-1 exited 3' \
  'docker-compose-createbucket-run-4f2a9c1b7e3d exited 0' >"${STUB_DIR}/listing"
run_case
check "service exited 3 beside a run container exited 0: prints 3" "3" "$(out)"
check "service exited 3 beside a run container exited 0: returns 0" "0" "$(rc)"

# The docker compose invocation itself: the compose file and the format
# string the listing lines above are shaped after.
check "the poll lists the createbucket service with the Name State ExitCode format" \
  "compose -f deploy/docker-compose/rustfs.yml ps --all --format {{.Name}} {{.State}} {{.ExitCode}} createbucket" \
  "$(head -n 1 "${STUB_DIR}/calls")"

# --- only run containers: never read, bounded timeout ----------------------

new_case only-run-hex
printf '%s\n' 'docker-compose-createbucket-run-4f2a9c1b7e3d exited 0' >"${STUB_DIR}/listing"
run_case
check "only a run container: prints nothing" "" "$(out)"
check "only a run container: returns 1" "1" "$(rc)"
check "only a run container: polls the bounded count" "4" "$(polls)"
check "only a run container: sleeps between polls" "4" "$(wc -l <"${STUB_DIR}/sleeps" | tr -d ' ')"
check "only a run container: the timeout says the service was not listed" \
  "yes" "$(err_has "${TIMEOUT_PREFIX}not listed)")"

new_case only-run-digits
printf '%s\n' 'docker-compose-createbucket-run-123 exited 0' >"${STUB_DIR}/listing"
run_case
check "only an all-digit run container: prints nothing" "" "$(out)"
check "only an all-digit run container: returns 1" "1" "$(rc)"
check "only an all-digit run container: polls the bounded count" "4" "$(polls)"
check "only an all-digit run container: the timeout says the service was not listed" \
  "yes" "$(err_has "${TIMEOUT_PREFIX}not listed)")"

new_case service-running-forever
printf '%s\n' \
  'docker-compose-createbucket-run-123 exited 0' \
  'docker-compose-createbucket-1 running 0' >"${STUB_DIR}/listing"
run_case
check "service never exits beside an exited run container: prints nothing" "" "$(out)"
check "service never exits beside an exited run container: returns 1" "1" "$(rc)"
check "service never exits: polls the bounded count" "4" "$(polls)"
check "service never exits: the timeout names the last state" \
  "yes" "$(err_has "${TIMEOUT_PREFIX}running 0)")"

# --- running on the first poll, exited on the second -----------------------

new_case running-then-exited
printf '%s\n' 'docker-compose-createbucket-1 running 0' >"${STUB_DIR}/listing.1"
printf '%s\n' 'docker-compose-createbucket-1 exited 0' >"${STUB_DIR}/listing.2"
run_case
check "running then exited 0: prints 0" "0" "$(out)"
check "running then exited 0: returns 0" "0" "$(rc)"
check "running then exited 0: two polls" "2" "$(polls)"
check "running then exited 0: one sleep between them" "1" "$(wc -l <"${STUB_DIR}/sleeps" | tr -d ' ')"

# --- nothing listed, or docker failing: bounded timeout --------------------

new_case empty-listing
: >"${STUB_DIR}/listing"
run_case
check "empty listing: prints nothing" "" "$(out)"
check "empty listing: returns 1" "1" "$(rc)"
check "empty listing: polls the bounded count" "4" "$(polls)"
check "empty listing: the timeout says the service was not listed" \
  "yes" "$(err_has "${TIMEOUT_PREFIX}not listed)")"

new_case docker-fails
: >"${STUB_DIR}/fail"
run_case
check "docker failing: prints nothing" "" "$(out)"
check "docker failing: returns 1" "1" "$(rc)"
check "docker failing: polls the bounded count" "4" "$(polls)"
check "docker failing: the timeout says the service was not listed" \
  "yes" "$(err_has "${TIMEOUT_PREFIX}not listed)")"
check "docker failing: its stderr does not reach the log" "no" "$(err_has "stub docker: failing")"

# --- no override: the 180-poll default --------------------------------------

new_case default-poll-count
: >"${STUB_DIR}/listing"
(
  unset DEMO_CREATEBUCKET_POLLS
  run_case
)
check "no override: polls 180 times" "180" "$(polls)"
check "no override: sleeps one second between polls" "1" "$(sort -u "${STUB_DIR}/sleeps")"
check "no override: returns 1" "1" "$(rc)"

# --- result ----------------------------------------------------------------

printf '\ndemo.test.sh: %s passed, %s failed\n' "${PASSED}" "${FAILED}"
if [[ "${FAILED}" -ne 0 ]]; then
  exit 1
fi
if [[ "${PASSED}" -lt 35 ]]; then
  printf 'demo.test.sh: only %s cases ran; a suite that shrank silently is not a pass\n' \
    "${PASSED}" >&2
  exit 1
fi
exit 0
