#!/usr/bin/env bash
# Usage: scripts/chaos/agents-replace-worker.test.sh [--help]
#
# Cases for the assertions in scripts/chaos/agents-replace-worker.sh, run on
# canned figures through its --dry-run mode, with no store and no server. The
# passing figures are the ones a real run against RustFS printed, except the
# three timing figures (GRANT_TO_KILL_MS, RECOVERY_FROM_GRANT_MS,
# RESTART_READY_MS), which were added after that run and are canned values
# inside their bands. Each failing case changes exactly the figure the failure
# it stands for would change.
#
# Exit 0 when every case passes, 1 when one fails, 64 on bad usage.
set -uo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  printf 'Usage: scripts/chaos/agents-replace-worker.test.sh\n\nRuns the canned-figure cases for scripts/chaos/agents-replace-worker.sh.\n'
  exit 0
fi
if [[ $# -gt 0 ]]; then
  printf 'agents-replace-worker.test.sh: unexpected argument: %s\n' "$1" >&2
  exit 64
fi

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/agents-replace-worker.sh"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "${SCRATCH}"' EXIT
unset CHAOS_LOG_DIR CHAOS_KEEP_LOGS

PASSED=0
FAILED=0

PASS_FIGURES="${SCRATCH}/pass.figures"
cat >"${PASS_FIGURES}" <<'EOF'
OP1_OPERATION_ID=djE6b3A6NmFkZjYwMzI0ZTg5MDY1ODcwMzA3YTI3YzkzNGExYzQ6MTox
OP1_SEND_DUPLICATE=false
A_KILLED_BEFORE_COMMIT=true
A_WORKER_ID=w-1cc849d5bd4e9dcd
A_ASSIGNED_OPERATION_ID=djE6b3A6NmFkZjYwMzI0ZTg5MDY1ODcwMzA3YTI3YzkzNGExYzQ6MTox
A_TURN_ID=d746246fc5bd6d52e210c7e1883ca81e
A_EFFECT_ID=djE6ZWZmZWN0OjZhZGY2MDMyNGU4OTA2NTg3MDMwN2EyN2M5MzRhMWM0OjE6NjdiMTlmMmYyZWE2MDcwYTk5ZDM4ZTk5ZjRjOWExOTY
A_STEP_ATTEMPT=1
OP1_RETRIED_OPERATION_ID=djE6b3A6NmFkZjYwMzI0ZTg5MDY1ODcwMzA3YTI3YzkzNGExYzQ6MTox
OP1_RETRIED_DUPLICATE=true
GRANT_TO_KILL_MS=412
RECOVERY_FROM_GRANT_MS=40622
B_WORKER_ID=w-77de2a170426fb10
B_ASSIGNED_OPERATION_ID=djE6b3A6NmFkZjYwMzI0ZTg5MDY1ODcwMzA3YTI3YzkzNGExYzQ6MTox
B_TURN_ID=d746246fc5bd6d52e210c7e1883ca81e
B_EFFECT_ID=djE6ZWZmZWN0OjZhZGY2MDMyNGU4OTA2NTg3MDMwN2EyN2M5MzRhMWM0OjE6NjdiMTlmMmYyZWE2MDcwYTk5ZDM4ZTk5ZjRjOWExOTY
B_STEP_ATTEMPT=2
B_EFFECT_STATUS=completed
B_COMMIT_STATUS=completed
OP1_RESULT_STATUS=completed
OP1_RESULT_WORKER=w-77de2a170426fb10
OP1_STATE_VERSION=1
OP2_OPERATION_ID=djE6b3A6NmFkZjYwMzI0ZTg5MDY1ODcwMzA3YTI3YzkzNGExYzQ6MToy
OP2_SEND_DUPLICATE=false
STATE_VERSION_BEFORE_RESTART=1
RESTART_READY_MS=1840
OP2_STATUS_AFTER_RESTART=running
C_WORKER_ID=w-3a3ec2afbbcfe24c
C_ASSIGNED_OPERATION_ID=djE6b3A6NmFkZjYwMzI0ZTg5MDY1ODcwMzA3YTI3YzkzNGExYzQ6MToy
C_TURN_ID=9966c37ae23216e185f50e0145da274a
C_EFFECT_ID=djE6ZWZmZWN0OjZhZGY2MDMyNGU4OTA2NTg3MDMwN2EyN2M5MzRhMWM0OjE6ZDYxOGEyMTc1ZTljOWM2YTFhNmJmMTE3Nzc5ZDQwOTE
C_STEP_ATTEMPT=1
C_EFFECT_STATUS=completed
C_COMMIT_STATUS=completed
OP2_RESULT_STATUS=completed
OP2_RESULT_WORKER=w-3a3ec2afbbcfe24c
OP2_RETRIED_OPERATION_ID=djE6b3A6NmFkZjYwMzI0ZTg5MDY1ODcwMzA3YTI3YzkzNGExYzQ6MToy
OP2_RETRIED_DUPLICATE=true
STATE_VERSION_AFTER_RESTART=2
TURNS_COMMITTED_AFTER_RESTART=1
TAKEOVERS_AFTER_RESTART=0
EOF

# A copy of the passing figures with each KEY=VALUE argument replacing that
# key's line, and each bare KEY argument deleting it.
variant() {
  local out="${SCRATCH}/$1.figures" arg key
  shift
  cp "${PASS_FIGURES}" "${out}"
  for arg in "$@"; do
    key="${arg%%=*}"
    if [[ "${arg}" == *=* ]]; then
      awk -v k="${key}=" -v line="${arg}" 'index($0, k) == 1 { print line; next } { print }' \
        "${out}" >"${out}.tmp"
    else
      awk -v k="${key}=" 'index($0, k) != 1' "${out}" >"${out}.tmp"
    fi
    mv "${out}.tmp" "${out}"
  done
  printf '%s\n' "${out}"
}

# case NAME WANT_EXIT WANT_TEXT -- ARGS...: run the script with ARGS and check
# its exit code and that its output names WANT_TEXT.
case_run() {
  local name=$1 want_exit=$2 want_text=$3 code=0 out
  shift 4
  out="$(bash "${SCRIPT}" "$@" 2>&1)" || code=$?
  if [[ "${code}" -eq "${want_exit}" && "${out}" == *"${want_text}"* ]]; then
    PASSED=$((PASSED + 1))
    printf 'ok   %s\n' "${name}"
  else
    FAILED=$((FAILED + 1))
    printf 'FAIL %s: exit %s (want %s), output:\n%s\n' "${name}" "${code}" "${want_exit}" "${out}"
  fi
}

case_run "real-run figures pass" 0 "every assertion held" -- \
  --dry-run "${PASS_FIGURES}"

case_run "a retried send that mints a new operation fails" 1 \
  "OP1_RETRIED_OPERATION_ID=djE6b3A6bmV3" -- --dry-run "$(variant new-op \
  OP1_RETRIED_OPERATION_ID=djE6b3A6bmV3)"

case_run "a retried send not reported as a duplicate fails" 1 \
  "OP1_RETRIED_DUPLICATE=false, expected true" -- --dry-run "$(variant not-dup \
  OP1_RETRIED_DUPLICATE=false)"

case_run "a restart that duplicates the result fails on the state version" 1 \
  "STATE_VERSION_AFTER_RESTART=3, expected 2" -- --dry-run "$(variant dup-result \
  STATE_VERSION_AFTER_RESTART=3)"

case_run "a second commit after the restart fails on the turn count" 1 \
  "TURNS_COMMITTED_AFTER_RESTART=2, expected 1" -- --dry-run "$(variant dup-turn \
  TURNS_COMMITTED_AFTER_RESTART=2)"

case_run "a takeover before the lease and grace lapse from the grant fails" 1 \
  "RECOVERY_FROM_GRANT_MS=38999, expected within [39000, 60000]" -- --dry-run \
  "$(variant early RECOVERY_FROM_GRANT_MS=38999)"

case_run "a takeover after the band fails" 1 \
  "RECOVERY_FROM_GRANT_MS=60001, expected within [39000, 60000]" -- --dry-run \
  "$(variant late RECOVERY_FROM_GRANT_MS=60001)"

case_run "a missing takeover figure fails" 1 "RECOVERY_FROM_GRANT_MS printed 0 times" -- \
  --dry-run "$(variant no-recovery RECOVERY_FROM_GRANT_MS)"

case_run "a kill later than the grant-to-kill bound fails" 1 \
  "GRANT_TO_KILL_MS=10001, expected within [0, 10000]" -- --dry-run \
  "$(variant late-kill GRANT_TO_KILL_MS=10001)"

case_run "a kill timed before the grant fails" 1 \
  "GRANT_TO_KILL_MS=-5 is not a number" -- --dry-run \
  "$(variant kill-before-grant GRANT_TO_KILL_MS=-5)"

case_run "a missing grant-to-kill figure fails" 1 "GRANT_TO_KILL_MS printed 0 times" -- \
  --dry-run "$(variant no-grant-to-kill GRANT_TO_KILL_MS)"

case_run "a restart slower than the lease fails" 1 \
  "RESTART_READY_MS=30001, expected within [0, 30000]" -- --dry-run \
  "$(variant slow-restart RESTART_READY_MS=30001)"

case_run "a restart ready before the kill fails" 1 \
  "RESTART_READY_MS=-1 is not a number" -- --dry-run \
  "$(variant restart-before-kill RESTART_READY_MS=-1)"

case_run "a missing restart figure fails" 1 "RESTART_READY_MS printed 0 times" -- \
  --dry-run "$(variant no-restart-ready RESTART_READY_MS)"

case_run "a replacement that runs the step as attempt 1 fails" 1 \
  "B_STEP_ATTEMPT=1, expected 2" -- --dry-run "$(variant attempt B_STEP_ATTEMPT=1)"

case_run "a replacement under a new effect id fails" 1 \
  "B_EFFECT_ID=other, expected the same as A_EFFECT_ID" -- --dry-run "$(variant effect \
  B_EFFECT_ID=other)"

case_run "a result written by the killed worker fails" 1 \
  "OP1_RESULT_WORKER=w-1cc849d5bd4e9dcd, expected the same as B_WORKER_ID" -- \
  --dry-run "$(variant result-a OP1_RESULT_WORKER=w-1cc849d5bd4e9dcd)"

case_run "an operation not running after the restart fails" 1 \
  "OP2_STATUS_AFTER_RESTART=completed, expected running" -- --dry-run "$(variant restart \
  OP2_STATUS_AFTER_RESTART=completed)"

case_run "a missing figure fails" 1 "B_STEP_ATTEMPT printed 0 times" -- \
  --dry-run "$(variant missing B_STEP_ATTEMPT)"

DOUBLED="${SCRATCH}/doubled.figures"
cp "${PASS_FIGURES}" "${DOUBLED}"
printf 'OP1_STATE_VERSION=1\n' >>"${DOUBLED}"
case_run "a figure printed twice fails" 1 "OP1_STATE_VERSION printed 2 times" -- \
  --dry-run "${DOUBLED}"

case_run "--check passes with no store" 0 "check: PASS" -- --check
case_run "--dry-run without a file is a usage error" 64 "Usage:" -- --dry-run
case_run "an unknown argument is a usage error" 64 "unknown argument" -- --bogus

printf '\n%d passed, %d failed\n' "${PASSED}" "${FAILED}"
[[ "${FAILED}" -eq 0 ]]
