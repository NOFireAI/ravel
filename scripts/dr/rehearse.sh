#!/usr/bin/env bash
# Usage: scripts/dr/rehearse.sh [--fault <name>] [--exports N] [--dry-run] [--help]
#
# The single entry point for the disaster-recovery rehearsal (issue #814). It
# runs the whole sequence, times each phase, and prints the wall clock of every
# phase it ran:
#
#   seed                        write a known corpus into bucket A
#   replicate                   mirror A into an empty bucket B
#   inject                      (only with --fault) damage B in one named way
#   start-refusal-probe         prove start.sh REFUSES while B has no marker
#   restore-check               the four ordered checks against B
#   start                       (clean runs) start ravel-server against B
#   start-refusal-after-failure (fault runs) prove start.sh still refuses
#
# The probe before restore-check is what makes the ordering mechanical rather
# than procedural: a start attempted before the checks have passed is refused,
# so a rehearsal cannot accidentally serve from an unverified bucket and then
# claim the checks ran first.
#
# With --fault the rehearsal is expected to go RED, and this script asserts it
# went red in the right place AND for the right reason: restore-check must fail
# at the phase that fault targets, with that phase's exit code, every earlier
# phase must have passed, no later phase may have run, start.sh must still
# refuse afterwards, and the failing phase's own output must name the artefact
# inject.sh injected (or the exact figure line that artefact puts out of band).
# A phase name and an exit code alone would pass on any unrelated failure that
# happened to land in the same phase, which is how a rehearsal starts proving
# nothing while staying green.
#
#   --fault <name>  inject one of inject.sh's faults before the checks
#   --exports N     corpus size passed to seed.sh
#   --i-know-this-bucket  passed through to seed.sh and replicate.sh, letting
#                   their --reset empty a bucket that carries no rehearsal
#                   marker
#   --dry-run       show the sequence and the configuration, run nothing
#
# Exit 0 on a clean rehearsal that ended with the server started, or on a
# fault rehearsal caught at the expected phase. 64 on bad usage, 65 on an
# unmet precondition, 1 when the sequence did not behave as the fault matrix
# requires.
set -euo pipefail

# shellcheck source=scripts/dr/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

DR_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The check each fault must be caught by, and that check's exit code. This is
# the fault matrix: a fault caught by the wrong check is a failed rehearsal
# even though something did go red.
DR_RESTORE_PHASES=(custody-manifest commit-reconstruction catalog-fold-verification canary-query)

fault_target_phase() {
  case "$1" in
    dangling-commit-record) printf 'custody-manifest\n' ;;
    missing-data-object) printf 'commit-reconstruction\n' ;;
    canary-error) printf 'canary-query\n' ;;
    *) return 1 ;;
  esac
}

phase_exit_code() {
  case "$1" in
    custody-manifest) printf '11\n' ;;
    commit-reconstruction) printf '12\n' ;;
    catalog-fold-verification) printf '13\n' ;;
    canary-query) printf '14\n' ;;
    *) return 1 ;;
  esac
}

usage() {
  cat <<'USAGE'
Usage: scripts/dr/rehearse.sh [--fault <name>] [--exports N] [--dry-run] [--help]

Runs the whole disaster-recovery rehearsal and prints the wall clock of each
phase as `phase <name> seconds=<n>`.

Clean run:  seed, replicate, start-refusal-probe, restore-check, start.
Fault run:  seed, replicate, inject, start-refusal-probe, restore-check,
            start-refusal-after-failure. The run passes only when
            restore-check failed at the check that fault targets, and that
            check's output named the injected artefact:

  dangling-commit-record  ->  custody-manifest           (exit 11)
  missing-data-object     ->  commit-reconstruction      (exit 12)
  canary-error            ->  canary-query               (exit 14)

Options:
  --fault <name>  inject one fault before the checks
  --exports N     corpus size passed to seed.sh
  --i-know-this-bucket
                  passed to seed.sh and replicate.sh, letting their --reset
                  empty a bucket that carries no rehearsal marker
  --dry-run       show the sequence and the configuration, run nothing
  --help, -h      this message

USAGE
  dr_usage_environment
}

FAULT=""
EXPORTS=""
KNOW_BUCKET=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fault)
      [[ $# -ge 2 ]] || dr_die "${DR_EX_USAGE}" "--fault needs a value"
      FAULT="$2"
      shift 2
      ;;
    --i-know-this-bucket)
      KNOW_BUCKET=1
      shift
      ;;
    --exports)
      [[ $# -ge 2 ]] || dr_die "${DR_EX_USAGE}" "--exports needs a value"
      EXPORTS="$2"
      shift 2
      ;;
    --dry-run | --check)
      DRY_RUN=1
      shift
      ;;
    --help | -h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      dr_die "${DR_EX_USAGE}" "unknown argument: $1"
      ;;
  esac
done

TARGET_PHASE=""
TARGET_CODE=""
if [[ -n "${FAULT}" ]]; then
  TARGET_PHASE="$(fault_target_phase "${FAULT}")" || {
    usage >&2
    dr_die "${DR_EX_USAGE}" "unknown fault: ${FAULT}"
  }
  TARGET_CODE="$(phase_exit_code "${TARGET_PHASE}")"
fi

dr_init

declare -a SEED_ARGS=(--reset)
declare -a REPLICATE_ARGS=(--reset)
if [[ -n "${EXPORTS}" ]]; then
  SEED_ARGS+=(--exports "${EXPORTS}")
fi
if [[ "${KNOW_BUCKET}" -eq 1 ]]; then
  SEED_ARGS+=(--i-know-this-bucket)
  REPLICATE_ARGS+=(--i-know-this-bucket)
fi

if [[ "${DRY_RUN}" -eq 1 ]]; then
  dr_dry_run_common "rehearse.sh"
  if [[ -n "${FAULT}" ]]; then
    printf '  fault: %s, expected to be caught at %s (exit %s)\n' \
      "${FAULT}" "${TARGET_PHASE}" "${TARGET_CODE}"
    printf '         and to name the injected artefact in that check output\n'
    printf '  sequence: seed, replicate, inject, start-refusal-probe,\n'
    printf '            restore-check, start-refusal-after-failure\n'
  else
    printf '  fault: none (clean rehearsal)\n'
    printf '  sequence: seed, replicate, start-refusal-probe, restore-check, start\n'
  fi
  printf '  each phase prints "phase <name> seconds=<n>" exactly once\n'
  printf '  seed.sh and replicate.sh are run with --reset, which refuses unless\n'
  printf '    each bucket carries %s\n' "${DR_BUCKET_MARKER_KEY}"
  if [[ "${KNOW_BUCKET}" -eq 1 ]]; then
    printf '    --i-know-this-bucket is passed through: that marker check is waived\n'
  fi
  printf '  an incomplete run stops any ravel-server it started (EXIT trap)\n'
  for script in seed.sh replicate.sh inject.sh restore-check.sh start.sh; do
    if [[ -x "${DR_HERE}/${script}" ]]; then
      printf '  OK    %s is executable\n' "${script}"
    else
      printf '  WARN  %s is not executable\n' "${script}"
    fi
  done
  exit 0
fi

# A rehearsal starts from a clean slate. Every one of these is a record of
# what happened in THIS run, and a leftover from an earlier one would be read
# as this run's own evidence by both the assertions below and the workflow's.
rm -f \
  "${DR_LOG_DIR}/dr-failed-phase" \
  "${DR_LOG_DIR}/dr-marker-written-at" \
  "${DR_LOG_DIR}/dr-restore-started-at" \
  "${DR_LOG_DIR}/dr-server-started-at" \
  "${DR_LOG_DIR}/dr-restore-check-exit" \
  "${DR_LOG_DIR}/dr-injected.env" \
  "${DR_LOG_DIR}/dr-server.pid"

# A rehearsal that does not reach its own end must not leave ravel-server
# serving bucket B. Three phases here can start one (both refusal probes and
# the clean run's final start all invoke start.sh --background, which starts a
# server the moment the marker satisfies it), and every path out of this script
# other than a completed clean rehearsal has to take it down again. Without
# this, a run that goes red after the start leaves a server answering on a
# bucket the rehearsal just declared unverified.
SERVER_LEFT_RUNNING=0

stop_server() {
  local pid_file="${DR_LOG_DIR}/dr-server.pid" pid
  [[ -f "${pid_file}" ]] || return 0
  pid="$(cat "${pid_file}")"
  if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
    dr_log "stopping ravel-server (pid ${pid}) left running by an incomplete rehearsal"
    kill "${pid}" 2>/dev/null || true
    for _ in $(seq 1 30); do
      kill -0 "${pid}" 2>/dev/null || break
      sleep 1
    done
    kill -9 "${pid}" 2>/dev/null || true
  fi
  rm -f "${pid_file}"
}

on_exit() {
  local code=$?
  if [[ "${code}" -ne 0 || "${SERVER_LEFT_RUNNING}" -ne 1 ]]; then
    stop_server
  fi
  return "${code}"
}
trap on_exit EXIT

TIMING=""
EXPECTED_PHASES=()

# Run one phase, print its wall clock, and keep the timing line so the run can
# assert at the end that every phase it claims to have run emitted exactly one
# timing line. A phase that silently did not run must not read as a fast one.
run_phase() {
  local name="$1"
  shift
  local start_ns end_ns code=0 line
  EXPECTED_PHASES+=("${name}")
  start_ns="$(dr_now_ns)"
  "$@" || code=$?
  end_ns="$(dr_now_ns)"
  line="$(dr_emit_phase_seconds "${name}" "${start_ns}" "${end_ns}")"
  TIMING+="${line}"$'\n'
  printf '%s\n' "${line}"
  return "${code}"
}

step_seed() { "${DR_HERE}/seed.sh" "${SEED_ARGS[@]}"; }
step_replicate() { "${DR_HERE}/replicate.sh" "${REPLICATE_ARGS[@]}"; }
step_inject() { "${DR_HERE}/inject.sh" --fault "${FAULT}"; }

RESTORE_LOG="${DR_LOG_DIR}/rehearse-restore-check.log"
RESTORE_RC=0
step_restore_check() {
  RESTORE_RC=0
  "${DR_HERE}/restore-check.sh" >"${RESTORE_LOG}" 2>&1 || RESTORE_RC=$?
  cat "${RESTORE_LOG}"
  return "${RESTORE_RC}"
}

PROBE_RC=0
step_start_probe() {
  PROBE_RC=0
  "${DR_HERE}/start.sh" --background >"${DR_LOG_DIR}/rehearse-start-probe.log" 2>&1 \
    || PROBE_RC=$?
  cat "${DR_LOG_DIR}/rehearse-start-probe.log"
  return 0
}

step_start() { "${DR_HERE}/start.sh" --background; }

run_phase seed step_seed
run_phase replicate step_replicate
if [[ -n "${FAULT}" ]]; then
  run_phase inject step_inject
fi

# The ordering proof. Bucket B has just been mirrored and holds no marker, so
# a start here must be refused with exit 10; a start that succeeded would mean
# ravel-server can serve an unverified restore.
run_phase start-refusal-probe step_start_probe
if [[ "${PROBE_RC}" -ne 10 ]]; then
  dr_die 1 \
    "start.sh exited ${PROBE_RC} before the restore checks ran; it must refuse with 10"
fi

restore_rc=0
run_phase restore-check step_restore_check || restore_rc=$?
printf '%s\n' "${restore_rc}" >"${DR_LOG_DIR}/dr-restore-check-exit"

# Which checks reported a verdict, read from restore-check's own output rather
# than inferred from its exit code.
passed_phases="$(awk '
  index($0, "dr-phase-pass: ") == 1 { print substr($0, 16) }
' "${RESTORE_LOG}")"
failed_phase=""
if [[ -f "${DR_LOG_DIR}/dr-failed-phase" ]]; then
  failed_phase="$(cat "${DR_LOG_DIR}/dr-failed-phase")"
fi

phase_reported() {
  local wanted="$1"
  if [[ "${failed_phase}" == "${wanted}" ]]; then
    return 0
  fi
  awk -v w="${wanted}" 'BEGIN { f = 1 } $0 == w { f = 0 } END { exit f }' <<<"${passed_phases}"
}

if [[ -z "${FAULT}" ]]; then
  if [[ "${restore_rc}" -ne 0 ]]; then
    dr_die "${restore_rc}" \
      "a clean rehearsal failed at ${failed_phase:-an unnamed check} (exit ${restore_rc})"
  fi
  for phase in "${DR_RESTORE_PHASES[@]}"; do
    phase_reported "${phase}" || dr_die 1 \
      "restore-check exited 0 without reporting the ${phase} check; a check that did not run is not a check that passed"
  done
  run_phase start step_start
  # The one path that deliberately leaves a server up: the rehearsal's stated
  # outcome is ravel-server serving the restored bucket.
  SERVER_LEFT_RUNNING=1
else
  if [[ "${restore_rc}" -ne "${TARGET_CODE}" ]]; then
    dr_die 1 \
      "fault ${FAULT} made restore-check exit ${restore_rc}; it must be caught at ${TARGET_PHASE} (exit ${TARGET_CODE})"
  fi
  if [[ "${failed_phase}" != "${TARGET_PHASE}" ]]; then
    dr_die 1 \
      "fault ${FAULT} failed at check '${failed_phase:-<none recorded>}'; it must be caught at ${TARGET_PHASE}"
  fi
  # Every check before the target must have passed, and no check after it may
  # have reported anything: "stop at the first failure" is the behaviour under
  # test, not a description of it.
  reached_target=0
  for phase in "${DR_RESTORE_PHASES[@]}"; do
    if [[ "${phase}" == "${TARGET_PHASE}" ]]; then
      reached_target=1
      continue
    fi
    if [[ "${reached_target}" -eq 0 ]]; then
      phase_reported "${phase}" || dr_die 1 \
        "check ${phase} runs before ${TARGET_PHASE} and reported nothing"
    else
      if phase_reported "${phase}"; then
        dr_die 1 \
          "check ${phase} runs after ${TARGET_PHASE} and still reported a verdict; the checks did not stop at the first failure"
      fi
    fi
  done

  # The failure must be about the artefact that was injected. A phase name and
  # an exit code together say only that something went wrong in the right
  # check; inject.sh already records what it injected, and this is what ties
  # the two ends together. The evidence is the injected key, the forged
  # identity, or the exact out-of-band figure line the injection produces,
  # decided by inject.sh at injection time.
  INJECTED_ENV="${DR_LOG_DIR}/dr-injected.env"
  [[ -f "${INJECTED_ENV}" ]] || dr_die 1 \
    "inject.sh left no ${INJECTED_ENV}; there is nothing to tie the failure to"
  expected_evidence="$(awk -v k="DR_EXPECTED_EVIDENCE=" '
    index($0, k) == 1 { print substr($0, length(k) + 1) }
  ' "${INJECTED_ENV}")"
  [[ -n "${expected_evidence}" ]] || dr_die 1 \
    "inject.sh recorded no DR_EXPECTED_EVIDENCE for fault ${FAULT}"
  evidence_hits="$(awk -v needle="${expected_evidence}" '
    BEGIN { c = 0 } index($0, needle) > 0 { c++ } END { print c }
  ' "${RESTORE_LOG}")"
  if [[ "${evidence_hits}" -eq 0 ]]; then
    dr_die 1 \
      "check ${TARGET_PHASE} failed, but its output never names the injected artefact '${expected_evidence}'; the fault was not what made it fail"
  fi
  printf 'rehearse: %s named the injected artefact %s time(s): %s\n' \
    "${TARGET_PHASE}" "${evidence_hits}" "${expected_evidence}"

  # A failed restore must leave the server unstartable. The marker is written
  # only on success, so this is the same refusal as the probe, now after the
  # checks have run and failed.
  run_phase start-refusal-after-failure step_start_probe
  if [[ "${PROBE_RC}" -ne 10 ]]; then
    dr_die 1 \
      "start.sh exited ${PROBE_RC} after a failed restore; it must still refuse with 10"
  fi
fi

dr_assert_timing_lines "${TIMING}" "${EXPECTED_PHASES[@]}" || dr_die 1 \
  "the phase timings do not account for every phase that ran"

if [[ -z "${FAULT}" ]]; then
  printf 'rehearse: clean rehearsal complete; ravel-server started against %s after the marker\n' \
    "${DR_BUCKET_REPLICA}"
else
  printf 'rehearse: fault %s was caught at %s (exit %s), named in that check output, and the server stayed refused\n' \
    "${FAULT}" "${TARGET_PHASE}" "${TARGET_CODE}"
fi
