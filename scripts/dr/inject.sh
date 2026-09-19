#!/usr/bin/env bash
# Usage: scripts/dr/inject.sh --fault <name> [--dry-run] [--help]
#
# Step 3 of the disaster-recovery rehearsal (issue #814): damage the restore
# bucket B in one named way, so the restore checks can be shown red rather
# than merely shown green.
#
# Faults, each named for the shape of damage it reproduces and each caught by
# exactly one of restore-check.sh's four ordered checks:
#
#   dangling-commit-record  Copy an L0 commit record to an unused
#                           <writer>.<epoch>.<seq> with no data object behind
#                           it. This is the record-survived-its-object shape
#                           the runbook's reconciliation step names. Caught by
#                           check 1, custody-manifest: the record cannot be
#                           paired to an L0 data object.
#
#   missing-data-object     Delete one L0 data object AND its commit record,
#                           so the pairing stays internally consistent and
#                           only the inventory is short. This is the shape a
#                           replica that never received an object has. Caught
#                           by check 2, commit-reconstruction: the number of
#                           L0 data objects reconstruction has to work with is
#                           below what replicate.sh pre-registered, and an
#                           object that reached neither the bucket nor a
#                           record cannot be rebuilt from anything.
#
#   canary-error            Overwrite one L0 data object's bytes in place,
#                           leaving its key and its commit record untouched.
#                           Every count stays right and every structural check
#                           passes; the object only fails when something reads
#                           it. This is the shape a truncated or mis-ranged
#                           replication leaves. Caught by check 4,
#                           canary-query: the canary is the first phase that
#                           GETs and decodes the data objects the catalog
#                           resolved, so it is the first phase that can see a
#                           body that no longer parses.
#
# Every fault is applied to bucket B only. Bucket A is never touched: a
# rehearsal that damaged the primary would not be a rehearsal.
#
# Each fault also records, in <DR_LOG_DIR>/dr-injected.env, the string the
# failing restore-check phase must name (DR_EXPECTED_EVIDENCE): the injected
# artefact itself, or the exact figure line its absence puts out of band. That
# is what lets rehearse.sh assert the run failed on THIS fault rather than on
# any failure that happens to land in the right phase with the right exit code.
#
#   --fault <name>  one of the three above (required)
#   --dry-run       name the fault and the keys it would touch, change nothing
#
# Exit 0 on success, 64 on bad usage, 65 on an unmet precondition.
set -euo pipefail

# shellcheck source=scripts/dr/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

DR_FAULTS=(dangling-commit-record missing-data-object canary-error)

usage() {
  cat <<'USAGE'
Usage: scripts/dr/inject.sh --fault <name> [--dry-run] [--help]

Injects one named fault into the restore bucket B (never bucket A).

Faults:
  dangling-commit-record  an L0 commit record with no data object behind it
                          (caught by restore-check step 1, custody-manifest)
  missing-data-object     an L0 data object and its record both absent
                          (caught by restore-check step 2,
                          commit-reconstruction)
  canary-error            an L0 data object whose bytes no longer parse, with
                          its key and record intact (caught by restore-check
                          step 4, canary-query)

Options:
  --fault <name>  which fault to inject (required)
  --dry-run       name the fault and the keys it would touch, change nothing
  --help, -h      this message

USAGE
  dr_usage_environment
}

FAULT=""
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fault)
      [[ $# -ge 2 ]] || dr_die "${DR_EX_USAGE}" "--fault needs a value"
      FAULT="$2"
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

if [[ -z "${FAULT}" ]]; then
  usage >&2
  dr_die "${DR_EX_USAGE}" "--fault is required"
fi
known=0
for candidate in "${DR_FAULTS[@]}"; do
  if [[ "${FAULT}" == "${candidate}" ]]; then
    known=1
  fi
done
if [[ "${known}" -ne 1 ]]; then
  usage >&2
  dr_die "${DR_EX_USAGE}" "unknown fault: ${FAULT}"
fi

dr_init

if [[ "${DRY_RUN}" -eq 1 ]]; then
  dr_dry_run_common "inject.sh"
  printf '  fault: %s\n' "${FAULT}"
  case "${FAULT}" in
    dangling-commit-record)
      printf '  would: copy one L0 .cmt in %s to an unused seq with no data object\n' \
        "${DR_BUCKET_REPLICA}"
      printf '         (base 10, re-padded to %s digits)\n' "${DR_SEQ_WIDTH}"
      printf '  expected to fail restore-check at: custody-manifest (exit 11)\n'
      printf '  expected evidence: the forged <writer>.<epoch>.<seq> identity, named\n'
      printf '                     by the phase as a dangling commit record\n'
      ;;
    missing-data-object)
      printf '  would: delete one L0 .rseg in %s and its paired .cmt\n' \
        "${DR_BUCKET_REPLICA}"
      printf '  expected to fail restore-check at: commit-reconstruction (exit 12)\n'
      printf '  expected evidence: the out-of-band figure line\n'
      printf '                     reconstruct.input_l0_data_objects=<n-1> band=[n,n]\n'
      ;;
    canary-error)
      printf '  would: overwrite one L0 .rseg body in %s, key and record intact\n' \
        "${DR_BUCKET_REPLICA}"
      printf '  expected to fail restore-check at: canary-query (exit 14)\n'
      printf '  expected evidence: the victim data object key, named by\n'
      printf '                     verify-custody as a CONTENT MISMATCH\n'
      ;;
  esac
  printf '  writes: %s, with DR_EXPECTED_EVIDENCE for rehearse.sh to assert on\n' \
    "${DR_LOG_DIR}/dr-injected.env"
  exit 0
fi

dr_mc_available || dr_die "${DR_EX_PRECONDITION}" \
  "no mc binary and no docker: cannot reach the object store"

keys="$(dr_list_keys "${DR_BUCKET_REPLICA}")" || dr_die "${DR_EX_PRECONDITION}" \
  "could not list ${DR_BUCKET_REPLICA}"
if [[ "$(dr_count_lines "${keys}")" -eq 0 ]]; then
  dr_die "${DR_EX_PRECONDITION}" \
    "bucket ${DR_BUCKET_REPLICA} is empty; run replicate.sh before injecting a fault"
fi

data_keys="$(dr_l0_data_keys "${keys}" | sort)"
commit_keys="$(dr_l0_commit_keys "${keys}" | sort)"
if [[ -z "${data_keys}" ]]; then
  dr_die "${DR_EX_PRECONDITION}" "no L0 data objects in ${DR_BUCKET_REPLICA}"
fi
if [[ -z "${commit_keys}" ]]; then
  dr_die "${DR_EX_PRECONDITION}" "no L0 commit records in ${DR_BUCKET_REPLICA}"
fi

first_line() { awk 'NR == 1 { print; exit }' <<<"$1"; }

# The commit record paired with a data key, matched on the
# <writer>.<epoch>.<seq> identity the two keys share.
commit_key_for_identity() {
  local identity="$1" line
  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    if [[ "${line##*/}" == "${identity}.cmt" ]]; then
      printf '%s\n' "${line}"
      return 0
    fi
  done <<<"${commit_keys}"
  return 1
}

injected_keys=""
# The string the failing restore-check phase must name for the assertion to be
# about THIS fault rather than about any failure of the right phase. It is
# either the injected artefact itself or the figure the artefact moves out of
# band; nothing here is a phase name.
expected_evidence=""

case "${FAULT}" in
  dangling-commit-record)
    victim="$(first_line "${commit_keys}")"
    base="${victim##*/}"
    prefix="${victim%/*}"
    identity="${base%.cmt}"
    writer="${identity%%.*}"
    rest="${identity#*.}"
    epoch="${rest%%.*}"
    seq="${rest#*.}"
    # A sequence number far outside anything a writer epoch of this rehearsal
    # produced, so the fabricated record cannot collide with a real object.
    # dr_forged_seq forces base 10 (a zero-padded seq inside $(( )) is read as
    # OCTAL) and re-pads to the twenty digits the key layout freezes (an
    # unpadded seq makes the forged key malformed, so the record would be
    # rejected by the key parser instead of being caught as unpaired).
    new_seq="$(dr_forged_seq "${seq}")" || dr_die "${DR_EX_PRECONDITION}" \
      "could not forge an unused seq from ${victim}"
    forged_identity="${writer}.${epoch}.${new_seq}"
    forged="${prefix}/${forged_identity}.cmt"
    dr_log "copying ${victim} to ${forged} (no data object behind it)"
    dr_mc cp "dr/${DR_BUCKET_REPLICA}/${victim}" "dr/${DR_BUCKET_REPLICA}/${forged}" \
      >"${DR_LOG_DIR}/inject-${FAULT}.log" 2>&1
    injected_keys="${forged}"
    # custody-manifest prints the identities it could not pair to a data
    # object. The forged identity is one of them and nothing else is.
    expected_evidence="${forged_identity}"
    ;;

  missing-data-object)
    victim="$(first_line "${data_keys}")"
    identity="$(dr_l0_identity "${victim}")"
    paired="$(commit_key_for_identity "${identity}")" || dr_die "${DR_EX_PRECONDITION}" \
      "no commit record paired with ${victim}; bucket B was already inconsistent"
    expect_data="$(dr_expect DR_EXPECT_DATA_OBJECTS)" || dr_die "${DR_EX_PRECONDITION}" \
      "no pre-registered DR_EXPECT_DATA_OBJECTS; run seed.sh and replicate.sh first"
    before_data="$(dr_count_lines "${data_keys}")"
    [[ "${before_data}" -eq "${expect_data}" ]] || dr_die "${DR_EX_PRECONDITION}" \
      "bucket ${DR_BUCKET_REPLICA} holds ${before_data} L0 data object(s), not the ${expect_data} that were replicated; injecting here would predict the wrong figure"
    dr_log "deleting ${victim} and its record ${paired}"
    {
      dr_mc rm "dr/${DR_BUCKET_REPLICA}/${victim}"
      dr_mc rm "dr/${DR_BUCKET_REPLICA}/${paired}"
    } >"${DR_LOG_DIR}/inject-${FAULT}.log" 2>&1
    injected_keys="${victim} ${paired}"
    # The object cannot be named by the phase that misses it (it is gone), so
    # the evidence is the exact out-of-band figure line its absence produces.
    expected_evidence="figure reconstruct.input_l0_data_objects=$((before_data - 1)) band=[${expect_data},${expect_data}]"
    ;;

  canary-error)
    victim="$(first_line "${data_keys}")"
    identity="$(dr_l0_identity "${victim}")"
    # Assert the record is present and stays present: the whole point of this
    # fault is that every structural check still passes.
    commit_key_for_identity "${identity}" >/dev/null || dr_die "${DR_EX_PRECONDITION}" \
      "no commit record paired with ${victim}; bucket B was already inconsistent"
    dr_log "overwriting the body of ${victim} in place (key and record intact)"
    printf 'dr-rehearsal injected corruption: this object no longer parses as an RSEG\n' \
      | dr_mc pipe "dr/${DR_BUCKET_REPLICA}/${victim}" \
        >"${DR_LOG_DIR}/inject-${FAULT}.log" 2>&1
    injected_keys="${victim}"
    # canary-query runs `maintain verify-custody` first, which prints
    # `CONTENT MISMATCH (l0) <data_key>` for this object and then bails, so the
    # victim key itself is named in the phase's output.
    expected_evidence="${victim}"
    ;;
esac

{
  printf 'DR_INJECTED_FAULT=%s\n' "${FAULT}"
  printf 'DR_INJECTED_KEYS=%s\n' "${injected_keys}"
  printf 'DR_EXPECTED_EVIDENCE=%s\n' "${expected_evidence}"
} >"${DR_LOG_DIR}/dr-injected.env"

printf 'inject: fault=%s bucket=%s keys=%s\n' \
  "${FAULT}" "${DR_BUCKET_REPLICA}" "${injected_keys}"
printf 'inject: expected_evidence=%s\n' "${expected_evidence}"
