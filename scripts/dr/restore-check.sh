#!/usr/bin/env bash
# Usage: scripts/dr/restore-check.sh [--dry-run] [--help]
#
# Step 4 of the disaster-recovery rehearsal (issue #814): with writers
# stopped, run the restore checklist's reconciliation and verification checks
# against bucket B in a fixed order, stopping at the first failure, and write
# the reconciled marker only when all four have passed.
#
# The order is the checklist's order (docs/guides/disaster-recovery.md steps 3
# and 4) and it is enforced mechanically rather than by convention: each check
# is a named phase, a phase that fails records its own name and exits with its
# own code, and no later phase runs.
#
#   1 custody-manifest           exit 11
#       (a) the step-0 custody items are declared: the tenant-hash derivation
#           (a deployment key file, or an explicit opt out), the per-tenant KMS
#           configuration (a file, or an explicit `none`), and the admin
#           credential (a file, or an explicit `none`). "Unset" and
#           "deliberately none" are different answers and only one of them is a
#           restore-ready deployment, so an unset value is refused rather than
#           defaulted.
#       (b) the restore bucket's own manifest reconciles: every L0 commit
#           record pairs with an L0 data object of the same
#           <writer>.<epoch>.<seq> identity, and every L0 data object pairs
#           with a record. The pairing is readable from the key layout
#           (docs/catalog-and-mvcc.md) with no decode step.
#   2 commit-reconstruction      exit 12
#       Every L0 data object replicate.sh moved is still present, and
#       `ravel-cli commit reconstruct` rebuilds a record for each one that
#       lost its own. An object that reached neither the bucket nor a record
#       cannot be rebuilt from anything, which is what the inventory figure
#       here is for.
#   3 catalog-fold-verification  exit 13
#       `ravel-cli catalog fold` then `ravel-cli catalog verify`: the snapshot
#       agrees with the sealed commit history.
#
#       READ THIS BEFORE TRUSTING THIS PHASE. A fold seals an ingest hour only
#       `max_flush_lifetime + clock_skew_allowance + fold_safety_margin` after
#       that hour ends. A rehearsal that does not wait that margin out seals
#       nothing, publishes no snapshot HEAD, and gives catalog verify nothing
#       to diff, so THE PHASE VERIFIES NOTHING about the folded catalog. In
#       that case it prints `fold.verification: NO-OP ...` and makes exactly
#       two claims, both of which can fail: the fold's own entry_count (read
#       from the tool) is inside its band, and a fold that sealed entries
#       while publishing no HEAD is a contradiction. Set
#       DR_FOLD_SEAL_MARGIN_WAITED=1 on a run that really did wait the margin
#       out, and a missing HEAD becomes a failure instead of a no-op.
#   4 canary-query               exit 14
#       The pre-serving read verification: `ravel-cli maintain verify-custody
#       --versioning-aware` re-checks every live object's content against its
#       key, then the canary reads back through ravel-cli
#       (`catalog list` to resolve the segments, `segment inspect` to GET and
#       decode each one) and the rows it returns are asserted against the
#       seeded corpus. This is the first phase that reads object BODIES, so it
#       is the first phase that can see a replicated object that no longer
#       parses.
#
#   --dry-run   validate configuration and dependencies, touch nothing
#
# Exit 0 when all four pass and the marker is written, 64 on bad usage, 10 on
# an unmet precondition (a writer still running, or no pre-registered
# figures), 11/12/13/14 on the phase of that number.
set -euo pipefail

# shellcheck source=scripts/dr/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

DR_PHASES=(custody-manifest commit-reconstruction catalog-fold-verification canary-query)
DR_EX_PHASE_PRECONDITION=10

usage() {
  cat <<'USAGE'
Usage: scripts/dr/restore-check.sh [--dry-run] [--help]

Runs the four restore checks against bucket B in order, stopping at the first
failure, and writes the reconciled marker only when all four pass.

  1 custody-manifest           exit 11
  2 commit-reconstruction      exit 12
  3 catalog-fold-verification  exit 13
  4 canary-query               exit 14

Phase 3 is a NO-OP on a run that did not wait the catalog seal margin out:
no ingest hour is sealed, no snapshot HEAD is published, and catalog verify
has nothing to diff. It prints `fold.verification: NO-OP ...` when that
happens rather than reporting green figures it did not read from the tool.
Set DR_FOLD_SEAL_MARGIN_WAITED=1 on a run that did wait, and a missing HEAD
becomes a phase failure.

The failing phase's name is printed as `dr-phase-fail: <name>` and written to
<DR_LOG_DIR>/dr-failed-phase, so a caller can assert WHICH check failed rather
than only that something did.

Options:
  --dry-run    validate configuration and dependencies without touching a
               bucket or the network
  --help, -h   this message

USAGE
  dr_usage_environment
}

DRY_RUN=0
while [[ $# -gt 0 ]]; do
  case "$1" in
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

dr_init

if [[ "${DRY_RUN}" -eq 1 ]]; then
  dr_dry_run_common "restore-check.sh"
  printf '  phases, in order and stopping at the first failure:\n'
  printf '    1 custody-manifest           exit 11\n'
  printf '    2 commit-reconstruction      exit 12\n'
  printf '    3 catalog-fold-verification  exit 13\n'
  printf '    4 canary-query               exit 14\n'
  printf '  custody items declared: tenant-hash=%s kms=%s admin-credential=%s\n' \
    "${DR_TENANT_HASH_MODE}" "${DR_TENANT_KMS_CONFIG:-<unset: phase 1 refuses>}" \
    "${DR_ADMIN_CREDENTIAL_FILE:-<unset: phase 1 refuses>}"
  printf '  would: delete %s/%s and write %s/%s before any check runs\n' \
    "${DR_BUCKET_REPLICA}" "${DR_MARKER_KEY}" "${DR_BUCKET_REPLICA}" "${DR_RESTORE_START_KEY}"
  if [[ "${DR_FOLD_SEAL_MARGIN_WAITED}" -eq 1 ]]; then
    printf '  fold phase: REAL, DR_FOLD_SEAL_MARGIN_WAITED=1 requires a snapshot HEAD\n'
  else
    printf '  fold phase: NO-OP unless the run waited the seal margin out\n'
  fi
  if dr_writers_stopped; then
    printf '  OK    no writer answering on %s\n' "${DR_HTTP_ADDR}"
  else
    printf '  WARN  a writer is answering on %s; a real run would refuse\n' "${DR_HTTP_ADDR}"
  fi
  if [[ -f "$(dr_expect_file)" ]]; then
    printf '  OK    pre-registered figures present at %s\n' "$(dr_expect_file)"
  else
    printf '  WARN  no pre-registered figures yet: run seed.sh and replicate.sh first\n'
  fi
  exit 0
fi

PHASE_LOG="${DR_LOG_DIR}/restore-check.log"
FAILED_PHASE_FILE="${DR_LOG_DIR}/dr-failed-phase"
: >"${PHASE_LOG}"
# Both records describe THIS run. A stale one would be read as this run's own
# evidence that a check failed, or that the marker was written.
rm -f "${FAILED_PHASE_FILE}" "${DR_LOG_DIR}/dr-marker-written-at"

phase_fail() {
  local name="$1" code="$2"
  shift 2
  printf '%s\n' "${name}" >"${FAILED_PHASE_FILE}"
  printf 'dr-phase-fail: %s\n' "${name}"
  printf 'dr: %s\n' "$*" >&2
  exit "${code}"
}

phase_pass() { printf 'dr-phase-pass: %s\n' "$1"; }

# Run a ravel-cli subcommand against bucket B, appending its output to the
# phase log and leaving it in CLI_OUT / CLI_RC for the caller to assert on.
CLI_OUT=""
CLI_RC=0
cli() {
  CLI_RC=0
  CLI_OUT="$(dr_ravel_cli "$@" 2>&1)" || CLI_RC=$?
  printf '--- ravel-cli %s (exit %s)\n%s\n' "$*" "${CLI_RC}" "${CLI_OUT}" >>"${PHASE_LOG}"
  if [[ "${CLI_RC}" -ne 0 ]]; then
    # The tool's own output carries the artefact it failed on (verify-custody
    # names the mismatching data key on its `<-- ANOMALY` line, for one), and a
    # caller asserting the failure is about the fault that was injected has to
    # be able to read it. A pointer to the phase log is not evidence.
    printf 'dr-cli-failed: ravel-cli %s (exit %s); its output follows\n' "$*" "${CLI_RC}" >&2
    printf '%s\n' "${CLI_OUT}" >&2
  fi
}

# --- preconditions -------------------------------------------------------

dr_mc_available || dr_die "${DR_EX_PHASE_PRECONDITION}" \
  "no mc binary and no docker: cannot reach the object store"
dr_ravel_binaries_available || dr_die "${DR_EX_PHASE_PRECONDITION}" \
  "no ravel-cli and no cargo: cannot verify"

# Writers stopped. Every figure below is a whole-bucket count and a writer
# still flushing into B would move each one under the assertion that reads it.
dr_writers_stopped || dr_die "${DR_EX_PHASE_PRECONDITION}" \
  "a writer is still answering on ${DR_HTTP_ADDR}; freeze writers before the restore checks"

EXPECT_DATA="$(dr_expect DR_EXPECT_DATA_OBJECTS)" \
  || dr_die "${DR_EX_PHASE_PRECONDITION}" "no pre-registered figures"
EXPECT_COMMITS="$(dr_expect DR_EXPECT_COMMIT_RECORDS)" \
  || dr_die "${DR_EX_PHASE_PRECONDITION}" "no pre-registered figures"
EXPECT_SAMPLES="$(dr_expect DR_EXPECT_SAMPLES)" \
  || dr_die "${DR_EX_PHASE_PRECONDITION}" "no pre-registered figures"
EXPECT_TENANT_HASH="$(dr_expect DR_EXPECT_TENANT_HASH)" \
  || dr_die "${DR_EX_PHASE_PRECONDITION}" "no pre-registered figures"

dr_export_s3_env "${DR_BUCKET_REPLICA}"

# --- clear the previous marker, then stamp this restore's start -------------
#
# This is the FIRST thing this script does to the bucket. An earlier run's
# reconciled marker is a live object in the restore target, and leaving it
# there means a run that fails at phase 1 still leaves start.sh a marker to be
# satisfied by. Deleting it locally is not enough: start.sh reads the object,
# not the local copy.
#
# The restore-start stamp written in its place is what makes the ordering
# checkable rather than merely plausible. start.sh requires the marker to
# post-date this stamp, so a marker from any previous restore is refused on its
# timestamp alone, without depending on the delete above having reached the
# bucket.

RESTORE_START_NS="$(dr_now_ns)"
dr_mc rm --force "dr/${DR_BUCKET_REPLICA}/${DR_MARKER_KEY}" \
  >"${DR_LOG_DIR}/marker-clear.log" 2>&1 || true
if dr_mc stat "dr/${DR_BUCKET_REPLICA}/${DR_MARKER_KEY}" >/dev/null 2>&1; then
  dr_die "${DR_EX_PHASE_PRECONDITION}" \
    "the previous reconciled marker at ${DR_BUCKET_REPLICA}/${DR_MARKER_KEY} is still present after the delete; a restore cannot run with a stale marker in the target"
fi
{
  printf '{\n'
  printf '  "rehearsal": "ravel-dr",\n'
  printf '  "bucket": "%s",\n' "${DR_BUCKET_REPLICA}"
  printf '  "restore_started_at_unix_ns": %s\n' "${RESTORE_START_NS}"
  printf '}\n'
} | dr_mc pipe "dr/${DR_BUCKET_REPLICA}/${DR_RESTORE_START_KEY}" \
  >"${DR_LOG_DIR}/restore-start-write.log" 2>&1
printf '%s\n' "${RESTORE_START_NS}" >"${DR_LOG_DIR}/dr-restore-started-at"
printf 'restore-check: restore_start_ns=%s (previous marker cleared from %s)\n' \
  "${RESTORE_START_NS}" "${DR_BUCKET_REPLICA}"

# ---------------------------------------------------------------------------
# 1. custody-manifest
# ---------------------------------------------------------------------------
phase_custody_manifest() {
  local name="custody-manifest"

  # (a) the step-0 custody items, each declared rather than defaulted.
  if [[ "${DR_TENANT_HASH_MODE}" == "keyed" ]]; then
    [[ -r "${DR_TENANT_HASH_KEY_FILE}" ]] || phase_fail "${name}" 11 \
      "DR_TENANT_HASH_MODE=keyed but the deployment key file ${DR_TENANT_HASH_KEY_FILE} is not readable"
  fi
  # These two carry no default in lib.sh, so an operator who declared nothing
  # reaches here with an empty value and is refused. A default of `none` would
  # have made this check unfailable: it would report the declaration the
  # default made on the operator's behalf.
  dr_custody_declared DR_TENANT_KMS_CONFIG "${DR_TENANT_KMS_CONFIG}" \
    || phase_fail "${name}" 11 \
      "the per-tenant KMS custody item is not declared; set DR_TENANT_KMS_CONFIG to a readable file or the literal 'none'"
  dr_custody_declared DR_ADMIN_CREDENTIAL_FILE "${DR_ADMIN_CREDENTIAL_FILE}" \
    || phase_fail "${name}" 11 \
      "the admin credential custody item is not declared; set DR_ADMIN_CREDENTIAL_FILE to a readable file or the literal 'none'"
  printf 'custody items: tenant-hash=%s kms=%s admin-credential=%s\n' \
    "${DR_TENANT_HASH_MODE}" "${DR_TENANT_KMS_CONFIG}" "${DR_ADMIN_CREDENTIAL_FILE}"

  # (b) the restore bucket's own manifest.
  local keys tenant_hash data_keys commit_keys data_count commit_count
  keys="$(dr_list_keys "${DR_BUCKET_REPLICA}")" || phase_fail "${name}" 11 \
    "could not list ${DR_BUCKET_REPLICA}"
  tenant_hash="$(dr_tenant_hash_from_keys "${keys}")" || phase_fail "${name}" 11 \
    "the restore bucket does not hold exactly one tenant prefix"
  [[ "${tenant_hash}" == "${EXPECT_TENANT_HASH}" ]] || phase_fail "${name}" 11 \
    "restore bucket holds tenant hash ${tenant_hash}, expected ${EXPECT_TENANT_HASH}"

  data_keys="$(dr_l0_data_keys "${keys}")"
  commit_keys="$(dr_l0_commit_keys "${keys}")"
  data_count="$(dr_count_lines "${data_keys}")"
  commit_count="$(dr_count_lines "${commit_keys}")"

  # Identity sets, from the key layout alone.
  local data_ids commit_ids orphan_records orphan_objects key
  data_ids=""
  while IFS= read -r key; do
    [[ -n "${key}" ]] || continue
    data_ids+="$(dr_l0_identity "${key}")"$'\n'
  done <<<"${data_keys}"
  commit_ids=""
  while IFS= read -r key; do
    [[ -n "${key}" ]] || continue
    commit_ids+="$(dr_l0_identity "${key}")"$'\n'
  done <<<"${commit_keys}"

  orphan_records="$(comm -23 \
    <(printf '%s' "${commit_ids}" | sort -u) \
    <(printf '%s' "${data_ids}" | sort -u))"
  orphan_objects="$(comm -13 \
    <(printf '%s' "${commit_ids}" | sort -u) \
    <(printf '%s' "${data_ids}" | sort -u))"

  local orphan_record_count orphan_object_count failures=0
  orphan_record_count="$(dr_count_lines "${orphan_records}")"
  orphan_object_count="$(dr_count_lines "${orphan_objects}")"

  # Bands. The two counts get a corpus-derived upper bound (the restore
  # bucket cannot legitimately hold more L0 objects or records than the
  # primary held) and a floor of one; the two orphan counts are the check
  # this phase is named for and are exact zeroes. The counts are deliberately
  # NOT pinned to the pre-registered inventory here: an object that is simply
  # absent is check 2's finding, and pinning it here would move the failure
  # to the wrong phase.
  dr_assert_figure "custody.l0_data_objects" "${data_count}" 1 "${EXPECT_DATA}" || failures=1
  dr_assert_figure "custody.l0_commit_records" "${commit_count}" 1 "${EXPECT_COMMITS}" || failures=1
  dr_assert_figure "custody.records_without_data_object" "${orphan_record_count}" 0 0 || failures=1
  dr_assert_figure "custody.data_objects_without_record" "${orphan_object_count}" 0 0 || failures=1
  if [[ "${failures}" -ne 0 ]]; then
    if [[ -n "${orphan_records}" ]]; then
      printf 'dangling commit record identities:\n%s\n' "${orphan_records}" >&2
    fi
    if [[ -n "${orphan_objects}" ]]; then
      printf 'unrecorded data object identities:\n%s\n' "${orphan_objects}" >&2
    fi
    phase_fail "${name}" 11 "the restore bucket's custody manifest does not reconcile"
  fi
  phase_pass "${name}"
}

# ---------------------------------------------------------------------------
# 2. commit-reconstruction
# ---------------------------------------------------------------------------
phase_commit_reconstruction() {
  local name="commit-reconstruction"
  local keys data_count failures=0

  keys="$(dr_list_keys "${DR_BUCKET_REPLICA}")" || phase_fail "${name}" 12 \
    "could not list ${DR_BUCKET_REPLICA}"
  data_count="$(dr_count_lines "$(dr_l0_data_keys "${keys}")")"

  # The inventory reconstruction has to work with. Reconstruction rebuilds a
  # record from a surviving object's own footer, so an object that never
  # reached the restore bucket is unrecoverable here and this is the phase
  # that owns that finding. Exact equality against the figure replicate.sh
  # pre-registered.
  dr_assert_figure "reconstruct.input_l0_data_objects" "${data_count}" \
    "${EXPECT_DATA}" "${EXPECT_DATA}" || failures=1
  if [[ "${failures}" -ne 0 ]]; then
    phase_fail "${name}" 12 \
      "the restore bucket holds ${data_count} L0 data object(s), not the ${EXPECT_DATA} that were replicated into it"
  fi

  local shard reconstructed=0 already_present=0 failed=0 summary line
  local -a fields
  for ((shard = 0; shard < DR_SHARDS; shard++)); do
    cli --store s3 commit reconstruct \
      --tenant "${DR_TENANT}" --signal metrics --shard "${shard}"
    if [[ "${CLI_RC}" -ne 0 ]]; then
      phase_fail "${name}" 12 \
        "commit reconstruct failed on shard ${shard} (exit ${CLI_RC}); see ${PHASE_LOG}"
    fi
    summary="$(dr_field_once "reconstruct summary" "${CLI_OUT}")" || phase_fail "${name}" 12 \
      "commit reconstruct on shard ${shard} printed no single summary line"
    read -r -a fields <<<"${summary}"
    for line in "${fields[@]}"; do
      case "${line}" in
        reconstructed=*) reconstructed=$((reconstructed + ${line#reconstructed=})) ;;
        already_present_skipped=*)
          already_present=$((already_present + ${line#already_present_skipped=}))
          ;;
        failed=*) failed=$((failed + ${line#failed=})) ;;
      esac
    done
  done

  # Bands. A faithful mirror of a frozen primary has no record-less data
  # object at all, so all three are exact zeroes: `reconstructed` above zero
  # means the replica lost records the primary held, `already_present_skipped`
  # above zero means a candidate raced a record that was there all along, and
  # `failed` above zero means an object's footer would not decode.
  dr_assert_figure "reconstruct.reconstructed" "${reconstructed}" 0 0 || failures=1
  dr_assert_figure "reconstruct.already_present_skipped" "${already_present}" 0 0 || failures=1
  dr_assert_figure "reconstruct.failed" "${failed}" 0 0 || failures=1
  if [[ "${failures}" -ne 0 ]]; then
    phase_fail "${name}" 12 "commit reconstruction did not leave the restore bucket whole"
  fi
  phase_pass "${name}"
}

# ---------------------------------------------------------------------------
# 3. catalog-fold-verification
# ---------------------------------------------------------------------------
phase_catalog_fold_verification() {
  local name="catalog-fold-verification"
  local failures=0 entry_count no_head

  cli --store s3 catalog fold \
    --tenant "${DR_TENANT}" --shards "${DR_SHARDS}" --signal metrics
  if [[ "${CLI_RC}" -ne 0 ]]; then
    phase_fail "${name}" 13 "catalog fold failed (exit ${CLI_RC}); see ${PHASE_LOG}"
  fi
  entry_count="$(dr_field_once "entry_count" "${CLI_OUT}")" || phase_fail "${name}" 13 \
    "catalog fold printed no single entry_count"

  # The fold seals an ingest hour only
  # `max_flush_lifetime + clock_skew_allowance + fold_safety_margin` after
  # that hour ends, so a rehearsal that does not wait the margin out seals
  # nothing and zero is the expected value here. The band is therefore
  # [0, replicated L0 data objects]: corpus-derived at the top (no fold can
  # seal more entries than there are data objects) and open at the bottom
  # because the seal margin is a precondition on TIME that this harness
  # deliberately does not wait for. Nothing is published off this figure.
  dr_assert_figure "fold.entry_count" "${entry_count}" 0 "${EXPECT_DATA}" || failures=1

  cli --store s3 catalog verify --tenant "${DR_TENANT}" --signal metrics
  if [[ "${CLI_RC}" -ne 0 ]]; then
    phase_fail "${name}" 13 "catalog verify failed (exit ${CLI_RC}); see ${PHASE_LOG}"
  fi

  no_head="$(awk 'BEGIN { c = 0 } index($0, "no HEAD found at ") == 1 { c++ } END { print c }' \
    <<<"${CLI_OUT}")"
  if [[ "${no_head}" -eq 1 ]]; then
    # No snapshot HEAD was published, so catalog verify diffed nothing and
    # there is no figure to read out of it. This phase then verifies NOTHING
    # about the fold's agreement with the commit history, and says so. It used
    # to assert three literal zeroes here, which read as three green figure
    # lines and could not fail: they were the script's own constants, not the
    # tool's output.
    #
    # Two real assertions survive the no-op, and they are the only claims this
    # branch makes: a fold that sealed entries and left no HEAD is a
    # contradiction whatever the seal margin was, and a run that declares it
    # waited the margin out (DR_FOLD_SEAL_MARGIN_WAITED=1) must have a HEAD.
    printf 'fold.verification: NO-OP no snapshot HEAD was published, so nothing was diffed and this phase verified nothing about the folded catalog\n'
    printf 'fold.verification: cause the fold seals an ingest hour only max_flush_lifetime + clock_skew_allowance + fold_safety_margin after that hour ends, and this run did not wait that margin out\n'
    printf 'fold.verification: DR_FOLD_SEAL_MARGIN_WAITED=%s\n' "${DR_FOLD_SEAL_MARGIN_WAITED}"
    if [[ "${DR_FOLD_SEAL_MARGIN_WAITED}" -eq 1 ]]; then
      failures=1
      printf 'dr: DR_FOLD_SEAL_MARGIN_WAITED=1 says this run waited the seal margin out, but the fold published no snapshot HEAD\n' >&2
    fi
    if [[ "${entry_count}" -ne 0 ]]; then
      failures=1
      printf 'dr: the fold sealed %s entries but published no HEAD\n' "${entry_count}" >&2
    fi
  else
    local snapshot_entries missing mismatches
    printf 'fold.verification: REAL a snapshot HEAD is present and the three figures below are read from catalog verify\n'
    snapshot_entries="$(dr_field_once "snapshot entries" "${CLI_OUT}")" \
      || phase_fail "${name}" 13 "catalog verify printed no single snapshot-entry count"
    missing="$(dr_field_once "missing from snapshot" "${CLI_OUT}")" \
      || phase_fail "${name}" 13 "catalog verify printed no single missing-entry count"
    mismatches="$(dr_field_once "content_hash mismatches" "${CLI_OUT}")" \
      || phase_fail "${name}" 13 "catalog verify printed no single mismatch count"
    dr_assert_figure "verify.snapshot_entries" "${snapshot_entries}" 0 "${EXPECT_DATA}" || failures=1
    dr_assert_figure "verify.missing_from_snapshot" "${missing}" 0 0 || failures=1
    dr_assert_figure "verify.content_hash_mismatches" "${mismatches}" 0 0 || failures=1
  fi

  if [[ "${failures}" -ne 0 ]]; then
    phase_fail "${name}" 13 "the folded catalog does not agree with the sealed commit history"
  fi
  phase_pass "${name}"
}

# ---------------------------------------------------------------------------
# 4. canary-query
# ---------------------------------------------------------------------------
CANARY_SEGMENTS=0
CANARY_SAMPLES=0
CANARY_SERIES=0
CANARY_DECODED_SAMPLES=0

phase_canary_query() {
  local name="canary-query"
  local failures=0

  # Content custody, re-checked immediately before serving: every live object
  # a surviving record names is present and still hashes to the hash16 its key
  # carries.
  cli --store s3 maintain verify-custody \
    --tenant "${DR_TENANT}" --shards "${DR_SHARDS}" --versioning-aware
  if [[ "${CLI_RC}" -ne 0 ]]; then
    phase_fail "${name}" 14 \
      "maintain verify-custody failed (exit ${CLI_RC}); see ${PHASE_LOG}"
  fi
  local verified mismatches missing anomalies
  verified="$(dr_field_once "  live data objects verified (content hash matches key)" "${CLI_OUT}")" \
    || phase_fail "${name}" 14 "verify-custody printed no single verified-object count"
  mismatches="$(dr_field_once "  content-hash mismatches (ANOMALY)" "${CLI_OUT}")" \
    || phase_fail "${name}" 14 "verify-custody printed no single mismatch count"
  missing="$(dr_field_once "  live objects missing from store (ANOMALY)" "${CLI_OUT}")" \
    || phase_fail "${name}" 14 "verify-custody printed no single missing-object count"
  anomalies="$(dr_field_once "  total anomalies" "${CLI_OUT}")" \
    || phase_fail "${name}" 14 "verify-custody printed no single anomaly total"
  # One live object is verified per surviving L0 commit record, so the
  # verified count is pinned to the records replicate.sh moved.
  dr_assert_figure "custody.objects_verified" "${verified}" \
    "${EXPECT_COMMITS}" "${EXPECT_COMMITS}" || failures=1
  dr_assert_figure "custody.content_hash_mismatches" "${mismatches}" 0 0 || failures=1
  dr_assert_figure "custody.missing_live_objects" "${missing}" 0 0 || failures=1
  dr_assert_figure "custody.total_anomalies" "${anomalies}" 0 0 || failures=1

  # The canary read, through ravel-cli against bucket B: resolve the tenant's
  # live segments, then GET and decode each one.
  cli --store s3 catalog list \
    --tenant "${DR_TENANT}" --hours 2 --shards "${DR_SHARDS}"
  if [[ "${CLI_RC}" -ne 0 ]]; then
    phase_fail "${name}" 14 "the canary query failed (exit ${CLI_RC}); see ${PHASE_LOG}"
  fi
  local segments_line segment_keys
  segments_line="$(awk '
    BEGIN { c = 0 }
    $2 == "segment(s)" { c++; v = $1 }
    END { if (c == 1) print v; else print "" }
  ' <<<"${CLI_OUT}")"
  [[ -n "${segments_line}" ]] || phase_fail "${name}" 14 \
    'the canary query printed no single "N segment(s)" line'
  CANARY_SEGMENTS="${segments_line}"
  CANARY_SAMPLES="$(awk '
    BEGIN { total = 0 }
    { for (i = 1; i <= NF; i++) if (index($i, "samples=") == 1) total += substr($i, 9) }
    END { print total }
  ' <<<"${CLI_OUT}")"
  CANARY_SERIES="$(awk '
    BEGIN { total = 0 }
    { for (i = 1; i <= NF; i++) if (index($i, "series=") == 1) total += substr($i, 8) }
    END { print total }
  ' <<<"${CLI_OUT}")"
  segment_keys="$(awk '$2 ~ /^shard=/ { print $1 }' <<<"${CLI_OUT}")"

  local listed_keys_count
  listed_keys_count="$(dr_count_lines "${segment_keys}")"
  if [[ "${listed_keys_count}" -ne "${CANARY_SEGMENTS}" ]]; then
    phase_fail "${name}" 14 \
      "the canary listed ${listed_keys_count} segment line(s) but reported ${CANARY_SEGMENTS} segment(s)"
  fi

  # Decode every segment the canary resolved. `catalog list` reads the
  # catalog's own metadata and never opens an object, so this is the step that
  # proves the bytes behind the catalog are still readable.
  local key inspected=0 decoded=0 sample_count
  while IFS= read -r key; do
    [[ -n "${key}" ]] || continue
    cli --store s3 segment inspect "${key}"
    if [[ "${CLI_RC}" -ne 0 ]]; then
      phase_fail "${name}" 14 \
        "the canary could not decode ${key} (exit ${CLI_RC}); see ${PHASE_LOG}"
    fi
    sample_count="$(dr_field_once "sample_count" "${CLI_OUT}")" || phase_fail "${name}" 14 \
      "segment inspect printed no single sample_count for ${key}"
    decoded=$((decoded + sample_count))
    inspected=$((inspected + 1))
  done <<<"${segment_keys}"
  CANARY_DECODED_SAMPLES="${decoded}"

  # Bands, all derived from the seeded corpus:
  #   segments          [1, replicated L0 data objects]. Several exports may
  #                     share a flush, so the segment count is not the export
  #                     count, but it can never exceed the L0 objects that
  #                     were replicated and a restore that resolves none is a
  #                     failed restore.
  #   samples           exactly the accepted exports. The fixture is one
  #                     resource, one gauge, one data point, so one export is
  #                     one sample.
  #   series            [1, accepted exports]. Every export carries the same
  #                     labels, so a segment holds one series and the sum over
  #                     segments is between one and the sample count.
  #   decoded samples   exactly the accepted exports, and exactly what the
  #                     catalog claimed: the catalog's metadata and the bytes
  #                     on the object have to agree.
  #   segments inspected   [1, replicated L0 data objects], the same
  #                     corpus-derived band as the segment count. It is NOT
  #                     banded on CANARY_SEGMENTS: the listed-versus-reported
  #                     check above already proves those two equal, and the
  #                     loop increments this counter once per listed key, so
  #                     that band could not fail.
  dr_assert_figure "canary.segments" "${CANARY_SEGMENTS}" 1 "${EXPECT_DATA}" || failures=1
  dr_assert_figure "canary.segments_inspected" "${inspected}" 1 "${EXPECT_DATA}" || failures=1
  dr_assert_figure "canary.samples" "${CANARY_SAMPLES}" \
    "${EXPECT_SAMPLES}" "${EXPECT_SAMPLES}" || failures=1
  dr_assert_figure "canary.series" "${CANARY_SERIES}" 1 "${EXPECT_SAMPLES}" || failures=1
  dr_assert_figure "canary.decoded_samples" "${CANARY_DECODED_SAMPLES}" \
    "${EXPECT_SAMPLES}" "${EXPECT_SAMPLES}" || failures=1

  if [[ "${failures}" -ne 0 ]]; then
    phase_fail "${name}" 14 "the canary did not read back the seeded corpus"
  fi
  phase_pass "${name}"
}

# --- run the phases in order, stopping at the first failure ---------------

run_phase() {
  local name="$1" fn="$2" start_ns end_ns
  start_ns="$(dr_now_ns)"
  "${fn}"
  end_ns="$(dr_now_ns)"
  dr_emit_phase_seconds "${name}" "${start_ns}" "${end_ns}"
}

run_phase "custody-manifest" phase_custody_manifest
run_phase "commit-reconstruction" phase_commit_reconstruction
run_phase "catalog-fold-verification" phase_catalog_fold_verification
run_phase "canary-query" phase_canary_query

# --- the reconciled marker ------------------------------------------------
#
# Written only here, after all four checks have passed. It names the bucket it
# was written for, so start.sh cannot be satisfied by a marker left over from
# a restore into a different bucket, and it carries the restore-start stamp
# this run wrote before phase 1, so start.sh can require it to belong to THIS
# restore rather than to any earlier one. The key lives outside `t/` and
# `sys/`, the two key families docs/catalog-and-mvcc.md freezes.

marker_ns="$(dr_now_ns)"
dr_ns_not_after "${RESTORE_START_NS}" "${marker_ns}" || dr_die 1 \
  "the clock went backwards during the run: restore start ${RESTORE_START_NS} is after the marker stamp ${marker_ns}"
checks_json=""
for phase_name in "${DR_PHASES[@]}"; do
  if [[ -n "${checks_json}" ]]; then
    checks_json+=", "
  fi
  checks_json+="\"${phase_name}\""
done
marker_body="$(
  printf '{\n'
  printf '  "rehearsal": "ravel-dr",\n'
  printf '  "bucket": "%s",\n' "${DR_BUCKET_REPLICA}"
  printf '  "tenant": "%s",\n' "${DR_TENANT}"
  printf '  "tenant_hash": "%s",\n' "${EXPECT_TENANT_HASH}"
  printf '  "restore_started_at_unix_ns": %s,\n' "${RESTORE_START_NS}"
  printf '  "reconciled_at_unix_ns": %s,\n' "${marker_ns}"
  printf '  "checks": [%s],\n' "${checks_json}"
  printf '  "canary_segments": %s,\n' "${CANARY_SEGMENTS}"
  printf '  "canary_samples": %s,\n' "${CANARY_SAMPLES}"
  printf '  "canary_decoded_samples": %s\n' "${CANARY_DECODED_SAMPLES}"
  printf '}\n'
)"
printf '%s\n' "${marker_body}" | dr_mc pipe "dr/${DR_BUCKET_REPLICA}/${DR_MARKER_KEY}" \
  >"${DR_LOG_DIR}/marker-write.log" 2>&1
printf '%s\n' "${marker_ns}" >"${DR_LOG_DIR}/dr-marker-written-at"

printf 'restore-check: all %s checks passed; reconciled marker written to %s/%s\n' \
  "${#DR_PHASES[@]}" "${DR_BUCKET_REPLICA}" "${DR_MARKER_KEY}"
