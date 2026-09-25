#!/usr/bin/env bash
# Usage: scripts/dr/replicate.sh [--reset] [--i-know-this-bucket] [--dry-run]
#                                [--help]
#
# Step 2 of the disaster-recovery rehearsal (issue #814): mirror bucket A into
# an EMPTY bucket B, and assert the mirror moved exactly what bucket A holds.
#
# The restore checklist in docs/guides/disaster-recovery.md restores into a
# new, empty bucket rather than over a live one, so a non-empty bucket B is
# refused here rather than merged into.
#
#   --reset    empty bucket B first. Refuses unless DR_BUCKET_REPLICA names
#              the bucket explicitly and it carries this harness's rehearsal
#              marker.
#   --i-know-this-bucket  let --reset empty a bucket with no rehearsal marker
#   --dry-run  validate configuration and dependencies, touch nothing
#
# Exit 0 on success, 64 on bad usage, 65 on an unmet precondition, 1 when a
# mirrored figure lands outside its band.
set -euo pipefail

# shellcheck source=scripts/dr/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/dr/replicate.sh [--reset] [--i-know-this-bucket] [--dry-run]
                               [--help]

Mirrors bucket A into an empty bucket B and asserts the object counts moved
match what seed.sh pre-registered for bucket A.

Options:
  --reset      empty bucket B before mirroring. Refuses unless
               DR_BUCKET_REPLICA names the bucket explicitly and it carries
               this harness's rehearsal marker.
  --i-know-this-bucket
               let --reset empty a bucket with no rehearsal marker
  --dry-run    validate configuration and dependencies without touching a
               bucket or the network
  --help, -h   this message

USAGE
  dr_usage_environment
}

RESET=0
KNOW_BUCKET=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset)
      RESET=1
      shift
      ;;
    --i-know-this-bucket)
      KNOW_BUCKET=1
      shift
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

dr_init

if [[ "${DRY_RUN}" -eq 1 ]]; then
  dr_dry_run_common "replicate.sh"
  printf '  would: require %s to be empty (every version, not just current),\n' \
    "${DR_BUCKET_REPLICA}"
  printf '         then mirror %s into it, excluding the %s harness prefix\n' \
    "${DR_BUCKET_PRIMARY}" "${DR_HARNESS_PREFIX}"
  printf '         and the %s qualification scratch prefix\n' \
    "${DR_QUALIFY_SCRATCH_PREFIX}"
  if [[ "${RESET}" -eq 1 ]]; then
    printf '  reset: yes, and it refuses unless %s carries %s\n' \
      "${DR_BUCKET_REPLICA}" "${DR_BUCKET_MARKER_KEY}"
    if [[ "${KNOW_BUCKET}" -eq 1 ]]; then
      printf '         --i-know-this-bucket: the marker requirement is waived\n'
    fi
  else
    printf '  reset: no, a non-empty bucket B is refused\n'
  fi
  printf '  bands: mirrored objects == DR_EXPECT_TOTAL_OBJECTS exactly;\n'
  printf '         mirrored L0 data objects == DR_EXPECT_DATA_OBJECTS exactly;\n'
  printf '         mirrored L0 commit records == DR_EXPECT_COMMIT_RECORDS exactly\n'
  if [[ -f "$(dr_expect_file)" ]]; then
    printf '  OK    pre-registered figures present at %s\n' "$(dr_expect_file)"
  else
    printf '  WARN  no pre-registered figures yet: run seed.sh first\n'
  fi
  exit 0
fi

dr_aws_available || dr_die "${DR_EX_PRECONDITION}" \
  "no aws binary and no docker: cannot reach the object store"

expect_total="$(dr_expect DR_EXPECT_TOTAL_OBJECTS)"
expect_data="$(dr_expect DR_EXPECT_DATA_OBJECTS)"
expect_commits="$(dr_expect DR_EXPECT_COMMIT_RECORDS)"

dr_log "ensuring bucket ${DR_BUCKET_REPLICA} exists in region ${DR_REGION}"
dr_ensure_bucket "${DR_BUCKET_REPLICA}" || dr_die "${DR_EX_PRECONDITION}" \
  "could not create bucket ${DR_BUCKET_REPLICA} in region ${DR_REGION}"

# Emptiness is asserted over ALL versions. On a versioned bucket a recursive
# delete writes delete markers and leaves every prior version in place, so a
# current-version listing reports an empty bucket that still holds all of its
# data, and `maintain verify-custody --versioning-aware` reads those versions.
before="$(dr_list_all_versions "${DR_BUCKET_REPLICA}")" || dr_die "${DR_EX_PRECONDITION}" \
  "could not list ${DR_BUCKET_REPLICA}"
before_count="$(dr_count_lines "${before}")"
if [[ "${before_count}" -gt 0 ]]; then
  if [[ "${RESET}" -eq 1 ]]; then
    dr_reset_bucket "${DR_BUCKET_REPLICA}" "${KNOW_BUCKET}"
    before="$(dr_list_all_versions "${DR_BUCKET_REPLICA}")" || dr_die "${DR_EX_PRECONDITION}" \
      "could not re-list ${DR_BUCKET_REPLICA}"
    before_count="$(dr_count_lines "${before}")"
  fi
fi
if [[ "${before_count}" -ne 0 ]]; then
  dr_die "${DR_EX_PRECONDITION}" \
    "bucket ${DR_BUCKET_REPLICA} holds ${before_count} object version(s); the restore target must be empty (pass --reset)"
fi

dr_log "mirroring ${DR_BUCKET_PRIMARY} into ${DR_BUCKET_REPLICA}"
# The harness's own prefix is excluded: bucket A's creation marker is not
# corpus, and copying it would overwrite bucket B's own marker with one naming
# bucket A, which is exactly what dr_reset_bucket refuses to delete against.
# The qualification scratch prefix is excluded for the same reason it is
# dropped from every count: a probe fixture left behind by `store qualify` is
# tooling output, and a restore target holding it is not a copy of the corpus.
dr_aws s3 sync --exclude "${DR_HARNESS_PREFIX}*" \
  --exclude "${DR_QUALIFY_SCRATCH_PREFIX}*" \
  "s3://${DR_BUCKET_PRIMARY}/" "s3://${DR_BUCKET_REPLICA}/" \
  >"${DR_LOG_DIR}/replicate-mirror.log" 2>&1

source_keys="$(dr_list_keys "${DR_BUCKET_PRIMARY}")" || dr_die "${DR_EX_PRECONDITION}" \
  "could not list ${DR_BUCKET_PRIMARY}"
replica_keys="$(dr_list_keys "${DR_BUCKET_REPLICA}")" || dr_die "${DR_EX_PRECONDITION}" \
  "could not list ${DR_BUCKET_REPLICA}"

source_total="$(dr_count_lines "${source_keys}")"
mirrored_total="$(dr_count_lines "${replica_keys}")"
mirrored_data="$(dr_count_lines "$(dr_l0_data_keys "${replica_keys}")")"
mirrored_commits="$(dr_count_lines "$(dr_l0_commit_keys "${replica_keys}")")"

# Bands. Every one of these is an equality against a figure pre-registered
# from bucket A before anything was mirrored or injected, which is the only
# band a mirror can honestly be held to: a mirror that moved "some" objects,
# or "more than zero", is not a restore source.
failures=0
dr_assert_figure "replicate.source_objects" "${source_total}" \
  "${expect_total}" "${expect_total}" || failures=1
dr_assert_figure "replicate.mirrored_objects" "${mirrored_total}" \
  "${expect_total}" "${expect_total}" || failures=1
dr_assert_figure "replicate.mirrored_l0_data_objects" "${mirrored_data}" \
  "${expect_data}" "${expect_data}" || failures=1
dr_assert_figure "replicate.mirrored_l0_commit_records" "${mirrored_commits}" \
  "${expect_commits}" "${expect_commits}" || failures=1
if [[ "${failures}" -ne 0 ]]; then
  dr_die 1 "the mirror did not reproduce bucket A; ${DR_BUCKET_REPLICA} is not a restore source"
fi

dr_expect_write DR_EXPECT_MIRRORED_OBJECTS "${mirrored_total}"

dr_log "mirrored ${mirrored_total} object(s) into ${DR_BUCKET_REPLICA}"
printf 'replicate: source=%s target=%s objects=%s l0_data_objects=%s l0_commit_records=%s\n' \
  "${DR_BUCKET_PRIMARY}" "${DR_BUCKET_REPLICA}" "${mirrored_total}" \
  "${mirrored_data}" "${mirrored_commits}"
