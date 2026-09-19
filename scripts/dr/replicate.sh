#!/usr/bin/env bash
# Usage: scripts/dr/replicate.sh [--reset] [--dry-run] [--help]
#
# Step 2 of the disaster-recovery rehearsal (issue #814): mirror bucket A into
# an EMPTY bucket B, and assert the mirror moved exactly what bucket A holds.
#
# The restore checklist in docs/guides/disaster-recovery.md restores into a
# new, empty bucket rather than over a live one, so a non-empty bucket B is
# refused here rather than merged into.
#
#   --reset    empty bucket B first
#   --dry-run  validate configuration and dependencies, touch nothing
#
# Exit 0 on success, 64 on bad usage, 65 on an unmet precondition, 1 when a
# mirrored figure lands outside its band.
set -euo pipefail

# shellcheck source=scripts/dr/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/dr/replicate.sh [--reset] [--dry-run] [--help]

Mirrors bucket A into an empty bucket B and asserts the object counts moved
match what seed.sh pre-registered for bucket A.

Options:
  --reset      empty bucket B before mirroring
  --dry-run    validate configuration and dependencies without touching a
               bucket or the network
  --help, -h   this message

USAGE
  dr_usage_environment
}

RESET=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset)
      RESET=1
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
  printf '  would: require %s to be empty, then mirror %s into it\n' \
    "${DR_BUCKET_REPLICA}" "${DR_BUCKET_PRIMARY}"
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

dr_mc_available || dr_die "${DR_EX_PRECONDITION}" \
  "no mc binary and no docker: cannot reach the object store"

expect_total="$(dr_expect DR_EXPECT_TOTAL_OBJECTS)"
expect_data="$(dr_expect DR_EXPECT_DATA_OBJECTS)"
expect_commits="$(dr_expect DR_EXPECT_COMMIT_RECORDS)"

dr_log "ensuring bucket ${DR_BUCKET_REPLICA} exists"
dr_mc mb -p "dr/${DR_BUCKET_REPLICA}" >/dev/null 2>&1 || true

before="$(dr_list_keys "${DR_BUCKET_REPLICA}")" || dr_die "${DR_EX_PRECONDITION}" \
  "could not list ${DR_BUCKET_REPLICA}"
before_count="$(dr_count_lines "${before}")"
if [[ "${before_count}" -gt 0 ]]; then
  if [[ "${RESET}" -eq 1 ]]; then
    dr_log "emptying bucket ${DR_BUCKET_REPLICA} (${before_count} object(s))"
    dr_mc rm --recursive --force "dr/${DR_BUCKET_REPLICA}/" >/dev/null
    before="$(dr_list_keys "${DR_BUCKET_REPLICA}")" || dr_die "${DR_EX_PRECONDITION}" \
      "could not re-list ${DR_BUCKET_REPLICA}"
    before_count="$(dr_count_lines "${before}")"
  fi
fi
if [[ "${before_count}" -ne 0 ]]; then
  dr_die "${DR_EX_PRECONDITION}" \
    "bucket ${DR_BUCKET_REPLICA} holds ${before_count} object(s); the restore target must be empty (pass --reset)"
fi

dr_log "mirroring ${DR_BUCKET_PRIMARY} into ${DR_BUCKET_REPLICA}"
dr_mc mirror --overwrite "dr/${DR_BUCKET_PRIMARY}/" "dr/${DR_BUCKET_REPLICA}/" \
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
