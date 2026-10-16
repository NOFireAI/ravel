#!/usr/bin/env bash
# Format-bump release-note guard (issue #2725, ADR-2708 decision D5d): a
# change to a bulk-format version constant or reader window must ship a
# changelog fragment that names every signal that format carries, so an
# operator reading the release notes learns that every one of those signals
# crosses a format boundary.
#
# Usage:
#   scripts/guards/check-format-bump-release-note.sh <base-ref> [<head-ref>]
#
# <head-ref> defaults to HEAD. The range is <merge-base>..<head-ref>, where the
# merge base is that of <base-ref> and <head-ref>, as in
# check-changelog-touched.sh.
#
# Formats, their anchors, and the signals a fragment must name:
#
#   RLOG   crates/ravel-logseg/src/footer.rs  `pub const VERSION: u16` and
#                                             `pub const SUPPORTED_VERSIONS`
#          logs (or log), alerts (or alert), audit
#   RSPAN  crates/ravel-rspan/src/footer.rs   `pub const VERSION: u16` and
#                                             `pub const SUPPORTED_VERSIONS`
#          spans (or span)
#   RSEG   crates/ravel-segment/src/format.rs `SegmentVersion::WINDOW`
#          metrics (or metric)
#
# RSEG's `VERSION` constant pins the retired v1 number and never moves, so its
# anchor is the reader window, which the PR adding a new RSEG version edits.
# For RLOG and RSPAN the reader window (`SUPPORTED_VERSIONS`) is watched beside
# `VERSION`, because dropping an old reader changes the window and not the
# version, and makes stored objects of that version unreadable just the same.
#
# A format counts as bumped when its anchor's VALUE differs between the merge
# base and head; an edit elsewhere in the same file does not count. Fragments
# are changelog.d/ files named like fragments (changelog.d/README.md) that the
# range adds or edits, read at head and taken together. Signals match
# case-insensitively as whole words, after code spans (`...`) and URLs or
# paths (a token containing "://" or starting with "/") are removed, so
# `ravel-cli audit-versions` or /v1/logs does not count as naming a signal.
#
# Exit 0: no anchor value changed, or every bumped format's signals are named.
# Exit 1: a bumped format has a signal no fragment names; each is printed.
# Exit 2: the question could not be answered (missing argument, a ref git
#         cannot resolve, a git command failing, or an anchor missing or
#         ambiguous at either end). Could-not-check is never a pass.
set -uo pipefail

me="check-format-bump-release-note.sh"

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 2

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  awk 'NR > 1 && !/^#/ { exit } NR > 1' "$0"
  exit 0
fi

base_ref="${1:-}"
head_ref="${2:-HEAD}"

if [[ -z "${base_ref}" ]]; then
  echo "${me}: missing <base-ref>" >&2
  exit 2
fi

base_sha="$(git rev-parse --verify "${base_ref}^{commit}" 2>/dev/null)" || {
  echo "${me}: cannot resolve base ref: ${base_ref}" >&2
  exit 2
}
head_sha="$(git rev-parse --verify "${head_ref}^{commit}" 2>/dev/null)" || {
  echo "${me}: cannot resolve head ref: ${head_ref}" >&2
  exit 2
}

# Pass a pull request's branch tip as head, not its refs/pull/N/merge commit;
# check-changelog-touched.sh explains why.
merge_base="$(git merge-base "${base_sha}" "${head_sha}" 2>/dev/null)" || {
  echo "${me}: no merge base for ${base_sha} and ${head_sha}" >&2
  exit 2
}
if [[ -z "${merge_base}" ]]; then
  echo "${me}: empty merge base for ${base_sha} and ${head_sha}" >&2
  exit 2
fi
range="${merge_base}..${head_sha}"

# format|file|anchor regex (one capture group: the value)|signal alternatives
formats=(
  'RLOG|crates/ravel-logseg/src/footer.rs|^pub const VERSION: u16 = ([0-9]+);|logs,log alerts,alert audit'
  'RLOG|crates/ravel-logseg/src/footer.rs|^pub const SUPPORTED_VERSIONS: SupportedVersions = (.*);|logs,log alerts,alert audit'
  'RSPAN|crates/ravel-rspan/src/footer.rs|^pub const VERSION: u16 = ([0-9]+);|spans,span'
  'RSPAN|crates/ravel-rspan/src/footer.rs|^pub const SUPPORTED_VERSIONS: SupportedVersions = (.*);|spans,span'
  "RSEG|crates/ravel-segment/src/format.rs|^[[:space:]]*pub const WINDOW: &'static \\[SegmentVersion\\] = (.*);|metrics,metric"
)

# anchor_value <commit> <file> <regex>: prints the single captured value, or
# fails when the file is absent or the anchor matches zero or several lines.
anchor_value() {
  local commit="$1" file="$2" re="$3" content line found=0 value=""
  content="$(git show "${commit}:${file}" 2>/dev/null)" || return 1
  while IFS= read -r line; do
    if [[ "${line}" =~ ${re} ]]; then
      found=$((found + 1))
      value="${BASH_REMATCH[1]}"
    fi
  done <<<"${content}"
  [[ "${found}" -eq 1 ]] || return 1
  printf '%s\n' "${value}"
}

bumped=()
for entry in "${formats[@]}"; do
  IFS='|' read -r name file re signals <<<"${entry}"
  old="$(anchor_value "${merge_base}" "${file}" "${re}")" || {
    echo "${me}: ${name} version anchor not found exactly once in ${file} at ${merge_base:0:12}" >&2
    exit 2
  }
  new="$(anchor_value "${head_sha}" "${file}" "${re}")" || {
    echo "${me}: ${name} version anchor not found exactly once in ${file} at ${head_sha:0:12}" >&2
    exit 2
  }
  if [[ "${old}" != "${new}" ]]; then
    echo "${me}: ${name} format version changed in ${range}: ${old} -> ${new}"
    already=0
    for b in "${bumped[@]+"${bumped[@]}"}"; do
      [[ "${b%%|*}" == "${name}" ]] && already=1
    done
    [[ "${already}" -eq 1 ]] || bumped+=("${name}|${signals}")
  fi
done

if [[ "${#bumped[@]}" -eq 0 ]]; then
  echo "${me}: clean (no format version constant changed in ${range})"
  exit 0
fi

if ! changed_fragments="$(git diff --name-status -M --diff-filter=AMR "${range}" -- \
    changelog.d/ 2>/dev/null)"; then
  echo "${me}: git diff failed for ${range}" >&2
  exit 2
fi
fragment_re='^changelog\.d/[1-9][0-9]*(-([2-9]|[1-9][0-9]+))?\.(added|changed|removed|fixed|security)\.md$'
notes=""
while IFS=$'\t' read -r change first second; do
  case "${change}" in
    A | M) f="${first}" ;;
    R100) continue ;;
    R*) f="${second}" ;;
    *) continue ;;
  esac
  [[ "${f}" =~ ${fragment_re} ]] || continue
  body="$(git show "${head_sha}:${f}" 2>/dev/null)" || {
    echo "${me}: cannot read ${f} at ${head_sha:0:12}" >&2
    exit 2
  }
  notes+="${body}"$'\n'
done <<<"${changed_fragments}"

# Drop code spans, then URLs and paths, so a command or endpoint name is not
# read as naming a signal.
# shellcheck disable=SC2016 # the backticks are literal: they delimit code spans
prose="$(printf '%s' "${notes}" | sed -E -e 's/`[^`]*`/ /g' -e 's#[^[:space:]]*://[^[:space:]]*# #g' -e 's#(^|[[:space:](])/[^[:space:]]*#\1 #g')"

missing_any=0
for entry in "${bumped[@]}"; do
  IFS='|' read -r name signals <<<"${entry}"
  for signal in ${signals}; do
    alternatives="${signal//,/|}"
    if ! grep -qiwE "(${alternatives})" <<<"${prose}"; then
      echo "${me}: ${name} bump: no changelog fragment in ${range} names the signal '${signal%%,*}'" >&2
      missing_any=1
    fi
  done
done

if [[ "${missing_any}" -eq 1 ]]; then
  echo "  Fix: add or edit a fragment changelog.d/<issue>.<section>.md (see" >&2
  echo "  changelog.d/README.md) whose text names every signal listed above." >&2
  exit 1
fi

echo "${me}: clean (fragments in ${range} name every signal of each bumped format)"
exit 0
