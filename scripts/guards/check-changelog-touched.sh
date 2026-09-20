#!/usr/bin/env bash
# Changelog guard (issue #1719): a pull request whose feat/fix commits touch
# crates/ or services/ must also touch CHANGELOG.md somewhere in the same
# range, or carry an explicit opt-out trailer.
#
# Usage:
#   scripts/guards/check-changelog-touched.sh <base-ref> [<head-ref>]
#
# <head-ref> defaults to HEAD. The range checked is <base-ref>..<head-ref>
# (base excluded, head included), the same range git log/git diff use for a
# pull request's base..head.
#
# A commit qualifies when its own subject line starts with a `feat` or `fix`
# conventional-commit type (with an optional `(scope)` and an optional `!`)
# AND its own diff touches a path under crates/ or services/. Whether the
# range as a whole is exempt is decided over the WHOLE range, not per commit:
#
#   - CHANGELOG.md changed anywhere between base and head, or
#   - some commit in the range carries a trailer line reading exactly
#     `Changelog: none`
#
# Exit 0: no qualifying commit, or the range is exempt by either rule above.
# Exit 1: a qualifying commit exists and neither exemption applies. Findings
#         print the exact fix: add a CHANGELOG.md entry, or add the
#         `Changelog: none` trailer to the commit.
# Exit 2: the question could not be answered (missing argument, a ref git
#         cannot resolve, or a git command failing outright). Could-not-check
#         is never reported as a pass.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 2

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,29p' "$0"
  exit 0
fi

base_ref="${1:-}"
head_ref="${2:-HEAD}"

if [[ -z "${base_ref}" ]]; then
  echo "check-changelog-touched.sh: missing <base-ref>" >&2
  exit 2
fi

base_sha="$(git rev-parse --verify "${base_ref}^{commit}" 2>/dev/null)" || {
  echo "check-changelog-touched.sh: cannot resolve base ref: ${base_ref}" >&2
  exit 2
}
head_sha="$(git rev-parse --verify "${head_ref}^{commit}" 2>/dev/null)" || {
  echo "check-changelog-touched.sh: cannot resolve head ref: ${head_ref}" >&2
  exit 2
}

range="${base_sha}..${head_sha}"

commits="$(git rev-list "${range}" 2>/dev/null)" || {
  echo "check-changelog-touched.sh: git rev-list failed for ${range}" >&2
  exit 2
}

changelog_touched=0
if ! changed_changelog="$(git diff --name-only "${range}" -- CHANGELOG.md 2>/dev/null)"; then
  echo "check-changelog-touched.sh: git diff failed for ${range}" >&2
  exit 2
fi
[[ -n "${changed_changelog}" ]] && changelog_touched=1

trailer_present=0
if ! messages="$(git log --format=%B "${range}" 2>/dev/null)"; then
  echo "check-changelog-touched.sh: git log failed for ${range}" >&2
  exit 2
fi
if printf '%s\n' "${messages}" | grep -qE '^Changelog: none[[:space:]]*$'; then
  trailer_present=1
fi

qualifying_sha=""
qualifying_subject=""
conventional_type_re='^(feat|fix)(\([^)]*\))?!?:'
while IFS= read -r sha; do
  [[ -z "${sha}" ]] && continue
  subject="$(git log -1 --format=%s "${sha}")"
  if [[ ! "${subject}" =~ ${conventional_type_re} ]]; then
    continue
  fi
  if git diff-tree --no-commit-id --name-only -r "${sha}" 2>/dev/null \
      | grep -qE '^(crates|services)/'; then
    qualifying_sha="${sha}"
    qualifying_subject="${subject}"
    break
  fi
done <<<"${commits}"

if [[ -z "${qualifying_sha}" ]]; then
  echo "check-changelog-touched.sh: clean (no feat/fix commit in ${range} touches crates/ or services/)"
  exit 0
fi

if [[ "${changelog_touched}" -eq 1 ]]; then
  echo "check-changelog-touched.sh: clean (CHANGELOG.md touched in ${range})"
  exit 0
fi

if [[ "${trailer_present}" -eq 1 ]]; then
  echo "check-changelog-touched.sh: clean (a commit in ${range} carries the 'Changelog: none' trailer)"
  exit 0
fi

echo "check-changelog-touched.sh: ${qualifying_sha:0:12} (${qualifying_subject}) touches crates/ or services/ but CHANGELOG.md is untouched in ${range}" >&2
echo "  Fix: add a CHANGELOG.md entry under [Unreleased] describing this change," >&2
echo "  or add a trailer reading exactly 'Changelog: none' to the commit if none is needed." >&2
exit 1
