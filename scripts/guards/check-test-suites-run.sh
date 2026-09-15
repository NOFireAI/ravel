#!/usr/bin/env bash
# Every shell test suite must be named in a workflow, or be listed here as a
# deliberate exception with a reason.
#
# The two sides of this repository's test tree fail differently, and only one
# of them can develop an orphan:
#
#   Python:  `make test-python` runs `unittest discover -p 'test_*.py'`.
#            Discovery is by PATTERN, so adding a matching file enrols it. A
#            Python suite cannot be orphaned, and grepping a workflow for its
#            filename finds nothing and proves nothing.
#   Shell:   every suite is enumerated by hand in a workflow step. A new one
#            runs nowhere until somebody remembers, and nothing says so.
#
# That has now bitten three times, each time the same way: a suite that was
# green locally, had never executed in CI, and sat beside a real defect. The
# PreToolUse guard's 107 cases ran in no job; so did
# verify-dispatch-gates-with-gates.test.sh, next to a worktree-deletion bug;
# issue #1834 found five more. CLAUDE.md's own rule is that a test-hygiene
# rule which bites twice becomes a check rather than another paragraph.
#
# This is the enumeration side made visible. It does not run the suites; it
# asserts that something does.
#
# Usage: check-test-suites-run.sh [--list]
#   --list   print every suite and its state, not only the failures
#
# Exit codes:
#   0   every suite is run by a workflow, or knowingly excepted
#   1   at least one suite runs nowhere
#   2   the check could not be performed (no workflows dir, git failed)
set -uo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "check-test-suites-run.sh: not inside a git repository" >&2
  exit 2
}
cd "${repo_root}" || exit 2

workflow_dir=".github/workflows"
if [[ ! -d "${workflow_dir}" ]]; then
  echo "check-test-suites-run.sh: no ${workflow_dir}; cannot tell what CI runs" >&2
  exit 2
fi

list_all=0
[[ "${1:-}" == "--list" ]] && list_all=1

# Suites deliberately not run by any workflow. Each entry carries the reason
# inline, because an allowlist without one becomes a place to make failures
# quiet. Add to this only when a suite genuinely cannot run on a runner; a
# slow suite belongs in a nightly workflow instead, which this check counts.
#
# Format: "<path>|<reason>". Keep the path repo-relative, as git reports it.
excepted=(
  # (none today; the five from #1834 are wired into doc-scripts by the same
  # change that added this guard)
)

is_excepted() {
  local want="$1" entry
  for entry in ${excepted[@]+"${excepted[@]}"}; do
    [[ "${entry%%|*}" == "${want}" ]] && return 0
  done
  return 1
}

excepted_reason() {
  local want="$1" entry
  for entry in ${excepted[@]+"${excepted[@]}"}; do
    if [[ "${entry%%|*}" == "${want}" ]]; then
      printf '%s' "${entry#*|}"
      return 0
    fi
  done
  return 1
}

# Tracked files only: an untracked scratch suite in someone's worktree is not
# this check's business, and would fail it on every machine differently.
suites="$(git ls-files -- '*.test.sh' 2>/dev/null)" || {
  echo "check-test-suites-run.sh: git ls-files failed" >&2
  exit 2
}

if [[ -z "${suites}" ]]; then
  # Zero suites is not a pass. The glob changed, the tree moved, or this ran
  # somewhere unexpected -- all of which mean the check did not check.
  echo "check-test-suites-run.sh: found no *.test.sh files at all; refusing to report clean" >&2
  exit 2
fi

orphans=0
total=0
while IFS= read -r suite; do
  [[ -z "${suite}" ]] && continue
  total=$((total + 1))
  base="$(basename "${suite}")"
  # Named anywhere under the workflows dir. A basename is the right key: a
  # step may invoke it via `bash scripts/tests/x.test.sh` or a variable path,
  # and matching the full repo-relative path would miss the latter.
  if grep -rqF -- "${base}" "${workflow_dir}" 2>/dev/null; then
    ((list_all == 1)) && printf 'run       %s\n' "${suite}"
    continue
  fi
  if is_excepted "${suite}"; then
    ((list_all == 1)) && printf 'excepted  %s (%s)\n' "${suite}" "$(excepted_reason "${suite}")"
    continue
  fi
  printf 'ORPHAN    %s runs in no workflow\n' "${suite}"
  orphans=$((orphans + 1))
done <<<"${suites}"

if ((orphans > 0)); then
  echo
  echo "${orphans} of ${total} shell test suite(s) run in no CI job." >&2
  echo "A suite that runs nowhere is green from the day it lands and proves nothing." >&2
  echo "Add a step naming it (doc-scripts in ci.yml is where the bash-only suites" >&2
  echo "live), or add it to this script's 'excepted' list with a reason." >&2
  exit 1
fi

echo "check-test-suites-run.sh: ${total} shell test suite(s), all run by a workflow."
