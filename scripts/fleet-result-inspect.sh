#!/usr/bin/env bash
# Fetch a claude-fleet dispatched task's result branch and print what's on
# it, so the caller can review scope before merging (never trust an
# executor's own "gates green" claim; see the merge-fleet-result skill).
#
# Usage: fleet-result-inspect.sh <task-id>
#
# Prints: commits on the result branch not yet on main, and the diff stat
# vs main. Exits nonzero (from `git fetch`/`git ls-remote`) if the result
# ref does not exist: a `done` status with no result ref means the
# executor never committed and the work is gone; re-dispatch instead of
# retrying this script.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <task-id>" >&2
  exit 64
fi

task_id="$1"
result_ref="refs/heads/task/${task_id}/result"

echo "==> Verifying ${result_ref} exists on origin"
git ls-remote --exit-code origin "${result_ref}" >/dev/null

echo "==> Fetching ${result_ref}"
git fetch origin "${result_ref}"

echo "==> Commits on the result branch not yet on main:"
git log --oneline "main..FETCH_HEAD"

echo
echo "==> Diff scope vs main:"
git diff --stat "main...FETCH_HEAD"

echo
echo "Review the above. If scope matches what was dispatched, merge with:"
echo "  scripts/fleet-result-merge.sh ${task_id} <message-file>"
