#!/usr/bin/env bash
# Fetch a claude-fleet dispatched task's result branch and print what's on
# it, so the caller can review scope before merging (never trust an
# executor's own "gates green" claim; see the merge-fleet-result skill).
#
# Usage: fleet-result-inspect.sh <task-id> [base-ref]
#
# base-ref defaults to origin/main. No session working in this repo checks
# out or advances local `main`, so it sits wherever it was when the clone
# or the session started; diffing against it silently mixes in every commit
# another session has landed since, and presents a correctly-scoped result
# as far larger than it is (or hides a real scope violation inside that
# noise). This script always fetches origin/main itself before comparing,
# so the base is never whatever local main happened to be.
#
# Prints: commits on the result branch not yet on the base, and the diff
# stat vs the base. Exits nonzero (from `git fetch`/`git ls-remote`) if the
# result ref does not exist: a `done` status with no result ref means the
# executor never committed and the work is gone; re-dispatch instead of
# retrying this script.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <task-id> [base-ref]" >&2
  exit 64
fi

task_id="$1"
base_ref="${2:-origin/main}"
result_ref="refs/heads/task/${task_id}/result"

echo "==> Verifying ${result_ref} exists on origin"
git ls-remote --exit-code origin "${result_ref}" >/dev/null

echo "==> Fetching origin/main"
git fetch -q origin main

echo "==> Fetching ${result_ref}"
git fetch origin "${result_ref}"
result_sha="$(git rev-parse FETCH_HEAD)"

echo "==> Commits on the result branch not yet on ${base_ref}:"
git log --oneline "${base_ref}..${result_sha}"

echo
echo "==> Diff scope vs ${base_ref}:"
git diff --stat "${base_ref}...${result_sha}"

echo
echo "Review the above. If scope matches what was dispatched, merge with:"
echo "  scripts/fleet-result-merge.sh ${task_id} <message-file>"
