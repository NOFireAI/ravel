#!/usr/bin/env bash
# One-call answer to "where are we at?" for a fleet-delivered epic.
#
# Reads the epic issue body (cached for 60 s), extracts every fleet task
# UUID in it, and reconciles each against the task refs on origin and the
# task-branch PRs. This is the pre-dispatch ledger reconciliation from
# CLAUDE.md as one command.
#
# Usage: epic-status.sh <epic-issue-number> [--fresh]
#   --fresh bypasses the cache.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <epic-issue-number> [--fresh]" >&2
  exit 64
fi

issue="$1"
fresh="${2:-}"

cache_dir="${TMPDIR:-/tmp}/ravel-epic-status"
mkdir -p "${cache_dir}"
cache_file="${cache_dir}/issue-${issue}.json"

now=$(date +%s)
use_cache=0
if [[ "${fresh}" != "--fresh" && -f "${cache_file}" ]]; then
  if [[ "$(uname)" == "Darwin" ]]; then
    mtime=$(stat -f %m "${cache_file}")
  else
    mtime=$(stat -c %Y "${cache_file}")
  fi
  if (( now - mtime < 60 )); then
    use_cache=1
  fi
fi

if [[ ${use_cache} -eq 0 ]]; then
  gh issue view "${issue}" --json number,title,state,body >"${cache_file}.tmp"
  mv "${cache_file}.tmp" "${cache_file}"
fi

title=$(jq -r .title "${cache_file}")
issue_state=$(jq -r .state "${cache_file}")
echo "== Epic #${issue}: ${title} [${issue_state}]"

task_ids=$(jq -r .body "${cache_file}" \
  | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' \
  | sort -u || true)

if [[ -z "${task_ids}" ]]; then
  echo "No fleet task ids found in the issue body."
else
  # One remote round-trip for all task refs; one API call for all task PRs.
  remote_refs=$(git ls-remote origin 'refs/heads/task/*' 2>/dev/null || true)
  pr_rows=$(gh pr list --state all --limit 100 \
    --json number,state,headRefName,mergedAt \
    --jq '.[] | select(.headRefName | startswith("task/")) | "\(.headRefName) #\(.number) \(.state)"' \
    2>/dev/null || true)

  printf '%-38s %-6s %-7s %s\n' "task" "start" "result" "merge PR"
  while IFS= read -r tid; do
    have_start="no"
    have_result="no"
    grep -q "refs/heads/task/${tid}/start$" <<<"${remote_refs}" && have_start="yes"
    grep -q "refs/heads/task/${tid}/result$" <<<"${remote_refs}" && have_result="yes"
    pr_info=$(grep "^task/${tid}/merge " <<<"${pr_rows}" | head -1 || true)
    pr_info="${pr_info#task/${tid}/merge }"
    [[ -z "${pr_info}" ]] && pr_info="-"
    printf '%-38s %-6s %-7s %s\n' "${tid}" "${have_start}" "${have_result}" "${pr_info}"
  done <<<"${task_ids}"

  echo
  echo "Legend: start=yes result=no PR=-  -> in flight or dead (check fleet_status"
  echo "        before a new dispatch); result=yes PR=-  -> ready to inspect/merge;"
  echo "        PR OPEN  -> waiting on checks or a stuck auto-merge."
fi

open_task_prs=$(gh pr list --state open --json number,title,headRefName \
  --jq '.[] | select(.headRefName | startswith("task/")) | "#\(.number) \(.headRefName) \(.title)"' \
  2>/dev/null || true)
if [[ -n "${open_task_prs}" ]]; then
  echo
  echo "== Open task PRs:"
  printf '%s\n' "${open_task_prs}"
fi
