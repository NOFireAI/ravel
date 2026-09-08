#!/usr/bin/env bash
# Find cancelled ci runs on the head SHA of open PRs and rerun them.
# Dry run by default; -y applies.
#
# Cancelled is not failed, so nothing retries it, but a cancelled
# required check blocks auto-merge the same as a red one. Most come from
# the auto-merge/late-push concurrency race.
#
# Usage: ci-sweep-cancelled.sh [-y]
set -euo pipefail

apply=0
if [[ "${1:-}" == "-y" ]]; then
  apply=1
fi

open_prs=$(gh pr list --state open --json number,headRefName,headRefOid \
  --jq '.[] | "\(.number) \(.headRefName) \(.headRefOid)"')

if [[ -z "${open_prs}" ]]; then
  echo "No open PRs; nothing to sweep."
  exit 0
fi

found=0
while IFS= read -r row; do
  pr_num="${row%% *}"
  rest="${row#* }"
  head_branch="${rest%% *}"
  head_sha="${rest##* }"

  runs=$(gh run list --branch "${head_branch}" --limit 20 \
    --json databaseId,conclusion,headSha,workflowName \
    --jq ".[] | select(.conclusion == \"cancelled\" and .headSha == \"${head_sha}\") | \"\(.databaseId) \(.workflowName)\"" \
    2>/dev/null || true)
  [[ -z "${runs}" ]] && continue

  while IFS= read -r run_row; do
    run_id="${run_row%% *}"
    wf_name="${run_row#* }"
    found=1
    if [[ ${apply} -eq 1 ]]; then
      echo "PR #${pr_num}: rerunning cancelled '${wf_name}' run ${run_id}"
      # Cancelled jobs do not count as failed, so rerun the whole run.
      gh run rerun "${run_id}"
    else
      echo "PR #${pr_num}: cancelled '${wf_name}' run ${run_id} on ${head_sha:0:12} (dry run; -y to rerun)"
    fi
  done <<<"${runs}"
done <<<"${open_prs}"

if [[ ${found} -eq 0 ]]; then
  echo "No cancelled runs on open PR heads."
fi
