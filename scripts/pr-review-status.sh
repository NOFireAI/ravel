#!/usr/bin/env bash
# One-line status for a Ravel PR under the wait-for-CodeRabbit-then-merge-
# by-hand rule (2026-08-26): mergeStateStatus, the CI check rollup, and the
# coderabbitai[bot] review/finding counts, in one call instead of the three
# or four `gh` invocations every session was hand-rolling.
#
# Usage: pr-review-status.sh <pr-number>
set -euo pipefail

pr="${1:?usage: pr-review-status.sh <pr-number>}"
repo="NOFireAI/ravel"

pr_json="$(gh pr view "${pr}" --repo "${repo}" \
  --json state,mergeStateStatus,statusCheckRollup,headRefOid)"
state="$(echo "${pr_json}" | jq -r '.state')"
merge_state="$(echo "${pr_json}" | jq -r '.mergeStateStatus')"
head_sha="$(echo "${pr_json}" | jq -r '.headRefOid')"

pending=$(echo "${pr_json}" | jq '[.statusCheckRollup[]? | select(.status=="IN_PROGRESS" or .status=="QUEUED" or .status=="PENDING")] | length')
success=$(echo "${pr_json}" | jq '[.statusCheckRollup[]? | select(.conclusion=="SUCCESS")] | length')
failing=$(echo "${pr_json}" | jq '[.statusCheckRollup[]? | select(.conclusion=="FAILURE" or .conclusion=="CANCELLED" or .conclusion=="TIMED_OUT")] | length')
failing_names=$(echo "${pr_json}" | jq -r '[.statusCheckRollup[]? | select(.conclusion=="FAILURE" or .conclusion=="CANCELLED" or .conclusion=="TIMED_OUT") | .name] | join(",")')
# Every check must resolve to a conclusion this script recognizes as
# accepted (SUCCESS, or NEUTRAL/SKIPPED, which GitHub also treats as
# passing) before CI counts as settled; a status this script has never
# seen must not silently fall out of both the success and failing counts.
other=$(echo "${pr_json}" | jq '[.statusCheckRollup[]? | select(.status=="COMPLETED" and (.conclusion|IN("SUCCESS","FAILURE","CANCELLED","TIMED_OUT","NEUTRAL","SKIPPED")|not))] | length')

# `--paginate` on an array-returning endpoint writes one JSON array per page,
# back to back, not one combined array -- `jq -s add` slurps every page into
# a single flat array regardless of how many pages came back. No `|| echo
# '[]'` fallback: a real gh api failure (auth, rate limit, network) must
# abort the script (set -e), not be reported as "zero reviews found", which
# would read as "not reviewed yet, don't merge" instead of "the check itself
# didn't run".
reviews_json="$(gh api "repos/${repo}/pulls/${pr}/reviews" --paginate | jq -s 'add')"
# Only a review against the PR's CURRENT head commit counts: a stale review
# from before the last push covers code that no longer exists on the branch.
cr_reviews=$(echo "${reviews_json}" | jq --arg sha "${head_sha}" \
  '[.[] | select(.user.login=="coderabbitai[bot]" and .commit_id==$sha)] | length')
cr_last_state=$(echo "${reviews_json}" | jq -r --arg sha "${head_sha}" \
  '[.[] | select(.user.login=="coderabbitai[bot]" and .commit_id==$sha)] | last | .state // "none"')

# The REST review-comments endpoint carries no resolved/unresolved field
# (resolution is a review-THREAD concept, GraphQL-only) -- this reports the
# raw inline-comment count. A nonzero count needs a human/session read of
# `gh api repos/${repo}/pulls/${pr}/comments` to judge whether each finding
# was already fixed or answered; this script cannot tell that for you. A
# comment's own count never drops to zero just because the code it flagged
# changed, so "clean" below means CI green plus a current-head review, not
# zero comments -- see the merge-fleet-result skill.
comments_json="$(gh api "repos/${repo}/pulls/${pr}/comments" --paginate | jq -s 'add')"
cr_comments=$(echo "${comments_json}" | jq '[.[] | select(.user.login=="coderabbitai[bot]")] | length')

summary="PR #${pr}: state=${state} mergeState=${merge_state} CI=${success} pass/${pending} pending/${failing} fail"
if [[ "${other}" != "0" ]]; then
  summary="${summary}/${other} unrecognized"
fi
if [[ "${failing}" != "0" ]]; then
  summary="${summary} (${failing_names})"
fi
summary="${summary} | CodeRabbit: reviews@head=${cr_reviews} last=${cr_last_state} inline_comments=${cr_comments}"
echo "${summary}"

if [[ "${state}" != "OPEN" ]]; then
  echo "  -> PR is ${state}, not open; nothing to merge"
elif [[ "${merge_state}" == "DIRTY" || "${merge_state}" == "DRAFT" || "${merge_state}" == "BEHIND" ]]; then
  echo "  -> mergeState is ${merge_state}; not mergeable regardless of CI/CodeRabbit state below"
elif [[ "${cr_reviews}" == "0" ]]; then
  echo "  -> no CodeRabbit review against the current head commit yet; do not merge until one lands"
elif [[ "${failing}" != "0" ]]; then
  echo "  -> CI has failing/cancelled checks; not clean to merge"
elif [[ "${other}" != "0" ]]; then
  echo "  -> CI has ${other} check(s) in an unrecognized state; verify by hand before merging"
elif [[ "${pending}" != "0" ]]; then
  echo "  -> CI still running; wait"
elif [[ "${cr_comments}" != "0" ]]; then
  echo "  -> ${cr_comments} CodeRabbit inline comment(s); read them and confirm each is fixed or answered before merging"
elif [[ "${merge_state}" != "CLEAN" && "${merge_state}" != "UNSTABLE" ]]; then
  echo "  -> every check and review looks clean, but mergeState is ${merge_state} (not CLEAN/UNSTABLE); verify by hand before merging"
else
  echo "  -> clean: CI green, CodeRabbit reviewed the current head with zero inline comments"
fi
