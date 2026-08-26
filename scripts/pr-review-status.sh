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
  --json state,mergeStateStatus,statusCheckRollup)"
state="$(echo "${pr_json}" | jq -r '.state')"
merge_state="$(echo "${pr_json}" | jq -r '.mergeStateStatus')"

pending=$(echo "${pr_json}" | jq '[.statusCheckRollup[]? | select(.status=="IN_PROGRESS" or .status=="QUEUED" or .status=="PENDING")] | length')
success=$(echo "${pr_json}" | jq '[.statusCheckRollup[]? | select(.conclusion=="SUCCESS")] | length')
failing=$(echo "${pr_json}" | jq '[.statusCheckRollup[]? | select(.conclusion=="FAILURE" or .conclusion=="CANCELLED" or .conclusion=="TIMED_OUT")] | length')
failing_names=$(echo "${pr_json}" | jq -r '[.statusCheckRollup[]? | select(.conclusion=="FAILURE" or .conclusion=="CANCELLED" or .conclusion=="TIMED_OUT") | .name] | join(",")')

reviews_json="$(gh api "repos/${repo}/pulls/${pr}/reviews" --paginate 2>/dev/null || echo '[]')"
cr_reviews=$(echo "${reviews_json}" | jq '[.[] | select(.user.login=="coderabbitai[bot]")] | length')
cr_last_state=$(echo "${reviews_json}" | jq -r '[.[] | select(.user.login=="coderabbitai[bot]")] | last | .state // "none"')

# The REST review-comments endpoint carries no resolved/unresolved field
# (resolution is a review-THREAD concept, GraphQL-only) -- this reports the
# raw inline-comment count. A nonzero count needs a human/session read of
# `gh api repos/${repo}/pulls/${pr}/comments` to judge whether each finding
# was already fixed or answered; this script cannot tell that for you.
comments_json="$(gh api "repos/${repo}/pulls/${pr}/comments" --paginate 2>/dev/null || echo '[]')"
cr_comments=$(echo "${comments_json}" | jq '[.[] | select(.user.login=="coderabbitai[bot]")] | length')

summary="PR #${pr}: state=${state} mergeState=${merge_state} CI=${success} pass/${pending} pending/${failing} fail"
if [[ "${failing}" != "0" ]]; then
  summary="${summary} (${failing_names})"
fi
summary="${summary} | CodeRabbit: reviews=${cr_reviews} last=${cr_last_state} inline_comments=${cr_comments}"
echo "${summary}"

if [[ "${cr_reviews}" == "0" ]]; then
  echo "  -> no CodeRabbit review posted yet; do not merge until one lands"
elif [[ "${cr_comments}" != "0" ]]; then
  echo "  -> ${cr_comments} CodeRabbit inline comment(s); read them and confirm each is fixed or answered before merging"
elif [[ "${failing}" != "0" ]]; then
  echo "  -> CI has failing/cancelled checks; not clean to merge"
elif [[ "${pending}" != "0" ]]; then
  echo "  -> CI still running; wait"
else
  echo "  -> clean: CI green, CodeRabbit reviewed with zero inline comments (walkthrough-only counts as clean)"
fi
