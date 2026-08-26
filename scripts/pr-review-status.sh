#!/usr/bin/env bash
# One-line status for a Ravel PR under the wait-for-CodeRabbit-then-merge-
# by-hand rule (2026-08-26): mergeStateStatus, the CI check rollup, and the
# coderabbitai[bot] review/finding counts, in one call instead of the three
# or four `gh` invocations every session was hand-rolling.
#
# Usage: pr-review-status.sh <pr-number> [--confirm-addressed]
#
# --confirm-addressed: the operator's explicit statement that every
# CodeRabbit inline comment on the PR has been read and each one fixed or
# answered. The REST API has no resolved/unresolved field (see below), so
# once a PR has ever had a finding its comment count never returns to zero
# and the clean branch below could otherwise never fire again (issue #764:
# PR #754 had 13 addressed comments across 4 fix rounds and no way to get
# the SHA-pinned merge command). The flag skips ONLY the comment-count
# conjunct; CI, the current-head review, and the mergeState checks still
# gate exactly as without it.
set -euo pipefail

pr="${1:?usage: pr-review-status.sh <pr-number> [--confirm-addressed]}"
repo="NOFireAI/ravel"
confirm_addressed=0
if [[ "${2:-}" == "--confirm-addressed" ]]; then
  confirm_addressed=1
elif [[ -n "${2:-}" ]]; then
  echo "usage: pr-review-status.sh <pr-number> [--confirm-addressed]" >&2
  exit 2
fi

pr_json="$(gh pr view "${pr}" --repo "${repo}" \
  --json state,mergeStateStatus,statusCheckRollup,headRefOid)"
state="$(echo "${pr_json}" | jq -r '.state')"
merge_state="$(echo "${pr_json}" | jq -r '.mergeStateStatus')"
head_sha="$(echo "${pr_json}" | jq -r '.headRefOid')"

# `statusCheckRollup` can mix two shapes: a `CheckRun` (GitHub Actions and
# most modern integrations -- `status`/`conclusion`, name in `.name`) and a
# legacy `StatusContext` (the older commit-status API some third-party
# integrations still use -- `state` only, name in `.context`). Classify each
# entry once, by shape, into one bucket, so neither shape nor an unrecognized
# value inside a recognized shape can silently vanish from every count.
normalized="$(echo "${pr_json}" | jq '
  [.statusCheckRollup[]? | {
    name: (.name // .context // "unknown"),
    class: (
      if has("state") then
        (if .state=="SUCCESS" then "success"
         elif (.state=="PENDING" or .state=="EXPECTED") then "pending"
         elif (.state=="FAILURE" or .state=="ERROR") then "failing"
         else "other" end)
      elif has("status") then
        (if .status!="COMPLETED" then "pending"
         elif (.conclusion=="SUCCESS" or .conclusion=="NEUTRAL" or .conclusion=="SKIPPED") then "success"
         elif (.conclusion=="FAILURE" or .conclusion=="CANCELLED" or .conclusion=="TIMED_OUT") then "failing"
         else "other" end)
      else "other" end
    )
  }]')"
pending=$(echo "${normalized}" | jq '[.[] | select(.class=="pending")] | length')
success=$(echo "${normalized}" | jq '[.[] | select(.class=="success")] | length')
failing=$(echo "${normalized}" | jq '[.[] | select(.class=="failing")] | length')
failing_names=$(echo "${normalized}" | jq -r '[.[] | select(.class=="failing") | .name] | join(",")')
# Every check must land in success/pending/failing above (an ACTION_REQUIRED
# or STALE conclusion, an unrecognized state value, or a shape this script
# has never seen) before CI counts as settled; this catches whatever falls
# through all three.
other=$(echo "${normalized}" | jq '[.[] | select(.class=="other")] | length')

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

summary="PR #${pr} @ ${head_sha}: state=${state} mergeState=${merge_state} CI=${success} pass/${pending} pending/${failing} fail"
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
elif [[ "${cr_last_state}" != "APPROVED" && "${cr_last_state}" != "COMMENTED" ]]; then
  echo "  -> CodeRabbit's current-head review state is ${cr_last_state} (need APPROVED or COMMENTED); not clean"
elif [[ "${failing}" != "0" ]]; then
  echo "  -> CI has failing/cancelled checks; not clean to merge"
elif [[ "${other}" != "0" ]]; then
  echo "  -> CI has ${other} check(s) in an unrecognized state; verify by hand before merging"
elif [[ "${pending}" != "0" ]]; then
  echo "  -> CI still running; wait"
elif [[ "${cr_comments}" != "0" && "${confirm_addressed}" != "1" ]]; then
  echo "  -> ${cr_comments} CodeRabbit inline comment(s); read them, then re-run with --confirm-addressed once each is fixed or answered (the API cannot tell; see the header comment)"
elif [[ "${merge_state}" != "CLEAN" && "${merge_state}" != "UNSTABLE" ]]; then
  echo "  -> every check and review looks clean, but mergeState is ${merge_state} (not CLEAN/UNSTABLE); verify by hand before merging"
else
  if [[ "${cr_comments}" != "0" ]]; then
    echo "  -> clean (operator confirmed all ${cr_comments} inline comment(s) addressed): CI green, CodeRabbit reviewed the current head"
  else
    echo "  -> clean: CI green, CodeRabbit reviewed the current head with zero inline comments"
  fi
  echo "  -> gh pr merge ${pr} --rebase --delete-branch --match-head-commit ${head_sha}"
fi
