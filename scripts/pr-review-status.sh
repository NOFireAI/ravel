#!/usr/bin/env bash
# One-line status for a Ravel PR under the wait-for-the-review-then-merge-
# by-hand rule: mergeStateStatus, the CI check rollup, and the state of the
# fleet review, in one call instead of the three or four `gh` invocations
# every session was hand-rolling.
#
# The review is requested by commenting `@claude-fleet review` on the PR and
# arrives as a review from `claude-fleet[bot]` (ADR-1586). That bot posts one
# review object per task and pins it to the commit it read, so freshness here
# is `review.commit_id == headRefOid` and nothing looser.
#
# The bot never approves and never requests changes, by design: a person
# decides that. So COMMENTED is the success state, and "not approved" must
# never be read as "not clean".
#
# Usage: pr-review-status.sh <pr-number> [--confirm-addressed]
#
# --confirm-addressed: the operator's explicit statement that every finding on
# the PR has been read and each one fixed or answered, both the inline
# comments and the outside-diff findings in the review body. The REST API has
# no resolved/unresolved field (see below), so once a PR has ever had a
# finding its comment count never returns to zero and the clean branch below
# could otherwise never fire again (issue #764: PR #754 had 13 addressed
# comments across 4 fix rounds and no way to get the SHA-pinned merge
# command). The flag skips ONLY the finding-count conjuncts; CI, the
# current-head review, and the mergeState checks still gate exactly as
# without it.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The review bot's GitHub login. One place, because three checks below key on
# it and a typo in one of them reads as "no review yet" -- a false block, but
# an indefinite one.
bot="claude-fleet[bot]"

# Runs the merge-base guard and keeps both halves of its answer. It is a
# function so the exit code survives: `$?` read after an `if`/`elif` reports the
# test, not the command. The guard asks git about whatever repository the cwd
# belongs to while every other check here is pinned to ${repo} by `gh --repo`,
# so it runs with the cwd pinned to this script's own checkout; otherwise the
# two halves of the verdict can be about different repositories.
#
# The `cd` failing is its own outcome, not a stale base: left bare in the `&&`
# it would exit 1, which is the code reserved for "behind", and the operator
# would be told to rebase over a guard that never ran.
guard_rc=0
guard_out=""
merge_base_guard() {
  guard_rc=0
  guard_out="$( { { cd "${script_dir}/.." || exit 2; } && \
    "${script_dir}/guards/assert-fresh-merge-base.sh" "${pr}"; } 2>&1)" || guard_rc=$?
  return "${guard_rc}"
}

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
         elif .conclusion=="SKIPPED" then "skipped"
         elif (.conclusion=="SUCCESS" or .conclusion=="NEUTRAL") then "success"
         elif (.conclusion=="FAILURE" or .conclusion=="CANCELLED" or .conclusion=="TIMED_OUT") then "failing"
         else "other" end)
      else "other" end
    )
  }]')"
pending=$(echo "${normalized}" | jq '[.[] | select(.class=="pending")] | length')
success=$(echo "${normalized}" | jq '[.[] | select(.class=="success")] | length')
failing=$(echo "${normalized}" | jq '[.[] | select(.class=="failing")] | length')
failing_names=$(echo "${normalized}" | jq -r '[.[] | select(.class=="failing") | .name] | join(",")')
# A skipped check is not a failure and does not block a merge -- GitHub's own
# ruleset treats a path-filtered required check as satisfied -- but it is not
# a pass either, and folding it into the pass count reports "19 pass" for a
# pull request on which nothing ran at all. Counted on its own so the line
# says which it was.
skipped=$(echo "${normalized}" | jq '[.[] | select(.class=="skipped")] | length')
# Every check must land in success/skipped/pending/failing above (an ACTION_REQUIRED
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
# The bot sets commit_id once, when it posts, so this is a real freshness
# test rather than the count-the-objects guess it replaces.
reviews_at_head=$(echo "${reviews_json}" | jq --arg sha "${head_sha}" --arg bot "${bot}" \
  '[.[] | select(.user.login==$bot and .commit_id==$sha)] | length')
review_last_state=$(echo "${reviews_json}" | jq -r --arg sha "${head_sha}" --arg bot "${bot}" \
  '[.[] | select(.user.login==$bot and .commit_id==$sha)] | last | .state // "none"')
# Reviews the bot posted for some OTHER commit. Reported, never gating: it is
# the difference between "nobody has reviewed this branch" and "the head moved
# after the last review", and the operator's next action differs.
reviews_stale=$(echo "${reviews_json}" | jq --arg sha "${head_sha}" --arg bot "${bot}" \
  '[.[] | select(.user.login==$bot and .commit_id!=$sha)] | length')

issue_comments_json="$(gh api "repos/${repo}/issues/${pr}/comments" --paginate | jq -s 'add')"
# The bot's task comment for the head commit, as one word: none, running,
# done, dead, or unknown. Without it, "no review at head" collapses five
# different situations into one, and two of them need opposite actions: a
# queued task needs waiting, a failed one needs re-triggering and will
# otherwise never arrive.
task_state=$(echo "${issue_comments_json}" | jq -r --arg sha "${head_sha}" --arg bot "${bot}" \
  -f "${script_dir}/lib/fleet-review-task-state.jq")
# Did anyone ask? A trigger with no task comment is its own diagnosis: the
# mention was malformed (the bot answers an unrecognized argument with a
# confused reaction and nothing else), or the app is not installed here.
# Counted from non-bot comments so the bot quoting the trigger cannot pass
# for someone asking for a review.
triggers=$(echo "${issue_comments_json}" | jq --arg bot "${bot}" \
  '[.[] | select(.user.login!=$bot) | select((.body // "") | test("@claude-fleet[[:space:]]+review"))] | length')

# The REST review-comments endpoint carries no resolved/unresolved field
# (resolution is a review-THREAD concept, GraphQL-only) -- this reports the
# raw inline-comment count. A nonzero count needs a human/session read of
# `gh api repos/${repo}/pulls/${pr}/comments` to judge whether each finding
# was already fixed or answered; this script cannot tell that for you. A
# comment's own count never drops to zero just because the code it flagged
# changed, so "clean" below means CI green plus a current-head review, not
# zero comments -- see the merge-fleet-result skill and issue #1579.
comments_json="$(gh api "repos/${repo}/pulls/${pr}/comments" --paginate | jq -s 'add')"
inline_comments=$(echo "${comments_json}" | jq --arg bot "${bot}" \
  '[.[] | select(.user.login==$bot)] | length')

# A finding whose line GitHub will not accept an inline comment on goes into
# the review BODY instead, under a "Findings outside the diff:" heading. It
# leaves no entry on the review-comments endpoint above, so nothing read so
# far can see it. On #908 the two inline comments were pinned to a superseded
# commit and had already been fixed, so `inline_comments=2` read as "two stale
# findings, nothing new" while an unaddressed body finding sat at the current
# head -- the exact shape this script exists to prevent.
outside_diff=$(echo "${reviews_json}" | jq --arg sha "${head_sha}" --arg bot "${bot}" \
  -f "${script_dir}/lib/fleet-review-outside-diff.jq")

# "CI green" is a claim about checks that RAN, so a rollup of nothing but
# skips should not make it. This branch is DEFENSIVE, not a case this
# repository currently reaches: no workflow here carries an `on.*.paths`
# filter, and ci.yml's `changes` job has neither `needs:` nor `if:`, so it and
# the other ungated lanes run and pass on every pull request. A docs-only PR
# therefore reads 4 pass / 15 skipped, not 0 / 19, and "CI green" is honest on
# it. What was wrong there was only the COUNT, which said 19 pass. Keep this
# branch anyway: it costs three lines and it is what stops the phrase lying if
# a path filter is ever added, which is the change that would make it
# reachable.
ci_phrase="CI green"
if [[ "${success}" == "0" && "${skipped}" != "0" ]]; then
  ci_phrase="every check skipped, nothing ran"
fi
summary="PR #${pr} @ ${head_sha}: state=${state} mergeState=${merge_state} CI=${success} pass/${pending} pending/${failing} fail"
if [[ "${skipped}" != "0" ]]; then
  summary="${summary}/${skipped} skipped"
fi
if [[ "${other}" != "0" ]]; then
  summary="${summary}/${other} unrecognized"
fi
if [[ "${failing}" != "0" ]]; then
  summary="${summary} (${failing_names})"
fi
summary="${summary} | review: task@head=${task_state} reviews@head=${reviews_at_head} last=${review_last_state} inline_comments=${inline_comments}"
if [[ "${reviews_stale}" != "0" ]]; then
  summary="${summary} reviews_at_older_commits=${reviews_stale}"
fi
# Appended only when nonzero, so a PR with no body findings prints exactly the
# line it printed before this field existed. The name says body_findings rather
# than anything with "comment" in it: these are not inline comments and are not
# fetched from the comments endpoint, and an operator who reads the two counts
# as one number is back to the #908 failure.
if [[ "${outside_diff}" != "0" ]]; then
  summary="${summary} outside_diff_body_findings@head=${outside_diff}"
fi
echo "${summary}"

if [[ "${state}" != "OPEN" ]]; then
  echo "  -> PR is ${state}, not open; nothing to merge"
elif [[ "${merge_state}" == "DIRTY" || "${merge_state}" == "DRAFT" || "${merge_state}" == "BEHIND" ]]; then
  echo "  -> mergeState is ${merge_state}; not mergeable regardless of CI/review state below"
elif [[ "${reviews_at_head}" == "0" ]]; then # PROVE-FLIP
  # Flipping this one condition off clears a PR whose head has no review at
  # all; the test file sed-flips it and asserts the flipped script clears the
  # unreviewed fixture, which is what proves the condition carries the gate.
  #
  # Five states, and they are not variations on "wait": a dead task never
  # arrives, a missing trigger means nobody asked, and an unknown status means
  # this script cannot say. Collapsing them is the failure mode this branch
  # exists to prevent, in both directions -- an indefinite wait, or a merge
  # with nothing reviewed.
  case "${task_state}" in
    none)
      if [[ "${triggers}" == "0" ]]; then
        echo "  -> no review at head and nobody asked for one: comment \`@claude-fleet review\` on the PR (that exact body, arguments after \`review\` are parsed and an unrecognized word gets a confused reaction and no review)"
      else
        echo "  -> ${triggers} \`@claude-fleet review\` comment(s) but no task comment for ${head_sha}: if the last one is seconds old, the task comment lands within seconds, so re-run this; otherwise the mention was malformed (check for a confused reaction on it), the app is not installed here, or the bot is not receiving deliveries"
      fi
      ;;
    running)
      echo "  -> review task for ${head_sha} is queued or running; wait (the bot edits its task comment in place, and posts the review when the task finishes)"
      ;;
    dead)
      echo "  -> review task for ${head_sha} went terminal with NO review posted; nothing will arrive, so re-trigger with \`@claude-fleet review\` (read the task comment for the failure class: \`gh api repos/${repo}/issues/${pr}/comments --jq '.[] | select(.user.login==\"${bot}\") | .body'\`)"
      ;;
    done)
      echo "  -> the task comment for ${head_sha} says the review was posted, but no review object at that commit is visible; check by hand before merging"
      ;;
    *)
      echo "  -> review task for ${head_sha} is in an unrecognized state (${task_state}); read its task comment by hand before merging"
      ;;
  esac
  if [[ "${reviews_stale}" != "0" ]]; then
    echo "     (${reviews_stale} review(s) exist at older commits; the head moved after them, so they do not cover it)"
  fi
# Block-only, and the `reviews_at_head` conjunct is what keeps it that way: the
# branch above owns "no review at head" and prints which of the five states the
# PR is in, so this one refuses only a review that EXISTS and carries a state
# neither APPROVED nor COMMENTED. Without the conjunct it also fires on
# `last=none`, which reads as a wrong diagnosis (a review in a bad state rather
# than no review) and, worse, masks the branch above from the test that proves
# it carries the gate.
#
# COMMENTED is the bot's own success state -- it never approves and never
# requests changes -- so this catches a DISMISSED review or a human's
# CHANGES_REQUESTED, whoever left it.
elif [[ "${reviews_at_head}" != "0" && "${review_last_state}" != "APPROVED" && "${review_last_state}" != "COMMENTED" ]]; then
  echo "  -> the current-head review state is ${review_last_state} (need APPROVED or COMMENTED); not clean"
elif [[ "${failing}" != "0" ]]; then
  echo "  -> CI has failing/cancelled checks; not clean to merge"
elif [[ "${other}" != "0" ]]; then
  echo "  -> CI has ${other} check(s) in an unrecognized state; verify by hand before merging"
elif [[ "${pending}" != "0" ]]; then
  echo "  -> CI still running; wait"
elif [[ "${inline_comments}" != "0" && "${confirm_addressed}" != "1" ]]; then
  echo "  -> ${inline_comments} inline review comment(s); read them, then re-run with --confirm-addressed once each is fixed or answered (the API cannot tell; see the header comment)"
elif [[ "${outside_diff}" != "0" && "${confirm_addressed}" != "1" ]]; then
  echo "  -> ${outside_diff} outside-diff finding(s) in the review BODY at head, not inline; read the body with \`gh api repos/${repo}/pulls/${pr}/reviews --jq '.[] | select(.commit_id==\"${head_sha}\") | .body'\`, then re-run with --confirm-addressed once each is fixed or answered"
# Ahead of the mergeState check on purpose. A stale base is always actionable
# and the fix is always the same; mergeState UNKNOWN is often just GitHub still
# computing. Green CI on a stale base says nothing about the merge: a PR that
# passed against an older base can still break `main`, and a gate added to
# `main` after the PR went green has never run against the PR at all. Main's
# own push CI catches the first of those after the merge has landed, which is
# detection rather than prevention, and it never catches a landing loop that
# silently reverts a concurrent change. Costs one fetch.
elif ! merge_base_guard; then
  if [[ "${guard_rc}" == "1" ]]; then
    echo "  -> merge base is behind origin/main; rebase and let CI re-run before merging"
  else
    echo "  -> could not check merge-base freshness (guard exit ${guard_rc}); check by hand before merging"
  fi
  echo "${guard_out//guard: /     }"
elif [[ "${merge_state}" != "CLEAN" && "${merge_state}" != "UNSTABLE" ]]; then
  echo "  -> every check and review looks clean, but mergeState is ${merge_state} (not CLEAN/UNSTABLE); verify by hand before merging"
else
  if [[ "${inline_comments}" != "0" && "${outside_diff}" != "0" ]]; then
    echo "  -> clean (operator confirmed all ${inline_comments} inline comment(s) and ${outside_diff} outside-diff body finding(s) addressed): ${ci_phrase}, review at the current head"
  elif [[ "${inline_comments}" != "0" ]]; then
    echo "  -> clean (operator confirmed all ${inline_comments} inline comment(s) addressed): ${ci_phrase}, review at the current head"
  elif [[ "${outside_diff}" != "0" ]]; then
    echo "  -> clean (operator confirmed all ${outside_diff} outside-diff body finding(s) addressed): ${ci_phrase}, review at the current head"
  else
    echo "  -> clean: ${ci_phrase}, review at the current head with zero findings"
  fi
  # The freshness check above proved the base current at the moment it ran, not
  # for however long the operator takes to run this line; `--match-head-commit`
  # pins the PR head, not `main`. So the printed command re-runs the guard and
  # merges only if it still passes.
  echo "  -> scripts/guards/assert-fresh-merge-base.sh ${pr} && gh pr merge ${pr} --rebase --delete-branch --match-head-commit ${head_sha}"
fi
