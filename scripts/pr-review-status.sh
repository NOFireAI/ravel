#!/usr/bin/env bash
# One-line status for a Ravel PR under the wait-for-CodeRabbit-then-merge-
# by-hand rule (2026-08-26): mergeStateStatus, the CI check rollup, and the
# coderabbitai[bot] review/finding counts, in one call instead of the three
# or four `gh` invocations every session was hand-rolling.
#
# Usage: pr-review-status.sh <pr-number> [--confirm-addressed]
#
# --confirm-addressed: the operator's explicit statement that every
# CodeRabbit finding on the PR has been read and each one fixed or
# answered, both the inline comments and the outside-diff findings in the
# review body. The REST API has no resolved/unresolved field (see below), so
# once a PR has ever had a finding its comment count never returns to zero
# and the clean branch below could otherwise never fire again (issue #764:
# PR #754 had 13 addressed comments across 4 fix rounds and no way to get
# the SHA-pinned merge command). The flag skips ONLY the finding-count
# conjuncts; CI, the current-head review, and the mergeState checks still
# gate exactly as without it.
#
# Sourcing this file defines its parsing helpers and runs nothing, so
# scripts/tests/pr-review-status.test.sh can drive them with captured comment
# bodies instead of live API calls.
set -euo pipefail

repo="${RAVEL_PR_REVIEW_REPO:-NOFireAI/ravel}"

# --- CodeRabbit parsing helpers (unit-tested; no network) ----------------
#
# Issue #950: `reviews@head`, a count of review OBJECTS against the head
# commit, is unreliable in BOTH directions. CodeRabbit does not post a review
# object per push; it EDITS one walkthrough issue-comment in place. On #943 and
# #945 the count read 0 while the walkthrough already said the PR was clean at
# head (false "not reviewed", which would have parked two mergeable PRs), and on
# #946 and #947 the same fields read as reviewed while the review they described
# had run against the PREVIOUS head (false "reviewed", which would have shipped
# unreviewed fix commits on CLEAN plus green CI).
#
# The signal that tracks reality is the sha in the bot's merge-risk line, which
# names the commit the review actually covered:
#
#   **Merge Risk:** _(emoji) Low_ * up to `221f5`
#
# That sha is ABBREVIATED, so it is compared as a PREFIX of headRefOid, never
# for equality.

# jq program over `gh api repos/<repo>/issues/<pr>/comments`. Emits one object,
# always, so the caller reads every field without a presence test:
#
#   kind = "none"          no coderabbitai[bot] review comment on the PR at all
#                          (a fresh PR, or the bot has not run yet)
#   kind = "no-risk-line"  a bot review comment exists but carries no parsable
#                          merge-risk line (an older walkthrough format, or a
#                          verdict-only comment). level/sha are null and the
#                          caller falls back to its other signals rather than
#                          clearing the gate on a sha it never read.
#   kind = "risk"          level and sha are populated.
#
# Candidate set: a bot comment counts as a REVIEW comment only when it carries a
# merge-risk line or one of the two verdict markers the bot emits only on a
# completed review. Without that filter the newest bot comment can be a
# rate-limit notice (#788), which would mask the real walkthrough behind it and
# report "no risk line" for a PR that was reviewed. Newest wins among the
# candidates, by updated_at: the bot edits its walkthrough in place, so
# created_at is the FIRST push's timestamp and sorting on it picks a stale body.
CR_RISK_JQ='
def is_review_comment:
  (.body // "")
  | (test("Merge Risk"; "i")
     or contains("No actionable comments were generated")
     or contains("Actionable comments posted"));

# The LAST merge-risk line in the body: a walkthrough that quotes the format
# while discussing it puts that prose above the line the bot itself appends.
def risk_line:
  [ split("\n")[] | select(test("Merge Risk"; "i")) ] | last;

# The severity word, kept to the vocabulary the bot emits rather than "the next
# word": the line carries an emoji and markdown emphasis between the label and
# the word, and a looser capture picks those up instead.
def risk_level($line):
  ($line
   | capture("(?<lvl>Critical|Very[ _]?High|High|Moderate|Medium|Minimal|Very[ _]?Low|Low|None|Unknown)"; "i")
   | .lvl)
  // null;

# The abbreviated sha the line names, anchored on "up to" so a sha mentioned
# elsewhere on the line cannot be read as the reviewed commit. The backticks are
# optional so a format tweak that drops them still parses; 4 hex chars is git s
# floor for an abbreviation.
def risk_sha($line):
  ($line
   | capture("up[ _]?to[^0-9a-fA-F]*`?(?<sha>[0-9a-fA-F]{4,40})`?"; "i")
   | .sha
   | ascii_downcase)
  // null;

[ .[]? | select(.user.login == "coderabbitai[bot]" and is_review_comment) ]
| sort_by((.updated_at // .created_at // ""), (.id // 0))
| last
| if . == null then
    {kind: "none", level: "none", sha: "none"}
  else
    . as $c
    | (($c.body // "") | risk_line) as $line
    | (if $line == null then null else risk_sha($line) end) as $sha
    | if $sha == null then
        {kind: "no-risk-line", level: "none", sha: "none"}
      else
        {kind: "risk", level: (risk_level($line) // "unknown"), sha: $sha}
      end
  end
'

# jq program over `gh api repos/<repo>/pulls/<pr>/comments`. Splits the inline
# comments into CodeRabbit FINDINGS and the replies to them:
#
#   findings = top-level coderabbitai[bot] inline comments (no in_reply_to_id).
#              The bot's own replies inside a thread are not new findings and
#              must not inflate the count.
#   replies  = comments by anyone other than the bot, posted into a thread
#              rooted at one of those findings.
#
# A reply is evidence a finding was ENGAGED with, not that it was fixed, and the
# REST endpoint carries no resolved/unresolved field (resolution is a
# review-THREAD concept, GraphQL only). So replies < findings proves at least
# one finding has no answer at all, while replies >= findings still needs the
# operator read that --confirm-addressed stands for. This count never clears the
# gate on its own.
CR_THREADS_JQ='
def bot_root_ids:
  [ .[]? | select(.user.login == "coderabbitai[bot]" and (.in_reply_to_id == null)) | .id ];
. as $all
| ($all | bot_root_ids) as $roots
| {
    findings: ($roots | length),
    replies: ([ $all[]?
                | select(.user.login != "coderabbitai[bot]")
                | select(.in_reply_to_id != null)
                | select(.in_reply_to_id as $r | $roots | index($r) != null) ]
              | length)
  }
'

# cr_risk: reads an issue-comments JSON array on stdin, prints the compact risk
# object.
cr_risk() { jq -c "${CR_RISK_JQ}"; }

# cr_threads: reads a pull-comments JSON array on stdin, prints {findings,
# replies}.
cr_threads() { jq -c "${CR_THREADS_JQ}"; }

# cr_head_review <head-sha> <risk-kind> <risk-sha> <fallback-reviewed>
#
# Answers question (a) ONLY -- is the CURRENT head reviewed -- and prints one
# token:
#
#   reviewed           the risk sha is a prefix of the head sha
#   stale              a risk sha was read and does NOT prefix the head sha
#   reviewed-fallback  no parsable risk line, but the older review-object /
#                      walkthrough signals fired. Named so the caller can say
#                      which signal cleared the gate: this is the one direction
#                      #946/#947 got wrong, and it must not read as certainty.
#   unknown            no risk line and no fallback signal
#
# Prefix, not equality: the sha in the risk line is abbreviated.
cr_head_review() {
  local head="$1" kind="$2" sha="$3" fallback="$4"
  if [[ "${kind}" == "risk" && -n "${sha}" && "${sha}" != "none" ]]; then
    if [[ "${head}" == "${sha}"* ]]; then
      printf 'reviewed\n'
    else
      printf 'stale\n'
    fi
    return 0
  fi
  if [[ "${fallback}" == "1" ]]; then
    printf 'reviewed-fallback\n'
  else
    printf 'unknown\n'
  fi
}

# cr_findings_state <findings> <replies> <confirm-addressed>
#
# Answers question (b) ONLY -- are the findings ADDRESSED -- and prints one
# token: none | confirmed | outstanding | partial | answered.
#
# Kept separate from cr_head_review on purpose. A risk line at the current head
# is NOT permission to merge: #948's risk line named its own head while carrying
# seven unfixed findings, because that was the review that FOUND them. Folding
# the two questions into one verdict is how those seven would have shipped.
cr_findings_state() {
  local findings="$1" replies="$2" confirmed="$3"
  if [[ "${findings}" == "0" ]]; then
    printf 'none\n'
  elif [[ "${confirmed}" == "1" ]]; then
    printf 'confirmed\n'
  elif [[ "${replies}" == "0" ]]; then
    printf 'outstanding\n'
  elif (( replies < findings )); then
    printf 'partial\n'
  else
    printf 'answered\n'
  fi
}

pr_review_status_main() {
  local pr confirm_addressed=0
  pr="${1:?usage: pr-review-status.sh <pr-number> [--confirm-addressed]}"
  if [[ "${2:-}" == "--confirm-addressed" ]]; then
    confirm_addressed=1
  elif [[ -n "${2:-}" ]]; then
    echo "usage: pr-review-status.sh <pr-number> [--confirm-addressed]" >&2
    exit 2
  fi

  local pr_json state merge_state head_sha
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
  local normalized
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
  local pending success failing failing_names other
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
  local reviews_json cr_reviews cr_last_state
  reviews_json="$(gh api "repos/${repo}/pulls/${pr}/reviews" --paginate | jq -s 'add')"
  # Only a review against the PR's CURRENT head commit counts: a stale review
  # from before the last push covers code that no longer exists on the branch.
  cr_reviews=$(echo "${reviews_json}" | jq --arg sha "${head_sha}" \
    '[.[] | select(.user.login=="coderabbitai[bot]" and .commit_id==$sha)] | length')
  cr_last_state=$(echo "${reviews_json}" | jq -r --arg sha "${head_sha}" \
    '[.[] | select(.user.login=="coderabbitai[bot]" and .commit_id==$sha)] | last | .state // "none"')

  # When CodeRabbit finds nothing it posts an ISSUE comment ("No actionable
  # comments were generated in the recent review") and files no formal review at
  # all, so the review count above stays 0 for a PR that was reviewed and came
  # back clean. Counting only reviews therefore held a clean PR indefinitely,
  # which is how a wait-for-the-bot rule gets bypassed rather than followed.
  # The walkthrough names the exact commit range it reviewed, so requiring the
  # head sha in the body keeps this as strict as the commit_id match above: a
  # stale walkthrough from before the last push does not count.
  #
  # The sha alone is NOT enough. The bot posts other comment kinds that quote a
  # commit without being a review at all -- a rate-limit notice is the one that
  # bit: on #788 those comments carry two 40-hex shas and zero verdict markers,
  # so keying on the sha alone would have cleared the review gate for a PR the
  # bot had not reviewed. That is the false-CLEAR direction, which is worse than
  # the false-block this whole change set out to fix. So also require a verdict
  # marker: one of the two lines the bot emits only when a review actually
  # completed, whether it found something or nothing.
  #
  # Both of these remain, unchanged, as the FALLBACK for a walkthrough that
  # carries no merge-risk line. They are no longer the primary head signal: a
  # walkthrough's commit-range table can contain the head sha while the review it
  # describes ran against the previous one, which is exactly how #946 and #947
  # read as reviewed when they were not.
  local issue_comments_json cr_walkthroughs
  issue_comments_json="$(gh api "repos/${repo}/issues/${pr}/comments" --paginate | jq -s 'add')"
  cr_walkthroughs=$(echo "${issue_comments_json}" | jq --arg sha "${head_sha}" \
    -f "$(dirname "$0")/lib/coderabbit-verdict.jq")

  # The primary head signal: the sha CodeRabbit's merge-risk line names.
  local risk_json risk_kind risk_level risk_sha head_review risk_cmp
  risk_json="$(echo "${issue_comments_json}" | cr_risk)"
  risk_kind="$(echo "${risk_json}" | jq -r '.kind')"
  risk_level="$(echo "${risk_json}" | jq -r '.level')"
  risk_sha="$(echo "${risk_json}" | jq -r '.sha')"
  local fallback_reviewed=0
  if [[ "${cr_reviews}" != "0" || "${cr_walkthroughs}" != "0" ]]; then
    fallback_reviewed=1
  fi
  head_review="$(cr_head_review "${head_sha}" "${risk_kind}" "${risk_sha}" "${fallback_reviewed}")"
  case "${head_review}" in
    reviewed) risk_cmp="== head" ;;
    stale) risk_cmp="!= head" ;;
    *) risk_cmp="n/a" ;;
  esac

  # The REST review-comments endpoint carries no resolved/unresolved field
  # (resolution is a review-THREAD concept, GraphQL-only) -- this reports the
  # raw inline-comment count. A nonzero count needs a human/session read of
  # `gh api repos/${repo}/pulls/${pr}/comments` to judge whether each finding
  # was already fixed or answered; this script cannot tell that for you. A
  # comment's own count never drops to zero just because the code it flagged
  # changed, so "clean" below means CI green plus a current-head review, not
  # zero comments -- see the merge-fleet-result skill.
  local comments_json cr_comments threads_json cr_findings cr_replies findings_state
  comments_json="$(gh api "repos/${repo}/pulls/${pr}/comments" --paginate | jq -s 'add')"
  cr_comments=$(echo "${comments_json}" | jq '[.[] | select(.user.login=="coderabbitai[bot]")] | length')
  threads_json="$(echo "${comments_json}" | cr_threads)"
  cr_findings=$(echo "${threads_json}" | jq -r '.findings')
  cr_replies=$(echo "${threads_json}" | jq -r '.replies')
  findings_state="$(cr_findings_state "${cr_findings}" "${cr_replies}" "${confirm_addressed}")"

  # A finding that falls outside the range GitHub accepts an inline comment on
  # goes into the review BODY instead, under an "Outside diff range comments (N)"
  # heading. It leaves no entry on the review-comments endpoint above, so nothing
  # read so far can see it. On #908 the two inline comments were pinned to a
  # superseded commit and had already been fixed, so `inline_comments=2` read as
  # "two stale findings, nothing new" while an unaddressed body finding sat at
  # the current head -- the exact shape this script exists to prevent. Head
  # discipline is the review's own commit_id, matching cr_reviews above.
  local cr_outside_diff
  cr_outside_diff=$(echo "${reviews_json}" | jq --arg sha "${head_sha}" \
    -f "$(dirname "$0")/lib/coderabbit-outside-diff.jq")

  local summary
  summary="PR #${pr} @ ${head_sha}: state=${state} mergeState=${merge_state} CI=${success} pass/${pending} pending/${failing} fail"
  if [[ "${other}" != "0" ]]; then
    summary="${summary}/${other} unrecognized"
  fi
  if [[ "${failing}" != "0" ]]; then
    summary="${summary} (${failing_names})"
  fi
  summary="${summary} | CodeRabbit: risk=${risk_level} up-to=${risk_sha} ${risk_cmp}, ${cr_findings} inline, ${cr_replies} replies"
  # Every field this script printed before issue #950 is still printed, in the
  # same order, after the new ones: other workflows read this line.
  summary="${summary} | reviews@head=${cr_reviews} walkthroughs@head=${cr_walkthroughs} last=${cr_last_state} inline_comments=${cr_comments}"
  # Appended only when nonzero, so a PR with no body findings prints exactly the
  # line it printed before this field existed. The name says body_findings rather
  # than anything with "comment" in it: these are not inline comments and are not
  # fetched from the comments endpoint, and an operator who reads the two counts
  # as one number is back to the #908 failure.
  if [[ "${cr_outside_diff}" != "0" ]]; then
    summary="${summary} outside_diff_body_findings@head=${cr_outside_diff}"
  fi
  echo "${summary}"

  # The two questions, on two lines, never merged into one verdict.
  case "${head_review}" in
    reviewed)
      echo "  -> head review: reviewed at head (risk sha ${risk_sha} is a prefix of ${head_sha})" ;;
    stale)
      echo "  -> head review: NOT reviewed at head (CodeRabbit reviewed up to ${risk_sha}, head is ${head_sha}); the commits pushed since are unreviewed" ;;
    reviewed-fallback)
      echo "  -> head review: no merge-risk line to read (${risk_kind}); falling back to reviews@head=${cr_reviews} walkthroughs@head=${cr_walkthroughs}, which say the head was reviewed. Confirm by hand: this fallback is the signal that read #946/#947 as reviewed when they were not" ;;
    *)
      echo "  -> head review: no CodeRabbit review of this PR found (${risk_kind}, reviews@head=${cr_reviews}, walkthroughs@head=${cr_walkthroughs})" ;;
  esac
  case "${findings_state}" in
    none)
      echo "  -> findings: 0 CodeRabbit inline finding(s)" ;;
    confirmed)
      echo "  -> findings: ${cr_findings} inline finding(s), ${cr_replies} reply/replies; operator confirmed each is fixed or answered" ;;
    outstanding)
      echo "  -> findings: ${cr_findings} inline finding(s), ${cr_replies} replies -> findings outstanding" ;;
    partial)
      echo "  -> findings: ${cr_findings} inline finding(s), only ${cr_replies} reply/replies -> at least one finding is unanswered" ;;
    *)
      echo "  -> findings: ${cr_findings} inline finding(s), ${cr_replies} reply/replies; a reply is not a fix, read them before merging" ;;
  esac

  if [[ "${state}" != "OPEN" ]]; then
    echo "  -> PR is ${state}, not open; nothing to merge"
  elif [[ "${merge_state}" == "DIRTY" || "${merge_state}" == "DRAFT" || "${merge_state}" == "BEHIND" ]]; then
    echo "  -> mergeState is ${merge_state}; not mergeable regardless of CI/CodeRabbit state below"
  elif [[ "${head_review}" == "stale" ]]; then
    echo "  -> CodeRabbit's review covers ${risk_sha}, not the current head; do not merge until a review of ${head_sha} lands"
  elif [[ "${head_review}" == "unknown" ]]; then
    echo "  -> no CodeRabbit review against the current head commit yet; do not merge until one lands"
  elif [[ "${cr_reviews}" != "0" && "${cr_last_state}" != "APPROVED" && "${cr_last_state}" != "COMMENTED" ]]; then
    echo "  -> CodeRabbit's current-head review state is ${cr_last_state} (need APPROVED or COMMENTED); not clean"
  elif [[ "${failing}" != "0" ]]; then
    echo "  -> CI has failing/cancelled checks; not clean to merge"
  elif [[ "${other}" != "0" ]]; then
    echo "  -> CI has ${other} check(s) in an unrecognized state; verify by hand before merging"
  elif [[ "${pending}" != "0" ]]; then
    echo "  -> CI still running; wait"
  elif [[ "${cr_findings}" != "0" && "${confirm_addressed}" != "1" ]]; then
    echo "  -> ${cr_findings} CodeRabbit inline comment(s); read them, then re-run with --confirm-addressed once each is fixed or answered (the API cannot tell; see the header comment)"
  elif [[ "${cr_outside_diff}" != "0" && "${confirm_addressed}" != "1" ]]; then
    echo "  -> ${cr_outside_diff} CodeRabbit outside-diff finding(s) in the review BODY at head, not inline; read the body with \`gh api repos/${repo}/pulls/${pr}/reviews --jq '.[] | select(.commit_id==\"${head_sha}\") | .body'\`, then re-run with --confirm-addressed once each is fixed or answered"
  elif [[ "${merge_state}" != "CLEAN" && "${merge_state}" != "UNSTABLE" ]]; then
    echo "  -> every check and review looks clean, but mergeState is ${merge_state} (not CLEAN/UNSTABLE); verify by hand before merging"
  else
    if [[ "${cr_findings}" != "0" && "${cr_outside_diff}" != "0" ]]; then
      echo "  -> clean (operator confirmed all ${cr_findings} inline comment(s) and ${cr_outside_diff} outside-diff body finding(s) addressed): CI green, CodeRabbit reviewed the current head"
    elif [[ "${cr_findings}" != "0" ]]; then
      echo "  -> clean (operator confirmed all ${cr_findings} inline comment(s) addressed): CI green, CodeRabbit reviewed the current head"
    elif [[ "${cr_outside_diff}" != "0" ]]; then
      echo "  -> clean (operator confirmed all ${cr_outside_diff} outside-diff body finding(s) addressed): CI green, CodeRabbit reviewed the current head"
    else
      echo "  -> clean: CI green, CodeRabbit reviewed the current head with zero inline comments"
    fi
    echo "  -> gh pr merge ${pr} --rebase --delete-branch --match-head-commit ${head_sha}"
  fi
}

# Sourcing defines the helpers and runs nothing; executing runs the report.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  pr_review_status_main "$@"
fi
