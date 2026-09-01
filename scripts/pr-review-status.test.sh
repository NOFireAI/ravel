#!/usr/bin/env bash
# Cases for the three filters behind scripts/pr-review-status.sh:
# lib/coderabbit-risk.jq, which extracts the walkthrough's Merge Risk sha and
# so decides whether CodeRabbit's assessment covers the current head;
# lib/coderabbit-verdict.jq, the older review-presence count; and
# lib/coderabbit-outside-diff.jq, which counts the findings the bot reports in
# a review BODY instead of inline.
#
# Fixtures only: no network, no gh. Each case is a comment or review shape
# observed on a real PR, named with the PR it came from.
#
# Exit 0 all pass, 1 on a failure.
set -uo pipefail

FILTER="$(dirname "$0")/lib/coderabbit-verdict.jq"
[[ -r "${FILTER}" ]] || { echo "missing ${FILTER}" >&2; exit 64; }
OUTSIDE_FILTER="$(dirname "$0")/lib/coderabbit-outside-diff.jq"
[[ -r "${OUTSIDE_FILTER}" ]] || { echo "missing ${OUTSIDE_FILTER}" >&2; exit 64; }
RISK_FILTER="$(dirname "$0")/lib/coderabbit-risk.jq"
[[ -r "${RISK_FILTER}" ]] || { echo "missing ${RISK_FILTER}" >&2; exit 64; }

SHA="0c65a3d3983384d663189257a25cc2d40ca95d32"
OTHER="1111111111111111111111111111111111111111"
passes=0
fails=0

run_case() {
  local filter="$1" name="$2" want="$3" json="$4" got
  got="$(printf '%s' "${json}" | jq --arg sha "${SHA}" -f "${filter}")" || got="ERROR"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${name}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %s: want %s, got %s\n' "${name}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

check() { run_case "${FILTER}" "$@"; }
check_outside() { run_case "${OUTSIDE_FILTER}" "$@"; }
# The risk filter emits a two-field TSV line, so its expectations are written
# as "<sha>|<paused>" and the tab is translated before comparison.
check_risk() {
  local name="$1" want="$2" json="$3" got
  got="$(printf '%s' "${json}" | jq -r -f "${RISK_FILTER}" | tr '\t' '|')" || got="ERROR"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${name}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %s: want %s, got %s\n' "${name}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

# The clean case (#893): zero findings, reported as an issue comment, no formal
# review filed at all. This is what used to be missed, holding a reviewed PR
# forever.
check "zero-finding walkthrough at head counts" 1 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},
  "body":"Reviewing files that changed between abc and ${SHA}.\n\nNo actionable comments were generated in the recent review."}]
EOF
)"

# A review that DID find things still counts as a review; its findings are
# gated separately by the inline-comment check.
check "walkthrough reporting findings counts" 1 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},
  "body":"Actionable comments posted: 6\n\nReviewed up to ${SHA}."}]
EOF
)"

# The regression this file exists for (#788): a rate-limit notice quotes the
# commit but is not a review. Keying on the sha alone counted it and cleared
# the review gate for a PR the bot never reviewed -- the false-CLEAR direction.
check "rate-limit notice quoting the sha does NOT count" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},
  "body":"⚠️ Rate limit exceeded\n\n@user has exceeded the limit. Please wait before requesting another review.\n\nCommits: reviewed up to ${SHA}."}]
EOF
)"

# A verdict for a DIFFERENT commit is a stale review of code that is no longer
# the head, exactly as a stale formal review would be.
check "verdict naming another commit does NOT count" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},
  "body":"Reviewed up to ${OTHER}.\n\nNo actionable comments were generated in the recent review."}]
EOF
)"

# Only the bot's own verdicts count; a human quoting the marker does not.
check "human comment quoting the marker does NOT count" 0 "$(cat <<EOF
[{"user":{"login":"pmoust"},
  "body":"CodeRabbit said: No actionable comments were generated, at ${SHA}."}]
EOF
)"

# A body-less comment must not blow up the filter.
check "null body is tolerated" 0 '[{"user":{"login":"coderabbitai[bot]"},"body":null}]'

check "no comments at all" 0 '[]'

# --- lib/coderabbit-outside-diff.jq --------------------------------------
#
# Input is the pulls/<pr>/reviews array, so head discipline is the review's own
# commit_id, not a sha quoted in the body.
#
# Counting rule (asserted below): the heading carries the number of findings in
# the block in parentheses, so a heading WITH a count contributes that count and
# a heading WITHOUT one contributes 1; headings sum across a body and across
# reviews at the same head.

# The #908 shape: an unaddressed finding the bot could not post inline, folded
# into the review body at the current head. Nothing else in the script sees it.
check_outside "outside-diff block at head is counted" 1 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"> [!CAUTION]\n> Some comments are outside the diff and can't be posted inline due to platform limitations.\n>\n> <details>\n> <summary>⚠️ Outside diff range comments (1)</summary>\n>\n> scripts/foo.sh line 12: the guard is inverted.\n"}]
EOF
)"

# Summary/walkthrough prose is not a finding. Counting a non-empty body would
# make every reviewed PR look like it carried findings, which trains the
# operator to pass --confirm-addressed without reading anything.
check_outside "walkthrough-only body at head is NOT counted" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"Actionable comments posted: 0\n\n<details>\n<summary>Walkthrough</summary>\n\nThe change adds a guard to the merge script.\n</details>"}]
EOF
)"

# A body on a superseded commit describes code that no longer exists on the
# branch, exactly as a stale formal review does.
check_outside "outside-diff block on a superseded commit is NOT counted" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${OTHER}",
  "body":"> <summary>⚠️ Outside diff range comments (2)</summary>"}]
EOF
)"

# 3 from the counted heading plus 1 for the heading with no count: 4.
check_outside "multiple findings sum per heading count" 4 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"> <summary>⚠️ Outside diff range comments (3)</summary>\n\nprose\n\n> <summary>⚠️ Outside diff range and nitpick comments</summary>"}]
EOF
)"

check_outside "null review body is tolerated" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}","body":null}]
EOF
)"

check_outside "absent body key is tolerated" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}"}]
EOF
)"

check_outside "no reviews at all" 0 '[]'

# The false-CLEAR direction, the same one that forced the verdict filter to be
# tightened on #788: a bot comment that quotes the sha but reports no findings
# must not be counted as one.
check_outside "rate-limit notice quoting the sha is NOT counted" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"⚠️ Rate limit exceeded\n\n@user has exceeded the limit. Please wait before requesting another review.\n\nCommits: reviewed up to ${SHA}."}]
EOF
)"

# Only the bot's own reviews count.
check_outside "human review quoting the marker is NOT counted" 0 "$(cat <<EOF
[{"user":{"login":"pmoust"},"commit_id":"${SHA}",
  "body":"CodeRabbit reported ⚠️ Outside diff range comments (1) earlier, already fixed."}]
EOF
)"

# The bot itself quotes the heading when it discusses this mechanism, and a
# quote is not a finding. This is the case the user filter above does NOT
# cover: same phrase, same line, bot author, current head -- and no <summary>
# element, because nothing was actually reported. Matching the phrase rather
# than the emitted element counted this and blocked a clean PR.
check_outside "bot review quoting the marker in prose is NOT counted" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"Line 40 counts any same-line prose containing Outside diff range comments (1), which is a false positive."}]
EOF
)"

# Two qualifying elements on ONE line must count as two headings, not one. A
# greedy [^\n]* between the tags let the first match swallow both, and the
# count capture then read only the first number: 1 + 2 reported as 1.
check_outside "two summaries on one line count separately" 3 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"> <summary>⚠️ Outside diff range comments (1)</summary><summary>⚠️ Outside diff range comments (2)</summary>"}]
EOF
)"

# The emitted form puts <blockquote> immediately after </summary> on the same
# line. Bounding the match with [^<\n]* must not break that.
check_outside "emitted form with a trailing blockquote still counts" 1 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"> <summary>⚠️ Outside diff range comments (1)</summary><blockquote>\n>\n> <details>\n> <summary>scripts/foo.sh (1)</summary><blockquote>"}]
EOF
)"

# Backticked inline code is the same shape and must not count either.
check_outside "bot review with the marker in inline code is NOT counted" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"Match the emitted \`⚠️ Outside diff range comments (1)\` summary marker instead of the phrase."}]
EOF
)"

# --- lib/coderabbit-risk.jq ----------------------------------------------
#
# Input is the issues/<pr>/comments array. Expectations are "<sha>|<paused>".
#
# The signal exists because CodeRabbit re-reviews by EDITING one walkthrough
# comment in place: a review object at head can be absent after a real
# re-review and present after a stale one, so the sha on the risk line is the
# only thing that tracks what was actually assessed (issue #950).

check_risk "risk line yields its short sha" "6f458|active" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-01T00:00:00Z",
  "body":"<details>\n<summary>Walkthrough</summary>\n</details>\n\nMerge Risk: Moderate . up to `6f458`"}]
EOF
)"

# The emitted form bolds the label; the substring is unchanged.
check_risk "bolded risk label still parses" "6f458|active" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"body":"**Merge Risk: Low** . up to `6f458`"}]
EOF
)"

# A risk line that backticks something before the sha (a file name, a label)
# must still yield the trailing "up to" sha, not the first backticked token.
check_risk "the last backticked token on the line wins" "6f458|active" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"body":"Merge Risk: Moderate for `scripts/foo.sh` . up to `6f458`"}]
EOF
)"

# A backticked token on a LATER line is not part of the risk line. Without the
# [^\n]* bound the greedy match would cross the newline and report the wrong
# commit, which is a false freshness signal in either direction.
check_risk "a backticked token on a later line is not the risk sha" "6f458|active" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"body":"Merge Risk: Moderate . up to `6f458`\n\nFiles reviewed: `deadbeef`"}]
EOF
)"

# Newest wins. Two walkthroughs, the older one fresh and the newer one stale:
# reading the older would clear a PR whose latest assessment is behind.
check_risk "newest walkthrough wins when the older one is fresher" "aaaaa|active" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-02T00:00:00Z",
  "body":"Merge Risk: Low . up to `bbbbb`"},
 {"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-03T00:00:00Z",
  "body":"Merge Risk: Low . up to `aaaaa`"}]
EOF
)"

# Array order is not trusted: created_at decides.
check_risk "out-of-order comments are ordered by created_at" "aaaaa|active" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-03T00:00:00Z",
  "body":"Merge Risk: Low . up to `aaaaa`"},
 {"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-02T00:00:00Z",
  "body":"Merge Risk: Low . up to `bbbbb`"}]
EOF
)"

# A newer bot comment with no risk line (a chat reply, a rate-limit notice)
# does not erase the newest ASSESSMENT; only risk-carrying comments are ranked.
check_risk "a newer non-walkthrough comment does not hide the risk line" "6f458|active" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-02T00:00:00Z",
  "body":"Merge Risk: Low . up to `6f458`"},
 {"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-04T00:00:00Z",
  "body":"@user I have resolved that thread."}]
EOF
)"

check_risk "paused reviews are reported alongside the sha" "6f458|paused" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-02T00:00:00Z",
  "body":"Merge Risk: Low . up to `6f458`\n\n> Reviews paused"}]
EOF
)"

check_risk "paused with no risk line yields an empty sha" "|paused" "$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"body":"⏸️ Reviews paused\n\nUse @coderabbitai resume to restart."}]
EOF
)"

# A human quoting a risk line is not an assessment.
check_risk "a human risk line does not count" "|active" "$(cat <<'EOF'
[{"user":{"login":"pmoust"},"body":"CodeRabbit said Merge Risk: Low . up to `6f458`, merging."}]
EOF
)"

check_risk "no comments at all" "|active" '[]'
check_risk "null body is tolerated" "|active" \
  '[{"user":{"login":"coderabbitai[bot]"},"body":null}]'

# --- end-to-end verdict, pr-review-status.sh ------------------------------
#
# The count above only matters if it flips the verdict. These run the real
# script against fixtures with a stand-in `gh` on PATH: no network, no token.

E2E_DIR="$(mktemp -d)"
trap 'rm -rf "${E2E_DIR}"' EXIT
mkdir -p "${E2E_DIR}/bin"
cat >"${E2E_DIR}/bin/gh" <<'SHIM'
#!/usr/bin/env bash
# Fixture-backed stand-in for gh, dispatching on the endpoint in the args.
# The /pulls/*/reviews case must precede /pulls/*/comments: both are pulls.
case "$*" in
  *"pr view"*)              cat "${FIXTURES}/pr-view.json" ;;
  *"/pulls/"*"/reviews"*)   cat "${FIXTURES}/reviews.json" ;;
  *"/issues/"*"/comments"*) cat "${FIXTURES}/issue-comments.json" ;;
  *"/pulls/"*"/comments"*)  cat "${FIXTURES}/review-comments.json" ;;
  *) echo "unexpected gh call: $*" >&2; exit 90 ;;
esac
SHIM
chmod +x "${E2E_DIR}/bin/gh"

# Everything except the review body is held constant and clean: CI green,
# mergeState CLEAN, one COMMENTED review at head, zero inline comments, and a
# walkthrough whose risk line names the head. So the only thing that can move
# the verdict is the body -- or, in the risk cases below, E2E_ISSUE_COMMENTS.
#
# The default walkthrough deliberately carries neither the full head sha nor a
# verdict marker, so walkthroughs@head stays 0 and these cases keep exercising
# the outside-diff path rather than accidentally depending on the verdict
# filter as well.
FRESH_WALKTHROUGH="$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-02T00:00:00Z",
  "body":"<details>\n<summary>Walkthrough</summary>\nAdds a guard.\n</details>\n\n**Merge Risk: Low** . up to \`${SHA:0:5}\`"}]
EOF
)"

e2e() {
  local review_body="$1"
  shift
  local fx="${E2E_DIR}/fx"
  rm -rf "${fx}"
  mkdir -p "${fx}"
  printf '{"state":"OPEN","mergeStateStatus":"CLEAN","statusCheckRollup":[{"name":"ci","status":"COMPLETED","conclusion":"SUCCESS"}],"headRefOid":"%s"}\n' \
    "${SHA}" >"${fx}/pr-view.json"
  printf '[{"user":{"login":"coderabbitai[bot]"},"state":"COMMENTED","commit_id":"%s","body":%s}]\n' \
    "${SHA}" "${review_body}" >"${fx}/reviews.json"
  printf '%s\n' "${E2E_ISSUE_COMMENTS:-${FRESH_WALKTHROUGH}}" >"${fx}/issue-comments.json"
  printf '[]\n' >"${fx}/review-comments.json"
  FIXTURES="${fx}" PATH="${E2E_DIR}/bin:${PATH}" \
    bash "${E2E_SCRIPT:-$(dirname "$0")/pr-review-status.sh}" 908 "$@"
}

check_eq() {
  local name="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${name}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %s:\n  want: %s\n  got:  %s\n' "${name}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

CLEAN_BODY_JSON='"Actionable comments posted: 0\n\n<details>\n<summary>Walkthrough</summary>\n\nAdds a guard.\n</details>"'
FINDING_BODY_JSON="$(cat <<'EOF'
"**Actionable comments posted: 2**\n\n> [!CAUTION]\n> Some comments are outside the diff and can't be posted inline due to platform limitations.\n>\n> <details>\n> <summary>⚠️ Outside diff range comments (1)</summary>\n>\n> `scripts/foo.sh` line 12: the guard is inverted.\n>\n> </details>"
EOF
)"

clean_out="$(e2e "${CLEAN_BODY_JSON}")"
finding_out="$(e2e "${FINDING_BODY_JSON}")"
confirmed_out="$(e2e "${FINDING_BODY_JSON}" --confirm-addressed)"

# A body with no findings prints exactly what it printed before this field
# existed: no extra summary field, and the unchanged clean verdict.
check_eq "walkthrough-only body: summary line carries no outside-diff field" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | CodeRabbit: risk_line=head reviews@head=1 walkthroughs@head=0 last=COMMENTED inline_comments=0" \
  "$(printf '%s\n' "${clean_out}" | sed -n 1p)"
check_eq "walkthrough-only body: verdict is unchanged clean" \
  "  -> clean: CI green, CodeRabbit's risk line names the current head with zero inline comments" \
  "$(printf '%s\n' "${clean_out}" | sed -n 2p)"
check_eq "walkthrough-only body: merge command still printed" \
  "  -> gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${clean_out}" | sed -n 3p)"

# The #908 regression: same PR, same green CI, same zero inline comments, one
# outside-diff finding in the body. It must be visible and it must block.
check_eq "outside-diff body finding: counted on the summary line" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | CodeRabbit: risk_line=head reviews@head=1 walkthroughs@head=0 last=COMMENTED inline_comments=0 outside_diff_body_findings@head=1" \
  "$(printf '%s\n' "${finding_out}" | sed -n 1p)"
check_eq "outside-diff body finding: verdict is not clean" \
  "  -> 1 CodeRabbit outside-diff finding(s) in the review BODY at head, not inline; read the body with \`gh api repos/NOFireAI/ravel/pulls/908/reviews --jq '.[] | select(.commit_id==\"${SHA}\") | .body'\`, then re-run with --confirm-addressed once each is fixed or answered" \
  "$(printf '%s\n' "${finding_out}" | sed -n 2p)"
check_eq "outside-diff body finding: no merge command offered" \
  "" \
  "$(printf '%s\n' "${finding_out}" | sed -n 3p)"

# --confirm-addressed overrides it, in the same shape as the inline branch.
check_eq "outside-diff body finding: --confirm-addressed clears it" \
  "  -> clean (operator confirmed all 1 outside-diff body finding(s) addressed): CI green, CodeRabbit's risk line names the current head" \
  "$(printf '%s\n' "${confirmed_out}" | sed -n 2p)"
check_eq "outside-diff body finding: --confirm-addressed prints the merge command" \
  "  -> gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${confirmed_out}" | sed -n 3p)"

# --- risk-line freshness end-to-end (issue #950) --------------------------
#
# Everything else in these fixtures is clean and constant -- CI green,
# mergeState CLEAN, one COMMENTED review at head, no findings anywhere -- so
# the ONLY thing that moves the verdict is the walkthrough's risk line. That is
# the point: under the old rule this exact fixture read as reviewed and clean.

STALE_WALKTHROUGH="$(cat <<'EOF'
[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-02T00:00:00Z",
  "body":"<details>\n<summary>Walkthrough</summary>\nAdds a guard.\n</details>\n\n**Merge Risk: Moderate** . up to `6f458`"}]
EOF
)"

E2E_ISSUE_COMMENTS="${STALE_WALKTHROUGH}"
stale_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "stale risk line: reported on the summary line" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | CodeRabbit: risk_line=stale:6f458 reviews@head=1 walkthroughs@head=0 last=COMMENTED inline_comments=0" \
  "$(printf '%s\n' "${stale_out}" | sed -n 1p)"
check_eq "stale risk line: verdict blocks the merge" \
  "  -> CodeRabbit's risk line is at STALE sha 6f458 vs head ${SHA} (do not merge yet)" \
  "$(printf '%s\n' "${stale_out}" | sed -n 2p)"
check_eq "stale risk line: no merge command offered" \
  "" \
  "$(printf '%s\n' "${stale_out}" | sed -n 3p)"

# Proof this pins the fix rather than passing anyway: flip the single marked
# condition off, which leaves the pre-#950 behaviour (a review object at head
# decides), and the SAME fixture must come back clean with a merge command.
FLIP_DIR="${E2E_DIR}/flip"
mkdir -p "${FLIP_DIR}"
ln -s "$(cd "$(dirname "$0")" && pwd)/lib" "${FLIP_DIR}/lib"
sed 's/^elif .*# PROVE-FLIP$/elif false; then/' \
  "$(dirname "$0")/pr-review-status.sh" >"${FLIP_DIR}/pr-review-status.sh"
if grep -q '^elif false; then$' "${FLIP_DIR}/pr-review-status.sh"; then
  printf 'ok    %s\n' "prove: PROVE-FLIP line found and flipped"
  passes=$((passes + 1))
else
  printf 'FAIL  %s\n' "prove: PROVE-FLIP line found and flipped: sed did not match the marked line"
  fails=$((fails + 1))
fi

E2E_ISSUE_COMMENTS="${STALE_WALKTHROUGH}" E2E_SCRIPT="${FLIP_DIR}/pr-review-status.sh"
flipped_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS E2E_SCRIPT

check_eq "prove: the pre-#950 rule calls the stale fixture clean" \
  "  -> clean: CI green, CodeRabbit's risk line names the current head with zero inline comments" \
  "$(printf '%s\n' "${flipped_out}" | sed -n 2p)"
check_eq "prove: the pre-#950 rule even offers the merge command" \
  "  -> gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${flipped_out}" | sed -n 3p)"

# Degraded mode 1: no CodeRabbit comment at all. Nothing to parse, and the
# script must say so explicitly rather than fall through to clean.
E2E_ISSUE_COMMENTS='[]'
none_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "no CodeRabbit comment: risk_line=none on the summary line" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | CodeRabbit: risk_line=none reviews@head=1 walkthroughs@head=0 last=COMMENTED inline_comments=0" \
  "$(printf '%s\n' "${none_out}" | sed -n 1p)"
check_eq "no CodeRabbit comment: verdict says not assessed" \
  "  -> no CodeRabbit Merge Risk line found: CodeRabbit has not assessed this PR; do not merge until a risk line naming the head commit appears" \
  "$(printf '%s\n' "${none_out}" | sed -n 2p)"

# Degraded mode 2: reviews paused, no assessment. The pause is the reason the
# risk line will never arrive, so it is named in the same line.
E2E_ISSUE_COMMENTS='[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-02T00:00:00Z","body":"⏸️ Reviews paused\n\nUse `@coderabbitai resume` to restart."}]'
paused_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "paused reviews: flagged on the summary line" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | CodeRabbit: risk_line=none reviews@head=1 walkthroughs@head=0 last=COMMENTED inline_comments=0 reviews_paused=yes" \
  "$(printf '%s\n' "${paused_out}" | sed -n 1p)"
check_eq "paused reviews: verdict names the pause as the reason" \
  "  -> no CodeRabbit Merge Risk line found: CodeRabbit has not assessed this PR; CodeRabbit reviews are PAUSED on this PR, so the assessment will not refresh until they resume; do not merge until a risk line naming the head commit appears" \
  "$(printf '%s\n' "${paused_out}" | sed -n 2p)"

# A pause AFTER a head-fresh assessment is not a blocker: the assessment that
# exists already covers the head. Blocking here would be a false block that
# only an unrelated bot state caused.
E2E_ISSUE_COMMENTS="$(printf '[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-02T00:00:00Z","body":"**Merge Risk: Low** . up to `%s`\\n\\n⏸️ Reviews paused"}]' "${SHA:0:5}")"
paused_fresh_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "paused after a head-fresh assessment still merges" \
  "  -> clean: CI green, CodeRabbit's risk line names the current head with zero inline comments" \
  "$(printf '%s\n' "${paused_fresh_out}" | sed -n 2p)"

# Degraded mode 3: several walkthrough comments. The bot posts a fresh one
# after a force-push or a re-summon, and the older one still carries its own
# risk line. Newest by created_at decides; here the newest is stale, so a
# reader that took the first (or the freshest-looking) one would clear a PR
# whose latest assessment is behind the head.
E2E_ISSUE_COMMENTS="$(printf '[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-01T00:00:00Z","body":"**Merge Risk: Low** . up to `%s`"},{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-05T00:00:00Z","body":"**Merge Risk: High** . up to `6f458`"}]' "${SHA:0:5}")"
multi_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "multiple walkthroughs: the newest one decides, stale blocks" \
  "  -> CodeRabbit's risk line is at STALE sha 6f458 vs head ${SHA} (do not merge yet)" \
  "$(printf '%s\n' "${multi_out}" | sed -n 2p)"

# The mirror: newest fresh, older stale. Newest wins in both directions, so the
# case above cannot be passing merely because something always blocks.
E2E_ISSUE_COMMENTS="$(printf '[{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-01T00:00:00Z","body":"**Merge Risk: Low** . up to `6f458`"},{"user":{"login":"coderabbitai[bot]"},"created_at":"2026-01-05T00:00:00Z","body":"**Merge Risk: High** . up to `%s`"}]' "${SHA:0:5}")"
multi_fresh_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "multiple walkthroughs: the newest one decides, fresh clears" \
  "  -> clean: CI green, CodeRabbit's risk line names the current head with zero inline comments" \
  "$(printf '%s\n' "${multi_fresh_out}" | sed -n 2p)"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
