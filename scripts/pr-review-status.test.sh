#!/usr/bin/env bash
# Cases for the two filters behind scripts/pr-review-status.sh:
# lib/coderabbit-verdict.jq, which decides whether CodeRabbit has actually
# reviewed a PR's current head, and lib/coderabbit-outside-diff.jq, which
# counts the findings the bot reports in a review BODY instead of inline.
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

# Backticked inline code is the same shape and must not count either.
check_outside "bot review with the marker in inline code is NOT counted" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},"commit_id":"${SHA}",
  "body":"Match the emitted \`⚠️ Outside diff range comments (1)\` summary marker instead of the phrase."}]
EOF
)"

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
# mergeState CLEAN, one COMMENTED review at head, zero inline comments. So the
# only thing that can move the verdict is the body.
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
  printf '[]\n' >"${fx}/issue-comments.json"
  printf '[]\n' >"${fx}/review-comments.json"
  FIXTURES="${fx}" PATH="${E2E_DIR}/bin:${PATH}" \
    bash "$(dirname "$0")/pr-review-status.sh" 908 "$@"
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
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | CodeRabbit: reviews@head=1 walkthroughs@head=0 last=COMMENTED inline_comments=0" \
  "$(printf '%s\n' "${clean_out}" | sed -n 1p)"
check_eq "walkthrough-only body: verdict is unchanged clean" \
  "  -> clean: CI green, CodeRabbit reviewed the current head with zero inline comments" \
  "$(printf '%s\n' "${clean_out}" | sed -n 2p)"
check_eq "walkthrough-only body: merge command still printed" \
  "  -> gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${clean_out}" | sed -n 3p)"

# The #908 regression: same PR, same green CI, same zero inline comments, one
# outside-diff finding in the body. It must be visible and it must block.
check_eq "outside-diff body finding: counted on the summary line" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | CodeRabbit: reviews@head=1 walkthroughs@head=0 last=COMMENTED inline_comments=0 outside_diff_body_findings@head=1" \
  "$(printf '%s\n' "${finding_out}" | sed -n 1p)"
check_eq "outside-diff body finding: verdict is not clean" \
  "  -> 1 CodeRabbit outside-diff finding(s) in the review BODY at head, not inline; read the body with \`gh api repos/NOFireAI/ravel/pulls/908/reviews --jq '.[] | select(.commit_id==\"${SHA}\") | .body'\`, then re-run with --confirm-addressed once each is fixed or answered" \
  "$(printf '%s\n' "${finding_out}" | sed -n 2p)"
check_eq "outside-diff body finding: no merge command offered" \
  "" \
  "$(printf '%s\n' "${finding_out}" | sed -n 3p)"

# --confirm-addressed overrides it, in the same shape as the inline branch.
check_eq "outside-diff body finding: --confirm-addressed clears it" \
  "  -> clean (operator confirmed all 1 outside-diff body finding(s) addressed): CI green, CodeRabbit reviewed the current head" \
  "$(printf '%s\n' "${confirmed_out}" | sed -n 2p)"
check_eq "outside-diff body finding: --confirm-addressed prints the merge command" \
  "  -> gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${confirmed_out}" | sed -n 3p)"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
