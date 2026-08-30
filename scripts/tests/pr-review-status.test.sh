#!/usr/bin/env bash
# The CodeRabbit signals scripts/pr-review-status.sh must derive (issue #950).
# Run:
#   bash scripts/tests/pr-review-status.test.sh
#
# The status script is sourced, not executed, so only its parsing helpers run:
# every case drives them with a CAPTURED comment body. Nothing is fetched, and
# `gh` is never invoked.
set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/pr-review-status.sh"
LIBDIR="$(cd "$(dirname "$0")/.." && pwd)/lib"
# shellcheck source=../pr-review-status.sh
source "${SCRIPT}"
# The status script turns on errexit for its own main body. Cases below capture
# exit codes explicitly instead.
set +e

pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

# Two real 40-hex shas from the issue's reproduction, so the prefix comparison
# is exercised against the abbreviation lengths the bot actually emits.
HEAD_946="221f536c9a5f1c4e5d3b7a90e1f2c3d4a5b6c7d8"
PREV_947="a1231f0b2c3d4e5f60718293a4b5c6d7e8f90a1b"
HEAD_947="093761dd2e3f4a5b6c7d8e9f0a1b2c3d4e5f6071"

# walkthrough_body <risk-level> <up-to-sha> <extra-line>: the shape CodeRabbit
# posts as a single ISSUE comment and then EDITS in place on every push. The
# commit-range table quotes FULL shas including the current head -- that is what
# made the pre-#950 walkthrough check read #946/#947 as reviewed while the risk
# line named the previous head.
walkthrough_body() {
  local level="$1" upto="$2" extra="$3"
  printf '## Walkthrough\n\nSome prose about the change.\n\n<details>\n<summary>Commits</summary>\n\nReviewed everything up to %s.\n\n</details>\n\n%s\n\n**Merge Risk:** _(:hourglass:) %s_ * up to `%s`\n' \
    "${extra}" "No actionable comments were generated in the recent review." "${level}" "${upto}"
}

# comments_json: builds the issue-comments array the API returns, from
# id/login/updated_at/body quadruples passed as repeated 4-arg groups.
comments_json() {
  local out="[]"
  while (($# >= 4)); do
    out="$(jq -c --argjson arr "${out}" \
      --argjson id "$1" --arg login "$2" --arg upd "$3" --arg body "$4" \
      -n '$arr + [{id: $id, user: {login: $login}, created_at: $upd, updated_at: $upd, body: $body}]')"
    shift 4
  done
  printf '%s\n' "${out}"
}

# --- (a) risk sha is a prefix of the head: reviewed at head ---------------
body_a="$(walkthrough_body "Low" "221f5" "${HEAD_946}")"
json_a="$(comments_json 1 "coderabbitai[bot]" "2026-08-29T10:00:00Z" "${body_a}")"
risk_a="$(echo "${json_a}" | cr_risk)"
check_eq "(a) kind" "risk" "$(echo "${risk_a}" | jq -r .kind)"
check_eq "(a) level" "Low" "$(echo "${risk_a}" | jq -r .level)"
check_eq "(a) sha" "221f5" "$(echo "${risk_a}" | jq -r .sha)"
check_eq "(a) head review" "reviewed" \
  "$(cr_head_review "${HEAD_946}" "$(echo "${risk_a}" | jq -r .kind)" "$(echo "${risk_a}" | jq -r .sha)" 0)"

# --- (b) risk sha names the PREVIOUS head: NOT reviewed at head -----------
# This is PR #947. The walkthrough body contains the current head sha (the
# commit-range table names it), so the pre-#950 signals both read "reviewed";
# the risk line names a1231, the head is 093761dd, and the fix commits pushed
# since are unreviewed. The assertion below on the OLD signal pins that.
body_b="$(walkthrough_body "Minimal" "a1231" "${HEAD_947}")"
json_b="$(comments_json 1 "coderabbitai[bot]" "2026-08-29T10:00:00Z" "${body_b}")"
risk_b="$(echo "${json_b}" | cr_risk)"
check_eq "(b) kind" "risk" "$(echo "${risk_b}" | jq -r .kind)"
check_eq "(b) sha" "a1231" "$(echo "${risk_b}" | jq -r .sha)"
check_eq "(b) head review" "stale" \
  "$(cr_head_review "${HEAD_947}" "$(echo "${risk_b}" | jq -r .kind)" "$(echo "${risk_b}" | jq -r .sha)" 1)"
# The pre-#950 signal, on the SAME fixture, says the head was reviewed. Kept as
# an assertion rather than a comment: if the old check ever stops firing here,
# this fixture no longer reproduces the defect and the case above proves less
# than it claims.
old_signal_b="$(echo "${json_b}" | jq --arg sha "${HEAD_947}" -f "${LIBDIR}/coderabbit-verdict.jq")"
check_eq "(b) pre-#950 walkthroughs@head signal is the false 'reviewed'" "1" "${old_signal_b}"
# A risk sha that is a prefix must not be defeated by case or by a full 40-hex
# sha in the risk line.
check_eq "(b) prefix compare is not equality" "reviewed" \
  "$(cr_head_review "${PREV_947}" "risk" "a1231" 0)"

# --- (c) no CodeRabbit comment at all (fresh PR) --------------------------
json_c="$(comments_json 7 "some-human" "2026-08-29T10:00:00Z" "LGTM, merging after CI.")"
risk_c="$(echo "${json_c}" | cr_risk)"
check_eq "(c) kind" "none" "$(echo "${risk_c}" | jq -r .kind)"
check_eq "(c) level" "none" "$(echo "${risk_c}" | jq -r .level)"
check_eq "(c) sha" "none" "$(echo "${risk_c}" | jq -r .sha)"
check_eq "(c) head review with no fallback" "unknown" \
  "$(cr_head_review "${HEAD_946}" "none" "none" 0)"
# An empty array is the same case and must not error under `set -e`.
check_eq "(c) empty comment list" "none" "$(echo '[]' | cr_risk | jq -r .kind)"

# --- (d) a walkthrough with no risk line ----------------------------------
body_d="$(printf '## Walkthrough\n\nSome prose.\n\n%s\n' \
  "No actionable comments were generated in the recent review.")"
json_d="$(comments_json 1 "coderabbitai[bot]" "2026-08-29T10:00:00Z" "${body_d}")"
risk_d="$(echo "${json_d}" | cr_risk)"
check_eq "(d) kind" "no-risk-line" "$(echo "${risk_d}" | jq -r .kind)"
check_eq "(d) sha" "none" "$(echo "${risk_d}" | jq -r .sha)"
# With no sha to read, the older signals decide, and the caller says so rather
# than reporting certainty it does not have.
check_eq "(d) head review falls back" "reviewed-fallback" \
  "$(cr_head_review "${HEAD_946}" "no-risk-line" "none" 1)"
check_eq "(d) head review with no fallback either" "unknown" \
  "$(cr_head_review "${HEAD_946}" "no-risk-line" "none" 0)"

# --- (e) several bot comments: only the NEWEST matters --------------------
# An old walkthrough for the previous head, a rate-limit notice posted after it
# (not a review), and the current walkthrough. The rate-limit notice must not
# mask the walkthrough, and the stale walkthrough must not outrank it.
old_wt="$(walkthrough_body "Moderate" "a1231" "${PREV_947}")"
new_wt="$(walkthrough_body "Low" "221f5" "${HEAD_946}")"
rate_limit="$(printf 'Note: CodeRabbit is on a rate limit. Commits %s and %s were not processed.\n' \
  "${PREV_947}" "${HEAD_946}")"
json_e="$(comments_json \
  1 "coderabbitai[bot]" "2026-08-28T09:00:00Z" "${old_wt}" \
  3 "coderabbitai[bot]" "2026-08-29T12:00:00Z" "${rate_limit}" \
  2 "coderabbitai[bot]" "2026-08-29T11:00:00Z" "${new_wt}")"
risk_e="$(echo "${json_e}" | cr_risk)"
check_eq "(e) newest review comment wins" "221f5" "$(echo "${risk_e}" | jq -r .sha)"
check_eq "(e) level from the newest" "Low" "$(echo "${risk_e}" | jq -r .level)"
check_eq "(e) head review" "reviewed" \
  "$(cr_head_review "${HEAD_946}" "risk" "$(echo "${risk_e}" | jq -r .sha)" 0)"
# The rate-limit notice alone is not a review (the #788 false-CLEAR shape).
json_e2="$(comments_json 3 "coderabbitai[bot]" "2026-08-29T12:00:00Z" "${rate_limit}")"
check_eq "(e) rate-limit notice alone is not a review" "none" \
  "$(echo "${json_e2}" | cr_risk | jq -r .kind)"

# --- (f) findings vs replies ----------------------------------------------
# review_comments_json: repeated id/login/in_reply_to (null or an id) triples.
review_comments_json() {
  local out="[]"
  while (($# >= 3)); do
    out="$(jq -c --argjson arr "${out}" \
      --argjson id "$1" --arg login "$2" --argjson reply "$3" \
      -n '$arr + [{id: $id, user: {login: $login}, in_reply_to_id: $reply}]')"
    shift 3
  done
  printf '%s\n' "${out}"
}

# PR #948: seven findings, no replies. The risk line names its own head because
# that was the review that FOUND them, so "reviewed at head" and "findings
# addressed" must not collapse into one verdict.
json_f="$(review_comments_json \
  101 "coderabbitai[bot]" null \
  102 "coderabbitai[bot]" null \
  103 "coderabbitai[bot]" null \
  104 "coderabbitai[bot]" null \
  105 "coderabbitai[bot]" null \
  106 "coderabbitai[bot]" null \
  107 "coderabbitai[bot]" null)"
threads_f="$(echo "${json_f}" | cr_threads)"
check_eq "(f) findings" "7" "$(echo "${threads_f}" | jq -r .findings)"
check_eq "(f) replies" "0" "$(echo "${threads_f}" | jq -r .replies)"
check_eq "(f) findings state" "outstanding" \
  "$(cr_findings_state 7 0 0)"
check_eq "(f) a head review at head does not answer the findings question" "reviewed" \
  "$(cr_head_review "${HEAD_946}" "risk" "221f5" 0)"

# Inline comments outnumbering replies: three findings, one answered. The bot's
# own reply in a thread is neither a new finding nor an answer.
json_g="$(review_comments_json \
  201 "coderabbitai[bot]" null \
  202 "coderabbitai[bot]" null \
  203 "coderabbitai[bot]" null \
  204 "a-human" 201 \
  205 "coderabbitai[bot]" 201)"
threads_g="$(echo "${json_g}" | cr_threads)"
check_eq "(g) findings exclude the bot's own reply" "3" "$(echo "${threads_g}" | jq -r .findings)"
check_eq "(g) replies count only non-bot answers" "1" "$(echo "${threads_g}" | jq -r .replies)"
check_eq "(g) findings state" "partial" "$(cr_findings_state 3 1 0)"
check_eq "(g) every finding answered is still not a fix" "answered" "$(cr_findings_state 3 3 0)"
check_eq "(g) zero findings" "none" "$(cr_findings_state 0 0 0)"
check_eq "(g) operator confirmation wins" "confirmed" "$(cr_findings_state 3 0 1)"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
if [[ ${fail} -ne 0 ]]; then
  exit 1
fi
