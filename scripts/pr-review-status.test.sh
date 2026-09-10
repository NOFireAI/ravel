#!/usr/bin/env bash
# Cases for the two filters behind scripts/pr-review-status.sh:
# lib/fleet-review-task-state.jq, which classifies the review bot's task
# comment for the head commit into one word and so decides whether a missing
# review means "wait", "re-trigger" or "nobody asked"; and
# lib/fleet-review-outside-diff.jq, which counts the findings the bot reports
# in a review BODY instead of inline.
#
# Fixtures only: no network, no gh. Each case is a comment or review shape the
# bot actually emits (reviewbot/format.go, reviewbot/result.go) or one observed
# on a real PR, named with the PR it came from.
#
# Exit 0 all pass, 1 on a failure.
set -uo pipefail

TASK_FILTER="$(dirname "$0")/lib/fleet-review-task-state.jq"
[[ -r "${TASK_FILTER}" ]] || { echo "missing ${TASK_FILTER}" >&2; exit 64; }
OUTSIDE_FILTER="$(dirname "$0")/lib/fleet-review-outside-diff.jq"
[[ -r "${OUTSIDE_FILTER}" ]] || { echo "missing ${OUTSIDE_FILTER}" >&2; exit 64; }

SHA="0c65a3d3983384d663189257a25cc2d40ca95d32"
OTHER="1111111111111111111111111111111111111111"
BOT="claude-fleet[bot]"
passes=0
fails=0

run_case() {
  local filter="$1" name="$2" want="$3" json="$4" got
  got="$(printf '%s' "${json}" | jq -r --arg sha "${SHA}" --arg bot "${BOT}" -f "${filter}")" || got="ERROR"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${name}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %s: want %s, got %s\n' "${name}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

check_task() { run_case "${TASK_FILTER}" "$@"; }
check_outside() { run_case "${OUTSIDE_FILTER}" "$@"; }

# --- lib/fleet-review-task-state.jq --------------------------------------
#
# The five words this filter emits are five different next actions, and two of
# them are opposites: `running` means wait, `dead` means the review will never
# arrive and the trigger has to be posted again. A filter that collapsed them
# would either hang the gate forever or clear a PR nothing reviewed.

QUEUED_HEAD="Review task queued.\n\nTask: 3d030033-05ed-48d5-98f1-b086b1684655\nCommit: ${SHA}\nModel: executor default (effort high)\nExecutor: pending"
RUNNING_HEAD="Review task queued.\n\nTask: 3d030033-05ed-48d5-98f1-b086b1684655\nCommit: ${SHA}\nModel: executor default (effort high)\nExecutor: pimox5"

check_task "no comments at all is none" none '[]'

check_task "a task comment at a DIFFERENT commit is none" none "$(cat <<EOF
[{"user":{"login":"${BOT}"},
  "body":"Review task queued.\n\nTask: t1\nCommit: ${OTHER}\nModel: opus (effort high)\nExecutor: rp2\nStatus: done. Review posted."}]
EOF
)"

check_task "queued, executor still pending, is running" running "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${QUEUED_HEAD}"}]
EOF
)"

check_task "claimed by an executor, no status line, is running" running "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}"}]
EOF
)"

check_task "done with the review posted is done" "done" "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}\nStatus: done. Review posted."}]
EOF
)"

# The bot posts the agent's raw text as the review when it cannot parse a
# structured result, so a review DOES exist and the gate must read it.
check_task "done (unstructured) is done, because a review was still posted" "done" "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}\nStatus: done (unstructured)."}]
EOF
)"

# Every terminal-without-a-review path the bot writes, one case each. These are
# the cases that must never read as `running`: nothing retries them.
check_task "failed with a class is dead" dead "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}\nStatus: failed (setup). No review posted.\nTranscript: fleet-cp task 3d030033."}]
EOF
)"

check_task "the sidecar losing the task status is dead" dead "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}\nStatus: failed (sidecar could not read the task status). No review posted."}]
EOF
)"

check_task "a timeout is dead" dead "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}\nStatus: timeout. The sidecar stopped waiting after 3h0m0s. No review posted."}]
EOF
)"

check_task "abandoned before enqueue is dead" dead "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${QUEUED_HEAD}\nStatus: abandoned (sidecar restarted before enqueue)."}]
EOF
)"

# The nastiest of the set: the task finished and the agent wrote a review, but
# GitHub refused it. The word "done" is in the line, so a filter that matched
# on it loosely would report a review that does not exist.
check_task "done but the review post failed is dead, not done" dead "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}\nStatus: done, but posting the review failed (422 Unprocessable Entity)."}]
EOF
)"

# A status string this filter has never seen must surface as "read it by hand"
# rather than being folded into whichever branch is checked last.
check_task "an unrecognized status line is unknown" unknown "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}\nStatus: something new upstream."}]
EOF
)"

# Only the bot's own task comment counts. A human pasting the shape (quoting a
# failure while asking about it, say) must not decide the gate.
check_task "a human comment in the task-comment shape is none" none "$(cat <<EOF
[{"user":{"login":"pmoust"},
  "body":"Review task queued.\n\nTask: t1\nCommit: ${SHA}\nStatus: done. Review posted."}]
EOF
)"

# The bot's OTHER comments carry a Commit: line too. Only the comment that
# opens with the task-comment first line is a task comment.
check_task "a bot comment that is not the task comment is none" none "$(cat <<EOF
[{"user":{"login":"${BOT}"},
  "body":"A review task already runs for this commit.\n\nCommit: ${SHA}"}]
EOF
)"

# A Commit: line has to name the whole sha. A prefix match would let a task
# comment for one commit answer for any commit sharing its first hex digits.
check_task "a Commit line naming a prefix of the head is none" none "$(cat <<EOF
[{"user":{"login":"${BOT}"},
  "body":"Review task queued.\n\nTask: t1\nCommit: ${SHA:0:12}\nStatus: done. Review posted."}]
EOF
)"

# Two tasks for one commit: the first went terminal, a re-trigger made another.
# The last one is the live one, so it decides.
check_task "with two task comments for the head, the last decides" running "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":"${RUNNING_HEAD}\nStatus: failed (setup). No review posted."},
 {"user":{"login":"${BOT}"},"body":"${QUEUED_HEAD}"}]
EOF
)"

check_task "null body is tolerated" none "$(cat <<EOF
[{"user":{"login":"${BOT}"},"body":null}]
EOF
)"

# --- lib/fleet-review-outside-diff.jq ------------------------------------
#
# A finding GitHub will not accept inline goes into the review body. Nothing on
# the review-comments endpoint sees it, which is how #908 shipped an
# unaddressed finding while `inline_comments` read as stale-and-fixed.

check_outside "no reviews at all" 0 '[]'

check_outside "a body with no findings block" 0 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"Verdict: comment.\n\nI verified the bloom counter against page.rs:43.\n\nTask t1 on rp2, model opus, effort high."}]
EOF
)"

check_outside "one finding in the body block" 1 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"Verdict: comment.\n\nFindings outside the diff:\n- scripts/foo.sh:12 (major): the guard is inverted.\n\nTask t1 on rp2, model opus, effort high."}]
EOF
)"

check_outside "three findings in the body block" 3 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"Findings outside the diff:\n- a.rs:1: one\n- b.rs:2: two\n- c.rs: three\n\nTask t1 on rp2, model opus, effort high."}]
EOF
)"

# The bot appends a SECOND block when GitHub answers 422 to an inline line it
# will not take (MoveCommentsToBody), so both blocks in one body must count.
check_outside "two blocks in one body both count" 3 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"Findings outside the diff:\n- a.rs:1: one\n- b.rs:2: two\n\nTask t1 on rp2.\n\nFindings outside the diff:\n- c.rs:3: moved out of the diff by a 422\n"}]
EOF
)"

# The "Not checked:" list uses the same bullet syntax EARLIER in the same body.
# Counting bullets only inside an open block is what keeps it out; a regex over
# the whole body would report the gaps a review names as findings, which is a
# false block on every honest review.
check_outside "the Not checked list is not counted as findings" 1 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"Verdict: comment.\n\nNot checked:\n- the k8s manifests\n- the bench lane\n\nFindings outside the diff:\n- a.rs:1: one\n\nTask t1 on rp2."}]
EOF
)"

# Head discipline, the same rule the review count uses: a body on a superseded
# commit describes code that is no longer on the branch.
check_outside "a block on another commit does not count" 0 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${OTHER}",
  "body":"Findings outside the diff:\n- a.rs:1: one\n"}]
EOF
)"

check_outside "a human review with the same block does not count" 0 "$(cat <<EOF
[{"user":{"login":"pmoust"},"commit_id":"${SHA}",
  "body":"Findings outside the diff:\n- a.rs:1: one\n"}]
EOF
)"

check_outside "two reviews at the head sum" 2 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"Findings outside the diff:\n- a.rs:1: one\n"},
 {"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"Findings outside the diff:\n- b.rs:2: two\n"}]
EOF
)"

check_outside "null body is tolerated" 0 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}","body":null}]
EOF
)"

# The heading has to be the whole line. Prose that mentions the phrase mid-line
# does not open a block, so a review discussing this mechanism does not block
# itself.
check_outside "the phrase inside a prose line does not open a block" 0 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"I checked that the Findings outside the diff: list is parsed correctly.\n- this bullet is prose, not a finding\n"}]
EOF
)"

# The accepted limit, pinned so it is a decision rather than a surprise: a body
# that reproduces a whole block verbatim counts those bullets. Over-counting
# costs one read of the body; under-counting ships unfixed findings.
check_outside "a verbatim quoted block still counts (accepted over-count)" 1 "$(cat <<EOF
[{"user":{"login":"${BOT}"},"commit_id":"${SHA}",
  "body":"The bot writes this shape:\n\nFindings outside the diff:\n- a.rs:1: an example finding\n"}]
EOF
)"

# --- end-to-end, with gh and git stood in for ----------------------------

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

# Stand-in for git, so the merge-base guard the script calls runs offline and
# deterministically. Fresh by default; E2E_GIT_STALE=1 puts the pull request's
# base three commits behind main. The unseen-commit subjects carry an escape
# sequence and a carriage return on purpose: the guard is supposed to strip
# both before the text reaches a terminal, and a test below checks that it did.
cat >"${E2E_DIR}/bin/git" <<'SHIM'
#!/usr/bin/env bash
tip=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
old=cccccccccccccccccccccccccccccccccccccccc
resolve() {
  case "$1" in
    *freshness-check*) printf '%s' "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ;;
    [0-9a-f][0-9a-f]*) printf '%s' "$1" ;;
    *) printf '%s' "${tip}" ;;
  esac
}
# E2E_GIT_FAIL_AT names one call the guard makes and fails exactly that one, so
# each of the guard's internal failure paths can be reached on its own. The two
# rev-parse calls are distinguished by which ref they resolve, since reverting
# only the second one's exit code is otherwise invisible.
fail_at="${E2E_GIT_FAIL_AT:-}"
case "$1:${2:-}" in
  "rev-parse:refs/remotes/origin/main")
    [[ "${fail_at}" == "rev-parse-tip" ]] && exit 1 ;;
  "rev-parse:refs/remotes/origin/freshness-check-"*)
    [[ "${fail_at}" == "rev-parse-local" ]] && exit 1 ;;
esac
case "$1" in
  merge-base|rev-list) [[ "${fail_at}" == "$1" ]] && exit 1 ;;
esac
case "$1" in
  fetch)
    # E2E_GIT_FETCH_FAIL=1 makes the fetch fail while the remote still has the
    # pull request ref, which is the guard's "could not answer" path.
    if [[ "${E2E_GIT_FETCH_FAIL:-0}" == "1" ]]; then exit 1; fi
    exit 0
    ;;
  update-ref|ls-remote) exit 0 ;;
  rev-parse)
    if [[ "$2" == "--short" ]]; then
      resolve "$3" | cut -c1-8
    else
      resolve "$2"
      printf '\n'
    fi
    ;;
  merge-base)
    if [[ "${E2E_GIT_STALE:-0}" == "1" ]]; then printf '%s\n' "${old}"
    else printf '%s\n' "${tip}"; fi
    ;;
  rev-list) printf '3\n' ;;
  log) printf 'ccccccc1 innocent\033[31mSPOOFED\033[0m\rsubject\ncccccc2 tab:\there\ncccccc3 third\n' ;;
  *) echo "unexpected git call: $*" >&2; exit 91 ;;
esac
SHIM
chmod +x "${E2E_DIR}/bin/git"

# Everything except the review body is held constant and clean: CI green,
# mergeState CLEAN, one COMMENTED review at head, zero inline comments, and a
# task comment saying the review for the head was posted. So the only thing
# that can move the verdict is the body -- or, in the cases below, one of the
# E2E_* overrides.
DONE_TASK_COMMENT="$(cat <<EOF
[{"user":{"login":"${BOT}"},"created_at":"2026-01-02T00:00:00Z",
  "body":"Review task queued.\n\nTask: t1\nCommit: ${SHA}\nModel: opus (effort high)\nExecutor: rp2\nStatus: done. Review posted."}]
EOF
)"

e2e() {
  local review_body="$1"
  shift
  local fx="${E2E_DIR}/fx"
  rm -rf "${fx}"
  mkdir -p "${fx}"
  local rollup="${E2E_ROLLUP:-}"
  if [[ -z "${rollup}" ]]; then
    rollup='[{"name":"ci","status":"COMPLETED","conclusion":"SUCCESS"}]'
  fi
  printf '{"state":"OPEN","mergeStateStatus":"CLEAN","statusCheckRollup":%s,"headRefOid":"%s"}\n' \
    "${rollup}" "${SHA}" >"${fx}/pr-view.json"
  printf '%s\n' "${E2E_REVIEWS:-$(printf '[{"user":{"login":"%s"},"state":"%s","commit_id":"%s","body":%s}]' \
    "${BOT}" "${E2E_REVIEW_STATE:-COMMENTED}" "${SHA}" "${review_body}")}" >"${fx}/reviews.json"
  printf '%s\n' "${E2E_ISSUE_COMMENTS:-${DONE_TASK_COMMENT}}" >"${fx}/issue-comments.json"
  printf '%s\n' "${E2E_REVIEW_COMMENTS:-[]}" >"${fx}/review-comments.json"
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

CLEAN_BODY_JSON='"Verdict: comment.\n\nI read the diff and the code around it.\n\nTask t1 on rp2, model opus, effort high."'
FINDING_BODY_JSON='"Verdict: comment.\n\nFindings outside the diff:\n- scripts/foo.sh:12 (major): the guard is inverted.\n\nTask t1 on rp2, model opus, effort high."'

clean_out="$(e2e "${CLEAN_BODY_JSON}")"
finding_out="$(e2e "${FINDING_BODY_JSON}")"
confirmed_out="$(e2e "${FINDING_BODY_JSON}" --confirm-addressed)"

# A body with no findings: no extra summary field, and the clean verdict.
check_eq "no findings: summary line carries no outside-diff field" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | review: task@head=done reviews@head=1 last=COMMENTED inline_comments=0" \
  "$(printf '%s\n' "${clean_out}" | sed -n 1p)"
check_eq "no findings: verdict is clean" \
  "  -> clean: CI green, review at the current head with zero findings" \
  "$(printf '%s\n' "${clean_out}" | sed -n 2p)"
check_eq "no findings: merge command printed" \
  "  -> scripts/guards/assert-fresh-merge-base.sh 908 && gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${clean_out}" | sed -n 3p)"

# The #908 regression: same PR, same green CI, same zero inline comments, one
# outside-diff finding in the body. It must be visible and it must block.
check_eq "outside-diff body finding: counted on the summary line" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | review: task@head=done reviews@head=1 last=COMMENTED inline_comments=0 outside_diff_body_findings@head=1" \
  "$(printf '%s\n' "${finding_out}" | sed -n 1p)"
check_eq "outside-diff body finding: verdict is not clean" \
  "  -> 1 outside-diff finding(s) in the review BODY at head, not inline; read the body with \`gh api repos/NOFireAI/ravel/pulls/908/reviews --jq '.[] | select(.commit_id==\"${SHA}\") | .body'\`, then re-run with --confirm-addressed once each is fixed or answered" \
  "$(printf '%s\n' "${finding_out}" | sed -n 2p)"
check_eq "outside-diff body finding: no merge command offered" \
  "" \
  "$(printf '%s\n' "${finding_out}" | sed -n 3p)"

# --confirm-addressed overrides it, in the same shape as the inline branch.
check_eq "outside-diff body finding: --confirm-addressed clears it" \
  "  -> clean (operator confirmed all 1 outside-diff body finding(s) addressed): CI green, review at the current head" \
  "$(printf '%s\n' "${confirmed_out}" | sed -n 2p)"
check_eq "outside-diff body finding: --confirm-addressed prints the merge command" \
  "  -> scripts/guards/assert-fresh-merge-base.sh 908 && gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${confirmed_out}" | sed -n 3p)"

# An inline comment blocks the same way, and names the other flag path.
E2E_REVIEW_COMMENTS="$(printf '[{"user":{"login":"%s"},"body":"the retry drops the error"}]' "${BOT}")"
inline_out="$(e2e "${CLEAN_BODY_JSON}")"
inline_confirmed_out="$(e2e "${CLEAN_BODY_JSON}" --confirm-addressed)"
unset E2E_REVIEW_COMMENTS

check_eq "inline comment: blocks until confirmed" \
  "  -> 1 inline review comment(s); read them, then re-run with --confirm-addressed once each is fixed or answered (the API cannot tell; see the header comment)" \
  "$(printf '%s\n' "${inline_out}" | sed -n 2p)"
check_eq "inline comment: --confirm-addressed clears it" \
  "  -> clean (operator confirmed all 1 inline comment(s) addressed): CI green, review at the current head" \
  "$(printf '%s\n' "${inline_confirmed_out}" | sed -n 2p)"

# --- no review at head: five states, five different next actions ---------
#
# Everything else in these fixtures stays clean, so the ONLY thing that moves
# the verdict is what the review and the task comment say. Under a check that
# only asked "is there a review object", four of these five read identically.

E2E_REVIEWS='[]'

E2E_ISSUE_COMMENTS='[]'
noask_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "nobody asked: summary says no task at head" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | review: task@head=none reviews@head=0 last=none inline_comments=0" \
  "$(printf '%s\n' "${noask_out}" | sed -n 1p)"
check_eq "nobody asked: verdict says to post the trigger" \
  "  -> no review at head and nobody asked for one: comment \`@claude-fleet review\` on the PR (that exact body, arguments after \`review\` are parsed and an unrecognized word gets a confused reaction and no review)" \
  "$(printf '%s\n' "${noask_out}" | sed -n 2p)"
check_eq "nobody asked: no merge command offered" \
  "0" \
  "$(printf '%s\n' "${noask_out}" | grep -c 'gh pr merge')"

# Asked, but no task comment came back: the mention was malformed (a confused
# reaction and nothing else) or the app is not installed. Distinguishing this
# from "nobody asked" is the whole point -- the operator's next move differs.
E2E_ISSUE_COMMENTS='[{"user":{"login":"pmoust"},"body":"@claude-fleet review this PR carefully"}]'
malformed_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "asked but no task: verdict says the mention did not take" \
  "  -> 1 \`@claude-fleet review\` comment(s) but no task comment for ${SHA}: if the last one is seconds old, the task comment lands within seconds, so re-run this; otherwise the mention was malformed (check for a confused reaction on it), the app is not installed here, or the bot is not receiving deliveries" \
  "$(printf '%s\n' "${malformed_out}" | sed -n 2p)"

# The bot quoting the trigger in its own comment is not somebody asking.
E2E_ISSUE_COMMENTS="$(printf '[{"user":{"login":"%s"},"body":"Write @claude-fleet review to start a review."}]' "${BOT}")"
botquote_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "the bot quoting the trigger does not count as asking" \
  "  -> no review at head and nobody asked for one: comment \`@claude-fleet review\` on the PR (that exact body, arguments after \`review\` are parsed and an unrecognized word gets a confused reaction and no review)" \
  "$(printf '%s\n' "${botquote_out}" | sed -n 2p)"

E2E_ISSUE_COMMENTS="$(printf '[{"user":{"login":"%s"},"body":"Review task queued.\\n\\nTask: t1\\nCommit: %s\\nModel: opus (effort high)\\nExecutor: pending"}]' "${BOT}" "${SHA}")"
running_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "task running: verdict says wait" \
  "  -> review task for ${SHA} is queued or running; wait (the bot edits its task comment in place, and posts the review when the task finishes)" \
  "$(printf '%s\n' "${running_out}" | sed -n 2p)"

# The state that must never read as "wait": nothing retries a failed task, so a
# check that folded this into `running` waits forever.
E2E_ISSUE_COMMENTS="$(printf '[{"user":{"login":"%s"},"body":"Review task queued.\\n\\nTask: t1\\nCommit: %s\\nModel: opus (effort high)\\nExecutor: rp2\\nStatus: failed (setup). No review posted."}]' "${BOT}" "${SHA}")"
dead_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "task dead: verdict says re-trigger, not wait" \
  "  -> review task for ${SHA} went terminal with NO review posted; nothing will arrive, so re-trigger with \`@claude-fleet review\` (read the task comment for the failure class: \`gh api repos/NOFireAI/ravel/issues/908/comments --jq '.[] | select(.user.login==\"${BOT}\") | .body'\`)" \
  "$(printf '%s\n' "${dead_out}" | sed -n 2p)"
check_eq "task dead: no merge command offered" \
  "0" \
  "$(printf '%s\n' "${dead_out}" | grep -c 'gh pr merge')"

# The task says it posted a review and no review object is there. Whatever that
# is, it is not a merge: say so instead of clearing on the task comment alone.
donenoreview_out="$(e2e "${CLEAN_BODY_JSON}")"

check_eq "task done but no review object: verdict says check by hand" \
  "  -> the task comment for ${SHA} says the review was posted, but no review object at that commit is visible; check by hand before merging" \
  "$(printf '%s\n' "${donenoreview_out}" | sed -n 2p)"

E2E_ISSUE_COMMENTS="$(printf '[{"user":{"login":"%s"},"body":"Review task queued.\\n\\nTask: t1\\nCommit: %s\\nModel: opus (effort high)\\nExecutor: rp2\\nStatus: something new upstream."}]' "${BOT}" "${SHA}")"
unknown_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ISSUE_COMMENTS

check_eq "task in an unrecognized state: verdict says read it by hand" \
  "  -> review task for ${SHA} is in an unrecognized state (unknown); read its task comment by hand before merging" \
  "$(printf '%s\n' "${unknown_out}" | sed -n 2p)"

# A review for an EARLIER commit is the case where the head moved after a
# review. It must not clear the head, and the count belongs on the line so the
# operator can tell it from "never reviewed".
E2E_REVIEWS="$(printf '[{"user":{"login":"%s"},"state":"COMMENTED","commit_id":"%s","body":"Verdict: comment."}]' "${BOT}" "${OTHER}")"
E2E_ISSUE_COMMENTS="$(printf '[{"user":{"login":"%s"},"body":"Review task queued.\\n\\nTask: t1\\nCommit: %s\\nModel: opus (effort high)\\nExecutor: rp2\\nStatus: done. Review posted."}]' "${BOT}" "${OTHER}")"
stalereview_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_REVIEWS E2E_ISSUE_COMMENTS

check_eq "review at an older commit: reported on the summary line" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail | review: task@head=none reviews@head=0 last=none inline_comments=0 reviews_at_older_commits=1" \
  "$(printf '%s\n' "${stalereview_out}" | sed -n 1p)"
check_eq "review at an older commit: does not clear the head" \
  "  -> no review at head and nobody asked for one: comment \`@claude-fleet review\` on the PR (that exact body, arguments after \`review\` are parsed and an unrecognized word gets a confused reaction and no review)" \
  "$(printf '%s\n' "${stalereview_out}" | sed -n 2p)"
check_eq "review at an older commit: the head-moved note is printed" \
  "     (1 review(s) exist at older commits; the head moved after them, so they do not cover it)" \
  "$(printf '%s\n' "${stalereview_out}" | sed -n 3p)"

# Proof this pins the rule rather than passing anyway: flip the single marked
# condition off, which leaves a check that clears on green CI alone, and the
# SAME unreviewed fixture must come back clean with a merge command.
FLIP_DIR="${E2E_DIR}/flip"
mkdir -p "${FLIP_DIR}"
ln -s "$(cd "$(dirname "$0")" && pwd)/lib" "${FLIP_DIR}/lib"
# The copy resolves its own script_dir, so the guard has to be reachable from
# there too; without this the flipped script cannot run the guard, reads that
# as a refusal, and the case proves nothing.
ln -s "$(cd "$(dirname "$0")" && pwd)/guards" "${FLIP_DIR}/guards"
sed 's/^elif .*# PROVE-FLIP$/elif false; then/' \
  "$(dirname "$0")/pr-review-status.sh" >"${FLIP_DIR}/pr-review-status.sh"
if grep -q '^elif false; then$' "${FLIP_DIR}/pr-review-status.sh"; then
  printf 'ok    %s\n' "prove: PROVE-FLIP line found and flipped"
  passes=$((passes + 1))
else
  printf 'FAIL  %s\n' "prove: PROVE-FLIP line found and flipped: sed did not match the marked line"
  fails=$((fails + 1))
fi

E2E_REVIEWS='[]' E2E_ISSUE_COMMENTS='[]' E2E_SCRIPT="${FLIP_DIR}/pr-review-status.sh"
flipped_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_REVIEWS E2E_ISSUE_COMMENTS E2E_SCRIPT

check_eq "prove: without the review conjunct, an unreviewed PR reads clean" \
  "  -> clean: CI green, review at the current head with zero findings" \
  "$(printf '%s\n' "${flipped_out}" | sed -n 2p)"
check_eq "prove: without it, the merge command is even offered" \
  "  -> scripts/guards/assert-fresh-merge-base.sh 908 && gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${flipped_out}" | sed -n 3p)"

unset E2E_REVIEWS

# A review state that is neither APPROVED nor COMMENTED blocks. The bot never
# approves and never requests changes, so this catches a dismissed review or a
# human's CHANGES_REQUESTED, and it can only ever block, never clear.
E2E_REVIEW_STATE=DISMISSED
dismissed_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_REVIEW_STATE

check_eq "a DISMISSED review at head blocks" \
  "  -> the current-head review state is DISMISSED (need APPROVED or COMMENTED); not clean" \
  "$(printf '%s\n' "${dismissed_out}" | sed -n 2p)"

# COMMENTED is the bot's success state, so it must NOT be treated as "not
# approved, therefore not clean" -- the first e2e case above already proves
# that. APPROVED is accepted too, for a human review on the same head.
E2E_REVIEW_STATE=APPROVED
approved_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_REVIEW_STATE

check_eq "an APPROVED review at head is also clean" \
  "  -> clean: CI green, review at the current head with zero findings" \
  "$(printf '%s\n' "${approved_out}" | sed -n 2p)"

# --- merge-base freshness end-to-end --------------------------------------
#
# Same clean fixture throughout; the only thing that moves is what git says
# about the base. The fresh direction is already covered by the very first e2e
# case, which comes back clean with a merge command, so these two cover the
# ways the check can refuse, and the third covers not being able to check.

# Exported, not just assigned: unlike E2E_ISSUE_COMMENTS, which e2e() reads
# itself, this one is read by the stand-in git two processes down.
export E2E_GIT_STALE=1
stale_base_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_GIT_STALE

check_eq "stale merge base: blocks a verdict that is otherwise clean" \
  "  -> merge base is behind origin/main; rebase and let CI re-run before merging" \
  "$(printf '%s\n' "${stale_base_out}" | sed -n 2p)"
check_eq "stale merge base: no merge command offered" \
  "0" \
  "$(printf '%s\n' "${stale_base_out}" | grep -c 'gh pr merge')"
# The stand-in git puts an escape sequence and a carriage return in the first
# unseen subject. Both must be gone by the time the text is printed: a subject
# is attacker-controlled and this output is what the operator reads before
# deciding to merge.
check_eq "stale merge base: printed commit subjects carry no control bytes" \
  "0" \
  "$(printf '%s' "${stale_base_out}" | LC_ALL=C tr -cd '\000-\010\013-\037\177' | wc -c | tr -d ' ')"
# The other half of the filter's claim. Counting bytes in the complement of the
# code's own delete set cannot see a set that is too WIDE, and a set eating tab
# and newline would flatten this listing into one line while still passing the
# count above. So: the tab in the second stub subject survives, and the three
# subjects are still three prefixed lines.
check_eq "stale merge base: the filter keeps tab" \
  "1" \
  "$(printf '%s' "${stale_base_out}" | LC_ALL=C grep -cF "$(printf 'tab:\there')")"
check_eq "stale merge base: one prefixed line per unseen commit" \
  "3" \
  "$(printf '%s\n' "${stale_base_out}" | grep -c '^ \{1,\}c\{1,\}[123] ')"

# A guard that cannot run at all must not be reported as a stale base: one is
# fixed by rebasing and the other by fixing the checkout, and the headline is
# the line the operator acts on.
NOGUARD_DIR="${E2E_DIR}/noguard"
mkdir -p "${NOGUARD_DIR}"
ln -s "$(cd "$(dirname "$0")" && pwd)/lib" "${NOGUARD_DIR}/lib"
cp "$(dirname "$0")/pr-review-status.sh" "${NOGUARD_DIR}/pr-review-status.sh"
E2E_SCRIPT="${NOGUARD_DIR}/pr-review-status.sh"
noguard_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_SCRIPT

check_eq "a guard that cannot run is not reported as a stale base" \
  "  -> could not check merge-base freshness (guard exit 127); check by hand before merging" \
  "$(printf '%s\n' "${noguard_out}" | sed -n 2p)"

# The guard's own half of that contract: 2 for a question it cannot answer, and
# 1 kept for the stale verdict. The case above only exercises bash's 127 for a
# missing file, which the guard never reaches, so without these two the guard
# could go back to exiting 1 on every internal failure unnoticed.
guard_direct_rc=0
"$(dirname "$0")/guards/assert-fresh-merge-base.sh" not-a-number >/dev/null 2>&1 \
  || guard_direct_rc=$?
check_eq "the guard exits 2 on an argument it cannot use" \
  "2" \
  "${guard_direct_rc}"

export E2E_GIT_FETCH_FAIL=1
fetchfail_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_GIT_FETCH_FAIL

check_eq "a fetch that fails is reported as could-not-check, not as behind" \
  "  -> could not check merge-base freshness (guard exit 2); check by hand before merging" \
  "$(printf '%s\n' "${fetchfail_out}" | sed -n 2p)"
# And which diagnostic it chose: the guard asks the remote whether the pull
# request ref exists at all, and reporting "no such pull request" for a fetch
# that merely failed sends the operator somewhere else entirely.
check_eq "a fetch that fails says so, rather than blaming the pull request" \
  "     git fetch origin (main, refs/pull/908/head) failed" \
  "$(printf '%s\n' "${fetchfail_out}" | sed -n 3p)"

# The guard's remaining internal failure paths, one at a time, so that a revert
# of any single `exit 2` in it shows up as a verdict of "behind" here. Without
# these only two of its seven such sites are pinned.
for failing_call in rev-parse-tip rev-parse-local merge-base; do
  export E2E_GIT_FAIL_AT="${failing_call}"
  failed_call_out="$(e2e "${CLEAN_BODY_JSON}")"
  unset E2E_GIT_FAIL_AT
  check_eq "a failing ${failing_call} is reported as could-not-check" \
    "  -> could not check merge-base freshness (guard exit 2); check by hand before merging" \
    "$(printf '%s\n' "${failed_call_out}" | sed -n 2p)"
done

# rev-list runs only after the base is known to be behind, so this one needs
# the stale mode too.
export E2E_GIT_STALE=1 E2E_GIT_FAIL_AT=rev-list
revlist_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_GIT_STALE E2E_GIT_FAIL_AT
check_eq "a failing rev-list is reported as could-not-check" \
  "  -> could not check merge-base freshness (guard exit 2); check by hand before merging" \
  "$(printf '%s\n' "${revlist_out}" | sed -n 2p)"

# The caller pins the guard to its own checkout with a `cd`, and that `cd`
# failing is its own outcome rather than a stale base: left bare in the `&&` it
# exits 1, the code reserved for "behind", and the operator is told to rebase
# over a guard that never ran. Reaching it needs the script's directory to
# disappear after the last thing the script reads from there, which is the
# outside-diff jq filter, so this jq passes every call through to the real one
# and removes the tree once that filter has been served.
VANISH_DIR="${E2E_DIR}/vanish"
mkdir -p "${VANISH_DIR}/scripts" "${VANISH_DIR}/bin"
ln -s "$(cd "$(dirname "$0")" && pwd)/lib" "${VANISH_DIR}/scripts/lib"
ln -s "$(cd "$(dirname "$0")" && pwd)/guards" "${VANISH_DIR}/scripts/guards"
cp "$(dirname "$0")/pr-review-status.sh" "${VANISH_DIR}/scripts/pr-review-status.sh"
REAL_JQ="$(command -v jq)"
cat >"${VANISH_DIR}/bin/jq" <<SHIM
#!/usr/bin/env bash
"${REAL_JQ}" "\$@"
rc=\$?
case "\$*" in
  *fleet-review-outside-diff.jq*) rm -rf "${VANISH_DIR}/scripts" ;;
esac
exit \$rc
SHIM
chmod +x "${VANISH_DIR}/bin/jq"

vanished_out="$(E2E_SCRIPT="${VANISH_DIR}/scripts/pr-review-status.sh" \
  PATH="${VANISH_DIR}/bin:${PATH}" e2e "${CLEAN_BODY_JSON}")"

check_eq "a cd that fails is reported as could-not-check, not as behind" \
  "  -> could not check merge-base freshness (guard exit 2); check by hand before merging" \
  "$(printf '%s\n' "${vanished_out}" | sed -n 2p)"

# The usage path, which no e2e case can reach because the caller always passes
# the pull request number it was given.
guard_noarg_rc=0
"$(dirname "$0")/guards/assert-fresh-merge-base.sh" >/dev/null 2>&1 || guard_noarg_rc=$?
check_eq "the guard exits 2 when given no argument at all" \
  "2" \
  "${guard_noarg_rc}"

# --- skipped checks are not passes -------------------------------------
#
# A path-filtered pull request (a docs-only change, say) comes back with every
# required check COMPLETED/SKIPPED. GitHub treats that as satisfying the
# ruleset, so the merge is allowed and nothing here should block it -- but
# folding those into the pass count reported "19 pass" for a pull request on
# which nothing ran, which is what an operator reads as "the suite covered
# this change".

E2E_ROLLUP='[{"name":"check","status":"COMPLETED","conclusion":"SKIPPED"},{"name":"lint","status":"COMPLETED","conclusion":"SKIPPED"}]'
all_skipped_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ROLLUP

check_eq "all checks skipped: counted as skipped, not as passes" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=0 pass/0 pending/0 fail/2 skipped | review: task@head=done reviews@head=1 last=COMMENTED inline_comments=0" \
  "$(printf '%s\n' "${all_skipped_out}" | sed -n 1p)"
check_eq "all checks skipped: the verdict does not claim CI green" \
  "  -> clean: every check skipped, nothing ran, review at the current head with zero findings" \
  "$(printf '%s\n' "${all_skipped_out}" | sed -n 2p)"
check_eq "all checks skipped: the merge command is still offered" \
  "  -> scripts/guards/assert-fresh-merge-base.sh 908 && gh pr merge 908 --rebase --delete-branch --match-head-commit ${SHA}" \
  "$(printf '%s\n' "${all_skipped_out}" | sed -n 3p)"

# The mirror: one real pass beside one skip still says CI green, so the case
# above is not passing because the phrase changed unconditionally.
E2E_ROLLUP='[{"name":"check","status":"COMPLETED","conclusion":"SUCCESS"},{"name":"k8s","status":"COMPLETED","conclusion":"SKIPPED"}]'
mixed_out="$(e2e "${CLEAN_BODY_JSON}")"
unset E2E_ROLLUP

check_eq "one pass beside one skip: both counted, separately" \
  "PR #908 @ ${SHA}: state=OPEN mergeState=CLEAN CI=1 pass/0 pending/0 fail/1 skipped | review: task@head=done reviews@head=1 last=COMMENTED inline_comments=0" \
  "$(printf '%s\n' "${mixed_out}" | sed -n 1p)"
check_eq "one pass beside one skip: the verdict still says CI green" \
  "  -> clean: CI green, review at the current head with zero findings" \
  "$(printf '%s\n' "${mixed_out}" | sed -n 2p)"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
