#!/usr/bin/env bash
# Land a claude-fleet dispatched task's result branch on main via a pull
# request. `main` has required branch protection (PR required, required
# status checks, linear history / rebase-merge only, no direct pushes), so
# this script never merges or pushes to main itself. It cleans the result
# branch's history (see the squash step below), pushes the cleaned history
# to a PR head branch, and opens a PR.
#
# The PR is opened WITHOUT auto-merge by default (standing rule, 2026-08-26):
# CodeRabbit's GitHub App reviews every PR but posts as a review comment, not
# a required status check, so `--auto` merges before that review lands.
# #749/#750 landed with 6 real CodeRabbit findings unaddressed this way. Wait
# for the coderabbitai[bot] review, fix or explicitly answer every actionable
# finding it raises (a walkthrough-only comment with zero findings counts as
# clean), then merge by hand once CI is green:
#   gh pr merge <n> --rebase
# `scripts/pr-review-status.sh <n>` prints CI + CodeRabbit status in one line.
# FLEET_MERGE_AUTO=1 restores the old behavior (`gh pr merge --auto --rebase`)
# for a case that genuinely does not need a CodeRabbit wait; do not set this
# out of impatience, only when you have already confirmed no review applies.
#
# Run fleet-result-inspect.sh first and review its output; this script
# does not pause for review, it assumes you already decided the diff scope
# is correct.
#
# Usage: fleet-result-merge.sh <task-id> <message-file> [-p CRATE ...]
#
# <message-file> is a path to a file already containing the full commit
# message (write it with your editor / the Write tool first; this script
# does not construct one). Its first line becomes the PR title and the rest
# (after the blank line) becomes the PR body, so put any "Fixes: #N" /
# "Refs: #N" trailers in that file yourself.
#
# Trailing "-p CRATE ..." arguments are passed through to gates.sh to scope
# the local pre-flight gate run; omit them to run the full workspace gates
# (the default, and the right choice whenever the merge touches more than
# one crate). CI re-runs the full required checks on the PR regardless.
#
# FLEET_MERGE_DRY_RUN=1 stops after the history-cleaning step and prints the
# cleaned commits without running gates, pushing, or opening a PR. Useful for
# previewing the cleaned history; harmless in production but never lands
# anything.
set -euo pipefail

# --- History-cleaning helpers --------------------------------------------
#
# These are defined before any argument handling so the file can be sourced
# to get the rewrite without running a merge; that is what
# scripts/tests/fleet-result-merge-squash.test.sh drives.
#
# Two classes of commit have reached protected main through fleet result
# branches and must never survive into the merge:
#
#   1. `wip:` headers: executors that commit work-in-progress snapshots
#      and never reword them.
#   2. Pure formatting/style fixups: a commit appended after a failed
#      `cargo fmt --all --check` whose only content is reformatting.
#
# Detection heuristic (documented so it can be tuned):
#   - wip: subject line begins with `wip:` (case-insensitive).
#   - formatting fixup: subject contains `fmt` or `style fix`
#     (case-insensitive) AND the commit's diff against its parent is empty
#     under `git diff -w` (all changes are whitespace/indentation only).
#     `-w` will not catch a reformat that rewraps lines, so this is a
#     conservative filter: it squashes only the unambiguous cases and
#     leaves anything with a real content change untouched.

# Return the leading `wip:` (case-insensitive) stripped from a subject.
strip_wip() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  shopt -s nocasematch
  if [[ "${s}" == wip:* ]]; then
    s="${s#*:}"
  fi
  shopt -u nocasematch
  s="${s#"${s%%[![:space:]]*}"}"
  printf '%s' "${s}"
}

# True if a subject already opens with a Conventional Commits type token.
has_cc_type() {
  [[ "$1" =~ ^[a-zA-Z]+(\([^\)]+\))?!?:[[:space:]] ]]
}

# True if <sha> is a commit the rewrite must not keep on its own, per the
# heuristic above.
is_flagged_commit() {
  local sha="$1" subject parent flagged=0
  subject="$(git log -1 --format=%s "${sha}")"
  shopt -s nocasematch
  if [[ "${subject}" == wip:* ]]; then
    flagged=1
  elif [[ "${subject}" == *fmt* || "${subject}" == *"style fix"* ]]; then
    parent="$(git rev-parse "${sha}^")"
    if [[ -z "$(git diff -w "${parent}" "${sha}")" ]]; then
      flagged=1
    fi
  fi
  shopt -u nocasematch
  [[ ${flagged} -eq 1 ]]
}

# Print <message> with every trailer of <donor-message> that it does not
# already carry appended to it, so a Refs:/Fixes:/Signed-off-by: that lived
# only on a folded commit is never dropped.
merge_trailers() {
  local message="$1" donor="$2" tl
  local -a targs=()
  while IFS= read -r tl; do
    if [[ -n "${tl}" ]]; then
      targs+=(--trailer "${tl}")
    fi
  done < <(printf '%s\n' "${donor}" | git interpret-trailers --parse)
  if [[ ${#targs[@]} -gt 0 ]]; then
    printf '%s\n' "${message}" | git interpret-trailers \
      --if-exists addIfDifferent --if-missing add "${targs[@]}"
  else
    printf '%s\n' "${message}"
  fi
}

# Rebuild <base>..<result-sha> linearly on <rewrite-branch>, folding every
# flagged commit into the substantive commit it belongs to, and print the
# resulting commit sha. Progress output goes to stderr so the sha is the
# only thing on stdout. Leaves HEAD on <rewrite-branch>.
#
# A flagged commit that FOLLOWS a substantive one folds backwards into it.
# A flagged commit that PRECEDES every substantive one folds forwards into
# the first substantive commit instead: the commit-early practice puts a
# `wip:` snapshot first on nearly every fleet branch, and keeping it as a
# separate commit ahead of the deliverable splits one change into two
# commits on main. The deliverable's message, author and sign-off win; the
# wip content rides along, and any trailer that lived only on the wip
# commit is carried over.
#
# A branch whose commits are ALL flagged has nothing to fold into, so its
# first commit is reworded instead: the `wip:` prefix is stripped and, if
# what remains has no Conventional Commits type, `chore:` is prepended so
# the subject stays valid. <fallback-label> names the task in the subject
# of a wip commit that has no text left after stripping.
fleet_rewrite_history() {
  local base="$1" result_sha="$2" rewrite_branch="$3" fallback_label="$4"
  local sha subject stripped new_subject body msg_file
  # retained: HEAD already holds a commit rebuilt from this branch.
  # pending_wip: that commit is a provisional wip fold still looking for the
  # deliverable to be folded into.
  local retained=0 pending_wip=0

  {
    git checkout -q -B "${rewrite_branch}" "${base}"

    while IFS= read -r sha; do
      if ! is_flagged_commit "${sha}"; then
        if [[ ${pending_wip} -eq 1 ]]; then
          # Fold the leading wip content forwards into this deliverable,
          # whose message and author replace the provisional ones.
          git cherry-pick -n "${sha}"
          msg_file="$(mktemp)"
          merge_trailers "$(git log -1 --format=%B "${sha}")" \
            "$(git log -1 --format=%B HEAD)" >"${msg_file}"
          # --amend keeps the amended commit's author unless it is given a
          # new one explicitly; GIT_AUTHOR_* alone would leave the wip
          # commit's author on the deliverable.
          git commit --amend --no-verify --allow-empty \
            --author="$(git log -1 --format='%an <%ae>' "${sha}")" \
            --date="$(git log -1 --format=%aI "${sha}")" \
            -F "${msg_file}"
          rm -f "${msg_file}"
          pending_wip=0
        else
          # Ordinary commit: replay verbatim (author preserved by cherry-pick).
          git cherry-pick "${sha}"
        fi
        retained=1
        continue
      fi

      if [[ ${retained} -eq 0 ]]; then
        # Nothing retained to fold into yet: commit provisionally under a
        # valid subject. The next substantive commit amends this message
        # away; it survives only on an all-flagged branch.
        subject="$(git log -1 --format=%s "${sha}")"
        stripped="$(strip_wip "${subject}")"
        if [[ -z "${stripped}" ]]; then
          new_subject="chore: fleet result for task ${fallback_label}"
        elif has_cc_type "${stripped}"; then
          new_subject="${stripped}"
        else
          new_subject="chore: ${stripped}"
        fi
        body="$(git log -1 --format=%b "${sha}")"
        msg_file="$(mktemp)"
        if [[ -n "${body}" ]]; then
          printf '%s\n\n%s\n' "${new_subject}" "${body}" >"${msg_file}"
        else
          printf '%s\n' "${new_subject}" >"${msg_file}"
        fi
        git cherry-pick -n "${sha}"
        GIT_AUTHOR_NAME="$(git log -1 --format=%an "${sha}")" \
        GIT_AUTHOR_EMAIL="$(git log -1 --format=%ae "${sha}")" \
        GIT_AUTHOR_DATE="$(git log -1 --format=%aI "${sha}")" \
          git commit --no-verify -F "${msg_file}"
        rm -f "${msg_file}"
        retained=1
        pending_wip=1
      else
        # Fold into whatever HEAD holds, preserving its trailers.
        git cherry-pick -n "${sha}"
        msg_file="$(mktemp)"
        merge_trailers "$(git log -1 --format=%B HEAD)" \
          "$(git log -1 --format=%B "${sha}")" >"${msg_file}"
        git commit --amend --no-verify --allow-empty -F "${msg_file}"
        rm -f "${msg_file}"
      fi
    done < <(git rev-list --reverse "${base}..${result_sha}")
  } >&2

  git rev-parse HEAD
}

# Sourced rather than executed: stop here, with the helpers above defined and
# nothing run. Everything below this line performs a merge.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  return 0
fi

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <task-id> <message-file> [-p CRATE ...]" >&2
  exit 64
fi

task_id="$1"
message_file="$2"
shift 2

if [[ ! -f "${message_file}" ]]; then
  echo "fleet-result-merge.sh: message file not found: ${message_file}" >&2
  exit 66
fi

result_ref="refs/heads/task/${task_id}/result"
start_ref="refs/heads/task/${task_id}/start"
pr_branch="task/${task_id}/merge"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dry_run="${FLEET_MERGE_DRY_RUN:-0}"

# --- Recovery trap -------------------------------------------------------
# A failure anywhere below (a cherry-pick conflict, an aborted gate run, a
# killed process) must never leave the caller's checkout mid-rewrite or on
# a stray detached HEAD. This aborts any in-progress cherry-pick/rebase,
# restores the ref the script started on, and drops the temp rewrite branch.
start_checkout=""
rewrite_branch=""
_cleaned=0
cleanup() {
  [[ ${_cleaned} -eq 1 ]] && return
  _cleaned=1
  local gp
  gp="$(git rev-parse --git-path rebase-merge 2>/dev/null || true)"
  if [[ -n "${gp}" && -d "${gp}" ]]; then git rebase --abort 2>/dev/null || true; fi
  gp="$(git rev-parse --git-path rebase-apply 2>/dev/null || true)"
  if [[ -n "${gp}" && -d "${gp}" ]]; then git rebase --abort 2>/dev/null || true; fi
  gp="$(git rev-parse --git-path CHERRY_PICK_HEAD 2>/dev/null || true)"
  if [[ -n "${gp}" && -f "${gp}" ]]; then git cherry-pick --abort 2>/dev/null || true; fi
  if [[ -n "${start_checkout}" ]]; then git checkout -q "${start_checkout}" 2>/dev/null || true; fi
  if [[ -n "${rewrite_branch}" ]]; then git branch -D "${rewrite_branch}" 2>/dev/null || true; fi
}
trap cleanup EXIT
trap cleanup ERR

# --- Pre-flight guard ----------------------------------------------------
# Authenticate first: a dead token found at the push step, after the
# rewrite and gates, strands finished work. Fail here while it is cheap.
"${script_dir}/guards/assert-gh-auth.sh"

# Compute the merge base against main, not against whatever HEAD happens to
# be: a HEAD pointing at an old commit would yield a too-old base and replay
# already-landed commits as duplicates. Require HEAD to be main (or a
# detached HEAD sitting exactly on origin/main) and the tree to be clean.
git fetch -q origin main
current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "${current_branch}" != "main" ]]; then
  if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
    echo "fleet-result-merge.sh: refusing to run from '${current_branch}':" >&2
    echo "  HEAD is neither 'main' nor a detached checkout of origin/main." >&2
    echo "  Check out main (git checkout main && git pull) and retry." >&2
    exit 65
  fi
fi
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "fleet-result-merge.sh: working tree has uncommitted changes; commit or stash first." >&2
  exit 65
fi
start_checkout="${current_branch}"
if [[ "${start_checkout}" == "HEAD" ]]; then
  start_checkout="$(git rev-parse HEAD)"
fi

echo "==> Fetching ${result_ref}"
git fetch origin "${result_ref}"
result_sha="$(git rev-parse FETCH_HEAD)"

base="$(git merge-base origin/main "${result_sha}")"

# --- Reject branches with merge commits ----------------------------------
# The squash step below rebuilds history linearly with cherry-pick, which
# cannot replay a merge commit. Detect that up front and abort with a clear
# message rather than corrupting history halfway through.
if [[ -n "$(git rev-list --merges "${base}..${result_sha}")" ]]; then
  echo "fleet-result-merge.sh: result branch contains merge commit(s):" >&2
  git log --oneline --merges "${base}..${result_sha}" >&2
  echo "  This script only handles linear result branches. Flatten the" >&2
  echo "  branch manually (or re-dispatch without the merge) and retry." >&2
  exit 65
fi

# --- Squash wip:/formatting-fixup commits out of the result branch -------
# The detection heuristic and the fold rules are documented on
# is_flagged_commit and fleet_rewrite_history at the top of this file.
need_rewrite=0
while IFS= read -r c; do
  if is_flagged_commit "${c}"; then
    need_rewrite=1
  fi
done < <(git rev-list --reverse "${base}..${result_sha}")

clean_ref="${result_sha}"

if [[ ${need_rewrite} -eq 1 ]]; then
  echo "==> Squashing wip:/formatting-fixup commits out of the result branch"
  rewrite_branch="_fleet_rewrite_${task_id//\//_}"
  clean_ref="$(fleet_rewrite_history "${base}" "${result_sha}" "${rewrite_branch}" "${task_id}")"
  git checkout -q "${start_checkout}"
fi
# --- end squash step -----------------------------------------------------

# --- Rewrite authorship and sign-off to the merging identity -------------
# Every commit that lands on main is authored by the person landing it, not
# by the disposable fleet-executor identity the dispatched clone committed
# under (fleet clones set user.email=fleet-executor@nofire.ai for their own
# commits; that identity must never reach main). The squash step above
# preserves the executor author on cherry-pick, and a clean result branch
# (no wip/fmt commits) skips that step entirely and would otherwise land
# verbatim as <fleet-executor@nofire.ai>. Once such a commit lands on
# protected main it cannot be rewritten, so this pass must catch it first.
#
# This pass ALWAYS runs. It replays every retained commit onto the base with
# the merger's git identity as author and committer, preserving the original
# author date, dropping any fleet-executor sign-off, and adding the merger's
# own DCO sign-off. The result is the cleaned, correctly-attributed history
# that gets pushed to the PR branch.
merge_name="$(git config user.name || true)"
merge_email="$(git config user.email || true)"
if [[ -z "${merge_name}" || -z "${merge_email}" ]]; then
  echo "fleet-result-merge.sh: git user.name and user.email must be set so" >&2
  echo "  the landed commits are authored under the merger's identity, not" >&2
  echo "  the fleet-executor identity. Set them and retry." >&2
  exit 65
fi
prev_rewrite_branch="${rewrite_branch}"
authorship_branch="_fleet_authorship_${task_id//\//_}"
rewrite_branch="${authorship_branch}"
git checkout -q -B "${authorship_branch}" "${base}"
while IFS= read -r sha; do
  git cherry-pick -n "${sha}"
  msg_file="$(mktemp)"
  # Keep every trailer except a sign-off from the fleet-executor identity;
  # the merger's sign-off is (re)added by `git commit -s` below.
  git log -1 --format=%B "${sha}" \
    | grep -viE '^Signed-off-by:[[:space:]]+.*<fleet-executor@nofire\.ai>[[:space:]]*$' \
    >"${msg_file}"
  GIT_AUTHOR_NAME="${merge_name}" GIT_AUTHOR_EMAIL="${merge_email}" \
  GIT_AUTHOR_DATE="$(git log -1 --format=%aI "${sha}")" \
  GIT_COMMITTER_NAME="${merge_name}" GIT_COMMITTER_EMAIL="${merge_email}" \
    git commit --no-verify -s -F "${msg_file}"
  rm -f "${msg_file}"
done < <(git rev-list --reverse "${base}..${clean_ref}")
clean_ref="$(git rev-parse HEAD)"
git checkout -q "${start_checkout}"
# The squash step's temp branch (if any) is no longer needed: its commits
# have been re-authored onto the authorship branch.
if [[ -n "${prev_rewrite_branch}" && "${prev_rewrite_branch}" != "${authorship_branch}" ]]; then
  git branch -D "${prev_rewrite_branch}" 2>/dev/null || true
fi

# Prove the rewrite worked before anything is pushed: a fleet-executor
# identity that reaches protected main cannot be fixed later.
"${script_dir}/guards/assert-clean-authorship.sh" "${clean_ref}" "${merge_email}"
# --- end authorship rewrite ----------------------------------------------

if [[ "${dry_run}" == "1" ]]; then
  echo "==> DRY RUN: cleaned history (${base}..${clean_ref}):"
  git log --reverse --format='commit %h  %an <%ae>%n%B%n----' "${base}..${clean_ref}"
  if [[ -n "${rewrite_branch}" ]]; then
    git branch -D "${rewrite_branch}" 2>/dev/null || true
    rewrite_branch=""
  fi
  echo "DRY RUN complete; nothing pushed, no PR opened."
  exit 0
fi

# main's branch protection (required status checks plus auto-merge) means a
# broken branch cannot land: CI fails, the PR just sits. What the local
# pre-flight run buys is EARLIER failure detection, at the price of
# re-running the same lanes CI is about to run (and often the same lanes
# the orchestrator just ran on the identical tree).
# FLEET_MERGE_SKIP_GATES=1 skips them and lets the PR's required checks be
# the gate; the failure mode is discovering a red PR ~15 minutes later
# instead of a red gate now. Never use it when the merged tree diverges
# from what was already gated (a conflict resolution, a manual edit).
#
# The precondition is enforced, not remembered: gates.sh writes a receipt
# (keyed by tree hash, which the rewrite above preserves) for every full
# clean-tree run, and skip is honored only when the history about to be
# pushed has a receipt younger than 24 h. An amend or manual edit changes
# the tree and voids the receipt.
if [[ "${FLEET_MERGE_SKIP_GATES:-0}" == "1" ]]; then
  skip_tree="$(git rev-parse "${clean_ref}^{tree}")"
  receipt_file="$(cd "$(git rev-parse --git-common-dir)" && pwd)/gates-pass/${skip_tree}"
  receipt_ok=0
  if [[ -f "${receipt_file}" ]]; then
    if [[ "$(uname)" == "Darwin" ]]; then
      receipt_mtime="$(stat -f %m "${receipt_file}")"
    else
      receipt_mtime="$(stat -c %Y "${receipt_file}")"
    fi
    if (( $(date +%s) - receipt_mtime < 86400 )); then
      receipt_ok=1
    fi
  fi
  if [[ ${receipt_ok} -ne 1 ]]; then
    echo "fleet-result-merge.sh: FLEET_MERGE_SKIP_GATES=1 refused:" >&2
    echo "  no gates-pass receipt (< 24 h) for tree ${skip_tree}." >&2
    echo "  This tree was never taken through a full scripts/gates.sh run here" >&2
    echo "  (or was amended since). Run the gates, or drop SKIP_GATES and let" >&2
    echo "  this script run them." >&2
    exit 65
  fi
  echo "==> Skipping local pre-flight gates (receipt found for tree ${skip_tree}); PR required checks re-verify"
else
  echo "==> Running local pre-flight gates"
  git checkout -q --detach "${clean_ref}"
  "${script_dir}/gates.sh" "$@"
  git checkout -q "${start_checkout}"
fi

echo "==> Pushing cleaned history to ${pr_branch}"
git push --force-with-lease origin "${clean_ref}:refs/heads/${pr_branch}"

echo "==> Opening pull request"
pr_title="$(head -n 1 "${message_file}")"
second_line="$(sed -n '2p' "${message_file}")"
if [[ -n "${second_line}" ]]; then
  echo "fleet-result-merge.sh: ${message_file} line 2 must be blank" >&2
  echo "  (line 1 is the PR title, the body starts at line 3)." >&2
  exit 66
fi
body_file="$(mktemp)"
tail -n +3 "${message_file}" >"${body_file}"
pr_url="$(gh pr create --base main --head "${pr_branch}" \
  --title "${pr_title}" --body-file "${body_file}" | tail -n 1)"
rm -f "${body_file}"
pr_number="${pr_url##*/}"

if [[ "${FLEET_MERGE_AUTO:-0}" == "1" ]]; then
  echo "==> FLEET_MERGE_AUTO=1: enabling auto-merge (--rebase)"
  gh pr merge --auto --rebase --delete-branch "${pr_number}"
  echo "Opened PR for task ${task_id}; auto-merge (--rebase) will land it once required checks pass."
else
  echo "==> Opened without auto-merge (standing rule): wait for coderabbitai[bot], then merge by hand"
  echo "  ${pr_url}"
  echo "  Check status:   ${script_dir}/pr-review-status.sh ${pr_number}"
  echo "  Or by hand:     gh pr checks ${pr_number}"
  echo "                  gh api repos/NOFireAI/ravel/pulls/${pr_number}/reviews"
  echo "                  gh api repos/NOFireAI/ravel/pulls/${pr_number}/comments"
  echo "  Once ${script_dir}/pr-review-status.sh ${pr_number} reports clean, run the"
  echo "  exact merge command it prints (it pins --match-head-commit to the SHA it"
  echo "  just checked, so the merge refuses if the branch moved since):"
  echo "    gh pr merge ${pr_number} --rebase --delete-branch --match-head-commit <sha from pr-review-status.sh>"
fi

# Do NOT delete the task/<id>/result and task/<id>/start refs here, with or
# without auto-merge: this script reporting success does not mean the PR has
# landed. Deleting them now reintroduces the exact silent-loss shape this
# script exists to prevent: if a required check (or an unresolved CodeRabbit
# finding) later blocks the merge, the PR sits open with no way to recover
# the original result branch. The task refs are cleaned up by the
# merge-fleet-result skill's "after the PR is open" step, once it has
# confirmed via `gh pr view --json state,mergedAt` that the PR actually merged.

# Drop the temp rewrite branch now that it is safely pushed.
if [[ -n "${rewrite_branch}" ]]; then
  git branch -D "${rewrite_branch}" 2>/dev/null || true
  rewrite_branch=""
fi

echo "Task refs (task/${task_id}/result, task/${task_id}/start) are left in place;"
echo "delete them only after confirming the PR merged (see merge-fleet-result skill)."
