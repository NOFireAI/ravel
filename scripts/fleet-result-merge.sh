#!/usr/bin/env bash
# Land a claude-fleet dispatched task's result branch on main via a pull
# request. `main` has required branch protection (PR required, required
# status checks, linear history / rebase-merge only, no direct pushes), so
# this script never merges or pushes to main itself. It cleans the result
# branch's history (see the squash step below), pushes the cleaned history
# to a PR head branch, opens a PR, and enables auto-merge (--rebase) so
# GitHub lands it once the required checks pass.
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
#
# Rewrite: history is rebuilt linearly with cherry-pick. Each flagged commit
# is folded (`fixup`-style) into the previous retained commit, but its own
# Refs:/Fixes:/Signed-off-by: trailers are carried into that commit's
# message if they are not already there, so a required trailer that lived
# only on a `wip:` snapshot is never dropped. A flagged commit that is the
# FIRST retained commit has nothing to fold into, so it is reworded instead:
# its `wip:` prefix is stripped and, if what remains has no Conventional
# Commits type, a `chore:` type is prepended so the subject stays valid.
branch_commits=()
while IFS= read -r c; do
  branch_commits+=("${c}")
done < <(git rev-list --reverse "${base}..${result_sha}")

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

need_rewrite=0
for sha in "${branch_commits[@]+"${branch_commits[@]}"}"; do
  subject="$(git log -1 --format=%s "${sha}")"
  shopt -s nocasematch
  if [[ "${subject}" == wip:* ]]; then
    need_rewrite=1
  elif [[ "${subject}" == *fmt* || "${subject}" == *"style fix"* ]]; then
    parent="$(git rev-parse "${sha}^")"
    if [[ -z "$(git diff -w "${parent}" "${sha}")" ]]; then
      need_rewrite=1
    fi
  fi
  shopt -u nocasematch
done

clean_ref="${result_sha}"

if [[ ${need_rewrite} -eq 1 ]]; then
  echo "==> Squashing wip:/formatting-fixup commits out of the result branch"
  rewrite_branch="_fleet_rewrite_${task_id//\//_}"
  git checkout -q -B "${rewrite_branch}" "${base}"

  have_real=0
  for sha in "${branch_commits[@]+"${branch_commits[@]}"}"; do
    subject="$(git log -1 --format=%s "${sha}")"
    flagged=0
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

    if [[ ${flagged} -eq 0 ]]; then
      # Ordinary commit: replay verbatim (author preserved by cherry-pick).
      git cherry-pick "${sha}"
      have_real=1
      continue
    fi

    if [[ ${have_real} -eq 0 ]]; then
      # First retained commit is flagged: reword it into a valid subject.
      stripped="$(strip_wip "${subject}")"
      if [[ -z "${stripped}" ]]; then
        new_subject="chore: fleet result for task ${task_id}"
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
      have_real=1
    else
      # Fold into the previous retained commit, preserving its trailers.
      git cherry-pick -n "${sha}"
      targs=()
      while IFS= read -r tl; do
        [[ -n "${tl}" ]] && targs+=(--trailer "${tl}")
      done < <(git log -1 --format=%B "${sha}" | git interpret-trailers --parse)
      cur_msg="$(git log -1 --format=%B HEAD)"
      msg_file="$(mktemp)"
      if [[ ${#targs[@]} -gt 0 ]]; then
        printf '%s\n' "${cur_msg}" | git interpret-trailers \
          --if-exists addIfDifferent --if-missing add \
          "${targs[@]}" >"${msg_file}"
      else
        printf '%s\n' "${cur_msg}" >"${msg_file}"
      fi
      git commit --amend --no-verify --allow-empty -F "${msg_file}"
      rm -f "${msg_file}"
    fi
  done

  clean_ref="$(git rev-parse HEAD)"
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
if [[ "${FLEET_MERGE_SKIP_GATES:-0}" == "1" ]]; then
  echo "==> Skipping local pre-flight gates (FLEET_MERGE_SKIP_GATES=1); PR required checks are the gate"
else
  echo "==> Running local pre-flight gates"
  git checkout -q --detach "${clean_ref}"
  "${script_dir}/gates.sh" "$@"
  git checkout -q "${start_checkout}"
fi

echo "==> Pushing cleaned history to ${pr_branch}"
git push --force-with-lease origin "${clean_ref}:refs/heads/${pr_branch}"

echo "==> Opening pull request and enabling auto-merge (--rebase)"
pr_title="$(head -n 1 "${message_file}")"
second_line="$(sed -n '2p' "${message_file}")"
if [[ -n "${second_line}" ]]; then
  echo "fleet-result-merge.sh: ${message_file} line 2 must be blank" >&2
  echo "  (line 1 is the PR title, the body starts at line 3)." >&2
  exit 66
fi
body_file="$(mktemp)"
tail -n +3 "${message_file}" >"${body_file}"
gh pr create --base main --head "${pr_branch}" \
  --title "${pr_title}" --body-file "${body_file}"
rm -f "${body_file}"
gh pr merge --auto --rebase --delete-branch "${pr_branch}"

# Do NOT delete the task/<id>/result and task/<id>/start refs here: auto-merge
# only enables the merge, it does not wait for required checks. Deleting them
# now, before the PR has actually landed, reintroduces the exact silent-loss
# shape this script exists to prevent: if a required check later fails, the
# PR sits open with no way to recover the original result branch, and this
# script would already have reported success. The task refs are cleaned up by
# the merge-fleet-result skill's "after the PR is open" step, once it has
# confirmed via `gh pr view --json state,mergedAt` that the PR actually merged.

# Drop the temp rewrite branch now that it is safely pushed.
if [[ -n "${rewrite_branch}" ]]; then
  git branch -D "${rewrite_branch}" 2>/dev/null || true
  rewrite_branch=""
fi

echo "Opened PR for task ${task_id}; auto-merge (--rebase) will land it once required checks pass."
echo "Task refs (task/${task_id}/result, task/${task_id}/start) are left in place;"
echo "delete them only after confirming the PR merged (see merge-fleet-result skill)."
