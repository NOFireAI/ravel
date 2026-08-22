#!/usr/bin/env bash
# Reclaim disk from stale agent build state before the volume fills.
#
# Multi-session days have filled this volume to zero bytes free more than
# once. At zero, the Bash tool cannot run any command (even `true`), gates
# fail with fake errors, and a human has to clean up by hand. Each cleanup
# used the same heuristics; this script encodes them.
#
# Usage:
#   scripts/disk-reap.sh        # dry run: print what it would remove
#   scripts/disk-reap.sh -y     # apply
#
# What it reaps:
#   1. Worktrees of this repo that are clean and fully merged
#      (HEAD is an ancestor of origin/main). Locked worktrees are skipped
#      and reported, never removed.
#   2. Orphaned cargo target dirs matching /tmp/wt-*-target and
#      target/ dirs under /private/tmp/claude-*, when no cargo or rustc
#      process is running. A live build skips this class entirely.
#
# Never reaps: the primary checkout, dirty or unmerged worktrees, locked
# worktrees, anything while cargo/rustc runs.
set -euo pipefail

apply=0
if [[ "${1:-}" == "-y" ]]; then
  apply=1
fi

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

act() {
  if [[ ${apply} -eq 1 ]]; then
    "$@"
  else
    echo "DRY-RUN: $*"
  fi
}

echo "==> free space before"
df -h / | tail -1

echo "==> pruning worktree records for deleted directories"
act git -C "${repo_root}" worktree prune

git -C "${repo_root}" fetch origin main --quiet || true
main_sha="$(git -C "${repo_root}" rev-parse origin/main)"
# The primary checkout is never a candidate, whatever its state. (git
# refuses to remove a main working tree, but do not rely on that.)
main_wt="$(dirname "$(git -C "${repo_root}" rev-parse --path-format=absolute --git-common-dir)")"
# owner/name for the PR-state fallback below, derived from the remote rather
# than hardcoded. Empty is fine: the fallback treats an unresolvable repo the
# same as an absent gh and keeps the conservative skip.
gh_repo="$(git -C "${repo_root}" remote get-url origin 2>/dev/null |
  sed -E 's#^(https://[^/]+/|git@[^:]+:)##; s#\.git$##')"

echo "==> merged, clean worktrees"
# Parse `worktree list --porcelain` blocks; macOS ships bash 3.2, so no
# associative arrays here.
git -C "${repo_root}" worktree list --porcelain | awk '
  /^worktree / { wt = substr($0, 10) }
  /^locked/    { print "LOCKED\t" wt; wt = "" }
  /^$/         { if (wt != "") print "CAND\t" wt; wt = "" }
  END          { if (wt != "") print "CAND\t" wt }
' | while IFS=$'\t' read -r kind wt; do
  if [[ "${wt}" == "${repo_root}" || "${wt}" == "${main_wt}" ]]; then
    continue
  fi
  if [[ "${kind}" == "LOCKED" ]]; then
    echo "skip (locked): ${wt}"
    continue
  fi
  if [[ ! -d "${wt}" ]]; then
    continue
  fi
  if [[ -n "$(git -C "${wt}" status --porcelain 2>/dev/null)" ]]; then
    echo "skip (dirty): ${wt}"
    continue
  fi
  head_sha="$(git -C "${wt}" rev-parse HEAD 2>/dev/null || true)"
  if [[ -z "${head_sha}" ]]; then
    echo "skip (no HEAD): ${wt}"
    continue
  fi
  if git -C "${repo_root}" merge-base --is-ancestor "${head_sha}" "${main_sha}"; then
    act git -C "${repo_root}" worktree remove --force "${wt}"
    continue
  fi
  # Ancestry is necessary but not sufficient on this repo: `main` is
  # protected and merges are REBASE-only, so a merged branch's commits are
  # rewritten and its HEAD is never an ancestor of main. Ancestry alone
  # therefore refuses every merged worktree forever -- six were sitting in
  # that state, all with merged PRs, and this script would never have
  # touched any of them.
  #
  # So fall back to the PR state, which is the authoritative signal for
  # "has this landed". Two conditions, both required, because a merged PR
  # does not by itself mean the worktree is disposable:
  #   - a PR whose head is this branch is MERGED, and
  #   - the worktree's HEAD is still exactly that PR's head commit, so any
  #     commit added after the merge blocks the reap rather than being
  #     silently discarded.
  # Any uncertainty (no gh, not authenticated, no PR, a mismatched sha)
  # keeps the old conservative skip: this script must never be the reason
  # work disappears.
  branch="$(git -C "${wt}" symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
  if [[ -z "${branch}" ]]; then
    echo "skip (detached, not an ancestor of main): ${wt}"
    continue
  fi
  if ! command -v gh >/dev/null 2>&1; then
    echo "skip (not an ancestor of main; gh absent, cannot check PR state): ${wt}"
    continue
  fi
  pr_head=""
  pr_num=""
  pr_line="$(gh pr list --repo "${gh_repo}" --head "${branch}" --state merged \
    --limit 1 --json number,headRefOid \
    --jq '.[0] | select(.number) | "\(.number)\t\(.headRefOid)"' 2>/dev/null || true)"
  if [[ -n "${pr_line}" ]]; then
    pr_num="${pr_line%%$'\t'*}"
    pr_head="${pr_line##*$'\t'}"
  fi
  if [[ -z "${pr_num}" ]]; then
    echo "skip (not an ancestor of main, no merged PR for ${branch}): ${wt}"
    continue
  fi
  if [[ "${pr_head}" != "${head_sha}" ]]; then
    echo "skip (PR #${pr_num} merged but ${wt} has moved past its head): ${wt}"
    continue
  fi
  echo "reap (PR #${pr_num} merged, rebase-merged so not an ancestor): ${wt}"
  act git -C "${repo_root}" worktree remove --force "${wt}"
done

echo "==> orphaned cargo target dirs"
if pgrep -x cargo >/dev/null 2>&1 || pgrep -x rustc >/dev/null 2>&1; then
  echo "skip (cargo or rustc is running): target dirs left alone"
else
  # Both patterns come from real incidents: land worktrees write
  # /tmp/wt-<name>-land-target, and session scratchpads accumulate
  # target/ dirs after their worktrees are gone. Only dirs idle for
  # more than 2 hours are candidates.
  find /tmp -maxdepth 1 -type d -name 'wt-*-target' -mmin +120 2>/dev/null \
    | while read -r d; do
        act rm -rf "${d}"
      done
  find /private/tmp -maxdepth 6 -type d -name target -path '*claude-*' -mmin +120 2>/dev/null \
    | while read -r d; do
        act rm -rf "${d}"
      done
fi

echo "==> free space after"
df -h / | tail -1
if [[ ${apply} -eq 0 ]]; then
  echo "Dry run only. Re-run with -y to apply."
fi
