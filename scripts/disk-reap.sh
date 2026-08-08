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
  else
    echo "skip (unmerged): ${wt}"
  fi
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
