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
#   1. Worktrees of this repo that are clean and provably landed on
#      origin/main. `main` is protected and merged with REBASE-merge, so a
#      landed branch's commits are rewritten and its HEAD is never an
#      ancestor of origin/main. Ancestry is therefore only one of the ways a
#      worktree can qualify; patch equivalence (`git cherry`) is the one that
#      actually fires on this repo. Locked worktrees are skipped and
#      reported, never removed.
#   2. Orphaned cargo target dirs matching wt-*-target and target/ dirs under
#      claude-* scratchpads, when no cargo or rustc process is running. A live
#      build skips this class entirely.
#
# Never reaps: the primary checkout, dirty worktrees, worktrees carrying any
# commit not already on origin/main, worktrees with an operation in progress,
# locked worktrees, anything while cargo/rustc runs.
#
# Fail closed: deleting an unmerged worktree destroys work that exists
# nowhere else, which is far worse than failing to reclaim disk. Every branch
# below that cannot positively prove a worktree is reclaimable skips it and
# says why.
#
# Test hooks (used only by scripts/disk-reap.test.sh, never in normal runs):
#   DISK_REAP_REPO_ROOT  operate on this repo instead of the script's own
#   DISK_REAP_TMP_ROOTS  space-separated roots to scan for orphaned targets
set -uo pipefail

apply=0
if [[ "${1:-}" == "-y" ]]; then
  apply=1
fi

repo_root="${DISK_REAP_REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
tmp_roots="${DISK_REAP_TMP_ROOTS:-/tmp /private/tmp}"

reclaimed_kb=0
reaped_count=0
skipped_count=0

act() {
  if [[ ${apply} -eq 1 ]]; then
    "$@"
  else
    echo "DRY-RUN: $*"
  fi
}

# Size of a directory in KiB, 0 when it cannot be measured. Always taken
# before the removal: after it there is nothing left to measure.
dir_kb() {
  local d="$1" out
  out="$(du -sk "${d}" 2>/dev/null | awk 'NR==1 {print $1}')"
  case "${out}" in
    ''|*[!0-9]*) echo 0 ;;
    *) echo "${out}" ;;
  esac
}

human_kb() {
  local kb="$1"
  awk -v kb="${kb}" 'BEGIN {
    if (kb >= 1048576) printf "%.1f GiB", kb / 1048576
    else if (kb >= 1024) printf "%.1f MiB", kb / 1024
    else printf "%d KiB", kb
  }'
}

note_reaped() { # note_reaped <kb>
  reclaimed_kb=$((reclaimed_kb + $1))
  reaped_count=$((reaped_count + 1))
}

# An operation in progress leaves state that `status --porcelain` can report
# as clean while a half-finished rebase or merge still holds the only copy of
# a conflict resolution.
operation_in_progress() { # operation_in_progress <worktree>
  local wt="$1" p
  for p in rebase-merge rebase-apply MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD BISECT_LOG; do
    local resolved
    resolved="$(git -C "${wt}" rev-parse --git-path "${p}" 2>/dev/null || true)"
    if [[ -n "${resolved}" && -e "${wt}/${resolved}" ]] || [[ -n "${resolved}" && -e "${resolved}" ]]; then
      return 0
    fi
  done
  return 1
}

# The merged test that survives a sha change.
#
# `git cherry <upstream> <head>` lists each commit reachable from <head> but
# not from <upstream>, prefixed `-` when an equivalent patch (same patch-id)
# is already upstream and `+` when it is not. A rebase-merge rewrites commit
# shas but replays the same patches, so every landed commit comes back `-`.
# Empty output means <head> has no commits of its own at all.
#
# Reclaimable means: no `+` lines. Any `+` is a commit whose content is not
# on origin/main, so the worktree may hold the only copy and is skipped.
#
# Merge commits are excluded from `git cherry`'s view, so a merge commit
# carrying content of its own would not be reported. The caller refuses any
# worktree with a merge commit in the range rather than reasoning about it.
patch_equivalent() { # patch_equivalent <head-sha>
  local head="$1" out
  out="$(git -C "${repo_root}" cherry "${main_sha}" "${head}" 2>/dev/null)" || return 1
  printf '%s\n' "${out}" | grep -q '^+' && return 1
  return 0
}

has_merge_commits() { # has_merge_commits <head-sha>
  local head="$1" out
  out="$(git -C "${repo_root}" rev-list --merges "${main_sha}..${head}" 2>/dev/null)" || return 0
  [[ -n "${out}" ]]
}

echo "==> free space before"
df -h / | tail -1

echo "==> pruning worktree records for deleted directories"
act git -C "${repo_root}" worktree prune

git -C "${repo_root}" fetch origin main --quiet 2>/dev/null || true
# --verify --quiet, not a bare rev-parse: a bare `rev-parse origin/main` echoes
# the string "origin/main" on stdout when the ref does not exist, which reads as
# a resolved sha to every check below.
main_sha="$(git -C "${repo_root}" rev-parse --verify --quiet origin/main^{commit} 2>/dev/null || true)"
if [[ ! "${main_sha}" =~ ^[0-9a-f]{40,64}$ ]]; then
  echo "FATAL: cannot resolve origin/main; refusing to judge any worktree" >&2
  exit 1
fi
# The primary checkout is never a candidate, whatever its state. (git
# refuses to remove a main working tree, but do not rely on that.)
#
# Validate the result before trusting it as a guard. If the rev-parse fails,
# `dirname` of an empty string is ".", which matches no worktree path, so the
# guard would silently stop guarding while every message still looked normal.
# This script deletes directories, so an unverifiable guard is a stop, not a
# warning.
main_common_dir="$(git -C "${repo_root}" rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)"
main_wt="$(dirname "${main_common_dir}")"
if [[ -z "${main_common_dir}" || "${main_wt}" != /* || ! -d "${main_wt}" ]]; then
  echo "FATAL: cannot resolve the primary checkout path; refusing to remove anything" >&2
  exit 1
fi

echo "==> landed, clean worktrees"
# Parse `worktree list --porcelain` blocks into a file first: piping straight
# into the loop would put the accumulators in a subshell and lose the
# reclaimed byte total, which is the number this run has to report.
# macOS ships bash 3.2, so no associative arrays here.
cand_file="$(mktemp)"
# target_file is created later, in the orphaned-target section. Name it in the
# trap now so an early exit between here and there cannot leak it.
target_file=""
trap 'rm -f "${cand_file}" "${target_file}"' EXIT
git -C "${repo_root}" worktree list --porcelain | awk '
  /^worktree / { wt = substr($0, 10) }
  /^locked/    { print "LOCKED\t" wt; wt = "" }
  /^$/         { if (wt != "") print "CAND\t" wt; wt = "" }
  END          { if (wt != "") print "CAND\t" wt }
' >"${cand_file}"

while IFS=$'\t' read -r kind wt; do
  [[ -n "${wt}" ]] || continue
  if [[ "${wt}" == "${repo_root}" || "${wt}" == "${main_wt}" ]]; then
    continue
  fi
  if [[ "${kind}" == "LOCKED" ]]; then
    echo "skip (locked): ${wt}"
    skipped_count=$((skipped_count + 1))
    continue
  fi
  if [[ ! -d "${wt}" ]]; then
    continue
  fi
  if [[ -n "$(git -C "${wt}" status --porcelain 2>/dev/null)" ]]; then
    echo "skip (dirty): ${wt}"
    skipped_count=$((skipped_count + 1))
    continue
  fi
  if operation_in_progress "${wt}"; then
    echo "skip (rebase/merge in progress): ${wt}"
    skipped_count=$((skipped_count + 1))
    continue
  fi
  head_sha="$(git -C "${wt}" rev-parse --verify --quiet HEAD 2>/dev/null || true)"
  if [[ ! "${head_sha}" =~ ^[0-9a-f]{40,64}$ ]]; then
    echo "skip (no HEAD): ${wt}"
    skipped_count=$((skipped_count + 1))
    continue
  fi
  if has_merge_commits "${head_sha}"; then
    echo "skip (merge commit not on origin/main, cannot prove its content landed): ${wt}"
    skipped_count=$((skipped_count + 1))
    continue
  fi
  if patch_equivalent "${head_sha}"; then # PROVE-FLIP: ancestry-only regression
    kb="$(dir_kb "${wt}")"
    echo "reap ($(human_kb "${kb}"), every commit already on origin/main): ${wt}"
    act git -C "${repo_root}" worktree remove --force "${wt}"
    note_reaped "${kb}"
    continue
  fi
  # Report the real reason. `git cherry` failing produces no output, which
  # `grep -c` turns into 0, so the old message read "0 commit(s) not on
  # origin/main" -- a count that means the opposite of what it says. The
  # outcome is a skip either way (fail closed), but a wrong reason sends the
  # next reader looking in the wrong place.
  cherry_out=""
  cherry_rc=0
  cherry_out="$(git -C "${repo_root}" cherry "${main_sha}" "${head_sha}" 2>&1)" || cherry_rc=$?
  if [[ "${cherry_rc}" -ne 0 ]]; then
    echo "skip (cannot compare against origin/main, git cherry exited ${cherry_rc}): ${wt}"
  else
    unlanded="$(printf '%s\n' "${cherry_out}" | grep -c '^+' || true)"
    echo "skip (${unlanded} commit(s) not on origin/main): ${wt}"
  fi
  skipped_count=$((skipped_count + 1))
done <"${cand_file}"

echo "==> orphaned cargo target dirs"
if pgrep -x cargo >/dev/null 2>&1 || pgrep -x rustc >/dev/null 2>&1; then
  echo "skip (cargo or rustc is running): target dirs left alone"
else
  # Both patterns come from real incidents: land worktrees write
  # wt-<name>-land-target, and session scratchpads accumulate target/ dirs
  # after their worktrees are gone. Only dirs idle for more than 2 hours are
  # candidates.
  target_file="$(mktemp)"
  for root in ${tmp_roots}; do
    [[ -d "${root}" ]] || continue
    find "${root}" -maxdepth 1 -type d -name 'wt-*-target' -mmin +120 2>/dev/null >>"${target_file}"
    find "${root}" -maxdepth 6 -type d -name target -path '*claude-*' -mmin +120 2>/dev/null >>"${target_file}"
  done
  while read -r d; do
    [[ -n "${d}" && -d "${d}" ]] || continue
    kb="$(dir_kb "${d}")"
    echo "reap ($(human_kb "${kb}")): ${d}"
    act rm -rf "${d}"
    note_reaped "${kb}"
  done <"${target_file}"
  rm -f "${target_file}"
fi

echo "==> free space after"
df -h / | tail -1

# The failure this reporting exists for: a run that skipped 70 worktrees and
# freed nothing still printed a tidy summary and read as success. State the
# number, and state a zero as a zero.
if [[ ${apply} -eq 1 ]]; then
  if [[ ${reaped_count} -eq 0 && ${skipped_count} -eq 0 ]]; then
    echo "RECLAIMED NOTHING: there was nothing to reap."
  elif [[ ${reaped_count} -eq 0 ]]; then
    echo "RECLAIMED NOTHING: 0 of ${skipped_count} candidate(s) removed; every one was skipped for the reason printed above."
  else
    echo "RECLAIMED: $(human_kb "${reclaimed_kb}") from ${reaped_count} dir(s); ${skipped_count} skipped."
  fi
else
  if [[ ${reaped_count} -eq 0 ]]; then
    echo "WOULD RECLAIM NOTHING: 0 of ${skipped_count} candidate(s) are reclaimable."
  else
    echo "WOULD RECLAIM: $(human_kb "${reclaimed_kb}") from ${reaped_count} dir(s); ${skipped_count} skipped."
  fi
  echo "Dry run only. Re-run with -y to apply."
fi
