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
#   1. Worktrees of this repo that are clean and have landed on origin/main.
#      Locked worktrees are skipped and reported, never removed.
#   2. Orphaned cargo target dirs matching /tmp/wt-*-target and
#      target/ dirs under /private/tmp/claude-*, when no cargo or rustc
#      process is running. A live build skips this class entirely.
#
# Never reaps: the primary checkout, dirty worktrees, worktrees whose state
# cannot be determined, locked worktrees, anything while cargo/rustc runs.
#
# Every worktree that survives prints one line saying why, under one of two
# verbs, because they call for different actions:
#
#   keep (...)          the state IS known and says the worktree is in use
#                       (dirty, locked, or carrying work that has not landed)
#   undetermined (...)  the state could NOT be determined, so the worktree is
#                       kept by default; the line names what was missing
#
# Conflating those two is the bug this script shipped with: a detached
# worktree printed a `skip (detached, ...)` line that read like "still in
# use" and meant "no idea", and a dry run over 69 worktrees proposed 3
# removals while 65 sat in the undetermined bucket unexamined.
#
# Test hooks (both unset in normal use):
#   RAVEL_DISK_REAP_REPO   operate on this repo instead of the one this
#                          script lives in. Setting it also skips the
#                          host-global target-dir sweep, which has nothing
#                          to do with a scratch repo.
set -euo pipefail

# --- Worktree classification helpers -------------------------------------
#
# Defined before any argument handling so the file can be sourced to get the
# classifiers without reaping anything; that is what
# scripts/tests/disk-reap-detached.test.sh drives.
#
# The problem these solve: `main` is protected and merges are REBASE-only, so
# a landed branch's commits are rewritten and its HEAD is NEVER an ancestor of
# origin/main. Ancestry alone therefore refuses every landed worktree forever.
# Three signals are tried, cheapest first, and each is authoritative only when
# it says yes:
#
#   1. ancestry              (works only for the rare fast-forwarded case)
#   2. merged PR head        (the authoritative "has this landed" signal)
#   3. patch-id on main      (offline fallback: the rebase-written commit has
#                             the same patch content as the one in the worktree)
#
# None of them may remove on uncertainty. A worktree whose state cannot be
# established is kept and reported as undetermined.

# dr_contains_line <haystack> <needle>: exact-line membership without a pipe,
# so no exit code is masked by a pipeline.
dr_contains_line() {
  local hay="$1" needle="$2"
  [[ -n "${needle}" ]] || return 1
  case $'\n'"${hay}"$'\n' in
    *$'\n'"${needle}"$'\n'*) return 0 ;;
  esac
  return 1
}

# dr_patch_ids <gitdir> <range> [limit]: stable patch ids of the non-merge
# commits in <range>, one per line, oldest last. `git log --patch` keeps the
# `commit <sha>` headers that git patch-id uses as record boundaries; dropping
# them would fold every diff into a single id.
dr_patch_ids() {
  local gitdir="$1" range="$2" limit="${3:-2000}"
  git -C "${gitdir}" log --no-merges --max-count="${limit}" --patch "${range}" 2>/dev/null |
    git patch-id --stable 2>/dev/null |
    awk '{print $1}'
}

# dr_combined_patch_id <gitdir> <base> <head>: the patch id of the whole
# <base>..<head> diff as one patch. Covers the squash case, where the single
# commit on main corresponds to several commits in the worktree.
dr_combined_patch_id() {
  local gitdir="$1" base="$2" head="$3"
  git -C "${gitdir}" diff "${base}" "${head}" 2>/dev/null |
    git patch-id --stable 2>/dev/null |
    awk '{print $1}'
}

# dr_landed_by_patch_id <gitdir> <head_sha> <main_sha>: exit 0 when every
# commit the worktree carries is present on origin/main as a rewritten commit
# with the same patch content, or when the worktree's combined diff matches one
# commit on main. Exit 1 on any doubt, including an empty commit (which has no
# patch id and so can never be matched).
dr_landed_by_patch_id() {
  local gitdir="$1" head_sha="$2" main_sha="$3"
  local base main_ids wt_ids combined n_commits n_ids id
  base="$(git -C "${gitdir}" merge-base "${head_sha}" "${main_sha}" 2>/dev/null || true)"
  [[ -n "${base}" ]] || return 1
  n_commits="$(git -C "${gitdir}" rev-list --count --no-merges "${base}..${head_sha}" 2>/dev/null || echo 0)"
  [[ "${n_commits}" -gt 0 ]] || return 1
  main_ids="$(dr_patch_ids "${gitdir}" "${base}..${main_sha}")"
  [[ -n "${main_ids}" ]] || return 1

  combined="$(dr_combined_patch_id "${gitdir}" "${base}" "${head_sha}")"
  if dr_contains_line "${main_ids}" "${combined}"; then
    return 0
  fi

  wt_ids="$(dr_patch_ids "${gitdir}" "${base}..${head_sha}")"
  [[ -n "${wt_ids}" ]] || return 1
  n_ids="$(printf '%s\n' "${wt_ids}" | grep -c '^' || true)"
  # An empty commit yields no patch id: fewer ids than commits means something
  # in the range is unaccounted for, so refuse.
  [[ "${n_ids}" -eq "${n_commits}" ]] || return 1
  while IFS= read -r id; do
    [[ -n "${id}" ]] || continue
    dr_contains_line "${main_ids}" "${id}" || return 1
  done <<EOF
${wt_ids}
EOF
  return 0
}

# dr_merged_pr <gh_repo> <head_sha> <branch>: look up a MERGED pull request
# whose head commit is exactly <head_sha>. Prints `<number>\t<head sha>` on a
# hit and nothing on a miss.
#
# Two lookups, because they fail in different situations:
#   - by SHA, via the commit's associated pull requests. This is the one that
#     works for a DETACHED worktree, which is every worktree this repo's
#     workflow creates, and which the branch-keyed lookup could never serve.
#   - by branch, for a worktree still on a named branch whose ref has since
#     been deleted from the remote (the SHA lookup can 404 once GitHub garbage
#     collects an unreferenced head).
#
# Exit code carries the distinction the caller needs:
#   0  the API answered; stdout is the answer (possibly empty = no merged PR)
#   2  the API could not be consulted at all (gh absent, unauthenticated,
#      network down, repo unresolvable) -- the caller must treat this as
#      undetermined, never as "not merged"
dr_merged_pr() {
  local gh_repo="$1" head_sha="$2" branch="$3"
  local out code reachable=0
  if ! command -v gh >/dev/null 2>&1; then
    return 2
  fi
  if [[ -z "${gh_repo}" ]]; then
    return 2
  fi

  if [[ -n "${head_sha}" ]]; then
    code=0
    out="$(gh api "repos/${gh_repo}/commits/${head_sha}/pulls" \
      --jq '[.[] | select(.merged_at != null)] | .[0] | select(.number) | "\(.number)\t\(.head.sha)"' \
      2>/dev/null)" || code=$?
    if [[ ${code} -eq 0 ]]; then
      reachable=1
      if [[ -n "${out}" ]]; then
        printf '%s\n' "${out}"
        return 0
      fi
    fi
  fi

  if [[ -n "${branch}" ]]; then
    code=0
    out="$(gh pr list --repo "${gh_repo}" --head "${branch}" --state merged \
      --limit 1 --json number,headRefOid \
      --jq '.[0] | select(.number) | "\(.number)\t\(.headRefOid)"' 2>/dev/null)" || code=$?
    if [[ ${code} -eq 0 ]]; then
      reachable=1
      if [[ -n "${out}" ]]; then
        printf '%s\n' "${out}"
        return 0
      fi
    fi
  fi

  if [[ ${reachable} -eq 1 ]]; then
    return 0
  fi
  return 2
}

# dr_classify_worktree <gitdir> <wt> <main_sha> <gh_repo>: print exactly one
# line, `<VERDICT>\t<reason>`, where VERDICT is REAP, KEEP, or UNDETERMINED.
# Only REAP may lead to a removal.
dr_classify_worktree() {
  local gitdir="$1" wt="$2" main_sha="$3" gh_repo="$4"
  local head_sha branch ref_desc pr_line pr_num pr_head code n_commits

  if [[ -n "$(git -C "${wt}" status --porcelain 2>/dev/null)" ]]; then
    printf 'KEEP\tdirty: uncommitted or untracked changes; commit or discard them, then re-run\n'
    return 0
  fi

  head_sha="$(git -C "${wt}" rev-parse HEAD 2>/dev/null || true)"
  if [[ -z "${head_sha}" ]]; then
    printf 'UNDETERMINED\tno readable HEAD in this worktree; inspect it by hand\n'
    return 0
  fi

  branch="$(git -C "${wt}" symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
  if [[ -n "${branch}" ]]; then
    ref_desc="branch ${branch} at ${head_sha:0:12}"
  else
    ref_desc="detached HEAD ${head_sha:0:12}"
  fi

  if git -C "${gitdir}" merge-base --is-ancestor "${head_sha}" "${main_sha}" 2>/dev/null; then
    printf 'REAP\t%s is an ancestor of origin/main\n' "${ref_desc}"
    return 0
  fi

  code=0
  pr_line="$(dr_merged_pr "${gh_repo}" "${head_sha}" "${branch}")" || code=$?
  if [[ ${code} -eq 0 ]]; then
    if [[ -n "${pr_line}" ]]; then
      pr_num="${pr_line%%$'\t'*}"
      pr_head="${pr_line##*$'\t'}"
      if [[ "${pr_head}" == "${head_sha}" ]]; then
        printf 'REAP\tPR #%s merged with this exact head (%s); rebase-merged, so not an ancestor\n' \
          "${pr_num}" "${ref_desc}"
        return 0
      fi
      printf 'KEEP\tPR #%s merged at %s but this worktree has moved past it (%s); rebase it or discard the extra commits\n' \
        "${pr_num}" "${pr_head:0:12}" "${ref_desc}"
      return 0
    fi
    # The API answered and knows of no merged PR for this head. That is a
    # determination, not a gap: the work is still open.
    n_commits="$(git -C "${gitdir}" rev-list --count --no-merges "${main_sha}..${head_sha}" 2>/dev/null || echo '?')"
    printf 'KEEP\tnot landed: %s carries %s commit(s) absent from origin/main and no merged PR has that head\n' \
      "${ref_desc}" "${n_commits}"
    return 0
  fi

  # The PR API could not be consulted. Fall back to matching the rewritten
  # commit on origin/main by patch content. A match is proof it landed; a
  # non-match proves nothing, so it stays undetermined rather than becoming a
  # keep.
  if dr_landed_by_patch_id "${gitdir}" "${head_sha}" "${main_sha}"; then
    printf 'REAP\t%s matches a rewritten commit on origin/main by patch-id (PR API unavailable)\n' "${ref_desc}"
    return 0
  fi

  if command -v gh >/dev/null 2>&1; then
    printf 'UNDETERMINED\tPR API unreachable (gh present but the call failed: auth, network, or repo) and no patch-id match on origin/main for %s; re-run once gh works, or remove by hand\n' \
      "${ref_desc}"
  else
    printf 'UNDETERMINED\tgh is not installed, so PR state cannot be checked, and no patch-id match on origin/main for %s; install gh and re-run, or remove by hand\n' \
      "${ref_desc}"
  fi
  return 0
}

# Sourced rather than executed: stop here, with the helpers above defined and
# nothing run. Everything below this line inspects the host and reaps.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  return 0
fi

apply=0
if [[ "${1:-}" == "-y" ]]; then
  apply=1
fi

scratch_repo="${RAVEL_DISK_REAP_REPO:-}"
if [[ -n "${scratch_repo}" ]]; then
  repo_root="$(cd "${scratch_repo}" && pwd)"
else
  repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fi

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
# owner/name for the PR lookups above, derived from the remote rather than
# hardcoded. Empty is fine: an unresolvable repo is treated the same as an
# absent gh, so the worktree lands in the undetermined bucket and is kept.
gh_repo="$(git -C "${repo_root}" remote get-url origin 2>/dev/null |
  sed -E 's#^(https://[^/]+/|git@[^:]+:)##; s#\.git$##')"

echo "==> landed, clean worktrees"
# Parse `worktree list --porcelain` blocks; macOS ships bash 3.2, so no
# associative arrays here. The list is collected into a variable rather than
# piped into the loop so the tallies below survive it: a `while` on the right
# of a pipe runs in a subshell and its counters are lost.
candidates="$(git -C "${repo_root}" worktree list --porcelain | awk '
  /^worktree / { wt = substr($0, 10) }
  /^locked/    { print "LOCKED\t" wt; wt = "" }
  /^$/         { if (wt != "") print "CAND\t" wt; wt = "" }
  END          { if (wt != "") print "CAND\t" wt }
')"

n_reap=0
n_keep=0
n_unknown=0

while IFS=$'\t' read -r kind wt; do
  if [[ -z "${kind}" || -z "${wt}" ]]; then
    continue
  fi
  if [[ "${wt}" == "${repo_root}" || "${wt}" == "${main_wt}" ]]; then
    continue
  fi
  if [[ "${kind}" == "LOCKED" ]]; then
    echo "keep (locked; git worktree unlock it first if it is really stale): ${wt}"
    n_keep=$((n_keep + 1))
    continue
  fi
  if [[ ! -d "${wt}" ]]; then
    continue
  fi

  verdict=""
  reason=""
  line="$(dr_classify_worktree "${repo_root}" "${wt}" "${main_sha}" "${gh_repo}")"
  verdict="${line%%$'\t'*}"
  reason="${line#*$'\t'}"

  case "${verdict}" in
    REAP)
      echo "reap (${reason}): ${wt}"
      n_reap=$((n_reap + 1))
      act git -C "${repo_root}" worktree remove --force "${wt}"
      ;;
    KEEP)
      echo "keep (${reason}): ${wt}"
      n_keep=$((n_keep + 1))
      ;;
    *)
      # Anything unrecognized is treated as undetermined: this script must
      # never be the reason work disappears.
      echo "undetermined (${reason}): ${wt}"
      n_unknown=$((n_unknown + 1))
      ;;
  esac
done <<EOF
${candidates}
EOF

echo "==> worktree summary: ${n_reap} reapable, ${n_keep} in use, ${n_unknown} undetermined"
if [[ ${n_unknown} -gt 0 ]]; then
  echo "    undetermined worktrees were KEPT. Each line above names what was missing."
fi

if [[ -n "${scratch_repo}" ]]; then
  echo "==> orphaned cargo target dirs: skipped (RAVEL_DISK_REAP_REPO is set)"
else
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
fi

echo "==> free space after"
df -h / | tail -1
if [[ ${apply} -eq 0 ]]; then
  echo "Dry run only. Re-run with -y to apply."
fi
