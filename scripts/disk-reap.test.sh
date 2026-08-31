#!/usr/bin/env bash
# Cases for disk-reap.sh. Add a case here before changing a behaviour, the way
# .claude/guards/pretooluse.test.sh works for the hook.
#
# SAFETY: every case builds a throwaway repo under its own mktemp -d and points
# the script at it with DISK_REAP_REPO_ROOT. DISK_REAP_TMP_ROOTS is pinned to a
# scratch dir inside the same temp tree, so the orphaned-target scan can never
# see the real /tmp. Nothing here reads or writes the real checkout, the real
# worktree list, or any path outside $TMP.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/disk-reap.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; fails=$((fails + 1)); }

git_q() { git -c init.defaultBranch=main -c user.email=t@example.com -c user.name=Test "$@" >/dev/null 2>&1; }

# Builds a fresh origin+clone pair and returns the clone path on stdout.
# Layout: $TMP/<case>/origin (bare), $TMP/<case>/repo (primary checkout),
# $TMP/<case>/wt (worktrees), $TMP/<case>/scratch (fake tmp roots).
new_repo() { # new_repo <case-name>
  local name="$1"
  local base="$TMP/$name"
  mkdir -p "$base/wt" "$base/scratch"
  git_q init --bare "$base/origin"
  git_q clone "$base/origin" "$base/repo"
  git_q -C "$base/repo" config user.email t@example.com
  git_q -C "$base/repo" config user.name Test
  echo base >"$base/repo/base.txt"
  git_q -C "$base/repo" add base.txt
  git_q -C "$base/repo" commit -m "base"
  git_q -C "$base/repo" push origin main
  echo "$base"
}

# Runs the script against a case repo with -y and echoes its output.
run_reap() { # run_reap <base> [script-override]
  local base="$1"
  local script="${2:-$SCRIPT}"
  DISK_REAP_REPO_ROOT="$base/repo" DISK_REAP_TMP_ROOTS="$base/scratch" \
    bash "$script" -y 2>&1
}

assert_gone() { # assert_gone <name> <dir> <output>
  if [ -d "$2" ]; then
    fail "$1" "worktree still present, should have been reclaimed"
    printf '%s\n' "$3" | sed 's/^/    /'
  else
    pass "$1"
  fi
}

assert_kept() { # assert_kept <name> <dir> <output> <want-substring>
  if [ ! -d "$2" ]; then
    fail "$1" "worktree was DELETED, should have been skipped"
    printf '%s\n' "$3" | sed 's/^/    /'
    return
  fi
  case "$3" in
    *"$4"*) pass "$1" ;;
    *)
      fail "$1" "kept, but output missing '$4'"
      printf '%s\n' "$3" | sed 's/^/    /'
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Case 1: a worktree whose commits landed via REBASE-merge.
#
# This is the bug. origin/main gained an unrelated commit and then a cherry-pick
# of the branch commit: same patch, different sha, so the worktree's HEAD is NOT
# an ancestor of origin/main. The old ancestry-only check refused it forever,
# which is how wt-adr927 sat on 105 GB.
# ---------------------------------------------------------------------------
setup_rebase_merged() { # setup_rebase_merged <base>
  local base="$1"
  local repo="$base/repo"
  git_q -C "$repo" checkout -b feature
  echo feature >"$repo/feature.txt"
  git_q -C "$repo" add feature.txt
  git_q -C "$repo" commit -m "feat: add feature.txt"
  local feature_sha
  feature_sha="$(git -C "$repo" rev-parse HEAD)"
  git_q -C "$repo" checkout main
  # main moves on, so the branch cannot be replayed at the same sha.
  echo other >"$repo/other.txt"
  git_q -C "$repo" add other.txt
  git_q -C "$repo" commit -m "chore: unrelated"
  # The rebase-merge: same patch, new sha.
  git_q -C "$repo" cherry-pick "$feature_sha"
  git_q -C "$repo" push origin main
  git_q -C "$repo" branch -D feature
  # Fleet worktrees are detached, which is what short-circuited the old script
  # before it ever reached its PR fallback.
  git_q -C "$repo" worktree add --detach "$base/wt/wt-rebased" "$feature_sha"
  echo "$base/wt/wt-rebased"
}

base1="$(new_repo case1)"
wt1="$(setup_rebase_merged "$base1")"
# Guard the fixture itself: if HEAD were an ancestor of main, the case would be
# testing ordinary ancestry and would pass for the wrong reason.
if git -C "$base1/repo" merge-base --is-ancestor \
  "$(git -C "$wt1" rev-parse HEAD)" "$(git -C "$base1/repo" rev-parse origin/main)"; then
  fail "fixture: rebase-merged HEAD is not an ancestor of main" "it is an ancestor"
else
  pass "fixture: rebase-merged HEAD is not an ancestor of main"
fi
out1="$(run_reap "$base1")"
assert_gone "rebase-merged worktree is reclaimed" "$wt1" "$out1"
case "$out1" in
  *"RECLAIMED: "*) pass "a -y run that freed something prints the total" ;;
  *)
    fail "a -y run that freed something prints the total" "no RECLAIMED line"
    printf '%s\n' "$out1" | sed 's/^/    /'
    ;;
esac

# Proof that case 1 pins the fix rather than passing anyway: flip the single
# reclaimability line (marked PROVE-FLIP in disk-reap.sh) to `if false`, which
# leaves only the ancestry path, and the same fixture must now be kept.
flipped="$TMP/disk-reap-ancestry-only.sh"
sed 's/^  if patch_equivalent "${head_sha}"; then.*$/  if false; then/' "$SCRIPT" >"$flipped"
chmod +x "$flipped"
if grep -q '^  if false; then$' "$flipped"; then
  pass "prove: PROVE-FLIP line found and flipped"
else
  fail "prove: PROVE-FLIP line found and flipped" "sed did not match the marked line"
fi
base1b="$(new_repo case1b)"
wt1b="$(setup_rebase_merged "$base1b")"
out1b="$(run_reap "$base1b" "$flipped")"
assert_kept "prove: ancestry-only build fails this case" "$wt1b" "$out1b" "commit(s) not on origin/main"

# ---------------------------------------------------------------------------
# Case 2: genuinely unpushed, unmerged commits. Nothing on origin/main carries
# this patch, so the worktree may hold the only copy.
# ---------------------------------------------------------------------------
base2="$(new_repo case2)"
repo2="$base2/repo"
git_q -C "$repo2" worktree add -b work "$base2/wt/wt-unmerged"
git_q -C "$base2/wt/wt-unmerged" config user.email t@example.com
git_q -C "$base2/wt/wt-unmerged" config user.name Test
echo secret >"$base2/wt/wt-unmerged/only-here.txt"
git_q -C "$base2/wt/wt-unmerged" add only-here.txt
git_q -C "$base2/wt/wt-unmerged" commit -m "feat: work that exists nowhere else"
out2="$(run_reap "$base2")"
assert_kept "unmerged commits are not reclaimed" "$base2/wt/wt-unmerged" "$out2" \
  "1 commit(s) not on origin/main"
case "$out2" in
  *"RECLAIMED NOTHING"*) pass "a run that freed nothing says so plainly" ;;
  *)
    fail "a run that freed nothing says so plainly" "no RECLAIMED NOTHING line"
    printf '%s\n' "$out2" | sed 's/^/    /'
    ;;
esac

# ---------------------------------------------------------------------------
# Case 3: a dirty worktree. Its HEAD is landed (ancestor of main), so only the
# dirty check can save it.
# ---------------------------------------------------------------------------
base3="$(new_repo case3)"
git_q -C "$base3/repo" worktree add --detach "$base3/wt/wt-dirty" \
  "$(git -C "$base3/repo" rev-parse origin/main)"
echo "uncommitted edit" >>"$base3/wt/wt-dirty/base.txt"
out3="$(run_reap "$base3")"
assert_kept "dirty worktree is not reclaimed" "$base3/wt/wt-dirty" "$out3" "skip (dirty)"

# Untracked-file dirt counts too: a scratch file nobody committed is still the
# only copy of itself.
base3b="$(new_repo case3b)"
git_q -C "$base3b/repo" worktree add --detach "$base3b/wt/wt-untracked" \
  "$(git -C "$base3b/repo" rev-parse origin/main)"
echo notes >"$base3b/wt/wt-untracked/notes.md"
out3b="$(run_reap "$base3b")"
assert_kept "untracked files count as dirty" "$base3b/wt/wt-untracked" "$out3b" "skip (dirty)"

# ---------------------------------------------------------------------------
# Case 4: fast-forward merge, the case the ancestry check already handled. No
# regression: still reclaimed.
# ---------------------------------------------------------------------------
base4="$(new_repo case4)"
repo4="$base4/repo"
git_q -C "$repo4" checkout -b ff
echo ff >"$repo4/ff.txt"
git_q -C "$repo4" add ff.txt
git_q -C "$repo4" commit -m "feat: fast-forwarded"
ff_sha="$(git -C "$repo4" rev-parse HEAD)"
git_q -C "$repo4" push origin ff:main
git_q -C "$repo4" checkout main
git_q -C "$repo4" branch -D ff
git_q -C "$repo4" worktree add --detach "$base4/wt/wt-ff" "$ff_sha"
out4="$(run_reap "$base4")"
assert_gone "fast-forward-merged worktree is still reclaimed" "$base4/wt/wt-ff" "$out4"

# ---------------------------------------------------------------------------
# Fail-closed extras.
# ---------------------------------------------------------------------------
# A locked worktree is never removed, landed or not.
base5="$(new_repo case5)"
git_q -C "$base5/repo" worktree add --detach "$base5/wt/wt-locked" \
  "$(git -C "$base5/repo" rev-parse origin/main)"
git_q -C "$base5/repo" worktree lock "$base5/wt/wt-locked"
out5="$(run_reap "$base5")"
assert_kept "locked worktree is not reclaimed" "$base5/wt/wt-locked" "$out5" "skip (locked)"

# The primary checkout is never a candidate.
base6="$(new_repo case6)"
out6="$(run_reap "$base6")"
if [ -d "$base6/repo" ] && [ -f "$base6/repo/base.txt" ]; then
  pass "primary checkout is never touched"
else
  fail "primary checkout is never touched" "primary checkout damaged"
  printf '%s\n' "$out6" | sed 's/^/    /'
fi

# An unresolvable origin/main means every judgement is unsound, so the script
# must refuse rather than guess.
base7="$(new_repo case7)"
git_q -C "$base7/repo" update-ref -d refs/remotes/origin/main
git_q -C "$base7/repo" remote remove origin
rc7=0
out7="$(DISK_REAP_REPO_ROOT="$base7/repo" DISK_REAP_TMP_ROOTS="$base7/scratch" bash "$SCRIPT" -y 2>&1)" || rc7=$?
if [ "$rc7" != 0 ] && case "$out7" in *"cannot resolve origin/main"*) true ;; *) false ;; esac; then
  pass "missing origin/main refuses to judge anything"
else
  fail "missing origin/main refuses to judge anything" "exit $rc7"
  printf '%s\n' "$out7" | sed 's/^/    /'
fi

# Dry run must not delete a reclaimable worktree.
base8="$(new_repo case8)"
wt8="$(setup_rebase_merged "$base8")"
out8="$(DISK_REAP_REPO_ROOT="$base8/repo" DISK_REAP_TMP_ROOTS="$base8/scratch" bash "$SCRIPT" 2>&1)"
if [ -d "$wt8" ] && case "$out8" in *"WOULD RECLAIM"*) true ;; *) false ;; esac; then
  pass "dry run reports without deleting"
else
  fail "dry run reports without deleting" "worktree gone or no WOULD RECLAIM line"
  printf '%s\n' "$out8" | sed 's/^/    /'
fi

# Orphaned cargo target dirs: an idle one is reaped, a fresh one is not.
# The age rule matters as much as the pattern -- reaping a target dir a live
# build is still writing to would destroy that build's cache mid-run, which is
# why the script requires 2 hours of idleness.
base9="$(new_repo case9)"
old_target="$base9/scratch/wt-stale-target"
new_target="$base9/scratch/wt-fresh-target"
mkdir -p "$old_target" "$new_target"
echo stale >"$old_target/blob"
echo fresh >"$new_target/blob"
# Backdate the stale one well past the 120-minute floor. touch -t is the form
# both GNU and BSD touch accept.
touch -t 202001010000 "$old_target/blob" "$old_target"

out9="$(run_reap "$base9")"
if [ ! -d "$old_target" ]; then
  pass "idle orphaned target dir is reaped"
else
  fail "idle orphaned target dir is reaped" "stale target dir survived"
  printf '%s\n' "$out9" | sed 's/^/    /'
fi
if [ -d "$new_target" ]; then
  pass "recently-touched target dir is left alone"
else
  fail "recently-touched target dir is left alone" "fresh target dir was deleted"
  printf '%s\n' "$out9" | sed 's/^/    /'
fi

echo
if [ "$fails" != 0 ]; then
  echo "$fails failing case(s)"
  exit 1
fi
echo "all cases passed"
