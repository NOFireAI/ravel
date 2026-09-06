#!/bin/sh
# assert-fresh-merge-base.sh <pr-number> [remote] [branch]
# Verify a pull request's merge base IS the current tip of origin/main, fetched
# in THIS invocation. Exit 0 when the branch already contains that tip; exit 1
# when main has moved ahead of it, printing how far behind and what landed.
#
# Green CI on a stale base is not evidence about the merge. `main` runs no CI of
# its own here, so a PR that passed against an older base can still break `main`
# the moment it lands: that is how commit 8a534f43 left main uncompilable, when a
# five-argument call site landed minutes before another PR made the same function
# take six. Both PRs were green. Neither had seen the other.
#
# It also silently skips gates. A gate added to `main` after a PR went green has
# never run against that PR, so the branch merges without the check its author
# added precisely to catch it.
#
# ALLOW_STALE_MERGE_BASE=1 to proceed anyway (a docs-only branch whose CI you
# have reason to trust across the gap).
#
# Fetches with an explicit destination refspec rather than reading FETCH_HEAD.
# `git fetch origin main <other-ref>` leaves FETCH_HEAD naming main, so a check
# written that way compares main with itself and reports success for any branch.
set -u

pr=${1:-}
remote=${2:-origin}
branch=${3:-main}

if [ -z "$pr" ]; then
  echo "guard: usage: assert-fresh-merge-base.sh <pr-number> [remote] [branch]" >&2
  exit 1
fi

case "$pr" in
  *[!0-9]* | '')
    echo "guard: pr-number must be numeric, got '$pr'" >&2
    exit 1
    ;;
esac

# Both fetches name their destination, per the note above: relying on the
# opportunistic remote-tracking update would make this depend on the clone's
# `remote.<name>.fetch` refspec being the standard one.
tip_ref="refs/remotes/${remote}/${branch}"
git fetch "$remote" "+${branch}:${tip_ref}" >/dev/null 2>&1 || {
  echo "guard: git fetch $remote $branch failed" >&2
  exit 1
}

# refs/pull/<n>/head needs no branch name, so this works for a PR from any
# branch and cannot be aimed at the wrong ref by a stale local copy. The
# destination carries the pid and is deleted on exit: a name per PR would leave
# one ref behind for every PR ever checked, and a single fixed name would race
# two concurrent runs against each other. The leading `+` is load-bearing rather
# than habit: SIGKILL cannot run the trap, so a ref can survive, and forcing the
# update means a later run that draws the same pid overwrites the orphan instead
# of failing on it.
local_ref="refs/remotes/${remote}/freshness-check-$$"
# shellcheck disable=SC2329  # invoked via the exit trap below
cleanup() {
  git update-ref -d "$local_ref" 2>/dev/null || true
}
# Cleanup on exit, and make each signal actually exit. A handler bound directly
# to HUP/INT/TERM only runs and RETURNS in POSIX sh, so the script would carry
# on past the interruption having already deleted the ref it is about to read.
# Exiting from the handler runs the exit trap, so cleanup still happens once.
trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
git fetch "$remote" "+refs/pull/${pr}/head:${local_ref}" >/dev/null 2>&1 || {
  echo "guard: git fetch $remote refs/pull/${pr}/head failed (is $pr a pull request on $remote?)" >&2
  exit 1
}

tip=$(git rev-parse "$tip_ref") || exit 1
head=$(git rev-parse "$local_ref") || exit 1
base=$(git merge-base "$tip" "$head") || exit 1

if [ "$tip" = "$base" ]; then
  echo "guard: PR #$pr merge base is the current $remote/$branch ($(git rev-parse --short "$tip"))"
  exit 0
fi

behind=$(git rev-list --count "$base".."$tip") || {
  echo "guard: git rev-list --count $base..$tip failed" >&2
  exit 1
}
echo "guard: PR #$pr is $behind commit(s) behind $remote/$branch." >&2
echo "guard: its CI ran against $(git rev-parse --short "$base"), not $(git rev-parse --short "$tip")." >&2
echo "guard: most recent unseen commits:" >&2
git log --oneline --max-count=10 "$base".."$tip" 2>/dev/null | sed 's/^/guard:   /' >&2
if [ "$behind" -gt 10 ]; then
  echo "guard:   ... and $((behind - 10)) more" >&2
fi

if [ "${ALLOW_STALE_MERGE_BASE:-0}" = "1" ]; then
  echo "guard: ALLOW_STALE_MERGE_BASE=1, proceeding on a stale base" >&2
  exit 0
fi

echo "guard: rebase onto $remote/$branch and let CI re-run before merging." >&2
exit 1
