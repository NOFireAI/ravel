#!/bin/sh
# assert-worktree.sh - refuse to mutate the primary checkout.
#
# Run this before the first Edit/Write/commit of any isolated task (top-level
# session or dispatched subagent). Exit 0 when cwd is inside a LINKED git
# worktree, or inside a Brig sandbox clone from within its own guest; exit 1
# when cwd resolves to the PRIMARY checkout (the main working tree) or is not
# a git repo. A concurrent session may be sitting on the
# primary checkout at any moment, so CLAUDE.md's workspace-isolation rule has
# no doc-only / one-file exception.
#
# Note: assumes worktree paths contain no spaces (true for this repo layout).
set -u

toplevel=''
toplevel=$(git rev-parse --show-toplevel 2>/dev/null) || {
  echo "guard: not inside a git working tree (cwd=$(pwd))" >&2
  exit 1
}

# The main working tree is always the FIRST entry of `git worktree list`,
# regardless of the cwd the command runs from.
main_wt=''
main_wt=$(git worktree list --porcelain 2>/dev/null | awk '/^worktree /{print $2; exit}')
if [ -z "$main_wt" ]; then
  echo "guard: could not resolve the main working tree" >&2
  exit 1
fi

# A Brig sandbox clone (docs/internal/dev-in-brig.md) is a standalone clone,
# so it is its own main working tree. It is accepted only inside its own
# guest: marked by scripts/brig.sh new in the clone's own config, and the
# project Brig mounted. The same clone on the host is refused like any
# primary checkout.
if [ "$toplevel" = "$main_wt" ] && [ -n "${BRIG_PROJECT:-}" ] &&
  [ "$(git config --local --bool ravel.sandboxCheckout 2>/dev/null)" = true ]; then
  project=$(cd "$BRIG_PROJECT" 2>/dev/null && pwd -P) || project=''
  if [ -n "$project" ] && [ "$project" = "$(cd "$toplevel" && pwd -P)" ]; then
    exit 0
  fi
fi

if [ "$toplevel" = "$main_wt" ]; then
  echo "guard: REFUSING - cwd ($toplevel) is the PRIMARY checkout, not a worktree." >&2
  echo "guard: A concurrent session can hold in-flight state here; an edit or a" >&2
  echo "guard: 'git checkout -- .' / 'git reset --hard' would clobber it silently." >&2
  echo "guard: Create an isolated worktree first:" >&2
  echo "guard:   git worktree add ../store-<task> origin/main && cd ../store-<task>" >&2
  echo "guard: CLAUDE.md workspace-isolation has no doc-only / one-file exception." >&2
  exit 1
fi

exit 0
