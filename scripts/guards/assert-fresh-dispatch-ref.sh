#!/bin/sh
# assert-fresh-dispatch-ref.sh <ref-sha> [remote] [branch]
# Verify a fleet_dispatch ref is the CURRENT tip of origin/main, fetched in
# THIS invocation - not a SHA read earlier in the session and gone stale while
# another PR merged. Exit 0 on match; exit 1 on mismatch or bad input.
# To dispatch an intentionally older ref, set ALLOW_STALE_REF=1.
set -u

want=${1:-}
remote=${2:-origin}
branch=${3:-main}

if [ -z "$want" ]; then
  echo "guard: usage: assert-fresh-dispatch-ref.sh <ref-sha> [remote] [branch]" >&2
  exit 1
fi

git fetch "$remote" "$branch" >/dev/null 2>&1 || {
  echo "guard: git fetch $remote $branch failed" >&2
  exit 1
}

tip=''
tip=$(git rev-parse "$remote/$branch" 2>/dev/null) || {
  echo "guard: cannot resolve $remote/$branch" >&2
  exit 1
}

# Accept a branch name, short sha, or full sha for the requested ref.
want_sha=''
want_sha=$(git rev-parse "${want}^{commit}" 2>/dev/null) || want_sha="$want"

if [ "$want_sha" = "$tip" ]; then
  exit 0
fi

behind=''
behind=$(git rev-list --count "${want_sha}..${tip}" 2>/dev/null) || behind='?'
echo "guard: STALE dispatch ref." >&2
echo "guard:   requested:                 $want_sha" >&2
echo "guard:   $remote/$branch (just fetched): $tip" >&2
echo "guard:   the ref is $behind commit(s) behind; the executor will build on a" >&2
echo "guard:   tree missing just-merged crates and may fabricate against them." >&2

if [ "${ALLOW_STALE_REF:-0}" = "1" ]; then
  echo "guard: ALLOW_STALE_REF=1 - proceeding with the older ref deliberately." >&2
  exit 0
fi
echo "guard: re-dispatch with $tip, or set ALLOW_STALE_REF=1 to override." >&2
exit 1
