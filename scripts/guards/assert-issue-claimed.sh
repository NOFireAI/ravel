#!/bin/sh
# assert-issue-claimed.sh <issue-number>
# Refuse to dispatch a fleet task on an unclaimed issue. Exit 0 when the given
# GitHub issue has at least one assignee; exit 1 when it has none, or when gh
# cannot resolve the issue at all. A gh transport/auth/not-found failure is
# reported distinctly from a genuinely unassigned issue, so an infra problem is
# never mistaken for "nobody owns this ticket".
#
# No override env var by design: dispatching on a deliberately-unclaimed issue
# is never correct, unlike a deliberately-stale ref (assert-fresh-dispatch-ref).
set -u

issue=${1:-}

if [ -z "$issue" ]; then
  echo "guard: usage: assert-issue-claimed.sh <issue-number>" >&2
  exit 1
fi

# Capture the assignee count only when gh itself succeeds: a non-zero gh exit
# (auth, network, no such issue) must NOT be read as "the issue has no
# assignee". gh's built-in --jq keeps this dependency-free (no external jq).
count=''
count=$(gh issue view "$issue" --json assignees --jq '.assignees | length' 2>/dev/null) || {
  echo "guard: gh could not read issue #$issue (auth, network, or no such issue)." >&2
  echo "guard: this is NOT a 'no assignee' result. Fix gh access or the issue number" >&2
  echo "guard:   first (gh auth status; gh issue view $issue), then re-run." >&2
  exit 1
}

# A successful gh call always yields a numeric count; anything else is a
# contract change we refuse to interpret rather than guess an owner exists.
case "$count" in
  '' | *[!0-9]*)
    echo "guard: gh returned an unparseable assignee count for issue #$issue: '$count'." >&2
    exit 1
    ;;
esac

if [ "$count" -ge 1 ]; then
  exit 0
fi

echo "guard: issue #$issue has no assignee - refusing to dispatch an unclaimed ticket." >&2
echo "guard: claim it first (gh issue edit $issue --add-assignee @me), then re-dispatch." >&2
exit 1
