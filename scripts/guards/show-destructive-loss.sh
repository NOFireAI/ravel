#!/usr/bin/env bash
# Print exactly what a destructive git operation would discard, before it is
# run. This is the "printed diff of what would be lost" that
# ALLOW_DESTRUCTIVE=1 is supposed to be read alongside; the PreToolUse guard
# names this script when it refuses a reset or a filter-branch.
#
# Usage:
#   show-destructive-loss.sh <target-ref> [--from <ref>] [--hard]
#
#   <target-ref>  where the branch would end up (e.g. origin/main).
#   --from        what it would move away from (default: HEAD).
#   --hard        also list the uncommitted work `reset --hard` would erase.
#
# Exit codes:
#   0   nothing would be lost
#   64  usage
#   66  the operation would discard commits or uncommitted work (the point
#       of running this: a non-zero means read the output)
set -euo pipefail

target=""
from="HEAD"
hard=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --from) from="${2:-}"; shift 2 ;;
    --hard) hard=1; shift ;;
    -*) echo "show-destructive-loss.sh: unknown flag '$1'" >&2; exit 64 ;;
    *)
      [[ -z "${target}" ]] || { echo "show-destructive-loss.sh: one target only" >&2; exit 64; }
      target="$1"; shift ;;
  esac
done

[[ -n "${target}" ]] || { echo "usage: show-destructive-loss.sh <target-ref> [--from <ref>] [--hard]" >&2; exit 64; }

git rev-parse --verify --quiet "${target}^{commit}" >/dev/null ||
  { echo "show-destructive-loss.sh: '${target}' does not resolve to a commit" >&2; exit 64; }
git rev-parse --verify --quiet "${from}^{commit}" >/dev/null ||
  { echo "show-destructive-loss.sh: '${from}' does not resolve to a commit" >&2; exit 64; }

lost="$(git log --oneline --no-decorate "${target}..${from}" || true)"
lost_count=0
[[ -n "${lost}" ]] && lost_count="$(wc -l <<<"${lost}" | tr -d ' ')"

dirty=""
if ((hard == 1)); then
  dirty="$(git status --porcelain)"
fi

echo "== moving ${from} to ${target}"
if ((lost_count > 0)); then
  echo "-- ${lost_count} commit(s) would be left with no branch pointing at them:"
  sed 's/^/   /' <<<"${lost}"
  echo "-- their combined effect on the tree:"
  git diff --stat "${target}...${from}" | sed 's/^/   /'
else
  echo "-- no commits would be orphaned (${from} is already contained in ${target})"
fi

if ((hard == 1)); then
  if [[ -n "${dirty}" ]]; then
    echo "-- uncommitted work that --hard would erase with no reflog entry:"
    sed 's/^/   /' <<<"${dirty}"
  else
    echo "-- working tree is clean; --hard would erase nothing uncommitted"
  fi
fi

if ((lost_count > 0)) || [[ -n "${dirty}" ]]; then
  echo "-- orphaned commits stay in the reflog for a while; uncommitted work does not."
  exit 66
fi
exit 0
