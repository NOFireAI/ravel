#!/usr/bin/env bash
# Resolve a conflict in scripts/docs_lint_baseline.txt between two or more
# documentation task branches.
#
# Every wave-2 and wave-3 task deletes the baseline entries for the findings it
# fixed. Four branches deleting different lines from one 985-line file conflict
# textually every time, and the resolution is always the same: the union of the
# deletions. A hand-resolved conflict on a file this size is where a live
# finding gets silently un-baselined, so this does it mechanically instead.
#
# Usage, from inside the integration worktree mid-conflict:
#   merge-docs-baseline.sh <base-ref> <ours-ref> <theirs-ref>
#
# It writes the resolved file and stages it. It does NOT commit: the caller
# still verifies with `make check-docs` and `--strict-baseline` before
# committing, because a union that is correct line-wise can still be wrong
# about the tree.
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <base-ref> <ours-ref> <theirs-ref>" >&2
  exit 2
fi

base=$1
ours=$2
theirs=$3
file=scripts/docs_lint_baseline.txt

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

read_side() {
  local side=$1 ref=$2
  if ! git show "${ref}:${file}" > "${tmp}/${side}" 2>/dev/null; then
    echo "error: cannot read ${file} at ${ref}" >&2
    exit 2
  fi
}

read_side base "$base"
read_side ours "$ours"
read_side theirs "$theirs"

# Multiset arithmetic, the same Counter the gate loads the file with. The
# baseline holds one line per finding, so a key fixed in three places is
# three identical lines. A sort -u over the deletions collapses those to one,
# and comm then removes one copy per key from the base, leaving the rest in
# the merged file as entries nobody baselined on purpose. A union of 48 and
# 223 deletions once came back as 82 that way, and 189 findings both sides
# had fixed were silently restored.
python3 - "${tmp}/base" "${tmp}/ours" "${tmp}/theirs" "$file" <<'PY'
import sys
from collections import Counter


def load(path):
    header, entries = [], []
    with open(path, encoding="utf-8") as fh:
        for line in fh.read().split("\n"):
            if line.startswith("#"):
                header.append(line)
            elif line.strip():
                entries.append(line)
    return header, Counter(entries)


header, base = load(sys.argv[1])
_, ours = load(sys.argv[2])
_, theirs = load(sys.argv[3])
out_path = sys.argv[4]

# A deletion is an entry base has that a side no longer has. Both sides'
# deletions leave the merged file; an entry both deleted is counted once.
deleted_ours = base - ours
deleted_theirs = base - theirs
kept = base - deleted_ours - deleted_theirs

# An addition is an entry a side has that base does not. The task specs
# forbid adding entries, so it is kept (the gate must stay green) and
# reported loudly: it is a finding a task chose to silence instead of report.
added = (ours - base) + (theirs - base)

with open(out_path, "w", encoding="utf-8") as fh:
    fh.write("\n".join(header) + "\n")
    for line in sorted((kept + added).elements()):
        fh.write(line + "\n")

print(
    f"baseline merge: {sum(base.values())} entries at base, "
    f"{sum(deleted_ours.values())} deleted by ours and "
    f"{sum(deleted_theirs.values())} by theirs "
    f"({sum((deleted_ours & deleted_theirs).values())} by both), "
    f"{sum(kept.values())} remain"
)
if added:
    print("WARNING: a side ADDED baseline entries, which the task specs forbid:", file=sys.stderr)
    for line in sorted(added.elements()):
        print(f"  {line}", file=sys.stderr)
    print("They are kept so the gate stays green, but each one is a finding a task", file=sys.stderr)
    print("chose to silence instead of reporting. Review before committing.", file=sys.stderr)
PY

git add "$file"

echo
echo "Now verify before committing, each unpiped:"
echo "  make check-docs"
echo "  python3 scripts/check_docs.py --strict-baseline"
