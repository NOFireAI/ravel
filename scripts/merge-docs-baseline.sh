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
#   merge-baseline.sh <base-ref> <ours-ref> <theirs-ref>
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

# Entries, without the comment header, one per line.
strip() { grep -v '^#' "$1" | grep -v '^[[:space:]]*$' || true; }

strip "${tmp}/base"   | sort > "${tmp}/base.s"
strip "${tmp}/ours"   | sort > "${tmp}/ours.s"
strip "${tmp}/theirs" | sort > "${tmp}/theirs.s"

# A deletion is a line in base that is absent from a side. The union of both
# sides' deletions is what leaves base.
comm -23 "${tmp}/base.s" "${tmp}/ours.s"   > "${tmp}/del.ours"
comm -23 "${tmp}/base.s" "${tmp}/theirs.s" > "${tmp}/del.theirs"
sort -u "${tmp}/del.ours" "${tmp}/del.theirs" > "${tmp}/del.all"

# An addition is a line on a side that base does not have. The specs forbid
# adding entries, so an addition is reported loudly rather than merged
# silently: it means a task baselined a finding it should have reported.
comm -13 "${tmp}/base.s" "${tmp}/ours.s"   > "${tmp}/add.ours"
comm -13 "${tmp}/base.s" "${tmp}/theirs.s" > "${tmp}/add.theirs"
added=$(sort -u "${tmp}/add.ours" "${tmp}/add.theirs")

comm -23 "${tmp}/base.s" "${tmp}/del.all" > "${tmp}/kept"

# Header from base, entries sorted, so the file's diff stays readable.
grep '^#' "${tmp}/base" > "${tmp}/out"
if [ -n "$added" ]; then
  printf '%s\n' "$added" >> "${tmp}/kept"
fi
sort "${tmp}/kept" >> "${tmp}/out"

cp "${tmp}/out" "$file"
git add "$file"

base_n=$(wc -l < "${tmp}/base.s" | tr -d ' ')
del_n=$(wc -l < "${tmp}/del.all" | tr -d ' ')
kept_n=$(grep -vc '^#' "$file" || true)

echo "baseline merge: ${base_n} entries at base, ${del_n} deleted by the two sides, ${kept_n} remain"

if [ -n "$added" ]; then
  echo "WARNING: a side ADDED baseline entries, which the task specs forbid:" >&2
  printf '  %s\n' "$added" >&2
  echo "They are kept so the gate stays green, but each one is a finding a task" >&2
  echo "chose to silence instead of reporting. Review before committing." >&2
fi

echo
echo "Now verify before committing, each unpiped:"
echo "  make check-docs"
echo "  python3 scripts/check_docs.py --strict-baseline"
