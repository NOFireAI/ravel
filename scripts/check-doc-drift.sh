#!/usr/bin/env bash
# Doc-drift check for the derived counts in docs/query-engine.md's generated
# PromQL conformance block (ADR-0053 decision 6).
#
# The block is written by `cargo test -p ravel-promql-difftest --test
# conformance_table` (with RAVEL_UPDATE_CONFORMANCE_TABLE=1), so its numbers
# are derived, not authored. A merge that is textually clean can still leave
# them stale: adding a corpus entry on one branch and regenerating the block on
# another produces a doc that no longer describes the corpus it claims to
# summarize. That happened once already, and nothing checked it.
#
# This is the cheap check, not a substitute for that test: it recomputes the
# same counts straight from the corpus files and the doc itself, with no build
# and no dependency beyond bash, grep, and sed. Run the conformance_table test
# for the per-construct states.
#
# Exit 0 when every count matches, 1 on drift (with both numbers named), 2 when
# the doc or the corpus registration cannot be read at all.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
doc_rel=docs/query-engine.md
doc=$repo_root/$doc_rel
table_test=$repo_root/crates/ravel-promql-difftest/tests/conformance_table.rs
corpus_dir=$repo_root/crates/ravel-promql-difftest/corpus

begin_marker='<!-- BEGIN GENERATED PROMQL CONFORMANCE TABLE -->'
end_marker='<!-- END GENERATED PROMQL CONFORMANCE TABLE -->'
regen_hint="regenerate with: RAVEL_UPDATE_CONFORMANCE_TABLE=1 cargo test -p ravel-promql-difftest --test conformance_table"

fail_setup() {
  echo "check-doc-drift: $1" >&2
  exit 2
}

for f in "$doc" "$table_test"; do
  [[ -r $f ]] || fail_setup "cannot read $f"
done
[[ -d $corpus_dir ]] || fail_setup "cannot read $corpus_dir"

# The generated block, so a count in the surrounding prose can never be
# mistaken for the derived one.
block=$(awk -v b="$begin_marker" -v e="$end_marker" \
  'index($0, b) { inside = 1; next } index($0, e) { inside = 0; next } inside' \
  "$doc")
[[ -n $block ]] || fail_setup "no generated conformance block in $doc_rel"

# Source of truth for which corpus files count: the CORPUS_FILES table in
# conformance_table.rs. A file sitting in corpus/ that no include_str! cites
# contributes nothing to the generated numbers, so it must not contribute here
# either.
registered=$(sed -n 's|.*include_str!("\.\./corpus/\([A-Za-z0-9_.-]*\.txt\)").*|\1|p' \
  "$table_test" | sort -u)
[[ -n $registered ]] || fail_setup "no corpus files registered in $table_test"

# One corpus entry is one blank-line-separated block, and every block carries
# exactly one `name:` field (ravel_promql_difftest::corpus::build_entry rejects
# a block without one), so unindented `name:` lines count entries exactly.
expected_entries=0
expected_files=0
while IFS= read -r name; do
  file=$corpus_dir/$name
  [[ -r $file ]] || fail_setup "$table_test registers $name, which is not readable at $file"
  entries=$(grep -c '^name:' "$file" || true)
  expected_entries=$((expected_entries + entries))
  expected_files=$((expected_files + 1))
done <<<"$registered"

# Not drift in the doc, but the same class of silent divergence: a corpus file
# nobody registered is evidence nobody counts.
while IFS= read -r file; do
  name=${file##*/}
  if ! grep -qxF "$name" <<<"$registered"; then
    echo "check-doc-drift: warning: $name is in corpus/ but not registered in ${table_test#"$repo_root"/}; its entries count for nothing" >&2
  fi
done < <(find "$corpus_dir" -maxdepth 1 -name '*.txt' | sort)

# `Surface: <constructs> constructs over <entries> corpus entries in <files>
# corpus files.`, as scoring.rs's to_markdown renders it.
surface_line=$(grep -c '^Surface: [0-9]* constructs over [0-9]* corpus entries in [0-9]* corpus files\.$' <<<"$block" || true)
[[ $surface_line == 1 ]] || fail_setup \
  "expected exactly one generated Surface line in $doc_rel's conformance block, found $surface_line"
read -r doc_constructs doc_entries doc_files < <(
  sed -n 's/^Surface: \([0-9]*\) constructs over \([0-9]*\) corpus entries in \([0-9]*\) corpus files\.$/\1 \2 \3/p' <<<"$block"
)

# The construct count has no cheap out-of-band source (it is the length of
# scoring.rs's REGISTRY), but the same generator writes one table row and one
# state tally per construct, so a hand-edited number contradicts them.
table_rows=$(awk '
  /^\| Construct \| Category \| State \| Evidence \|$/ { inside = 1; next }
  inside && /^\| --- \|/ { next }
  inside && /^\|/ { rows++; next }
  inside { inside = 0 }
  END { print rows + 0 }
' <<<"$block")
state_total=0
for state in 'supported' 'intentionally rejected' 'accepted divergence' 'unclassified'; do
  count=$(sed -n "s/^| $state | \([0-9]*\) |$/\1/p" <<<"$block")
  [[ -n $count ]] || fail_setup "no '$state' row in $doc_rel's state table"
  state_total=$((state_total + count))
done

drift=0
report_drift() {
  echo "$doc_rel: $1 expected $2, doc says $3" >&2
  drift=1
}

[[ $doc_entries == "$expected_entries" ]] ||
  report_drift 'corpus entries' "$expected_entries" "$doc_entries"
[[ $doc_files == "$expected_files" ]] ||
  report_drift 'corpus files' "$expected_files" "$doc_files"
[[ $table_rows == "$doc_constructs" ]] ||
  report_drift 'constructs (counted from the generated table rows)' "$table_rows" "$doc_constructs"
[[ $state_total == "$doc_constructs" ]] ||
  report_drift 'constructs (summed from the generated state table)' "$state_total" "$doc_constructs"

if ((drift)); then
  echo "check-doc-drift: $doc_rel's generated conformance block is stale; $regen_hint" >&2
  exit 1
fi

echo "check-doc-drift: $doc_rel is current ($doc_constructs constructs, $doc_entries corpus entries, $doc_files corpus files)"
