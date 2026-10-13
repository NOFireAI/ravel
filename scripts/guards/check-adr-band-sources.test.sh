#!/usr/bin/env bash
# Cases for check-adr-band-sources.sh, in the pattern of
# check-amendment-integrity.test.sh: add a case here before changing a rule.
#
# Each case builds a throwaway ADR tree under $TMPDIR, except the last, which
# runs the guard against the real docs/adrs/ tree and expects it clean: that
# is the proof every shipped band table cites its sources or carries a
# reasoned exemption.
#
# Run: bash scripts/guards/check-adr-band-sources.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
GUARD="${HERE}/check-adr-band-sources.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-adr-band-sources-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_tree <name> [body]: a scratch tree whose one ADR is a title followed
# by <body>. Prints its path.
new_tree() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/docs/adrs"
  cp "${GUARD}" "${dir}/scripts/guards/check-adr-band-sources.sh"
  printf '# ADR-0001: Test decision\n\n%s\n' "${2:-}" >"${dir}/docs/adrs/0001-test-decision.md"
  printf '%s\n' "${dir}"
}

# band <source-cell>: a one-row band table with a source column.
band() {
  printf '| figure | band | source |\n|---|---|---|\n| q1 wall | under 1 s | %s |\n' "$1"
}

# check <name> <tree> <want-exit> <want-substring-or-empty> [root-arg]
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}" root_arg="${5:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-adr-band-sources.sh ${root_arg} 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing %s\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# Findings: a basis cell with no source token.
t="$(new_tree unsourced "$(band 'from the last run')")"
check "an unsourced basis cell exits 1 naming the row and the cell" "${t}" 1 \
  "docs/adrs/0001-test-decision.md:5: band table at line 3 (| figure | band | source |): row 'q1 wall': basis cell 'source' carries no source token: 'from the last run'"

t="$(new_tree empty-cell "$(band '')")"
check "an empty basis cell is a finding" "${t}" 1 "carries no source token: ''"

t="$(new_tree short-row "$(printf '| figure | band | source |\n|---|---|---|\n| q1 wall | under 1 s |\n')")"
check "a row with no cell under the basis column is a finding" "${t}" 1 "carries no source token: ''"

t="$(new_tree token-elsewhere "$(printf '| figure | band | source |\n|---|---|---|\n| q1 wall, predicted | under 1 s | from the last run |\n')")"
check "a token outside the basis cell does not count" "${t}" 1 "row 'q1 wall, predicted'"

t="$(new_tree last-cell "$(printf '| figure | miss | note |\n|---|---|---|\n| q1 wall | over 1 s | predicted |\n| q2 wall | over 2 s | from the last run |\n')")"
check "with no basis column the last cell is the basis cell" "${t}" 1 \
  "row 'q2 wall': basis cell 'note' carries no source token"

t="$(new_tree named-basis "$(printf '| figure | **Basis** | target |\n|---|---|---|\n| q1 wall | predicted | under 1 s |\n')")"
check "a named basis column that is not last is the basis cell" "${t}" 0 "clean (1 band table(s), 1 row(s) checked"

t="$(new_tree ratio "$(band 'the usual 12.5:1 ratio')")"
check "a ratio is not a file:line citation" "${t}" 1 "carries no source token"

t="$(new_tree short-id "$(band 'comment 12')")"
check "a comment number under six digits is not a comment id" "${t}" 1 "carries no source token"

# Each source token on its own passes.
t="$(new_tree comment-link "$(band 'https://github.com/nofire/ravel/issues/2023#issuecomment-5848140511')")"
check "an issue comment link passes" "${t}" 0 "clean"

t="$(new_tree comment-id "$(band 'bars in #2023 comment 5854876916')")"
check "a bare comment id with the word comment passes" "${t}" 0 "clean"

t="$(new_tree s3 "$(band 's3://ravel-clickbench-e28771/de/deA.tgz')")"
check "an s3:// path passes" "${t}" 0 "clean"

t="$(new_tree bench-path "$(band 'report at bench/reports/q1.json')")"
check "a bench/ report path passes" "${t}" 0 "clean"

t="$(new_tree file-line "$(band 'docs/adrs/2707-exact-key-index-for-spans-and-logs.md:41')")"
check "a file:line citation passes" "${t}" 0 "clean"

t="$(new_tree measured "$(band 'measured 2026-10-11')")"
check "a measured YYYY-MM-DD stamp passes" "${t}" 0 "clean"

t="$(new_tree predicted "$(band 'predicted')")"
check "the word predicted passes" "${t}" 0 "clean"

# The exemption marker.
t="$(new_tree allow "$(printf '<!-- band-sources-allow: source is an issue comment not quoted in the repository -->\n\n'; band 'from the last run')")"
check "an allow marker with a reason exempts the table below it" "${t}" 0 "1 exempt"

t="$(new_tree allow-empty "$(printf '<!-- band-sources-allow: -->\n'; band 'from the last run')")"
check "an allow marker without a reason is a finding" "${t}" 1 "has no reason"

t="$(new_tree allow-blank "$(printf '<!-- band-sources-allow:    -->\n'; band 'from the last run')")"
check "an allow marker whose reason is only spaces is a finding" "${t}" 1 "has no reason"

t="$(new_tree allow-unparsed "$(printf '<!-- band-sources-allow the colon is missing -->\n'; band 'from the last run')")"
check "an allow marker that does not parse is a finding" "${t}" 1 "does not parse"

t="$(new_tree allow-wrapped "$(printf '<!-- band-sources-allow: wrapped\nonto two lines -->\n'; band 'from the last run')")"
check "a wrapped allow marker exempts nothing" "${t}" 1 "exempts nothing"

t="$(new_tree allow-orphan "$(printf '<!-- band-sources-allow: nothing below -->\n\nprose\n\n'; band 'predicted')")"
check "an allow marker with no band table below it is a finding" "${t}" 1 "exempts nothing"

t="$(new_tree allow-one-table "$(printf '<!-- band-sources-allow: the first table only -->\n'; band 'predicted'; printf '\n'; band 'from the last run')")"
check "an allow marker exempts only the table directly below it" "${t}" 1 "docs/adrs/0001-test-decision.md:10:"

# Tables that are not band tables.
t="$(new_tree ignored "$(band 'predicted'; printf '\n| figure | value |\n|---|---|\n| q1 wall | from the last run |\n')")"
check "a table whose header names none of the four words is ignored" "${t}" 0 "clean (1 band table(s), 1 row(s) checked"

t="$(new_tree plural "$(band 'predicted'; printf '\n| figure | targets | misses |\n|---|---|---|\n| q1 wall | 1 s | from the last run |\n')")"
check "plurals of the four words do not make a band table" "${t}" 0 "clean (1 band table(s)"

t="$(new_tree fenced "$(band 'predicted'; printf '\n```\n'; band 'from the last run'; printf '```\n')")"
check "a band table inside fenced code is ignored" "${t}" 0 "clean (1 band table(s)"

t="$(new_tree readme "$(band 'predicted')")"
band 'from the last run' >"${t}/docs/adrs/README.md"
check "docs/adrs/README.md is not scanned" "${t}" 0 "over 1 file(s)"

# Could not check.
t="$(new_tree no-band "$(printf '| figure | value |\n|---|---|\n| q1 wall | from the last run |\n')")"
check "a tree with no band table exits 70" "${t}" 70 "no band table found"

t="$(new_tree no-rows "$(band 'predicted'; printf '\n| figure | band | source |\n|---|---|---|\n\nprose\n')")"
check "a band header with no data rows exits 70" "${t}" 70 "band table header has no data rows"

t="$(new_tree no-dir "$(band 'predicted')")"
check "a missing directory exits 70" "${t}" 70 "no such directory" "docs/missing"

t="$(new_tree no-adrs "$(band 'predicted')")"
rm "${t}/docs/adrs/0001-test-decision.md"
band 'predicted' >"${t}/docs/adrs/README.md"
check "a directory with no ADR files exits 70" "${t}" 70 "no ADR files"

t="$(new_tree bad-usage "$(band 'predicted')")"
out="$(cd "${t}" && bash scripts/guards/check-adr-band-sources.sh docs/adrs extra-arg 2>&1)"; rc=$?
if [[ "${rc}" == 64 ]]; then
  printf 'ok    %s\n' "two arguments is bad usage"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s, wanted 64\n' "two arguments is bad usage" "${rc}"
  printf '%s\n' "${out}" | sed 's/^/      /'
  fails=$((fails + 1))
fi

# Table shapes GFM accepts and the scan must not skip: a one-hyphen delimiter
# row, and a table written without leading pipes.
t="$(new_tree short_separator "$(printf '| figure | band | source |\n|-|-|-|\n| q1 wall | under 1 s | from the last run |\n')")"
check "a band table with a one-hyphen delimiter row is scanned" "${t}" 1 "carries no source token"
t="$(new_tree no_leading_pipe "$(printf 'figure | band | source\n--- | --- | ---\nq1 wall | under 1 s | from the last run\n')")"
check "a band table without leading pipes is scanned" "${t}" 1 "carries no source token"
t="$(new_tree rule_not_table "$(printf 'a | band\n\n---\n\nprose after a horizontal rule\n')")"
check "a horizontal rule is not a delimiter row" "${t}" 70 ""

# Each header word on its own, in any case, selects the table.
t="$(new_tree word_bar "$(printf '| figure | bar | source |\n|---|---|---|\n| q1 wall | under 1 s | from the last run |\n')")"
check "the header word bar selects the table" "${t}" 1 "carries no source token"
t="$(new_tree word_upper "$(printf '| figure | TARGET | source |\n|---|---|---|\n| q1 wall | under 1 s | from the last run |\n')")"
check "an upper-case header word selects the table" "${t}" 1 "carries no source token"
t="$(new_tree word_miss "$(printf '| figure | miss | source |\n|---|---|---|\n| q1 wall | over 2 s | predicted |\n')")"
check "the header word miss selects the table" "${t}" 0 "clean (1 band table(s), 1 row(s) checked"

# A basis column named evidence or measured, not last, is the basis cell.
t="$(new_tree basis_evidence "$(printf '| figure | evidence | band |\n|---|---|---|\n| q1 wall | predicted | under 1 s |\n')")"
check "a column named evidence is the basis cell" "${t}" 0 "clean (1 band table(s), 1 row(s) checked"
t="$(new_tree basis_measured "$(printf '| figure | measured | band |\n|---|---|---|\n| q1 wall | not yet | predicted |\n')")"
check "a column named measured is the basis cell, not the last cell" "${t}" 1 "carries no source token"

# The real thing: every band table under the shipped docs/adrs/ cites a
# source in every row, or carries a reasoned exemption.
out="$(cd "${REPO_ROOT}" && bash "${GUARD}" 2>&1)"; rc=$?
if [[ "${rc}" == 0 ]]; then
  printf 'ok    %s\n' "the real docs/adrs/ tree is clean"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s, wanted 0\n' "the real docs/adrs/ tree is clean" "${rc}"
  printf '%s\n' "${out}" | sed 's/^/      /'
  fails=$((fails + 1))
fi

printf '\ncheck-adr-band-sources.test.sh: %d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
