#!/usr/bin/env bash
# Cases for check-amendment-integrity.sh, in the pattern of
# check-doc-figures.test.sh: add a case here before changing a rule.
#
# Each case builds a throwaway ADR under $TMPDIR, except the last, which
# runs the guard against the real docs/adrs/ tree and expects it clean: that
# is the proof every real amendment heading carries a marker and every claim
# it makes still holds.
#
# Run: bash scripts/guards/check-amendment-integrity.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
GUARD="${HERE}/check-amendment-integrity.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-amendment-integrity-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_tree <name>: a scratch tree with one clean ADR. Prints its path.
#   - Section A and Section B each carry the amendment's pointer.
#   - the retired phrase "storage class STANDARD_FALLBACK" appears nowhere
#     unqualified.
new_tree() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/docs/adrs"
  cp "${GUARD}" "${dir}/scripts/guards/check-amendment-integrity.sh"

  cat >"${dir}/docs/adrs/0001-test-decision.md" <<'MD'
# ADR-0001: Test decision

## Decision

The store picks a placement class per write.

## Section A

Placement follows the active policy (2026-09-23 amendment below).

## Section B

Nothing here disagrees with Section A (2026-09-23 amendment below).

## Amendment (2026-09-23): placement narrows to one class

<!-- amendment-applies: sections="Section A|Section B" pointer="2026-09-23 amendment" -->

Retires the "storage class STANDARD_FALLBACK" wording Section A and Section B
used to carry; both now read as placement class above.

<!-- amendment-supersedes: phrase="storage class STANDARD_FALLBACK" pointer="2026-09-23 amendment" -->
MD
  printf '%s\n' "${dir}"
}

# check <name> <tree> <want-exit> <want-substring-or-empty> [root-arg]
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}" root_arg="${5:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-amendment-integrity.sh ${root_arg} 2>&1)" || rc=$?
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

t="$(new_tree clean)"
check "a clean ADR with both markers satisfied passes" "${t}" 0 "clean"

# The defect class this guard exists for: the marker claims Section A carries
# the pointer, and the edit never reached it.
t="$(new_tree missing-pointer)"
sed -i.bak 's/Placement follows the active policy (2026-09-23 amendment below)./Placement follows the active policy./' "${t}/docs/adrs/0001-test-decision.md"
check "a named section missing its pointer is a finding" "${t}" 1 "does not carry the pointer"
check "a finding's output carries the marker-syntax hint" "${t}" 1 "Marker syntax: docs/adrs/README.md"

# The retired phrase survives, unqualified, outside the amendment.
t="$(new_tree unqualified-phrase)"
sed -i.bak 's/## Decision/## Decision\n\nThe fallback keeps using storage class STANDARD_FALLBACK for cold data./' "${t}/docs/adrs/0001-test-decision.md"
check "a retired phrase left unqualified is a finding" "${t}" 1 "appears without its pointer"

# The pointer in the same sentence is what qualifies a surviving phrase,
# including when the sentence wraps across the match.
t="$(new_tree qualified-phrase)"
sed -i.bak 's/## Decision/## Decision\n\nThe fallback kept using storage class STANDARD_FALLBACK for cold data\nuntil the 2026-09-23 amendment below./' "${t}/docs/adrs/0001-test-decision.md"
check "a retired phrase with the pointer beside it passes" "${t}" 0 "clean"

# The pointer itself may straddle the wrap, indentation and all: the window
# is read as collapsed prose, not as the lines it was typed on.
t="$(new_tree wrapped-pointer)"
sed -i.bak 's/## Decision/## Decision\n\n- The fallback kept using storage class STANDARD_FALLBACK until the 2026-09-23\n  amendment below narrowed it./' "${t}/docs/adrs/0001-test-decision.md"
check "a pointer split across an indented wrap still qualifies" "${t}" 0 "clean"

# amendment-supersedes-allow suppresses the same finding when it carries a
# reason, on the line above the phrase.
t="$(new_tree allowed-phrase)"
sed -i.bak 's/## Decision/## Decision\n\n<!-- amendment-supersedes-allow: cites the retired class by name on purpose -->\nThe fallback keeps using storage class STANDARD_FALLBACK for cold data./' "${t}/docs/adrs/0001-test-decision.md"
check "an amendment-supersedes-allow marker with a reason suppresses" "${t}" 0 "clean"

# An amendment-supersedes-allow marker with no reason does not suppress.
t="$(new_tree empty-reason-allow)"
sed -i.bak 's/## Decision/## Decision\n\n<!-- amendment-supersedes-allow: -->\nThe fallback keeps using storage class STANDARD_FALLBACK for cold data./' "${t}/docs/adrs/0001-test-decision.md"
check "an amendment-supersedes-allow marker with no reason is still a finding" "${t}" 1 "appears without its pointer"

# An amendment heading with neither marker is unreadable, not clean.
t="$(new_tree no-marker)"
sed -i.bak '/amendment-applies:/d; /amendment-supersedes:/d' "${t}/docs/adrs/0001-test-decision.md"
check "an amendment heading with no marker cannot be checked" "${t}" 70 "carries no amendment-applies"
check "a could-not-check result carries the marker-syntax hint" "${t}" 70 "Marker syntax: docs/adrs/README.md"

# Recognition is case-insensitive on the heading text, not just the noun
# form: a lowercase "amendment" heading is recognised too.
t="$(new_tree lowercase-heading)"
sed -i.bak 's/^## Section B$/### amendment (2026-09-24): a lowercase heading\n\nNo marker here.\n\n## Section B/' "${t}/docs/adrs/0001-test-decision.md"
check "a lowercase 'amendment' heading with no marker cannot be checked" "${t}" 70 "carries no"

# A "Correction" heading is recognised the same way as "Amendment": the
# guard's own header says nothing is excluded by spelling.
t="$(new_tree correction-heading)"
sed -i.bak 's/^## Section B$/## Correction: the placement rule was misstated\n\nNo marker here.\n\n## Section B/' "${t}/docs/adrs/0001-test-decision.md"
check "a Correction heading with no marker cannot be checked" "${t}" 70 "carries no"

# A marker names a section heading that does not exist in this document.
t="$(new_tree bad-section)"
sed -i.bak 's/sections="Section A|Section B"/sections="Section A|Section C"/' "${t}/docs/adrs/0001-test-decision.md"
check "a marker naming a nonexistent section cannot be checked" "${t}" 70 "no matching heading"

# A heading whose own text contains the separator is named with `\|`.
t="$(new_tree escaped-pipe)"
sed -i.bak 's/^## Section A$/## Section A: one \| two/' "${t}/docs/adrs/0001-test-decision.md"
sed -i.bak 's/sections="Section A|Section B"/sections="Section A: one \\| two|Section B"/' "${t}/docs/adrs/0001-test-decision.md"
check "a section name may escape the separator" "${t}" 0 "clean"

# Without the escape the same name splits into two, and neither half is a
# heading: unreadable, not clean.
t="$(new_tree unescaped-pipe)"
sed -i.bak 's/^## Section A$/## Section A: one \| two/' "${t}/docs/adrs/0001-test-decision.md"
sed -i.bak 's/sections="Section A|Section B"/sections="Section A: one | two|Section B"/' "${t}/docs/adrs/0001-test-decision.md"
check "an unescaped separator inside a name cannot be checked" "${t}" 70 "no matching heading"

# An amendment heading is recognised at any level below the title, and an
# unmarked one is refused rather than skipped.
t="$(new_tree amendment-level-3)"
sed -i.bak 's/^## Section B$/### Amendment (2026-09-24): a level-3 amendment\n\nNo marker here.\n\n## Section B/' "${t}/docs/adrs/0001-test-decision.md"
check "a level-3 amendment with no marker cannot be checked" "${t}" 70 "carries no"

t="$(new_tree amendment-level-4)"
sed -i.bak 's/^## Section B$/#### Amendment (2026-09-24): a level-4 amendment\n\nNo marker here.\n\n## Section B/' "${t}/docs/adrs/0001-test-decision.md"
check "a level-4 amendment with no marker cannot be checked" "${t}" 70 "carries no"

# Recognition is on the word stem, not the noun: "Amended ..." counts too.
t="$(new_tree amended-heading)"
sed -i.bak 's/^## Section B$/## Amended 2026-09-24: the placement rule again\n\nNo marker here.\n\n## Section B/' "${t}/docs/adrs/0001-test-decision.md"
check "an 'Amended ...' heading with no marker cannot be checked" "${t}" 70 "carries no"

# The document title is not an amendment section, whatever it mentions.
t="$(new_tree amend-in-title)"
cat >"${t}/docs/adrs/0003-how-we-amend.md" <<'MD'
# ADR-0003: How we amend decisions

## Decision

A title naming amendment is not itself an amendment section.
MD
check "a title mentioning amendment is not an amendment" "${t}" 0 "clean"

# A subheading inside an amendment block belongs to that block: the
# enclosing amendment's markers speak for it.
t="$(new_tree nested-subheading)"
cat >>"${t}/docs/adrs/0001-test-decision.md" <<'MD'

### Consequences of this amendment

A heading inside the block is not a second amendment needing its own
marker.
MD
check "a subheading inside an amendment block is part of it" "${t}" 0 "clean"

# `none` turns the checks off, so it has to say why.
t="$(new_tree none-no-reason)"
sed -i.bak 's/sections="Section A|Section B" pointer="2026-09-23 amendment"/none/' "${t}/docs/adrs/0001-test-decision.md"
check "amendment-applies: none with no reason is a finding" "${t}" 1 "with no reason"

t="$(new_tree none-with-reason)"
sed -i.bak 's/sections="Section A|Section B" pointer="2026-09-23 amendment"/none reason="adds a placement class rather than retiring one"/' "${t}/docs/adrs/0001-test-decision.md"
check "amendment-applies: none with a reason passes" "${t}" 0 "clean"

# A reason= of pure whitespace is refused the same as an empty one: the
# reason has to say something, not just be present.
t="$(new_tree none-whitespace-reason)"
sed -i.bak 's/sections="Section A|Section B" pointer="2026-09-23 amendment"/none reason="   "/' "${t}/docs/adrs/0001-test-decision.md"
check "amendment-applies: none with a whitespace-only reason is a finding" "${t}" 1 "with no reason"

# A marker that names nothing, or omits half of what it needs, is
# unreadable rather than vacuously satisfied.
t="$(new_tree empty-sections)"
sed -i.bak 's/sections="Section A|Section B"/sections=""/' "${t}/docs/adrs/0001-test-decision.md"
check "an empty sections= names no section" "${t}" 70 "names no section"

t="$(new_tree applies-no-pointer)"
sed -i.bak 's/sections="Section A|Section B" pointer="2026-09-23 amendment"/sections="Section A|Section B"/' "${t}/docs/adrs/0001-test-decision.md"
check "an amendment-applies marker with no pointer= is refused" "${t}" 70 "missing sections= or pointer="

t="$(new_tree applies-no-sections)"
sed -i.bak 's/sections="Section A|Section B" pointer=/pointer=/' "${t}/docs/adrs/0001-test-decision.md"
check "an amendment-applies marker with a pointer and no sections= is refused" "${t}" 70 "missing sections= or pointer="

t="$(new_tree supersedes-no-pointer)"
sed -i.bak 's/phrase="storage class STANDARD_FALLBACK" pointer="2026-09-23 amendment"/phrase="storage class STANDARD_FALLBACK"/' "${t}/docs/adrs/0001-test-decision.md"
check "an amendment-supersedes marker with no pointer= is refused" "${t}" 70 "missing phrase= or pointer="

t="$(new_tree supersedes-no-phrase)"
sed -i.bak 's/phrase="storage class STANDARD_FALLBACK" pointer="2026-09-23 amendment"/pointer="2026-09-23 amendment"/' "${t}/docs/adrs/0001-test-decision.md"
check "an amendment-supersedes marker with no phrase= is refused" "${t}" 70 "missing phrase= or pointer="

# Two headings with the named text: which one the pointer must reach is not
# a question the guard may guess at.
t="$(new_tree duplicate-heading)"
sed -i.bak 's/^## Section B$/## Section A/' "${t}/docs/adrs/0001-test-decision.md"
check "a section name matching two headings is refused" "${t}" 70 "occurs 2 times"

# A heading inside an amendment's own block is not a section a pointer can
# be sent to, so it does not make the name ambiguous either.
t="$(new_tree nested-section-heading)"
cat >>"${t}/docs/adrs/0001-test-decision.md" <<'MD'

### Section A

The amendment restates the rule; this is not the document's Section A.
MD
check "a section heading inside an amendment block is not a target" "${t}" 0 "clean"

# An amendment nested under the section it amends may not answer for the
# pointer: that would make the marker prove itself.
t="$(new_tree nested-amendment-prose)"
sed -i.bak 's/Placement follows the active policy (2026-09-23 amendment below)./Placement follows the active policy./' "${t}/docs/adrs/0001-test-decision.md"
sed -i.bak 's/^## Section B$/### Amendment (2026-09-24): nested under Section A\n\n<!-- amendment-applies: none reason="restates the rule above rather than retiring it" -->\n\nThis block names the 2026-09-23 amendment, from inside Section A.\n\n## Section B/' "${t}/docs/adrs/0001-test-decision.md"
check "an amendment nested in a section cannot carry that section's pointer" "${t}" 1 "does not carry the pointer"

# A heading nested inside an amendment's own block does not become a valid
# pointer target just because its own text is itself amendment-named: it
# still belongs to the enclosing block, the same as any other nested
# heading, so a later marker naming it cannot be checked.
t="$(new_tree nested-amendment-as-target)"
cat >>"${t}/docs/adrs/0001-test-decision.md" <<'MD'

### Amendment (2026-09-24): a nested heading naming itself an amendment

<!-- amendment-applies: none reason="restates the placement narrowing above; nested and retires nothing new" -->

This heading is intentionally amendment-named and sits inside the
2026-09-23 amendment's own block.

## Amendment (2026-09-25): a third amendment naming the nested heading as a section

<!-- amendment-applies: sections="Amendment (2026-09-24): a nested heading naming itself an amendment" pointer="2026-09-25 amendment" -->
MD
check "an amendment-named heading nested in another amendment's block is not a target" "${t}" 70 "no matching heading"

# Phrase matching is case-insensitive and whitespace-collapsed, so
# capitalisation and a line wrap do not hide a surviving phrase.
t="$(new_tree capitalised-phrase)"
sed -i.bak 's/## Decision/## Decision\n\nThe fallback keeps using Storage Class STANDARD_FALLBACK for cold data./' "${t}/docs/adrs/0001-test-decision.md"
check "a retired phrase recapitalised is still a finding" "${t}" 1 "appears without its pointer"

t="$(new_tree wrapped-phrase)"
sed -i.bak 's/## Decision/## Decision\n\nThe fallback keeps using storage\nclass STANDARD_FALLBACK for cold data./' "${t}/docs/adrs/0001-test-decision.md"
check "a retired phrase split across a wrap is still a finding" "${t}" 1 "appears without its pointer"

# A marker comment is metadata, not prose: it neither qualifies the phrase
# beside it nor counts as an occurrence of one.
t="$(new_tree marker-not-a-qualifier)"
sed -i.bak 's/## Decision/## Decision\n\n<!-- amendment-applies: sections="Section A" pointer="2026-09-23 amendment" -->\nThe fallback keeps using storage class STANDARD_FALLBACK for cold data./' "${t}/docs/adrs/0001-test-decision.md"
check "a marker comment does not qualify the phrase beside it" "${t}" 1 "appears without its pointer"

t="$(new_tree marker-not-an-occurrence)"
sed -i.bak 's/## Decision/## Decision\n\n<!-- amendment-supersedes: phrase="storage class STANDARD_FALLBACK" pointer="2026-09-23 amendment" -->/' "${t}/docs/adrs/0001-test-decision.md"
check "a phrase inside a marker comment is not an occurrence" "${t}" 0 "clean"

# docs/adrs/README.md is the index, not an ADR.
t="$(new_tree readme-index)"
cat >"${t}/docs/adrs/README.md" <<'MD'
# ADR index

## Amending an ADR

The index documents the marker syntax and carries no markers of its own.
MD
check "docs/adrs/README.md is an index, not an ADR" "${t}" 0 "clean"

# A directory that is not there is not an empty one.
t="$(new_tree missing-dir)"
check "a missing docs directory cannot be checked" "${t}" 70 "no such directory" "docs/nope"

# An empty docs directory is a structural failure, not a vacuous pass.
t="${TMP}/empty-docs"
mkdir -p "${t}/scripts/guards" "${t}/docs/adrs"
cp "${GUARD}" "${t}/scripts/guards/check-amendment-integrity.sh"
check "an empty docs/adrs directory cannot be checked" "${t}" 70 "no ADR files found"

# A directory with ADRs but zero amendment headings is the same failure
# shape: nothing to scan is never a silent pass.
t="${TMP}/no-amendments"
mkdir -p "${t}/scripts/guards" "${t}/docs/adrs"
cp "${GUARD}" "${t}/scripts/guards/check-amendment-integrity.sh"
cat >"${t}/docs/adrs/0002-no-amendments.md" <<'MD'
# ADR-0002: A decision with no amendments

## Decision

Nothing here has ever been amended.
MD
check "an ADR tree with zero amendment headings cannot be checked" "${t}" 70 "no amendment headings found"

t="$(new_tree bad-usage)"
out="$(cd "${t}" && bash scripts/guards/check-amendment-integrity.sh docs/adrs extra-arg 2>&1)"; rc=$?
if [[ "${rc}" == 64 ]]; then
  printf 'ok    %s\n' "two arguments is bad usage"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s, wanted 64\n' "two arguments is bad usage" "${rc}"
  fails=$((fails + 1))
fi

# The guard's failure output sends the author to docs/adrs/README.md,
# "Amending an ADR". A hint naming a section that is not there is worse
# than no hint.
if grep -q '^## Amending an ADR$' "${REPO_ROOT}/docs/adrs/README.md"; then
  printf 'ok    %s\n' "docs/adrs/README.md carries the section the hint names"
  passes=$((passes + 1))
else
  printf 'FAIL  %s\n' "docs/adrs/README.md has no \"Amending an ADR\" section"
  fails=$((fails + 1))
fi

# The real thing: every amendment heading under the shipped docs/adrs/ has a
# marker the guard can read, and every claim that marker makes is true of
# the document as committed.
out="$(cd "${REPO_ROOT}" && bash "${GUARD}" 2>&1)"; rc=$?
if [[ "${rc}" == 0 ]]; then
  printf 'ok    %s\n' "the real docs/adrs/ tree is clean"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s, wanted 0\n' "the real docs/adrs/ tree is clean" "${rc}"
  printf '%s\n' "${out}" | sed 's/^/      /'
  fails=$((fails + 1))
fi

printf '\ncheck-amendment-integrity.test.sh: %d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
