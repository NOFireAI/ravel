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

# The retired phrase survives, unqualified, outside the amendment.
t="$(new_tree unqualified-phrase)"
sed -i.bak 's/## Decision/## Decision\n\nThe fallback keeps using storage class STANDARD_FALLBACK for cold data./' "${t}/docs/adrs/0001-test-decision.md"
check "a retired phrase left unqualified is a finding" "${t}" 1 "appears without its pointer"

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

# A marker names a section heading that does not exist in this document.
t="$(new_tree bad-section)"
sed -i.bak 's/sections="Section A|Section B"/sections="Section A|Section C"/' "${t}/docs/adrs/0001-test-decision.md"
check "a marker naming a nonexistent section cannot be checked" "${t}" 70 "no matching heading"

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
