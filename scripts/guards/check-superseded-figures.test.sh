#!/usr/bin/env bash
# Cases for check-superseded-figures.sh. Add a case here before changing a rule,
# the way check-guarded-sql-parse.test.sh works for the guarded-parse guard.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into its
# scripts/guards/, so the guard's own `cd repo_root` lands on the fixture and
# nothing here touches the real checkout. Every fixture carries a
# docs/guides/cost-model.md so the anchor is satisfied unless a case removes it
# on purpose.
#
# Run: bash scripts/guards/check-superseded-figures.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-superseded-figures.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-superseded-figures-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: a scratch repo with the guard installed and a clean anchor
# page. Prints its path. Cases that need the anchor page gone remove it.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/docs/guides"
  cp "${GUARD}" "${dir}/scripts/guards/check-superseded-figures.sh"
  cat >"${dir}/docs/guides/cost-model.md" <<'MD'
# Predicting the S3 request bill

The ranged pass finished faster despite the extra requests, at 222.19 s cold.
Such a pass lands at or below 203,243 requests and at or above 403.97 GB.
MD
  printf '%s\n' "${dir}"
}

# check <name> <repo> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-superseded-figures.sh 2>&1)" || rc=$?
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

# --- the retired pair is refused -------------------------------------------

d="$(new_repo rejects)"
# The two lines the ticket deleted, restored verbatim, at lines 3 and 4.
cat >"${d}/docs/guides/cost-model.md" <<'MD'
# Predicting the S3 request bill

measures slower and heavier, not faster: 486.0s and 463.79 GB transferred
against 285.8s and 150.28 GB for the ranged shape, on the same
42-statement corpus, cold.
MD
out="$(cd "${d}" && bash scripts/guards/check-superseded-figures.sh 2>&1)"
rc=$?
name="rejects_superseded_clickbench_pair_in_a_user_guide"
if [[ "${rc}" == "1" \
      && "${out}" == *"docs/guides/cost-model.md:3: superseded-figure:"* \
      && "${out}" == *"docs/guides/cost-model.md:4: superseded-figure:"* ]]; then
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s, or a line was not named\n' "${name}" "${rc}"
  printf '%s\n' "${out}" | sed 's/^/      /'
  fails=$((fails + 1))
fi

# Each of the four retired figures is caught on its own.
for fig in 463.79 486.0 150.28 285.8; do
  d="$(new_repo "one_${fig}")"
  printf '# guide\n\nthe value %s appears alone here\n' "${fig}" \
    >"${d}/docs/guides/other.md"
  check "flags the retired figure ${fig} on its own" "${d}" 1 "other.md:3: superseded-figure:"
done

# --- negative control: a guide without the pair passes ---------------------

d="$(new_repo clean)"
cat >"${d}/docs/guides/other.md" <<'MD'
# Some other guide

Reading every segment whole moves 403.97 GB against 194.19 GB ranged, and the
request count runs 203,243 whole against 751,409 ranged. These stay.
MD
check "passes_on_a_guide_without_the_superseded_pair" "${d}" 0 "no retired figure appears"

# A longer number that merely contains a retired figure is not a hit.
d="$(new_repo boundary)"
printf '# guide\n\nunrelated values 1486.0 and 486.02 and 2285.8 stay\n' \
  >"${d}/docs/guides/other.md"
check "a longer number containing a retired figure is not a hit" "${d}" 0 "no retired figure appears"

# --- the anchor ------------------------------------------------------------

d="$(new_repo empty_root)"
rm -f "${d}/docs/guides"/*.md
check "an empty guides root is the anchor-gone case, not a clean pass" "${d}" 2 "anchor is gone"

d="$(new_repo anchor_absent)"
rm -f "${d}/docs/guides/cost-model.md"
printf '# guide\n\nno retired figure here\n' >"${d}/docs/guides/other.md"
check "the anchor page missing among other pages still fails" "${d}" 2 "not among"

# --- the escape marker -----------------------------------------------------

d="$(new_repo marker_inline)"
printf '# guide\n\nthe old 486.0 s stays, superseded-figure-allow: kept for the migration note\n' \
  >"${d}/docs/guides/other.md"
check "an inline marker with a reason suppresses" "${d}" 0 "no retired figure appears"

d="$(new_repo marker_block)"
{
  printf '# guide\n\n'
  printf 'superseded-figure-allow: the line below quotes the retired pass on purpose\n'
  printf 'the old 486.0 s stays here\n'
} >"${d}/docs/guides/other.md"
check "a marker in the block directly above the line suppresses" "${d}" 0 "no retired figure appears"

d="$(new_repo marker_no_reason)"
printf '# guide\n\nthe old 486.0 s stays, superseded-figure-allow:\n' \
  >"${d}/docs/guides/other.md"
check "a marker with no reason does not suppress" "${d}" 1 "other.md:3: superseded-figure:"

d="$(new_repo marker_stale)"
{
  printf '# guide\n\n'
  printf 'superseded-figure-allow: this reason belongs to the line right below it\n'
  printf 'a documented line with no figure\n'
  printf 'a later 486.0 s that the marker does not reach\n'
} >"${d}/docs/guides/other.md"
check "a marker does not carry past its own block" "${d}" 1 "other.md:5: superseded-figure:"

# --- usage -----------------------------------------------------------------

d="$(new_repo usage)"
out="$(cd "${d}" && bash scripts/guards/check-superseded-figures.sh --nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"unknown option"* ]]; then
  printf 'ok    %s\n' "an unknown option is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "an unknown option is a usage error" "${rc}"
  fails=$((fails + 1))
fi

out="$(cd "${d}" && bash scripts/guards/check-superseded-figures.sh docs/nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"no such directory"* ]]; then
  printf 'ok    %s\n' "a missing root is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "a missing root is a usage error" "${rc}"
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
