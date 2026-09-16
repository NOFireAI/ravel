#!/usr/bin/env bash
# Cases for check-superseded-figures.sh. Add a case here before changing a rule,
# the way check-guarded-sql-parse.test.sh works for the guarded-parse guard.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into its
# scripts/guards/, so the guard's own `cd repo_root` lands on the fixture and
# nothing here touches the real checkout. Every fixture carries a
# docs/guides/cost-model.md so the anchor is satisfied unless a case removes it
# on purpose. One case runs the guard against the real tree instead, because a
# guard that only ever sees fixtures can pass while failing on this platform.
#
# Run: bash scripts/guards/check-superseded-figures.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
GUARD="${HERE}/check-superseded-figures.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-superseded-figures-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

pass() {
  printf 'ok    %s\n' "$1"
  passes=$((passes + 1))
}

fail() {
  printf 'FAIL  %s: %s\n' "$1" "$2"
  printf '%s\n' "$3" | sed 's/^/      /'
  fails=$((fails + 1))
}

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

# check <name> <repo> <want-exit> [want-substring ...]
check() {
  local name="$1" dir="$2" want_rc="$3"
  shift 3
  local out rc=0 sub
  out="$(cd "${dir}" && bash scripts/guards/check-superseded-figures.sh 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    fail "${name}" "exit ${rc}, wanted ${want_rc}" "${out}"
    return
  fi
  for sub in "$@"; do
    if [[ "${out}" != *"${sub}"* ]]; then
      fail "${name}" "output missing ${sub}" "${out}"
      return
    fi
  done
  pass "${name}"
}

# --- the real tree ---------------------------------------------------------

# The guard has to run here, on this platform's awk, over the pages it actually
# guards. A fixture-only suite passed while the real run exited 2.
out="$(bash "${GUARD}" 2>&1)"
rc=$?
name="the guard runs clean over the repo's own docs/guides"
if [[ "${rc}" == "0" && "${out}" == *"no retired figure appears"* ]]; then
  pass "${name}"
else
  fail "${name}" "exit ${rc}, wanted 0" "${out}"
fi

# --- the retired pair is refused -------------------------------------------

d="$(new_repo rejects)"
# The two lines the ticket deleted, restored verbatim, at lines 3 and 4.
cat >"${d}/docs/guides/cost-model.md" <<'MD'
# Predicting the S3 request bill

measures slower and heavier, not faster: 486.0s and 463.79 GB transferred
against 285.8s and 150.28 GB for the ranged shape, on the same
42-statement corpus, cold.
MD
check "rejects_superseded_clickbench_pair_in_a_user_guide" "${d}" 1 \
  "docs/guides/cost-model.md:3: superseded-figure:" \
  "docs/guides/cost-model.md:4: superseded-figure:"

# The same revert against the real page: the shipped cost-model.md with the two
# deleted lines put back where they were. This is the regression the ticket is
# about, so it is checked against the page itself, not a stand-in.
d="$(new_repo revert_of_the_real_page)"
cp "${REPO_ROOT}/docs/guides/cost-model.md" "${d}/docs/guides/cost-model.md"
DELETED_1='   measures slower and heavier, not faster: 486.0s and 463.79 GB transferred'
DELETED_2='   against 285.8s and 150.28 GB for the ranged shape, on the same'
export DELETED_1 DELETED_2
anchor_at="$(awk '/measures slower and heavier, not faster/ { print NR; exit }' \
  "${d}/docs/guides/cost-model.md")"
awk '
  { print }
  /measures slower and heavier, not faster/ && !done {
    print ENVIRON["DELETED_1"]
    print ENVIRON["DELETED_2"]
    done = 1
  }
' "${d}/docs/guides/cost-model.md" >"${TMP}/reverted.md"
cp "${TMP}/reverted.md" "${d}/docs/guides/cost-model.md"
name="the deleted pair put back into the real cost-model.md is refused"
if [[ -z "${anchor_at}" ]]; then
  fail "${name}" "the sentence the deletion left behind is gone from the page" ""
else
  check "${name}" "${d}" 1 \
    "docs/guides/cost-model.md:$((anchor_at + 1)): superseded-figure:" \
    "docs/guides/cost-model.md:$((anchor_at + 2)): superseded-figure:"
fi

# Each of the four retired figures is caught on its own.
for fig in 463.79 486.0 150.28 285.8; do
  d="$(new_repo "one_${fig}")"
  printf '# guide\n\nthe value %s appears alone here\n' "${fig}" \
    >"${d}/docs/guides/other.md"
  check "flags the retired figure ${fig} on its own" "${d}" 1 "other.md:3: superseded-figure:"
done

# Two retired figures on one line name both: stopping at the first hides the
# second from whoever is fixing the page.
d="$(new_repo two_on_one_line)"
printf '# guide\n\nthe pair reads 486.0 s and 463.79 GB on this one line\n' \
  >"${d}/docs/guides/other.md"
check "two retired figures on one line are both named" "${d}" 1 \
  "other.md:3: superseded-figure: 486.0 s is a retired" \
  "other.md:3: superseded-figure: 463.79 GB is a retired"

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

# The trailing boundary has to reject a following digit and a following
# dot-digit, or a version-like 486.0.5 reads as the retired 486.0.
d="$(new_repo boundary_dotted)"
printf '# guide\n\nbuild 486.0.5 and revision 285.8.1 are unrelated\n' \
  >"${d}/docs/guides/other.md"
check "a dotted longer number containing a retired figure is not a hit" "${d}" 0 \
  "no retired figure appears"

# The dot in a figure is a literal, not a wildcard.
d="$(new_repo boundary_wildcard)"
printf '# guide\n\nunrelated codes 486x0 and 285a8 and 150928 stay\n' \
  >"${d}/docs/guides/other.md"
check "the dot in a retired figure is not a wildcard" "${d}" 0 "no retired figure appears"

# A dot that ends a sentence is still a hit, or the rule above would let the
# figure back in by putting it at the end of a sentence.
d="$(new_repo boundary_sentence)"
printf '# guide\n\nthe whole-object pass took 486.0. That is the retired number.\n' \
  >"${d}/docs/guides/other.md"
check "a retired figure that ends a sentence is still a hit" "${d}" 1 \
  "other.md:3: superseded-figure:"

# --- the anchor ------------------------------------------------------------

d="$(new_repo empty_root)"
rm -f "${d}/docs/guides"/*.md
check "an empty guides root is the anchor-gone case, not a clean pass" "${d}" 2 "anchor is gone"

d="$(new_repo anchor_absent)"
rm -f "${d}/docs/guides/cost-model.md"
printf '# guide\n\nno retired figure here\n' >"${d}/docs/guides/other.md"
check "the anchor page missing among other pages still fails" "${d}" 2 "not among"

# --- awk failure is not the anchor case ------------------------------------

# awk exits 2 on a syntax error and on an unreadable input, the same code the
# anchor case used to take. A scan that never ran gets its own code.
mkdir -p "${TMP}/stub"
cat >"${TMP}/stub/awk" <<'SH'
#!/usr/bin/env bash
echo "stub awk: refusing to run" >&2
exit 2
SH
chmod +x "${TMP}/stub/awk"
d="$(new_repo awk_fails)"
out="$(cd "${d}" && SUPERSEDED_FIGURES_AWK="${TMP}/stub/awk" \
  bash scripts/guards/check-superseded-figures.sh 2>&1)"
rc=$?
name="a crashed awk exits 70, not 2 and not 0"
if [[ "${rc}" == "70" && "${out}" == *"awk"* ]]; then
  pass "${name}"
else
  fail "${name}" "exit ${rc}, wanted 70" "${out}"
fi

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

d="$(new_repo marker_html_single)"
{
  printf '# guide\n\n'
  printf '<!-- superseded-figure-allow: the line below quotes the retired pass -->\n'
  printf 'the old 486.0 s stays here\n'
} >"${d}/docs/guides/other.md"
check "a single-line HTML comment marker suppresses" "${d}" 0 "no retired figure appears"

d="$(new_repo marker_html_multi)"
{
  printf '# guide\n\n'
  printf '<!--\n'
  printf 'superseded-figure-allow: the line below quotes the retired pass\n'
  printf '\n'
  printf '%s\n' '-->'
  printf 'the old 486.0 s stays here\n'
} >"${d}/docs/guides/other.md"
check "a marker inside a multi-line HTML comment suppresses" "${d}" 0 "no retired figure appears"

# The comment has to carry a marker. A plain multi-line comment above the line
# must not suppress, or any comment would.
d="$(new_repo marker_html_multi_empty)"
{
  printf '# guide\n\n'
  printf '<!--\n'
  printf 'an ordinary note with no marker in it\n'
  printf '%s\n' '-->'
  printf 'the old 486.0 s stays here\n'
} >"${d}/docs/guides/other.md"
check "a multi-line HTML comment without a marker does not suppress" "${d}" 1 \
  "other.md:6: superseded-figure:"

# "Directly above" is literal for the HTML form too.
d="$(new_repo marker_html_multi_stale)"
{
  printf '# guide\n\n'
  printf '<!--\n'
  printf 'superseded-figure-allow: this reason belongs to the line right below\n'
  printf '%s\n' '-->'
  printf 'a documented line with no figure\n'
  printf 'a later 486.0 s that the marker does not reach\n'
} >"${d}/docs/guides/other.md"
check "an HTML comment marker does not carry past its own block" "${d}" 1 \
  "other.md:7: superseded-figure:"

d="$(new_repo marker_no_reason)"
printf '# guide\n\nthe old 486.0 s stays, superseded-figure-allow:\n' \
  >"${d}/docs/guides/other.md"
check "a marker with no reason does not suppress" "${d}" 1 "other.md:3: superseded-figure:"

# The comment close is not a reason.
d="$(new_repo marker_html_no_reason)"
{
  printf '# guide\n\n'
  printf '<!-- superseded-figure-allow: -->\n'
  printf 'the old 486.0 s stays here\n'
} >"${d}/docs/guides/other.md"
check "an HTML marker whose only text is the comment close does not suppress" "${d}" 1 \
  "other.md:4: superseded-figure:"

# The reason lands on a page the documentation gate scans, so it may not carry
# a tracker token. One that does is not a marker, and the finding says so.
d="$(new_repo marker_adr_token)"
printf '# guide\n\nthe old 486.0 s stays, superseded-figure-allow: kept, see ADR-1196\n' \
  >"${d}/docs/guides/other.md"
check "a marker reason carrying an ADR token does not suppress" "${d}" 1 \
  "other.md:3: superseded-figure:" \
  "does not suppress"

d="$(new_repo marker_issue_token)"
{
  printf '# guide\n\n'
  printf 'superseded-figure-allow: kept for the migration note, see #1736\n'
  printf 'the old 486.0 s stays here\n'
} >"${d}/docs/guides/other.md"
check "a marker reason carrying an issue number does not suppress" "${d}" 1 \
  "other.md:4: superseded-figure:" \
  "does not suppress"

d="$(new_repo marker_stale)"
{
  printf '# guide\n\n'
  printf 'superseded-figure-allow: this reason belongs to the line right below it\n'
  printf 'a documented line with no figure\n'
  printf 'a later 486.0 s that the marker does not reach\n'
} >"${d}/docs/guides/other.md"
check "a marker does not carry past its own block" "${d}" 1 "other.md:5: superseded-figure:"

# --- the guard's own table stays paste-safe --------------------------------

# The reasons the guard prints are the text an author copies into a marker. If
# one of them carried a tracker token, the marker built from it would both fail
# the documentation gate and stop suppressing.
d="$(new_repo table_reasons)"
printf '# guide\n\nthe value 486.0 appears here\n' >"${d}/docs/guides/other.md"
out="$(cd "${d}" && bash scripts/guards/check-superseded-figures.sh 2>&1)"
rc=$?
name="no printed reason carries a tracker token"
if [[ "${rc}" == "1" ]] \
  && ! printf '%s\n' "${out}" | grep -Eq 'ADR-[0-9]+|#[0-9]+'; then
  pass "${name}"
else
  fail "${name}" "exit ${rc}, or a reason carried a token" "${out}"
fi

# --- usage -----------------------------------------------------------------

d="$(new_repo usage)"
out="$(cd "${d}" && bash scripts/guards/check-superseded-figures.sh --nope 2>&1)"
rc=$?
name="an unknown option is a usage error"
if [[ "${rc}" == "64" && "${out}" == *"unknown option"* ]]; then
  pass "${name}"
else
  fail "${name}" "exit ${rc}, wanted 64" "${out}"
fi

out="$(cd "${d}" && bash scripts/guards/check-superseded-figures.sh docs/nope 2>&1)"
rc=$?
name="a missing root is a usage error"
if [[ "${rc}" == "64" && "${out}" == *"no such directory"* ]]; then
  pass "${name}"
else
  fail "${name}" "exit ${rc}, wanted 64" "${out}"
fi

# --help prints the header, including the marker forms it documents.
out="$(cd "${d}" && bash scripts/guards/check-superseded-figures.sh --help 2>&1)"
rc=$?
name="--help prints the header and the marker forms"
if [[ "${rc}" == "0" \
  && "${out}" == *"superseded-figure-allow: <reason>"* \
  && "${out}" == *"multi-line"* \
  && "${out}" == *"ADR-1196 superseded"* \
  && "${out}" != *"ADR-0996 itself superseded"* \
  && "${out}" == *"SUPERSEDED_FIGURES_AWK"* ]]; then
  pass "${name}"
else
  fail "${name}" "exit ${rc}, or the header was cut short" "${out}"
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
