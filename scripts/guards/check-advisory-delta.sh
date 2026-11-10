#!/usr/bin/env bash
# Advisory delta guard (ADR-4034 D4, issue #4041): compares two
# `cargo deny --format json check advisories` outputs, one taken at a pull
# request's head and one at its base, and fails only on a finding the head
# has and the base does not. A finding already on the base is main's to fix,
# not this change's; it is printed as a warning annotation naming the base.
#
# Input shape (cargo-deny's JSON format, written to stderr, one JSON object
# per line): diagnostics as
#   {"type":"diagnostic","fields":{"severity":"error","code":"vulnerability",
#    "message":..., "labels":[{"span":...}], "notes":[...],
#    "advisory":{"id":"RUSTSEC-...","package":...}}}
# and exactly one closing
#   {"type":"summary","fields":{"advisories":{"errors":N,...}}}
# Log lines are {"type":"log",...}. A finding is an error-severity
# diagnostic, the ones that fail `cargo deny check` today. Its key is the
# advisory id; an error with no advisory attached (a yanked crate, an
# index failure) is keyed as `<code>: <first label span or message>` so it
# takes part in the comparison instead of being dropped.
#
# An input is usable only when every non-blank line parses as a JSON object
# with a string "type", and it holds exactly one summary carrying an
# "advisories" object. cargo-deny prints that summary only after the check
# ran to the end, so a run that died early (exit 1 with no diagnostics) is
# never read as "no advisories".
#
# Usage:
#   check-advisory-delta.sh HEAD_JSON BASE_JSON BASE_SHA
#
# Exit 0 nothing new (prints one ::warning:: per finding present on both
# sides, and "0 new advisories vs base" when nothing is new), 1 a finding new
# at the head (prints one ::error:: per finding), 2 an input missing, empty
# or not cargo-deny JSON, 64 bad usage, 70 jq missing.
set -uo pipefail

if [[ $# -ne 3 || -z "${3}" ]]; then
  echo "usage: check-advisory-delta.sh HEAD_JSON BASE_JSON BASE_SHA" >&2
  exit 64
fi
head_json="$1"
base_json="$2"
base_sha="$3"

if ! command -v jq >/dev/null 2>&1; then
  echo "check-advisory-delta.sh: jq not found" >&2
  exit 70
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

# finding_keys <label> <file> <out>: writes the sorted unique finding keys of
# one cargo-deny JSON output to <out>; returns 2 when the file is unusable.
finding_keys() {
  local label="$1" file="$2" out="$3" summaries
  if [[ ! -f "${file}" ]]; then
    echo "check-advisory-delta.sh: ${label} JSON ${file} does not exist" >&2
    return 2
  fi
  if ! grep -q '[^[:space:]]' "${file}"; then
    echo "check-advisory-delta.sh: ${label} JSON ${file} is empty" >&2
    return 2
  fi
  # jq -R reports a fromjson error on one line and carries on with the next,
  # so each line's verdict is printed and read back rather than trusting
  # jq's exit code.
  if ! jq -R -r 'select(test("\\S"))
      | try (fromjson | if type == "object" and (.type | type) == "string" then "ok" else "bad" end)
        catch "bad"' \
    "${file}" >"${tmpdir}/shape" || grep -qvx ok "${tmpdir}/shape"; then
    echo "check-advisory-delta.sh: ${label} JSON ${file} is not cargo-deny JSON (line $(grep -nvx ok "${tmpdir}/shape" | head -n1 | cut -d: -f1) of its non-blank lines is not a JSON object with a \"type\")" >&2
    return 2
  fi
  if ! summaries="$(jq -R 'select(test("\\S")) | fromjson
      | select(.type == "summary" and ((.fields.advisories | type) == "object"))' \
    "${file}" | jq -s 'length')"; then
    echo "check-advisory-delta.sh: ${label} JSON ${file}: could not count summaries" >&2
    return 2
  fi
  if [[ "${summaries}" != "1" ]]; then
    echo "check-advisory-delta.sh: ${label} JSON ${file} holds ${summaries} advisories summaries, wanted exactly 1 (the check did not run to the end)" >&2
    return 2
  fi
  if ! jq -R -r 'select(test("\\S")) | fromjson
      | select(.type == "diagnostic" and .fields.severity == "error")
      | .fields
      | (.advisory.id // ((.code // "unknown") + ": " + (.labels[0].span // .message // "")))' \
    "${file}" >"${tmpdir}/raw_keys"; then
    echo "check-advisory-delta.sh: ${label} JSON ${file}: could not read diagnostics" >&2
    return 2
  fi
  LC_ALL=C sort -u "${tmpdir}/raw_keys" >"${out}"
}

rc=0
finding_keys head "${head_json}" "${tmpdir}/head_keys" || rc=$?
if ((rc != 0)); then exit 2; fi
finding_keys base "${base_json}" "${tmpdir}/base_keys" || rc=$?
if ((rc != 0)); then exit 2; fi

LC_ALL=C comm -23 "${tmpdir}/head_keys" "${tmpdir}/base_keys" >"${tmpdir}/new"
LC_ALL=C comm -12 "${tmpdir}/head_keys" "${tmpdir}/base_keys" >"${tmpdir}/both"

while IFS= read -r key; do
  [[ -z "${key}" ]] && continue
  echo "::warning title=pre-existing advisory::${key} is also present at the base ${base_sha}; not new in this change, so it does not fail this job (it fails main's push run, where the fix belongs)"
done <"${tmpdir}/both"

new_count="$(grep -c '' "${tmpdir}/new")"
if ((new_count == 0)); then
  echo "0 new advisories vs base"
  exit 0
fi
while IFS= read -r key; do
  echo "::error title=new advisory::${key} is present at the head and absent at the base ${base_sha}"
done <"${tmpdir}/new"
echo "${new_count} new advisories vs base ${base_sha}"
exit 1
