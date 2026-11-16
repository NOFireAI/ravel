#!/usr/bin/env bash
# Cases for check-advisory-delta.sh (ADR-4034 D4, issue #4041). Each case
# writes cargo-deny JSON fixtures under $TMPDIR, built from cargo-deny's own
# serializers (diag::grapher::diag_to_json for a diagnostic line,
# advisories::model::Metadata::to_json for its "advisory" field, and
# stats.rs's {"type":"summary","fields":...} line), and runs the real guard
# against them. No cargo, no network.
#
# Run: bash scripts/guards/check-advisory-delta.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-advisory-delta.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-advisory-delta-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

BASE_SHA="0123456789abcdef0123456789abcdef01234567"

fails=0
passes=0

pass() {
  printf 'ok    %s\n' "$1"
  passes=$((passes + 1))
}
fail() {
  printf 'FAIL  %s: %s\n' "$1" "$2"
  printf '%s\n' "${OUT}" | sed 's/^/      /'
  fails=$((fails + 1))
}

# run_guard <head_file> <base_file>: sets RC and OUT (stdout and stderr).
run_guard() {
  RC=0
  OUT="$(bash "${GUARD}" "$1" "$2" "${BASE_SHA}" 2>&1)" || RC=$?
}

log_line() {
  printf '%s\n' '{"type":"log","fields":{"timestamp":"2026-11-10T00:00:00Z","level":"INFO","message":"fetched advisory database"}}'
}

# vuln_line <id> <crate> <version> [severity]
vuln_line() {
  local id="$1" krate="$2" version="$3" severity="${4:-error}"
  printf '{"type":"diagnostic","fields":{"severity":"%s","message":"Title of %s","code":"vulnerability","labels":[{"message":"security vulnerability detected","span":"%s %s","line":1,"column":1}],"notes":["ID: %s","Advisory: https://rustsec.org/advisories/%s","Description.","Solution: Upgrade to >=9.9.9 (try `cargo update -p %s`)"],"graphs":[{"Krate":{"name":"%s","version":"%s"}}],"advisory":{"id":"%s","package":"%s","title":"Title of %s","description":"Description.","date":"2026-11-09","aliases":[],"related":[],"collection":"crates","categories":[],"keywords":[],"cvss":null,"informational":null,"references":[],"source":null,"url":null,"withdrawn":null,"license":"CC0-1.0","expect-deleted":false}}}\n' \
    "${severity}" "${id}" "${krate}" "${version}" "${id}" "${id}" "${krate}" "${krate}" "${version}" "${id}" "${krate}" "${id}"
}

summary_line() {
  printf '{"type":"summary","fields":{"advisories":{"errors":%s,"warnings":0,"notes":0,"helps":0}}}\n' "$1"
}

test_new_id_at_head_only_exits_1() {
  local name="new_id_at_head_only_exits_1"
  {
    log_line
    vuln_line RUSTSEC-2026-0001 rmcp 0.8.0
    summary_line 1
  } >"${TMP}/${name}.head"
  {
    log_line
    summary_line 0
  } >"${TMP}/${name}.base"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.base"
  # FLIP: compare the two inputs' error counts instead of their ids, or
  # read only the base, and this exits 0 without naming the id.
  if [[ "${RC}" -eq 1 && "${OUT}" == *"::error title=new advisory::RUSTSEC-2026-0001 rmcp is present at the head and absent at the base ${BASE_SHA}"* ]]; then
    pass "${name}"
  else
    fail "${name}" "rc=${RC}, wanted 1 naming RUSTSEC-2026-0001"
  fi
}

test_id_on_both_exits_0_with_warning() {
  local name="id_on_both_exits_0_with_warning"
  {
    vuln_line RUSTSEC-2026-0002 rmcp 0.8.0
    summary_line 1
  } >"${TMP}/${name}.head"
  {
    vuln_line RUSTSEC-2026-0002 rmcp 0.8.0
    summary_line 1
  } >"${TMP}/${name}.base"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.base"
  # FLIP: fail on every error at the head (today's full check), and this
  # exits 1 on an advisory main already carries; drop the annotation loop,
  # and the warning naming the base is missing.
  if [[ "${RC}" -eq 0 && "${OUT}" == *"::warning title=pre-existing advisory::RUSTSEC-2026-0002 rmcp is also present at the base ${BASE_SHA}"* ]]; then
    pass "${name}"
  else
    fail "${name}" "rc=${RC}, wanted 0 with a warning naming the base"
  fi
}

test_nothing_new_prints_zero_line() {
  local name="nothing_new_prints_zero_line"
  # A warning-severity diagnostic does not fail cargo deny today, so it is
  # not a finding on either side.
  {
    log_line
    vuln_line RUSTSEC-2026-0003 paste 1.0.15 warning
    summary_line 0
  } >"${TMP}/${name}.head"
  {
    log_line
    summary_line 0
  } >"${TMP}/${name}.base"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.base"
  # FLIP: key every diagnostic regardless of severity, and the head-only
  # warning exits 1; print the zero line only when the base is non-empty, and
  # it is missing here.
  if [[ "${RC}" -eq 0 && "${OUT}" == "0 new advisories vs base" ]]; then
    pass "${name}"
  else
    fail "${name}" "rc=${RC}, wanted 0 and exactly the line '0 new advisories vs base'"
  fi
}

test_missing_or_empty_base_json_exits_2() {
  local name="missing_or_empty_base_json_exits_2"
  {
    vuln_line RUSTSEC-2026-0004 rmcp 0.8.0
    summary_line 1
  } >"${TMP}/${name}.head"
  : >"${TMP}/${name}.empty"
  printf '\n  \n' >"${TMP}/${name}.blank"
  local rc_missing rc_empty rc_blank out_missing out_empty
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.does-not-exist"
  rc_missing="${RC}"; out_missing="${OUT}"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.empty"
  rc_empty="${RC}"; out_empty="${OUT}"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.blank"
  rc_blank="${RC}"
  # A base that only logged and never printed its summary died before the
  # check finished; it is as unusable as an empty file.
  log_line >"${TMP}/${name}.nosummary"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.nosummary"
  # FLIP: read a missing or empty base as "no advisories at the base", and
  # every head finding reads as new (exit 1), or with an empty head too, as
  # clean (exit 0); drop the summary requirement and the log-only base does
  # the same.
  if [[ "${rc_missing}" -eq 2 && "${out_missing}" == *"does not exist"* &&
    "${rc_empty}" -eq 2 && "${out_empty}" == *"is empty"* &&
    "${rc_blank}" -eq 2 && "${RC}" -eq 2 && "${OUT}" == *"wanted exactly 1"* ]]; then
    pass "${name}"
  else
    OUT="missing: ${out_missing}"$'\n'"empty: ${out_empty}"$'\n'"nosummary: ${OUT}"
    fail "${name}" "missing=${rc_missing} empty=${rc_empty} blank=${rc_blank} nosummary=${RC}, wanted 2 each"
  fi
}

test_malformed_json_exits_2() {
  local name="malformed_json_exits_2"
  {
    vuln_line RUSTSEC-2026-0005 rmcp 0.8.0
    summary_line 1
  } >"${TMP}/${name}.base"
  # A diagnostic line cut short: the id it carried must not silently drop
  # out of the head's findings.
  {
    vuln_line RUSTSEC-2026-0006 rmcp 0.8.0 | cut -c1-120
    summary_line 1
  } >"${TMP}/${name}.head"
  local rc_trunc out_trunc
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.base"
  rc_trunc="${RC}"; out_trunc="${OUT}"
  # Valid JSON that is not cargo-deny's line-object shape.
  printf '["RUSTSEC-2026-0005"]\n' >"${TMP}/${name}.array"
  run_guard "${TMP}/${name}.array" "${TMP}/${name}.base"
  # FLIP: skip lines that do not parse (jq's `fromjson?`), and the truncated
  # head reads as clean, exit 0; check only that each line is JSON, and the
  # array is not refused for its shape (only, later, for its missing summary).
  if [[ "${rc_trunc}" -eq 2 && "${out_trunc}" == *"is not cargo-deny JSON"* &&
    "${RC}" -eq 2 && "${OUT}" == *"is not cargo-deny JSON"* ]]; then
    pass "${name}"
  else
    OUT="truncated: ${out_trunc}"$'\n'"array: ${OUT}"
    fail "${name}" "truncated=${rc_trunc} array=${RC}, wanted 2 each"
  fi
}

test_two_new_ids_both_named() {
  local name="two_new_ids_both_named"
  {
    vuln_line RUSTSEC-2026-0007 rmcp 0.8.0
    vuln_line RUSTSEC-2026-0008 quick-xml 0.39.4
    # The same advisory on the same crate version twice: one finding, not two.
    vuln_line RUSTSEC-2026-0008 quick-xml 0.39.4
    vuln_line RUSTSEC-2026-0009 bincode 1.3.3
    summary_line 4
  } >"${TMP}/${name}.head"
  {
    vuln_line RUSTSEC-2026-0009 bincode 1.3.3
    summary_line 1
  } >"${TMP}/${name}.base"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.base"
  # FLIP: stop at the first new id (exit on the first hit), and the second
  # is never named; count lines rather than unique keys, and the count says 3.
  if [[ "${RC}" -eq 1 &&
    "${OUT}" == *"::error title=new advisory::RUSTSEC-2026-0007 rmcp "* &&
    "${OUT}" == *"::error title=new advisory::RUSTSEC-2026-0008 quick-xml "* &&
    "${OUT}" != *"::error title=new advisory::RUSTSEC-2026-0009"* &&
    "${OUT}" == *"2 new advisories vs base ${BASE_SHA}"* ]]; then
    pass "${name}"
  else
    fail "${name}" "rc=${RC}, wanted 1 naming 0007 and 0008 and not 0009"
  fi
}

test_version_move_within_range_is_not_new() {
  local name="version_move_within_range_is_not_new"
  {
    vuln_line RUSTSEC-2026-0461 rmcp 0.9.1
    summary_line 1
  } >"${TMP}/${name}.head"
  {
    vuln_line RUSTSEC-2026-0461 rmcp 0.9.0
    summary_line 1
  } >"${TMP}/${name}.base"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.base"
  # FLIP: put the crate version back in the key, and an unrelated bump that
  # re-resolves rmcp inside the advisory's range reads as a new finding, exit 1.
  if [[ "${RC}" -eq 0 &&
    "${OUT}" == *"::warning title=pre-existing advisory::RUSTSEC-2026-0461 rmcp is also present at the base ${BASE_SHA}"* &&
    "${OUT}" == *"0 new advisories vs base"* ]]; then
    pass "${name}"
  else
    fail "${name}" "rc=${RC}, wanted 0 with rmcp's advisory as pre-existing"
  fi
}

test_summary_without_advisories_exits_2() {
  local name="summary_without_advisories_exits_2"
  {
    vuln_line RUSTSEC-2026-0010 rmcp 0.8.0
    summary_line 1
  } >"${TMP}/${name}.head"
  # The summary of a run that did not check advisories (cargo-deny
  # serializes the unchecked kinds as null).
  printf '%s\n' '{"type":"summary","fields":{"advisories":null,"bans":{"errors":0,"warnings":0,"notes":0,"helps":0},"licenses":null,"sources":null}}' \
    >"${TMP}/${name}.base"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.base"
  # FLIP: drop the (.fields.advisories | type) == "object" condition from the
  # summary count, and this base counts as one summary with no findings, so
  # the head finding reads as new, exit 1.
  if [[ "${RC}" -eq 2 && "${OUT}" == *"holds 0 advisories summaries, wanted exactly 1"* ]]; then
    pass "${name}"
  else
    fail "${name}" "rc=${RC}, wanted 2"
  fi
}

# config_error_lines <path>: what cargo-deny prints when ValidConfig::load
# bails on a deny.toml it rejects: the config diagnostics, then main's ERROR
# log, and no summary.
config_error_lines() {
  printf '{"type":"diagnostic","fields":{"severity":"error","message":"unknown field","labels":[{"message":"unknown field","span":"unmaintained","line":3,"column":1}],"notes":[]}}\n'
  printf '{"type":"log","fields":{"timestamp":"2026-11-10T00:00:00Z","level":"ERROR","message":"failed to validate configuration file %s"}}\n' "$1"
}

test_base_config_rejected_judges_every_head_finding_new() {
  local name="base_config_rejected_judges_every_head_finding_new"
  config_error_lines /tmp/base/deny.toml >"${TMP}/${name}.base"
  {
    vuln_line RUSTSEC-2026-0011 rmcp 0.8.0
    summary_line 1
  } >"${TMP}/${name}.head"
  {
    log_line
    summary_line 0
  } >"${TMP}/${name}.cleanhead"
  # The same shape with an ERROR log that is not a config rejection: the
  # check died for some other reason, and that base is still refused.
  {
    printf '%s\n' '{"type":"log","fields":{"timestamp":"2026-11-10T00:00:00Z","level":"ERROR","message":"failed to load advisory database: no such file"}}'
  } >"${TMP}/${name}.otherbase"
  local rc_new out_new rc_clean out_clean rc_other rc_head
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.base"
  rc_new="${RC}"; out_new="${OUT}"
  run_guard "${TMP}/${name}.cleanhead" "${TMP}/${name}.base"
  rc_clean="${RC}"; out_clean="${OUT}"
  run_guard "${TMP}/${name}.head" "${TMP}/${name}.otherbase"
  rc_other="${RC}"
  # A head whose own config is rejected is never compared.
  run_guard "${TMP}/${name}.base" "${TMP}/${name}.head"
  rc_head="${RC}"
  # FLIP: return 2 for a config-rejected base like any other summary-less
  # base, and the first two runs exit 2; fall back on any summary-less base
  # with an ERROR log, and the third exits 1; apply the fallback to the head
  # as well, and the fourth exits 0.
  if [[ "${rc_new}" -eq 1 &&
    "${out_new}" == *"::warning title=base advisories not compared::cargo-deny rejected the config at the base ${BASE_SHA} (failed to validate configuration file /tmp/base/deny.toml)"* &&
    "${out_new}" == *"::error title=new advisory::RUSTSEC-2026-0011 rmcp is present at the head"* &&
    "${rc_clean}" -eq 0 && "${out_clean}" == *"0 new advisories vs base"* &&
    "${rc_other}" -eq 2 && "${rc_head}" -eq 2 ]]; then
    pass "${name}"
  else
    OUT="new: ${out_new}"$'\n'"clean: ${out_clean}"
    fail "${name}" "new=${rc_new} clean=${rc_clean} other=${rc_other} head=${rc_head}, wanted 1 0 2 2"
  fi
}

test_new_id_at_head_only_exits_1
test_id_on_both_exits_0_with_warning
test_nothing_new_prints_zero_line
test_missing_or_empty_base_json_exits_2
test_malformed_json_exits_2
test_two_new_ids_both_named
test_version_move_within_range_is_not_new
test_summary_without_advisories_exits_2
test_base_config_rejected_judges_every_head_finding_new

printf '\ncheck-advisory-delta.test.sh: %d passed, %d failed\n' "${passes}" "${fails}"
if [[ "${fails}" -ne 0 ]]; then
  exit 1
fi
exit 0
