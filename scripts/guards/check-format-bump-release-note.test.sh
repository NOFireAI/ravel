#!/usr/bin/env bash
# Cases for check-format-bump-release-note.sh, in the pattern of
# check-changelog-touched.test.sh: add a case here before changing a rule.
#
# Each case builds a throwaway git repo under $TMPDIR seeded with the three
# version anchors, commits a range on top, and runs the guard against it.
#
# Run: bash scripts/guards/check-format-bump-release-note.test.sh
set -uo pipefail

export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-format-bump-release-note.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-format-bump-release-note-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

RLOG=crates/ravel-logseg/src/footer.rs
RSPAN=crates/ravel-rspan/src/footer.rs
RSEG=crates/ravel-segment/src/format.rs

# write_rlog <dir> <version> [<supported>]: the RLOG anchors; <supported>
# defaults to SupportedVersions::single(VERSION).
write_rlog() {
  printf '/// RLOG trailer version.\npub const VERSION: u16 = %s;\npub const SUPPORTED_VERSIONS: SupportedVersions = %s;\n' \
    "$2" "${3:-SupportedVersions::single(VERSION)}" >"$1/${RLOG}"
}
write_rspan() {
  printf '/// RSPAN trailer version.\npub const VERSION: u16 = %s;\npub const SUPPORTED_VERSIONS: SupportedVersions = %s;\n' \
    "$2" "${3:-SupportedVersions::single(VERSION)}" >"$1/${RSPAN}"
}
write_rseg() {
  printf 'pub const VERSION: u16 = 1;\nimpl SegmentVersion {\n    pub const WINDOW: &'"'"'static [SegmentVersion] = &[%s];\n}\n' \
    "$2" >"$1/${RSEG}"
}

# new_repo <name>: a repo with a seed commit holding all three anchors
# (RLOG 5, RSPAN 4, RSEG window [V7]) and the guard. Prints its path.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/changelog.d" \
    "${dir}/crates/ravel-logseg/src" "${dir}/crates/ravel-rspan/src" \
    "${dir}/crates/ravel-segment/src"
  cp "${GUARD}" "${dir}/scripts/guards/check-format-bump-release-note.sh"
  git -C "${dir}" init -q -b main 2>/dev/null
  git -C "${dir}" config user.email t@example.com
  git -C "${dir}" config user.name Test
  write_rlog "${dir}" 5
  write_rspan "${dir}" 4
  write_rseg "${dir}" "SegmentVersion::V7"
  printf '# Changelog fragments\n' >"${dir}/changelog.d/README.md"
  git -C "${dir}" add -A
  git -C "${dir}" commit -q -m "chore: seed"
  printf '%s\n' "${dir}"
}

commit_all() {
  git -C "$1" add -A
  git -C "$1" commit -q -m "$2"
}

# check <name> <repo> <base> <head> <want-exit> [<want-substring> ...]
check() {
  local name="$1" dir="$2" base="$3" head="$4" want_rc="$5"
  shift 5
  local out code=0 sub
  out="$(cd "${dir}" && "${BASH}" scripts/guards/check-format-bump-release-note.sh "${base}" "${head}" 2>&1)" || code=$?
  if [[ "${code}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${code}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  for sub in "$@"; do
    if [[ "${out}" != *"${sub}"* ]]; then
      printf 'FAIL  %s: output missing %s\n' "${name}" "${sub}"
      printf '%s\n' "${out}" | sed 's/^/      /'
      fails=$((fails + 1))
      return
    fi
  done
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# check_absent <name> <repo> <base> <head> <substring>: the guard's output
# does not contain <substring>.
check_absent() {
  local name="$1" dir="$2" base="$3" head="$4" sub="$5" out
  out="$(cd "${dir}" && "${BASH}" scripts/guards/check-format-bump-release-note.sh "${base}" "${head}" 2>&1)" || true
  if [[ "${out}" == *"${sub}"* ]]; then
    printf 'FAIL  %s: output contains %s\n' "${name}" "${sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- (a) no_constant_change_passes ------------------------------------------

d="$(new_repo no-change)"
base="$(git -C "${d}" rev-parse HEAD)"
printf 'fn other() {}\n' >"${d}/crates/ravel-logseg/src/other.rs"
commit_all "${d}" "feat(logseg): unrelated"
check "a_no_constant_change_passes" "${d}" "${base}" HEAD 0 "clean (no format version"

# --- (b) rlog_bump_with_all_signals_passes ----------------------------------

d="$(new_repo rlog-all)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6
printf -- '- **RLOG v6.** Stored logs, alerts and audit records need migration.\n' \
  >"${d}/changelog.d/100.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
check "b_rlog_bump_with_all_signals_passes" "${d}" "${base}" HEAD 0 "5 -> 6"

# --- (c) rlog_bump_naming_only_logs_fails -----------------------------------

d="$(new_repo rlog-logs-only)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6
printf -- '- **RLOG v6.** Stored logs need migration.\n' >"${d}/changelog.d/100.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
check "c_rlog_bump_naming_only_logs_fails" "${d}" "${base}" HEAD 1 \
  "RLOG bump: no changelog fragment" "signal 'alerts'" "signal 'audit'"
check_absent "c_rlog_bump_does_not_report_logs" "${d}" "${base}" HEAD "signal 'logs'"

# --- (d) rspan_bump_without_fragment_fails ----------------------------------

d="$(new_repo rspan-none)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rspan "${d}" 5
commit_all "${d}" "feat(rspan): bump RSPAN to v5"
check "d_rspan_bump_without_fragment_fails" "${d}" "${base}" HEAD 1 \
  "RSPAN bump" "signal 'spans'"

# --- (e) footer_edit_without_version_change_passes --------------------------
# The trigger is the anchor's value, not the file changing.

d="$(new_repo footer-edit)"
base="$(git -C "${d}" rev-parse HEAD)"
printf '/// Reworded doc comment.\npub const VERSION: u16 = 5;\npub const SUPPORTED_VERSIONS: SupportedVersions = SupportedVersions::single(VERSION);\npub const OTHER: u16 = 9;\n' \
  >"${d}/${RLOG}"
commit_all "${d}" "docs(logseg): reword footer docs"
check "e_footer_edit_without_version_change_passes" "${d}" "${base}" HEAD 0 \
  "clean (no format version"

# --- (f) unresolvable_base_ref_exits_2 --------------------------------------

d="$(new_repo bad-ref)"
check "f_unresolvable_base_ref_exits_2" "${d}" no-such-ref HEAD 2 "cannot resolve base ref"

# --- (g) missing_constant_exits_2 -------------------------------------------
# A moved anchor must not pass silently, even with no bump to check.

d="$(new_repo missing)"
base="$(git -C "${d}" rev-parse HEAD)"
printf 'pub const TRAILER_VERSION: u16 = 4;\n' >"${d}/${RSPAN}"
commit_all "${d}" "refactor(rspan): rename the constant"
check "g_missing_constant_exits_2" "${d}" "${base}" HEAD 2 \
  "RSPAN version anchor not found exactly once"

# --- missing_argument_exits_2 -----------------------------------------------

d="$(new_repo no-arg)"
code=0
(cd "${d}" && "${BASH}" scripts/guards/check-format-bump-release-note.sh >/dev/null 2>&1) || code=$?
if [[ "${code}" == 2 ]]; then
  printf 'ok    missing_argument_exits_2\n'
  passes=$((passes + 1))
else
  printf 'FAIL  missing_argument_exits_2: exit %s, wanted 2\n' "${code}"
  fails=$((fails + 1))
fi

# --- ambiguous_constant_exits_2 ---------------------------------------------

d="$(new_repo ambiguous)"
base="$(git -C "${d}" rev-parse HEAD)"
printf 'pub const VERSION: u16 = 5;\npub const VERSION: u16 = 6;\n' >"${d}/${RLOG}"
commit_all "${d}" "chore: two anchors"
check "ambiguous_constant_exits_2" "${d}" "${base}" HEAD 2 \
  "RLOG version anchor not found exactly once"

# --- rseg_window_bump_without_metrics_fails ---------------------------------

d="$(new_repo rseg-none)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rseg "${d}" "SegmentVersion::V8, SegmentVersion::V7"
printf -- '- **RSEG v8.** Segments gain a section.\n' >"${d}/changelog.d/100.added.md"
commit_all "${d}" "feat(segment): add RSEG v8 to the window"
check "rseg_window_bump_without_metrics_fails" "${d}" "${base}" HEAD 1 \
  "RSEG format version changed" "signal 'metrics'"

# --- rseg_window_bump_naming_metric_passes ----------------------------------

d="$(new_repo rseg-metric)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rseg "${d}" "SegmentVersion::V8"
printf -- '- **RSEG v8.** Every stored metric object needs migration.\n' \
  >"${d}/changelog.d/100.changed.md"
commit_all "${d}" "feat(segment): bump RSEG to v8"
check "rseg_window_bump_naming_metric_passes" "${d}" "${base}" HEAD 0

# --- rseg_retired_version_constant_is_not_the_anchor ------------------------

d="$(new_repo rseg-retired)"
base="$(git -C "${d}" rev-parse HEAD)"
sed -i.bak 's/^pub const VERSION: u16 = 1;/pub const VERSION: u16 = 2;/' "${d}/${RSEG}"
rm -f "${d}/${RSEG}.bak"
commit_all "${d}" "chore(segment): edit the retired constant"
check "rseg_retired_version_constant_is_not_the_anchor" "${d}" "${base}" HEAD 0 \
  "clean (no format version"

# --- signals_match_case_insensitively ---------------------------------------

d="$(new_repo case)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6
printf -- '- **RLOG v6.** LOGS, Alert transitions and AUDIT records.\n' \
  >"${d}/changelog.d/100.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
check "signals_match_case_insensitively" "${d}" "${base}" HEAD 0

# --- signals_match_whole_words_only -----------------------------------------
# "catalogs", "alertsmanager" and "auditor" contain the signals but are not
# them.

d="$(new_repo whole-word)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6
printf -- '- **RLOG v6.** catalogs, alertsmanager, auditor.\n' \
  >"${d}/changelog.d/100.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
check "signals_match_whole_words_only" "${d}" "${base}" HEAD 1 \
  "signal 'logs'" "signal 'alerts'" "signal 'audit'"

# --- fragments_are_taken_together -------------------------------------------

d="$(new_repo together)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6
printf -- '- Stored logs need migration.\n' >"${d}/changelog.d/100.changed.md"
printf -- '- Alerts and audit records too.\n' >"${d}/changelog.d/100-2.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
check "fragments_are_taken_together" "${d}" "${base}" HEAD 0

# --- edited_fragment_counts -------------------------------------------------

d="$(new_repo edited)"
printf -- '- An unreleased entry.\n' >"${d}/changelog.d/90.changed.md"
commit_all "${d}" "chore: unreleased fragment"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6
printf -- '- An unreleased entry. RLOG v6: logs, alerts and audit migrate.\n' \
  >"${d}/changelog.d/90.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
check "edited_fragment_counts" "${d}" "${base}" HEAD 0

# --- non_fragment_file_does_not_count ---------------------------------------

d="$(new_repo non-fragment)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rspan "${d}" 5
printf 'spans\n' >"${d}/changelog.d/notes.txt"
commit_all "${d}" "feat(rspan): bump RSPAN to v5"
check "non_fragment_file_does_not_count" "${d}" "${base}" HEAD 1 "signal 'spans'"

# --- range_starts_at_merge_base ---------------------------------------------
# A fragment that landed on the base branch after the fork point is not this
# range's.

d="$(new_repo merge-base)"
fork="$(git -C "${d}" rev-parse HEAD)"
git -C "${d}" checkout -q -b topic
write_rspan "${d}" 5
commit_all "${d}" "feat(rspan): bump RSPAN to v5"
git -C "${d}" checkout -q main
printf -- '- Something about spans.\n' >"${d}/changelog.d/200.fixed.md"
commit_all "${d}" "fix: neighbour"
check "range_starts_at_merge_base" "${d}" main topic 1 "signal 'spans'" "${fork}.."

# --- rlog_reader_window_change_requires_a_note -------------------------------
# Dropping the previous reader moves SUPPORTED_VERSIONS and not VERSION, and
# leaves stored objects of that version unreadable just the same.

d="$(new_repo rlog-window)"
write_rlog "${d}" 5 "SupportedVersions::n_and_prev(VERSION)"
commit_all "${d}" "feat(logseg): read v4 and v5"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 5
commit_all "${d}" "feat(logseg): drop the v4 reader"
check "rlog_reader_window_change_requires_a_note" "${d}" "${base}" HEAD 1 \
  "RLOG format version changed" "signal 'logs'" "signal 'alerts'" "signal 'audit'"

# --- rspan_reader_window_change_with_note_passes ----------------------------

d="$(new_repo rspan-window)"
write_rspan "${d}" 4 "SupportedVersions::n_and_prev(VERSION)"
commit_all "${d}" "feat(rspan): read v3 and v4"
base="$(git -C "${d}" rev-parse HEAD)"
write_rspan "${d}" 4
printf -- '- Stored spans written as RSPAN v3 are no longer readable.\n' \
  >"${d}/changelog.d/301.removed.md"
commit_all "${d}" "feat(rspan): drop the v3 reader"
check "rspan_reader_window_change_with_note_passes" "${d}" "${base}" HEAD 0 \
  "fragments in"

# --- version_and_window_in_one_change_reports_each_signal_once --------------

d="$(new_repo rlog-both)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6 "SupportedVersions::n_and_prev(VERSION)"
printf -- '- Logs and alerts move to RLOG v6.\n' >"${d}/changelog.d/302.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
out="$(cd "${d}" && "${BASH}" scripts/guards/check-format-bump-release-note.sh "${base}" HEAD 2>&1)" || true
n="$(printf '%s\n' "${out}" | grep -c "signal 'audit'")" || true
if [[ "${n}" == "1" ]]; then
  printf 'ok    %s\n' "version_and_window_in_one_change_reports_each_signal_once"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: audit reported %s times, wanted 1\n' \
    "version_and_window_in_one_change_reports_each_signal_once" "${n}"
  printf '%s\n' "${out}" | sed 's/^/      /'
  fails=$((fails + 1))
fi

# --- code_spans_and_urls_do_not_name_a_signal --------------------------------

d="$(new_repo code-spans)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6
# shellcheck disable=SC2016 # literal backticks: the fragment's code span
printf -- '- Logs and alerts move to RLOG v6. Run `ravel-cli audit-versions` and\n  check /v1/audit or https://example.com/audit before upgrading.\n' \
  >"${d}/changelog.d/303.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
check "code_spans_and_urls_do_not_name_a_signal" "${d}" "${base}" HEAD 1 "signal 'audit'"

# --- singular_log_names_the_logs_signal --------------------------------------

d="$(new_repo singular-log)"
base="$(git -C "${d}" rev-parse HEAD)"
write_rlog "${d}" 6
printf -- '- Log, alert and audit objects move to RLOG v6.\n' >"${d}/changelog.d/304.changed.md"
commit_all "${d}" "feat(logseg): bump RLOG to v6"
check "singular_log_names_the_logs_signal" "${d}" "${base}" HEAD 0 "fragments in"

echo
echo "check-format-bump-release-note.test.sh: ${passes} passed, ${fails} failed"
[[ "${fails}" -eq 0 ]]
