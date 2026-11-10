#!/usr/bin/env bash
# Cases for ci-flake-sweep.sh. A stub `gh` on PATH serves canned responses
# from a per-case fixture directory and logs every call; no network call is
# made. Every timestamp is built with jq from a fixed "now", so no case
# depends on GNU or BSD date(1). macOS bash 3.2 subset: no associative
# arrays, no mapfile.
set -uo pipefail

SWEEP="$(cd "$(dirname "$0")" && pwd)/ci-flake-sweep.sh"

pass=0
fail=0
ok() {
  pass=$((pass + 1))
  echo "ok - $1"
}
not_ok() {
  fail=$((fail + 1))
  echo "not ok - $1: $2"
}

command -v jq >/dev/null 2>&1 || {
  echo "ci-flake-sweep.test.sh: jq is required" >&2
  exit 70
}

# at <now-iso> <offset-seconds> -> ISO-8601 UTC time offset from now.
at() {
  jq -nr --arg n "$1" --argjson o "$2" '($n | fromdateiso8601) + $o | todate'
}

root="$(mktemp -d)"
trap 'rm -rf "${root}"' EXIT

bin="${root}/bin"
mkdir -p "${bin}"
cat >"${bin}/gh" <<'STUB'
#!/usr/bin/env bash
# Fixture files under $STUB_DIR:
#   runs-merge_group.json runs-push.json  gh run list, by --event
#   jobs-<run>.json                       gh run view <run> --json jobs
#   log-<job>.txt                         gh run view --job <job> --log-failed
#   issues-<label>.json                   gh issue list --label <label>
#   prs.json                              gh pr list
# STUB_FAIL, when set, is a substring of the joined arguments that fails.
set -u
d="${STUB_DIR}"
printf '%s\n' "$*" >>"${d}/calls.log"
if [[ -n "${STUB_FAIL:-}" && "$*" == *"${STUB_FAIL}"* ]]; then
  echo "HTTP 502: Bad Gateway (stub)" >&2
  exit 1
fi
arg_after() {
  local want="$1"
  shift
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "${want}" ]]; then
      printf '%s' "${2:-}"
      return 0
    fi
    shift
  done
  return 1
}
serve() { if [[ -f "$1" ]]; then cat "$1"; else echo '[]'; fi; }
case "$1 $2" in
  "run list") serve "${d}/runs-$(arg_after --event "$@").json" ;;
  "run view")
    if job="$(arg_after --job "$@")"; then
      cat "${d}/log-${job}.txt" 2>/dev/null
    else
      if [[ -f "${d}/jobs-$3.json" ]]; then cat "${d}/jobs-$3.json"; else echo '{"jobs":[]}'; fi
    fi
    ;;
  "issue list") serve "${d}/issues-$(arg_after --label "$@").json" ;;
  "pr list") serve "${d}/prs.json" ;;
  "label create") ;;
  "issue create")
    n=$(($(ls "${d}" | grep -c '^created-') + 1))
    cp "$(arg_after --body-file "$@")" "${d}/created-${n}.md"
    echo "https://github.com/o/r/issues/$((900 + n))"
    ;;
  "issue edit") cp "$(arg_after --body-file "$@")" "${d}/edited-$3.md" ;;
  "issue reopen") ;;
  *)
    echo "stub gh: unexpected call: $*" >&2
    exit 1
    ;;
esac
STUB
chmod +x "${bin}/gh"

NOW="2026-11-10T12:00:00Z" # a Tuesday

new_case() {
  case_dir="${root}/$1"
  mkdir -p "${case_dir}"
  : >"${case_dir}/calls.log"
}

# one_failed_run <run> <job> <created-iso> <log-text> -> a merge_group run
# with one failed job whose log is <log-text>.
one_failed_run() {
  printf '[{"databaseId":%s,"createdAt":"%s","event":"merge_group","url":"https://github.com/o/r/actions/runs/%s"}]\n' \
    "$1" "$3" "$1" >"${case_dir}/runs-merge_group.json"
  printf '{"jobs":[{"databaseId":%s,"name":"test (1)","conclusion":"failure"},{"databaseId":1,"name":"lint","conclusion":"success"}]}\n' \
    "$2" >"${case_dir}/jobs-$1.json"
  printf '%b' "$4" >"${case_dir}/log-$2.txt"
}

# flaky_issue <number> <labels-json> <body> -> the flaky-test issue list.
flaky_issue() {
  jq -n --argjson n "$1" --argjson l "$2" --arg b "$3" \
    '[{number: $n, title: "flaky-test: ravel_sql::tests::alpha", state: "OPEN", body: $b, labels: ($l | map({name: .}))}]' \
    >"${case_dir}/issues-flaky-test.json"
}

run_sweep() {
  rc=0
  STUB_DIR="${case_dir}" PATH="${bin}:${PATH}" CI_FLAKE_SWEEP_NOW="${now_iso:-${NOW}}" \
    CI_FLAKE_SWEEP_RETRY_DELAY_S=0 bash "${SWEEP}" >"${case_dir}/out.txt" 2>&1 || rc=$?
}

count_calls() { grep -c "^$1" "${case_dir}/calls.log"; }

LOG_ALPHA='2026-11-10T01:00:00.0000000Z \033[0;31mtest ravel_sql::tests::alpha ... FAILED\033[0m\n'
LOG_ALPHA+="2026-11-10T01:00:00.0000000Z thread 'ravel_sql::tests::alpha' panicked at crates/ravel-sql/src/lib.rs:10:5:\n"
LOG_ALPHA+='2026-11-10T01:00:00.0000000Z error[E0308]: mismatched types\n'

# --- first_ejection_files_one_issue ---------------------------------------
# FLIP: keying the issue on the grep's raw match instead of the normalised
# identifier (normalize_ids reduced to a `sort -u` pass-through) files three
# issues here, one for the `test ... FAILED` line, one for the `thread ...
# panicked` line, one for `error[E0308]`; shown failing with that change.
new_case first_ejection
one_failed_run 501 7001 "$(at "${NOW}" -3600)" "${LOG_ALPHA}"
run_sweep
creates="$(count_calls 'issue create')"
if [[ "${rc}" == "0" && "${creates}" == "1" ]] &&
  grep -q -- "--title flaky-test: ravel_sql::tests::alpha --label flaky-test --body-file" "${case_dir}/calls.log" &&
  ! grep -q -- "flake-quarantine-due --body-file" "${case_dir}/calls.log" &&
  grep -q '^- failure run=501 at=' "${case_dir}/created-1.md"; then
  ok first_ejection_files_one_issue
else
  not_ok first_ejection_files_one_issue "rc=${rc} creates=${creates}; $(cat "${case_dir}/calls.log" "${case_dir}/out.txt")"
fi

# --- second_within_14_days_adds_quarantine_label --------------------------
# FLIP: a repeat window of 0 (REPEAT_WINDOW_S=0, i.e. counting only failures
# at the same instant) never adds the label; shown failing with that change.
new_case second_within
one_failed_run 502 7002 "$(at "${NOW}" -3600)" "${LOG_ALPHA}"
flaky_issue 77 '["flaky-test"]' "intro
- failure run=401 at=$(at "${NOW}" $((-3 * 86400))) event=merge_group https://x/401"
run_sweep
if [[ "${rc}" == "0" && "$(count_calls 'issue create')" == "0" ]] &&
  grep -q -- "^issue edit 77 --body-file .* --add-label flake-quarantine-due$" "${case_dir}/calls.log" &&
  grep -q '^- failure run=401 ' "${case_dir}/edited-77.md" &&
  grep -q '^- failure run=502 ' "${case_dir}/edited-77.md" &&
  grep -q "^- quarantine-due at=${NOW}$" "${case_dir}/edited-77.md"; then
  ok second_within_14_days_adds_quarantine_label
else
  not_ok second_within_14_days_adds_quarantine_label "rc=${rc}; $(cat "${case_dir}/calls.log" "${case_dir}/out.txt")"
fi

# --- third_updates_not_creates --------------------------------------------
# FLIP: looking the issue up among issues without flake-quarantine-due (a
# `select(.labels | length == 1)` in the lookup) creates a second issue for
# the same identifier on its third failure; shown failing with that change.
new_case third
one_failed_run 503 7003 "$(at "${NOW}" -3600)" "${LOG_ALPHA}"
flaky_issue 77 '["flaky-test","flake-quarantine-due"]' "intro
- failure run=401 at=$(at "${NOW}" $((-5 * 86400))) event=merge_group https://x/401
- failure run=402 at=$(at "${NOW}" $((-3 * 86400))) event=merge_group https://x/402
- quarantine-due at=$(at "${NOW}" $((-3 * 86400)))"
printf '[{"number":3001,"title":"test: quarantine alpha","body":"Refs: #77"}]\n' >"${case_dir}/prs.json"
cp "${case_dir}/issues-flaky-test.json" "${case_dir}/issues-flake-quarantine-due.json"
run_sweep
if [[ "${rc}" == "0" && "$(count_calls 'issue create')" == "0" ]] &&
  grep -q -- "^issue edit 77 --body-file [^ ]*$" "${case_dir}/calls.log" &&
  grep -q '^- failure run=503 ' "${case_dir}/edited-77.md" &&
  [[ "$(grep -c '^- quarantine-due' "${case_dir}/edited-77.md")" == "1" ]]; then
  ok third_updates_not_creates
else
  not_ok third_updates_not_creates "rc=${rc}; $(cat "${case_dir}/calls.log" "${case_dir}/out.txt")"
fi

# --- same_run_seen_twice_is_one_ejection ----------------------------------
# Sweeps every 6 h over a 24 h window see each run up to four times.
# FLIP: appending without checking the ledger's run ids (the `$seen` filter
# removed) edits the issue and adds the label for a run already counted;
# shown failing with that change.
new_case same_run
one_failed_run 401 7004 "$(at "${NOW}" -3600)" "${LOG_ALPHA}"
flaky_issue 77 '["flaky-test"]' "intro
- failure run=401 at=$(at "${NOW}" -3600) event=merge_group https://x/401"
run_sweep
if [[ "${rc}" == "0" && "$(count_calls 'issue create')" == "0" && "$(count_calls 'issue edit')" == "0" ]]; then
  ok same_run_seen_twice_is_one_ejection
else
  not_ok same_run_seen_twice_is_one_ejection "rc=${rc}; $(cat "${case_dir}/calls.log" "${case_dir}/out.txt")"
fi

# --- second_after_14_days_does_not_add_label ------------------------------
# FLIP: adding the label on any second failure regardless of age (the
# `<= $repeat` test replaced by `true`) labels this one; shown failing with
# that change.
new_case second_after
one_failed_run 504 7005 "$(at "${NOW}" -3600)" "${LOG_ALPHA}"
flaky_issue 77 '["flaky-test"]' "intro
- failure run=401 at=$(at "${NOW}" $((-15 * 86400))) event=merge_group https://x/401"
run_sweep
if [[ "${rc}" == "0" && "$(count_calls 'issue create')" == "0" ]] &&
  grep -q -- "^issue edit 77 --body-file [^ ]*$" "${case_dir}/calls.log" &&
  grep -q '^- failure run=504 ' "${case_dir}/edited-77.md" &&
  ! grep -q '^- quarantine-due' "${case_dir}/edited-77.md"; then
  ok second_after_14_days_does_not_add_label
else
  not_ok second_after_14_days_does_not_add_label "rc=${rc}; $(cat "${case_dir}/calls.log" "${case_dir}/out.txt")"
fi

# due_issue <case> <due-since-iso> writes an open flake-quarantine-due issue
# #77 whose ledger marks it due at <due-since-iso>, and no failed runs.
due_issue() {
  jq -n --arg since "$1" \
    '[{number: 77, title: "flaky-test: ravel_sql::tests::alpha", createdAt: "2026-10-01T00:00:00Z",
       body: ("intro\n- failure run=401 at=\($since) event=merge_group u\n- quarantine-due at=\($since)\n")}]' \
    >"${case_dir}/issues-flake-quarantine-due.json"
}

# --- due_issue_older_than_one_working_day_without_pr_fails ----------------
# Due Monday 09:00, now Tuesday 12:00: 27 working hours. The only open pull
# request cites #770, not #77.
# FLIP: matching the reference as a plain substring (the `([^0-9]|$)` tail
# dropped from the regex) reads #770 as a reference to #77 and passes;
# shown failing with that change.
new_case due_no_pr
due_issue "2026-11-09T09:00:00Z"
printf '[{"number":3002,"title":"fix other","body":"Fixes: #770"}]\n' >"${case_dir}/prs.json"
run_sweep
if [[ "${rc}" == "1" ]] && grep -q '^OVERDUE: #77 ' "${case_dir}/out.txt"; then
  ok due_issue_older_than_one_working_day_without_pr_fails
else
  not_ok due_issue_older_than_one_working_day_without_pr_fails "rc=${rc}; $(cat "${case_dir}/out.txt")"
fi

# --- due_issue_with_referencing_pr_passes ---------------------------------
# FLIP: failing on every overdue issue without consulting open pull requests
# (`refs` forced empty) exits 1 here; shown failing with that change.
new_case due_with_pr
due_issue "2026-11-09T09:00:00Z"
printf '[{"number":3002,"title":"fix other","body":"Fixes: #770"},{"number":3003,"title":"test: quarantine alpha (#77)","body":""}]\n' \
  >"${case_dir}/prs.json"
run_sweep
if [[ "${rc}" == "0" ]] && grep -q '^#77 .*referenced by #3003$' "${case_dir}/out.txt"; then
  ok due_issue_with_referencing_pr_passes
else
  not_ok due_issue_with_referencing_pr_passes "rc=${rc}; $(cat "${case_dir}/out.txt")"
fi

# --- weekend_does_not_count_as_a_working_day ------------------------------
# Due Friday 18:00, now Sunday 23:00: 53 wall-clock hours, 6 working hours.
# FLIP: counting every day (the `$w >= 1 and $w <= 5` test replaced by
# `true`) calls it overdue and exits 1; shown failing with that change.
new_case weekend
due_issue "2026-11-06T18:00:00Z"
now_iso="2026-11-08T23:00:00Z"
run_sweep
now_iso=""
if [[ "${rc}" == "0" ]] && ! grep -q '^OVERDUE' "${case_dir}/out.txt"; then
  ok weekend_does_not_count_as_a_working_day
else
  not_ok weekend_does_not_count_as_a_working_day "rc=${rc}; $(cat "${case_dir}/out.txt")"
fi

# --- gh_failure_exits_2 ---------------------------------------------------
# The run list fails on every attempt. FLIP: treating a failed run list as
# "no failed runs" (`|| could_not_ask` replaced by `|| true`) exits 0, a
# clean sweep that asked nothing; shown failing with that change.
new_case gh_fail
one_failed_run 505 7006 "$(at "${NOW}" -3600)" "${LOG_ALPHA}"
export STUB_FAIL="run list"
run_sweep
unset STUB_FAIL
attempts="$(count_calls 'run list')"
if [[ "${rc}" == "2" && "${attempts}" == "3" && "$(count_calls 'issue create')" == "0" ]]; then
  ok gh_failure_exits_2
else
  not_ok gh_failure_exits_2 "rc=${rc} run-list attempts=${attempts}; $(cat "${case_dir}/out.txt")"
fi

echo
echo "${pass} passed, ${fail} failed"
[[ "${fail}" == "0" ]]
