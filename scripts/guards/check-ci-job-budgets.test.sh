#!/usr/bin/env bash
# Cases for check-ci-job-budgets.sh. A stub `gh` on PATH serves canned
# per-call responses from environment variables; no network call is made.
# Every case is fixture-isolated: its own temp git repo with its own
# .github/workflows/ci.yml, so these never depend on (or race with) the
# real repo's ci.yml, which another task edits this same wave.
set -uo pipefail

GUARD="$(cd "$(dirname "$0")" && pwd)/check-ci-job-budgets.sh"
BASE_EPOCH=1735689600 # 2025-01-01T00:00:00Z

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

ts() {
  # ts <offset_minutes> -> ISO-8601 timestamp that many minutes after
  # BASE_EPOCH. GNU date first, then BSD/macOS date.
  local e=$((BASE_EPOCH + $1 * 60))
  date -u -d "@${e}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null && return 0
  date -u -r "${e}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null
}

# Every fixture row is built from ts(); a date that cannot format an epoch
# would leave every timestamp blank and every case asking about nothing.
if [[ -z "$(ts 0)" ]]; then
  echo "check-ci-job-budgets.test.sh: ts 0 printed nothing; this date(1) can format an epoch neither as GNU nor as BSD date" >&2
  exit 70
fi

new_repo() {
  # new_repo <ci_yml_content_varname> -> prints the repo path. Writes,
  # git-inits and commits a repo whose ci.yml is the content of the named
  # variable.
  local content_var="$1" dir
  dir="$(mktemp -d)"
  mkdir -p "${dir}/.github/workflows"
  printf '%s' "${!content_var}" >"${dir}/.github/workflows/ci.yml"
  (
    cd "${dir}" || exit 1
    git init -q
    git config user.email "test@example.invalid"
    git config user.name "Test"
    git add -A
    git commit -q -m init
  )
  printf '%s' "${dir}"
}

write_stub_gh() {
  # write_stub_gh <bin_dir> -- installs a `gh` that answers only the two call
  # shapes the guard makes (a run-list query, a per-run jobs query), reading
  # canned responses from STUB_* environment variables the test exported.
  local bindir="$1"
  mkdir -p "${bindir}"
  cat >"${bindir}/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
if [[ "$1" != "api" ]]; then
  echo "stub gh: unsupported subcommand: $1" >&2
  exit 1
fi
path="$2"
case "${path}" in
  *"/jobs?"*)
    run_id="$(printf '%s' "${path}" | sed -E 's#.*/runs/([0-9]+)/jobs.*#\1#')"
    rc_var="STUB_JOBS_RC_${run_id}"
    rc="${!rc_var:-0}"
    if [[ "${rc}" != "0" ]]; then
      echo "stub gh: simulated jobs failure for ${run_id}" >&2
      exit "${rc}"
    fi
    # STUB_JOBS_FAILS_<id>=N: the first N calls for that run answer 502.
    fails_var="STUB_JOBS_FAILS_${run_id}"
    fails="${!fails_var:-0}"
    if [[ "${fails}" != "0" ]]; then
      count_file="${STUB_STATE_DIR}/jobs_calls_${run_id}"
      calls="$(cat "${count_file}" 2>/dev/null || echo 0)"
      echo $((calls + 1)) >"${count_file}"
      if ((calls < fails)); then
        echo "gh: Server Error (HTTP 502)" >&2
        exit 1
      fi
    fi
    var="STUB_JOBS_${run_id}"
    val="${!var:-}"
    [[ -n "${val}" ]] && printf '%s' "${val}"
    exit 0
    ;;
  *"/actions/workflows/ci.yml/runs?"*)
    key=""
    case "${path}" in
      *event=push*created=*) key="PUSH_OLDER" ;;
      *event=merge_group*created=*) key="MG_OLDER" ;;
      *event=push*) key="PUSH_CURRENT" ;;
      *event=merge_group*) key="MG_CURRENT" ;;
      *)
        echo "stub gh: unrecognized runs query: ${path}" >&2
        exit 1
        ;;
    esac
    rc_var="STUB_${key}_RC"
    rc="${!rc_var:-0}"
    if [[ "${rc}" != "0" ]]; then
      echo "stub gh: simulated runs failure for ${key}" >&2
      exit "${rc}"
    fi
    var="STUB_${key}_IDS"
    val="${!var:-}"
    if [[ -n "${val}" ]]; then
      for id in ${val}; do printf '%s\n' "${id}"; done
    fi
    exit 0
    ;;
  *)
    echo "stub gh: unrecognized api path: ${path}" >&2
    exit 1
    ;;
esac
STUB
  chmod +x "${bindir}/gh"
}

job_row() {
  # job_row <name> <conclusion> <start_offset_min> <duration_min>
  local name="$1" conclusion="$2" start_off="$3" dur="$4"
  printf '%s\t%s\t%s\t%s\n' "${name}" "${conclusion}" "$(ts "${start_off}")" "$(ts "$((start_off + dur))")"
}

CI_YML_TESTJOB='jobs:
  testjob:
    runs-on: ubuntu-latest
    timeout-minutes: 100
    steps:
      - run: echo hi
'

CI_YML_TESTJOB_BIGCAP='jobs:
  testjob:
    runs-on: ubuntu-latest
    timeout-minutes: 1000
    steps:
      - run: echo hi
'

CI_YML_SQL='jobs:
  sql:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - run: echo hi
'

# Base fixture used by the cap/growth live-mode cases below: current pool
# gets its points from push_current (2 runs) and mg_current (2 runs); older
# pool from push_older/mg_older the same way. Both pools need at least one
# job row: an empty pool is "could not ask" (exit 2), not clean. A run whose
# jobs response is empty is fine as long as another run fills the pool.
run_live_case() {
  # run_live_case <name> <repo_dir> <extra exported STUB_* already set by caller>
  local case_name="$1" repo_dir="$2" bindir out rc
  bindir="$(mktemp -d)"
  write_stub_gh "${bindir}"
  out="$(cd "${repo_dir}" && STUB_STATE_DIR="${bindir}" CI_BUDGET_RETRY_DELAY_S=0 PATH="${bindir}:${PATH}" "${GUARD}" --repo acme/widgets --runs 4 2>&1)"
  rc=$?
  LAST_OUT="${out}"
  LAST_RC="${rc}"
}

test_p90_at_071_of_cap_is_a_finding() {
  local name="p90_at_071_of_cap_is_a_finding"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS="101 102"
  export STUB_MG_CURRENT_IDS="103 104"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 20)"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 30)"
  export STUB_JOBS_104; STUB_JOBS_104="$(job_row testjob success 0 71)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 71)"
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_104 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP this rule's assertion by comparing p50 instead of p90 and this case
  # would pass (p50 of {10,20,30,71} is 20, far under 0.7 of a 100m cap).
  if [[ "${LAST_RC}" -eq 1 ]] && grep -q "FINDING cap job=testjob" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_p90_at_069_of_cap_passes() {
  local name="p90_at_069_of_cap_passes"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS="101 102"
  export STUB_MG_CURRENT_IDS="103 104"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 20)"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 30)"
  export STUB_JOBS_104; STUB_JOBS_104="$(job_row testjob success 0 69)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 69)"
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_104 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # The row and its ratio are asserted, not only rc 0: a guard that read no
  # rows at all also exits 0 with nothing to say.
  if [[ "${LAST_RC}" -eq 0 ]] && grep -q "^testjob	.*p90=69.0000m	cap=100	ratio=0.6900	" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_transient_502_is_retried() {
  local name="transient_502_is_retried"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS="101 102"
  export STUB_MG_CURRENT_IDS="103 104"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 20)"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 30)"
  export STUB_JOBS_104; STUB_JOBS_104="$(job_row testjob success 0 69)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 69)"
  export STUB_JOBS_202=""
  export STUB_JOBS_FAILS_104=2
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_104 STUB_JOBS_201 STUB_JOBS_202 \
    STUB_JOBS_FAILS_104
  rm -rf "${repo}"
  # FLIP: one attempt per call, and the two 502s make this exit 2.
  if [[ "${LAST_RC}" -eq 0 ]] && grep -q "^testjob	.*p90=69.0000m	cap=100	ratio=0.6900	" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_persistent_502_exits_2() {
  local name="persistent_502_exits_2"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS="101 102"
  export STUB_MG_CURRENT_IDS="103 104"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 20)"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 30)"
  export STUB_JOBS_104; STUB_JOBS_104="$(job_row testjob success 0 69)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 69)"
  export STUB_JOBS_202=""
  export STUB_JOBS_FAILS_104=3
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_104 STUB_JOBS_201 STUB_JOBS_202 \
    STUB_JOBS_FAILS_104
  rm -rf "${repo}"
  # FLIP: retry without a bound, or treat the last failure as no jobs.
  if [[ "${LAST_RC}" -eq 2 ]] && grep -q "jobs for run 104 failed" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_growth_25_percent_is_a_finding() {
  local name="growth_25_percent_is_a_finding"
  local repo; repo="$(new_repo CI_YML_TESTJOB_BIGCAP)"
  export STUB_PUSH_CURRENT_IDS="101 102"
  export STUB_MG_CURRENT_IDS="103 104"
  export STUB_PUSH_OLDER_IDS="201 202"
  export STUB_MG_OLDER_IDS="203 204"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 20)"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 30)"
  export STUB_JOBS_104; STUB_JOBS_104="$(job_row testjob success 0 125)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 10)"
  export STUB_JOBS_202; STUB_JOBS_202="$(job_row testjob success 0 20)"
  export STUB_JOBS_203; STUB_JOBS_203="$(job_row testjob success 0 30)"
  export STUB_JOBS_204; STUB_JOBS_204="$(job_row testjob success 0 100)"
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_104 \
    STUB_JOBS_201 STUB_JOBS_202 STUB_JOBS_203 STUB_JOBS_204
  rm -rf "${repo}"
  # FLIP: drop the growth rule and this case would pass (ratio to the 1000m
  # cap is only 0.125, nowhere near the 0.7 cap threshold).
  if [[ "${LAST_RC}" -eq 1 ]] && grep -q "FINDING growth job=testjob" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

run_growth_floor_case() {
  # run_growth_floor_case <current_minutes>: older p90 2 minutes, current p90
  # <current_minutes>, so growth is well above 1.25 either way.
  local cur="$1" repo; repo="$(new_repo CI_YML_TESTJOB_BIGCAP)"
  export STUB_PUSH_CURRENT_IDS="101 102"
  export STUB_MG_CURRENT_IDS="103 104"
  export STUB_PUSH_OLDER_IDS="201 202"
  export STUB_MG_OLDER_IDS="203 204"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 "${cur}")"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 "${cur}")"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 "${cur}")"
  export STUB_JOBS_104; STUB_JOBS_104="$(job_row testjob success 0 "${cur}")"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 2)"
  export STUB_JOBS_202; STUB_JOBS_202="$(job_row testjob success 0 2)"
  export STUB_JOBS_203; STUB_JOBS_203="$(job_row testjob success 0 2)"
  export STUB_JOBS_204; STUB_JOBS_204="$(job_row testjob success 0 2)"
  run_live_case "growth_floor_${cur}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_104 \
    STUB_JOBS_201 STUB_JOBS_202 STUB_JOBS_203 STUB_JOBS_204
  rm -rf "${repo}"
}

test_short_job_growth_is_not_a_finding() {
  local name="short_job_growth_is_not_a_finding"
  run_growth_floor_case 4
  # FLIP: apply the growth rule whatever the job's length, and 2 to 4
  # minutes (2.0x) is a finding.
  if [[ "${LAST_RC}" -eq 0 ]] && grep -q "^testjob	.*p90=4.0000m	.*growth=2.0000	" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_five_minute_job_growth_is_a_finding() {
  local name="five_minute_job_growth_is_a_finding"
  run_growth_floor_case 5
  # FLIP: a floor above 5 minutes, or a strict greater-than at the floor.
  if [[ "${LAST_RC}" -eq 1 ]] && grep -q "FINDING growth job=testjob" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_growth_24_percent_passes() {
  local name="growth_24_percent_passes"
  local repo; repo="$(new_repo CI_YML_TESTJOB_BIGCAP)"
  export STUB_PUSH_CURRENT_IDS="101 102"
  export STUB_MG_CURRENT_IDS="103 104"
  export STUB_PUSH_OLDER_IDS="201 202"
  export STUB_MG_OLDER_IDS="203 204"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 20)"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 30)"
  export STUB_JOBS_104; STUB_JOBS_104="$(job_row testjob success 0 124)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 10)"
  export STUB_JOBS_202; STUB_JOBS_202="$(job_row testjob success 0 20)"
  export STUB_JOBS_203; STUB_JOBS_203="$(job_row testjob success 0 30)"
  export STUB_JOBS_204; STUB_JOBS_204="$(job_row testjob success 0 100)"
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_104 \
    STUB_JOBS_201 STUB_JOBS_202 STUB_JOBS_203 STUB_JOBS_204
  rm -rf "${repo}"
  # The row and its growth figure are asserted, not only rc 0.
  if [[ "${LAST_RC}" -eq 0 ]] && grep -q "^testjob	.*p90=124.0000m	.*growth=1.2400	n=4$" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_failed_query_exits_2() {
  local name="failed_query_exits_2"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_RC="1"
  export STUB_MG_CURRENT_IDS="103"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_103=""
  export STUB_JOBS_201=""
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_RC STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_103 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP: treat a failed call as zero runs (swallow it) and this would exit 2
  # only coincidentally via "no runs" rather than via the failure itself --
  # the distinct stub-failure message pins that the failure path fired.
  if [[ "${LAST_RC}" -eq 2 ]] && grep -q "gh api .* failed" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_empty_run_list_exits_2() {
  local name="empty_run_list_exits_2"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS=""
  export STUB_MG_CURRENT_IDS="103"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_103=""
  export STUB_JOBS_201=""
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_103 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP: treat an empty run list as zero runs with no error and this would
  # exit 0 or 1 depending on the (empty) pool, never report a query problem.
  if [[ "${LAST_RC}" -eq 2 ]] && grep -q "returned no runs" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_matrix_leg_maps_to_its_job() {
  local name="matrix_leg_maps_to_its_job"
  local repo; repo="$(new_repo CI_YML_SQL)"
  export STUB_PUSH_CURRENT_IDS="101"
  export STUB_MG_CURRENT_IDS="102"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row "sql (shard 1)" success 0 5)"
  export STUB_JOBS_102=""
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row "sql (shard 1)" success 0 5)"
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP: map by the raw API display name with no matrix-suffix strip and
  # "sql (shard 1)" would not match the ci.yml key "sql", printing
  # cap=unmapped instead of the job's real 30m cap.
  if [[ "${LAST_RC}" -eq 0 ]] && grep -q "^sql	.*cap=30	" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

CI_YML_NOCAPS='jobs:
  testjob:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
'

test_unparseable_timestamp_exits_2() {
  local name="unparseable_timestamp_exits_2"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS="101 102"
  export STUB_MG_CURRENT_IDS="103"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(printf 'testjob\tsuccess\tnot-a-time\t%s' "$(ts 90)")"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 10)"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 20)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 20)"
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP: drop a row whose duration does not parse (`|| continue`) and the
  # other rows still give a clean exit 0 with the 90-minute run never seen.
  if [[ "${LAST_RC}" -eq 2 ]] && grep -q "job 'testjob' in run 101: started_at='not-a-time'" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_no_current_rows_exits_2() {
  local name="no_current_rows_exits_2"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS="101"
  export STUB_MG_CURRENT_IDS="102"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101=""
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob skipped 0 0)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 20)"
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP: no empty-pool check and the stats loop runs over an empty file,
  # prints nothing and exits 0.
  if [[ "${LAST_RC}" -eq 2 ]] && grep -q "no job rows in the current window" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_no_older_rows_exits_2() {
  local name="no_older_rows_exits_2"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS="101"
  export STUB_MG_CURRENT_IDS="102"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102=""
  export STUB_JOBS_201=""
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP: no older-pool check and the job prints growth=n/a and exits 0,
  # the growth rule never asked.
  if [[ "${LAST_RC}" -eq 2 ]] && grep -q "growth rule cannot be asked" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_cancelled_at_cap_counts_toward_p90() {
  local name="cancelled_at_cap_counts_toward_p90"
  local repo; repo="$(new_repo CI_YML_TESTJOB)"
  export STUB_PUSH_CURRENT_IDS="101 102 103"
  export STUB_MG_CURRENT_IDS="104 105 106"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102; STUB_JOBS_102="$(job_row testjob success 0 20)"
  export STUB_JOBS_103; STUB_JOBS_103="$(job_row testjob success 0 30)"
  export STUB_JOBS_104; STUB_JOBS_104="$(job_row testjob cancelled 0 99)"
  export STUB_JOBS_105; STUB_JOBS_105="$(job_row testjob timed_out 0 100)"
  export STUB_JOBS_106; STUB_JOBS_106="$(job_row testjob cancelled 0 50)"
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 100)"
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_103 STUB_JOBS_104 STUB_JOBS_105 \
    STUB_JOBS_106 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP: keep only success and failure and p90 of {10,20,30} is 30, ratio
  # 0.3, clean. n=5 also rules out keeping every cancelled job: the one
  # cancelled at 50 minutes was superseded, not killed at its cap.
  if [[ "${LAST_RC}" -eq 1 ]] && grep -q "^testjob	.*p90=100.0000m	cap=100	ratio=1.0000	.*n=5$" <<<"${LAST_OUT}" \
    && grep -q "FINDING cap job=testjob" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

test_empty_caps_file_exits_70() {
  local name="empty_caps_file_exits_70"
  local repo; repo="$(new_repo CI_YML_NOCAPS)"
  export STUB_PUSH_CURRENT_IDS="101"
  export STUB_MG_CURRENT_IDS="102"
  export STUB_PUSH_OLDER_IDS="201"
  export STUB_MG_OLDER_IDS="202"
  export STUB_JOBS_101; STUB_JOBS_101="$(job_row testjob success 0 10)"
  export STUB_JOBS_102=""
  export STUB_JOBS_201; STUB_JOBS_201="$(job_row testjob success 0 10)"
  export STUB_JOBS_202=""
  run_live_case "${name}" "${repo}"
  unset STUB_PUSH_CURRENT_IDS STUB_MG_CURRENT_IDS STUB_PUSH_OLDER_IDS STUB_MG_OLDER_IDS \
    STUB_JOBS_101 STUB_JOBS_102 STUB_JOBS_201 STUB_JOBS_202
  rm -rf "${repo}"
  # FLIP: no empty-caps check and testjob reads as the 360-minute default,
  # ratio 0.0278, clean.
  if [[ "${LAST_RC}" -eq 70 ]] && grep -q "no timeout-minutes cap parsed" <<<"${LAST_OUT}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${LAST_RC} out=${LAST_OUT}"
  fi
}

# ---------------------------------------------------------- --raise-check

new_raise_repo() {
  # new_raise_repo <base_ci_yml_varname> <head_ci_yml_varname>
  # -> prints "<repo_dir> <base_sha>". Two commits on one repo: base, then
  # head with the given ci.yml content.
  local base_var="$1" head_var="$2" dir base_sha
  dir="$(mktemp -d)"
  mkdir -p "${dir}/.github/workflows"
  printf '%s' "${!base_var}" >"${dir}/.github/workflows/ci.yml"
  (
    cd "${dir}" || exit 1
    git init -q
    git config user.email "test@example.invalid"
    git config user.name "Test"
    git add -A
    git commit -q -m base
  )
  base_sha="$(cd "${dir}" && git rev-parse HEAD)"
  printf '%s' "${!head_var}" >"${dir}/.github/workflows/ci.yml"
  (
    cd "${dir}" || exit 1
    git add -A
    git commit -q -m head
  )
  printf '%s %s' "${dir}" "${base_sha}"
}

CI_YML_RAISE_BASE='jobs:
  doc-scripts:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - run: echo hi
'

CI_YML_RAISE_HEAD_NO_COMMENT='jobs:
  doc-scripts:
    runs-on: ubuntu-latest
    timeout-minutes: 40
    steps:
      - run: echo hi
'

CI_YML_RAISE_HEAD_WITH_COMMENT='jobs:
  doc-scripts:
    runs-on: ubuntu-latest
    # budget: p90 35 min on 2026-11-01
    timeout-minutes: 40
    steps:
      - run: echo hi
'

CI_YML_RAISE_HEAD_LOWERED='jobs:
  doc-scripts:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - run: echo hi
'

test_raise_without_budget_comment_is_a_finding() {
  local name="raise_without_budget_comment_is_a_finding"
  local repo_and_sha repo base_sha out rc
  repo_and_sha="$(new_raise_repo CI_YML_RAISE_BASE CI_YML_RAISE_HEAD_NO_COMMENT)"
  repo="${repo_and_sha% *}"
  base_sha="${repo_and_sha##* }"
  out="$(cd "${repo}" && "${GUARD}" --raise-check "${base_sha}" 2>&1)"
  rc=$?
  rm -rf "${repo}"
  # FLIP: drop the growth/raise rule entirely and a raise with no comment
  # would pass silently.
  if [[ "${rc}" -eq 1 ]] && grep -q "no '# budget" <<<"${out}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${rc} out=${out}"
  fi
}

test_raise_with_budget_comment_passes() {
  local name="raise_with_budget_comment_passes"
  local repo_and_sha repo base_sha out rc
  repo_and_sha="$(new_raise_repo CI_YML_RAISE_BASE CI_YML_RAISE_HEAD_WITH_COMMENT)"
  repo="${repo_and_sha% *}"
  base_sha="${repo_and_sha##* }"
  out="$(cd "${repo}" && "${GUARD}" --raise-check "${base_sha}" 2>&1)"
  rc=$?
  rm -rf "${repo}"
  # FLIP: require the comment to match literally (e.g. demand "p90:" with a
  # colon) and this well-formed comment would wrongly fail.
  if [[ "${rc}" -eq 0 ]]; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${rc} out=${out}"
  fi
}

test_lowered_cap_passes() {
  local name="lowered_cap_passes"
  local repo_and_sha repo base_sha out rc
  repo_and_sha="$(new_raise_repo CI_YML_RAISE_BASE CI_YML_RAISE_HEAD_LOWERED)"
  repo="${repo_and_sha% *}"
  base_sha="${repo_and_sha##* }"
  out="$(cd "${repo}" && "${GUARD}" --raise-check "${base_sha}" 2>&1)"
  rc=$?
  rm -rf "${repo}"
  # FLIP: compare caps with != instead of > and a lowered cap would also be
  # flagged as a finding requiring a budget comment it has no reason to carry.
  if [[ "${rc}" -eq 0 ]]; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${rc} out=${out}"
  fi
}

test_raise_check_empty_caps_exits_70() {
  local name="raise_check_empty_caps_exits_70"
  local repo_and_sha repo base_sha out rc
  repo_and_sha="$(new_raise_repo CI_YML_RAISE_BASE CI_YML_NOCAPS)"
  repo="${repo_and_sha% *}"
  base_sha="${repo_and_sha##* }"
  out="$(cd "${repo}" && "${GUARD}" --raise-check "${base_sha}" 2>&1)"
  rc=$?
  rm -rf "${repo}"
  # FLIP: no empty-caps check and the loop over an empty caps file prints
  # "raise-check clean" and exits 0.
  if [[ "${rc}" -eq 70 ]] && grep -q "no timeout-minutes cap parsed" <<<"${out}"; then
    ok "${name}"
  else
    not_ok "${name}" "rc=${rc} out=${out}"
  fi
}

test_p90_at_071_of_cap_is_a_finding
test_p90_at_069_of_cap_passes
test_growth_25_percent_is_a_finding
test_growth_24_percent_passes
test_short_job_growth_is_not_a_finding
test_five_minute_job_growth_is_a_finding
test_failed_query_exits_2
test_transient_502_is_retried
test_persistent_502_exits_2
test_empty_run_list_exits_2
test_matrix_leg_maps_to_its_job
test_unparseable_timestamp_exits_2
test_no_current_rows_exits_2
test_no_older_rows_exits_2
test_cancelled_at_cap_counts_toward_p90
test_empty_caps_file_exits_70
test_raise_without_budget_comment_is_a_finding
test_raise_with_budget_comment_passes
test_lowered_cap_passes
test_raise_check_empty_caps_exits_70

echo "check-ci-job-budgets.test.sh: ${pass} passed, ${fail} failed"
if [[ "${fail}" -ne 0 ]]; then
  exit 1
fi
exit 0
