#!/usr/bin/env bash
# Cases for scripts/ci-sweep-cancelled.sh's timeout-vs-supersede
# discrimination (issue #1590). GitHub reports a TIMED-OUT job with
# conclusion "cancelled", identical to an ordinary superseded run, so the
# sweep used to blind-rerun timeouts; on a faster runner the rerun passed
# and the missing headroom was never diagnosed.
#
# All `gh` calls are stubbed; nothing hits the network. Each case builds an
# isolated stub directory with canned PR/run/job data and a fixture
# workflow YAML, points the script's `gh` at a dispatcher stub on PATH, and
# asserts the exact behaviour: which run ids were rerun (recorded by the
# stub) and the script's exit code.
#
# Run by hand:   bash scripts/tests/ci-sweep-cancelled.test.sh
# Wired into CI: the doc-scripts job in .github/workflows/ci.yml.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# Overridable so the same cases can be run against a pre-fix copy of the
# script to demonstrate they fail there.
SWEEP_SCRIPT="${SWEEP_SCRIPT:-${SCRIPT_DIR}/ci-sweep-cancelled.sh}"

pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

# A fixture ci.yml: features caps at 30m, quick at 10m, nocap sets none
# (so it must inherit GitHub's 360m default). A step-level timeout-minutes
# is included to prove the parser reads only the job-level one.
fixture_workflow_b64() {
  base64 -w0 <<'YAML'
name: ci
on:
  push:
    branches: [main]
jobs:
  features:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    steps:
      - name: a
        timeout-minutes: 5
        run: echo hi
  quick:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo hi
  nocap:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YAML
}

# Build a stub gh in ${1} that reads canned data from that same directory.
write_gh_stub() {
  local dir="$1"
  cat >"${dir}/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
sub="${1:-}"; shift || true
case "${sub}" in
  repo) echo "myorg/myrepo" ;;
  pr)   cat "${STUB_DIR}/prs.txt" ;;
  run)
    action="${1:-}"; shift || true
    case "${action}" in
      list) cat "${STUB_DIR}/runs.txt" ;;
      view) id="${1:-}"; cat "${STUB_DIR}/run-${id}.jobs" ;;
      rerun) id="${1:-}"; echo "${id}" >>"${STUB_DIR}/reruns.log" ;;
      *) echo "stub gh: unknown run action ${action}" >&2; exit 2 ;;
    esac ;;
  api)
    url="${1:-}"
    case "${url}" in
      */actions/runs/*)
        id="${url##*/actions/runs/}"; id="${id%%\?*}"; id="${id%%/*}"
        cat "${STUB_DIR}/run-${id}.meta" ;;
      */contents/*) cat "${STUB_DIR}/workflow.b64" ;;
      *) echo "stub gh: unknown api url ${url}" >&2; exit 2 ;;
    esac ;;
  *) echo "stub gh: unknown subcommand ${sub}" >&2; exit 2 ;;
esac
STUB
  chmod +x "${dir}/gh"
}

# Run the sweep against a freshly built stub dir. Echoes nothing; sets the
# globals RC (exit code) and RERUNS (space-joined rerun ids, empty if none).
# Args: <apply-flag: -y|""> then the scenario is prepared by the caller into
# the dir named by $SCN_DIR before calling.
run_sweep() {
  local apply_flag="$1"
  local rc=0
  PATH="${SCN_DIR}:${PATH}" STUB_DIR="${SCN_DIR}" \
    bash "${SWEEP_SCRIPT}" ${apply_flag:+"${apply_flag}"} \
    >"${SCN_DIR}/out.txt" 2>&1 || rc=$?
  RC="${rc}"
  if [[ -f "${SCN_DIR}/reruns.log" ]]; then
    RERUNS="$(tr '\n' ' ' <"${SCN_DIR}/reruns.log" | sed 's/ *$//')"
  else
    RERUNS=""
  fi
}

new_scenario() {
  SCN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ci-sweep-test.XXXXXX")"
  write_gh_stub "${SCN_DIR}"
  fixture_workflow_b64 >"${SCN_DIR}/workflow.b64"
  # One open PR whose head SHA the runs are attributed to.
  printf '10 feature-a aaaaaaaaaaaa\n' >"${SCN_DIR}/prs.txt"
}

# A single job row: name, start, end (ISO-8601), optional conclusion
# (defaults to blank, matching a bare cancelled-run job).
job_row() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "${4:-}"; }

cleanup_dirs=()
cleanup() { for d in "${cleanup_dirs[@]:-}"; do [[ -n "${d}" ]] && rm -rf "${d}"; done; }
trap cleanup EXIT

# === Case A: a job within ~1 minute of its cap is a TIMEOUT -> REFUSED.
#     features cap is 30m; this run's features job ran 29m30s.
new_scenario; cleanup_dirs+=("${SCN_DIR}")
printf '100 ci\n' >"${SCN_DIR}/runs.txt"
printf '.github/workflows/ci.yml\taaaaaaaaaaaa\n' >"${SCN_DIR}/run-100.meta"
job_row features 2026-09-10T06:00:00Z 2026-09-10T06:29:30Z >"${SCN_DIR}/run-100.jobs"
run_sweep -y
check_eq "A: near-cap run is not rerun" "" "${RERUNS}"
check_eq "A: near-cap run refusal exits 3" "3" "${RC}"
grep -q "REFUSING" "${SCN_DIR}/out.txt" && a_ref=1 || a_ref=0
check_eq "A: output says REFUSING" "1" "${a_ref}"
grep -q "features" "${SCN_DIR}/out.txt" && a_job=1 || a_job=0
check_eq "A: output names the features job" "1" "${a_job}"
grep -q "30m cap" "${SCN_DIR}/out.txt" && a_cap=1 || a_cap=0
check_eq "A: output quotes the 30m cap" "1" "${a_cap}"

# === Case B: a job far below its cap is an ordinary supersede -> RERUN.
#     features cap is 30m; this run's features job ran 12m.
new_scenario; cleanup_dirs+=("${SCN_DIR}")
printf '200 ci\n' >"${SCN_DIR}/runs.txt"
printf '.github/workflows/ci.yml\taaaaaaaaaaaa\n' >"${SCN_DIR}/run-200.meta"
job_row features 2026-09-10T06:00:00Z 2026-09-10T06:12:00Z >"${SCN_DIR}/run-200.jobs"
run_sweep -y
check_eq "B: far-below run is rerun (exactly once)" "200" "${RERUNS}"
check_eq "B: far-below run exits 0" "0" "${RC}"

# === Case C1: a job with no timeout-minutes inherits GitHub's 360m default;
#     20m is far below -> RERUN.
new_scenario; cleanup_dirs+=("${SCN_DIR}")
printf '300 ci\n' >"${SCN_DIR}/runs.txt"
printf '.github/workflows/ci.yml\taaaaaaaaaaaa\n' >"${SCN_DIR}/run-300.meta"
job_row nocap 2026-09-10T06:00:00Z 2026-09-10T06:20:00Z >"${SCN_DIR}/run-300.jobs"
run_sweep -y
check_eq "C1: no-cap job far below the 360m default is rerun" "300" "${RERUNS}"
check_eq "C1: no-cap far-below exits 0" "0" "${RC}"

# === Case C2: the 360m default is genuinely applied, not skipped: a no-cap
#     job running 359m50s is within the margin -> REFUSED.
new_scenario; cleanup_dirs+=("${SCN_DIR}")
printf '350 ci\n' >"${SCN_DIR}/runs.txt"
printf '.github/workflows/ci.yml\taaaaaaaaaaaa\n' >"${SCN_DIR}/run-350.meta"
job_row nocap 2026-09-10T06:00:00Z 2026-09-10T11:59:50Z >"${SCN_DIR}/run-350.jobs"
run_sweep -y
check_eq "C2: no-cap job near the 360m default is not rerun" "" "${RERUNS}"
check_eq "C2: no-cap near-default refusal exits 3" "3" "${RC}"
grep -q "360m" "${SCN_DIR}/out.txt" && c_def=1 || c_def=0
check_eq "C2: output quotes the 360m default cap" "1" "${c_def}"

# === Case D: dry run (no -y) reruns nothing, even for a rerunnable run.
new_scenario; cleanup_dirs+=("${SCN_DIR}")
printf '400 ci\n' >"${SCN_DIR}/runs.txt"
printf '.github/workflows/ci.yml\taaaaaaaaaaaa\n' >"${SCN_DIR}/run-400.meta"
job_row features 2026-09-10T06:00:00Z 2026-09-10T06:12:00Z >"${SCN_DIR}/run-400.jobs"
run_sweep ""
check_eq "D: dry run reruns nothing" "" "${RERUNS}"
check_eq "D: dry run exits 0" "0" "${RC}"
grep -q "dry run" "${SCN_DIR}/out.txt" && d_dry=1 || d_dry=0
check_eq "D: dry run says so" "1" "${d_dry}"

# === Case E: a job's startedAt is the Go zero-value sentinel (job cancelled
#     before it ever started -- gh's JSON null decodes to this string, not
#     blank). A second, normal job in the same run is far below its cap.
#     The sentinel job must not manufacture a bogus multi-thousand-year
#     duration that gets refused as a timeout -> RERUN.
new_scenario; cleanup_dirs+=("${SCN_DIR}")
printf '500 ci\n' >"${SCN_DIR}/runs.txt"
printf '.github/workflows/ci.yml\taaaaaaaaaaaa\n' >"${SCN_DIR}/run-500.meta"
{
  job_row quick 0001-01-01T00:00:00Z 2026-09-10T06:05:00Z cancelled
  job_row features 2026-09-10T06:00:00Z 2026-09-10T06:12:00Z cancelled
} >"${SCN_DIR}/run-500.jobs"
run_sweep -y
check_eq "E: zero-value startedAt job is not a false timeout (rerun)" "500" "${RERUNS}"
check_eq "E: zero-value startedAt run exits 0" "0" "${RC}"

# === Case F: a job that finished successfully happens to land inside the
#     near-cap margin. A timed-out job never has conclusion "success", so
#     this must not be read as a timeout. A second, cancelled job in the
#     same run is nowhere near its cap -> RERUN.
new_scenario; cleanup_dirs+=("${SCN_DIR}")
printf '600 ci\n' >"${SCN_DIR}/runs.txt"
printf '.github/workflows/ci.yml\taaaaaaaaaaaa\n' >"${SCN_DIR}/run-600.meta"
{
  job_row features 2026-09-10T06:00:00Z 2026-09-10T06:29:50Z success
  job_row quick 2026-09-10T06:00:00Z 2026-09-10T06:02:00Z cancelled
} >"${SCN_DIR}/run-600.jobs"
run_sweep -y
check_eq "F: successful near-cap job is not a false timeout (rerun)" "600" "${RERUNS}"
check_eq "F: successful near-cap run exits 0" "0" "${RC}"

echo
echo "passed: ${pass}  failed: ${fail}"
[[ ${fail} -eq 0 ]]
