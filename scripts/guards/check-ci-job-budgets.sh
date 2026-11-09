#!/usr/bin/env bash
# CI job runtime budget guard (ADR-4034 D3, issue #4037).
#
# Live mode (default): reads the last --runs completed `push` runs of ci.yml
# on main and the last --runs `merge_group` runs, computes each job's p50 and
# p90 runtime in minutes over jobs that completed (success or failure, plus
# cancelled or timed_out jobs that ran to within a minute of their cap: a job
# killed at its timeout-minutes is reported as cancelled), reads each job's
# `timeout-minutes` cap from .github/workflows/ci.yml, and prints
# one line per job: name, p50, p90, cap, ratio. Two rules can fail it:
#
#   cap     a job's p90 exceeds 0.7 of its cap -- 30 percent of run-to-run
#           spread is the margin a cap is supposed to leave.
#   growth  a job's p90 is 25 percent or more above its own p90 over the
#           --runs runs that completed more than 14 days earlier, whatever
#           the cap. A cap alone can be raised past the alarm; this watches
#           the trend instead.
#
# A job in the API response that cannot be mapped back to a ci.yml job key is
# printed as `cap=unmapped ratio=n/a`, never dropped (it still takes part in
# the growth rule, which needs no cap).
#
# --raise-check BASE_REF: git only, no network. Exits 1 for each
# `timeout-minutes` line in the current (working-tree) ci.yml whose value is
# higher than the same job's value at BASE_REF, unless the line directly
# above it is a comment `# budget: p90 <N> min on <YYYY-MM-DD>`. A raise
# needs a reason on the record; this is the mechanical form of that rule.
#
# Usage:
#   check-ci-job-budgets.sh [--repo OWNER/REPO] [--runs N]
#   check-ci-job-budgets.sh --raise-check BASE_REF
#
# Exit codes (live mode): 0 clean, 1 a cap or growth finding, 2 could not
# ask (a gh call failed or returned no runs, a job row's timestamps did not
# parse, or the current or older window held no job rows), 64 bad usage, 70
# ci.yml missing or no timeout-minutes cap parsed from it.
# Exit codes (--raise-check): 0 no un-budgeted raise, 1 a raise with no
# budget comment, 64 bad usage, 70 ci.yml unreadable or no cap parsed.
set -uo pipefail

SELF_DIR="$(cd "$(dirname "$0")" && pwd)"

DEFAULT_REPO="NOFireAI/ravel"
DEFAULT_RUNS=30
CAP_RATIO_THRESHOLD="0.7"
GROWTH_RATIO_THRESHOLD="1.25"
OLD_WINDOW_DAYS=14
GITHUB_DEFAULT_TIMEOUT_MINUTES=360

usage() {
  sed -n '2,37p' "$0" >&2
}

repo="${DEFAULT_REPO}"
runs="${DEFAULT_RUNS}"
mode="live"
raise_base=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      [[ $# -ge 2 ]] || { echo "check-ci-job-budgets.sh: --repo needs a value" >&2; exit 64; }
      repo="$2"; shift 2
      ;;
    --runs)
      [[ $# -ge 2 ]] || { echo "check-ci-job-budgets.sh: --runs needs a value" >&2; exit 64; }
      case "$2" in
        ''|*[!0-9]*)
          echo "check-ci-job-budgets.sh: --runs must be a positive integer, got '$2'" >&2
          exit 64
          ;;
      esac
      runs="$2"; shift 2
      ;;
    --raise-check)
      [[ $# -ge 2 ]] || { echo "check-ci-job-budgets.sh: --raise-check needs BASE_REF" >&2; exit 64; }
      mode="raise-check"; raise_base="$2"; shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "check-ci-job-budgets.sh: unknown argument: $1" >&2
      usage
      exit 64
      ;;
  esac
done

# Reuse the exact timeout-minutes parser ci-sweep-cancelled.sh already has,
# rather than writing a second one (CLAUDE.md). Only this one function's text
# is extracted and eval'd: sourcing the whole file would run its top-level gh
# calls against the real network.
CI_SWEEP="${SELF_DIR}/../ci-sweep-cancelled.sh"
if [[ ! -f "${CI_SWEEP}" ]]; then
  echo "check-ci-job-budgets.sh: cannot find ${CI_SWEEP} to reuse its timeout parser" >&2
  exit 70
fi
parser_src="$(sed -n '/^parse_job_timeouts()/,/^}/p' "${CI_SWEEP}")"
if [[ -z "${parser_src}" ]]; then
  echo "check-ci-job-budgets.sh: parse_job_timeouts() not found in ${CI_SWEEP}" >&2
  exit 70
fi
eval "${parser_src}"

# Job key and effective display name ("name:" field when the job sets one,
# else the key itself): a different field than parse_job_timeouts reads, not
# a second copy of that parser. Mirrors its indent rules (job keys at two
# spaces, fields at four) so the two stay in lockstep with ci.yml's shape.
parse_job_names() {
  local yaml="$1"
  awk '
    /^jobs:[[:space:]]*$/ { in_jobs=1; next }
    in_jobs==1 && /^[^[:space:]#]/ { in_jobs=0 }
    in_jobs!=1 { next }
    /^  [A-Za-z0-9_.-]+:[[:space:]]*$/ {
      if (job != "") print job "\t" (name != "" ? name : job)
      line=$0; sub(/^  /,"",line); sub(/:.*$/,"",line); job=line; name=""; next
    }
    /^    name:[[:space:]]*/ {
      v=$0; sub(/^[[:space:]]*name:[[:space:]]*/,"",v)
      gsub(/^["'"'"']|["'"'"']$/,"",v)
      sub(/[[:space:]]+#.*$/,"",v)
      gsub(/[[:space:]]+$/,"",v)
      if (job!="" && name=="") name=v
      next
    }
    END { if (job != "") print job "\t" (name != "" ? name : job) }
  ' "${yaml}"
}

# repo_root: the caller's repo (so a fixture tree in a test, or the primary
# checkout here, each resolve against their own ci.yml), falling back to this
# script's own install tree when the caller is not inside a git repo at all.
repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"
if [[ -z "${repo_root}" ]]; then
  repo_root="$(cd "${SELF_DIR}/../.." && pwd)"
fi
CI_YML="${repo_root}/.github/workflows/ci.yml"

portable_epoch() {
  # GNU date, then BSD/macOS date. Whichever understands the format wins;
  # bash 3.2 (macOS) ships the latter.
  local ts="$1"
  date -u -d "${ts}" +%s 2>/dev/null && return 0
  date -u -j -f "%Y-%m-%dT%H:%M:%SZ" "${ts}" +%s 2>/dev/null
}

portable_days_ago_iso() {
  local days="$1"
  date -u -d "-${days} days" +%Y-%m-%d 2>/dev/null && return 0
  date -u -v-"${days}"d +%Y-%m-%d 2>/dev/null
}

# Minutes between two ISO-8601 timestamps, printed with 4 decimal places, or
# nothing (and a non-zero return) when either is blank or the Go zero-value
# sentinel a null job timestamp serializes as (ci-sweep-cancelled.sh's own
# duration_seconds has the same guard).
duration_minutes() {
  local started="$1" completed="$2"
  [[ -z "${started}" || -z "${completed}" ]] && return 1
  [[ "${started}" == "0001-01-01T00:00:00Z" || "${completed}" == "0001-01-01T00:00:00Z" ]] && return 1
  local s e
  s=$(portable_epoch "${started}") || return 1
  e=$(portable_epoch "${completed}") || return 1
  [[ -z "${s}" || -z "${e}" ]] && return 1
  awk -v s="${s}" -v e="${e}" 'BEGIN { if (e < s) exit 1; printf "%.4f", (e - s) / 60.0 }'
}

# Nearest-rank p50/p90 per key, from "<key>\t<minutes>" lines on stdin-less
# input file. Printed as "<key>\t<p50>\t<p90>\t<count>". Plain (SUBSEP-keyed)
# awk arrays, portable to BSD/mawk/gawk alike -- no gawk-only asort, no bash
# associative arrays (this must run under bash 3.2, which has none).
compute_percentiles() {
  local infile="$1"
  awk -F'\t' '
    function ceil(x) { return (x == int(x)) ? x : int(x) + 1 }
    {
      key = $1; dur = $2 + 0
      n[key]++
      vals[key, n[key]] = dur
    }
    END {
      for (k in n) {
        cnt = n[k]
        for (i = 2; i <= cnt; i++) {
          v = vals[k, i]; j = i - 1
          while (j >= 1 && vals[k, j] > v) {
            vals[k, j + 1] = vals[k, j]
            j--
          }
          vals[k, j + 1] = v
        }
        p50i = ceil(0.5 * cnt); if (p50i < 1) p50i = 1; if (p50i > cnt) p50i = cnt
        p90i = ceil(0.9 * cnt); if (p90i < 1) p90i = 1; if (p90i > cnt) p90i = cnt
        printf "%s\t%.4f\t%.4f\t%d\n", k, vals[k, p50i], vals[k, p90i], cnt
      }
    }
  ' "${infile}"
}

if [[ "${mode}" == "raise-check" ]]; then
  if [[ ! -f "${CI_YML}" ]]; then
    echo "check-ci-job-budgets.sh: ${CI_YML} not found" >&2
    exit 70
  fi
  base_yaml="$(mktemp)"
  cur_caps="$(mktemp)"
  base_caps="$(mktemp)"
  trap 'rm -f "${base_yaml}" "${cur_caps}" "${base_caps}"' EXIT

  if ! git -C "${repo_root}" show "${raise_base}:.github/workflows/ci.yml" >"${base_yaml}" 2>/dev/null; then
    echo "check-ci-job-budgets.sh: cannot read .github/workflows/ci.yml at ${raise_base}" >&2
    exit 70
  fi

  parse_job_timeouts "${CI_YML}" | sort >"${cur_caps}"
  parse_job_timeouts "${base_yaml}" | sort >"${base_caps}"
  if [[ ! -s "${cur_caps}" || ! -s "${base_caps}" ]]; then
    echo "check-ci-job-budgets.sh: no timeout-minutes cap parsed from ci.yml (working tree or ${raise_base}); cannot compare caps" >&2
    exit 70
  fi

  # The line directly above a job's timeout-minutes line, checked against the
  # required comment. A dedicated small awk pass per job (cheap: one job per
  # call, the number of jobs in ci.yml), rather than folding this into
  # parse_job_timeouts, which only ever emits the cap.
  raise_has_comment() {
    local want="$1"
    awk -v want="${want}" '
      /^jobs:[[:space:]]*$/ { in_jobs=1; prev=$0; next }
      in_jobs==1 && /^[^[:space:]#]/ { in_jobs=0 }
      in_jobs!=1 { prev=$0; next }
      /^  [A-Za-z0-9_.-]+:[[:space:]]*$/ {
        line=$0; sub(/^  /,"",line); sub(/:.*$/,"",line); job=line; prev=$0; next
      }
      /^    timeout-minutes:[[:space:]]*[0-9]+/ {
        if (job == want) {
          if (prev ~ /^[[:space:]]*#[[:space:]]*budget:[[:space:]]*p90[[:space:]]+[0-9]+[[:space:]]+min[[:space:]]+on[[:space:]]+[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][[:space:]]*$/) {
            print "yes"
          } else {
            print "no"
          }
          exit
        }
      }
      { prev=$0 }
    ' "${CI_YML}" | grep -qx yes
  }

  findings=0
  while IFS=$'\t' read -r key cur_min; do
    [[ -z "${key}" ]] && continue
    base_min="$(awk -F'\t' -v k="${key}" '$1==k{print $2; exit}' "${base_caps}")"
    [[ -z "${base_min}" ]] && continue
    if ((cur_min > base_min)); then
      if raise_has_comment "${key}"; then
        echo "check-ci-job-budgets.sh: raise ok job=${key} base=${base_min}m head=${cur_min}m (budget comment present)"
      else
        echo "check-ci-job-budgets.sh: FINDING raise job=${key} base=${base_min}m head=${cur_min}m: no '# budget: p90 <N> min on <YYYY-MM-DD>' comment directly above its timeout-minutes line"
        findings=1
      fi
    fi
  done <"${cur_caps}"

  if ((findings == 1)); then
    exit 1
  fi
  echo "check-ci-job-budgets.sh: raise-check clean against ${raise_base}"
  exit 0
fi

# ---------------------------------------------------------------- live mode

if [[ ! -f "${CI_YML}" ]]; then
  echo "check-ci-job-budgets.sh: ${CI_YML} not found" >&2
  exit 70
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

caps_tsv="${tmpdir}/caps.tsv"
names_tsv="${tmpdir}/names.tsv"
parse_job_timeouts "${CI_YML}" >"${caps_tsv}"
parse_job_names "${CI_YML}" >"${names_tsv}"
if [[ ! -s "${caps_tsv}" ]]; then
  echo "check-ci-job-budgets.sh: no timeout-minutes cap parsed from ${CI_YML}; every job would read as the ${GITHUB_DEFAULT_TIMEOUT_MINUTES}-minute default" >&2
  exit 70
fi

cap_for_key() {
  local key="$1" v
  v="$(awk -F'\t' -v k="${key}" '$1==k{print $2; exit}' "${caps_tsv}")"
  if [[ -z "${v}" ]]; then
    printf '%s' "${GITHUB_DEFAULT_TIMEOUT_MINUTES}"
  else
    printf '%s' "${v}"
  fi
}

key_for_display_name() {
  local disp="$1"
  awk -F'\t' -v d="${disp}" '$2==d{print $1; exit}' "${names_tsv}"
}

key_known() {
  local key="$1"
  awk -F'\t' -v k="${key}" '$1==k{print "yes"; exit}' "${names_tsv}" | grep -qx yes
}

# Map an API job's display name to its ci.yml job key: by the job's own
# `name:` field when it set one, else by its key directly. A matrix leg's
# suffix ("sql (shard 1)") is stripped first, the same way
# ci-sweep-cancelled.sh strips it when looking up a cap.
map_job_key() {
  local api_name="$1" stripped key
  stripped="${api_name%% \(*}"
  key="$(key_for_display_name "${stripped}")"
  if [[ -z "${key}" ]]; then
    key="$(key_for_display_name "${api_name}")"
  fi
  if [[ -z "${key}" ]] && key_known "${stripped}"; then
    key="${stripped}"
  fi
  printf '%s' "${key}"
}

cutoff="$(portable_days_ago_iso "${OLD_WINDOW_DAYS}")"
if [[ -z "${cutoff}" ]]; then
  echo "check-ci-job-budgets.sh: could not compute a ${OLD_WINDOW_DAYS}-day-ago date" >&2
  exit 2
fi

runs_base="repos/${repo}/actions/workflows/ci.yml/runs"
push_current_path="${runs_base}?branch=main&event=push&status=completed&per_page=${runs}"
mg_current_path="${runs_base}?event=merge_group&status=completed&per_page=${runs}"
push_older_path="${runs_base}?branch=main&event=push&status=completed&per_page=${runs}&created=%3C${cutoff}"
mg_older_path="${runs_base}?event=merge_group&status=completed&per_page=${runs}&created=%3C${cutoff}"

query_failed=0

fetch_run_ids() {
  local path="$1" out rc
  out="$(gh api "${path}" --jq '.workflow_runs[].id' 2>&1)"
  rc=$?
  if ((rc != 0)); then
    echo "check-ci-job-budgets.sh: gh api ${path} failed: $(printf '%s' "${out}" | tail -n1)" >&2
    return 1
  fi
  if [[ -z "${out}" ]]; then
    echo "check-ci-job-budgets.sh: gh api ${path} returned no runs" >&2
    return 1
  fi
  printf '%s\n' "${out}"
  return 0
}

fetch_jobs_for_run() {
  local run_id="$1" out rc
  out="$(gh api "repos/${repo}/actions/runs/${run_id}/jobs?per_page=100" \
    --jq '.jobs[] | "\(.name)\t\(.conclusion)\t\(.started_at)\t\(.completed_at)"' 2>&1)"
  rc=$?
  if ((rc != 0)); then
    echo "check-ci-job-budgets.sh: gh api jobs for run ${run_id} failed: $(printf '%s' "${out}" | tail -n1)" >&2
    return 1
  fi
  printf '%s\n' "${out}"
  return 0
}

push_current_ids="${tmpdir}/push_current.ids"
mg_current_ids="${tmpdir}/mg_current.ids"
push_older_ids="${tmpdir}/push_older.ids"
mg_older_ids="${tmpdir}/mg_older.ids"

fetch_run_ids "${push_current_path}" >"${push_current_ids}" || query_failed=1
fetch_run_ids "${mg_current_path}" >"${mg_current_ids}" || query_failed=1
fetch_run_ids "${push_older_path}" >"${push_older_ids}" || query_failed=1
fetch_run_ids "${mg_older_path}" >"${mg_older_ids}" || query_failed=1

if ((query_failed == 1)); then
  echo "check-ci-job-budgets.sh: could not gather CI run data; exiting 2" >&2
  exit 2
fi

current_rows="${tmpdir}/current_rows.tsv"
older_rows="${tmpdir}/older_rows.tsv"
unmapped="${tmpdir}/unmapped.txt"
: >"${current_rows}"
: >"${older_rows}"
: >"${unmapped}"

build_pool() {
  local rows_file="$1" ids_file
  shift
  for ids_file in "$@"; do
    while IFS= read -r run_id; do
      [[ -z "${run_id}" ]] && continue
      jobs_text="$(fetch_jobs_for_run "${run_id}")" || { query_failed=1; continue; }
      [[ -z "${jobs_text}" ]] && continue
      while IFS=$'\t' read -r jname jconclusion jstarted jcompleted; do
        [[ -z "${jname}" ]] && continue
        case "${jconclusion}" in
          success | failure) killed=0 ;;
          cancelled | timed_out) killed=1 ;;
          *) continue ;;
        esac
        # A cancelled job that never started has no runtime to measure.
        if ((killed == 1)) && [[ -z "${jstarted}" || "${jstarted}" == "0001-01-01T00:00:00Z" ]]; then
          continue
        fi
        dur="$(duration_minutes "${jstarted}" "${jcompleted}")"
        if [[ -z "${dur}" ]]; then
          echo "check-ci-job-budgets.sh: cannot read the duration of job '${jname}' in run ${run_id}: started_at='${jstarted}' completed_at='${jcompleted}'" >&2
          query_failed=1
          continue
        fi
        key="$(map_job_key "${jname}")"
        mapped=1
        if [[ -z "${key}" ]]; then
          key="${jname%% \(*}"
          mapped=0
        fi
        if ((killed == 1)); then
          # Only a cancellation at the cap is a timeout; an earlier one is a
          # superseded run and says nothing about the job's runtime.
          at_cap="$(awk -v d="${dur}" -v c="$(cap_for_key "${key}")" 'BEGIN{print (d >= c - 1) ? 1 : 0}')"
          [[ "${at_cap}" == "1" ]] || continue
        fi
        if ((mapped == 0)); then
          printf '%s\n' "${key}" >>"${unmapped}"
        fi
        printf '%s\t%s\n' "${key}" "${dur}" >>"${rows_file}"
      done <<<"${jobs_text}"
    done <"${ids_file}"
  done
}

build_pool "${current_rows}" "${push_current_ids}" "${mg_current_ids}"
build_pool "${older_rows}" "${push_older_ids}" "${mg_older_ids}"

if ((query_failed == 1)); then
  echo "check-ci-job-budgets.sh: could not read every job's timings; exiting 2" >&2
  exit 2
fi

if [[ ! -s "${current_rows}" ]]; then
  echo "check-ci-job-budgets.sh: no job rows in the current window; exiting 2" >&2
  exit 2
fi
if [[ ! -s "${older_rows}" ]]; then
  echo "check-ci-job-budgets.sh: no job rows in the window older than ${OLD_WINDOW_DAYS} days, so the growth rule cannot be asked; exiting 2" >&2
  exit 2
fi

sort -u "${unmapped}" -o "${unmapped}"

current_stats="${tmpdir}/current_stats.tsv"
older_stats="${tmpdir}/older_stats.tsv"
compute_percentiles "${current_rows}" | sort -t $'\t' -k1,1 >"${current_stats}"
compute_percentiles "${older_rows}" | sort -t $'\t' -k1,1 >"${older_stats}"

is_unmapped() {
  grep -qxF "$1" "${unmapped}" 2>/dev/null
}

exit_code=0
while IFS=$'\t' read -r key p50 p90 count; do
  [[ -z "${key}" ]] && continue

  cap_display="unmapped"
  ratio_display="n/a"
  cap_finding=0
  if ! is_unmapped "${key}"; then
    cap="$(cap_for_key "${key}")"
    cap_display="${cap}"
    ratio_display="$(awk -v p="${p90}" -v c="${cap}" 'BEGIN{printf "%.4f", p/c}')"
    cap_finding="$(awk -v r="${ratio_display}" -v t="${CAP_RATIO_THRESHOLD}" 'BEGIN{print (r>t)?1:0}')"
  fi

  growth_display="n/a"
  growth_finding=0
  older_p90="$(awk -F'\t' -v k="${key}" '$1==k{print $3; exit}' "${older_stats}")"
  if [[ -n "${older_p90}" ]]; then
    growth_display="$(awk -v cp="${p90}" -v op="${older_p90}" 'BEGIN{ if (op+0==0) {print "n/a"} else {printf "%.4f", cp/op} }')"
    if [[ "${growth_display}" != "n/a" ]]; then
      growth_finding="$(awk -v r="${growth_display}" -v t="${GROWTH_RATIO_THRESHOLD}" 'BEGIN{print (r>=t)?1:0}')"
    fi
  fi

  printf '%s\tp50=%sm\tp90=%sm\tcap=%s\tratio=%s\tgrowth=%s\tn=%s\n' \
    "${key}" "${p50}" "${p90}" "${cap_display}" "${ratio_display}" "${growth_display}" "${count}"

  if [[ "${cap_finding}" == "1" ]]; then
    echo "check-ci-job-budgets.sh: FINDING cap job=${key} p90=${p90}m cap=${cap_display}m ratio=${ratio_display} (threshold ${CAP_RATIO_THRESHOLD})"
    exit_code=1
  fi
  if [[ "${growth_finding}" == "1" ]]; then
    echo "check-ci-job-budgets.sh: FINDING growth job=${key} p90=${p90}m prior_p90=${older_p90}m ratio=${growth_display} (threshold ${GROWTH_RATIO_THRESHOLD})"
    exit_code=1
  fi
done <"${current_stats}"

exit "${exit_code}"
