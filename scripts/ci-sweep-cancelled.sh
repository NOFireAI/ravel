#!/usr/bin/env bash
# Find cancelled ci runs on the head SHA of open PRs and rerun them.
# Dry run by default; -y applies.
#
# Cancelled is not failed, so nothing retries it, but a cancelled
# required check blocks auto-merge the same as a red one. Most come from
# the auto-merge/late-push concurrency race.
#
# BUT a job that hits its own `timeout-minutes` is also reported with
# conclusion "cancelled", indistinguishable at the run level from the
# superseded-by-a-new-push case this script exists for. Blind-rerunning a
# timeout is worse than useless: on a faster runner the rerun can pass and
# the real cause (a job with no headroom) is never diagnosed. That is issue
# #1590: swept, rerun passed in 18m29s, timed out again on the next push.
#
# So before rerunning a cancelled run, this compares each of its jobs'
# durations against that job's `timeout-minutes`. A job that ran within
# about a minute of its cap is treated as a timeout: the run is REFUSED
# (never rerun, even with -y), the offending job and both numbers are
# printed, and the script exits non-zero so the refusal is visible rather
# than silent.
#
# Where the cap is read from: the workflow file at the run's OWN commit
# (gh api .../contents/<path>?ref=<head_sha>), not the checked-out working
# tree. A PR head can carry a different ci.yml than main, and the cap that
# governed the run is the one on the run's commit. A job that sets no
# `timeout-minutes` inherits GitHub's default of 360 minutes; that default
# is applied explicitly rather than skipping the job.
#
# Usage: ci-sweep-cancelled.sh [-y]
set -euo pipefail

# Exit code when at least one cancelled run was refused as a timeout. Any
# non-zero would do; a distinct value lets a caller tell "refused a timeout"
# from a generic failure.
readonly EXIT_TIMEOUT_REFUSED=3

# GitHub's default job timeout when a job sets no `timeout-minutes`.
readonly GITHUB_DEFAULT_TIMEOUT_MINUTES=360

# How close to the cap counts as a timeout rather than a supersede.
readonly TIMEOUT_MARGIN_SECONDS=60

apply=0
if [[ "${1:-}" == "-y" ]]; then
  apply=1
fi

repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)

# Parse job-level `timeout-minutes` out of a workflow YAML file into
# "<job-key>\t<minutes>" lines. Only the job-level setting (exactly four
# spaces of indent, directly under a two-space job key) is read; a
# step-level `timeout-minutes` sits deeper and is ignored. A job with no
# setting simply produces no line, and the caller applies the default.
parse_job_timeouts() {
  local yaml="$1"
  awk '
    /^jobs:[[:space:]]*$/ { in_jobs=1; next }
    in_jobs==1 && /^[^[:space:]#]/ { in_jobs=0 }
    in_jobs!=1 { next }
    /^  [A-Za-z0-9_.-]+:[[:space:]]*$/ {
      line=$0; sub(/^  /,"",line); sub(/:.*$/,"",line); job=line; next
    }
    /^    timeout-minutes:[[:space:]]*[0-9]+/ {
      v=$0; sub(/^[[:space:]]*timeout-minutes:[[:space:]]*/,"",v);
      sub(/[^0-9].*$/,"",v);
      if (job!="") print job "\t" v
    }
  ' "${yaml}"
}

# Seconds between two ISO-8601 timestamps, or empty if either is blank.
duration_seconds() {
  local started="$1" completed="$2"
  [[ -z "${started}" || -z "${completed}" ]] && return 0
  local s e
  s=$(date -d "${started}" +%s)
  e=$(date -d "${completed}" +%s)
  echo $((e - s))
}

open_prs=$(gh pr list --state open --json number,headRefName,headRefOid \
  --jq '.[] | "\(.number) \(.headRefName) \(.headRefOid)"')

if [[ -z "${open_prs}" ]]; then
  echo "No open PRs; nothing to sweep."
  exit 0
fi

found=0
refused=0
while IFS= read -r row; do
  pr_num="${row%% *}"
  rest="${row#* }"
  head_branch="${rest%% *}"
  head_sha="${rest##* }"

  runs=$(gh run list --branch "${head_branch}" --limit 20 \
    --json databaseId,conclusion,headSha,workflowName \
    --jq ".[] | select(.conclusion == \"cancelled\" and .headSha == \"${head_sha}\") | \"\(.databaseId) \(.workflowName)\"" \
    2>/dev/null || true)
  [[ -z "${runs}" ]] && continue

  while IFS= read -r run_row; do
    run_id="${run_row%% *}"
    wf_name="${run_row#* }"
    found=1

    # Read the cap from the workflow at the run's own commit. The run meta
    # gives the workflow path and the head SHA it ran against.
    meta=$(gh api "repos/${repo}/actions/runs/${run_id}" \
      --jq '"\(.path)\t\(.head_sha)"' 2>/dev/null || true)
    wf_path="${meta%%$'\t'*}"
    wf_sha="${meta##*$'\t'}"

    caps_file=""
    if [[ -n "${wf_path}" && -n "${wf_sha}" ]]; then
      wf_yaml=$(gh api "repos/${repo}/contents/${wf_path}?ref=${wf_sha}" \
        --jq '.content' 2>/dev/null | base64 -d 2>/dev/null || true)
      if [[ -n "${wf_yaml}" ]]; then
        caps_file=$(mktemp)
        printf '%s\n' "${wf_yaml}" >"${caps_file}"
      fi
    fi

    declare -A cap_map=()
    if [[ -n "${caps_file}" ]]; then
      while IFS=$'\t' read -r jk cap; do
        [[ -z "${jk}" ]] && continue
        cap_map["${jk}"]="${cap}"
      done < <(parse_job_timeouts "${caps_file}")
      rm -f "${caps_file}"
    fi

    # Inspect each job's duration against its cap. A job within the margin
    # of its cap marks the whole run as a timeout.
    timed_out_report=""
    jobs=$(gh run view "${run_id}" --json jobs \
      --jq '.jobs[] | "\(.name)\t\(.startedAt)\t\(.completedAt)"' \
      2>/dev/null || true)
    if [[ -n "${jobs}" ]]; then
      while IFS=$'\t' read -r job_name started completed; do
        [[ -z "${job_name}" ]] && continue
        dur=$(duration_seconds "${started}" "${completed}")
        [[ -z "${dur}" ]] && continue

        cap_min="${cap_map[${job_name}]:-}"
        if [[ -z "${cap_min}" ]]; then
          stripped="${job_name%% (*}"
          cap_min="${cap_map[${stripped}]:-}"
        fi
        default_note=""
        if [[ -z "${cap_min}" ]]; then
          cap_min="${GITHUB_DEFAULT_TIMEOUT_MINUTES}"
          default_note=" (GitHub default; job sets no timeout-minutes)"
        fi

        cap_sec=$((cap_min * 60))
        if ((dur >= cap_sec - TIMEOUT_MARGIN_SECONDS)); then
          dur_min=$((dur / 60))
          dur_rem=$((dur % 60))
          timed_out_report+=$'\n'"    job '${job_name}' ran ${dur_min}m${dur_rem}s against a ${cap_min}m cap${default_note}"
        fi
      done <<<"${jobs}"
    fi
    unset cap_map

    if [[ -n "${timed_out_report}" ]]; then
      refused=1
      echo "PR #${pr_num}: REFUSING to rerun '${wf_name}' run ${run_id} on ${head_sha:0:12}: looks like a TIMEOUT, not a supersede.${timed_out_report}"
      echo "    A rerun would hide the missing headroom; diagnose the job's budget instead (issue #1590)."
      continue
    fi

    if [[ ${apply} -eq 1 ]]; then
      echo "PR #${pr_num}: rerunning cancelled '${wf_name}' run ${run_id}"
      # Cancelled jobs do not count as failed, so rerun the whole run.
      gh run rerun "${run_id}"
    else
      echo "PR #${pr_num}: cancelled '${wf_name}' run ${run_id} on ${head_sha:0:12} (dry run; -y to rerun)"
    fi
  done <<<"${runs}"
done <<<"${open_prs}"

if [[ ${found} -eq 0 ]]; then
  echo "No cancelled runs on open PR heads."
fi

if [[ ${refused} -eq 1 ]]; then
  exit "${EXIT_TIMEOUT_REFUSED}"
fi
