#!/usr/bin/env bash
# Green means green ON THE HEAD COMMIT, and a red check means red until a
# second run says otherwise.
#
# Two failures this closes:
#
#   1. Merging on a stale run. A check rollup is attached to a commit, so
#      the head moving mid-check is the way a "green" verdict ends up
#      describing code that is no longer on the branch. The head SHA is read
#      before and after every query here, and a move aborts the verdict
#      rather than downgrading it.
#   2. Escalating a flake, or merging through a real failure. A single red
#      run cannot tell the two apart. So the failure SIGNATURE (failing job,
#      failing step, and the test identifiers in the failed log) is recorded
#      against the head SHA in the orchestrator ledger; the same signature
#      twice is a real failure and escalates, two different signatures are a
#      flake report, and neither is ever silently merged.
#
# A cancelled or timed-out check is NOT rerun here. scripts/ci-sweep-cancelled.sh
# owns that discrimination (issue #1590: a job that hit its own
# `timeout-minutes` reports as "cancelled", and rerunning it on a faster
# runner hides the missing headroom).
#
# Usage:
#   assert-green-head.sh <pr> [--rerun] [--epic <id>] [--repo owner/name]
#
#   --rerun  actually issue the one rerun this script allows per head SHA.
#            Without it the rerun command is printed and nothing is started.
#
# Exit codes:
#   0   green on the current head; merging is allowed
#   1   the same failure signature twice on this head: a real failure
#   2   checks are still running; ask again later
#   3   red once; a rerun is the next step (issued with --rerun)
#   4   two runs failed differently: a flake, and the rerun budget is spent
#   5   no verdict is possible (no checks on this SHA, the head moved, or a
#       query failed). Never merge on this; it is not a green
#   6   cancelled or timed-out checks are present; ci-sweep-cancelled.sh
#       decides whether those are a supersede or a missing budget
#   64  usage
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
orchestrator="${script_dir}/../epic-orchestrator.sh"
classify_filter="${script_dir}/../lib/check-rollup-classify.jq"

pr=""
rerun=0
epic=""
repo=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rerun) rerun=1; shift ;;
    --epic) epic="${2:-}"; shift 2 ;;
    --repo) repo="${2:-}"; shift 2 ;;
    -*) echo "assert-green-head.sh: unknown flag '$1'" >&2; exit 64 ;;
    *)
      [[ -z "${pr}" ]] || { echo "assert-green-head.sh: only one PR number" >&2; exit 64; }
      pr="$1"; shift ;;
  esac
done

[[ -n "${pr}" ]] || { echo "usage: assert-green-head.sh <pr> [--rerun] [--epic <id>]" >&2; exit 64; }
[[ "${pr}" =~ ^[0-9]+$ ]] || { echo "assert-green-head.sh: <pr> must be a number" >&2; exit 64; }

if [[ -z "${repo}" ]]; then
  repo="$(gh repo view --json nameWithOwner --jq .nameWithOwner 2>/dev/null || true)"
  [[ -n "${repo}" ]] || { echo "assert-green-head.sh: could not resolve the repository" >&2; exit 5; }
fi

# A failed query is not an answer. Exit 5 (no verdict), never 0.
ask() {
  local out
  if ! out="$("$@" 2>/dev/null)"; then
    echo "assert-green-head.sh: '$*' failed; no verdict (this is not a green)" >&2
    exit 5
  fi
  printf '%s' "${out}"
}

pr_json="$(ask gh pr view "${pr}" --repo "${repo}" \
  --json state,mergeStateStatus,statusCheckRollup,headRefOid,headRefName)"
head_sha="$(jq -r '.headRefOid' <<<"${pr_json}")"
head_branch="$(jq -r '.headRefName' <<<"${pr_json}")"
pr_state="$(jq -r '.state' <<<"${pr_json}")"

if [[ "${pr_state}" != "OPEN" ]]; then
  echo "NO-VERDICT PR #${pr} is ${pr_state}, not OPEN"
  exit 5
fi

normalized="$(jq -f "${classify_filter}" <<<"${pr_json}")"
total=$(jq 'length' <<<"${normalized}")
if ((total == 0)); then
  echo "NO-VERDICT PR #${pr}: no checks at all on ${head_sha:0:12}."
  echo "           An empty rollup is not a pass. Wait for CI to report, or find out why it never started."
  exit 5
fi

pending=$(jq '[.[] | select(.class == "pending")] | length' <<<"${normalized}")
failing=$(jq '[.[] | select(.class == "failing")] | length' <<<"${normalized}")
other=$(jq '[.[] | select(.class == "other")] | length' <<<"${normalized}")
success=$(jq '[.[] | select(.class == "success")] | length' <<<"${normalized}")
skipped=$(jq '[.[] | select(.class == "skipped")] | length' <<<"${normalized}")

# The head moving under the query is what makes a verdict describe code that
# is no longer on the branch. Re-read it and refuse rather than answer about
# the wrong commit.
head_now="$(ask gh pr view "${pr}" --repo "${repo}" --json headRefOid --jq .headRefOid)"
if [[ "${head_now}" != "${head_sha}" ]]; then
  echo "NO-VERDICT PR #${pr}: head moved ${head_sha:0:12} -> ${head_now:0:12} while checking. Re-run this."
  exit 5
fi

if ((pending > 0 || other > 0)); then
  echo "PENDING PR #${pr} on ${head_sha:0:12}: ${pending} running, ${other} unsettled, ${success} pass, ${skipped} skipped, ${failing} failing"
  exit 2
fi

if ((failing == 0)); then
  echo "GREEN PR #${pr} on ${head_sha:0:12}: ${success} pass, ${skipped} skipped, 0 failing"
  exit 0
fi

cancelled=$(jq '[.[] | select(.class == "failing")
                     | select(.conclusion == "CANCELLED" or .conclusion == "TIMED_OUT")] | length' <<<"${normalized}")
if ((cancelled > 0)); then
  echo "CANCELLED PR #${pr} on ${head_sha:0:12}: ${cancelled} cancelled or timed-out check(s)."
  jq -r '.[] | select(.class == "failing") | "          \(.name) [\(.conclusion)]"' <<<"${normalized}"
  echo "          A timed-out job reports as cancelled. Run scripts/ci-sweep-cancelled.sh; it refuses"
  echo "          to rerun a job that hit its own timeout-minutes instead of hiding the missing budget."
  exit 6
fi

# --- failure signature -------------------------------------------------

# Runs for THIS head SHA only. A run on an older commit says nothing about
# whether this one is green.
runs="$(ask gh run list --repo "${repo}" --branch "${head_branch}" --limit 50 \
  --json databaseId,headSha,conclusion,workflowName \
  --jq ".[] | select(.headSha == \"${head_sha}\" and (.conclusion == \"failure\" or .conclusion == \"startup_failure\")) | \"\(.databaseId)\t\(.workflowName)\"")"

signature_lines=""
coarse=0
unread=0
unread_reason=""
run_ids=()
esc=$'\033'
if [[ -n "${runs}" ]]; then
  while IFS=$'\t' read -r run_id wf; do
    [[ -z "${run_id}" ]] && continue
    run_ids+=("${run_id}")
    jobs="$(ask gh run view "${run_id}" --repo "${repo}" --json jobs \
      --jq '.jobs[] | select(.conclusion == "failure") | "\(.name)\t" + ([.steps[]? | select(.conclusion == "failure") | .name] | join(","))')"
    while IFS=$'\t' read -r job steps; do
      [[ -z "${job}" ]] && continue
      signature_lines+="${wf}/${job}: ${steps}"$'\n'
    done <<<"${jobs}"
    # The test identifiers inside the failed log. Job and step names alone
    # match for any failure in the same job, which would call two unrelated
    # defects the same signature.
    # Keep stderr. An empty log has at least three causes -- the run has no
    # parseable failure text, the fetch failed, or the run is not finished
    # ("logs will be available when it is complete") -- and only the first is
    # a statement about the failure. Discarding stderr merges them into one
    # wrong claim, which is the same collapse this script refuses everywhere
    # else.
    log_err="$(mktemp)"
    log="$(gh run view "${run_id}" --repo "${repo}" --log-failed 2>"${log_err}" || true)"
    if [[ -z "${log}" ]]; then
      unread=1
      unread_reason="$(tr -d '\r' <"${log_err}" | head -2 | tr '\n' ' ')"
      unread_reason="${unread_reason:-no output and no error}"
    fi
    rm -f "${log_err}"
    if [[ -n "${log}" ]]; then
      # Colour codes first. `CARGO_TERM_COLOR: always` in ci.yml puts an SGR
      # sequence right before the token, and the sequence ENDS in a letter
      # (`\033[0;31m`), so the word-boundary this pattern opens with sees `m`
      # and matches nothing. The failure is silent: the signature quietly
      # degrades to job-level and two different failures then read as one.
      log="$(printf '%s' "${log}" | LC_ALL=C sed "s/${esc}\\[[0-9;]*[A-Za-z]//g")"
      # No word-boundary prefix on the alternation. It only ever cost matches:
      # after colour stripping the character before the token is whatever the
      # log line carried (a timestamp, a `[2/9]` counter, nothing), and a
      # spurious match here changes only the signature TEXT, never a verdict.
      tests="$(grep -aoE 'test [a-zA-Z0-9_:]+ \.\.\. FAILED|thread .[^.]*. panicked|error\[E[0-9]+\]|assertion .failed|FAILED \[[^]]*\] [a-zA-Z0-9_:]+' <<<"${log}" |
        sort -u | head -40 || true)"
      if [[ -n "${tests}" ]]; then
        signature_lines+="$(sed "s|^|${wf}/log: |" <<<"${tests}")"$'\n'
      else
        # Say so rather than quietly signing on the job name alone. A
        # job-level signature cannot separate two different failures in the
        # same step, so the second one reads as "the same failure twice" and
        # escalates. Escalating is the safe direction, but the reader has to
        # know that is what happened.
        coarse=1
      fi
    fi
  done <<<"${runs}"
fi

if [[ -z "${signature_lines}" ]]; then
  # Red checks with no failing Actions run behind them: a third-party status,
  # or a run this query could not see. Do not invent a signature for it.
  signature_lines="$(jq -r '.[] | select(.class == "failing") | "check: \(.name) [\(.conclusion)]"' <<<"${normalized}")"$'\n'
fi

signature="$(printf '%s' "${signature_lines}" | sort | shasum -a 256 | cut -c1-16)"

# --- ledger ------------------------------------------------------------

state_id="${epic:-ci-pr-${pr}}"
state_file="$("${orchestrator}" path "${state_id}")"
[[ -f "${state_file}" ]] || "${orchestrator}" init "${state_id}" >/dev/null

seen_before=$(jq -r --arg pr "${pr}" --arg sha "${head_sha}" --arg sig "${signature}" \
  '[(.ci[$pr][$sha].signatures // [])[] | select(. == $sig)] | length' "${state_file}")
attempts=$(jq -r --arg pr "${pr}" --arg sha "${head_sha}" \
  '.ci[$pr][$sha].attempts // 0' "${state_file}")

"${orchestrator}" record "${state_id}" ci-attempt "pr=${pr}" "sha=${head_sha}" "signature=${signature}" >/dev/null

echo "RED   PR #${pr} on ${head_sha:0:12}: ${failing} failing check(s), signature ${signature}, attempt $((attempts + 1))"
printf '%s' "${signature_lines}" | sed 's/^/      /'
if ((unread == 1)); then
  echo "      UNREAD: the failed log could not be read (${unread_reason})."
  echo "      This signature is job-level only, and the reason is the reader, not the failure. Read it before"
  echo "      acting on a repeat: a run still in progress reports exactly like a run with nothing to report."
elif ((coarse == 1)); then
  echo "      COARSE: the failed log carried no test identifier, so this signature is job-level only."
  echo "      Two different failures in that step cannot be told apart and will read as the same one."
fi

if ((seen_before > 0)); then
  echo "FAIL  the same failure signature has now been seen $((seen_before + 1)) times on this commit."
  echo "      This is a real failure, not a flake. Escalate it; do not rerun again."
  exit 1
fi

if ((attempts >= 1)); then
  echo "FLAKE two runs on this commit failed DIFFERENTLY. The rerun budget for ${head_sha:0:12} is spent."
  jq -r --arg pr "${pr}" --arg sha "${head_sha}" \
    '"      signatures so far: " + ((.ci[$pr][$sha].signatures // []) | join(", "))' "${state_file}"
  echo "      Read both failures before merging anything; a flake here is still not a green."
  exit 4
fi

if ((rerun == 1)); then
  if ((${#run_ids[@]} == 0)); then
    echo "      No Actions run to rerun (the red check is not an Actions run). Handle it by hand."
    exit 3
  fi
  for run_id in "${run_ids[@]}"; do
    echo "      rerunning failed jobs of run ${run_id}"
    gh run rerun "${run_id}" --repo "${repo}" --failed ||
      echo "      rerun of ${run_id} failed to start; try again by hand" >&2
  done
  echo "RERUN issued once for ${head_sha:0:12}. Check again when it settles; a matching signature escalates."
else
  echo "      Rerun once, then check again: ${0##*/} ${pr} --rerun"
fi
exit 3
