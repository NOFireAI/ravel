#!/usr/bin/env bash
# Resumable orchestration state for a fleet-delivered epic.
#
# The epic ISSUE BODY stays authoritative: scripts/epic-status.sh parses it
# and nothing else, so a dispatch recorded only here would be invisible to
# the reconciliation that exists to catch a silently dead task
# (fleet-dispatch-intent.sh's header describes that failure; this script
# would reintroduce it one layer up). So `record task-dispatched` writes the
# ledger line into the issue body, reads the body back, and refuses when the
# task id is not there. The JSON file is an index over that truth: it
# answers "where was I" in one read, and `reconcile` rewrites it from live
# GitHub state.
#
# State lives in the PRIMARY checkout, never the current worktree. An epic
# runs one worktree per wave and deliver-epic removes each wave's worktree
# at the end of that wave; state written under the worktree would be deleted
# before the next wave read it, and every single-wave test would still pass.
#
# Usage:
#   epic-orchestrator.sh path <epic>
#   epic-orchestrator.sh init <epic> [--title <title>]
#   epic-orchestrator.sh show <epic>
#   epic-orchestrator.sh record <epic> <kind> [key=value ...]
#   epic-orchestrator.sh reconcile <epic> [--fresh]
#   epic-orchestrator.sh resume-set <epic> --reason <text> [--class <class>]
#   epic-orchestrator.sh resume-get <epic>
#   epic-orchestrator.sh resume-clear <epic>
#   epic-orchestrator.sh classify <error text>
#   epic-orchestrator.sh backoff <attempt>
#
# Exit codes:
#   0   done, or (reconcile) no drift that blocks a dispatch
#   1   classify: the text is a fatal error, not a recoverable one
#   64  usage
#   65  reconcile found a mismatch that must be fixed before dispatching
#   66  no state file for this epic
#   69  a recoverable interruption is parked; resume-get says when to retry
#   70  the ledger line did not reach the epic issue body
#   75  retry budget exhausted, or the error is fatal; escalate to a human
set -euo pipefail

readonly SCHEMA_VERSION=1
# ScheduleWakeup's own floor here is 900 s (.claude/guards/pretooluse.mjs
# refuses anything shorter) and the runtime clamps at 3600 s, so the ladder
# is 900, 1800, 3600, 3600. A textbook ladder starting at 30 s would be
# refused by the guard on its first step.
readonly BACKOFF_FLOOR_SECONDS=900
readonly BACKOFF_CEILING_SECONDS=3600
readonly MAX_RESUME_ATTEMPTS=6

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
  echo "epic-orchestrator.sh: $1" >&2
  exit "${2:-64}"
}

# The primary checkout, from a linked worktree or from the primary checkout
# itself. --git-common-dir points at the shared .git in both cases; in the
# primary checkout it comes back relative, so resolve it against the top
# level rather than against the cwd of whoever called.
state_dir() {
  if [[ -n "${RAVEL_EPIC_STATE_DIR:-}" ]]; then
    printf '%s\n' "${RAVEL_EPIC_STATE_DIR}"
    return 0
  fi
  local common primary
  common="$(git rev-parse --git-common-dir 2>/dev/null || true)"
  if [[ -z "${common}" ]]; then
    die "not inside a git repository, and RAVEL_EPIC_STATE_DIR is unset" 64
  fi
  # Relative only in the primary checkout, where it is plain `.git`.
  if [[ "${common}" != /* ]]; then
    common="$(git rev-parse --show-toplevel)/${common}"
  fi
  primary="$(cd "$(dirname "${common}")" && pwd)"
  printf '%s\n' "${primary}/.claude/epic-state"
}

state_path() {
  printf '%s/%s.json\n' "$(state_dir)" "$1"
}

now_iso() {
  date -u +%Y-%m-%dT%H:%M:%SZ
}

# Read-modify-write under a mkdir lock: several sessions drive epics on this
# machine at once, and a lost update here is a lost task id.
with_lock() {
  local file="$1"
  shift
  local lock="${file}.lock"
  local waited=0
  until mkdir "${lock}" 2>/dev/null; do
    sleep 0.2
    waited=$((waited + 1))
    if ((waited > 150)); then
      die "could not take the lock on ${file} after 30s; remove ${lock} if it is stale" 69
    fi
  done
  local rc=0
  "$@" || rc=$?
  rmdir "${lock}" 2>/dev/null || true
  return "${rc}"
}

# jq filter applied to the state file, written atomically. The temp file is
# in the same directory so the rename cannot cross a filesystem boundary and
# degrade into a copy that a reader can catch half-written.
mutate() {
  local file="$1" filter="$2"
  shift 2
  local tmp="${file}.tmp.$$"
  jq "$@" --arg now "$(now_iso)" "${filter}" "${file}" >"${tmp}"
  mv "${tmp}" "${file}"
}

require_state() {
  local file="$1"
  [[ -f "${file}" ]] || die "no state for epic ${2}; run 'init ${2}' first" 66
}

# First value for <key> among the remaining key=value arguments, empty if
# absent. A plain scan rather than an associative array: macOS ships bash
# 3.2, which has none, and these scripts run there as often as on a runner.
kv_get() {
  local want="$1"
  shift
  local pair
  for pair in "$@"; do
    [[ -z "${pair}" ]] && continue
    if [[ "${pair%%=*}" == "${want}" ]]; then
      printf '%s' "${pair#*=}"
      return 0
    fi
  done
  return 0
}

cmd_init() {
  local epic="$1" title="${2:-}"
  local dir file
  dir="$(state_dir)"
  mkdir -p "${dir}"
  file="${dir}/${epic}.json"
  if [[ -f "${file}" ]]; then
    echo "epic ${epic}: state already exists at ${file}"
    return 0
  fi
  jq -n --arg epic "${epic}" --arg title "${title}" --arg now "$(now_iso)" \
    --argjson schema "${SCHEMA_VERSION}" '{
      schema: $schema,
      epic: $epic,
      title: $title,
      created: $now,
      updated: $now,
      resume: null,
      issues: {},
      tasks: {},
      prs: {},
      ci: {},
      events: []
    }' >"${file}.tmp.$$"
  mv "${file}.tmp.$$" "${file}"
  echo "epic ${epic}: initialised ${file}"
}

# Append `- #<ticket> task=<uuid> <status>` to the epic issue body and prove
# it arrived. gh exits 0 on an edit whose body was mangled by the shell, so
# the read-back is the check, not the exit code.
body_append_task() {
  local epic="$1" ticket="$2" task="$3" status="$4"
  local line="- #${ticket} task=${task} ${status}"
  local body tmp
  body="$(gh issue view "${epic}" --json body --jq .body)" ||
    die "could not read the body of epic #${epic}; the ledger line was not written" 70
  if grep -qF "${task}" <<<"${body}"; then
    echo "epic ${epic}: body already carries ${task}"
    return 0
  fi
  tmp="$(mktemp)"
  printf '%s\n%s\n' "${body}" "${line}" >"${tmp}"
  gh issue edit "${epic}" --body-file "${tmp}" >/dev/null ||
    die "gh issue edit failed for epic #${epic}; the ledger line was not written" 70
  rm -f "${tmp}"
  body="$(gh issue view "${epic}" --json body --jq .body)" ||
    die "could not read back the body of epic #${epic} to confirm the ledger line" 70
  grep -qF "${task}" <<<"${body}" ||
    die "epic #${epic}'s body does not carry ${task} after the edit; another session may have overwritten it" 70
  echo "epic ${epic}: ledger line written to the issue body"
}

record_event() {
  local file="$1" kind="$2" detail="$3"
  mutate "${file}" \
    '.updated = $now | .events += [{at: $now, kind: $kind, detail: $detail}]' \
    --arg kind "${kind}" --argjson detail "${detail}"
}

cmd_record() {
  local epic="$1" kind="$2"
  shift 2
  local file
  file="$(state_path "${epic}")"
  require_state "${file}" "${epic}"

  # key=value pairs, kept as a plain list: macOS ships bash 3.2, which has no
  # associative arrays, and these scripts run there as often as on a CI
  # runner.
  local pairs=("$@")
  local pair
  for pair in "${pairs[@]:-}"; do
    [[ -z "${pair}" ]] && continue
    [[ "${pair}" == *=* ]] || die "record: '${pair}' is not key=value" 64
  done

  local detail
  detail="$(
    for pair in "${pairs[@]:-}"; do
      [[ -z "${pair}" ]] && continue
      printf '%s\t%s\n' "${pair%%=*}" "${pair#*=}"
    done | jq -R -s 'split("\n") | map(select(length > 0) | split("\t") | {(.[0]): .[1]}) | add // {}'
  )"

  local v_issue v_ticket v_task v_pr v_sha v_status v_signature
  v_issue="$(kv_get issue "${pairs[@]:-}")"
  v_ticket="$(kv_get ticket "${pairs[@]:-}")"
  v_task="$(kv_get task "${pairs[@]:-}")"
  v_pr="$(kv_get pr "${pairs[@]:-}")"
  v_sha="$(kv_get sha "${pairs[@]:-}")"
  v_status="$(kv_get status "${pairs[@]:-}")"
  v_signature="$(kv_get signature "${pairs[@]:-}")"

  case "${kind}" in
    issue-discovered)
      [[ -n "${v_issue}" ]] || die "issue-discovered needs issue=<number>" 64
      with_lock "${file}" mutate "${file}" \
        '.issues[$issue] = ((.issues[$issue] // {}) + $detail + {discovered_at: (.issues[$issue].discovered_at // $now)}) | .updated = $now' \
        --arg issue "${v_issue}" --argjson detail "${detail}"
      ;;
    task-dispatched)
      [[ -n "${v_ticket}" && -n "${v_task}" ]] ||
        die "task-dispatched needs ticket=<number> task=<uuid>" 64
      with_lock "${file}" mutate "${file}" \
        '.tasks[$task] = ((.tasks[$task] // {}) + $detail + {status: ($detail.status // "dispatched"), dispatched_at: $now}) | .updated = $now' \
        --arg task "${v_task}" --argjson detail "${detail}"
      if [[ "${RAVEL_EPIC_NO_REMOTE:-0}" != "1" ]]; then
        body_append_task "${epic}" "${v_ticket}" "${v_task}" "${v_status:-dispatched}"
      fi
      ;;
    task-terminal)
      [[ -n "${v_task}" ]] || die "task-terminal needs task=<uuid>" 64
      with_lock "${file}" mutate "${file}" \
        '.tasks[$task] = ((.tasks[$task] // {}) + $detail + {terminal_at: $now}) | .updated = $now' \
        --arg task "${v_task}" --argjson detail "${detail}"
      ;;
    pr-opened)
      [[ -n "${v_pr}" ]] || die "pr-opened needs pr=<number>" 64
      with_lock "${file}" mutate "${file}" \
        '.prs[$pr] = ((.prs[$pr] // {}) + $detail + {state: ($detail.state // "OPEN"), opened_at: (.prs[$pr].opened_at // $now), review_rounds: (.prs[$pr].review_rounds // 0)}) | .updated = $now' \
        --arg pr "${v_pr}" --argjson detail "${detail}"
      ;;
    review-round)
      [[ -n "${v_pr}" ]] || die "review-round needs pr=<number>" 64
      with_lock "${file}" mutate "${file}" \
        '.prs[$pr] = ((.prs[$pr] // {}) + $detail + {review_rounds: ((.prs[$pr].review_rounds // 0) + 1), last_review_at: $now}) | .updated = $now' \
        --arg pr "${v_pr}" --argjson detail "${detail}"
      ;;
    gates-passed)
      with_lock "${file}" mutate "${file}" \
        '.gates = ((.gates // {}) + $detail + {at: $now}) | .updated = $now' \
        --argjson detail "${detail}"
      ;;
    merged)
      [[ -n "${v_pr}" ]] || die "merged needs pr=<number>" 64
      with_lock "${file}" mutate "${file}" \
        '.prs[$pr] = ((.prs[$pr] // {}) + $detail + {state: "MERGED", merged_at: $now}) | .updated = $now' \
        --arg pr "${v_pr}" --argjson detail "${detail}"
      ;;
    ci-attempt)
      [[ -n "${v_pr}" && -n "${v_sha}" ]] || die "ci-attempt needs pr=<number> sha=<sha>" 64
      with_lock "${file}" mutate "${file}" \
        '.ci[$pr] = ((.ci[$pr] // {}) | .[$sha] = (((.[$sha] // {attempts: 0, signatures: []})) | .attempts += 1 | .signatures += [$sig])) | .updated = $now' \
        --arg pr "${v_pr}" --arg sha "${v_sha}" --arg sig "${v_signature}"
      ;;
    *)
      die "record: unknown kind '${kind}'" 64
      ;;
  esac

  with_lock "${file}" record_event "${file}" "${kind}" "${detail}"
  echo "epic ${epic}: recorded ${kind}"
}

# Live GitHub state wins over the local index. Prints one DRIFT line per
# disagreement and exits 65 when a disagreement must be resolved before the
# next dispatch: a task the index calls in-flight that the fleet never
# pushed a ref for is exactly the silently-dead task CLAUDE.md's
# reconciliation section exists to catch.
cmd_reconcile() {
  local epic="$1"
  local file
  file="$(state_path "${epic}")"
  require_state "${file}" "${epic}"

  local body refs pr_rows blocking=0
  body="$(gh issue view "${epic}" --json body --jq .body)" ||
    die "could not read epic #${epic}; refusing to report reconciliation as clean" 65
  # Neither of these may degrade into an empty answer. An unreachable
  # remote would otherwise read as "no task refs exist", and every task in
  # the epic would be reported LOST and re-dispatched over work that is
  # running. UNKNOWN blocks; it never becomes a verdict.
  local rc=0
  refs="$(git ls-remote origin 'refs/heads/task/*' 2>/dev/null)" || rc=$?
  if ((rc != 0)); then
    die "UNKNOWN: git ls-remote origin failed (exit ${rc}); task refs could not be read. Not reporting any task's state." 65
  fi
  rc=0
  pr_rows="$(gh pr list --state all --limit 200 \
    --json number,state,headRefName,mergedAt,headRefOid \
    --jq '.[] | "\(.headRefName)\t\(.number)\t\(.state)\t\(.headRefOid)"' 2>/dev/null)" || rc=$?
  if ((rc != 0)); then
    die "UNKNOWN: gh pr list failed (exit ${rc}); pull-request state could not be read." 65
  fi

  local body_tasks
  body_tasks="$(grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' <<<"${body}" | sort -u || true)"

  # A task in the local index that never reached the body is invisible to
  # epic-status.sh, so it can never be reported dead there.
  local known
  known="$(jq -r '.tasks | keys[]' "${file}")"
  local task
  for task in ${known}; do
    if ! grep -qF "${task}" <<<"${body_tasks}"; then
      echo "DRIFT task ${task} is in local state but not in epic #${epic}'s body (epic-status.sh cannot see it)"
      blocking=1
    fi
  done

  for task in ${body_tasks}; do
    local has_start=no has_result=no pr_line pr_num pr_state
    grep -q "refs/heads/task/${task}/start$" <<<"${refs}" && has_start=yes
    grep -q "refs/heads/task/${task}/result$" <<<"${refs}" && has_result=yes
    pr_line="$(awk -F'\t' -v b="task/${task}/merge" '$1 == b {print; exit}' <<<"${pr_rows}")"
    pr_num=""
    pr_state=""
    if [[ -n "${pr_line}" ]]; then
      pr_num="$(cut -f2 <<<"${pr_line}")"
      pr_state="$(cut -f3 <<<"${pr_line}")"
    fi

    # One word per task, and the four that matter are kept apart. RUNNING
    # and LOST both look like "no result ref", and only fleet_status can
    # separate them, so neither is asserted here: the pair is UNRESOLVED
    # and it blocks. Reporting it as RUNNING is how a dead task's ticket
    # sits unfixed for a session.
    local task_state
    if [[ "${pr_state}" == "MERGED" ]]; then
      task_state="LANDED"
    elif [[ "${has_result}" == "yes" ]]; then
      task_state="COMPLETE"
    elif [[ "${has_start}" == "yes" ]]; then
      task_state="UNRESOLVED"
    else
      task_state="LOST"
    fi

    with_lock "${file}" mutate "${file}" \
      '.tasks[$task] = ((.tasks[$task] // {}) + {start_ref: $start, result_ref: $result, pr: $pr, pr_state: $prstate, state: $state, reconciled_at: $now}) | .updated = $now' \
      --arg task "${task}" --arg start "${has_start}" --arg result "${has_result}" \
      --arg pr "${pr_num}" --arg prstate "${pr_state}" --arg state "${task_state}"

    case "${task_state}" in
      UNRESOLVED)
        echo "UNRESOLVED task ${task}: start pushed, no result ref. Either RUNNING or LOST; fleet_status decides. No new dispatch until it does."
        blocking=1
        ;;
      LOST)
        echo "LOST  task ${task}: no start ref on origin. The dispatch never reached the executor; re-dispatch from a fresh origin/main."
        blocking=1
        ;;
      COMPLETE)
        echo "OK    task ${task}: result ref present, no PR yet (inspect and merge, never re-dispatch)"
        ;;
      LANDED)
        echo "OK    task ${task}: merged as #${pr_num}"
        ;;
    esac
  done

  # PR state for everything the index believes is open.
  local pr
  for pr in $(jq -r '.prs | keys[]' "${file}"); do
    local live
    live="$(gh pr view "${pr}" --json state,headRefOid --jq '"\(.state)\t\(.headRefOid)"' 2>/dev/null || true)"
    if [[ -z "${live}" ]]; then
      echo "DRIFT PR #${pr} is in local state but gh cannot read it (deleted, or the query failed)"
      blocking=1
      continue
    fi
    with_lock "${file}" mutate "${file}" \
      '.prs[$pr] = ((.prs[$pr] // {}) + {state: $state, head: $head, reconciled_at: $now}) | .updated = $now' \
      --arg pr "${pr}" --arg state "$(cut -f1 <<<"${live}")" --arg head "$(cut -f2 <<<"${live}")"
  done

  if ((blocking == 1)); then
    echo "reconcile: resolve the lines above before dispatching anything new." >&2
    return 65
  fi
  echo "reconcile: local state matches GitHub."
}

cmd_show() {
  local epic="$1"
  local file
  file="$(state_path "${epic}")"
  require_state "${file}" "${epic}"
  jq -r '
    "== Epic #\(.epic) \(.title // "") [state \(.schema), updated \(.updated)]",
    (if .resume then "RESUME parked: class=\(.resume.class) attempt=\(.resume.attempt) retry_after=\(.resume.retry_after) reason=\(.resume.reason)" else "resume: clear" end),
    "tasks:",
    (.tasks | to_entries[] | "  \(.key) ticket=\(.value.ticket // "-") status=\(.value.status // "-") result=\(.value.result_ref // "?") pr=\(.value.pr // "-")"),
    "prs:",
    (.prs | to_entries[] | "  #\(.key) \(.value.state // "-") rounds=\(.value.review_rounds // 0) head=\(.value.head // "-")"),
    "events: \(.events | length)"
  ' "${file}"
}

# Recoverable means "the same call can succeed later". Anything else is a
# defect in the work itself and retrying it just burns the budget.
cmd_classify() {
  local text="$*"
  local lowered
  lowered="$(tr '[:upper:]' '[:lower:]' <<<"${text}")"
  local class="fatal"
  case "${lowered}" in
    *429*|*"rate limit"*|*"rate-limit"*|*"too many requests"*|*overloaded*|*"quota exceeded"*)
      class="rate-limit" ;;
    *500*|*502*|*503*|*504*|*529*|*"internal server error"*|*"bad gateway"*|*"service unavailable"*|*"gateway timeout"*)
      class="server-error" ;;
    *"session limit"*|*"usage limit"*|*"context deadline"*|*"idle timeout"*|*"stream closed"*|*"connection reset"*|*"unexpected eof"*|*timeout*|*"timed out"*)
      class="transient" ;;
    *oauth_org_not_allowed*|*"unavailable"*)
      class="outage" ;;
  esac
  printf '%s\n' "${class}"
  [[ "${class}" == "fatal" ]] && return 1
  return 0
}

cmd_backoff() {
  local attempt="$1"
  [[ "${attempt}" =~ ^[0-9]+$ ]] || die "backoff needs a non-negative integer attempt" 64
  ((attempt < 1)) && attempt=1
  local delay=$((BACKOFF_FLOOR_SECONDS))
  local i
  for ((i = 1; i < attempt; i++)); do
    delay=$((delay * 2))
    ((delay >= BACKOFF_CEILING_SECONDS)) && break
  done
  ((delay > BACKOFF_CEILING_SECONDS)) && delay=${BACKOFF_CEILING_SECONDS}
  ((delay < BACKOFF_FLOOR_SECONDS)) && delay=${BACKOFF_FLOOR_SECONDS}
  printf '%s\n' "${delay}"
}

# Park an interruption. A fatal class is not parked: it is reported so the
# caller stops instead of sleeping through a defect.
cmd_resume_set() {
  local epic="$1"
  shift
  local reason="" class=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --reason) reason="${2:-}"; shift 2 ;;
      --class) class="${2:-}"; shift 2 ;;
      *) die "resume-set: unexpected argument '$1'" 64 ;;
    esac
  done
  [[ -n "${reason}" ]] || die "resume-set needs --reason <text>" 64
  local file
  file="$(state_path "${epic}")"
  require_state "${file}" "${epic}"

  if [[ -z "${class}" ]]; then
    class="$(cmd_classify "${reason}" || true)"
  fi
  if [[ "${class}" == "fatal" ]]; then
    echo "resume-set: '${reason}' classifies as fatal; not parking a retry." >&2
    with_lock "${file}" record_event "${file}" "fatal-error" \
      "$(jq -n --arg reason "${reason}" '{reason: $reason}')"
    return 75
  fi

  local attempt
  attempt="$(jq -r '.resume.attempt // 0' "${file}")"
  attempt=$((attempt + 1))
  if ((attempt > MAX_RESUME_ATTEMPTS)); then
    echo "resume-set: ${MAX_RESUME_ATTEMPTS} attempts exhausted for epic ${epic}; escalate." >&2
    with_lock "${file}" mutate "${file}" \
      '.resume = ((.resume // {}) + {class: $class, reason: $reason, attempt: $attempt, exhausted: true, at: $now}) | .updated = $now' \
      --arg class "${class}" --arg reason "${reason}" --argjson attempt "${attempt}"
    return 75
  fi

  local delay retry_after
  delay="$(cmd_backoff "${attempt}")"
  # Epoch seconds, not a formatted stamp: `date -d` is GNU-only and `date -v`
  # is BSD-only, and this runs on both.
  retry_after=$(($(date +%s) + delay))
  with_lock "${file}" mutate "${file}" \
    '.resume = {class: $class, reason: $reason, attempt: $attempt, delay_seconds: $delay, retry_after_epoch: $retry, at: $now, exhausted: false} | .updated = $now' \
    --arg class "${class}" --arg reason "${reason}" --argjson attempt "${attempt}" \
    --argjson delay "${delay}" --argjson retry "${retry_after}"
  with_lock "${file}" record_event "${file}" "resume-parked" \
    "$(jq -n --arg class "${class}" --argjson attempt "${attempt}" --argjson delay "${delay}" \
      '{class: $class, attempt: $attempt, delay_seconds: $delay}')"
  echo "delay_seconds=${delay} attempt=${attempt} class=${class} retry_after=${retry_after}"
  return 69
}

cmd_resume_get() {
  local epic="$1"
  local file
  file="$(state_path "${epic}")"
  require_state "${file}" "${epic}"
  if [[ "$(jq -r '.resume // "null"' "${file}")" == "null" ]]; then
    echo "resume: clear"
    return 0
  fi
  if [[ "$(jq -r '.resume.exhausted // false' "${file}")" == "true" ]]; then
    jq -r '"resume: EXHAUSTED after \(.resume.attempt) attempts, class=\(.resume.class), reason=\(.resume.reason)"' "${file}"
    return 75
  fi
  jq -r '"delay_seconds=\(.resume.delay_seconds) attempt=\(.resume.attempt) class=\(.resume.class) retry_after=\(.resume.retry_after)"' "${file}"
  return 69
}

cmd_resume_clear() {
  local epic="$1"
  local file
  file="$(state_path "${epic}")"
  require_state "${file}" "${epic}"
  with_lock "${file}" mutate "${file}" '.resume = null | .updated = $now'
  with_lock "${file}" record_event "${file}" "resume-cleared" '{}'
  echo "epic ${epic}: resume marker cleared"
}

main() {
  [[ $# -ge 1 ]] || die "usage: epic-orchestrator.sh <command> [args]; see the header" 64
  local cmd="$1"
  shift
  case "${cmd}" in
    path) [[ $# -ge 1 ]] || die "path needs <epic>" 64; state_path "$1" ;;
    init)
      [[ $# -ge 1 ]] || die "init needs <epic>" 64
      local epic="$1"; shift
      local title=""
      [[ "${1:-}" == "--title" ]] && title="${2:-}"
      cmd_init "${epic}" "${title}"
      ;;
    show) [[ $# -ge 1 ]] || die "show needs <epic>" 64; cmd_show "$1" ;;
    record) [[ $# -ge 2 ]] || die "record needs <epic> <kind>" 64; cmd_record "$@" ;;
    reconcile) [[ $# -ge 1 ]] || die "reconcile needs <epic>" 64; cmd_reconcile "$1" ;;
    resume-set) [[ $# -ge 1 ]] || die "resume-set needs <epic>" 64; cmd_resume_set "$@" ;;
    resume-get) [[ $# -ge 1 ]] || die "resume-get needs <epic>" 64; cmd_resume_get "$1" ;;
    resume-clear) [[ $# -ge 1 ]] || die "resume-clear needs <epic>" 64; cmd_resume_clear "$1" ;;
    classify) [[ $# -ge 1 ]] || die "classify needs an error text" 64; cmd_classify "$@" ;;
    backoff) [[ $# -ge 1 ]] || die "backoff needs <attempt>" 64; cmd_backoff "$1" ;;
    *) die "unknown command '${cmd}'" 64 ;;
  esac
}

main "$@"
