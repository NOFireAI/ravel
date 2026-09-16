#!/usr/bin/env bash
# Cases for scripts/epic-orchestrator.sh: the resume path and the
# reconciliation path, which are the two that decide whether interrupted
# work is picked back up or dispatched a second time.
#
# All `gh` and `git ls-remote` calls are stubbed; nothing hits the network.
# Each case builds an isolated state directory and asserts the exact
# behaviour AND the exit code, because several of these differ only in the
# code (69 park, 75 escalate, 65 reconcile-first).
#
# Every case here fails against a specific mutation of the script; the
# mutation is named beside the case. Run the suite against a mutated copy
# with ORCHESTRATOR=<path> to see it fail.
#
# Run by hand:   bash scripts/tests/epic-orchestrator.test.sh
# Wired into CI: the doc-scripts job in .github/workflows/ci.yml.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ORCHESTRATOR="${ORCHESTRATOR:-${SCRIPT_DIR}/epic-orchestrator.sh}"

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

check_contains() {
  local label="$1" needle="$2" haystack="$3"
  if [[ "${haystack}" == *"${needle}"* ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want substring: %s\n  got: %s\n' "${label}" "${needle}" "${haystack}"
  fi
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# --- stubs -------------------------------------------------------------

# A gh stub reading canned answers from ${STUB_DIR}. Each file is optional;
# a missing one means "this call fails", which is how the could-not-ask
# cases are built.
write_gh_stub() {
  local dir="$1"
  cat >"${dir}/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
sub="${1:-}"; shift || true
case "${sub}" in
  issue)
    action="${1:-}"; shift || true
    case "${action}" in
      view)
        [[ -f "${STUB_DIR}/issue-body.txt" ]] || exit 1
        cat "${STUB_DIR}/issue-body.txt"
        ;;
      edit)
        # Append whatever --body-file names, so a read-back sees it, unless
        # the fixture asks for a silently-lossy edit.
        body_file=""
        while [[ $# -gt 0 ]]; do
          [[ "$1" == "--body-file" ]] && body_file="${2:-}"
          shift
        done
        if [[ -f "${STUB_DIR}/edit-is-lossy" ]]; then
          exit 0
        fi
        [[ -n "${body_file}" ]] && cp "${body_file}" "${STUB_DIR}/issue-body.txt"
        ;;
      *) exit 1 ;;
    esac
    ;;
  pr)
    action="${1:-}"; shift || true
    case "${action}" in
      list)
        [[ -f "${STUB_DIR}/prs.txt" ]] || exit 1
        cat "${STUB_DIR}/prs.txt"
        ;;
      view)
        [[ -f "${STUB_DIR}/pr-view.txt" ]] || exit 1
        cat "${STUB_DIR}/pr-view.txt"
        ;;
      *) exit 1 ;;
    esac
    ;;
  *) exit 1 ;;
esac
STUB
  chmod +x "${dir}/gh"
}

# A git stub for `ls-remote` only; every other git call goes to the real
# binary, because the script uses git for path resolution too.
write_git_stub() {
  local dir="$1"
  cat >"${dir}/git" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
if [[ "${1:-}" == "ls-remote" ]]; then
  [[ -f "${STUB_DIR}/task-refs.txt" ]] || exit 128
  cat "${STUB_DIR}/task-refs.txt"
  exit 0
fi
exec /usr/bin/git "$@"
STUB
  chmod +x "${dir}/git"
}

new_case() {
  local name="$1"
  local dir="${work}/${name}"
  mkdir -p "${dir}/bin" "${dir}/state"
  write_gh_stub "${dir}/bin"
  write_git_stub "${dir}/bin"
  printf '%s\n' "${dir}"
}

run_in() {
  local dir="$1"
  shift
  ( export STUB_DIR="${dir}" \
           PATH="${dir}/bin:${PATH}" \
           RAVEL_EPIC_STATE_DIR="${dir}/state"
    "$@" ) 2>&1
}

# --- state directory lives in the PRIMARY checkout ---------------------
#
# Mutation: resolve the state dir from `git rev-parse --show-toplevel`
# instead of --git-common-dir. Then a wave's worktree holds its own state
# file and deliver-epic deletes it with the worktree at the end of the wave.
repo="${work}/primary"
/usr/bin/git init -q "${repo}"
( cd "${repo}"
  /usr/bin/git config user.email t@example.com
  /usr/bin/git config user.name t
  : >f
  /usr/bin/git add f
  /usr/bin/git commit -qm init ) >/dev/null
/usr/bin/git -C "${repo}" worktree add -q "${work}/wave1" -b wave1 >/dev/null 2>&1
from_worktree="$( cd "${work}/wave1" && "${ORCHESTRATOR}" path 4242 )"
# Compared against the physical path: macOS /var is a symlink to /private/var
# and the script resolves through it, which is not the property under test.
repo_real="$(cd "${repo}" && pwd -P)"
check_eq "state file resolves to the primary checkout from inside a worktree" \
  "${repo_real}/.claude/epic-state/4242.json" "${from_worktree}"

# --- init / record -----------------------------------------------------

d="$(new_case init)"
out="$(run_in "${d}" "${ORCHESTRATOR}" init 900 --title "Epic nine hundred")"; rc=$?
check_eq "init exit" "0" "${rc}"
check_eq "init writes the state file" "yes" "$([[ -f "${d}/state/900.json" ]] && echo yes || echo no)"

# task-dispatched must reach the epic issue BODY: epic-status.sh reads the
# body and nothing else, so a task recorded only locally is invisible to the
# reconciliation that would otherwise report it dead.
# Mutation: drop the body_append_task call, or drop its read-back check.
d="$(new_case dispatch_body)"
printf 'Epic body\n\n## Ledger\n' >"${d}/issue-body.txt"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
out="$(run_in "${d}" "${ORCHESTRATOR}" record 900 task-dispatched ticket=101 \
        task=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee ref=deadbeef)"; rc=$?
check_eq "task-dispatched exit" "0" "${rc}"
check_contains "ledger line reaches the issue body" \
  "task=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" "$(cat "${d}/issue-body.txt")"
check_eq "task is in local state too" "dispatched" \
  "$(jq -r '.tasks["aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"].status' "${d}/state/900.json")"

# An edit that gh reports as successful but that did not land (the shell ate
# the body, another session overwrote it) must fail loudly. gh exits 0 on
# exactly that, so the read-back is the check.
# Mutation: delete the second `gh issue view` read-back.
d="$(new_case dispatch_lossy)"
printf 'Epic body\n' >"${d}/issue-body.txt"
touch "${d}/edit-is-lossy"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
out="$(run_in "${d}" "${ORCHESTRATOR}" record 900 task-dispatched ticket=101 \
        task=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee)"; rc=$?
check_eq "a silently-lost body edit exits 70" "70" "${rc}"
check_contains "and says the ledger line did not arrive" "does not carry" "${out}"

# --- backoff ladder ----------------------------------------------------
#
# Mutation: start the ladder below 900 (the classic 30s doubling). The
# PreToolUse guard refuses any ScheduleWakeup under 900s, so such a ladder
# is not a retry at all; it is a refused tool call.
check_eq "backoff attempt 1 is the 900s floor" "900" "$("${ORCHESTRATOR}" backoff 1)"
check_eq "backoff attempt 2 doubles" "1800" "$("${ORCHESTRATOR}" backoff 2)"
check_eq "backoff attempt 3 hits the ceiling" "3600" "$("${ORCHESTRATOR}" backoff 3)"
check_eq "backoff attempt 9 stays at the ceiling" "3600" "$("${ORCHESTRATOR}" backoff 9)"
check_eq "backoff attempt 0 is clamped up to the floor" "900" "$("${ORCHESTRATOR}" backoff 0)"

# --- error classification ---------------------------------------------
#
# Mutation: classify everything as recoverable. Then a real defect parks a
# retry and the epic sleeps through it six times.
check_eq "429 is recoverable" "rate-limit" "$("${ORCHESTRATOR}" classify 'HTTP 429 Too Many Requests')"
check_eq "529 is recoverable" "server-error" "$("${ORCHESTRATOR}" classify 'upstream returned 529')"
check_eq "session limit is recoverable" "transient" "$("${ORCHESTRATOR}" classify 'session limit reached')"
check_eq "a test failure is fatal" "fatal" "$("${ORCHESTRATOR}" classify 'assertion failed: left != right')"
"${ORCHESTRATOR}" classify 'assertion failed' >/dev/null; rc=$?
check_eq "classify exits non-zero on a fatal error" "1" "${rc}"

# --- resume path -------------------------------------------------------

d="$(new_case resume)"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null

out="$(run_in "${d}" "${ORCHESTRATOR}" resume-set 900 --reason 'HTTP 429 from the control plane')"; rc=$?
check_eq "parking a 429 exits 69 (recoverable, parked)" "69" "${rc}"
check_contains "and reports the first delay" "delay_seconds=900" "${out}"
check_eq "attempt recorded" "1" "$(jq -r '.resume.attempt' "${d}/state/900.json")"

out="$(run_in "${d}" "${ORCHESTRATOR}" resume-get 900)"; rc=$?
check_eq "resume-get on a parked epic exits 69" "69" "${rc}"
check_contains "resume-get reports the parked delay" "delay_seconds=900" "${out}"

out="$(run_in "${d}" "${ORCHESTRATOR}" resume-set 900 --reason '502 bad gateway')"
check_contains "a second interruption doubles the delay" "delay_seconds=1800" "${out}"
out="$(run_in "${d}" "${ORCHESTRATOR}" resume-set 900 --reason '503 service unavailable')"
check_contains "a third hits the ceiling" "delay_seconds=3600" "${out}"

out="$(run_in "${d}" "${ORCHESTRATOR}" resume-clear 900)"; rc=$?
check_eq "resume-clear exits 0" "0" "${rc}"
check_eq "resume marker is gone" "null" "$(jq -r '.resume // "null"' "${d}/state/900.json")"
out="$(run_in "${d}" "${ORCHESTRATOR}" resume-get 900)"; rc=$?
check_eq "resume-get on a clear epic exits 0" "0" "${rc}"

# A fatal error is not parked. Sleeping and retrying a compile error burns
# the whole budget and reports nothing.
# Mutation: delete the fatal branch in resume-set.
d="$(new_case resume_fatal)"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
out="$(run_in "${d}" "${ORCHESTRATOR}" resume-set 900 --reason 'error[E0061]: this function takes 6 arguments')"; rc=$?
check_eq "a fatal error exits 75 (escalate)" "75" "${rc}"
check_eq "and parks no retry" "null" "$(jq -r '.resume // "null"' "${d}/state/900.json")"

# The budget is finite: six parks, then escalate rather than retry forever.
d="$(new_case resume_exhaust)"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
for i in 1 2 3 4 5 6; do
  run_in "${d}" "${ORCHESTRATOR}" resume-set 900 --reason 'HTTP 429' >/dev/null
done
out="$(run_in "${d}" "${ORCHESTRATOR}" resume-set 900 --reason 'HTTP 429')"; rc=$?
check_eq "the seventh park exits 75 (exhausted)" "75" "${rc}"
out="$(run_in "${d}" "${ORCHESTRATOR}" resume-get 900)"; rc=$?
check_eq "resume-get reports exhaustion as 75" "75" "${rc}"
check_contains "and says so" "EXHAUSTED" "${out}"

# --- reconcile ---------------------------------------------------------

task_a="11111111-2222-3333-4444-555555555555"
task_b="66666666-7777-8888-9999-aaaaaaaaaaaa"

# A task with a start ref and no result ref is RUNNING or LOST, and only
# fleet_status can say which. Reporting either one is the failure that
# leaves a dead task's ticket unfixed for a session.
# Mutation: report it as "running" and exit 0.
d="$(new_case reconcile_unresolved)"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
printf 'Epic\n- #101 task=%s dispatched\n' "${task_a}" >"${d}/issue-body.txt"
printf '1111 refs/heads/task/%s/start\n' "${task_a}" >"${d}/task-refs.txt"
: >"${d}/prs.txt"
out="$(run_in "${d}" "${ORCHESTRATOR}" reconcile 900)"; rc=$?
check_eq "start-but-no-result blocks the next dispatch (65)" "65" "${rc}"
check_contains "and is reported as UNRESOLVED, not running" "UNRESOLVED" "${out}"
check_eq "state records UNRESOLVED" "UNRESOLVED" \
  "$(jq -r --arg t "${task_a}" '.tasks[$t].state' "${d}/state/900.json")"

# A result ref with a merged PR is done; nothing blocks.
d="$(new_case reconcile_landed)"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
printf 'Epic\n- #101 task=%s done\n' "${task_b}" >"${d}/issue-body.txt"
{ printf '1111 refs/heads/task/%s/start\n' "${task_b}"
  printf '2222 refs/heads/task/%s/result\n' "${task_b}"; } >"${d}/task-refs.txt"
printf 'task/%s/merge\t77\tMERGED\tcafe\n' "${task_b}" >"${d}/prs.txt"
out="$(run_in "${d}" "${ORCHESTRATOR}" reconcile 900)"; rc=$?
check_eq "a landed task reconciles clean (0)" "0" "${rc}"
check_contains "and is reported merged" "merged as #77" "${out}"

# A task the local index knows about that never reached the issue body is
# invisible to epic-status.sh: it cannot be reported dead, because it is
# never seen at all.
# Mutation: drop the body-membership loop.
d="$(new_case reconcile_drift)"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
printf 'Epic with an empty ledger\n' >"${d}/issue-body.txt"
: >"${d}/task-refs.txt"
: >"${d}/prs.txt"
RAVEL_EPIC_NO_REMOTE=1 run_in "${d}" "${ORCHESTRATOR}" record 900 task-dispatched \
  ticket=101 "task=${task_a}" >/dev/null
out="$(run_in "${d}" "${ORCHESTRATOR}" reconcile 900)"; rc=$?
check_eq "a task missing from the issue body blocks (65)" "65" "${rc}"
check_contains "and names the drift" "not in epic #900's body" "${out}"

# Could-not-ask is not asked-and-got-nothing. An unreachable remote must not
# read as "no task refs exist", which would report every live task LOST and
# re-dispatch over running work.
# Mutation: restore `git ls-remote ... || true`.
d="$(new_case reconcile_unknown_refs)"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
printf 'Epic\n- #101 task=%s dispatched\n' "${task_a}" >"${d}/issue-body.txt"
rm -f "${d}/task-refs.txt"   # ls-remote now exits 128
: >"${d}/prs.txt"
out="$(run_in "${d}" "${ORCHESTRATOR}" reconcile 900)"; rc=$?
check_eq "an unreadable remote exits 65, not 0" "65" "${rc}"
check_contains "and says UNKNOWN rather than LOST" "UNKNOWN" "${out}"
check_eq "no task was marked LOST on an unreadable remote" "" \
  "$(jq -r --arg t "${task_a}" '.tasks[$t].state // ""' "${d}/state/900.json")"

# The same for the issue itself.
d="$(new_case reconcile_unknown_issue)"
run_in "${d}" "${ORCHESTRATOR}" init 900 >/dev/null
rm -f "${d}/issue-body.txt"
out="$(run_in "${d}" "${ORCHESTRATOR}" reconcile 900)"; rc=$?
check_eq "an unreadable epic issue exits 65" "65" "${rc}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
