#!/usr/bin/env bash
# scripts/chaos/agents-replace-worker.sh -- ADR-3331 decision 13, real
# backend: worker replacement and coordinator restart on the compose RustFS
# store.
#
# It starts the compose store and `ravel-server --agents`, then drives two
# scenarios with workers that are separate processes (curl and jq against the
# worker HTTP routes), so every kill is a real SIGKILL:
#
#   replace-worker   worker A takes the turn of operation 1 and records an
#                    at_least_once step, then is SIGKILLed inside that step
#                    while worker B is already polling. The client resends
#                    under the same request id. B takes the same turn over
#                    once A's lease (30 s) and the grace (10 s), both counted
#                    from A's grant, have lapsed,
#                    gets attempt 2 of the same effect, and commits.
#   restart-server   worker C takes the turn of operation 2 and records a
#                    step; the server is SIGKILLed with that turn in flight
#                    and restarted on the same store inside C's 30 s lease.
#                    C completes the effect and commits through the
#                    restarted server under its original lease, and a resend
#                    returns the same operation.
#
# Every figure the run measures is written as KEY=VALUE to a figures file and
# asserted against an exact value or a stated band (assert_figures below); a
# figure missing, printed twice, or outside its band exits 1. The figures are
# printed at the end of every run.
#
# Usage:
#   agents-replace-worker.sh               real run (needs a store: see below)
#   agents-replace-worker.sh --check       structure and dependencies only
#   agents-replace-worker.sh --dry-run F   assert a canned figures file F
#
# Store: by default `rustfs_up` (lib.sh) starts the compose RustFS, empties
# the chaos bucket and qualifies it. CHAOS_AGENTS_STORE_READY=1 skips the
# compose start and the emptying for a store already running at
# RAVEL_S3_ENDPOINT, and only qualifies it. The server is
# CHAOS_AGENTS_SERVER_BIN (default `ravel-server` on PATH), which must be
# built with `--features agents`; lib.sh's cargo fallback builds without it.
#
# Exit status: 0 every assertion held; 1 an assertion failed or a step of the
# run failed; 64 usage.
#
# Shell-safety (CLAUDE.md "Writing gate and poll shell"): exit codes are
# captured as `cmd || code=$?` on the same line; no variable is named
# `status`, `path`, `argv`, or `PWD`; no checked command is piped through
# grep/head/tail.
set -euo pipefail

# shellcheck source=scripts/chaos/lib.sh disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

SCENARIO="agents-replace-worker"

SERVER_BIN="${CHAOS_AGENTS_SERVER_BIN:-ravel-server}"
HTTP_ADDR="${CHAOS_AGENTS_HTTP:-127.0.0.1:14518}"
GRPC_ADDR="${CHAOS_AGENTS_GRPC:-127.0.0.1:14517}"
BASE="http://${HTTP_ADDR}"
API="${BASE}/api/v1/agents"
READY_SECONDS="${CHAOS_AGENTS_READY_SECONDS:-120}"
# How long worker B polls for the takeover and worker C waits for the
# restart before giving up; both are far above the band they are measured in.
TAKEOVER_DEADLINE_SECONDS=150
GO_DEADLINE_SECONDS=300

# Development bearer tokens this script hands the server it starts; not
# credentials anywhere else.
CLIENT_TOKEN="chaos-agents"
WORKER_TOKEN="chaos-agents-worker"
TENANT="chaos-agents"

PINS='[{"handler":"place","version":"1.0.0","digest":"0101010101010101010101010101010101010101010101010101010101010101"}]'
INPUT_HASH="5892e7a2b7b5a3aa11b9984ec594f8eb935e25f2f1a69ce17c2387d2bb1c74f0"

# The server's worker lease is 30 s plus 10 s of grace (ADR-3331 decision 6),
# counted from the grant, so the takeover is measured from A's grant
# (A_ASSIGNED_MS, taken when A's poll returns), not from the kill. B
# long-polls for 20 s and takes the turn over on the first poll that scans
# after the grace lapses, so RECOVERY_FROM_GRANT_MS has the lease and grace
# as its floor and one long poll above it as its ceiling.
RECOVERY_FROM_GRANT_MS_MIN=40000
RECOVERY_FROM_GRANT_MS_MAX=60000
# A must die while its lease is live, so that B's takeover follows a dead
# owner rather than racing a live one whose lease simply ran out. 10 s is a
# third of the lease; between the grant and the kill the script only waits
# for A's step and B's registration, which take well under a second.
GRANT_TO_KILL_MS_MAX=10000
# Worker C commits through the restarted server under the lease it held
# before the kill, so the restart, from the SIGKILL to /readyz, must finish
# inside the 30 s lease. That bound is necessary, not sufficient: C's lease
# started at its grant, before the kill.
RESTART_READY_MS_MAX=30000

usage() {
  cat <<'EOF'
Usage: agents-replace-worker.sh [--check | --dry-run FIGURES | --help]

  --check            Validate structure and dependencies only. Starts nothing.
  --dry-run FIGURES  Run the assertions on a canned figures file (KEY=VALUE
                     lines) and exit with their verdict. Starts nothing.
  --help             Show this help.

With no flag, runs both scenarios against a real store (see the header).
EOF
}

fail() {
  log "${SCENARIO}: FAILED: $*"
  exit 1
}

# --- Assertions -------------------------------------------------------------
# Pure over a figures file, so --dry-run exercises them on canned output
# (scripts/chaos/agents-replace-worker.test.sh).

# The value of KEY in FILE; KEY must be printed exactly once.
figure() {
  local file=$1 key=$2 count
  count=$(awk -v k="$key=" 'index($0, k) == 1 { n++ } END { print n + 0 }' "$file")
  (( count == 1 )) || fail "$key printed $count times, expected exactly once"
  awk -v k="$key=" 'index($0, k) == 1 { print substr($0, length(k) + 1) }' "$file"
}

expect() {
  local file=$1 key=$2 want=$3 got
  got=$(figure "$file" "$key")
  [[ "$got" == "$want" ]] || fail "$key=$got, expected $want"
  log "ok $key=$got (expected $want)"
}

expect_band() {
  local file=$1 key=$2 lo=$3 hi=$4 got
  got=$(figure "$file" "$key")
  [[ "$got" =~ ^[0-9]+$ ]] || fail "$key=$got is not a number"
  (( got >= lo && got <= hi )) || fail "$key=$got, expected within [$lo, $hi]"
  log "ok $key=$got (expected within [$lo, $hi])"
}

# KEY_A and KEY_B both printed once, and equal (or, with "differ", not equal).
expect_same() {
  local file=$1 key_a=$2 key_b=$3 want=${4:-same} a b
  a=$(figure "$file" "$key_a")
  b=$(figure "$file" "$key_b")
  [[ -n "$a" ]] || fail "$key_a is empty"
  [[ -n "$b" ]] || fail "$key_b is empty"
  if [[ "$want" == "same" ]]; then
    [[ "$a" == "$b" ]] || fail "$key_b=$b, expected the same as $key_a=$a"
    log "ok $key_b=$b (expected equal to $key_a)"
  else
    [[ "$a" != "$b" ]] || fail "$key_b=$b, expected different from $key_a"
    log "ok $key_b=$b (expected different from $key_a=$a)"
  fi
}

assert_figures() {
  local f=$1
  [[ -r "$f" ]] || fail "figures file not readable: $f"

  # replace-worker: one operation, one turn, attempt 2 of one effect.
  expect "$f" OP1_SEND_DUPLICATE false
  expect_same "$f" OP1_OPERATION_ID OP1_RETRIED_OPERATION_ID
  expect "$f" OP1_RETRIED_DUPLICATE true
  expect_same "$f" OP1_OPERATION_ID A_ASSIGNED_OPERATION_ID
  expect "$f" A_STEP_ATTEMPT 1
  expect "$f" A_KILLED_BEFORE_COMMIT true
  expect_same "$f" A_WORKER_ID B_WORKER_ID differ
  expect_same "$f" OP1_OPERATION_ID B_ASSIGNED_OPERATION_ID
  expect_same "$f" A_TURN_ID B_TURN_ID
  expect "$f" B_STEP_ATTEMPT 2
  expect_same "$f" A_EFFECT_ID B_EFFECT_ID
  expect "$f" B_EFFECT_STATUS completed
  expect "$f" B_COMMIT_STATUS completed
  expect "$f" OP1_RESULT_STATUS completed
  expect_same "$f" B_WORKER_ID OP1_RESULT_WORKER
  expect "$f" OP1_STATE_VERSION 1
  expect_band "$f" GRANT_TO_KILL_MS 0 "$GRANT_TO_KILL_MS_MAX"
  expect_band "$f" RECOVERY_FROM_GRANT_MS "$RECOVERY_FROM_GRANT_MS_MIN" "$RECOVERY_FROM_GRANT_MS_MAX"

  # restart-server: the turn in flight across the restart commits once.
  expect_band "$f" RESTART_READY_MS 0 "$RESTART_READY_MS_MAX"
  expect_same "$f" OP1_OPERATION_ID OP2_OPERATION_ID differ
  expect "$f" OP2_SEND_DUPLICATE false
  expect_same "$f" OP2_OPERATION_ID C_ASSIGNED_OPERATION_ID
  expect_same "$f" A_TURN_ID C_TURN_ID differ
  expect_same "$f" A_EFFECT_ID C_EFFECT_ID differ
  expect "$f" C_STEP_ATTEMPT 1
  expect "$f" OP2_STATUS_AFTER_RESTART running
  expect "$f" C_EFFECT_STATUS completed
  expect "$f" C_COMMIT_STATUS completed
  expect "$f" OP2_RESULT_STATUS completed
  expect_same "$f" C_WORKER_ID OP2_RESULT_WORKER
  expect_same "$f" OP2_OPERATION_ID OP2_RETRIED_OPERATION_ID
  expect "$f" OP2_RETRIED_DUPLICATE true
  # Two committed turns in all: one before the restart, one after. A
  # duplicated result after the restart shows as 3.
  expect "$f" STATE_VERSION_BEFORE_RESTART 1
  expect "$f" STATE_VERSION_AFTER_RESTART 2
  expect "$f" TURNS_COMMITTED_AFTER_RESTART 1
  expect "$f" TAKEOVERS_AFTER_RESTART 0
  log "${SCENARIO}: every assertion held"
}

# --- Argument parsing -------------------------------------------------------

MODE="run"
CANNED=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) MODE="check"; shift ;;
    --dry-run)
      [[ $# -ge 2 ]] || { usage >&2; exit 64; }
      MODE="dry-run"; CANNED=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "agents-replace-worker.sh: unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

if [[ "$MODE" == "dry-run" ]]; then
  assert_figures "$CANNED"
  exit 0
fi

if [[ "$MODE" == "check" ]]; then
  echo "== agents-replace-worker.sh --check =="
  defects=0
  warnings=0
  for fn in figure expect expect_band expect_same assert_figures; do
    if declare -F "$fn" >/dev/null; then
      echo "  OK    assertion helper defined: $fn"
    else
      echo "  FAIL  assertion helper missing: $fn"
      defects=$(( defects + 1 ))
    fi
  done
  echo "  OK    recovery band from A's grant: [${RECOVERY_FROM_GRANT_MS_MIN}, ${RECOVERY_FROM_GRANT_MS_MAX}] ms"
  echo "  OK    grant-to-kill band: [0, ${GRANT_TO_KILL_MS_MAX}] ms"
  echo "  OK    restart-ready band: [0, ${RESTART_READY_MS_MAX}] ms"
  for tool in curl jq; do
    if chaos_have_command "$tool"; then
      echo "  OK    tool present: $tool"
    else
      echo "  WARN  tool absent (needed for a real run): $tool"
      warnings=$(( warnings + 1 ))
    fi
  done
  if chaos_have_command "$SERVER_BIN"; then
    echo "  OK    server binary: $(command -v "$SERVER_BIN")"
  else
    echo "  WARN  server binary not found: $SERVER_BIN (build with --features agents)"
    warnings=$(( warnings + 1 ))
  fi
  if [[ "${CHAOS_AGENTS_STORE_READY:-0}" != 1 ]]; then
    if chaos_have_command docker; then
      echo "  OK    tool present: docker"
    else
      echo "  WARN  docker absent: a real run cannot start the compose store"
      warnings=$(( warnings + 1 ))
    fi
  fi
  (( defects == 0 )) || { echo "check: FAIL -- $defects structural defect(s)"; exit 1; }
  if [[ "${CHAOS_CHECK_STRICT:-0}" == 1 ]] && (( warnings > 0 )); then
    echo "check: FAIL -- $warnings missing real-run dependency(ies) (CHAOS_CHECK_STRICT=1)"
    exit 1
  fi
  echo "check: PASS ($warnings real-run dependency warning(s))"
  exit 0
fi

# --- Real run ---------------------------------------------------------------

chaos_have_command curl || fail "curl not found on PATH"
chaos_have_command jq || fail "jq not found on PATH"
chaos_have_command "$SERVER_BIN" || fail "server binary not found: $SERVER_BIN"
chaos_tenant_hash_args || exit 64

WORK=$(mktemp -d)
FIGURES="$WORK/figures"
: >"$FIGURES"
SERVER_LOG=$(chaos_logfile "$SCENARIO" server)
server_pid=""
worker_pids=()
worker_logs=()

cleanup() {
  local code=$? pid
  for pid in "${worker_pids[@]}" "$server_pid"; do
    [[ -n "$pid" ]] || continue
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  if [[ "${CHAOS_AGENTS_STORE_READY:-0}" != 1 ]]; then
    rustfs_down
  fi
  chaos_release_logs "$code" "$SERVER_LOG" "${worker_logs[@]}"
  rm -rf "$WORK"
  exit "$code"
}
trap cleanup EXIT

record() {
  printf '%s=%s\n' "$1" "$2" >>"$FIGURES"
}

now_ms() {
  date +%s%3N
}

post() {
  local token=$1 route=$2 body=$3
  curl --silent --show-error --fail-with-body --max-time 30 \
    -H "authorization: Bearer ${token}" -H 'content-type: application/json' \
    -d "$body" "${API}/${route}"
}

get() {
  local token=$1 route=$2
  curl --silent --show-error --fail-with-body --max-time 30 \
    -H "authorization: Bearer ${token}" "${API}/${route}"
}

start_server() {
  "$SERVER_BIN" "${CHAOS_TENANT_HASH_ARGS[@]}" --store s3 --agents \
    --listen-http "$HTTP_ADDR" --listen-grpc "$GRPC_ADDR" \
    --tenant-token "${CLIENT_TOKEN}=${TENANT};agents" \
    --tenant-token "${WORKER_TOKEN}=${TENANT};agents_worker" \
    >>"$SERVER_LOG" 2>&1 &
  server_pid=$!
  local deadline=$(( SECONDS + READY_SECONDS ))
  until curl --silent --fail --max-time 2 "${BASE}/readyz" >/dev/null 2>&1; do
    kill -0 "$server_pid" 2>/dev/null || fail "ravel-server exited before it was ready (log: $SERVER_LOG)"
    (( SECONDS < deadline )) || fail "ravel-server not ready within ${READY_SECONDS}s"
    sleep 1
  done
  log "ravel-server ready (pid ${server_pid})"
}

wait_for_file() {
  local file=$1 seconds=$2 deadline
  deadline=$(( SECONDS + seconds ))
  until [[ -s "$file" ]]; do
    (( SECONDS < deadline )) || return 1
    sleep 0.2
  done
}

AGENT_KEY="replace-$(date +%s%N)"
AGENT_JSON=$(jq -cn --arg key "$AGENT_KEY" '{kind: "order", key: $key}')

send() {
  local request_id=$1
  post "$CLIENT_TOKEN" send "$(jq -cn --argjson agent "$AGENT_JSON" --arg r "$request_id" \
    '{agent: $agent, handler: "place", request_id: $r, payload: {request: $r}}')"
}

register() {
  post "$WORKER_TOKEN" worker/register "$(jq -cn --argjson h "$PINS" '{handlers: $h}')" |
    jq -er .worker_id
}

# Poll until an assignment arrives; prints the assignment.
poll_until_assigned() {
  local worker_id=$1 deadline_seconds=$2 deadline body
  deadline=$(( SECONDS + deadline_seconds ))
  while (( SECONDS < deadline )); do
    body=$(post "$WORKER_TOKEN" worker/poll "$(jq -cn --arg w "$worker_id" --argjson h "$PINS" \
      '{worker_id: $w, handlers: $h, wait_ms: 20000}')") || return 1
    if [[ "$(jq -c .assignment <<<"$body")" != null ]]; then
      jq -c .assignment <<<"$body"
      return 0
    fi
  done
  return 1
}

step() {
  local assignment=$1 step_key=$2
  post "$WORKER_TOKEN" worker/step "$(jq -cn --argjson a "$assignment" --arg k "$step_key" \
    --arg h "$INPUT_HASH" \
    '{lease: $a.lease, step_key: $k, tool_name: "chaos.tool", tool_version: "1",
      policy: "at_least_once", input_hash: $h}')"
}

complete_effect() {
  local effect=$1 attempt=$2 worker_id=$3
  post "$WORKER_TOKEN" worker/effects/complete "$(jq -cn --arg e "$effect" \
    --argjson n "$attempt" --arg w "$worker_id" '{effect: $e, attempt: $n, result: {worker: $w}}')"
}

commit() {
  local assignment=$1 worker_id=$2
  post "$WORKER_TOKEN" worker/commit "$(jq -cn --argjson a "$assignment" --arg w "$worker_id" \
    '{lease: $a.lease, outcome: "completed", state: {last_worker: $w},
      output: {worker: $w}, events: [], timers: [], intents: []}')"
}

# A worker process. Writes its figures to OUT as KEY=VALUE.
#   mode kill-in-step  take a turn, record a step, then stay inside the step
#                      until SIGKILLed.
#   mode replace       take a turn over, rerun the step, complete, commit.
#   mode await-go      take a turn, record a step, wait for GO, complete,
#                      commit.
worker() {
  local mode=$1 name=$2 out=$3 go=${4:-}
  local worker_id assignment assigned_ms operation turn stepped effect attempt effect_status committed
  worker_id=$(register)
  printf '%s_WORKER_ID=%s\n' "$name" "$worker_id" >>"$out"
  assignment=$(poll_until_assigned "$worker_id" "$TAKEOVER_DEADLINE_SECONDS")
  assigned_ms=$(now_ms)
  operation=$(jq -er .operation <<<"$assignment")
  turn=$(jq -er .lease.turn_id <<<"$assignment")
  stepped=$(step "$assignment" work)
  effect=$(jq -er .effect <<<"$stepped")
  attempt=$(jq -er .attempt <<<"$stepped")
  {
    printf '%s_ASSIGNED_MS=%s\n' "$name" "$assigned_ms"
    printf '%s_ASSIGNED_OPERATION_ID=%s\n' "$name" "$operation"
    printf '%s_TURN_ID=%s\n' "$name" "$turn"
    printf '%s_EFFECT_ID=%s\n' "$name" "$effect"
    printf '%s_STEP_ATTEMPT=%s\n' "$name" "$attempt"
  } >>"$out"
  case "$mode" in
    kill-in-step)
      printf '%s_IN_STEP=1\n' "$name" >>"$out"
      exec sleep 3600
      ;;
    await-go)
      printf '%s_IN_STEP=1\n' "$name" >>"$out"
      wait_for_file "$go" "$GO_DEADLINE_SECONDS"
      ;;
  esac
  effect_status=$(complete_effect "$effect" "$attempt" "$worker_id" | jq -er .status)
  committed=$(commit "$assignment" "$worker_id" | jq -er .status)
  {
    printf '%s_EFFECT_STATUS=%s\n' "$name" "$effect_status"
    printf '%s_COMMIT_STATUS=%s\n' "$name" "$committed"
    printf '%s_DONE=1\n' "$name"
  } >>"$out"
}

start_worker() {
  local mode=$1 name=$2 go=${3:-}
  local out="$WORK/worker-${name}.out"
  : >"$out"
  local log_file
  log_file=$(chaos_logfile "$SCENARIO" "worker-${name}")
  worker_logs+=("$log_file")
  worker "$mode" "$name" "$out" "$go" >>"$log_file" 2>&1 &
  worker_pids+=("$!")
  LAST_WORKER_PID=$!
  LAST_WORKER_OUT=$out
}

# Copy NAME_* figures a worker wrote, minus its bookkeeping lines and its raw
# NAME_ASSIGNED_MS timestamp, which the run reads with worker_ms and records
# only as the intervals asserted on it.
take_worker_figures() {
  local out=$1
  awk '!/_IN_STEP=|_DONE=|_ASSIGNED_MS=/' "$out" >>"$FIGURES"
}

worker_figure() {
  local out=$1 key=$2
  awk -v k="$key=" 'index($0, k) == 1 { print substr($0, length(k) + 1) }' "$out"
}

# A millisecond timestamp a worker wrote; fails the run when it is missing,
# written twice, or not a number, so an interval is never taken from a blank.
worker_ms() {
  local out=$1 key=$2 got
  got=$(worker_figure "$out" "$key")
  [[ "$got" =~ ^[0-9]+$ ]] || fail "$key=$got in $out is not one millisecond timestamp"
  printf '%s\n' "$got"
}

wait_for_line() {
  local out=$1 key=$2 seconds=$3 deadline
  deadline=$(( SECONDS + seconds ))
  until [[ -n "$(worker_figure "$out" "$key")" ]]; do
    (( SECONDS < deadline )) || return 1
    sleep 0.2
  done
}

state_version() {
  get "$CLIENT_TOKEN" "state?kind=order&key=${AGENT_KEY}" | jq -r .state_version
}

result_of() {
  get "$CLIENT_TOKEN" "result?operation=$1&wait_ms=10000"
}

if [[ "${CHAOS_AGENTS_STORE_READY:-0}" == 1 ]]; then
  log "CHAOS_AGENTS_STORE_READY=1: using the store at ${RAVEL_S3_ENDPOINT}; qualifying it"
  ravel_cli --store s3 store qualify || fail "store qualify failed"
else
  rustfs_up || fail "rustfs_up failed"
fi

start_server
post "$CLIENT_TOKEN" create "$(jq -cn --argjson agent "$AGENT_JSON" '{agent: $agent}')" >/dev/null

# --- Scenario replace-worker ------------------------------------------------

log "scenario replace-worker: SIGKILL worker A inside its step"
sent=$(send req-1)
op1=$(jq -r .operation <<<"$sent")
record OP1_OPERATION_ID "$op1"
record OP1_SEND_DUPLICATE "$(jq -r .duplicate <<<"$sent")"

start_worker kill-in-step A
a_pid=$LAST_WORKER_PID
a_out=$LAST_WORKER_OUT
wait_for_line "$a_out" A_IN_STEP 60 || fail "worker A did not reach its step (log: ${worker_logs[0]})"
start_worker replace B
b_pid=$LAST_WORKER_PID
b_out=$LAST_WORKER_OUT
wait_for_line "$b_out" B_WORKER_ID 30 || fail "worker B did not register"

kill_ms=$(now_ms)
sigkill_pid "$a_pid"
a_killed_before_commit=false
if [[ -z "$(worker_figure "$a_out" A_COMMIT_STATUS)" ]]; then
  a_killed_before_commit=true
fi
record A_KILLED_BEFORE_COMMIT "$a_killed_before_commit"
a_assigned_ms=$(worker_ms "$a_out" A_ASSIGNED_MS)
record GRANT_TO_KILL_MS "$(( kill_ms - a_assigned_ms ))"
take_worker_figures "$a_out"

resent=$(send req-1)
record OP1_RETRIED_OPERATION_ID "$(jq -r .operation <<<"$resent")"
record OP1_RETRIED_DUPLICATE "$(jq -r .duplicate <<<"$resent")"

b_code=0
wait "$b_pid" || b_code=$?
(( b_code == 0 )) || fail "worker B exited $b_code (log: ${worker_logs[1]})"
b_assigned_ms=$(worker_ms "$b_out" B_ASSIGNED_MS)
record RECOVERY_FROM_GRANT_MS "$(( b_assigned_ms - a_assigned_ms ))"
take_worker_figures "$b_out"

result1=$(result_of "$op1")
record OP1_RESULT_STATUS "$(jq -r .result.status <<<"$result1")"
record OP1_RESULT_WORKER "$(jq -r .result.output.worker <<<"$result1")"
record OP1_STATE_VERSION "$(state_version)"

# --- Scenario restart-server ------------------------------------------------

log "scenario restart-server: SIGKILL the server with a turn in flight"
sent=$(send req-2)
op2=$(jq -r .operation <<<"$sent")
record OP2_OPERATION_ID "$op2"
record OP2_SEND_DUPLICATE "$(jq -r .duplicate <<<"$sent")"
record STATE_VERSION_BEFORE_RESTART "$(state_version)"

go_file="$WORK/go-C"
start_worker await-go C "$go_file"
c_pid=$LAST_WORKER_PID
c_out=$LAST_WORKER_OUT
wait_for_line "$c_out" C_IN_STEP 60 || fail "worker C did not reach its step (log: ${worker_logs[2]})"

restart_kill_ms=$(now_ms)
sigkill_pid "$server_pid"
server_pid=""
start_server
record RESTART_READY_MS "$(( $(now_ms) - restart_kill_ms ))"
# Checked here as well as in assert_figures: a restart slower than C's lease
# then fails naming the restart, not as worker C's exit code below.
expect_band "$FIGURES" RESTART_READY_MS 0 "$RESTART_READY_MS_MAX"
record OP2_STATUS_AFTER_RESTART "$(get "$CLIENT_TOKEN" "operation?operation=${op2}" | jq -r .status)"
echo go >"$go_file"

c_code=0
wait "$c_pid" || c_code=$?
(( c_code == 0 )) || fail "worker C exited $c_code (log: ${worker_logs[2]})"
take_worker_figures "$c_out"

result2=$(result_of "$op2")
record OP2_RESULT_STATUS "$(jq -r .result.status <<<"$result2")"
record OP2_RESULT_WORKER "$(jq -r .result.output.worker <<<"$result2")"
resent=$(send req-2)
record OP2_RETRIED_OPERATION_ID "$(jq -r .operation <<<"$resent")"
record OP2_RETRIED_DUPLICATE "$(jq -r .duplicate <<<"$resent")"
record STATE_VERSION_AFTER_RESTART "$(state_version)"

metrics=$(curl --silent --show-error --fail --max-time 5 "${BASE}/metrics") || fail "metrics scrape failed"
record TURNS_COMMITTED_AFTER_RESTART \
  "$(metric_value_from_body "$metrics" ravel_agents_turns_committed_total mode=all)"
record TAKEOVERS_AFTER_RESTART \
  "$(metric_value_from_body "$metrics" ravel_agents_takeovers_total mode=all)"

echo "================ FIGURES: ${SCENARIO} ================"
cat "$FIGURES"
echo "-----------------------------------------------------------"
assert_figures "$FIGURES"
echo "RESULT: PASS (${SCENARIO})"
