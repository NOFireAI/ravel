#!/usr/bin/env bash
# Kill-and-recover demo for Ravel Agents (ADR-3331), in the shape of
# demo/kill-and-recover.sh.
#
# It starts the compose RustFS store (or MemoryStore with
# RAVEL_AGENTS_DEMO_STORE=memory) and a built ravel-server with --agents, then
# runs the assistant example (sdk/typescript/examples/assistant) twice:
#
#   at_least_once  the worker is killed inside the `kb.draft` step, after the
#                  tool's side effect and before its result is recorded. The
#                  client resends under the same request id, a second worker
#                  takes the turn over once the first worker's lease lapses,
#                  replays the journaled `kb.search` step without running it,
#                  and runs `kb.draft` again under the same effect id.
#   manual         the worker is killed inside the `desk.ticket` step. The
#                  second worker does not run the ticket tool again: the turn
#                  blocks with EffectInDoubt until the operator reconciles the
#                  effect with the ticket the killed run filed.
#
# The kill is an abort of the worker inside the driver process, not a process
# kill: the step journal is the SDK's provisional in-memory one until the
# server mounts its step and effect routes, so both workers share it in one
# process. See sdk/typescript/packages/agents/README.md, "Wire gaps".
#
# Every figure the driver prints is asserted here against an exact value or a
# stated band; a figure missing, printed twice, or outside its band exits 1.
#
# Shell-safety (CLAUDE.md "Writing gate and poll shell"): exit codes are
# captured as `cmd || code=$?` on the same line; no variable is named `status`,
# `path`, `argv`, or `PWD`; no checked command is piped through grep/head/tail.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
sdk_dir="$repo_root/sdk/typescript"

STORE="${RAVEL_AGENTS_DEMO_STORE:-rustfs}"
COMPOSE_FILE="${COMPOSE_FILE:-$repo_root/deploy/docker-compose/rustfs.yml}"
HTTP_ADDR="${RAVEL_AGENTS_DEMO_HTTP:-127.0.0.1:14318}"
GRPC_ADDR="${RAVEL_AGENTS_DEMO_GRPC:-127.0.0.1:14317}"
SERVER_BIN="${RAVEL_SERVER_BIN:-${CARGO_TARGET_DIR:-$repo_root/target}/debug/ravel-server}"
# Development bearer tokens this script hands the server it starts; not
# credentials anywhere else.
CLIENT_TOKEN="demo-agents"
WORKER_TOKEN="demo-worker"

READY_TIMEOUT_SECONDS="${RAVEL_AGENTS_DEMO_READY_TIMEOUT_SECONDS:-60}"

# The server's worker lease is 30 s plus 10 s of grace (ADR-3331), so the
# second worker takes the turn over about 40 s after the first stops
# heartbeating. The band leaves 5 s below for a lease granted just before the
# kill and 20 s above for the takeover poll and the remaining steps.
RECOVERY_MS_MIN=35000
RECOVERY_MS_MAX=60000

ANSWER="A worker that dies mid-turn is replaced and the turn is re-run. A step whose result is journaled is replayed, never run again."

log() { echo "[agents-kill-and-recover] $*" >&2; }

fail() {
  log "FAILED: $*"
  exit 1
}

# --- Preflight -------------------------------------------------------------

case "$STORE" in
  rustfs | memory) ;;
  *) fail "RAVEL_AGENTS_DEMO_STORE=$STORE is neither rustfs nor memory" ;;
esac
command -v curl >/dev/null 2>&1 || fail "curl not found on PATH"
command -v node >/dev/null 2>&1 || fail "node not found on PATH; the SDK needs node 22.15 or later 22.x"
# 22.15.0 is the first 22.x with module.registerHooks, which the driver's
# loader hook (sdk/typescript/examples/ts-hooks.js) uses.
node_version=$(node -p 'process.versions.node')
node_major=${node_version%%.*}
node_minor=${node_version#*.}
node_minor=${node_minor%%.*}
if [[ "$node_major" != "22" ]] || (( node_minor < 15 )); then
  fail "node $node_version found; the SDK needs node 22.15 or later 22.x"
fi
[[ -x "$SERVER_BIN" ]] || fail "ravel-server not found at $SERVER_BIN; build it with" \
  "'cargo build --locked -p ravel-server --features agents' or set RAVEL_SERVER_BIN"
[[ -d "$sdk_dir/node_modules" ]] || fail "SDK dependencies missing; run 'npm ci' in $sdk_dir"
if [[ "$STORE" == "rustfs" ]]; then
  command -v docker >/dev/null 2>&1 || fail "docker not found on PATH; RAVEL_AGENTS_DEMO_STORE=memory runs without it"
  [[ -f "$COMPOSE_FILE" ]] || fail "compose file not found: $COMPOSE_FILE"
fi

compose() { docker compose --file "$COMPOSE_FILE" "$@"; }

work_dir=$(mktemp -d)
server_pid=""
started_store=0
cleanup() {
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if (( started_store )); then
    compose stop rustfs >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT

# --- Store and server ------------------------------------------------------

store_args=(--store memory)
if [[ "$STORE" == "rustfs" ]]; then
  # A RustFS container that was already running is left running on exit.
  if [[ -n "$(compose ps --quiet --status running rustfs)" ]]; then
    log "using the compose RustFS store that is already running"
  else
    log "starting the compose RustFS store"
    compose up -d rustfs
    started_store=1
  fi
  compose run --rm createbucket
  # The compose file's published development credentials.
  store_args=(--store s3 --s3-endpoint http://127.0.0.1:9000 --s3-allow-http
    --s3-bucket ravel-dev --s3-region us-east-1
    --s3-access-key ravel --s3-secret-key ravel-dev-secret)
fi

log "starting ravel-server --agents on $HTTP_ADDR ($STORE store)"
"$SERVER_BIN" "${store_args[@]}" --agents --tenant-hash-unkeyed \
  --listen-http "$HTTP_ADDR" --listen-grpc "$GRPC_ADDR" \
  --tenant-token "$CLIENT_TOKEN=demo;agents" \
  --tenant-token "$WORKER_TOKEN=demo;agents_worker" \
  >"$work_dir/server.log" 2>&1 &
server_pid=$!

base="http://$HTTP_ADDR"
deadline=$(( SECONDS + READY_TIMEOUT_SECONDS ))
until curl --silent --fail --max-time 2 "$base/readyz" >/dev/null 2>&1; do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$work_dir/server.log" >&2
    fail "ravel-server exited before it was ready"
  fi
  (( SECONDS < deadline )) || fail "ravel-server not ready within ${READY_TIMEOUT_SECONDS}s"
  sleep 1
done
log "ravel-server ready"

# --- Scenarios -------------------------------------------------------------

run_driver() {
  local policy=$1 out=$2 code=0
  (
    cd "$sdk_dir"
    RAVEL_URL="$base" RAVEL_AGENTS_TOKEN="$CLIENT_TOKEN" RAVEL_WORKER_TOKEN="$WORKER_TOKEN" \
      node --experimental-transform-types --disable-warning=ExperimentalWarning \
      --import ./examples/ts-hooks.js examples/assistant/kill-and-recover.ts "$policy"
  ) >"$out" || code=$?
  (( code == 0 )) || fail "the $policy driver exited $code"
}

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

# Sets operation_id from FILE once OPERATION_ID is printed exactly once, is a
# non-empty operation handle (unpadded base64url), and equals
# RETRIED_OPERATION_ID, the operation the resend under the same request id got.
# It sets a variable rather than printing, so a miss exits the script instead
# of a command-substitution subshell.
operation_id=""
expect_operation() {
  local file=$1 id
  id=$(figure "$file" OPERATION_ID)
  [[ "$id" =~ ^[A-Za-z0-9_-]+$ ]] || fail "OPERATION_ID=$id, expected a non-empty base64url operation handle"
  log "ok OPERATION_ID=$id (expected a non-empty base64url operation handle)"
  expect "$file" RETRIED_OPERATION_ID "$id"
  operation_id=$id
}

log "scenario at_least_once: kill inside kb.draft"
alo="$work_dir/at_least_once.out"
run_driver at_least_once "$alo"
expect_operation "$alo"
alo_operation=$operation_id
expect "$alo" RETRIED_SAME_OPERATION true
expect "$alo" WORKERS_DIFFER true
expect "$alo" SEARCH_RUNS 1
expect "$alo" DRAFT_RUNS 2
expect "$alo" TICKET_RUNS 0
expect "$alo" DRAFT_EFFECTS_DISTINCT 1
expect "$alo" DRAFT_ATTEMPTS 1,2
expect "$alo" EFFECT_STATES kb.search:completed,kb.draft:completed
expect "$alo" RESULT_ERROR none
expect "$alo" RESULT_ARTICLES kb-2,kb-3
expect "$alo" RESULT_TICKET null
expect "$alo" RESULT_ANSWER "$ANSWER"
expect "$alo" ANSWERED 1
expect "$alo" IN_DOUBT_ERROR none
expect_band "$alo" RECOVERY_MS "$RECOVERY_MS_MIN" "$RECOVERY_MS_MAX"

log "scenario manual: kill inside desk.ticket"
man="$work_dir/manual.out"
run_driver manual "$man"
expect_operation "$man"
man_operation=$operation_id
[[ "$man_operation" != "$alo_operation" ]] ||
  fail "manual OPERATION_ID=$man_operation, expected a different operation from at_least_once's"
expect "$man" RETRIED_SAME_OPERATION true
expect "$man" WORKERS_DIFFER true
expect "$man" IN_DOUBT_ERROR EffectInDoubt
expect "$man" IN_DOUBT_TICKET_RUNS 1
# No server route reports a blocked operation until the effect routes land.
expect "$man" IN_DOUBT_SERVER_STATUS running
expect "$man" SEARCH_RUNS 1
expect "$man" DRAFT_RUNS 1
expect "$man" TICKET_RUNS 1
expect "$man" TICKET_ATTEMPTS 1
expect "$man" EFFECT_STATES kb.search:completed,kb.draft:completed,desk.ticket:completed
expect "$man" RESULT_ERROR none
expect "$man" RESULT_ARTICLES kb-2,kb-3
expect "$man" RESULT_TICKET T-1001
expect "$man" RESULT_ANSWER "$ANSWER"
expect "$man" ANSWERED 1
expect_band "$man" RECOVERY_MS "$RECOVERY_MS_MIN" "$RECOVERY_MS_MAX"

# Both values were asserted above; assigned first so a miss here still exits.
alo_answer=$(figure "$alo" RESULT_ANSWER)
man_ticket=$(figure "$man" RESULT_TICKET)
echo "at_least_once operation: $alo_operation"
echo "at_least_once result:    $alo_answer"
echo "manual operation:        $man_operation"
echo "manual result:           ticket $man_ticket"
log "PASSED"
