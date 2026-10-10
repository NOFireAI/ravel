#!/usr/bin/env bash
# scripts/chaos/kill-ingest-flush.sh -- ADR-0077 section 4, scenario 1:
# "Kill ingest mid-flush."
#
# Drive load through the server, SIGKILL it mid-flush (trigger observed via a
# metrics marker -- see below), restart, and assert the pinned oracle for
# this scenario:
#
#   * strict-ack-implies-durable: every write acked under strict ack before
#     the kill is durable and queryable after restart;
#   * custody-and-catalog verification clean, which is also what discharges
#     partial-flush visibility here: no live record references an unacked or
#     partial object. Commit tokens are opaque base64, so there is no "one
#     past the last ack" token to probe for; see the note in lib.sh.
#
# This is the exit criterion's "kill -9 mid-flush against RustFS with no
# strict-ack violation" row (ADR-0077 section 4).
#
# MID-FLUSH TRIGGER (named explicitly, per the task): the metrics pipeline's
# flush-attempt count, the sum of the five per-trigger `ravel_ingest_flushes_*`
# families scraped from the server's /metrics (`flush_attempts_from_body` in
# lib.sh), so a size-, age- or manually-triggered flush all count. After the
# strict-ack exports the scenario reads that count, sends one more, large,
# export in the background, and SIGKILLs the server as soon as the count
# rises, with no further scrape between the detecting poll and the kill. The
# kill landed mid-flush when that in-flight export got no answer: its curl
# exit code is one of CHAOS_CURL_UNANSWERED_CODES (lib.sh). An HTTP error
# status (curl exit 22) is an answer and reads as not mid-flush. The scenario
# prints that verdict as a `KILL-TIMING: mid-flush=yes|no` line before the
# oracle summary. With no rise within the budget it kills anyway and warns.
#
# --check / --dry-run validates structure and dependencies WITHOUT starting
# RustFS, driving load, or issuing a real kill. That is the only proof
# available in an environment with no object store; a real end-to-end run is
# the orchestrator's job (executors have no object store -- ADR-0077
# section 4).
#
# Exit status: 0 when every pinned oracle assertion held, 1 when one failed
# (an ordinary oracle failure; this scenario is not release-blocking), 3 on a
# setup error, 64 on a usage error.
#
# Gate-shell discipline: see scripts/chaos/lib.sh header.
set -eEuo pipefail
# -E carries the ERR trap into functions, so a setup command that fails inside
# a helper exits 3 like one at top level rather than with its own status,
# which is often the 1 an oracle failure uses. The oracle calls below carry
# `|| true` and never reach it.
trap 'exit 3' ERR

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
# shellcheck source=scripts/chaos/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

HTTP_ADDR="${CHAOS_HTTP_ADDR:-127.0.0.1:14318}"
GRPC_ADDR="${CHAOS_GRPC_ADDR:-127.0.0.1:14317}"
BASE_URL="http://${HTTP_ADDR}"
# The series the fixture actually emits: gen_otlp_fixture (services/ravel-server
# /examples/gen_otlp_fixture.rs) writes one gauge named `demo_requests_total`.
# A query for any other name is unsatisfiable and makes the durability oracle
# green-by-vacuity, which is the failure mode this whole lane exists to close.
SERIES="demo_requests_total"
# Number of strict-ack exports of the one-point fixture to drive before the
# in-flight export. Each returns a commit token recorded as an
# acked-before-kill write.
EXPORT_COUNT="${CHAOS_EXPORT_COUNT}"

usage() {
  cat <<'EOF'
Usage: kill-ingest-flush.sh [--check|--dry-run] [--help]

  --check, --dry-run   Validate structure and dependencies only. Does NOT
                       start RustFS, drive load, or issue a real kill -9.
  --help               Show this help.

With no flag, runs the full scenario against a real RustFS (orchestrator-only;
executors have no object store and must use --check).
EOF
}

MODE="run"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check|--dry-run) MODE="check"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "kill-ingest-flush.sh: unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

# Refuse a bad tenant-hash mode before anything starts: the start sites read
# ravel_server_cmd through a process substitution, which drops its status.
chaos_tenant_hash_args || exit 64

if [[ "$MODE" == "check" ]]; then
  echo "== kill-ingest-flush.sh --check (scenario 1: kill ingest mid-flush) =="
  echo "mid-flush trigger marker: sum of ${CHAOS_FLUSH_METRICS[*]} (${CHAOS_FLUSH_SELECTOR}, attempt-time increments)"
  echo "in-flight export: ${CHAOS_FIXTURE_SERIES} series x ${CHAOS_FIXTURE_POINTS} points"
  rc=0
  check_dependencies || rc=$?
  exit "$rc"
fi

# ---------------------------------------------------------------------------
# Real run (orchestrator, with RustFS). Executors must not reach here.
# ---------------------------------------------------------------------------

SERVER_PID=""
# One log per server instance: the restart must not truncate the pre-kill
# instance's output, which is where a flush killed mid-write would show.
SERVER_LOG="$(chaos_logfile ingest-flush server-pre-kill)"
SERVER_RESTART_LOG="$(chaos_logfile ingest-flush server-post-kill)"
FIXTURE_PATH="$(mktemp --suffix=.pb)"
INFLIGHT_FIXTURE_PATH="$(mktemp --suffix=.pb)"
INFLIGHT_TOKENS_PATH="$(mktemp)"
INFLIGHT_PID=""

cleanup() {
  # The pending exit status, read before anything else can replace it. A
  # failure in here must not replace it either, with 3 through the ERR trap
  # or with its own status through set -e.
  local code=$?
  trap - ERR
  set +e
  local pid
  for pid in "$SERVER_PID" "${INFLIGHT_PID:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  chaos_release_logs "$code" "$SERVER_LOG" "$SERVER_RESTART_LOG"
  rm -f "$FIXTURE_PATH" "${INFLIGHT_FIXTURE_PATH:-}" "${INFLIGHT_TOKENS_PATH:-}"
  rustfs_down
}
trap cleanup EXIT

start_server_bg() {
  # Launch the server in the background, writing to the log file $1, and
  # capture its PID for a later SIGKILL. mapfile reads the NUL-delimited argv
  # emitted by ravel_server_cmd.
  local logfile="$1"
  local argv=()
  mapfile -d '' -t argv < <(ravel_server_cmd \
    --store s3 \
    --listen-http "$HTTP_ADDR" \
    --listen-grpc "$GRPC_ADDR" \
    --tenant-token "${CHAOS_TENANT_TOKEN}=${CHAOS_TENANT_NAME}")
  "${argv[@]}" >"$logfile" 2>&1 &
  SERVER_PID=$!
}

server_reachable() {
  curl --silent --fail --max-time 2 \
    -H "Authorization: Bearer ${CHAOS_TENANT_TOKEN}" \
    "${BASE_URL}/api/v1/query?query=up" >/dev/null 2>&1
}

log "bringing up RustFS and qualifying the store"
rustfs_up

log "generating OTLP fixtures"
chaos_gen_fixture > "$FIXTURE_PATH"

log "starting ravel-server (pre-kill instance)"
start_server_bg "$SERVER_LOG"
chaos_wait_for "server to accept connections" 60 server_reachable

log "driving ${EXPORT_COUNT} strict-ack exports"
# Commit tokens are opaque base64 strings, not integers (see
# oracle_strict_ack_implies_durable in lib.sh): collect them verbatim and never
# compare or increment them numerically. An export that flushed through more
# than one shard returns one token per shard; each is an acked-before-kill
# write the oracle re-queries after restart.
ACKED_TOKENS=()
for _ in $(seq 1 "$EXPORT_COUNT"); do
  tokens="$(drive_one_export "$HTTP_ADDR" "$FIXTURE_PATH")" || {
    log "export failed before kill; aborting scenario setup"
    exit 3
  }
  mapfile -t export_tokens <<<"$tokens"
  ACKED_TOKENS+=("${export_tokens[@]}")
done
log "recorded ${#ACKED_TOKENS[@]} strict-ack commit token(s) before kill"

# Every strict-acked export has already flushed, so a flush to land in needs
# one more write. The baseline is read after the acked exports and before the
# in-flight one, so the rise is that export's flush, whatever triggered it.
chaos_gen_fixture "$CHAOS_FIXTURE_SERIES" "$CHAOS_FIXTURE_POINTS" > "$INFLIGHT_FIXTURE_PATH"
FLUSH_BASELINE="$(flush_attempts "$BASE_URL")" || FLUSH_BASELINE=""
[[ "$FLUSH_BASELINE" =~ ^[0-9]+$ ]] || FLUSH_BASELINE=0
log "flush attempts before the in-flight export: ${FLUSH_BASELINE}"
chaos_start_background_export "$HTTP_ADDR" "$INFLIGHT_FIXTURE_PATH" "$INFLIGHT_TOKENS_PATH"
INFLIGHT_PID="$CHAOS_BG_EXPORT_PID"

FLUSH_OBSERVED=0
log "waiting for a flush to start (mid-flush trigger: flush attempts > ${FLUSH_BASELINE})"
# The SIGKILL follows the detecting poll with no scrape in between: a flush
# can finish in less time than one scrape takes.
if wait_for_flush_started "$BASE_URL" "$FLUSH_BASELINE" 60; then
  FLUSH_OBSERVED=1
  sigkill_pid "$SERVER_PID"
  log "flush observed in flight at ${CHAOS_FLUSH_ATTEMPTS_SEEN} attempts -- SIGKILL issued"
else
  sigkill_pid "$SERVER_PID"
  log "no flush observed within budget; SIGKILL issued anyway (writes still strict-acked)"
  echo "::warning::chaos scenario 1 killed the server without observing a flush in flight; this run did not exercise a mid-flush kill"
fi
SERVER_PID=""
ATTEMPTS_AT_DETECTION="${CHAOS_FLUSH_ATTEMPTS_SEEN:-unknown}"

# The in-flight export ends once the server is gone. An exit code outside
# CHAOS_CURL_UNANSWERED_CODES means the server answered it before the kill
# (curl's 22 is an HTTP error status, 429 or 503 among them), so the kill did
# not land mid-flush; a success is an acked write like the others.
INFLIGHT_RC=0
wait "$INFLIGHT_PID" || INFLIGHT_RC=$?
INFLIGHT_PID=""
INFLIGHT_ANSWERED="$(chaos_inflight_answered "$INFLIGHT_RC")"
log "in-flight export exit code ${INFLIGHT_RC} (answered=${INFLIGHT_ANSWERED})"
if [[ "$INFLIGHT_RC" -eq 0 && -s "$INFLIGHT_TOKENS_PATH" ]]; then
  mapfile -t inflight_tokens < "$INFLIGHT_TOKENS_PATH"
  ACKED_TOKENS+=("${inflight_tokens[@]}")
  log "the in-flight export was acknowledged before the kill; its ${#inflight_tokens[@]} token(s) join the acked set"
fi
KILL_TIMING="$(kill_timing_line "$FLUSH_OBSERVED" "$INFLIGHT_ANSWERED" "$FLUSH_BASELINE" "$ATTEMPTS_AT_DETECTION")"
log "$KILL_TIMING"

log "restarting ravel-server (post-kill instance)"
start_server_bg "$SERVER_RESTART_LOG"
chaos_wait_for "server to accept connections after restart" 60 server_reachable

# ---- Oracle (each pinned assertion independently) ----
oracle_strict_ack_implies_durable \
  "$HTTP_ADDR" "$SERIES" "${ACKED_TOKENS[@]}" || true
oracle_custody_and_catalog_verify_clean "$CHAOS_TENANT_NAME" 4 || true

# Scenario 1 is not release-blocking by ADR-0077 section 4 (that clause is
# scenario 2), but a strict-ack violation here is the exit-criterion failure
# for the ingest row; report it as an ordinary oracle failure.
echo "$KILL_TIMING"
summary_rc=0
print_oracle_summary "scenario 1: kill ingest mid-flush" normal || summary_rc=$?
exit "$summary_rc"
