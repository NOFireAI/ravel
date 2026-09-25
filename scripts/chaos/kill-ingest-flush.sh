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
# MID-FLUSH TRIGGER (named explicitly, per the task): the counter
# `ravel_ingest_flushes_by_size_total`, scraped from the server's /metrics.
# It is incremented at flush ATTEMPT time
# (crates/ravel-ingest/src/span_metrics.rs), so an increment past the
# pre-load baseline means a flush has STARTED. The SIGKILL fires the moment
# it rises, landing the kill inside the flush window.
#
# --check / --dry-run validates structure and dependencies WITHOUT starting
# RustFS, driving load, or issuing a real kill. That is the only proof
# available in an environment with no object store; a real end-to-end run is
# the orchestrator's job (executors have no object store -- ADR-0077
# section 4).
#
# Gate-shell discipline: see scripts/chaos/lib.sh header.
set -euo pipefail

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
# Number of strict-ack exports to drive before the kill. Each returns a
# commit token recorded as an acked-before-kill write.
EXPORT_COUNT="${CHAOS_EXPORT_COUNT:-20}"

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

if [[ "$MODE" == "check" ]]; then
  echo "== kill-ingest-flush.sh --check (scenario 1: kill ingest mid-flush) =="
  echo "mid-flush trigger marker: ${CHAOS_FLUSH_METRIC} (attempt-time increment)"
  rc=0
  check_dependencies || rc=$?
  exit "$rc"
fi

# ---------------------------------------------------------------------------
# Real run (orchestrator, with RustFS). Executors must not reach here.
# ---------------------------------------------------------------------------

SERVER_PID=""
SERVER_LOG="$(mktemp)"
FIXTURE_PATH="$(mktemp --suffix=.pb)"

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -f "$SERVER_LOG" "$FIXTURE_PATH"
  rustfs_down
}
trap cleanup EXIT

start_server_bg() {
  # Launch the server in the background and capture its PID for a later
  # SIGKILL. mapfile reads the NUL-delimited argv emitted by ravel_server_cmd.
  local argv=()
  mapfile -d '' -t argv < <(ravel_server_cmd \
    --store s3 \
    --listen-http "$HTTP_ADDR" \
    --listen-grpc "$GRPC_ADDR" \
    --tenant-token "${CHAOS_TENANT_TOKEN}=${CHAOS_TENANT_NAME}")
  "${argv[@]}" >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
}

server_reachable() {
  curl --silent --fail --max-time 2 \
    -H "Authorization: Bearer ${CHAOS_TENANT_TOKEN}" \
    "${BASE_URL}/api/v1/query?query=up" >/dev/null 2>&1
}

log "bringing up RustFS and qualifying the store"
rustfs_up

log "generating OTLP fixture"
cargo run --quiet -p ravel-server --example gen_otlp_fixture > "$FIXTURE_PATH"

log "starting ravel-server (pre-kill instance)"
start_server_bg
chaos_wait_for "server to accept connections" 60 server_reachable

# Record the flush baseline before driving load, so "flush started" is a
# rise past this value.
FLUSH_BASELINE="$(metric_value "$BASE_URL" "$CHAOS_FLUSH_METRIC")" || FLUSH_BASELINE=0
[[ "${FLUSH_BASELINE%.*}" =~ ^[0-9]+$ ]] || FLUSH_BASELINE=0

log "driving ${EXPORT_COUNT} strict-ack exports"
# Commit tokens are opaque base64 strings, not integers (see
# oracle_strict_ack_implies_durable in lib.sh): collect them verbatim and never
# compare or increment them numerically. Each is an acked-before-kill write the
# oracle re-queries after restart.
ACKED_TOKENS=()
for _ in $(seq 1 "$EXPORT_COUNT"); do
  token="$(drive_one_export "$HTTP_ADDR" "$FIXTURE_PATH")" || {
    log "export failed before kill; aborting scenario setup"
    exit 1
  }
  ACKED_TOKENS+=("$token")
done
log "recorded ${#ACKED_TOKENS[@]} strict-ack commit token(s) before kill"

log "waiting for a flush to start (mid-flush trigger: ${CHAOS_FLUSH_METRIC})"
if wait_for_flush_started "$BASE_URL" "${FLUSH_BASELINE%.*}" 60; then
  log "flush observed in flight -- issuing SIGKILL mid-flush"
else
  log "no flush observed within budget; issuing SIGKILL anyway (writes still strict-acked)"
fi
sigkill_pid "$SERVER_PID"
SERVER_PID=""

log "restarting ravel-server (post-kill instance)"
start_server_bg
chaos_wait_for "server to accept connections after restart" 60 server_reachable

# ---- Oracle (each pinned assertion independently) ----
oracle_strict_ack_implies_durable \
  "$HTTP_ADDR" "$SERIES" "${ACKED_TOKENS[@]}" || true
oracle_custody_and_catalog_verify_clean "$CHAOS_TENANT_NAME" 4 || true

# Scenario 1 is not release-blocking by ADR-0077 section 4 (that clause is
# scenario 2), but a strict-ack violation here is the exit-criterion failure
# for the ingest row; report it as an ordinary oracle failure.
summary_rc=0
print_oracle_summary "scenario 1: kill ingest mid-flush" normal || summary_rc=$?
exit "$summary_rc"
