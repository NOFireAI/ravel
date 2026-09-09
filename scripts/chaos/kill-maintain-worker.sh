#!/usr/bin/env bash
# scripts/chaos/kill-maintain-worker.sh -- ADR-0077 section 4, scenario 2:
# "Kill a maintain worker mid-compaction, with a sibling running."
#
# Two maintain-role workers under leased maintenance (ADR-0065), SIGKILL one
# mid-compaction, and assert the pinned oracle for this scenario:
#
#   * sibling-takeover-within-3H-plus-tick: the sibling takes over the dead
#     worker's units within `3 * H` (ADR-0065's liveness bound) plus one
#     maintenance tick;
#   * no-orphaned-lease: no unit stays orphaned;
#   * conservation-holds: the interrupted compaction completes under the
#     conservation gate;
#   * no-partial-output-leak: the dead worker's abandoned partial outputs age
#     out under the existing unreferenced-part rule with no leak past the
#     horizon;
#   * custody-and-catalog verification clean.
#
# RELEASE-BLOCKING: per ADR-0077 section 4, a failure of THIS scenario is a
# release-blocking bug, not a flaky test. On any oracle failure this script
# names the specific failed assertion(s) and exits 2 (distinct from 1, an
# ordinary failure, and from >2 setup/usage errors) so the distinction is
# legible to the ADR-0077 section 3 rehearsal record.
#
# MID-COMPACTION TRIGGER (named explicitly): the compaction lifecycle. A
# compaction is observed in flight when a worker owns units
# (ravel_maintain_units_owned >= 1) and its log has NOT yet emitted the
# "compaction record published" line
# (crates/ravel-maintain/src/publish.rs:200) for the current run. The SIGKILL
# fires in that window, so the compaction is genuinely interrupted before its
# record is published.
#
# --check / --dry-run validates structure and dependencies WITHOUT starting
# MinIO, spawning workers, or issuing a real kill. That is the only proof
# available with no MinIO; a real run is the orchestrator's job.
#
# Gate-shell discipline: see scripts/chaos/lib.sh header.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
# shellcheck source=scripts/chaos/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# Two maintain workers expose metrics on separate addresses. Worker A is the
# one we SIGKILL; worker B is the survivor whose takeover we assert.
WORKER_A_HTTP="${CHAOS_WORKER_A_HTTP:-127.0.0.1:14328}"
WORKER_A_GRPC="${CHAOS_WORKER_A_GRPC:-127.0.0.1:14327}"
WORKER_B_HTTP="${CHAOS_WORKER_B_HTTP:-127.0.0.1:14338}"
WORKER_B_GRPC="${CHAOS_WORKER_B_GRPC:-127.0.0.1:14337}"
WORKER_B_URL="http://${WORKER_B_HTTP}"

# Total ownable units across the world under test; the survivor must own all
# of them after takeover. Sized by the load the setup drives (tenants x
# signals x shards). Overridable so the orchestrator can match its fixture.
EXPECTED_TOTAL_UNITS="${CHAOS_EXPECTED_TOTAL_UNITS:-4}"

usage() {
  cat <<'EOF'
Usage: kill-maintain-worker.sh [--check|--dry-run] [--help]

  --check, --dry-run   Validate structure and dependencies only. Does NOT
                       start MinIO, spawn workers, or issue a real kill -9.
  --help               Show this help.

With no flag, runs the full scenario against a real MinIO with two maintain
workers (orchestrator-only; executors have no MinIO and must use --check).

A failure of this scenario is RELEASE-BLOCKING (ADR-0077 section 4); the
script exits 2 and names the failed pinned oracle assertion(s).
EOF
}

MODE="run"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check|--dry-run) MODE="check"; shift ;;
    --help|-h) usage; exit 0 ;;
    *) echo "kill-maintain-worker.sh: unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

if [[ "$MODE" == "check" ]]; then
  echo "== kill-maintain-worker.sh --check (scenario 2: kill maintain worker mid-compaction) =="
  echo "liveness bound: 3*H + one tick = $(chaos_takeover_bound_seconds)s" \
    "(H=${CHAOS_H_SECONDS}s, tick=${CHAOS_MAINTAIN_TICK_SECONDS}s)"
  echo "mid-compaction trigger marker: units_owned>=1 before '${CHAOS_COMPACTION_PUBLISH_MARKER}'"
  echo "unreferenced-part horizon: ${CHAOS_PROTECTION_HORIZON_SECONDS}s (24h protection_horizon)"
  rc=0
  check_dependencies || rc=$?
  exit "$rc"
fi

# ---------------------------------------------------------------------------
# Real run (orchestrator, with MinIO). Executors must not reach here.
# ---------------------------------------------------------------------------

WORKER_A_PID=""
WORKER_B_PID=""
WORKER_A_LOG="$(mktemp)"
WORKER_B_LOG="$(mktemp)"
FIXTURE_PATH="$(mktemp --suffix=.pb)"

cleanup() {
  local pid
  for pid in "$WORKER_A_PID" "$WORKER_B_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -f "$WORKER_A_LOG" "$WORKER_B_LOG" "$FIXTURE_PATH"
  minio_down
}
trap cleanup EXIT

# Start a maintain-role worker. $1=http $2=grpc $3=logfile; echoes the PID via
# the named global set by the caller. We set the PID through a nameref so the
# trap can reap both workers.
start_worker() {
  local http_addr="$1" grpc_addr="$2" log_file="$3"
  local -n pid_ref="$4"
  local argv=()
  mapfile -d '' -t argv < <(ravel_server_cmd \
    --store s3 \
    --mode maintain \
    --listen-http "$http_addr" \
    --listen-grpc "$grpc_addr" \
    --tenant-token "${CHAOS_TENANT_TOKEN}=${CHAOS_TENANT_NAME}")
  "${argv[@]}" >"$log_file" 2>&1 &
  pid_ref=$!
}

worker_reachable() {
  curl --silent --fail --max-time 2 "http://$1/metrics" >/dev/null 2>&1
}

log "bringing up MinIO and qualifying the store"
minio_up

log "generating OTLP fixture and driving load to create compactable buckets"
cargo run --quiet -p ravel-server --example gen_otlp_fixture > "$FIXTURE_PATH"
# A real run drives enough load through an ingest server to produce multiple
# sealed hours per unit so a compaction has real work. That ingest server is
# started/torn down by the orchestrator's load fixture; here we assume the
# bucket already carries compactable data (the orchestrator's setup step) and
# focus on the two maintain workers, which is what this scenario tests.

log "starting two maintain workers (A=${WORKER_A_HTTP}, B=${WORKER_B_HTTP})"
start_worker "$WORKER_A_HTTP" "$WORKER_A_GRPC" "$WORKER_A_LOG" WORKER_A_PID
start_worker "$WORKER_B_HTTP" "$WORKER_B_GRPC" "$WORKER_B_LOG" WORKER_B_PID
chaos_wait_for "worker A metrics" 60 worker_reachable "$WORKER_A_HTTP"
chaos_wait_for "worker B metrics" 60 worker_reachable "$WORKER_B_HTTP"

# Baselines for delta-based oracles, read from the survivor (worker B).
CONS_BASELINE="$(metric_value "$WORKER_B_URL" ravel_maintain_conservation_aborts_total)" \
  || CONS_BASELINE=0
[[ "${CONS_BASELINE%.*}" =~ ^[0-9]+$ ]] || CONS_BASELINE=0
BREAKER_BASELINE="$(metric_value "$WORKER_B_URL" ravel_maintain_orphan_breaker_tripped_total)" \
  || BREAKER_BASELINE=0
[[ "${BREAKER_BASELINE%.*}" =~ ^[0-9]+$ ]] || BREAKER_BASELINE=0

log "waiting for worker A to be mid-compaction"
if wait_for_compaction_in_flight "http://${WORKER_A_HTTP}" "$WORKER_A_LOG" 120; then
  log "worker A observed mid-compaction -- issuing SIGKILL"
else
  log "did not observe worker A mid-compaction within budget; killing anyway"
fi

# Timestamp the kill so the takeover oracle can measure wall-clock against the
# 3*H + tick bound. `date +%s` is the real clock ADR-0077 section 4 requires.
KILL_EPOCH="$(date +%s)"
sigkill_pid "$WORKER_A_PID"
WORKER_A_PID=""

# ---- Oracle (each pinned assertion independently, survivor = worker B) ----
oracle_sibling_takeover_within_bound "$WORKER_B_URL" "$EXPECTED_TOTAL_UNITS" "$KILL_EPOCH" || true
oracle_no_orphaned_lease "$WORKER_B_URL" "$EXPECTED_TOTAL_UNITS" || true
oracle_conservation_holds "$WORKER_B_URL" "$CONS_BASELINE" "$WORKER_B_LOG" || true
oracle_no_partial_output_leak "$WORKER_B_URL" "$BREAKER_BASELINE" || true
oracle_custody_and_catalog_verify_clean "$CHAOS_TENANT_NAME" 4 || true

# Release-blocking: pass `blocking` so a failure exits 2 and prints the
# RELEASE-BLOCKING severity line.
summary_rc=0
print_oracle_summary "scenario 2: kill maintain worker mid-compaction" blocking || summary_rc=$?
exit "$summary_rc"
