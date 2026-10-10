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
# MID-COMPACTION TRIGGER (named explicitly): the compaction lifecycle as
# worker A's log shows it. The SIGKILL fires once A owns a unit
# (ravel_maintain_units_owned >= 1) and its log shows unfinished compaction
# work: no "compaction record published" line
# (crates/ravel-maintain/src/publish.rs:331) yet, or none of its
# "maintenance: retention + compaction pass complete" lines with
# compacted>=1 after the last one. No per-bucket start signal exists to read,
# so this cannot tell a merge in progress from a pass still in its retention
# or sweep work; see compaction_unfinished_in_log in lib.sh. Nor can it tell
# a first unit being merged from a pass with nothing to compact, so when A
# published nothing before the kill and the survivor publishes nothing after
# it, conservation reports could-not-measure rather than a failure.
#
# SEAL WAIT: an ingest hour is compactable only once sealed, at the end of
# the hour plus the catalog's seal margin (CatalogConfig::default(), logged
# by the server as `seal_margin_secs=N`; 4800 s today). After the exports the
# ingest server is stopped with SIGTERM, so nothing writes into a later hour.
# A write can still land after the last successful ack (a strict write
# answered 503 `Abandoned` is stored by a later flush, see
# docs/consistency-model.md, and the shutdown flushes too), so the
# wait is measured from when the server has exited: the scenario sleeps
# until (floor(exit / 3600) + 1) * 3600 + seal_margin + 120 s before starting
# the maintain workers, up to about 2 h 22 min at the current margin.
#
# Exit status: 0 when every pinned oracle assertion held; 2 when any failed
# (release-blocking); 3 on a setup error, when the server log carries no
# parseable `seal_margin_secs=`, or when conservation could not be measured
# and no other assertion failed. Could-not-measure means the survivor
# published no compaction record, its conservation-abort counter did not
# rise, and worker A's log at the kill shows its last publish followed by a
# finished pass (oracle_conservation_or_unmeasured in lib.sh); 64 on a usage
# error.
#
# --check / --dry-run validates structure and dependencies WITHOUT starting
# RustFS, spawning workers, or issuing a real kill. That is the only proof
# available with no object store; a real run is the orchestrator's job.
#
# Gate-shell discipline: see scripts/chaos/lib.sh header.
set -eEuo pipefail
# -E carries the ERR trap below into functions, so a setup command that fails
# inside a helper exits 3 like one at top level rather than with its own
# status, which could be the 2 reserved for a release-blocking oracle failure.
# The oracle calls below carry `|| true` and never reach it, and the summary
# passes `blocking`, so the oracle path itself exits only 0 or 2.
trap 'exit 3' ERR

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

# An ingest server drives the load that creates compactable buckets before the
# maintain workers start. Its own address, distinct from either worker's.
INGEST_HTTP="${CHAOS_INGEST_HTTP:-127.0.0.1:14318}"
INGEST_GRPC="${CHAOS_INGEST_GRPC:-127.0.0.1:14317}"

# Number of strict-ack exports to drive into the ingest server so the bucket
# carries real, compactable data for the maintain workers to own and compact.
# Each carries CHAOS_FIXTURE_SERIES x CHAOS_FIXTURE_POINTS points (lib.sh).
EXPORT_COUNT="${CHAOS_EXPORT_COUNT}"

# Total ownable units across the world under test; the survivor must own all
# of them after takeover. Sized by the load the setup drives (tenants x
# signals x shards). Overridable so the orchestrator can match its fixture.
EXPECTED_TOTAL_UNITS="${CHAOS_EXPECTED_TOTAL_UNITS:-4}"

usage() {
  cat <<'EOF'
Usage: kill-maintain-worker.sh [--check|--dry-run] [--help]

  --check, --dry-run   Validate structure and dependencies only. Does NOT
                       start RustFS, spawn workers, or issue a real kill -9.
  --help               Show this help.

With no flag, runs the full scenario against a real RustFS with two maintain
workers (orchestrator-only; executors have no object store and must use
--check).

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

# Refuse a bad tenant-hash mode before anything starts: the start sites read
# ravel_server_cmd through a process substitution, which drops its status.
chaos_tenant_hash_args || exit 64

if [[ "$MODE" == "check" ]]; then
  echo "== kill-maintain-worker.sh --check (scenario 2: kill maintain worker mid-compaction) =="
  echo "liveness bound: 3*H + one tick = $(chaos_takeover_bound_seconds)s" \
    "(H=${CHAOS_H_SECONDS}s, tick=${CHAOS_MAINTAIN_TICK_SECONDS}s)"
  echo "mid-compaction trigger marker: units_owned>=1 and no '${CHAOS_COMPACTION_PASS_MARKER}' (compacted>=1) after the last '${CHAOS_COMPACTION_PUBLISH_MARKER}'"
  echo "unreferenced-part horizon: ${CHAOS_PROTECTION_HORIZON_SECONDS}s (24h protection_horizon)"
  echo "load: ${EXPORT_COUNT} exports of ${CHAOS_FIXTURE_SERIES} series x ${CHAOS_FIXTURE_POINTS} points"
  echo "seal wait: end of the ingest server's exit hour + seal_margin_secs from the ingest log + ${CHAOS_SEAL_SAFETY_SECONDS}s"
  rc=0
  check_dependencies || rc=$?
  exit "$rc"
fi

# ---------------------------------------------------------------------------
# Real run (orchestrator, with RustFS). Executors must not reach here.
# ---------------------------------------------------------------------------

WORKER_A_PID=""
WORKER_B_PID=""
INGEST_PID=""
WORKER_A_LOG="$(chaos_logfile maintain-worker worker-a)"
WORKER_B_LOG="$(chaos_logfile maintain-worker worker-b)"
INGEST_LOG="$(chaos_logfile maintain-worker ingest)"
FIXTURE_PATH="$(mktemp --suffix=.pb)"

cleanup() {
  # The pending exit status, read before anything else can replace it. A
  # failure in here must not replace a pending exit 2 either, with 3 through
  # the ERR trap or with its own status through set -e.
  local code=$?
  trap - ERR
  set +e
  local pid
  for pid in "$WORKER_A_PID" "$WORKER_B_PID" "$INGEST_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  chaos_release_logs "$code" "$WORKER_A_LOG" "$WORKER_B_LOG" "$INGEST_LOG"
  rm -f "$FIXTURE_PATH"
  rustfs_down
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

# Start an ingest-role server (default mode) in the background, capturing its
# PID so the trap can reap it. This is the server the load below is POSTed to.
start_ingest_bg() {
  local argv=()
  mapfile -d '' -t argv < <(ravel_server_cmd \
    --store s3 \
    --listen-http "$INGEST_HTTP" \
    --listen-grpc "$INGEST_GRPC" \
    --tenant-token "${CHAOS_TENANT_TOKEN}=${CHAOS_TENANT_NAME}")
  "${argv[@]}" >"$INGEST_LOG" 2>&1 &
  INGEST_PID=$!
}

ingest_reachable() {
  curl --silent --fail --max-time 2 \
    -H "Authorization: Bearer ${CHAOS_TENANT_TOKEN}" \
    "http://${INGEST_HTTP}/api/v1/query?query=up" >/dev/null 2>&1
}

log "bringing up RustFS and qualifying the store"
rustfs_up

# Actually drive the generated load: start an ingest server and POST the
# fixture through it so the bucket carries real, compactable data. A prior
# version generated the fixture and never sent it, then commented that it
# "assumes the bucket already carries compactable data" -- so the maintain
# workers below owned nothing, no compaction ever ran, and every oracle was
# vacuous. The maintain workers read the sealed inputs this load produces.
log "starting ingest server (${INGEST_HTTP}) and driving ${EXPORT_COUNT} exports"
start_ingest_bg
chaos_wait_for "ingest server to accept connections" 60 ingest_reachable
# A fresh fixture per export: identical bytes would carry identical
# timestamps, and deduplication would fold the repeats into one export's data.
SENT=0
LAST_ACK_UNIX_S=""
for _ in $(seq 1 "$EXPORT_COUNT"); do
  chaos_gen_fixture "$CHAOS_FIXTURE_SERIES" "$CHAOS_FIXTURE_POINTS" > "$FIXTURE_PATH"
  if drive_one_export "$INGEST_HTTP" "$FIXTURE_PATH" >/dev/null; then
    SENT=$(( SENT + 1 ))
    LAST_ACK_UNIX_S="$(chaos_now_unix_s)"
  fi
done
# Every export must land: the seal wait is paid for a known compaction input,
# and a rejected export (a 429 under the ingest byte rate, say) would shrink
# it with nothing else in the output to say so.
if [[ "$SENT" -lt "$EXPORT_COUNT" ]]; then
  log "only ${SENT}/${EXPORT_COUNT} exports were accepted; the compaction input is short"
  exit 3
fi
log "sent ${SENT}/${EXPORT_COUNT} strict-ack exports into the ingest server (last ack at unix ${LAST_ACK_UNIX_S})"

# Stop ingest before the wait, so nothing writes into a later hour while the
# scenario waits for the hours it wrote to seal.
log "stopping the ingest server before the seal wait"
kill "$INGEST_PID" 2>/dev/null || true
wait "$INGEST_PID" 2>/dev/null || true
INGEST_PID=""
# Its last write, including any shutdown flush, is no later than its exit.
INGEST_EXIT_UNIX_S="$(chaos_now_unix_s)"
log "ingest server exited at unix ${INGEST_EXIT_UNIX_S} (last successful ack at unix ${LAST_ACK_UNIX_S})"

SEAL_MARGIN_S="$(chaos_seal_margin_from_log "$(cat "$INGEST_LOG")")" || {
  log "the ingest server log carries no parseable 'seal_margin_secs=N' line (${INGEST_LOG}); cannot compute when the written hours seal"
  exit 3
}
SEALED_AT="$(chaos_sealed_at_unix_s "$INGEST_EXIT_UNIX_S" "$SEAL_MARGIN_S")"
log "seal_margin_secs=${SEAL_MARGIN_S} (from the ingest log), safety ${CHAOS_SEAL_SAFETY_SECONDS}s"
chaos_wait_until_sealed "$SEALED_AT"

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

# The >= comparison in the two takeover oracles below only discriminates while
# the store holds this scenario's tenant alone. See
# `assert_single_tenant_universe` in lib.sh for why. Checked before the kill,
# because a violated precondition otherwise reports PASS, and before the wait,
# so nothing runs between the in-flight observation and the kill.
assert_single_tenant_universe || true

log "waiting for worker A to be mid-compaction"
# Timestamp the kill so the takeover oracle can measure wall-clock against the
# 3*H + tick bound. `date +%s` is the real clock ADR-0077 section 4 requires.
if wait_for_compaction_in_flight "http://${WORKER_A_HTTP}" "$WORKER_A_LOG" 120; then
  KILL_EPOCH="$(date +%s)"
  sigkill_pid "$WORKER_A_PID"
  log "worker A observed mid-compaction -- SIGKILL issued"
else
  KILL_EPOCH="$(date +%s)"
  sigkill_pid "$WORKER_A_PID"
  log "did not observe worker A mid-compaction within budget; SIGKILL issued anyway"
  echo "::warning::chaos scenario 2 killed worker A without observing it mid-compaction; the takeover oracles still run, but this run did not exercise a mid-compaction kill"
fi
WORKER_A_PID=""
B_LOG_LINES_AT_KILL="$(chaos_line_count "$WORKER_B_LOG")"
log "worker A max units_owned seen while waiting: ${CHAOS_A_MAX_UNITS_OWNED:-0}"
log "$(chaos_worker_activity_line worker-a "$WORKER_A_LOG")"

# ---- Oracle (each pinned assertion independently, survivor = worker B) ----
oracle_sibling_takeover_within_bound "$WORKER_B_URL" "$EXPECTED_TOTAL_UNITS" "$KILL_EPOCH" || true
oracle_no_orphaned_lease "$WORKER_B_URL" "$EXPECTED_TOTAL_UNITS" || true
# Worker A is dead, so its log is its state at the kill: whether it shows
# unfinished compaction work decides whether a survivor that published
# nothing is a failure or could not be measured.
oracle_conservation_or_unmeasured "$WORKER_A_LOG" \
  "$WORKER_B_URL" "$CONS_BASELINE" "$WORKER_B_LOG" "$B_LOG_LINES_AT_KILL" || true
oracle_no_partial_output_leak "$WORKER_B_URL" "$BREAKER_BASELINE" || true
log "$(chaos_worker_activity_line worker-b "$WORKER_B_LOG")"
log "$(chaos_worker_activity_line worker-b-after-kill "$WORKER_B_LOG" "$B_LOG_LINES_AT_KILL")"
oracle_custody_and_catalog_verify_clean "$CHAOS_TENANT_NAME" 4 || true

# Release-blocking: pass `blocking` so a failure exits 2 and prints the
# RELEASE-BLOCKING severity line. A could-not-measure run exits 3, and only
# when nothing else failed, so it never hides a real oracle failure.
summary_rc=0
print_oracle_summary "scenario 2: kill maintain worker mid-compaction" blocking || summary_rc=$?
exit "$summary_rc"
