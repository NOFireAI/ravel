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
# MID-COMPACTION TRIGGER (named explicitly, issue #3037): the victim is the
# PREFERRED worker (CHAOS_PREFERRED_VICTIM, default the parity of
# GITHUB_RUN_NUMBER -- see chaos_default_preferred_victim in lib.sh), killed
# as soon as its own ravel_maintain_compactions_in_flight gauge reads >= 1.
# Both workers are scraped every round, the preferred worker last, so the
# SIGKILL follows the detecting scrape with nothing but bookkeeping between:
# a bucket merge has started (gates passed, the claim held where the run
# takes one, commit records read) and has not yet published, failed, or been
# abandoned. Before issue #3037 the victim was whichever worker's gauge fired
# FIRST: the rendezvous-hash split let worker A start its first merge several
# seconds before B in every observed run since #3019, so the B-victim path
# almost never ran. The other worker is never killed. A preferred worker that
# owns no data-holding metrics unit never merges, so the scenario runs on
# CHAOS_SHARD_COUNT=8 shards: with data in all 8 metrics shards, the chance a
# given worker owns none of them is 1/256 (it was 1/16 on 4 shards), and the
# run checks after the load that all 8 hold data (exit 3 otherwise).
# Each worker is scraped every CHAOS_COMPACTION_POLL_SECONDS (0.05 s) plus the
# scrape itself; the run logs the scrapes it made and the seconds it waited,
# and exactly one line of the form
# `VICTIM preferred=<a|b> chosen=<a|b> reason=<preferred|no-gauge>`,
# where no-gauge means the preferred worker gave no reading >= 1 within one
# maintenance tick plus 120 s. On no-gauge the preferred worker is still
# killed, with a warning, CHAOS_KILL_ON_GAUGE is 0, and conservation is
# could-not-measure (exit 3 when nothing else failed).
#
# CONSERVATION: after the kill, the victim's log names every bucket it was
# merging: each "compaction merge started" line with no "compaction merge
# ended" line for the same bucket. A worker runs several units at once, so
# more than one can be open. The oracle waits up to
# chaos_conservation_wait_seconds from the kill (the takeover bound, one more
# tick, and CHAOS_CONSERVATION_MERGE_ALLOWANCE_SECONDS; 1080 s at the
# defaults), ending the wait early once the survivor has logged a publish for
# every open bucket, then lists `<bucket>l1.` in the store for each: a
# compaction record there counts, whichever worker wrote it. With the gauge
# seen and the buckets known, any open bucket with no record by then is a
# FAIL. Without the gauge or an open bucket, or with a LIST that fails while
# every listed bucket has a record, the run could not measure conservation.
# It also logs the merge durations it can pair from both workers' logs and,
# per open bucket, whether the survivor started and published it.
#
# SEAL WAIT: an ingest hour is compactable once the compactor's seal margin
# has passed the end of the hour (CompactorConfig::seal_margin_ns, 3900 s at
# the defaults, not logged). The wait uses the catalog fold's larger margin,
# which the server logs as `seal_margin_secs=N` (4800 s), an upper bound while
# --gc-max-flush-lifetime is at its default; see the seal-wait block in
# lib.sh. After the exports the
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
# parseable `seal_margin_secs=`, when fewer than CHAOS_SHARD_COUNT metrics
# shards hold data after the load (or the LIST counting them fails), or
# when conservation could not be measured
# and no other assertion failed. The two takeover oracles run unconditionally
# (issue #3037 deliverable 2): an earlier version skipped them when the
# survivor's peak units_owned during the wait was already at or above
# EXPECTED_TOTAL_UNITS, guarding against a stale total that could pass
# vacuously, but EXPECTED_TOTAL_UNITS now derives, by default, from the shard
# count actually passed to both workers' --shards (CHAOS_SHARD_COUNT, issue
# #3037 nit), so absent a CHAOS_EXPECTED_TOTAL_UNITS override it equals the
# true total of ownable units. `oracle_sibling_takeover_within_bound` checks
# that the survivor's units_owned reaches that total within the ADR-0065
# bound after the kill with no unit stalled; that shows a takeover only if
# the victim held units at the kill, which it assumes. The run logs the
# survivor's last units_owned reading before the kill so a reader can see it
# was below the total. For conservation,
# could-not-measure means the survivor's conservation-abort counter did not
# rise and either the kill did not fire on the gauge, the victim's log at the
# kill names no interrupted bucket, or a store LIST of an interrupted
# bucket's compaction records failed while every bucket it could list had a
# record (oracle_conservation_or_unmeasured in lib.sh). It also exits 3
# before the kill when fewer than
# CHAOS_MAINTAIN_MIN_EXPORTS exports of the load were accepted; 64 on a usage
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

# Two maintain workers expose metrics on separate addresses. The preferred one
# (CHAOS_PREFERRED_VICTIM, below), once observed mid-compaction
# (wait_for_compaction_in_flight_preferred, below), is the victim we SIGKILL;
# the other is the survivor whose takeover we assert.
WORKER_A_HTTP="${CHAOS_WORKER_A_HTTP:-127.0.0.1:14328}"
WORKER_A_GRPC="${CHAOS_WORKER_A_GRPC:-127.0.0.1:14327}"
WORKER_B_HTTP="${CHAOS_WORKER_B_HTTP:-127.0.0.1:14338}"
WORKER_B_GRPC="${CHAOS_WORKER_B_GRPC:-127.0.0.1:14337}"
WORKER_A_URL="http://${WORKER_A_HTTP}"
WORKER_B_URL="http://${WORKER_B_HTTP}"

# An ingest server drives the load that creates compactable buckets before the
# maintain workers start. Its own address, distinct from either worker's.
INGEST_HTTP="${CHAOS_INGEST_HTTP:-127.0.0.1:14318}"
INGEST_GRPC="${CHAOS_INGEST_GRPC:-127.0.0.1:14317}"

# Number of strict-ack exports to drive into the ingest server so the bucket
# carries real, compactable data for the maintain workers to own and compact.
# Each carries CHAOS_MAINTAIN_FIXTURE_SERIES x CHAOS_MAINTAIN_FIXTURE_POINTS
# points; see lib.sh for the sizing.
EXPORT_COUNT="${CHAOS_MAINTAIN_EXPORT_COUNT}"

# Total ownable units across the world under test; the survivor must own all
# of them after takeover. A unit is one (signal, shard) pair for the chaos
# tenant: MAINTAINED_SIGNALS in services/ravel-server/src/maintain.rs lists 3
# signals (Metrics, Logs, Spans) today, crossed with CHAOS_SHARD_COUNT shards,
# giving 24 -- not all data-bearing, since only Metrics carries load, but all
# ownable and so all subject to takeover. CHAOS_EXPECTED_TOTAL_UNITS
# overrides the derived total so the orchestrator can match its fixture.
# 8 shards, so that each worker almost always owns a data-holding metrics
# unit (see MID-COMPACTION TRIGGER above).
CHAOS_MAINTAINED_SIGNALS="${CHAOS_MAINTAINED_SIGNALS:-3}"
CHAOS_SHARD_COUNT="${CHAOS_SHARD_COUNT:-8}"
if [[ ! "${CHAOS_SHARD_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  echo "kill-maintain-worker.sh: CHAOS_SHARD_COUNT must be a positive whole number, got '${CHAOS_SHARD_COUNT}'" >&2
  exit 64
fi
if [[ ! "${CHAOS_MAINTAINED_SIGNALS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "kill-maintain-worker.sh: CHAOS_MAINTAINED_SIGNALS must be a positive whole number, got '${CHAOS_MAINTAINED_SIGNALS}'" >&2
  exit 64
fi
EXPECTED_TOTAL_UNITS="${CHAOS_EXPECTED_TOTAL_UNITS:-$(( CHAOS_MAINTAINED_SIGNALS * CHAOS_SHARD_COUNT ))}"
# A total of 0 would let both takeover oracles pass on their first scrape.
if [[ ! "${EXPECTED_TOTAL_UNITS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "kill-maintain-worker.sh: the expected total of units must be a positive whole number, got '${EXPECTED_TOTAL_UNITS}'" >&2
  exit 64
fi

# Which worker scenario 2 prefers to kill (issue #3037): explicit
# CHAOS_PREFERRED_VICTIM, else alternating by GITHUB_RUN_NUMBER parity (see
# chaos_default_preferred_victim in lib.sh).
if [[ -n "${CHAOS_PREFERRED_VICTIM:-}" ]]; then
  case "${CHAOS_PREFERRED_VICTIM}" in
    a|b) ;;
    *)
      echo "kill-maintain-worker.sh: CHAOS_PREFERRED_VICTIM must be 'a' or 'b', got '${CHAOS_PREFERRED_VICTIM}'" >&2
      exit 64
      ;;
  esac
else
  CHAOS_PREFERRED_VICTIM="$(chaos_default_preferred_victim)"
fi

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
  echo "mid-compaction trigger: the preferred worker is killed as soon as its own ravel_maintain_compactions_in_flight reads >=1, both scraped every ${CHAOS_COMPACTION_POLL_SECONDS}s plus the scrape, the preferred worker last"
  echo "victim preference: CHAOS_PREFERRED_VICTIM=${CHAOS_PREFERRED_VICTIM} (killed on its first reading >=1 within $(( CHAOS_MAINTAIN_TICK_SECONDS + 120 ))s, else killed with reason=no-gauge and conservation could-not-measure; the other worker is never killed), override with CHAOS_PREFERRED_VICTIM=a|b, default the parity of GITHUB_RUN_NUMBER"
  echo "shard count: CHAOS_SHARD_COUNT=${CHAOS_SHARD_COUNT} passed to the ingest server's and both workers' --shards, multiplied into the expected total below, and required to hold data in every metrics shard after the load (exit 3 otherwise)"
  echo "conservation wait: up to ${CHAOS_CONSERVATION_WAIT_SECONDS:-$(chaos_conservation_wait_seconds)}s after the kill (takeover bound + one tick + ${CHAOS_CONSERVATION_MERGE_ALLOWANCE_SECONDS}s merge allowance, ended early by a survivor '${CHAOS_COMPACTION_PUBLISH_MARKER}'), then a store LIST of <bucket>l1. for a compaction record of every bucket an unended '${CHAOS_COMPACTION_START_MARKER}' in the victim's log names"
  echo "unreferenced-part horizon: ${CHAOS_PROTECTION_HORIZON_SECONDS}s (24h protection_horizon)"
  echo "load: ${EXPORT_COUNT} exports of ${CHAOS_MAINTAIN_FIXTURE_SERIES} series x ${CHAOS_MAINTAIN_FIXTURE_POINTS} points, at most one per ${CHAOS_MAINTAIN_EXPORT_INTERVAL_SECONDS}s, stopped at ${CHAOS_MAINTAIN_LOAD_MAX_SECONDS}s, at least ${CHAOS_MAINTAIN_MIN_EXPORTS} accepted to go on"
  echo "seal wait: end of the ingest server's exit hour + seal_margin_secs from the ingest log + ${CHAOS_SEAL_SAFETY_SECONDS}s"
  echo "expected total units: ${EXPECTED_TOTAL_UNITS} (${CHAOS_MAINTAINED_SIGNALS} signals x ${CHAOS_SHARD_COUNT} shards, override with CHAOS_EXPECTED_TOTAL_UNITS)"
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
PACE_PID=""
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
  for pid in "$WORKER_A_PID" "$WORKER_B_PID" "$INGEST_PID" "$PACE_PID"; do
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
    --tenant-token "${CHAOS_TENANT_TOKEN}=${CHAOS_TENANT_NAME}" \
    --shards "$CHAOS_SHARD_COUNT")
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
    --tenant-token "${CHAOS_TENANT_TOKEN}=${CHAOS_TENANT_NAME}" \
    --shards "$CHAOS_SHARD_COUNT")
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
ATTEMPTED=0
LAST_ACK_UNIX_S=""
LOAD_START_S="$(date +%s)"
# The pacing sleep runs beside each export, so an iteration lasts at least
# CHAOS_MAINTAIN_EXPORT_INTERVAL_SECONDS. A strict-ack export normally takes
# longer than that (see the sizing note in lib.sh), so this is a floor that
# keeps the load under the ingest byte rate, not the load's pace. The load
# stops at CHAOS_MAINTAIN_LOAD_MAX_SECONDS whatever count it has reached, so
# a slower ingest shrinks the load instead of pushing the job past its
# timeout with no verdict.
for _ in $(seq 1 "$EXPORT_COUNT"); do
  if (( $(date +%s) - LOAD_START_S >= CHAOS_MAINTAIN_LOAD_MAX_SECONDS )); then
    log "load deadline: ${CHAOS_MAINTAIN_LOAD_MAX_SECONDS}s reached after ${ATTEMPTED}/${EXPORT_COUNT} exports"
    break
  fi
  sleep "$CHAOS_MAINTAIN_EXPORT_INTERVAL_SECONDS" &
  PACE_PID=$!
  chaos_gen_fixture "$CHAOS_MAINTAIN_FIXTURE_SERIES" "$CHAOS_MAINTAIN_FIXTURE_POINTS" > "$FIXTURE_PATH"
  ATTEMPTED=$(( ATTEMPTED + 1 ))
  if drive_one_export "$INGEST_HTTP" "$FIXTURE_PATH" >/dev/null; then
    SENT=$(( SENT + 1 ))
    LAST_ACK_UNIX_S="$(chaos_now_unix_s)"
  fi
  wait "$PACE_PID"
  PACE_PID=""
done
LOAD_SECONDS=$(( $(date +%s) - LOAD_START_S ))
# The oracles key on the merges that actually ran, not on the export count,
# so a short or partly rejected load is reported and the scenario goes on.
# Below CHAOS_MAINTAIN_MIN_EXPORTS accepted exports (the 20-export load that
# did compact in nightly run 38050587878) there may be nothing to merge, and
# the scenario stops as could-not-measure.
if [[ "$SENT" -lt "$CHAOS_MAINTAIN_MIN_EXPORTS" ]]; then
  log "only ${SENT}/${ATTEMPTED} exports were accepted in ${LOAD_SECONDS}s, below the ${CHAOS_MAINTAIN_MIN_EXPORTS} minimum; the compaction input is too short"
  exit 3
fi
if [[ "$SENT" -lt "$ATTEMPTED" ]]; then
  log "warning: $(( ATTEMPTED - SENT ))/${ATTEMPTED} exports were rejected"
fi
log "sent ${SENT}/${EXPORT_COUNT} strict-ack exports (${ATTEMPTED} attempted) in ${LOAD_SECONDS}s, $(awk -v s="$LOAD_SECONDS" -v n="$ATTEMPTED" 'BEGIN { if (n > 0) printf "%.2f", s / n; else printf "n/a" }')s each (last ack at unix ${LAST_ACK_UNIX_S})"

# Stop ingest before the wait, so nothing writes into a later hour while the
# scenario waits for the hours it wrote to seal.
log "stopping the ingest server before the seal wait"
kill "$INGEST_PID" 2>/dev/null || true
wait "$INGEST_PID" 2>/dev/null || true
INGEST_PID=""
# Its last write, including any shutdown flush, is no later than its exit.
INGEST_EXIT_UNIX_S="$(chaos_now_unix_s)"
log "ingest server exited at unix ${INGEST_EXIT_UNIX_S} (last successful ack at unix ${LAST_ACK_UNIX_S})"

# Checked before the seal wait, so a load that missed a shard costs minutes,
# not hours.
coverage_rc=0
chaos_metrics_shard_coverage "$CHAOS_SHARD_COUNT" || coverage_rc=$?
if [[ "$coverage_rc" -ne 0 ]]; then
  log "could not measure: scenario 2 needs data in all ${CHAOS_SHARD_COUNT} metrics shards so each worker owns a data-holding unit"
  exit 3
fi

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

# Baselines for delta-based oracles, read from BOTH workers before the kill;
# chaos_select_survivor picks the survivor's pair after the kill.
CONS_BASELINE_A="$(metric_value "$WORKER_A_URL" ravel_maintain_conservation_aborts_total)" \
  || CONS_BASELINE_A=0
[[ "${CONS_BASELINE_A%.*}" =~ ^[0-9]+$ ]] || CONS_BASELINE_A=0
CONS_BASELINE_B="$(metric_value "$WORKER_B_URL" ravel_maintain_conservation_aborts_total)" \
  || CONS_BASELINE_B=0
[[ "${CONS_BASELINE_B%.*}" =~ ^[0-9]+$ ]] || CONS_BASELINE_B=0
BREAKER_BASELINE_A="$(metric_value "$WORKER_A_URL" ravel_maintain_orphan_breaker_tripped_total)" \
  || BREAKER_BASELINE_A=0
[[ "${BREAKER_BASELINE_A%.*}" =~ ^[0-9]+$ ]] || BREAKER_BASELINE_A=0
BREAKER_BASELINE_B="$(metric_value "$WORKER_B_URL" ravel_maintain_orphan_breaker_tripped_total)" \
  || BREAKER_BASELINE_B=0
[[ "${BREAKER_BASELINE_B%.*}" =~ ^[0-9]+$ ]] || BREAKER_BASELINE_B=0

# The >= comparison in the two takeover oracles below only discriminates while
# the store holds this scenario's tenant alone. See
# `assert_single_tenant_universe` in lib.sh for why. Checked before the kill,
# because a violated precondition otherwise reports PASS, and before the wait,
# so nothing runs between the in-flight observation and the kill.
assert_single_tenant_universe || true

# Timestamp the kill so the takeover oracle can measure wall-clock against the
# 3*H + tick bound. `date +%s` is the real clock ADR-0077 section 4 requires.
# The whole wait is one maintenance tick plus two minutes, so each worker runs
# at least one pass after its startup pass inside it.
INFLIGHT_BUDGET_SECONDS=$(( CHAOS_MAINTAIN_TICK_SECONDS + 120 ))
log "waiting up to ${INFLIGHT_BUDGET_SECONDS}s for the preferred worker (worker-${CHAOS_PREFERRED_VICTIM}) to be mid-compaction"
INFLIGHT_WAIT_START="$(date +%s)"
if wait_for_compaction_in_flight_preferred "$CHAOS_PREFERRED_VICTIM" "$WORKER_A_URL" "$WORKER_B_URL" "$INFLIGHT_BUDGET_SECONDS"; then
  KILL_EPOCH="$(date +%s)"
  VICTIM_PID_VAR="WORKER_${CHAOS_VICTIM^^}_PID"
  sigkill_pid "${!VICTIM_PID_VAR}"
  CHAOS_KILL_ON_GAUGE=1
else
  KILL_EPOCH="$(date +%s)"
  VICTIM_PID_VAR="WORKER_${CHAOS_VICTIM^^}_PID"
  sigkill_pid "${!VICTIM_PID_VAR}"
  CHAOS_KILL_ON_GAUGE=0
fi
echo "VICTIM preferred=${CHAOS_PREFERRED_VICTIM} chosen=${CHAOS_VICTIM} reason=${CHAOS_VICTIM_REASON}"
if [[ "$CHAOS_KILL_ON_GAUGE" -eq 1 ]]; then
  log "victim: worker-${CHAOS_VICTIM} (gauge, ${CHAOS_VICTIM_REASON})"
else
  log "victim: worker-${CHAOS_VICTIM} (no-gauge, no reading >= 1 from it within ${INFLIGHT_BUDGET_SECONDS}s)"
  echo "::warning::chaos scenario 2 killed worker-${CHAOS_VICTIM} without a live mid-compaction reading from it; the takeover oracles still run, but this run did not exercise a mid-compaction kill"
fi

# Select victim/survivor identity now that CHAOS_VICTIM is known, and use
# these EVERYWHERE below instead of a hardcoded worker.
chaos_select_survivor "$CHAOS_VICTIM"
SURVIVOR_LOG_LINES_AT_KILL="$(chaos_line_count "$SURVIVOR_LOG")"
log "worker-a max units_owned seen while waiting: ${CHAOS_A_MAX_UNITS_OWNED:-0}; worker-b max units_owned seen while waiting: ${CHAOS_B_MAX_UNITS_OWNED:-0}; /metrics scrapes: ${CHAOS_COMPACTION_POLLS:-0} in $(( KILL_EPOCH - INFLIGHT_WAIT_START ))s"
log "${SURVIVOR_LABEL} units_owned at its last scrape before the kill: ${SURVIVOR_LAST_UNITS_OWNED} of ${EXPECTED_TOTAL_UNITS}"
log "$(chaos_worker_activity_line "$VICTIM_LABEL" "$VICTIM_LOG")"
log "kill on gauge: ${CHAOS_KILL_ON_GAUGE}; ${VICTIM_LABEL}'s interrupted buckets: $(chaos_interrupted_buckets "$VICTIM_LOG" | tr '\n' ' ' || true)"

# ---- Oracle (each pinned assertion independently, survivor = $SURVIVOR_LABEL) ----
# Run both takeover oracles unconditionally (issue #3037 deliverable 2): a
# prior version skipped them when the survivor's peak units_owned during the
# wait was already at or above EXPECTED_TOTAL_UNITS, guarding against a
# vacuous pass against a stale total (the bug it guarded: a 4-unit default
# compared against a survivor that already owned 6 of the tenant's real 12
# units pre-kill). EXPECTED_TOTAL_UNITS now derives, by default, from
# CHAOS_SHARD_COUNT, the same value passed to both workers' --shards, so
# absent a CHAOS_EXPECTED_TOTAL_UNITS override it equals the true total of
# ownable units for this run. oracle_sibling_takeover_within_bound polls the
# survivor's units_owned after the kill until it reaches that total within
# the ADR-0065 bound, then requires no stalled unit. A survivor that already
# owned every unit would pass it with no takeover, so both takeover oracles
# run only when the survivor's last pre-kill reading is a number below the
# total (chaos_takeover_measurable); otherwise they are recorded as could
# not measure.
if chaos_takeover_measurable "$SURVIVOR_LAST_UNITS_OWNED" "$EXPECTED_TOTAL_UNITS"; then
  oracle_sibling_takeover_within_bound "$SURVIVOR_URL" "$EXPECTED_TOTAL_UNITS" "$KILL_EPOCH" || true
  oracle_no_orphaned_lease "$SURVIVOR_URL" "$EXPECTED_TOTAL_UNITS" || true
else
  ORACLE_UNMEASURED+=("sibling-takeover-within-3H-plus-tick: could not measure: ${SURVIVOR_LABEL} owned ${SURVIVOR_LAST_UNITS_OWNED} of ${EXPECTED_TOTAL_UNITS} units at its last scrape before the kill, so reaching the total would not show a takeover")
  ORACLE_UNMEASURED+=("no-orphaned-lease: could not measure: ${SURVIVOR_LABEL} owned ${SURVIVOR_LAST_UNITS_OWNED} of ${EXPECTED_TOTAL_UNITS} units before the kill")
fi
# The victim is dead, so its log is its state at the kill: every bucket it had
# open, read from it, must have a compaction record in the store. The oracle
# also reads CHAOS_KILL_ON_GAUGE, set above.
oracle_conservation_or_unmeasured "$VICTIM_LOG" \
  "$SURVIVOR_URL" "$CONS_BASELINE" "$SURVIVOR_LOG" "$SURVIVOR_LOG_LINES_AT_KILL" "$KILL_EPOCH" || true
oracle_no_partial_output_leak "$SURVIVOR_URL" "$BREAKER_BASELINE" || true
log "$(chaos_worker_activity_line "$SURVIVOR_LABEL" "$SURVIVOR_LOG")"
log "$(chaos_worker_activity_line "${SURVIVOR_LABEL}-after-kill" "$SURVIVOR_LOG" "$SURVIVOR_LOG_LINES_AT_KILL")"
oracle_custody_and_catalog_verify_clean "$CHAOS_TENANT_NAME" "$CHAOS_SHARD_COUNT" || true

# Release-blocking: pass `blocking` so a failure exits 2 and prints the
# RELEASE-BLOCKING severity line. A could-not-measure run exits 3, and only
# when nothing else failed, so it never hides a real oracle failure.
summary_rc=0
print_oracle_summary "scenario 2: kill maintain worker mid-compaction" blocking || summary_rc=$?
exit "$summary_rc"
