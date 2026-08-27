#!/usr/bin/env bash
# scripts/dr-rehearsal/restore-rehearsal.sh -- issue #814 disaster recovery
# rehearsal: restore a replica into an empty bucket, stop writers before
# reconciling, run custody -> reconstruct -> catalog -> canary in that order,
# and record RTO/RPO as artifacts.
#
# This operationalizes docs/guides/disaster-recovery.md's restore procedure
# steps 1-4 with measurement (docs/adrs/0077-dr-posture-and-chaos-evidence.md
# section 3's rehearsal-record discipline: no RPO/RTO number may be published
# ahead of a real measured rehearsal). Steps 5 (Resume) and 6 (Re-protect)
# are the operator's job after this reports clean; this script never starts
# a long-lived service against the restore bucket, only a temporary
# query-mode instance for the canary check, and only after reconciliation is
# mechanically proven complete (see dr_require_reconciliation_complete in
# lib.sh -- a stamp file check, not script order).
#
# --check / --dry-run validates structure and dependencies without starting
# MinIO, copying anything, or reconciling anything: the only proof available
# with no MinIO/S3 reachable (this repository's fleet executors have none).
# A real run needs a human-or-CI-supplied MinIO/S3 endpoint and credentials;
# see the "What a human must supply" section of the issue #814 final report.
#
# --inject-fault=dangling-commit-record|missing-data|canary-query-error lets
# an operator or CI prove the workflow actually fails on each of the three
# acceptance-criterion faults, not just run clean.
#
# Gate-shell discipline: see scripts/dr-rehearsal/lib.sh header.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
# shellcheck source=scripts/dr-rehearsal/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
  cat <<'EOF'
Usage: restore-rehearsal.sh [--check|--dry-run] [OPTIONS]

  --check, --dry-run          Validate structure and dependencies only. Does
                               NOT start MinIO, copy anything, or reconcile.
  --confirm-writers-stopped   Required for a real run: explicit operator
                               acknowledgement that ingest writers against the
                               primary are stopped (deliverable 2).
  --primary-http HOST:PORT    Optional: best-effort check that the primary is
                               actually unreachable before reconciling.
  --tenant NAME                Default: dr-rehearsal-tenant (or $DR_TENANT_NAME).
  --shards N                   Default: 4 (or $DR_SHARDS).
  --artifact-dir DIR            Where rehearsal-report.json and the
                               reconciliation stamp are written. Default: a
                               fresh mktemp -d (never inside this repo: the
                               fleet harness auto-commits with git add -A).
  --inject-fault FAULT         One of dangling-commit-record, missing-data,
                               canary-query-error. Corrupts the restore (test
                               hook, proves the workflow goes red on each).
  --help                       Show this help.

A failure at any reconciliation stage exits nonzero and names the stage.
EOF
}

MODE="run"
CONFIRM_WRITERS_STOPPED=0
PRIMARY_HTTP=""
TENANT="$DR_TENANT_NAME"
SHARDS="$DR_SHARDS"
ARTIFACT_DIR=""
INJECT_FAULT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check|--dry-run) MODE="check"; shift ;;
    --confirm-writers-stopped) CONFIRM_WRITERS_STOPPED=1; shift ;;
    --primary-http) PRIMARY_HTTP="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --shards) SHARDS="$2"; shift 2 ;;
    --artifact-dir) ARTIFACT_DIR="$2"; shift 2 ;;
    --inject-fault) INJECT_FAULT="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "restore-rehearsal.sh: unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

if [[ "$MODE" == "check" ]]; then
  echo "== restore-rehearsal.sh --check (issue #814 DR rehearsal) =="
  echo "order: restore-bucket-empty -> writers-stopped -> restore-copy ->" \
    "custody -> reconstruct -> catalog -> [stamp] -> canary"
  echo "the stamp file gates canary/service-start mechanically" \
    "(dr_require_reconciliation_complete), not by statement order"
  rc=0
  check_dependencies || rc=$?
  exit "$rc"
fi

if [[ "$INJECT_FAULT" != "" && "$INJECT_FAULT" != "dangling-commit-record" \
   && "$INJECT_FAULT" != "missing-data" && "$INJECT_FAULT" != "canary-query-error" ]]; then
  echo "restore-rehearsal.sh: unknown --inject-fault value: ${INJECT_FAULT}" >&2
  exit 64
fi

if [[ -z "$ARTIFACT_DIR" ]]; then
  ARTIFACT_DIR="$(mktemp -d)"
fi
mkdir -p "$ARTIFACT_DIR"
REPORT_PATH="${ARTIFACT_DIR}/rehearsal-report.json"
STAMP_PATH="${ARTIFACT_DIR}/reconciliation-complete.stamp"
NONCE="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
rm -f "$STAMP_PATH"

QUERY_PID=""
# shellcheck disable=SC2034  # written via dr_start_query_server's nameref, not read directly
QUERY_LOG=""
# shellcheck disable=SC2317  # invoked indirectly via the EXIT trap below
cleanup() {
  dr_stop_query_server "$QUERY_PID"
  minio_down
}
trap cleanup EXIT

log "artifacts: ${ARTIFACT_DIR}"

# ---------------------------------------------------------------------------
# Freeze: writers stopped before ANY reconciliation step runs (deliverable 2).
# ---------------------------------------------------------------------------
FREEZE_EPOCH="$(date +%s)"
if ! dr_require_writers_stopped "$CONFIRM_WRITERS_STOPPED" "$PRIMARY_HTTP"; then
  dr_write_report "$REPORT_PATH" 0 0 fail "${STEP_LOG[@]}"
  exit 1
fi

log "bringing up MinIO and ensuring buckets"
minio_up

# The replica bucket stands in for whatever the operator's own bucket-level
# replication has already produced (ADR-0077 decision 1: Ravel owns none of
# that). For canary-query-error, the fault is that the expected canary data
# never made it to the replica in the first place (see lib.sh's fault-
# injection header comment): skip seeding rather than corrupting the copy.
if [[ "$INJECT_FAULT" == "canary-query-error" ]]; then
  log "inject-fault=canary-query-error: skipping replica seed on purpose"
else
  dr_seed_replica
fi

# ---------------------------------------------------------------------------
# Deliverable 1: restore into an EMPTY bucket.
# ---------------------------------------------------------------------------
if ! dr_require_restore_bucket_empty "$DR_RESTORE_BUCKET"; then
  dr_write_report "$REPORT_PATH" 0 0 fail "${STEP_LOG[@]}"
  exit 1
fi

if ! dr_restore_copy "$DR_REPLICA_BUCKET" "$DR_RESTORE_BUCKET"; then
  dr_write_report "$REPORT_PATH" 0 0 fail "${STEP_LOG[@]}"
  exit 1
fi

RPO_LOST_OBJECTS="$(dr_measure_rpo_lost_objects "$DR_REPLICA_BUCKET" "$DR_RESTORE_BUCKET")"
log "RPO measurement: ${RPO_LOST_OBJECTS} object(s) lost between replica and restore copy"

if [[ -n "$INJECT_FAULT" && "$INJECT_FAULT" != "canary-query-error" ]]; then
  if ! dr_inject_fault "$INJECT_FAULT" "$DR_RESTORE_BUCKET"; then
    log "fault injection itself failed to apply -- treating as a hard failure"
    dr_write_report "$REPORT_PATH" 0 "$RPO_LOST_OBJECTS" fail "${STEP_LOG[@]}"
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Deliverable 3: reconciliation, in order, before service starts.
# ---------------------------------------------------------------------------
reconciliation_rc=0
dr_run_reconciliation "$TENANT" "$SHARDS" || reconciliation_rc=$?

RTO_SECONDS_SO_FAR=$(( $(date +%s) - FREEZE_EPOCH ))
if [[ "$reconciliation_rc" -ne 0 ]]; then
  log "reconciliation failed (exit ${reconciliation_rc}): refusing to start any query surface"
  dr_write_report "$REPORT_PATH" "$RTO_SECONDS_SO_FAR" "$RPO_LOST_OBJECTS" fail "${STEP_LOG[@]}"
  exit "$reconciliation_rc"
fi

dr_stamp_reconciliation_complete "$STAMP_PATH" "$NONCE"

# ---------------------------------------------------------------------------
# Deliverable 3 (canary): mechanically gated on the stamp, not on being
# reached next in the script.
# ---------------------------------------------------------------------------
if ! dr_require_reconciliation_complete "$STAMP_PATH" "$NONCE"; then
  dr_write_report "$REPORT_PATH" "$RTO_SECONDS_SO_FAR" "$RPO_LOST_OBJECTS" fail "${STEP_LOG[@]}"
  exit 1
fi

dr_start_query_server "$DR_QUERY_HTTP" QUERY_PID QUERY_LOG "$DR_QUERY_GRPC"
if ! dr_wait_for "query server" 60 curl --silent --fail --max-time 2 "http://${DR_QUERY_HTTP}/metrics"; then
  step_bad "canary" "query-mode server against ${DR_RESTORE_BUCKET} never became reachable"
  dr_write_report "$REPORT_PATH" "$RTO_SECONDS_SO_FAR" "$RPO_LOST_OBJECTS" fail "${STEP_LOG[@]}"
  exit 1
fi

canary_rc=0
dr_run_canary "$DR_QUERY_HTTP" "$DR_CANARY_SERIES" || canary_rc=$?

RTO_SECONDS="$(( $(date +%s) - FREEZE_EPOCH ))"

if [[ "$canary_rc" -ne 0 ]]; then
  dr_write_report "$REPORT_PATH" "$RTO_SECONDS" "$RPO_LOST_OBJECTS" fail "${STEP_LOG[@]}"
  exit "$canary_rc"
fi

log "RTO measurement: ${RTO_SECONDS}s from freeze to canary-clean"
dr_write_report "$REPORT_PATH" "$RTO_SECONDS" "$RPO_LOST_OBJECTS" pass "${STEP_LOG[@]}"
echo "restore-rehearsal: PASS (rto_seconds=${RTO_SECONDS} rpo_lost_objects=${RPO_LOST_OBJECTS})"
echo "report: ${REPORT_PATH}"
exit 0
