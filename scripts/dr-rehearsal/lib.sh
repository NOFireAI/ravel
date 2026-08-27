#!/usr/bin/env bash
# scripts/dr-rehearsal/lib.sh -- shared library for the issue #814 disaster
# recovery rehearsal: restore a bucket-level replica into an empty bucket and
# prove reconciliation before service starts.
#
# This operationalizes steps 1-4 of the restore procedure in
# docs/guides/disaster-recovery.md and docs/adrs/0077-dr-posture-and-chaos-
# evidence.md ("Freeze" -> "Choose restore bucket" -> "Reconcile" -> "Verify
# before serving"). Steps 5 ("Resume") and 6 ("Re-protect") are the operator's
# job after this rehearsal reports clean; this script never starts a
# long-lived service against the restore bucket.
#
# Ravel ships no in-product backup/restore (ADR-0077 decision 1): the copy
# from replica to restore bucket is an operator/platform-CLI action (`mc
# mirror` here, standing in for whatever object-storage-native replication or
# copy tool a real deployment uses), never a Ravel binary. This script drives
# that copy plus the three Ravel-owned reconciliation tools in the mandated
# order and a bounded canary-query check.
#
# Gate-shell discipline (CLAUDE.md "Writing gate and poll shell"): exit codes
# are captured as `cmd || rc=$?` on the command's own line, never read after
# an if/fi block; no variable is named status/path/argv/PWD; no gate command
# is piped through grep/head/tail or followed by `&& echo MARKER`.
#
# shellcheck shell=bash

# ---------------------------------------------------------------------------
# Configuration (mirrors scripts/chaos/lib.sh and scripts/demo.sh).
# ---------------------------------------------------------------------------

DR_MINIO_ENDPOINT="${DR_MINIO_ENDPOINT:-http://127.0.0.1:9000}"

export RAVEL_S3_ENDPOINT="${RAVEL_S3_ENDPOINT:-$DR_MINIO_ENDPOINT}"
export RAVEL_S3_REGION="${RAVEL_S3_REGION:-us-east-1}"
export RAVEL_S3_ACCESS_KEY="${RAVEL_S3_ACCESS_KEY:-ravel}"
export RAVEL_S3_SECRET_KEY="${RAVEL_S3_SECRET_KEY:-ravel-dev-secret}"

DR_REPLICA_BUCKET="${DR_REPLICA_BUCKET:-ravel-dr-replica}"
DR_RESTORE_BUCKET="${DR_RESTORE_BUCKET:-ravel-dr-restore}"

DR_TENANT_TOKEN="${DR_TENANT_TOKEN:-dr-rehearsal-token}"
DR_TENANT_NAME="${DR_TENANT_NAME:-dr-rehearsal-tenant}"

# The metric name scripts/demo.sh's `gen_otlp_fixture` example emits (see
# services/ravel-server/examples/gen_otlp_fixture.rs). Reused as the canary
# series rather than adding a second fixture generator: the rehearsal only
# needs one series it knows was ingested before the freeze.
DR_CANARY_SERIES="${DR_CANARY_SERIES:-demo_requests_total}"

DR_QUERY_HTTP="${DR_QUERY_HTTP:-127.0.0.1:14418}"
DR_QUERY_GRPC="${DR_QUERY_GRPC:-127.0.0.1:14417}"

DR_SHARDS="${DR_SHARDS:-4}"

DR_MINIO_COMPOSE="${DR_MINIO_COMPOSE:-deploy/docker-compose/minio.yml}"
DR_STARTED_MINIO=0

# ---------------------------------------------------------------------------
# Logging and step bookkeeping.
#
# Unlike scripts/chaos/lib.sh's pinned-oracle panel (independent assertions
# read once at the end), a restore rehearsal is a strict pipeline: custody,
# then reconstruct, then catalog, then canary, each gating the next (deliverable
# 3). STEP_LOG records what ran and its outcome, purely for the artifact
# report; it is not what enforces ordering (dr_require_reconciliation_complete
# below is).
# ---------------------------------------------------------------------------

STEP_LOG=()

log() {
  echo "[dr-rehearsal] $*" >&2
}

step_ok() {
  STEP_LOG+=("PASS $1")
  log "PASS: $1"
}

step_bad() {
  STEP_LOG+=("FAIL $1: $2")
  log "FAIL: $1 -- $2"
}

# Run a command, capturing its exit code on the command's own line, and
# return that code.
run_capture() {
  local rc=0
  "$@" || rc=$?
  return "$rc"
}

dr_have_command() {
  command -v "$1" >/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# Binary launch helpers (same PATH-or-cargo-run pattern as scripts/chaos/lib.sh).
# ---------------------------------------------------------------------------

ravel_cli() {
  if dr_have_command ravel-cli; then
    ravel-cli "$@"
  else
    cargo run --quiet -p ravel-cli -- "$@"
  fi
}

ravel_server_cmd() {
  if dr_have_command ravel-server; then
    printf '%s\0' ravel-server "$@"
  else
    printf '%s\0' cargo run --quiet -p ravel-server -- "$@"
  fi
}

# ---------------------------------------------------------------------------
# Structural / dependency validation (--check, never touches infra).
# ---------------------------------------------------------------------------

DR_REAL_RUN_TOOLS=(curl docker jq)

dr_minio_client_available() {
  if dr_have_command mc; then
    return 0
  fi
  if dr_have_command docker; then
    return 0
  fi
  return 1
}

dr_ravel_binaries_available() {
  if dr_have_command ravel-server && dr_have_command ravel-cli; then
    return 0
  fi
  if dr_have_command cargo; then
    return 0
  fi
  return 1
}

# Validate structure and dependencies without starting MinIO, copying
# anything, or reconciling anything. Mirrors scripts/chaos/lib.sh's
# check_dependencies: absent real-run tools WARN (not FAIL) unless
# DR_CHECK_STRICT=1, so --check passes in a MinIO-less executor.
check_dependencies() {
  local strict="${DR_CHECK_STRICT:-0}"
  local defects=0
  local warnings=0
  local tool

  echo "---- structural checks ----"

  local fn
  for fn in \
    dr_bucket_object_count \
    dr_require_writers_stopped \
    dr_stamp_reconciliation_complete \
    dr_require_reconciliation_complete \
    dr_run_reconciliation \
    dr_run_canary; do
    if declare -F "$fn" >/dev/null 2>&1; then
      echo "  OK    function defined: ${fn}"
    else
      echo "  FAIL  function MISSING: ${fn}"
      defects=$(( defects + 1 ))
    fi
  done

  echo "---- real-run dependency checks (WARN unless DR_CHECK_STRICT=1) ----"

  for tool in "${DR_REAL_RUN_TOOLS[@]}"; do
    if dr_have_command "$tool"; then
      echo "  OK    tool present: ${tool}"
    else
      echo "  WARN  tool absent (needed for a real run): ${tool}"
      warnings=$(( warnings + 1 ))
    fi
  done

  if dr_minio_client_available; then
    echo "  OK    MinIO client path available (mc or docker)"
  else
    echo "  WARN  no MinIO client (mc) and no docker: a real run cannot manage MinIO"
    warnings=$(( warnings + 1 ))
  fi

  if dr_ravel_binaries_available; then
    echo "  OK    ravel binaries runnable (prebuilt or via cargo)"
  else
    echo "  WARN  ravel-server/ravel-cli not on PATH and no cargo: cannot build/run binaries"
    warnings=$(( warnings + 1 ))
  fi

  echo "-----------------------------------------------------------"
  if [[ "$defects" -gt 0 ]]; then
    echo "check: FAIL -- ${defects} structural defect(s)"
    return 1
  fi
  if [[ "$strict" == "1" && "$warnings" -gt 0 ]]; then
    echo "check: FAIL -- ${warnings} missing real-run dependency(ies) (DR_CHECK_STRICT=1)"
    return 1
  fi
  if [[ "$warnings" -gt 0 ]]; then
    echo "check: PASS (structure well-formed; ${warnings} real-run dependency warning(s))"
  else
    echo "check: PASS (structure well-formed; all dependencies present)"
  fi
  return 0
}

# ---------------------------------------------------------------------------
# MinIO / bucket helpers (real-run only).
# ---------------------------------------------------------------------------

dr_mc() {
  docker run --rm --network host \
    -e "MC_HOST_local=http://${RAVEL_S3_ACCESS_KEY}:${RAVEL_S3_SECRET_KEY}@127.0.0.1:9000" \
    minio/mc:latest "$@"
}

dr_minio_healthy() {
  curl --silent --fail --max-time 2 "${DR_MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1
}

dr_wait_for() {
  local description="$1"
  local attempts="$2"
  shift 2
  local attempt
  # shellcheck disable=SC2034  # attempt is the loop counter, not read in the body
  for attempt in $(seq 1 "$attempts"); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done
  log "timed out waiting for ${description}"
  return 1
}

# Bring MinIO up (idempotent) and ensure both the replica and restore buckets
# exist. Mirrors scripts/chaos/lib.sh's minio_up. Never called in --check
# mode. Only creates buckets; seeding the replica bucket with rehearsal data
# is the caller's job (dr_seed_replica below), because a real rehearsal
# starts from a replica object storage already populated by the operator's
# own replication configuration (ADR-0077 decision 1), which this script does
# not own.
minio_up() {
  if dr_minio_healthy; then
    log "MinIO already running at ${DR_MINIO_ENDPOINT}"
  else
    log "starting MinIO via docker compose"
    docker compose -f "$DR_MINIO_COMPOSE" up -d
    DR_STARTED_MINIO=1
    dr_wait_for "MinIO to become healthy" 30 dr_minio_healthy || return 1
  fi

  local bucket
  for bucket in "$DR_REPLICA_BUCKET" "$DR_RESTORE_BUCKET"; do
    log "ensuring bucket ${bucket} exists"
    dr_mc mb -p "local/${bucket}" >/dev/null 2>&1 || true
  done
  return 0
}

minio_down() {
  if [[ "$DR_STARTED_MINIO" -eq 1 ]]; then
    log "stopping MinIO"
    docker compose -f "$DR_MINIO_COMPOSE" down >/dev/null 2>&1 || true
    DR_STARTED_MINIO=0
  fi
}

# Seed the replica bucket with one canary series via a real ingest server, so
# the rehearsal has known-ingested data to restore and query back. Stands in
# for "the primary already replicated to this bucket before the freeze."
# $1=data dir the ingest server's store points at is irrelevant (S3-backed);
# starts and stops its own ingest-mode server against DR_REPLICA_BUCKET.
dr_seed_replica() {
  local http_addr="127.0.0.1:14428"
  local grpc_addr="127.0.0.1:14427"
  local fixture_path
  fixture_path="$(mktemp --suffix=.pb)"
  local log_file
  log_file="$(mktemp)"

  cargo run --quiet -p ravel-server --example gen_otlp_fixture >"$fixture_path"

  local argv=()
  RAVEL_S3_BUCKET="$DR_REPLICA_BUCKET" \
    mapfile -d '' -t argv < <(ravel_server_cmd \
      --store s3 \
      --mode all \
      --listen-http "$http_addr" \
      --listen-grpc "$grpc_addr" \
      --tenant-hash-unkeyed \
      --tenant-token "${DR_TENANT_TOKEN}=${DR_TENANT_NAME}")
  RAVEL_S3_BUCKET="$DR_REPLICA_BUCKET" "${argv[@]}" >"$log_file" 2>&1 &
  local pid=$!

  local rc=0
  if ! dr_wait_for "seed server" 60 curl --silent --fail --max-time 2 "http://${http_addr}/metrics"; then
    rc=1
  fi

  if [[ "$rc" -eq 0 ]]; then
    run_capture curl --silent --show-error --fail \
      -X POST "http://${http_addr}/v1/metrics" \
      -H "Authorization: Bearer ${DR_TENANT_TOKEN}" \
      -H "Content-Type: application/x-protobuf" \
      --data-binary "@${fixture_path}" || rc=$?
  fi

  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -f "$fixture_path" "$log_file"

  if [[ "$rc" -ne 0 ]]; then
    step_bad "seed-replica" "failed to seed the canary series into ${DR_REPLICA_BUCKET}"
    return "$rc"
  fi
  step_ok "seed-replica (${DR_REPLICA_BUCKET})"
  return 0
}

# Count objects under a bucket. Captures the listing into a variable first
# (never pipes a gate through grep); prints "0" (not an error) when the
# bucket does not exist yet, matching "not there" and "empty" the same way
# for the emptiness precondition below.
dr_bucket_object_count() {
  local bucket="$1"
  local out=""
  local rc=0
  out="$(dr_mc ls -r "local/${bucket}" 2>/dev/null)" || rc=$?
  if [[ "$rc" -ne 0 || -z "$out" ]]; then
    echo "0"
    return 0
  fi
  local count=0
  local line
  while IFS= read -r line; do
    [[ -n "$line" ]] && count=$(( count + 1 ))
  done <<<"$out"
  echo "$count"
}

# ---------------------------------------------------------------------------
# Deliverable 1: restore into an empty bucket. Refuses to mirror onto a
# restore bucket that already carries objects, so a broken restore cannot
# silently "succeed" by reading data the rehearsal never actually copied.
# ---------------------------------------------------------------------------

dr_require_restore_bucket_empty() {
  local bucket="$1"
  local count
  count="$(dr_bucket_object_count "$bucket")"
  if [[ "$count" != "0" ]]; then
    step_bad "restore-bucket-empty" \
      "${bucket} already carries ${count} object(s); refusing to restore into a non-empty bucket"
    return 1
  fi
  step_ok "restore-bucket-empty (${bucket})"
  return 0
}

# ---------------------------------------------------------------------------
# Deliverable 2: stop writers before reconciliation starts. Mechanical, not
# a comment: refuses without the explicit acknowledgement flag, and (when a
# primary address is given) refuses if that primary still answers.
# ---------------------------------------------------------------------------

dr_require_writers_stopped() {
  local confirmed="$1"
  local primary_http="$2"

  if [[ "$confirmed" != "1" ]]; then
    step_bad "writers-stopped" \
      "--confirm-writers-stopped not given; refusing to reconcile against a possibly-live primary"
    return 1
  fi

  if [[ -n "$primary_http" ]]; then
    if curl --silent --fail --max-time 2 "http://${primary_http}/metrics" >/dev/null 2>&1; then
      step_bad "writers-stopped" \
        "primary at ${primary_http} still answers /metrics: writers are not actually stopped"
      return 1
    fi
  fi

  step_ok "writers-stopped"
  return 0
}

# ---------------------------------------------------------------------------
# The restore copy itself (replica bucket -> the now-proven-empty restore
# bucket). A real object-storage copy, not a Ravel operation (ADR-0077
# decision 1: no in-product backup/restore).
# ---------------------------------------------------------------------------

dr_restore_copy() {
  local replica_bucket="$1"
  local restore_bucket="$2"
  local rc=0
  run_capture dr_mc mirror "local/${replica_bucket}" "local/${restore_bucket}" || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    step_bad "restore-copy" "mc mirror ${replica_bucket} -> ${restore_bucket} exited ${rc}"
    return "$rc"
  fi
  step_ok "restore-copy (${replica_bucket} -> ${restore_bucket})"
  return 0
}

# ---------------------------------------------------------------------------
# Deliverable 3: ordered reconciliation -- custody, then reconstruct, then
# catalog, then canary. Each step's own nonzero exit is the gate: no output
# is grepped.
# ---------------------------------------------------------------------------

dr_run_custody() {
  local tenant="$1"
  local shards="$2"
  local rc=0
  run_capture ravel_cli --store s3 maintain verify-custody \
    --tenant "$tenant" --shards "$shards" --versioning-aware || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    step_bad "custody" "ravel-cli maintain verify-custody exited ${rc} (dangling or corrupt record)"
    return "$rc"
  fi
  step_ok "custody"
  return 0
}

dr_run_reconstruct() {
  local tenant="$1"
  local shards="$2"
  local signal shard rc=0
  for signal in metrics logs; do
    for shard in $(seq 0 $(( shards - 1 ))); do
      rc=0
      run_capture ravel_cli --store s3 commit reconstruct \
        --tenant "$tenant" --signal "$signal" --shard "$shard" || rc=$?
      if [[ "$rc" -ne 0 ]]; then
        step_bad "reconstruct" \
          "ravel-cli commit reconstruct --signal ${signal} --shard ${shard} exited ${rc} (unrecoverable data object)"
        return "$rc"
      fi
    done
  done
  step_ok "reconstruct"
  return 0
}

dr_run_catalog() {
  local tenant="$1"
  local signal rc=0
  for signal in metrics logs; do
    rc=0
    run_capture ravel_cli --store s3 catalog verify --tenant "$tenant" --signal "$signal" || rc=$?
    if [[ "$rc" -ne 0 ]]; then
      step_bad "catalog" "ravel-cli catalog verify --signal ${signal} exited ${rc}"
      return "$rc"
    fi
  done
  step_ok "catalog"
  return 0
}

# Run custody, reconstruct, and catalog verification, in that order, each
# gating the next. Returns nonzero on the first failure (deliverable 3).
dr_run_reconciliation() {
  local tenant="$1"
  local shards="$2"
  dr_run_custody "$tenant" "$shards" || return $?
  dr_run_reconstruct "$tenant" "$shards" || return $?
  dr_run_catalog "$tenant" || return $?
  return 0
}

# ---------------------------------------------------------------------------
# The mechanical "no service before reconciliation" gate (acceptance
# criterion: "assert that mechanically, not by the order statements appear in
# a script"). The stamp is a nonce written only by
# dr_stamp_reconciliation_complete after dr_run_reconciliation returns 0, and
# dr_require_reconciliation_complete refuses to let the canary (or any real
# service start) proceed without a stamp file whose nonce matches this run's
# own recorded nonce -- a leftover stamp from a previous, unrelated run
# cannot satisfy a fresh rehearsal.
# ---------------------------------------------------------------------------

dr_stamp_reconciliation_complete() {
  local stamp_file="$1"
  local nonce="$2"
  printf 'dr-rehearsal-reconciliation-complete %s\n' "$nonce" >"$stamp_file"
}

dr_require_reconciliation_complete() {
  local stamp_file="$1"
  local nonce="$2"
  if [[ ! -f "$stamp_file" ]]; then
    step_bad "service-start-gate" "no reconciliation stamp at ${stamp_file}: refusing to start a query surface"
    return 1
  fi
  local recorded
  recorded="$(cat "$stamp_file")"
  if [[ "$recorded" != "dr-rehearsal-reconciliation-complete ${nonce}" ]]; then
    step_bad "service-start-gate" "stamp at ${stamp_file} does not match this run's nonce: refusing to start a query surface"
    return 1
  fi
  step_ok "service-start-gate"
  return 0
}

# ---------------------------------------------------------------------------
# Deliverable 3 (canary queries) + acceptance criterion (canary-query error
# must fail the workflow). A temporary query-mode server against the restore
# bucket, gated behind dr_require_reconciliation_complete, queried for the
# canary series known-ingested before the freeze.
# ---------------------------------------------------------------------------

dr_query_series_visible() {
  local http_addr="$1"
  local series="$2"
  local body=""
  local rc=0
  body="$(curl --silent --show-error --fail \
    -H "Authorization: Bearer ${DR_TENANT_TOKEN}" \
    --get "http://${http_addr}/api/v1/query" \
    --data-urlencode "query=${series}")" || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    return 1
  fi
  if [[ "$body" == *'"status":"success"'* && "$body" == *"$series"* ]]; then
    return 0
  fi
  return 1
}

# Run the canary query set against a running query-mode server. Behind
# dr_require_reconciliation_complete in the caller, never called directly by
# the top-level script.
dr_run_canary() {
  local http_addr="$1"
  local series="$2"
  if ! dr_query_series_visible "$http_addr" "$series"; then
    step_bad "canary" "canary series '${series}' not visible via ${http_addr}/api/v1/query"
    return 1
  fi
  step_ok "canary (${series})"
  return 0
}

# Start a temporary query-mode server against the restore bucket, ONLY after
# the caller has verified dr_require_reconciliation_complete. Echoes the PID
# via the nameref $2, log path via $3.
dr_start_query_server() {
  local http_addr="$1"
  local -n pid_ref="$2"
  local -n log_ref="$3"
  local grpc_addr="$4"
  local log_file
  log_file="$(mktemp)"
  # shellcheck disable=SC2034  # log_ref is a nameref out-param, read by the caller
  log_ref="$log_file"
  local argv=()
  mapfile -d '' -t argv < <(ravel_server_cmd \
    --store s3 \
    --mode query \
    --listen-http "$http_addr" \
    --listen-grpc "$grpc_addr" \
    --tenant-hash-unkeyed \
    --tenant-token "${DR_TENANT_TOKEN}=${DR_TENANT_NAME}")
  RAVEL_S3_BUCKET="$DR_RESTORE_BUCKET" "${argv[@]}" >"$log_file" 2>&1 &
  # shellcheck disable=SC2034  # pid_ref is a nameref out-param, read by the caller
  pid_ref=$!
}

dr_stop_query_server() {
  local pid="$1"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

# ---------------------------------------------------------------------------
# Deliverable 4: RTO/RPO measurement and artifact output. RPO here is the
# measured data-loss figure the pre-registration in
# docs/guides/disaster-recovery.md commits to: the object-count gap between
# the replica and the restore bucket immediately after the copy, before any
# reconciliation has a chance to explain or repair it. RTO is real wall-clock
# from the freeze (writers-stopped confirmed) to the moment the canary check
# passes and the reconciliation-complete stamp is written -- the point at
# which the operator's own step 5 ("Resume") is safe to start.
# ---------------------------------------------------------------------------

dr_measure_rpo_lost_objects() {
  local replica_bucket="$1"
  local restore_bucket="$2"
  local replica_count restore_count
  replica_count="$(dr_bucket_object_count "$replica_bucket")"
  restore_count="$(dr_bucket_object_count "$restore_bucket")"
  local lost=$(( replica_count - restore_count ))
  if [[ "$lost" -lt 0 ]]; then
    lost=0
  fi
  echo "$lost"
}

# Write the rehearsal-report.json artifact. $1=output path, $2=rto_seconds,
# $3=rpo_lost_objects, $4=overall result ("pass"/"fail"), remaining args are
# the STEP_LOG entries. No Date.now()-style ambient clock read happens here:
# rto_seconds is computed by the caller from its own start/end epoch marks.
dr_write_report() {
  local out_path="$1"
  local rto_seconds="$2"
  local rpo_lost_objects="$3"
  local result="$4"
  shift 4

  local steps_json="["
  local first=1
  local entry
  for entry in "$@"; do
    if [[ "$first" -eq 0 ]]; then
      steps_json+=","
    fi
    first=0
    local escaped="${entry//\\/\\\\}"
    escaped="${escaped//\"/\\\"}"
    steps_json+="\"${escaped}\""
  done
  steps_json+="]"

  cat >"$out_path" <<EOF
{
  "result": "${result}",
  "rto_seconds": ${rto_seconds},
  "rpo_lost_objects": ${rpo_lost_objects},
  "replica_bucket": "${DR_REPLICA_BUCKET}",
  "restore_bucket": "${DR_RESTORE_BUCKET}",
  "tenant": "${DR_TENANT_NAME}",
  "steps": ${steps_json}
}
EOF
  log "wrote rehearsal report to ${out_path}"
}

# ---------------------------------------------------------------------------
# Fault injection (the acceptance criterion: "prove it by injecting that
# specific fault and watching the workflow go red"). Two of the three faults
# mutate the restore bucket after the restore copy and before reconciliation
# (never the replica, so re-running without --inject-fault starts from a
# clean copy again); the third (canary-query-error) is deliberately NOT
# implemented here as a post-copy mutation. Corrupting or stranding an object
# would also trip custody or reconstruct first, which would prove those two
# tools catch it but not that the canary step independently catches anything
# they miss. Instead the top-level script skips seeding the canary series
# into the replica at all for that fault: reconciliation then has nothing
# structurally wrong to find (an empty/quiescent tenant verifies clean), and
# only the canary query -- checking that specific, known-ingested data is
# actually there -- fails. That is the case the canary step exists for: a
# restore that is structurally consistent but has silently lost data no
# schema-level check would ever notice.
#   dangling-commit-record  -> maintain verify-custody's "missing-and-
#                               unexpected" anomaly (services/ravel-cli/src/
#                               maintain.rs)
#   missing-data             -> commit reconstruct's Outcome::Failed (a
#                               record-less object whose footer will not
#                               parse)
# ---------------------------------------------------------------------------

dr_inject_fault() {
  local fault="$1"
  local restore_bucket="$2"

  case "$fault" in
    dangling-commit-record)
      # Delete one L0 data object while leaving its commit record in place:
      # the record now points at nothing, custody's exact anomaly class.
      local key
      key="$(dr_mc find "local/${restore_bucket}" --path "*/l0/*" 2>/dev/null | head -n1)" || key=""
      if [[ -z "$key" ]]; then
        log "inject dangling-commit-record: no L0 data object found to strand"
        return 1
      fi
      dr_mc rm "$key" >/dev/null 2>&1
      log "injected dangling-commit-record: removed ${key}"
      ;;
    missing-data)
      # Overwrite one L0 data object's bytes in place: its footer no longer
      # parses, so reconstruct (if no record exists) or a live read (if one
      # does) both see corrupt content. Corrupting rather than deleting keeps
      # the object present-but-unreadable, distinct from the dangling-record
      # fault above.
      local key
      key="$(dr_mc find "local/${restore_bucket}" --path "*/l0/*" 2>/dev/null | head -n1)" || key=""
      if [[ -z "$key" ]]; then
        log "inject missing-data: no L0 data object found to corrupt"
        return 1
      fi
      local tmp
      tmp="$(mktemp)"
      printf 'corrupted-by-dr-rehearsal-fault-injection' >"$tmp"
      dr_mc pipe "$key" <"$tmp" >/dev/null 2>&1
      rm -f "$tmp"
      log "injected missing-data: corrupted ${key}"
      ;;
    canary-query-error)
      log "canary-query-error is injected by the top-level script skipping" \
        "the replica seed step, not by dr_inject_fault; see the header comment"
      return 64
      ;;
    *)
      log "unknown --inject-fault value: ${fault}"
      return 64
      ;;
  esac
  return 0
}
