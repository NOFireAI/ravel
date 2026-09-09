#!/usr/bin/env bash
# scripts/chaos/lib.sh -- shared library for the ADR-0077 section 4
# process-kill chaos-evidence lane.
#
# This lane is deliberately NOT part of ravel-sim and is NOT run in PR CI
# (ADR-0077 section 4, "Rejected alternatives / Running the chaos lane in
# PR CI"). It drives real binaries, a real MinIO, real multi-threaded
# runtime, a real clock, and a real `kill -9`. Executors have no MinIO and
# must not run it end to end; an orchestrator with MinIO runs it later and
# records the result in the ADR-0077 section 3 rehearsal-record discipline.
#
# The oracle is PINNED by ADR-0077 section 4 (the harness implements it, it
# does not choose it):
#
#   strict-ack-implies-durable,
#   sibling takeover within `3 * H` + one maintenance tick,
#   no orphaned lease,
#   conservation holds,
#   custody and catalog verification clean.
#
# Each pinned assertion is a separate, independently testable function below
# (see the `oracle_*` functions), not one monolithic check, so a rehearsal
# record can name exactly which assertion failed.
#
# Gate-shell discipline (CLAUDE.md "Writing gate and poll shell"): this file
# never reads `$?` after an `if`/`fi` block (exit codes are captured as
# `cmd || rc=$?` on the command's own line via `run_capture`), never names a
# variable `status`/`path`/`argv`/`PWD` (zsh reserves them), and never pipes
# a gate command through `grep`/`head`/`tail` or appends `&& echo MARKER`
# (either would mask the gate's exit code). Metric/HTTP bodies are captured
# into a variable first and parsed from the variable, so no data-extraction
# pipeline ever sits between a gate and its exit code.
#
# shellcheck shell=bash

# ---------------------------------------------------------------------------
# Pinned constants (ADR-0065 liveness model, ADR-0077 section 4).
# ---------------------------------------------------------------------------

# ADR-0065 Decision 1: heartbeat interval `H`, default 60 s. The live set is
# every worker whose heartbeat is within `3 * H` of the reader's clock.
CHAOS_H_SECONDS="${CHAOS_H_SECONDS:-60}"

# ADR-0048/0065: the maintain supervisor tick, default 5 min. Takeover is
# bounded by `3 * H` (membership) PLUS one maintenance tick (the surviving
# worker must run a discovery cycle to pick up the newly-owned units).
CHAOS_MAINTAIN_TICK_SECONDS="${CHAOS_MAINTAIN_TICK_SECONDS:-300}"

# docs/catalog-and-mvcc.md: `protection_horizon` is 24 h; the unreferenced-part
# sweep only deletes a part whose `last_modified` age exceeds it. This is the
# horizon a dead worker's abandoned partial outputs age out under.
CHAOS_PROTECTION_HORIZON_SECONDS="${CHAOS_PROTECTION_HORIZON_SECONDS:-86400}"

# Derived takeover bound in seconds: 3 * H + one maintenance tick.
chaos_takeover_bound_seconds() {
  echo $(( 3 * CHAOS_H_SECONDS + CHAOS_MAINTAIN_TICK_SECONDS ))
}

# ---------------------------------------------------------------------------
# MinIO / server / tenancy configuration (mirrors scripts/demo.sh).
# ---------------------------------------------------------------------------

CHAOS_MINIO_COMPOSE="${CHAOS_MINIO_COMPOSE:-deploy/docker-compose/minio.yml}"
CHAOS_MINIO_ENDPOINT="${CHAOS_MINIO_ENDPOINT:-http://127.0.0.1:9000}"

export RAVEL_S3_ENDPOINT="${RAVEL_S3_ENDPOINT:-$CHAOS_MINIO_ENDPOINT}"
export RAVEL_S3_BUCKET="${RAVEL_S3_BUCKET:-ravel-chaos}"
export RAVEL_S3_REGION="${RAVEL_S3_REGION:-us-east-1}"
export RAVEL_S3_ACCESS_KEY="${RAVEL_S3_ACCESS_KEY:-ravel}"
export RAVEL_S3_SECRET_KEY="${RAVEL_S3_SECRET_KEY:-ravel-dev-secret}"

CHAOS_TENANT_TOKEN="${CHAOS_TENANT_TOKEN:-chaos-token}"
CHAOS_TENANT_NAME="${CHAOS_TENANT_NAME:-chaos-tenant}"

# ---------------------------------------------------------------------------
# Logging and oracle bookkeeping.
# ---------------------------------------------------------------------------

# Two parallel arrays record oracle outcomes. ORACLE_PASS holds the names of
# assertions that held; ORACLE_FAIL holds "name: detail" for assertions that
# failed. print_oracle_summary renders both and sets the exit status.
ORACLE_PASS=()
ORACLE_FAIL=()

log() {
  echo "[chaos] $*" >&2
}

# Record a passing oracle assertion by pinned name.
oracle_ok() {
  ORACLE_PASS+=("$1")
  log "PASS: $1"
}

# Record a failing oracle assertion by pinned name, with a one-line detail.
oracle_bad() {
  ORACLE_FAIL+=("$1: $2")
  log "FAIL: $1 -- $2"
}

# Run a command, capturing its exit code on the command's own line (never
# read after an if/fi), and return that code. Use for every gate-shaped call
# (CLI verify commands, cargo, curl) so the caller can branch on the real
# exit status instead of a masked one.
run_capture() {
  local rc=0
  "$@" || rc=$?
  return "$rc"
}

# Print the pinned-oracle summary for a scenario and set the exit status.
# Scenario 2 failures are release-blocking (ADR-0077 section 4): the caller
# passes `blocking` as $2 to make the distinction legible in output and exit
# code (2 = release-blocking oracle failure, 1 = ordinary oracle failure).
print_oracle_summary() {
  local scenario="$1"
  local severity="${2:-normal}"
  local name
  echo "================ ORACLE SUMMARY: ${scenario} ================"
  if [[ ${#ORACLE_PASS[@]} -gt 0 ]]; then
    for name in "${ORACLE_PASS[@]}"; do
      echo "  PASS  ${name}"
    done
  fi
  if [[ ${#ORACLE_FAIL[@]} -gt 0 ]]; then
    for name in "${ORACLE_FAIL[@]}"; do
      echo "  FAIL  ${name}"
    done
  fi
  echo "-----------------------------------------------------------"
  if [[ ${#ORACLE_FAIL[@]} -eq 0 ]]; then
    echo "RESULT: PASS -- all pinned oracle assertions held (${scenario})"
    echo "Paste this block into the ADR-0077 section 3 rehearsal record."
    return 0
  fi
  # Name the failed assertions again, compactly, for the rehearsal record.
  echo "RESULT: FAIL -- ${#ORACLE_FAIL[@]} pinned oracle assertion(s) failed:"
  for name in "${ORACLE_FAIL[@]}"; do
    echo "  - ${name%%:*}"
  done
  if [[ "$severity" == "blocking" ]]; then
    echo "SEVERITY: RELEASE-BLOCKING (ADR-0077 section 4: a scenario-2"
    echo "          failure is a release-blocking bug, not a flaky test)."
    echo "Paste this block into the ADR-0077 section 3 rehearsal record."
    return 2
  fi
  echo "Paste this block into the ADR-0077 section 3 rehearsal record."
  return 1
}

# ---------------------------------------------------------------------------
# Dependency / structure validation (used by --check, never starts anything).
# ---------------------------------------------------------------------------

# Tools a real end-to-end run needs. Absent tools are reported by
# check_dependencies but, by default, do not fail --check: --check proves the
# script is well-formed in an environment with no MinIO. Set
# CHAOS_CHECK_STRICT=1 (orchestrator side, where the tools must exist) to make
# any absent real-run dependency fail the check.
CHAOS_REAL_RUN_TOOLS=(curl docker jq)

# The MinIO client is looked up as either `mc` on PATH or the mc container
# image used by scripts/demo.sh; check_dependencies reports which was found.
chaos_have_command() {
  command -v "$1" >/dev/null 2>&1
}

# Report whether a MinIO client is reachable, without starting anything.
chaos_minio_client_available() {
  if chaos_have_command mc; then
    return 0
  fi
  if chaos_have_command docker; then
    # demo.sh drives mc via `docker run minio/mc`; docker presence is the
    # gate for that path. We do not pull the image here (that would touch the
    # network); we only report the capability exists.
    return 0
  fi
  return 1
}

# Report whether the ravel binaries are runnable: either prebuilt on PATH or
# buildable via cargo from this checkout.
chaos_ravel_binaries_available() {
  if chaos_have_command ravel-server && chaos_have_command ravel-cli; then
    return 0
  fi
  if chaos_have_command cargo; then
    return 0
  fi
  return 1
}

# Validate structure and dependencies WITHOUT starting MinIO, driving load,
# or issuing any kill. Returns 0 when the script is structurally well-formed;
# returns nonzero only on a structural defect, or on any missing real-run
# tool when CHAOS_CHECK_STRICT=1. Absent real-run tools are reported as
# WARN (not FAIL) by default so --check passes in a MinIO-less executor.
check_dependencies() {
  local strict="${CHAOS_CHECK_STRICT:-0}"
  local defects=0
  local warnings=0
  local tool

  echo "---- structural checks ----"

  # Every pinned oracle assertion must be a defined, callable function.
  local fn
  for fn in \
    oracle_strict_ack_implies_durable \
    oracle_sibling_takeover_within_bound \
    oracle_no_orphaned_lease \
    oracle_conservation_holds \
    oracle_custody_and_catalog_verify_clean \
    oracle_no_partial_output_leak; do
    if declare -F "$fn" >/dev/null 2>&1; then
      echo "  OK    oracle function defined: ${fn}"
    else
      echo "  FAIL  oracle function MISSING: ${fn}"
      defects=$(( defects + 1 ))
    fi
  done

  # Pinned constants must be positive integers.
  local c
  for c in CHAOS_H_SECONDS CHAOS_MAINTAIN_TICK_SECONDS CHAOS_PROTECTION_HORIZON_SECONDS; do
    if [[ "${!c}" =~ ^[1-9][0-9]*$ ]]; then
      echo "  OK    constant ${c}=${!c}"
    else
      echo "  FAIL  constant ${c} is not a positive integer: '${!c}'"
      defects=$(( defects + 1 ))
    fi
  done

  echo "  OK    derived takeover bound = $(chaos_takeover_bound_seconds)s (3*H + one tick)"

  echo "---- real-run dependency checks (WARN unless CHAOS_CHECK_STRICT=1) ----"

  for tool in "${CHAOS_REAL_RUN_TOOLS[@]}"; do
    if chaos_have_command "$tool"; then
      echo "  OK    tool present: ${tool}"
    else
      echo "  WARN  tool absent (needed for a real run): ${tool}"
      warnings=$(( warnings + 1 ))
    fi
  done

  if chaos_minio_client_available; then
    echo "  OK    MinIO client path available (mc or docker)"
  else
    echo "  WARN  no MinIO client (mc) and no docker: a real run cannot manage MinIO"
    warnings=$(( warnings + 1 ))
  fi

  if chaos_ravel_binaries_available; then
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
    echo "check: FAIL -- ${warnings} missing real-run dependency(ies) (CHAOS_CHECK_STRICT=1)"
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
# MinIO startup / teardown helpers (real-run only).
# ---------------------------------------------------------------------------

CHAOS_STARTED_MINIO=0

chaos_minio_healthy() {
  curl --silent --fail --max-time 2 \
    "${CHAOS_MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1
}

# Wait up to N attempts (1 s apart) for a predicate command to succeed.
chaos_wait_for() {
  local description="$1"
  local attempts="$2"
  shift 2
  local attempt
  for attempt in $(seq 1 "$attempts"); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done
  log "timed out waiting for ${description}"
  return 1
}

# Bring MinIO up (idempotent) and ensure the bucket + store qualification
# exist. Mirrors scripts/demo.sh so a real run matches the demo's known-good
# startup. Never called in --check mode.
minio_up() {
  if chaos_minio_healthy; then
    log "MinIO already running at ${CHAOS_MINIO_ENDPOINT}"
  else
    log "starting MinIO via docker compose"
    docker compose -f "$CHAOS_MINIO_COMPOSE" up -d
    CHAOS_STARTED_MINIO=1
    chaos_wait_for "MinIO to become healthy" 30 chaos_minio_healthy || return 1
  fi

  log "ensuring bucket ${RAVEL_S3_BUCKET} exists"
  docker run --rm --network host \
    -e "MC_HOST_local=http://${RAVEL_S3_ACCESS_KEY}:${RAVEL_S3_SECRET_KEY}@127.0.0.1:9000" \
    minio/mc:latest mb -p "local/${RAVEL_S3_BUCKET}" >/dev/null 2>&1 || true

  # ADR-0050 EC7: a non-Memory store refuses to serve until `sys/qualification`
  # exists, and there is no bootstrap-and-continue path. Qualify before any
  # server start.
  log "qualifying store backend (ravel-cli store qualify)"
  local rc=0
  run_capture ravel_cli --store s3 store qualify || rc=$?
  return "$rc"
}

# Tear MinIO down only if this library started it.
minio_down() {
  if [[ "$CHAOS_STARTED_MINIO" -eq 1 ]]; then
    log "stopping MinIO"
    docker compose -f "$CHAOS_MINIO_COMPOSE" down >/dev/null 2>&1 || true
    CHAOS_STARTED_MINIO=0
  fi
}

# ---------------------------------------------------------------------------
# Binary launch helpers. `ravel_cli` / `ravel_server` prefer a prebuilt
# binary on PATH and fall back to `cargo run` from this checkout, so the same
# harness works on an orchestrator with installed binaries and on a dev tree.
# ---------------------------------------------------------------------------

ravel_cli() {
  if chaos_have_command ravel-cli; then
    ravel-cli "$@"
  else
    cargo run --quiet -p ravel-cli -- "$@"
  fi
}

ravel_server_cmd() {
  # Emit the argv for launching the server, so callers can background it and
  # capture the PID directly (needed to SIGKILL a specific process).
  if chaos_have_command ravel-server; then
    printf '%s\0' ravel-server "$@"
  else
    printf '%s\0' cargo run --quiet -p ravel-server -- "$@"
  fi
}

# SIGKILL a process by PID (real `kill -9`, ADR-0077 section 4) and reap it.
# Deliberately SIGKILL, never SIGTERM: the scenario tests death without any
# graceful-shutdown path running.
sigkill_pid() {
  local pid="$1"
  if [[ -z "$pid" ]]; then
    log "sigkill_pid: no PID given"
    return 1
  fi
  if kill -0 "$pid" 2>/dev/null; then
    log "SIGKILL pid ${pid}"
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  else
    log "sigkill_pid: pid ${pid} not running"
  fi
}

# ---------------------------------------------------------------------------
# Metric / marker helpers. Prometheus text is captured into a variable and
# parsed from the variable; no pipeline sits between a gate and its exit code.
# ---------------------------------------------------------------------------

# Read a single no-label counter/gauge value from a server's /metrics. Prints
# the value on stdout, or the empty string if the metric is absent. Returns 1
# only when the scrape itself fails (server unreachable).
metric_value() {
  local base_url="$1"
  local name="$2"
  local body
  body="$(curl --silent --fail --max-time 5 "${base_url}/metrics")" || return 1
  # $body is data, not a gate; awk selects the sample line by exact metric name.
  awk -v n="$name" '$1 == n { print $2; hit = 1 } END { if (!hit) print "" }' \
    <<<"$body"
}

# Block until a metric reaches at least `threshold`, or a deadline passes.
# Returns 0 on reaching the threshold, 1 on timeout/unreachable.
wait_for_metric_at_least() {
  local base_url="$1"
  local name="$2"
  local threshold="$3"
  local deadline_seconds="$4"
  local waited=0
  local value
  while [[ "$waited" -lt "$deadline_seconds" ]]; do
    value="$(metric_value "$base_url" "$name")" || value=""
    if [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
      # Integer-compare the truncated value; counters here are whole numbers.
      if [[ "${value%.*}" -ge "$threshold" ]]; then
        return 0
      fi
    fi
    sleep 1
    waited=$(( waited + 1 ))
  done
  return 1
}

# ---------------------------------------------------------------------------
# Flush / compaction observation markers.
#
# Scenario 1 keys off `ravel_ingest_flushes_by_size_total`. That counter is
# incremented at flush ATTEMPT time (crates/ravel-ingest/src/span_metrics.rs:
# "Attempt-time, same as flushes_by_size"), so an increment means a flush has
# STARTED but not necessarily completed -- exactly the "mid-flush" window
# ADR-0077 section 4 names. The kill fires as soon as the counter rises past
# its pre-load baseline.
#
# Scenario 2 keys off the compaction lifecycle: a compaction is observed
# in-flight when the maintain worker owns units and has begun a run but has
# not yet emitted the `"compaction record published"` log line
# (crates/ravel-maintain/src/publish.rs:200) for that unit. The kill fires
# after parts have begun landing but before that publish, so the compaction
# is genuinely interrupted mid-flight.
# ---------------------------------------------------------------------------

CHAOS_FLUSH_METRIC="ravel_ingest_flushes_by_size_total"
CHAOS_COMPACTION_PUBLISH_MARKER="compaction record published"

# Wait until the flush counter has risen past `baseline`, i.e. a flush has
# been attempted since load began. This is the scenario-1 "mid-flush" trigger.
wait_for_flush_started() {
  local base_url="$1"
  local baseline="$2"
  local deadline_seconds="$3"
  wait_for_metric_at_least \
    "$base_url" "$CHAOS_FLUSH_METRIC" "$(( baseline + 1 ))" "$deadline_seconds"
}

# Wait until a compaction has begun for the worker whose log is `log_file`,
# but before it publishes its record. We detect "begun" as the worker owning
# at least one unit AND its log not yet carrying the publish marker for the
# current run. This is the scenario-2 "mid-compaction" trigger.
wait_for_compaction_in_flight() {
  local base_url="$1"
  local log_file="$2"
  local deadline_seconds="$3"
  local waited=0
  local owned
  local body
  while [[ "$waited" -lt "$deadline_seconds" ]]; do
    owned="$(metric_value "$base_url" ravel_maintain_units_owned)" || owned=""
    # Capture the log into a variable, then test it: no gate pipeline.
    body=""
    if [[ -r "$log_file" ]]; then
      body="$(cat "$log_file")"
    fi
    if [[ "${owned%.*}" =~ ^[0-9]+$ ]] && [[ "${owned%.*}" -ge 1 ]]; then
      # A run is in flight the moment a unit is owned and being worked; the
      # kill must land before the publish marker for a true mid-compaction.
      if [[ "$body" != *"$CHAOS_COMPACTION_PUBLISH_MARKER"* ]]; then
        return 0
      fi
    fi
    sleep 1
    waited=$(( waited + 1 ))
  done
  return 1
}

# ---------------------------------------------------------------------------
# Load-driving helpers. A real run drives OTLP load through the server's HTTP
# ingest exactly as scripts/demo.sh does, collecting the strict-ack commit
# token from each accepted export.
# ---------------------------------------------------------------------------

# POST one OTLP metrics export and echo its strict-ack commit token on
# stdout. Returns nonzero if the export was not accepted or carried no token.
# The caller records the token as an acked-before-kill write.
drive_one_export() {
  local http_addr="$1"
  local fixture_path="$2"
  local header_file
  header_file="$(mktemp)"
  local rc=0
  run_capture curl --silent --show-error --fail \
    --dump-header "$header_file" \
    --output /dev/null \
    -X POST "http://${http_addr}/v1/metrics" \
    -H "Authorization: Bearer ${CHAOS_TENANT_TOKEN}" \
    -H "Content-Type: application/x-protobuf" \
    --data-binary "@${fixture_path}" || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    rm -f "$header_file"
    return "$rc"
  fi
  local header_body
  header_body="$(cat "$header_file")"
  rm -f "$header_file"
  local token
  token="$(awk 'BEGIN{IGNORECASE=1} /^x-ravel-commit-token:/ { print $2 }' \
    <<<"$header_body" | tr -d '\r')"
  if [[ -z "$token" ]]; then
    return 1
  fi
  echo "$token"
}

# Query one metric back at a given min_commit_token and report whether the
# series is visible. Returns 0 when visible, 1 when not. Used by the
# strict-ack-implies-durable oracle. The HTTP body is captured into a
# variable and tested from the variable (no gate pipeline).
query_series_visible() {
  local http_addr="$1"
  local series="$2"
  local min_commit_token="$3"
  local body
  local rc=0
  body="$(curl --silent --show-error --fail \
    -H "Authorization: Bearer ${CHAOS_TENANT_TOKEN}" \
    --get "http://${http_addr}/api/v1/query" \
    --data-urlencode "query=${series}" \
    --data-urlencode "min_commit_token=${min_commit_token}")" || rc=$?
  if [[ "$rc" -ne 0 ]]; then
    return 1
  fi
  if [[ "$body" == *'"status":"success"'* && "$body" == *"$series"* ]]; then
    return 0
  fi
  return 1
}

# ===========================================================================
# PINNED ORACLE ASSERTIONS (ADR-0077 section 4).
#
# Each is a separate, independently testable function. Each returns 0 on the
# assertion holding and records via oracle_ok; on failure it records via
# oracle_bad (naming the assertion) and returns nonzero. None of them starts
# MinIO or drives load: they read the post-kill/post-restart world the
# scenario scripts set up.
# ===========================================================================

# 1. strict-ack-implies-durable.
#    Every write acked under strict ack before the kill is durable and
#    queryable after restart, and no partial flush is visible (no commit
#    token beyond the last strict ack becomes queryable).
#
#    Args: http_addr, series, highest_acked_token, <acked_token...>
oracle_strict_ack_implies_durable() {
  local http_addr="$1"
  local series="$2"
  local highest_acked_token="$3"
  shift 3
  local acked_tokens=("$@")
  local name="strict-ack-implies-durable"

  local missing=0
  local token
  for token in "${acked_tokens[@]}"; do
    if ! query_series_visible "$http_addr" "$series" "$token"; then
      missing=$(( missing + 1 ))
      log "  strict-acked token not durable/queryable after restart: ${token}"
    fi
  done
  if [[ "$missing" -gt 0 ]]; then
    oracle_bad "$name" \
      "${missing}/${#acked_tokens[@]} strict-acked write(s) not durable after restart"
    return 1
  fi

  # No partial flush visible: a query gated at one-past the highest strict-ack
  # must NOT surface a higher committed token. If the store advanced the
  # visible commit frontier past the last strict ack, an unacked partial flush
  # leaked -- the exact failure this scenario exists to catch.
  local beyond=$(( highest_acked_token + 1 ))
  if query_series_visible "$http_addr" "$series" "$beyond"; then
    oracle_bad "$name" \
      "partial flush visible: data queryable at commit token ${beyond} > highest strict-ack ${highest_acked_token}"
    return 1
  fi

  oracle_ok "$name"
  return 0
}

# 2. sibling-takeover-within-3H-plus-tick.
#    After one worker is SIGKILLed, the surviving sibling takes over the dead
#    worker's units within `3 * H` + one maintenance tick. Measured as
#    wall-clock from the kill to when the survivor owns the expected total
#    unit count with no stalled units.
#
#    Args: survivor_base_url, expected_total_units, kill_epoch_seconds
oracle_sibling_takeover_within_bound() {
  local survivor_url="$1"
  local expected_units="$2"
  local kill_epoch="$3"
  local name="sibling-takeover-within-3H-plus-tick"
  local bound
  bound="$(chaos_takeover_bound_seconds)"

  # Poll the survivor until it owns every unit, bounded by the ADR budget.
  if ! wait_for_metric_at_least \
      "$survivor_url" ravel_maintain_units_owned "$expected_units" "$bound"; then
    oracle_bad "$name" \
      "survivor did not own all ${expected_units} units within ${bound}s (3*H + tick)"
    return 1
  fi

  # The takeover must also be clean: no owned unit left stalled.
  local stalled
  stalled="$(metric_value "$survivor_url" ravel_maintain_units_stalled)" || stalled=""
  if [[ "${stalled%.*}" =~ ^[0-9]+$ ]] && [[ "${stalled%.*}" -gt 0 ]]; then
    oracle_bad "$name" "survivor reports ${stalled} stalled unit(s) after takeover"
    return 1
  fi

  oracle_ok "$name"
  return 0
}

# 3. no-orphaned-lease.
#    No unit stays orphaned: after takeover, the union of live workers owns
#    every unit and none is stalled. In the single-survivor case this is the
#    survivor owning `expected_total_units` with zero stalled and at least one
#    worker live.
#
#    Args: survivor_base_url, expected_total_units
oracle_no_orphaned_lease() {
  local survivor_url="$1"
  local expected_units="$2"
  local name="no-orphaned-lease"

  local live
  live="$(metric_value "$survivor_url" ravel_maintain_workers_live)" || live=""
  if [[ ! "${live%.*}" =~ ^[0-9]+$ ]] || [[ "${live%.*}" -lt 1 ]]; then
    oracle_bad "$name" "no live maintain worker after kill (workers_live='${live}')"
    return 1
  fi

  local owned
  owned="$(metric_value "$survivor_url" ravel_maintain_units_owned)" || owned=""
  if [[ ! "${owned%.*}" =~ ^[0-9]+$ ]] || [[ "${owned%.*}" -lt "$expected_units" ]]; then
    oracle_bad "$name" \
      "only ${owned}/${expected_units} units owned by live workers: an orphan remains"
    return 1
  fi

  local stalled
  stalled="$(metric_value "$survivor_url" ravel_maintain_units_stalled)" || stalled=""
  if [[ "${stalled%.*}" =~ ^[0-9]+$ ]] && [[ "${stalled%.*}" -gt 0 ]]; then
    oracle_bad "$name" "${stalled} unit(s) stalled (owned but not progressing)"
    return 1
  fi

  oracle_ok "$name"
  return 0
}

# 4. conservation-holds.
#    The interrupted compaction completes under the conservation gate: the
#    conservation-abort counter did not advance across the interruption (the
#    gate never had to reject a record-dropping compaction), and the unit
#    reaches a published/compacted terminal state after takeover.
#
#    Args: survivor_base_url, conservation_aborts_baseline, survivor_log_file
oracle_conservation_holds() {
  local survivor_url="$1"
  local aborts_baseline="$2"
  local survivor_log="$3"
  local name="conservation-holds"

  local aborts_now
  aborts_now="$(metric_value "$survivor_url" ravel_maintain_conservation_aborts_total)" \
    || aborts_now=""
  if [[ ! "${aborts_now%.*}" =~ ^[0-9]+$ ]]; then
    oracle_bad "$name" "could not read ravel_maintain_conservation_aborts_total"
    return 1
  fi
  if [[ "${aborts_now%.*}" -gt "${aborts_baseline%.*}" ]]; then
    oracle_bad "$name" \
      "conservation gate aborted a compaction (aborts ${aborts_baseline} -> ${aborts_now}): records were not conserved"
    return 1
  fi

  # The interrupted unit must actually finish: the survivor must publish a
  # compaction record after taking over. The publish marker is captured from
  # the survivor's log into a variable and tested from the variable.
  local survivor_body=""
  if [[ -r "$survivor_log" ]]; then
    survivor_body="$(cat "$survivor_log")"
  fi
  if [[ "$survivor_body" != *"$CHAOS_COMPACTION_PUBLISH_MARKER"* ]]; then
    oracle_bad "$name" \
      "no '${CHAOS_COMPACTION_PUBLISH_MARKER}' after takeover: interrupted compaction did not complete"
    return 1
  fi

  oracle_ok "$name"
  return 0
}

# 5. custody-and-catalog-verification-clean.
#    `ravel-cli maintain verify-custody` and `ravel-cli catalog verify` both
#    exit clean. Both CLIs exit nonzero on any anomaly/mismatch, so the exit
#    code IS the gate -- no output is grepped (which would mask it).
#
#    Args: tenant_name [shards]
oracle_custody_and_catalog_verify_clean() {
  local tenant="$1"
  local shards="${2:-4}"
  local name="custody-and-catalog-verification-clean"

  local custody_rc=0
  run_capture ravel_cli --store s3 maintain verify-custody \
    --tenant "$tenant" --shards "$shards" --versioning-aware || custody_rc=$?

  local catalog_rc=0
  run_capture ravel_cli --store s3 catalog verify --tenant "$tenant" || catalog_rc=$?

  if [[ "$custody_rc" -ne 0 && "$catalog_rc" -ne 0 ]]; then
    oracle_bad "$name" \
      "verify-custody (exit ${custody_rc}) AND catalog verify (exit ${catalog_rc}) both reported anomalies"
    return 1
  fi
  if [[ "$custody_rc" -ne 0 ]]; then
    oracle_bad "$name" "verify-custody reported anomalies (exit ${custody_rc})"
    return 1
  fi
  if [[ "$catalog_rc" -ne 0 ]]; then
    oracle_bad "$name" "catalog verify reported anomalies (exit ${catalog_rc})"
    return 1
  fi

  oracle_ok "$name"
  return 0
}

# 6. no-partial-output-leak (scenario 2's abandoned-parts clause).
#    The dead worker's abandoned partial outputs age out under the existing
#    unreferenced-part rule with no leak past the horizon. In a bounded
#    rehearsal we cannot wait the full 24 h `protection_horizon`, so we assert
#    the two properties that make the age-out safe and complete:
#      (a) the abandoned parts are unreferenced -- verify-custody is clean, so
#          no live commit record points at a dead worker's partial part; and
#      (b) they are correctly classified and withheld, not leaked: the orphan
#          gauge accounts for them (orphans_present == orphans_withheld) and
#          the orphan breaker has not tripped, so nothing past the horizon is
#          being deleted prematurely nor leaked live.
#    The full horizon-length age-out itself is recorded by a separate
#    long-run rehearsal (ADR-0077 section 3); this asserts the invariant that
#    makes that age-out correct.
#
#    Args: survivor_base_url, orphan_breaker_baseline
oracle_no_partial_output_leak() {
  local survivor_url="$1"
  local breaker_baseline="$2"
  local name="no-partial-output-leak"

  # (b1) The orphan breaker must not have tripped: a trip means the
  #      unreferenced-part sweep saw more orphans than its safety bound and
  #      withheld ALL deletes -- parts would then leak past the horizon.
  local breaker_now
  breaker_now="$(metric_value "$survivor_url" ravel_maintain_orphan_breaker_tripped_total)" \
    || breaker_now=""
  if [[ ! "${breaker_now%.*}" =~ ^[0-9]+$ ]]; then
    oracle_bad "$name" "could not read ravel_maintain_orphan_breaker_tripped_total"
    return 1
  fi
  if [[ "${breaker_now%.*}" -gt "${breaker_baseline%.*}" ]]; then
    oracle_bad "$name" \
      "orphan breaker tripped (${breaker_baseline} -> ${breaker_now}): abandoned parts would leak past the horizon"
    return 1
  fi

  # (b2) Orphan accounting must balance: every present orphan is withheld
  #      (within horizon, correctly NOT yet deleted) rather than leaked into
  #      live references. present > withheld would mean an orphan escaped the
  #      accounting.
  local present withheld
  present="$(metric_value "$survivor_url" ravel_maintain_orphans_present)" || present=""
  withheld="$(metric_value "$survivor_url" ravel_maintain_orphans_withheld)" || withheld=""
  if [[ ! "${present%.*}" =~ ^[0-9]+$ ]] || [[ ! "${withheld%.*}" =~ ^[0-9]+$ ]]; then
    oracle_bad "$name" \
      "could not read orphan gauges (present='${present}', withheld='${withheld}')"
    return 1
  fi
  if [[ "${present%.*}" -gt "${withheld%.*}" ]]; then
    oracle_bad "$name" \
      "orphan accounting unbalanced: present=${present} > withheld=${withheld} (a partial output escaped withholding)"
    return 1
  fi

  # (a) is discharged by the separate custody-and-catalog oracle (verify-custody
  #     clean == no live reference to any abandoned part). We assert the
  #     accounting invariant here and rely on that oracle for referential
  #     cleanliness, keeping each assertion independently testable.
  oracle_ok "$name"
  return 0
}
