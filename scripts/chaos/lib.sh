#!/usr/bin/env bash
# scripts/chaos/lib.sh -- shared library for the ADR-0077 section 4
# process-kill chaos-evidence lane.
#
# This lane is deliberately NOT part of ravel-sim and is NOT run in PR CI
# (ADR-0077 section 4, "Rejected alternatives / Running the chaos lane in
# PR CI"). It drives real binaries, a real object store, real multi-threaded
# runtime, a real clock, and a real `kill -9`. Executors have no object store
# and must not run it end to end; an orchestrator with one runs it later and
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
# Object store / server / tenancy configuration (mirrors scripts/demo.sh).
# ---------------------------------------------------------------------------

CHAOS_RUSTFS_COMPOSE="${CHAOS_RUSTFS_COMPOSE:-deploy/docker-compose/rustfs.yml}"
CHAOS_RUSTFS_ENDPOINT="${CHAOS_RUSTFS_ENDPOINT:-http://127.0.0.1:9000}"
# Pinned by tag and digest, from a registry outside Docker Hub's per-IP
# anonymous pull allowance; see deploy/README.md.
CHAOS_AWS_CLI_IMAGE="${CHAOS_AWS_CLI_IMAGE:-public.ecr.aws/aws-cli/aws-cli:2.37.2@sha256:e38214027df83cb6631adcf980a092a98d1d29788789bff2a0f424e87e3da8ed}"

export RAVEL_S3_ENDPOINT="${RAVEL_S3_ENDPOINT:-$CHAOS_RUSTFS_ENDPOINT}"
export RAVEL_S3_BUCKET="${RAVEL_S3_BUCKET:-ravel-chaos}"
export RAVEL_S3_REGION="${RAVEL_S3_REGION:-us-east-1}"
export RAVEL_S3_ACCESS_KEY="${RAVEL_S3_ACCESS_KEY:-ravel}"
export RAVEL_S3_SECRET_KEY="${RAVEL_S3_SECRET_KEY:-ravel-dev-secret}"

CHAOS_TENANT_TOKEN="${CHAOS_TENANT_TOKEN:-chaos-token}"
CHAOS_TENANT_NAME="${CHAOS_TENANT_NAME:-chaos-tenant}"

# Tenant-hash scheme (ADR-0050 section 3). The tenant hash is keyed by default,
# and a fresh bucket refuses a server that names neither scheme; `rustfs_up`
# empties the bucket before every scenario, so every start here meets a fresh
# bucket. The lane has no deployment key to give a fresh bucket, and a keyed
# run would need one provisioned per run, so the dev-only chaos bucket is
# unkeyed by default. A keyed run sets CHAOS_TENANT_HASH_MODE=keyed together with
# CHAOS_TENANT_HASH_KEY_FILE, which selects --tenant-hash-key-file instead.
CHAOS_TENANT_HASH_MODE="${CHAOS_TENANT_HASH_MODE:-unkeyed}"
CHAOS_TENANT_HASH_KEY_FILE="${CHAOS_TENANT_HASH_KEY_FILE:-}"

# `--audit-text redacted`, the default, refuses to start an `all`-mode server
# with no tokenization key, and an unkeyed bucket has no deployment key to
# derive one from. This is the dev-only key deploy/docker-compose/ravel.yml
# also defaults to; a keyed run derives its key from the key file instead.
if [[ "${CHAOS_TENANT_HASH_MODE}" == "unkeyed" ]]; then
  export RAVEL_AUDIT_TOKEN_KEY="${RAVEL_AUDIT_TOKEN_KEY:-998626405d16aeca71f4fac7673b55213a774ba40401709022e81a27f050ffd8}"
fi

# Set CHAOS_TENANT_HASH_ARGS to the tenant-hash flags for the configured mode.
# Returns 64, naming the problem, for a mode that is neither `unkeyed` nor
# `keyed`, for `keyed` with no key file, and for a key file under `unkeyed`.
chaos_tenant_hash_args() {
  CHAOS_TENANT_HASH_ARGS=()
  case "${CHAOS_TENANT_HASH_MODE}" in
    unkeyed)
      if [[ -n "${CHAOS_TENANT_HASH_KEY_FILE}" ]]; then
        log "CHAOS_TENANT_HASH_KEY_FILE is set but CHAOS_TENANT_HASH_MODE=unkeyed; set CHAOS_TENANT_HASH_MODE=keyed to use the key"
        return 64
      fi
      CHAOS_TENANT_HASH_ARGS=(--tenant-hash-unkeyed)
      ;;
    keyed)
      if [[ -z "${CHAOS_TENANT_HASH_KEY_FILE}" ]]; then
        log "CHAOS_TENANT_HASH_MODE=keyed needs CHAOS_TENANT_HASH_KEY_FILE"
        return 64
      fi
      if [[ ! -r "${CHAOS_TENANT_HASH_KEY_FILE}" ]]; then
        log "CHAOS_TENANT_HASH_KEY_FILE is not a readable file: ${CHAOS_TENANT_HASH_KEY_FILE}"
        return 64
      fi
      CHAOS_TENANT_HASH_ARGS=(--tenant-hash-key-file "${CHAOS_TENANT_HASH_KEY_FILE}")
      ;;
    *)
      log "CHAOS_TENANT_HASH_MODE must be 'unkeyed' or 'keyed', got '${CHAOS_TENANT_HASH_MODE}'"
      return 64
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Logging and oracle bookkeeping.
# ---------------------------------------------------------------------------

# Two parallel arrays record oracle outcomes. ORACLE_PASS holds the names of
# assertions that held; ORACLE_FAIL holds "name: detail" for assertions that
# failed. print_oracle_summary renders both and sets the exit status.
ORACLE_PASS=()
ORACLE_FAIL=()
# "name: reason" for assertions the run could not evaluate because the event
# they judge did not happen (see oracle_conservation_or_unmeasured).
ORACLE_UNMEASURED=()
# Summary exit code for a run with no failure and at least one unmeasured
# assertion: a setup result, not a verdict.
CHAOS_UNMEASURED_EXIT=3

log() {
  echo "[chaos] $*" >&2
}

# Where a scenario keeps the logs of the processes it drives. Empty, the
# default, means one mktemp file per process, deleted at exit. When set, each
# scenario writes `<CHAOS_LOG_DIR>/<scenario>-<process>.log` and, on a
# non-zero exit, keeps the files and prints the tail of each: a server that
# never accepts a connection leaves its reason only in its own log, and a CI
# job can upload the directory.
CHAOS_LOG_DIR="${CHAOS_LOG_DIR:-}"

# Path for one process log: $1=scenario $2=process. A mktemp file when
# CHAOS_LOG_DIR is unset, else a named file under it (the directory is created).
chaos_logfile() {
  if [[ -n "${CHAOS_LOG_DIR}" ]]; then
    mkdir -p "${CHAOS_LOG_DIR}"
    printf '%s/%s-%s.log\n' "${CHAOS_LOG_DIR}" "$1" "$2"
  else
    mktemp
  fi
}

# Release the process logs at exit: $1=the scenario's exit code, the rest are
# log paths. Deleted unless the exit is non-zero and CHAOS_LOG_DIR is set, in
# which case each is kept and its last 40 lines go to stderr.
chaos_release_logs() {
  local code="$1" f
  shift
  if [[ "${code}" -ne 0 && -n "${CHAOS_LOG_DIR}" ]]; then
    for f in "$@"; do
      [[ -f "${f}" ]] || continue
      log "exit ${code}: kept ${f}; its last 40 lines:"
      tail -n 40 "${f}" >&2
    done
    return 0
  fi
  # A passing run keeps its logs too when asked, so a scenario that passed
  # without exercising its kill point can still be diagnosed.
  if [[ "${CHAOS_KEEP_LOGS:-0}" == 1 && -n "${CHAOS_LOG_DIR}" ]]; then
    log "exit ${code}: CHAOS_KEEP_LOGS=1, kept $*"
    return 0
  fi
  rm -f "$@"
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
# With no failure and at least one ORACLE_UNMEASURED entry it returns
# CHAOS_UNMEASURED_EXIT (3) instead of 0.
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
  if [[ ${#ORACLE_UNMEASURED[@]} -gt 0 ]]; then
    for name in "${ORACLE_UNMEASURED[@]}"; do
      echo "  SKIP  ${name}"
    done
  fi
  echo "-----------------------------------------------------------"
  if [[ ${#ORACLE_FAIL[@]} -eq 0 && ${#ORACLE_UNMEASURED[@]} -gt 0 ]]; then
    echo "RESULT: COULD NOT MEASURE -- no assertion failed, but ${#ORACLE_UNMEASURED[@]} could not be evaluated (${scenario}):"
    for name in "${ORACLE_UNMEASURED[@]}"; do
      echo "  - ${name#*: }"
    done
    echo "This is a setup result, not an oracle verdict; rerun after tuning the load."
    return "$CHAOS_UNMEASURED_EXIT"
  fi
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
# script is well-formed in an environment with no object store. Set
# CHAOS_CHECK_STRICT=1 (orchestrator side, where the tools must exist) to make
# any absent real-run dependency fail the check.
CHAOS_REAL_RUN_TOOLS=(curl docker jq)

# The S3 client is always the pinned AWS CLI container image scripts/demo.sh
# also uses, so check_dependencies only needs docker.
chaos_have_command() {
  command -v "$1" >/dev/null 2>&1
}

# Report whether an S3 client is reachable, without starting anything.
chaos_s3_client_available() {
  # chaos_aws always runs the pinned AWS CLI image, so docker is the only path.
  if chaos_have_command docker; then
    # demo.sh drives the AWS CLI via `docker run`; docker presence is the gate
    # for that path. We do not pull the image here (that would touch the
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

# Validate structure and dependencies WITHOUT starting the object store,
# driving load, or issuing any kill. Returns 0 when the script is structurally
# well-formed; returns nonzero only on a structural defect, or on any missing
# real-run tool when CHAOS_CHECK_STRICT=1. Absent real-run tools are reported
# as WARN (not FAIL) by default so --check passes in a store-less executor.
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
    oracle_conservation_or_unmeasured \
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

  if chaos_s3_client_available; then
    echo "  OK    S3 client path available (docker)"
  else
    echo "  WARN  no S3 client (aws) and no docker: a real run cannot manage the store"
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
# RustFS startup / teardown helpers (real-run only).
# ---------------------------------------------------------------------------

CHAOS_STARTED_RUSTFS=0

chaos_rustfs_healthy() {
  curl --silent --fail --max-time 2 \
    "${CHAOS_RUSTFS_ENDPOINT}/health" >/dev/null 2>&1
}

# Run one AWS CLI command against the chaos store, in the pinned container,
# with the chaos credentials. Arguments are the aws subcommand and its flags.
chaos_aws() {
  # Credentials are exported in a subshell and passed by name, so the secret
  # never reaches the docker argv or docker inspect (the same rule as dr_aws).
  (
    export AWS_ACCESS_KEY_ID="${RAVEL_S3_ACCESS_KEY}"
    export AWS_SECRET_ACCESS_KEY="${RAVEL_S3_SECRET_KEY}"
    export AWS_DEFAULT_REGION="${RAVEL_S3_REGION}"
    docker run --rm --network host \
      -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
      -e AWS_EC2_METADATA_DISABLED=true \
      "$CHAOS_AWS_CLI_IMAGE" \
      --endpoint-url "$CHAOS_RUSTFS_ENDPOINT" \
      "$@"
  )
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

# Bring RustFS up (idempotent) and ensure the bucket + store qualification
# exist. Mirrors scripts/demo.sh so a real run matches the demo's known-good
# startup. Never called in --check mode.
rustfs_up() {
  if chaos_rustfs_healthy; then
    log "RustFS already running at ${CHAOS_RUSTFS_ENDPOINT}"
  else
    log "starting RustFS via docker compose"
    docker compose -f "$CHAOS_RUSTFS_COMPOSE" up -d
    CHAOS_STARTED_RUSTFS=1
    chaos_wait_for "RustFS to become healthy" 30 chaos_rustfs_healthy || return 1
  fi

  log "ensuring bucket ${RAVEL_S3_BUCKET} exists"
  # The `|| true` covers a bucket left over on a reused data directory; the
  # emptying step below then works on whichever bucket is there.
  # Stays plain: the current-version `s3 rm` below cannot empty a versioned bucket.
  chaos_aws s3api create-bucket --bucket "$RAVEL_S3_BUCKET" >/dev/null 2>&1 || true

  # Start every scenario on an empty store. The compose file bind-mounts
  # ../../rustfs-data, so `docker compose down` leaves the objects on the host
  # and the next scenario, or a re-run of this one, inherits them. Why that
  # breaks the takeover oracles rather than merely being untidy: see
  # `assert_single_tenant_universe` below. Emptied through the S3 API rather
  # than rm because the container writes the host directory as root.
  # RAVEL_S3_BUCKET defaults to a chaos-only bucket.
  log "emptying bucket ${RAVEL_S3_BUCKET} so this scenario starts clean"
  chaos_aws s3 rm "s3://${RAVEL_S3_BUCKET}" --recursive >/dev/null 2>&1 || true

  # ADR-0050 EC7: a non-Memory store refuses to serve until `sys/qualification`
  # exists, and there is no bootstrap-and-continue path. Qualify before any
  # server start.
  log "qualifying store backend (ravel-cli store qualify)"
  local rc=0
  run_capture ravel_cli --store s3 store qualify || rc=$?
  return "$rc"
}

# Count the tenants the maintain tier would discover, and record a pinned
# oracle failure when it is not exactly one.
#
# NORMATIVE: this is the one place the single-tenant precondition is explained.
# Other sites point here rather than restating it.
#
# `discover_tenants` (crates/ravel-maintain/src/discover.rs) lists every tenant
# under `t/` and ignores --tenant-token, and `ravel_maintain_units_owned` is a
# single store-wide gauge. The takeover oracles compare that gauge against one
# scenario's worth of units with >=. So a tenant left in the store from another
# scenario inflates the reading while the expected value stays put, and the
# survivor clears the threshold owning only its pre-kill share: the oracle
# passes with no takeover having happened.
#
# Two things keep that from happening, and they are not interchangeable.
# `rustfs_up` empties the bucket so the precondition holds. This function
# asserts that it did, because the violated-precondition case is a PASS rather
# than an error and nothing else would mark it.
#
# `aws s3 ls` on a prefix prints one `PRE <name>/` line per child prefix, so
# the trailing-slash count below is the tenant count, exactly as it was under
# the equivalent `mc ls`.
assert_single_tenant_universe() {
  local listing count
  listing="$(chaos_aws s3 ls "s3://${RAVEL_S3_BUCKET}/t/" 2>/dev/null)" || {
    oracle_bad "single_tenant_universe" "could not list t/ in ${RAVEL_S3_BUCKET}"
    return 1
  }
  count="$(printf '%s\n' "$listing" | grep -c '/$' || true)"
  if [[ "$count" -ne 1 ]]; then
    oracle_bad "single_tenant_universe" \
      "expected exactly 1 tenant prefix under t/, found ${count}"
    return 1
  fi
  oracle_ok "single_tenant_universe"
  return 0
}

# Tear RustFS down only if this library started it.
rustfs_down() {
  if [[ "$CHAOS_STARTED_RUSTFS" -eq 1 ]]; then
    log "stopping RustFS"
    docker compose -f "$CHAOS_RUSTFS_COMPOSE" down >/dev/null 2>&1 || true
    CHAOS_STARTED_RUSTFS=0
  fi
}

# ---------------------------------------------------------------------------
# Binary launch helpers. `ravel_cli` / `ravel_server` prefer a prebuilt
# binary on PATH and fall back to `cargo run` from this checkout, so the same
# harness works on an orchestrator with installed binaries and on a dev tree.
# ---------------------------------------------------------------------------

# The tenant-hash flags go to every invocation. On a keyed bucket the
# tenant-hashing verify commands refuse without the key file; on an unkeyed one
# the flag only makes the expectation explicit.
ravel_cli() {
  chaos_tenant_hash_args || return 64
  if chaos_have_command ravel-cli; then
    ravel-cli "${CHAOS_TENANT_HASH_ARGS[@]}" "$@"
  else
    cargo run --quiet -p ravel-cli -- "${CHAOS_TENANT_HASH_ARGS[@]}" "$@"
  fi
}

# Shape of the large fixture: series count and points per series, passed to
# gen_otlp_fixture as `SERIES POINTS`. 1000 x 100 is 10^5 points per export,
# about 3.8 MB of OTLP protobuf, under the server's 16 MiB request-body limit.
# Scenario 2 sends CHAOS_EXPORT_COUNT of them (2 * 10^6 points at the
# defaults) so a compaction has enough input to take measurable time, and
# scenario 1 sends one as its in-flight export. These are starting values;
# tune them from a real nightly run.
CHAOS_FIXTURE_SERIES="${CHAOS_FIXTURE_SERIES:-1000}"
CHAOS_FIXTURE_POINTS="${CHAOS_FIXTURE_POINTS:-100}"
# Strict-ack exports each scenario drives before its kill. Scenario 1 sends
# the one-point fixture this many times; scenario 2 sends a freshly generated
# large fixture this many times.
CHAOS_EXPORT_COUNT="${CHAOS_EXPORT_COUNT:-20}"

# Write a fresh OTLP metrics fixture to stdout. With no arguments it is one
# `demo_requests_total` gauge point at the current wall clock; with
# `SERIES POINTS` it is SERIES series of POINTS points each, all within the
# last two minutes. Prefers a prebuilt example binary on PATH, as the nightly
# lane provides, over a per-invocation `cargo run`.
chaos_gen_fixture() {
  if chaos_have_command gen_otlp_fixture; then
    gen_otlp_fixture "$@"
  else
    cargo run --quiet -p ravel-server --example gen_otlp_fixture -- "$@"
  fi
}

# ---------------------------------------------------------------------------
# Seal wait (scenario 2). An ingest hour becomes compactable only once it is
# sealed: the end of the hour plus the catalog's seal margin. The margin is
# CatalogConfig::default(), compiled in, so it is read from the server's own
# startup log rather than restated here.
# ---------------------------------------------------------------------------

# Extra seconds past the computed seal, so a worker's first pass after the
# wait is not racing the boundary.
CHAOS_SEAL_SAFETY_SECONDS="${CHAOS_SEAL_SAFETY_SECONDS:-120}"
# Interval between progress lines while waiting.
CHAOS_SEAL_PROGRESS_SECONDS="${CHAOS_SEAL_PROGRESS_SECONDS:-600}"

# Print the seal margin in seconds from a server log body: the value of the
# first `seal_margin_secs=N` field (services/ravel-server/src/lib.rs,
# `log_resolved_request_budget`). ANSI colour codes around the field are
# removed first. Returns 1, printing nothing, when the field is absent or its
# value is not a non-negative integer. Pure: no I/O.
chaos_seal_margin_from_log() {
  local body="$1"
  local value
  value="$(awk '
    {
      line = $0
      gsub(/\033\[[0-9;]*m/, "", line)
      at = index(line, "seal_margin_secs=")
      if (at == 0) next
      rest = substr(line, at + length("seal_margin_secs="))
      split(rest, f, /[ \t\r]/)
      print f[1]
      exit
    }' <<<"$body")"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  printf '%s\n' "$value"
}

# Print the unix second at which every ingest hour up to and including the
# one holding `last_ack_unix_s` is sealed, plus the safety margin:
# (floor(last_ack / 3600) + 1) * 3600 + seal_margin + safety.
# Args: last_ack_unix_s seal_margin_s [safety_s]. Returns 64 on a non-integer
# argument. Pure: no clock.
chaos_sealed_at_unix_s() {
  local last_ack="$1" margin="$2" safety="${3:-$CHAOS_SEAL_SAFETY_SECONDS}"
  local v
  for v in "$last_ack" "$margin" "$safety"; do
    [[ "$v" =~ ^[0-9]+$ ]] || return 64
  done
  echo $(( (last_ack / 3600 + 1) * 3600 + margin + safety ))
}

# The clock and the sleep the seal wait uses; tests replace both.
chaos_now_unix_s() {
  date +%s
}
chaos_sleep() {
  sleep "$1"
}

# Block until the clock reaches `sealed_at`. Logs the target and the total
# wait once, then a progress line every CHAOS_SEAL_PROGRESS_SECONDS.
# Args: sealed_at_unix_s.
chaos_wait_until_sealed() {
  local sealed_at="$1"
  local now remaining step
  now="$(chaos_now_unix_s)"
  remaining=$(( sealed_at - now ))
  if [[ "$remaining" -le 0 ]]; then
    log "seal wait: sealed_at=${sealed_at} is already past (now=${now}); no wait"
    return 0
  fi
  log "seal wait: sealed_at=${sealed_at} ($(date -u -d "@${sealed_at}" +%FT%TZ 2>/dev/null || echo "unix ${sealed_at}")), waiting ${remaining}s"
  while [[ "$remaining" -gt 0 ]]; do
    step="$CHAOS_SEAL_PROGRESS_SECONDS"
    [[ "$remaining" -lt "$step" ]] && step="$remaining"
    chaos_sleep "$step"
    now="$(chaos_now_unix_s)"
    remaining=$(( sealed_at - now ))
    if [[ "$remaining" -gt 0 ]]; then
      log "seal wait: ${remaining}s left until sealed_at=${sealed_at}"
    fi
  done
  log "seal wait: done (now=${now})"
}

ravel_server_cmd() {
  # Emit the argv for launching the server, so callers can background it and
  # capture the PID directly (needed to SIGKILL a specific process). Callers
  # read it through a process substitution, which drops this function's exit
  # status, so each scenario also runs chaos_tenant_hash_args up front.
  chaos_tenant_hash_args || return 64
  if chaos_have_command ravel-server; then
    printf '%s\0' ravel-server "${CHAOS_TENANT_HASH_ARGS[@]}" "$@"
  else
    printf '%s\0' cargo run --quiet -p ravel-server -- "${CHAOS_TENANT_HASH_ARGS[@]}" "$@"
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

# Parse one metric family out of a Prometheus text body. Args: body, name,
# then zero or more `label=value` selectors. Prints the sum of every sample of
# `name` whose label set carries each selector exactly, or the empty string
# when no sample matches. Pure: no I/O, so it is testable against a captured
# body (scripts/chaos/lib.test.sh).
#
# A sample renders as `name value [timestamp]` or `name{labels} value
# [timestamp]`, and every family this lane reads carries a `mode` label and,
# for the per-signal families, a `signal` label
# (`ravel_ingest_flushes_by_size_total{mode="all",signal="metrics"} 5`). The
# label set is scanned quote-aware, so a label value holding a space, a comma,
# a brace or an escaped quote cannot shift the value field, and the value is
# the first field after the label set, never an optional timestamp. The name
# must be followed by `{` or whitespace, which keeps `ravel_maintain_units_owned`
# from matching `ravel_maintain_units_owned_total`. With no selector the family
# is summed across label sets: for a labeled counter that is its total across
# signals, for a single-series gauge just its value.
metric_value_from_body() {
  local body="$1"
  local name="$2"
  shift 2
  local selectors=""
  if [[ $# -gt 0 ]]; then
    selectors="$(printf '%s\n' "$@")"
  fi
  # Portable awk (mawk is the default on Debian and Ubuntu runners): no gawk
  # extensions.
  # The selectors travel through the environment, not -v: BSD awk rejects a
  # newline inside a -v value, and -v would also expand backslash escapes.
  CHAOS_METRIC_SELECTORS="$selectors" awk -v n="$name" '
    BEGIN {
      sel = ENVIRON["CHAOS_METRIC_SELECTORS"]
      nsel = 0
      if (sel != "") {
        cnt = split(sel, raw, "\n")
        for (i = 1; i <= cnt; i++) {
          if (raw[i] == "") continue
          eq = index(raw[i], "=")
          if (eq == 0) continue
          nsel++
          want_k[nsel] = substr(raw[i], 1, eq - 1)
          want_v[nsel] = substr(raw[i], eq + 1)
        }
      }
    }
    {
      line = $0
      sub(/^[ \t]+/, "", line)
      if (line == "" || substr(line, 1, 1) == "#") next
      if (substr(line, 1, length(n)) != n) next
      rest = substr(line, length(n) + 1)
      c = substr(rest, 1, 1)
      split("", got)
      if (c == "{") {
        i = 2
        len = length(rest)
        ok = 0
        while (i <= len) {
          ch = substr(rest, i, 1)
          if (ch == " " || ch == "\t" || ch == ",") { i++; continue }
          if (ch == "}") { ok = 1; i++; break }
          eq = index(substr(rest, i), "=")
          if (eq == 0) break
          key = substr(rest, i, eq - 1)
          gsub(/[ \t]+$/, "", key)
          i += eq
          if (substr(rest, i, 1) != "\"") break
          i++
          val = ""
          closed = 0
          while (i <= len) {
            ch = substr(rest, i, 1)
            if (ch == "\\") {
              nx = substr(rest, i + 1, 1)
              if (nx == "n") val = val "\n"; else val = val nx
              i += 2
              continue
            }
            if (ch == "\"") { closed = 1; i++; break }
            val = val ch
            i++
          }
          if (!closed) break
          got[key] = val
        }
        if (!ok) next
        rest = substr(rest, i)
      } else if (c != " " && c != "\t") {
        next
      }
      sub(/^[ \t]+/, "", rest)
      split(rest, fields, /[ \t]+/)
      if (fields[1] == "") next
      for (s = 1; s <= nsel; s++) {
        if (!(want_k[s] in got) || got[want_k[s]] != want_v[s]) next
      }
      sum += fields[1]
      hit = 1
    }
    END { if (hit) print sum; else print "" }' \
    <<<"$body"
}

# Read a counter/gauge value from a server's /metrics. Args: base_url, name,
# then optional `label=value` selectors (see metric_value_from_body). Prints
# the value, or the empty string if no sample matches. Returns 1 only when the
# scrape itself fails (server unreachable).
metric_value() {
  local base_url="$1"
  local name="$2"
  shift 2
  local body
  body="$(curl --silent --fail --max-time 5 "${base_url}/metrics")" || return 1
  metric_value_from_body "$body" "$name" "$@"
}

# Block until a metric reaches at least `threshold`, or a deadline passes.
# Args: base_url, name, threshold, deadline_seconds, then optional
# `label=value` selectors. Returns 0 on reaching the threshold, 1 on
# timeout/unreachable.
wait_for_metric_at_least() {
  local base_url="$1"
  local name="$2"
  local threshold="$3"
  local deadline_seconds="$4"
  shift 4
  local waited=0
  local value
  while [[ "$waited" -lt "$deadline_seconds" ]]; do
    value="$(metric_value "$base_url" "$name" "$@")" || value=""
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
# Scenario 1 keys off the count of flush attempts. No family counts every
# flush, so it is the sum of the five per-trigger families the server renders
# (services/ravel-server/src/metrics.rs): size, age, adaptive age, age floor,
# and manual. Each FlushTrigger increments exactly one of them
# (`IngestMetrics::record_flush`, crates/ravel-ingest/src/metrics.rs), so the
# sum counts each flush once, and each is incremented at flush ATTEMPT time,
# so a rise means a flush has STARTED but not necessarily completed. The
# adaptive family is rendered only for pipelines that carry adaptive
# counters; an absent family adds nothing. Only the metrics pipeline's samples count (`signal="metrics"`):
# the load is OTLP metrics, and a logs or traces flush in the same process
# would otherwise fire the kill outside any flush this scenario's writes are in.
#
# Scenario 2 keys off the compaction lifecycle as worker A's log shows it.
# Neither crates/ravel-maintain nor the server emits a per-bucket "compaction
# started" line or metric that rises before the publish, so the closest
# readable boundary is the end of a pass: the server logs
# `"maintenance: retention + compaction pass complete"` with `compacted=N`
# for each owned shard once the signal's pass is over
# (services/ravel-server/src/maintain.rs:2710), after every publish of that
# pass (`"compaction record published"`,
# crates/ravel-maintain/src/publish.rs:331). See
# compaction_unfinished_in_log for what this can and cannot distinguish.
# ---------------------------------------------------------------------------

CHAOS_FLUSH_METRICS=(
  ravel_ingest_flushes_by_size_total
  ravel_ingest_flushes_by_age_total
  ravel_ingest_flushes_by_age_adaptive_total
  ravel_ingest_flushes_by_age_floor_total
  ravel_ingest_flushes_manual_total
)
CHAOS_FLUSH_SELECTOR="signal=metrics"
# Scrapes per second while waiting for a flush to start. A flush can finish
# inside one second, so this polls faster than wait_for_metric_at_least.
CHAOS_FLUSH_POLLS_PER_SECOND="${CHAOS_FLUSH_POLLS_PER_SECOND:-5}"
CHAOS_COMPACTION_PUBLISH_MARKER="compaction record published"
CHAOS_COMPACTION_PASS_MARKER="maintenance: retention + compaction pass complete"
# Flush-attempt count read by the poll that saw the rise; empty until then.
CHAOS_FLUSH_ATTEMPTS_SEEN=""
# curl exit codes for a request the server never answered: 7 (connect
# refused), 18 (transfer closed early), 28 (timed out), 52 (empty reply),
# 55 (send failed), 56 (receive failed, including a reset connection).
# run_capture and drive_one_export return curl's code unchanged.
CHAOS_CURL_UNANSWERED_CODES=(7 18 28 52 55 56)

# Print the metrics pipeline's flush-attempt count from a /metrics body: the
# sum of CHAOS_FLUSH_METRICS under CHAOS_FLUSH_SELECTOR. Prints the empty
# string when none of the families has a matching sample. Pure: no I/O.
flush_attempts_from_body() {
  local body="$1"
  local name v total=""
  for name in "${CHAOS_FLUSH_METRICS[@]}"; do
    v="$(metric_value_from_body "$body" "$name" "$CHAOS_FLUSH_SELECTOR")"
    [[ "$v" =~ ^[0-9]+([.][0-9]+)?$ ]] || continue
    total=$(( ${total:-0} + ${v%.*} ))
  done
  printf '%s\n' "$total"
}

# Scrape a server's flush-attempt count. Returns 1 when the scrape fails.
flush_attempts() {
  local base_url="$1"
  local body
  body="$(curl --silent --fail --max-time 5 "${base_url}/metrics")" || return 1
  flush_attempts_from_body "$body"
}

# Wait until the flush-attempt count has risen past `baseline`, i.e. a flush
# of any trigger has started since the baseline was read. This is the
# scenario-1 "mid-flush" trigger. Args: base_url baseline deadline_seconds.
# Returns 0 on the rise, with the count that poll read in
# CHAOS_FLUSH_ATTEMPTS_SEEN, so the caller can kill without another scrape;
# 1 when the deadline passes first.
# Run drive_one_export in the background with its stdout in a file, and set
# CHAOS_BG_EXPORT_PID. The subshell drops the caller's ERR trap and errexit:
# under the scenarios' `set -eE` and `trap 'exit 3' ERR`, a failing export
# would otherwise exit 3, and `wait` would report 3 instead of curl's code.
# Args: http_addr fixture_path out_path.
chaos_start_background_export() {
  ( trap - ERR; set +e; drive_one_export "$1" "$2" >"$3" ) &
  CHAOS_BG_EXPORT_PID=$!
}

wait_for_flush_started() {
  local base_url="$1"
  local baseline="$2"
  local deadline_seconds="$3"
  local interval
  interval="$(awk -v n="$CHAOS_FLUSH_POLLS_PER_SECOND" 'BEGIN { printf "%.3f", 1 / n }')"
  local start value
  start="$(date +%s)"
  while (( $(date +%s) - start < deadline_seconds )); do
    value="$(flush_attempts "$base_url")" || value=""
    if [[ "$value" =~ ^[0-9]+$ ]] && [[ "$value" -gt "$baseline" ]]; then
      CHAOS_FLUSH_ATTEMPTS_SEEN="$value"
      return 0
    fi
    sleep "$interval"
  done
  return 1
}

# Print 1 when the in-flight export's exit code means the server answered it
# before the kill (a success, an HTTP error status such as curl's 22, or a
# response with no commit token), 0 when it is one of
# CHAOS_CURL_UNANSWERED_CODES. Args: the export's exit code. Pure.
chaos_inflight_answered() {
  local rc="$1" code
  for code in "${CHAOS_CURL_UNANSWERED_CODES[@]}"; do
    if [[ "$rc" == "$code" ]]; then
      echo 0
      return 0
    fi
  done
  echo 1
}

# The scenario-1 summary line for where the kill landed. Args: flush_observed
# (0|1), inflight_export_answered (0|1, see chaos_inflight_answered),
# baseline, attempts read when the rise was detected. The kill landed
# mid-flush when a flush had started and the export sent to fill it was still
# unanswered when the server died.
kill_timing_line() {
  local observed="$1" answered="$2" baseline="$3" at_kill="$4"
  if [[ "$observed" -eq 1 && "$answered" -eq 0 ]]; then
    echo "KILL-TIMING: mid-flush=yes (flush attempts ${baseline} -> ${at_kill}, in-flight export unacknowledged at the kill)"
  elif [[ "$observed" -eq 1 ]]; then
    echo "KILL-TIMING: mid-flush=no (flush attempts ${baseline} -> ${at_kill}, but the server answered the in-flight export before the kill)"
  else
    echo "KILL-TIMING: mid-flush=no (no flush attempt observed; attempts stayed at ${baseline})"
  fi
}

# Return 0 when a maintain worker's log shows compaction work it had not
# finished, 1 when its last publish is covered by a finished pass. Args: the
# log body. Pure.
#
# Unfinished means the log carries no CHAOS_COMPACTION_PUBLISH_MARKER, or its
# last one is not followed by a CHAOS_COMPACTION_PASS_MARKER line with
# `compacted=N`, N >= 1. The pass lines are logged only after every shard of
# the signal's pass has returned, so one after the last publish means no
# merge of that pass was still running. With no start signal to read, this
# cannot tell a pass still merging a bucket from one that published its last
# bucket and is still in retention or sweep work (both read as unfinished),
# and it cannot see a merge begun by a later pass that has not published
# (that reads as finished).
compaction_unfinished_in_log() {
  local body="$1"
  local positions
  positions="$(CHAOS_PUB="$CHAOS_COMPACTION_PUBLISH_MARKER" \
    CHAOS_PASS="$CHAOS_COMPACTION_PASS_MARKER" awk '
    BEGIN { pub = ENVIRON["CHAOS_PUB"]; pass = ENVIRON["CHAOS_PASS"]; p = 0; c = 0 }
    {
      line = $0
      gsub(/\033\[[0-9;]*m/, "", line)
      if (index(line, pub) > 0) p = NR
      if (index(line, pass) > 0 && match(line, /[ \t]compacted=[0-9]+/)) {
        n = substr(line, RSTART + 11, RLENGTH - 11) + 0
        if (n >= 1) c = NR
      }
    }
    END { print p, c }' <<<"$body")"
  local last_publish="${positions% *}" last_pass="${positions#* }"
  if [[ "$last_publish" -eq 0 || "$last_publish" -gt "$last_pass" ]]; then
    return 0
  fi
  return 1
}

# One line summarising what a maintain worker did, from its log past line
# `from` (0 for the whole log): compaction records published, compaction pass
# lines, and the summed compacted= and not_sealed= figures of those lines.
# Args: name log_file [from_line].
chaos_worker_activity_line() {
  local name="$1" file="$2" from="${3:-0}" body=""
  if [[ -r "$file" ]]; then
    body="$(tail -n "+$(( from + 1 ))" "$file")"
  fi
  CHAOS_PUB="$CHAOS_COMPACTION_PUBLISH_MARKER" CHAOS_PASS="$CHAOS_COMPACTION_PASS_MARKER" \
    awk -v name="$name" '
    BEGIN { pub = ENVIRON["CHAOS_PUB"]; pass = ENVIRON["CHAOS_PASS"]; p = 0; n = 0; c = 0; u = 0 }
    {
      line = $0
      gsub(/\033\[[0-9;]*m/, "", line)
      if (index(line, pub) > 0) p++
      if (index(line, pass) > 0) {
        n++
        if (match(line, /[ \t]compacted=[0-9]+/)) c += substr(line, RSTART + 11, RLENGTH - 11) + 0
        if (match(line, /[ \t]not_sealed=[0-9]+/)) u += substr(line, RSTART + 12, RLENGTH - 12) + 0
      }
    }
    END { printf "WORKER-ACTIVITY: %s publishes=%d passes=%d compacted=%d not_sealed=%d\n", name, p, n, c, u }' <<<"$body"
}

# Wait until worker A owns at least one unit and its log shows unfinished
# compaction work (compaction_unfinished_in_log). This is the scenario-2
# "mid-compaction" trigger. Args: base_url log_file deadline_seconds. Returns
# 0 when seen, 1 when the deadline passes first.
wait_for_compaction_in_flight() {
  local base_url="$1"
  local log_file="$2"
  local deadline_seconds="$3"
  local waited=0
  local owned
  local body
  while [[ "$waited" -lt "$deadline_seconds" ]]; do
    owned="$(metric_value "$base_url" ravel_maintain_units_owned)" || owned=""
    if [[ "${owned%.*}" =~ ^[0-9]+$ ]] && [[ "${owned%.*}" -gt "${CHAOS_A_MAX_UNITS_OWNED:-0}" ]]; then
      CHAOS_A_MAX_UNITS_OWNED="${owned%.*}"
    fi
    # Capture the log into a variable, then test it: no gate pipeline.
    body=""
    if [[ -r "$log_file" ]]; then
      body="$(cat "$log_file")"
    fi
    if [[ "${owned%.*}" =~ ^[0-9]+$ ]] && [[ "${owned%.*}" -ge 1 ]] \
      && compaction_unfinished_in_log "$body"; then
      return 0
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

# Extract the strict-ack commit tokens from a captured response-header block,
# one per line on stdout. Returns 1 when there is none. Pure: no I/O.
#
# The `x-ravel-commit-token` value is one `CommitToken::encode()` per shard
# the write flushed through, comma-joined
# (services/ravel-server/src/otlp_http.rs, `encode_commit_tokens`), and each is
# an opaque URL-safe base64 string. They are split on the comma and otherwise
# passed through untouched: `min_commit_token` is repeatable and takes one
# token per occurrence, so the joined header value is not itself a valid
# token. The header name is folded by hand because gawk's IGNORECASE is
# silently ignored by mawk, the default awk on Debian and Ubuntu runners.
commit_tokens_from_headers() {
  local headers="$1"
  local tokens
  tokens="$(awk '
    {
      line = $0
      sub(/\r$/, "", line)
      colon = index(line, ":")
      if (colon == 0) next
      if (tolower(substr(line, 1, colon - 1)) != "x-ravel-commit-token") next
      value = substr(line, colon + 1)
      cnt = split(value, parts, ",")
      for (i = 1; i <= cnt; i++) {
        t = parts[i]
        gsub(/^[ \t]+|[ \t]+$/, "", t)
        if (t != "") print t
      }
    }' <<<"$headers")"
  if [[ -z "$tokens" ]]; then
    return 1
  fi
  printf '%s\n' "$tokens"
}

# POST one OTLP metrics export and echo its strict-ack commit tokens on
# stdout, one per line (see commit_tokens_from_headers). Returns nonzero if
# the export was not accepted or carried no token. The caller records every
# token as an acked-before-kill write.
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
  commit_tokens_from_headers "$header_body"
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
  query_body_shows_series "$body" "$series"
}

# Report whether a /api/v1/query response body is a success carrying a sample
# of `series`. Pure: no I/O. The name is matched as the `__name__` label
# value, so an error message that quotes the series, or a longer series name
# that contains it, does not read as visible.
query_body_shows_series() {
  local body="$1"
  local series="$2"
  if [[ "$body" == *'"status":"success"'* \
      && "$body" == *"\"__name__\":\"${series}\""* ]]; then
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
# RustFS or drives load: they read the post-kill/post-restart world the
# scenario scripts set up.
# ===========================================================================

# 1. strict-ack-implies-durable.
#    Every write acked under strict ack before the kill is durable and
#    queryable after restart.
#
#    Commit tokens are opaque, versioned, URL-safe-base64 strings
#    (`v2:<shard>:<writer>:<epoch>:<seq>:<hour>`, base64-encoded --
#    crates/ravel-types CommitToken), NOT integers. They are matched and passed
#    through as strings; nothing here does arithmetic on a token or tests one
#    with a numeric regex. A prior version computed `highest_token + 1` to probe
#    for a leaked partial flush "one past" the last ack: that assumed an integer
#    frontier the token type has never had (base64 has no numeric successor, and
#    the catalog resolves `min_commit_token` by an exact GET of the pinned
#    commit key, so a fabricated token would resolve nothing). The
#    partial-visibility guarantee is instead discharged by the two oracles that
#    can express it against opaque tokens: custody-and-catalog verify (no live
#    record references an unacked/partial object) and, for scenario 2, the
#    conservation gate. This oracle asserts the direction a per-token query CAN
#    prove: acked implies durable-and-queryable after restart.
#
#    Args: http_addr, series, <acked_token...>
oracle_strict_ack_implies_durable() {
  local http_addr="$1"
  local series="$2"
  shift 2
  local acked_tokens=("$@")
  local name="strict-ack-implies-durable"

  if [[ "${#acked_tokens[@]}" -eq 0 ]]; then
    oracle_bad "$name" "no strict-acked tokens recorded before the kill"
    return 1
  fi

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

# Prints the number of lines in a log file, or 0 when it is not readable.
chaos_line_count() {
  if [[ -r "$1" ]]; then
    local n
    n="$(wc -l <"$1")"
    printf '%s\n' "${n//[[:space:]]/}"
  else
    printf '0\n'
  fi
}

# 4. conservation-holds, with a could-not-measure outcome.
#    The conservation-abort counter on the survivor did not advance across
#    the interruption (the gate never had to reject a record-dropping
#    compaction), and the survivor published a compaction record after
#    takeover.
#
#    Args: worker_a_log_file (read after the SIGKILL, so it is A's state at
#    the kill), survivor_base_url, conservation_aborts_baseline,
#    survivor_log_file, survivor_log_lines_at_kill (default 0). Both workers
#    run before the kill, so only survivor log lines past that count are a
#    publish after takeover; one B wrote before the kill does not pass.
#
#    The abort counter is checked first, and an unreadable or risen counter
#    is a FAIL whatever A's log shows. Only a missing survivor publish
#    depends on A: a FAIL when A's log shows unfinished work
#    (compaction_unfinished_in_log), else could-not-measure, recorded in
#    ORACLE_UNMEASURED with CHAOS_UNMEASURED_EXIT returned.
oracle_conservation_or_unmeasured() {
  local worker_a_log="$1"
  local survivor_url="$2"
  local aborts_baseline="$3"
  local survivor_log="$4"
  local survivor_from="${5:-0}"
  local name="conservation-holds"
  if [[ ! "$survivor_from" =~ ^[0-9]+$ ]]; then
    oracle_bad "$name" "survivor log line count at the kill is not an integer: '${survivor_from}'"
    return 1
  fi

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

  # Logs are captured into variables and tested from the variables.
  local survivor_body="" a_body=""
  if [[ -r "$survivor_log" ]]; then
    survivor_body="$(tail -n "+$(( survivor_from + 1 ))" "$survivor_log")"
  fi
  if [[ "$survivor_body" == *"$CHAOS_COMPACTION_PUBLISH_MARKER"* ]]; then
    oracle_ok "$name"
    return 0
  fi
  if [[ -r "$worker_a_log" ]]; then
    a_body="$(cat "$worker_a_log")"
  fi
  # With no publish in A's log there is no unit it can be shown to have been
  # merging: it may have been mid-way through its first, or held nothing to
  # compact. Its log cannot tell the two apart, so this is not a verdict.
  if [[ "$a_body" != *"$CHAOS_COMPACTION_PUBLISH_MARKER"* ]]; then
    ORACLE_UNMEASURED+=("${name}: could not measure: worker A published no compaction record before the kill, so its log cannot show an interrupted unit")
    log "could not measure: worker A's log has no '${CHAOS_COMPACTION_PUBLISH_MARKER}' line, and the survivor published nothing after the kill"
    return "$CHAOS_UNMEASURED_EXIT"
  fi
  if compaction_unfinished_in_log "$a_body"; then
    oracle_bad "$name" \
      "no '${CHAOS_COMPACTION_PUBLISH_MARKER}' after takeover: interrupted compaction did not complete"
    return 1
  fi
  ORACLE_UNMEASURED+=("${name}: could not measure: worker A finished its compaction pass before the kill")
  log "could not measure: worker A's log shows its last '${CHAOS_COMPACTION_PUBLISH_MARKER}' followed by a '${CHAOS_COMPACTION_PASS_MARKER}' line with compacted>=1, and the survivor published nothing"
  return "$CHAOS_UNMEASURED_EXIT"
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
