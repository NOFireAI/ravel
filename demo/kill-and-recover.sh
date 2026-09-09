#!/usr/bin/env bash
# Kill-and-recover demo for the container-first quickstart (ADR-0081 decision 9).
#
# Ravel's central claim is that object storage is the only durable component and
# every compute process is disposable. This script demonstrates it instead of
# arguing it:
#
#   1. ingest one OTLP metric export under STRICT acknowledgement, capturing the
#      `x-ravel-commit-token` response header (strict ack means the data is
#      already on the object store when the response is written, so nothing in
#      the server process is load-bearing after it);
#   2. HARD-kill the ravel-server container with SIGKILL (`docker compose kill`,
#      never `stop`): a graceful shutdown would let the process flush and would
#      prove nothing, so the process gets no chance to run any code at all;
#   3. remove the dead container and start a fresh one, so the replacement comes
#      up with an empty container filesystem and the only surviving state is
#      what is in MinIO;
#   4. query the ingested sample back, passing the captured token as
#      `min_commit_token`.
#
# It is assertive, and it is the artifact under test: it exits non-zero if the
# ingest returned no token, if the kill was not ungraceful, if the container was
# not actually replaced, if the restarted server never becomes ready, if the
# token comes back unsatisfiable, or if the sample is absent. `curl` exits 0 on
# an HTTP 401 and on a `{"status":"error"}` JSON envelope, so every check below
# asserts the response status and content, never the process exit code alone.
#
# It assumes the compose stack is ALREADY UP (same contract as
# demo/walkthrough.sh and scripts/check-readme-commands.sh); bringing MinIO,
# ravel-server, the collector, and Grafana up is
# `docker compose -f deploy/docker-compose/ravel.yml up -d`'s job.
#
# The GIF in the README is a recording of a passing run of this script.
# Recording is a local step; CI runs the assertions (ADR-0081 decision 9).
#
# Shell-safety (CLAUDE.md "Writing gate and poll shell"): exit codes are
# captured as `cmd || code=$?` on the same line; no variable is named `status`,
# `path`, `argv`, or `PWD`; no checked command is piped through grep/head/tail.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

COMPOSE_FILE="${COMPOSE_FILE:-$repo_root/deploy/docker-compose/ravel.yml}"
RAVEL_HTTP_BASE="${RAVEL_HTTP_BASE:-http://127.0.0.1:4318}"
RAVEL_TENANT_TOKEN="${RAVEL_TENANT_TOKEN:-demo-token}"
SERVICE="${RAVEL_KILL_RECOVER_SERVICE:-ravel-server}"

# Readiness and query waits are BOUNDED POLLS against named constants, never a
# fixed sleep (ADR-0081 decision 5). A fixed sleep either flakes on a slow
# runner or wastes time on a fast one; a bounded poll fails loudly on a real
# hang and returns immediately otherwise.
READY_TIMEOUT_SECONDS="${RAVEL_KILL_RECOVER_READY_TIMEOUT_SECONDS:-120}"
READY_POLL_SECONDS="${RAVEL_KILL_RECOVER_READY_POLL_SECONDS:-2}"
QUERY_TIMEOUT_SECONDS="${RAVEL_KILL_RECOVER_QUERY_TIMEOUT_SECONDS:-60}"
QUERY_POLL_SECONDS="${RAVEL_KILL_RECOVER_QUERY_POLL_SECONDS:-2}"
# Per-request bounds, so a server that accepts a connection and never answers
# cannot hang past the overall budget above.
CONNECT_TIMEOUT_SECONDS="${RAVEL_CONNECT_TIMEOUT_SECONDS:-5}"
REQUEST_TIMEOUT_SECONDS="${RAVEL_REQUEST_TIMEOUT_SECONDS:-10}"

# ravel-server registers /healthz, /readyz, /-/healthy, and /-/ready
# (services/ravel-server/src/health.rs). /readyz is the one that means "this
# process can serve a query", which is what the wait after the restart needs.
RAVEL_READY_PATH="${RAVEL_KILL_RECOVER_READY_PATH:-/readyz}"

# The default PromQL lookback delta is 5 minutes (ADR-0007,
# crates/ravel-promql/src/eval.rs). The final instant query has to find a sample
# written before the kill, so the whole kill-restart-wait sequence must fit
# inside that window or the query legitimately returns nothing for a reason that
# is not data loss. The budget is checked rather than assumed, because both
# waits are env-overridable.
PROMQL_LOOKBACK_SECONDS=300

# The SIGKILL exit code Docker reports for a container whose PID 1 was killed
# (128 + 9). Dockerfile.prebuilt's server target execs ravel-server as PID 1
# directly, with no shell wrapper, so the server itself takes the signal.
SIGKILL_EXIT_CODE=137

log() { echo "[kill-and-recover] $*" >&2; }

fail() {
  log "FAILED: $*"
  exit 1
}

# --- Preflight -------------------------------------------------------------

if ! command -v curl >/dev/null 2>&1; then
  fail "curl not found on PATH"
fi
if ! command -v docker >/dev/null 2>&1; then
  fail "docker not found on PATH; this demo kills a container, so it needs one"
fi
if [[ ! -f "$COMPOSE_FILE" ]]; then
  fail "compose file not found: $COMPOSE_FILE"
fi
if (( READY_TIMEOUT_SECONDS + QUERY_TIMEOUT_SECONDS >= PROMQL_LOOKBACK_SECONDS )); then
  fail "READY_TIMEOUT_SECONDS ($READY_TIMEOUT_SECONDS) + QUERY_TIMEOUT_SECONDS" \
    "($QUERY_TIMEOUT_SECONDS) must stay under the ${PROMQL_LOOKBACK_SECONDS}s PromQL lookback," \
    "or a timed-out run would look like data loss when it is not"
fi

compose() { docker compose --file "$COMPOSE_FILE" "$@"; }

container_before=$(compose ps --quiet "$SERVICE" || true)
if [[ -z "$container_before" ]]; then
  fail "no running '$SERVICE' container; bring the stack up first with" \
    "'docker compose -f $COMPOSE_FILE up -d'"
fi
log "target container: $container_before"

# --- OTLP protobuf assembly ------------------------------------------------
#
# Mirrors demo/walkthrough.sh: a minimal ExportMetricsServiceRequest built here
# in bash with a CURRENT timestamp. The compose quickstart carries no Rust
# toolchain to run gen_otlp_fixture, and ravel rejects data points more than 2
# hours old, so a static checked-in fixture would fail non-deterministically.
# The field numbers are the frozen OTLP metrics wire contract.

# Space-separated decimal bytes of a base-128 varint.
varint() {
  local n=$1 out="" b
  while :; do
    b=$(( n & 0x7f ))
    n=$(( n >> 7 ))
    if (( n != 0 )); then
      out+="$(( b | 0x80 )) "
    else
      out+="$b "
      break
    fi
  done
  printf '%s' "$out"
}

# Eight little-endian decimal bytes of an unsigned 64-bit integer.
le64() {
  local n=$1 i out=""
  for (( i = 0; i < 8; i++ )); do
    out+="$(( n & 0xff )) "
    n=$(( n >> 8 ))
  done
  printf '%s' "$out"
}

# A fixed64-wire field: tag, then the eight-byte payload verbatim.
field_fixed64() {
  printf '%s %s' "$(( ($1 << 3) | 1 ))" "$2"
}

# A length-delimited field: tag, varint(len), payload.
field_len_delimited() {
  local len
  len=$(wc -w <<<"$2")
  printf '%s %s %s' "$(( ($1 << 3) | 2 ))" "$(varint "$len")" "$2"
}

# The metric name carries a per-run suffix, and that is what keeps the final
# assertion from being vacuous. `minio-data/` is deliberately reused across runs
# (ADR-0081 Consequences), so a fixed name would let a PREVIOUS run's surviving
# sample satisfy this run's query even if this run's write were lost. A name no
# earlier run can have written means the sample the query finds is the one this
# run ingested, or the script fails.
run_suffix="$(date +%s)_$$"
metric_name="${RAVEL_KILL_RECOVER_METRIC:-kill_recover_demo}_${run_suffix}"

# Second granularity keeps this portable (BSD `date` has no %N); well inside the
# 2-hour freshness window ravel enforces.
now_ns=$(( $(date +%s) * 1000000000 ))

# NumberDataPoint { time_unix_nano = 3 (fixed64); as_double = 4 (double) = 1.0 }.
# 1.0 as IEEE-754 little-endian is 00 00 00 00 00 00 F0 3F.
ndp="$(field_fixed64 3 "$(le64 "$now_ns")") $(field_fixed64 4 "0 0 0 0 0 0 240 63")"
# Gauge { data_points = 1 }.
gauge="$(field_len_delimited 1 "$ndp")"
# Metric name bytes as decimals.
name_bytes=$(printf '%s' "$metric_name" | od -An -tu1 | tr -s ' \n' ' ')
# Metric { name = 1 (string); gauge = 5 (message) }.
metric="$(field_len_delimited 1 "$name_bytes") $(field_len_delimited 5 "$gauge")"
# ScopeMetrics { metrics = 2 }; ResourceMetrics { scope_metrics = 2 };
# ExportMetricsServiceRequest { resource_metrics = 1 }.
scope_metrics="$(field_len_delimited 2 "$metric")"
resource_metrics="$(field_len_delimited 2 "$scope_metrics")"
request="$(field_len_delimited 1 "$resource_metrics")"

fixture=$(mktemp)
headers=$(mktemp)
body=$(mktemp)
cleanup() { rm -f "$fixture" "$headers" "$body"; }
trap cleanup EXIT

# Emit the decimal byte list as raw bytes. Intentional word splitting over the
# space-separated decimals.
# shellcheck disable=SC2086
{
  for b in $request; do
    printf "\\$(printf '%03o' "$b")"
  done
} >"$fixture"

# --- 1. Ingest under strict acknowledgement --------------------------------

log "ingesting one OTLP metric export ($metric_name) to $RAVEL_HTTP_BASE/v1/metrics"
ingest_code=0
http_code=$(curl --silent --show-error \
  --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$REQUEST_TIMEOUT_SECONDS" \
  --dump-header "$headers" \
  --output "$body" \
  --write-out '%{http_code}' \
  -X POST "$RAVEL_HTTP_BASE/v1/metrics" \
  -H "Authorization: Bearer $RAVEL_TENANT_TOKEN" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary "@$fixture") || ingest_code=$?

if (( ingest_code != 0 )); then
  fail "ingest request could not be completed (curl exit $ingest_code)"
fi
if [[ "$http_code" != "200" ]]; then
  fail "ingest returned HTTP $http_code, expected 200 (body: $(<"$body"))"
fi

commit_token=$(grep -i '^x-ravel-commit-token:' "$headers" | sed 's/^[^:]*:[[:space:]]*//' | tr -d '\r' || true)
if [[ -z "$commit_token" ]]; then
  # Strict mode is the default and is the only mode that returns a token. No
  # token means either the write was buffered (acknowledged but not durable) or
  # nothing flushed, and in both cases there is no durability claim left to
  # test, so the run is a failure rather than a pass.
  fail "ingest returned HTTP 200 but no x-ravel-commit-token header;" \
    "without a strict-mode commit token there is no durability claim to prove"
fi
log "strict ack; commit token: $commit_token"
echo "commit token: $commit_token"

# --- 2. Hard-kill the server -----------------------------------------------
#
# SIGKILL, not SIGTERM: `docker compose kill` (default signal SIGKILL), never
# `docker compose stop`. A graceful shutdown would let the process flush on its
# way out, which would prove only that shutdown code works. The claim under test
# is that an instant, ungraceful death loses nothing, because the acknowledged
# data was already on the object store before the ack was written.

log "hard-killing $SERVICE with SIGKILL (no flush, no shutdown hook, no warning)"
compose kill --signal SIGKILL "$SERVICE"

inspect_code=0
state=$(docker inspect --format '{{.State.Running}} {{.State.ExitCode}}' "$container_before") || inspect_code=$?
if (( inspect_code != 0 )); then
  fail "could not inspect $SERVICE container $container_before after the kill" \
    "(docker inspect exit $inspect_code); its outcome is unverified"
fi
if [[ "$state" != "false $SIGKILL_EXIT_CODE" ]]; then
  # If the container is still running, the kill did not land and everything
  # after this would test nothing. If it exited with some other code, the
  # process shut down through some path other than SIGKILL, which is exactly the
  # graceful exit this demo must not credit.
  fail "expected $SERVICE to be dead with exit code $SIGKILL_EXIT_CODE (SIGKILL)," \
    "got running/exit: $state"
fi
log "$SERVICE is dead (exit $SIGKILL_EXIT_CODE, killed by SIGKILL)"

# --- 3. Replace it with a fresh container ----------------------------------
#
# Removing the dead container discards its writable layer, so the replacement
# starts from the image with an empty container filesystem. Restarting the same
# container would leave any local state intact and would weaken the claim: the
# point is that the replacement process shares nothing with the dead one except
# the MinIO bucket.

log "removing the dead container (discards its filesystem) and starting a fresh one"
compose rm --force --stop "$SERVICE"
compose up --detach "$SERVICE"

container_after=$(compose ps --quiet "$SERVICE" || true)
if [[ -z "$container_after" ]]; then
  fail "no $SERVICE container after restart"
fi
if [[ "$container_after" == "$container_before" ]]; then
  fail "restart reused container $container_after; the demo requires a NEW container" \
    "so that no local state can survive"
fi
log "fresh container: $container_after (was $container_before)"

# --- 4. Wait for readiness, bounded ----------------------------------------

log "waiting up to ${READY_TIMEOUT_SECONDS}s for $RAVEL_HTTP_BASE$RAVEL_READY_PATH"
ready=false
ready_deadline=$(( $(date +%s) + READY_TIMEOUT_SECONDS ))
while (( $(date +%s) < ready_deadline )); do
  probe_code=0
  http_code=$(curl --silent --show-error \
    --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$REQUEST_TIMEOUT_SECONDS" \
    --output /dev/null --write-out '%{http_code}' \
    "$RAVEL_HTTP_BASE$RAVEL_READY_PATH") || probe_code=$?
  if (( probe_code == 0 )) && [[ "$http_code" == "200" ]]; then
    ready=true
    break
  fi
  sleep "$READY_POLL_SECONDS"
done
if [[ "$ready" != "true" ]]; then
  fail "restarted $SERVICE did not report ready within ${READY_TIMEOUT_SECONDS}s"
fi
log "restarted $SERVICE is ready"

# --- 5. Read the pre-kill write back, by its own commit token --------------
#
# The token path is the assertion. Per docs/consistency-model.md
# "Read-your-write", a query carrying `min_commit_token` GETs the token's exact
# commit-record key and either includes its segments or fails with an
# unsatisfiable token; it never silently serves stale data. So on this path a
# missing sample cannot be explained away as freshness lag: it means the
# acknowledged data is gone.
#
# The poll exists for transient storage faults (both the unsatisfiable-token and
# storage-unavailable classes map to a retryable HTTP 503), not to paper over
# absence. Data that was actually lost never appears, and the bounded loop then
# fails.

log "querying $metric_name back with min_commit_token (up to ${QUERY_TIMEOUT_SECONDS}s)"
query_ok=false
query_body=""
query_http=""
query_deadline=$(( $(date +%s) + QUERY_TIMEOUT_SECONDS ))
while (( $(date +%s) < query_deadline )); do
  query_code=0
  query_http=$(curl --silent --show-error \
    --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$REQUEST_TIMEOUT_SECONDS" \
    --output "$body" --write-out '%{http_code}' \
    -H "Authorization: Bearer $RAVEL_TENANT_TOKEN" \
    --get "$RAVEL_HTTP_BASE/api/v1/query" \
    --data-urlencode "query=$metric_name" \
    --data-urlencode "min_commit_token=$commit_token") || query_code=$?
  query_body=$(<"$body")
  if (( query_code == 0 )) && [[ "$query_http" == "200" ]] \
    && grep -q '"status":"success"' <<<"$query_body" \
    && ! grep -q '"result":\[\]' <<<"$query_body" \
    && grep -q "$metric_name" <<<"$query_body"; then
    query_ok=true
    break
  fi
  sleep "$QUERY_POLL_SECONDS"
done

echo "query result: $query_body"

if [[ "$query_ok" != "true" ]]; then
  if grep -q 'commit token is not yet visible' <<<"$query_body"; then
    fail "commit token $commit_token came back UNSATISFIABLE after the kill." \
      "Ravel acknowledged this write in strict mode, so its commit record must" \
      "have been durable on the object store before the ack. DATA LOSS."
  fi
  fail "did not read back $metric_name by its own commit token within" \
    "${QUERY_TIMEOUT_SECONDS}s (last HTTP $query_http): $query_body"
fi

# Informational only, and deliberately NOT asserted. Without a
# `min_commit_token` a query sees "some recent consistent snapshot; freshness is
# bounded by listing behavior, not guaranteed" (docs/consistency-model.md
# "Read-your-write"). Asserting that this path sees the sample immediately after
# a restart would assert something Ravel never promised, and would produce a
# flaky test whose failures mean nothing.
untokened_code=0
untokened_body=$(curl --silent --show-error \
  --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$REQUEST_TIMEOUT_SECONDS" \
  -H "Authorization: Bearer $RAVEL_TENANT_TOKEN" \
  --get "$RAVEL_HTTP_BASE/api/v1/query" \
  --data-urlencode "query=$metric_name") || untokened_code=$?
if (( untokened_code == 0 )); then
  echo "query result without a token (informational, not asserted): $untokened_body"
fi

log "PASSED: the server was SIGKILLed and replaced by a fresh container, and the"
log "sample acknowledged before the kill read back by its own commit token."
log "Nothing survived in the process; everything survived in the object store."
