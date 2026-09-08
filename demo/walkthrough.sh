#!/usr/bin/env bash
# Read-your-write walkthrough for the container-first quickstart (ADR-0081).
#
# Ingests one OTLP metric export, captures the `x-ravel-commit-token` response
# header, queries the data back passing that token as `min_commit_token`, and
# prints both. It is assertive: it exits non-zero if the token is absent or the
# query does not return the ingested series.
#
# It assumes the compose stack is ALREADY UP (same contract as
# scripts/check-readme-commands.sh); bringing MinIO, ravel-server, the collector,
# and Grafana up is `docker compose -f deploy/docker-compose/ravel.yml up -d`'s
# job. Run this after the stack is healthy.
#
# The OTLP payload is a minimal ExportMetricsServiceRequest built here in bash
# with a CURRENT timestamp. The compose quickstart carries no Rust toolchain to
# run gen_otlp_fixture, and ravel rejects data points more than 2 hours old, so a
# static checked-in fixture would fail non-deterministically. The field numbers
# used below are the frozen OTLP metrics wire contract.
set -euo pipefail

RAVEL_HTTP_BASE="${RAVEL_HTTP_BASE:-http://127.0.0.1:4318}"
RAVEL_TENANT_TOKEN="${RAVEL_TENANT_TOKEN:-demo-token}"
METRIC_NAME="${RAVEL_WALKTHROUGH_METRIC:-walkthrough_demo}"

log() { echo "[walkthrough] $*" >&2; }

if ! command -v curl >/dev/null 2>&1; then
  log "curl not found on PATH"
  exit 2
fi

# --- OTLP protobuf assembly ------------------------------------------------

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

# Second granularity keeps this portable (BSD `date` has no %N); well inside the
# 2-hour freshness window ravel enforces.
now_ns=$(( $(date +%s) * 1000000000 ))

# NumberDataPoint { time_unix_nano = 3 (fixed64); as_double = 4 (double) = 1.0 }.
# 1.0 as IEEE-754 little-endian is 00 00 00 00 00 00 F0 3F.
ndp="$(field_fixed64 3 "$(le64 "$now_ns")") $(field_fixed64 4 "0 0 0 0 0 0 240 63")"
# Gauge { data_points = 1 }.
gauge="$(field_len_delimited 1 "$ndp")"
# Metric name bytes as decimals.
name_bytes=$(printf '%s' "$METRIC_NAME" | od -An -tu1 | tr -s ' \n' ' ')
# Metric { name = 1 (string); gauge = 5 (message) }.
metric="$(field_len_delimited 1 "$name_bytes") $(field_len_delimited 5 "$gauge")"
# ScopeMetrics { metrics = 2 }; ResourceMetrics { scope_metrics = 2 };
# ExportMetricsServiceRequest { resource_metrics = 1 }.
scope_metrics="$(field_len_delimited 2 "$metric")"
resource_metrics="$(field_len_delimited 2 "$scope_metrics")"
request="$(field_len_delimited 1 "$resource_metrics")"

fixture=$(mktemp)
headers=$(mktemp)
cleanup() { rm -f "$fixture" "$headers"; }
trap cleanup EXIT

# Emit the decimal byte list as raw bytes. Intentional word splitting over the
# space-separated decimals.
# shellcheck disable=SC2086
{
  for b in $request; do
    printf "\\$(printf '%03o' "$b")"
  done
} >"$fixture"

# --- Ingest ----------------------------------------------------------------
log "ingesting one OTLP metric export ($METRIC_NAME) to $RAVEL_HTTP_BASE/v1/metrics"
curl --silent --show-error --fail \
  --dump-header "$headers" \
  --output /dev/null \
  -X POST "$RAVEL_HTTP_BASE/v1/metrics" \
  -H "Authorization: Bearer $RAVEL_TENANT_TOKEN" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary "@$fixture"

commit_token=$(grep -i '^x-ravel-commit-token:' "$headers" | sed 's/^[^:]*:[[:space:]]*//' | tr -d '\r')
if [[ -z "$commit_token" ]]; then
  log "ingest succeeded but no x-ravel-commit-token header was returned"
  exit 1
fi
log "ingest accepted; commit token: $commit_token"

# --- Query back with the commit token --------------------------------------
log "querying $METRIC_NAME back with min_commit_token"
query_response=$(curl --silent --show-error --fail \
  -H "Authorization: Bearer $RAVEL_TENANT_TOKEN" \
  --get "$RAVEL_HTTP_BASE/api/v1/query" \
  --data-urlencode "query=$METRIC_NAME" \
  --data-urlencode "min_commit_token=$commit_token")

echo "commit token: $commit_token"
echo "query result: $query_response"

if ! grep -q '"status":"success"' <<<"$query_response"; then
  log "query response did not report success"
  exit 1
fi
if ! grep -q "$METRIC_NAME" <<<"$query_response"; then
  log "query response did not contain the ingested series $METRIC_NAME"
  exit 1
fi

log "walkthrough complete: ingested series read back by its own commit token"
