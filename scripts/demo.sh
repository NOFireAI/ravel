#!/usr/bin/env bash
# End-to-end demo: start RustFS, start ravel-server against it, push one OTLP
# metric export, query it back by commit token, print both results.
#
# The OTLP fixture is generated fresh on every run (via the ravel-server
# `gen_otlp_fixture` example) instead of being a static checked-in blob,
# because OTLP ingest rejects points more than 2 hours old and a stale
# checked-in timestamp would make the demo fail non-deterministically.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUSTFS_COMPOSE="deploy/docker-compose/rustfs.yml"
RUSTFS_ENDPOINT="http://127.0.0.1:9000"
FIXTURE_PATH="examples/otlp_metrics_fixture.pb"

export RAVEL_S3_ENDPOINT="$RUSTFS_ENDPOINT"
export RAVEL_S3_BUCKET="ravel-dev"
export RAVEL_S3_REGION="us-east-1"
export RAVEL_S3_ACCESS_KEY="ravel"
export RAVEL_S3_SECRET_KEY="ravel-dev-secret"

HTTP_ADDR="127.0.0.1:14318"
GRPC_ADDR="127.0.0.1:14317"
TENANT_TOKEN="demo-token"
TENANT_NAME="demo-tenant"

SERVER_PID=""
STARTED_RUSTFS=0

log() {
  echo "[demo] $*" >&2
}

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    log "stopping ravel-server (pid $SERVER_PID)"
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ "$STARTED_RUSTFS" -eq 1 ]]; then
    log "stopping RustFS"
    docker compose -f "$RUSTFS_COMPOSE" down >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

rustfs_healthy() {
  curl --silent --fail --max-time 2 "${RUSTFS_ENDPOINT}/health" >/dev/null 2>&1
}

wait_for() {
  local description="$1"
  shift
  local attempt
  for attempt in $(seq 1 30); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done
  log "timed out waiting for ${description}"
  return 1
}

# The compose file's createbucket service creates the ravel-dev bucket with
# Object Lock, versioning and the lifecycle rule and reads all three back. The
# demo does not create the bucket itself: a plain create racing that service
# would leave a bucket without Object Lock, and the service's read-back would
# then fail. `docker compose wait` (Compose 2.20 or later) exits with the
# service's own exit code.
if rustfs_healthy; then
  log "RustFS already running at ${RUSTFS_ENDPOINT}; ${RAVEL_S3_BUCKET} is expected to exist (the qualify step below reports it if not)"
else
  log "starting RustFS via docker compose"
  docker compose -f "$RUSTFS_COMPOSE" up -d
  STARTED_RUSTFS=1
  wait_for "RustFS to become healthy" rustfs_healthy
  log "waiting for the createbucket service to set up ${RAVEL_S3_BUCKET}"
  createbucket_rc=0
  docker compose -f "$RUSTFS_COMPOSE" wait createbucket >/dev/null || createbucket_rc=$?
  if [[ "$createbucket_rc" -ne 0 ]]; then
    log "the createbucket service failed (exit ${createbucket_rc}); its output:"
    docker compose -f "$RUSTFS_COMPOSE" logs createbucket >&2 || true
    exit 1
  fi
fi

log "building ravel-server and ravel-cli"
cargo build --quiet -p ravel-server -p ravel-cli

# ADR-0050 section 6 (EC7): server startup on a non-Memory store
# refuses unless `sys/qualification` is already present, and there is
# deliberately no bootstrap-and-continue path for it (unlike the tenancy
# marker and gc-config objects). A bucket created fresh above has no such
# record yet, so qualify it before starting the server or it refuses to start.
log "qualifying store backend (ravel-cli store qualify)"
cargo run --quiet -p ravel-cli -- --store s3 store qualify

log "generating fresh OTLP fixture at ${FIXTURE_PATH}"
mkdir -p examples
cargo run --quiet -p ravel-server --example gen_otlp_fixture > "$FIXTURE_PATH"

log "starting ravel-server on ${HTTP_ADDR}"
# A fresh bucket refuses to start unless a tenant-hash scheme is chosen
# (keyed is the default for a real deployment; an unspecified scheme is
# FreshBucketNeedsKey). This is a throwaway dev bucket, so opt out explicitly,
# exactly as the compose quickstart does. Safe on a reused rustfs-data/ too: an
# existing unkeyed marker validates against this flag.
cargo run --quiet -p ravel-server -- \
  --store s3 \
  --tenant-hash-unkeyed \
  --listen-http "$HTTP_ADDR" \
  --listen-grpc "$GRPC_ADDR" \
  --tenant-token "${TENANT_TOKEN}=${TENANT_NAME}" &
SERVER_PID=$!

server_query_reachable() {
  curl --silent --fail --max-time 2 \
    -H "Authorization: Bearer ${TENANT_TOKEN}" \
    "http://${HTTP_ADDR}/api/v1/query?query=up" >/dev/null 2>&1
}
log "waiting for ravel-server to accept connections"
wait_for "ravel-server to accept connections" server_query_reachable

log "sending OTLP metrics export"
export_headers_file="$(mktemp)"
curl --silent --show-error --fail \
  --dump-header "$export_headers_file" \
  --output /dev/null \
  -X POST "http://${HTTP_ADDR}/v1/metrics" \
  -H "Authorization: Bearer ${TENANT_TOKEN}" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary "@${FIXTURE_PATH}"

commit_token="$(grep -i '^x-ravel-commit-token:' "$export_headers_file" | sed 's/^[^:]*:[[:space:]]*//' | tr -d '\r')"
rm -f "$export_headers_file"

if [[ -z "$commit_token" ]]; then
  log "export succeeded but no commit token header was returned"
  exit 1
fi
log "export accepted, commit token: ${commit_token}"

log "querying ingested metric with min_commit_token"
query_response="$(curl --silent --show-error --fail \
  -H "Authorization: Bearer ${TENANT_TOKEN}" \
  --get "http://${HTTP_ADDR}/api/v1/query" \
  --data-urlencode "query=demo_requests_total" \
  --data-urlencode "min_commit_token=${commit_token}")"

echo "export result: commit_token=${commit_token}"
echo "query result: ${query_response}"

if ! grep -q '"status":"success"' <<<"$query_response"; then
  log "query response did not report success"
  exit 1
fi
if ! grep -q 'demo_requests_total' <<<"$query_response"; then
  log "query response did not contain the ingested series"
  exit 1
fi

log "demo complete"
