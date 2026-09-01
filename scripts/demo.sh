#!/usr/bin/env bash
# End-to-end demo: start MinIO, start ravel-server against it, push one OTLP
# metric export, query it back by commit token, print both results.
#
# The OTLP fixture is generated fresh on every run (via the ravel-server
# `gen_otlp_fixture` example) instead of being a static checked-in blob,
# because OTLP ingest rejects points more than 2 hours old and a stale
# checked-in timestamp would make the demo fail non-deterministically.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MINIO_COMPOSE="deploy/docker-compose/minio.yml"
MINIO_ENDPOINT="http://127.0.0.1:9000"
FIXTURE_PATH="examples/otlp_metrics_fixture.pb"

export RAVEL_S3_ENDPOINT="$MINIO_ENDPOINT"
export RAVEL_S3_BUCKET="ravel-dev"
export RAVEL_S3_REGION="us-east-1"
export RAVEL_S3_ACCESS_KEY="ravel"
export RAVEL_S3_SECRET_KEY="ravel-dev-secret"

HTTP_ADDR="127.0.0.1:14318"
GRPC_ADDR="127.0.0.1:14317"
TENANT_TOKEN="demo-token"
TENANT_NAME="demo-tenant"

SERVER_PID=""
STARTED_MINIO=0

log() {
  echo "[demo] $*" >&2
}

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    log "stopping ravel-server (pid $SERVER_PID)"
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ "$STARTED_MINIO" -eq 1 ]]; then
    log "stopping MinIO"
    docker compose -f "$MINIO_COMPOSE" down >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

minio_healthy() {
  curl --silent --fail --max-time 2 "${MINIO_ENDPOINT}/minio/health/live" >/dev/null 2>&1
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

if minio_healthy; then
  log "MinIO already running at ${MINIO_ENDPOINT}"
else
  log "starting MinIO via docker compose"
  docker compose -f "$MINIO_COMPOSE" up -d
  STARTED_MINIO=1
  wait_for "MinIO to become healthy" minio_healthy
fi

log "ensuring bucket ${RAVEL_S3_BUCKET} exists"
docker run --rm --network host \
  -e "MC_HOST_local=http://${RAVEL_S3_ACCESS_KEY}:${RAVEL_S3_SECRET_KEY}@127.0.0.1:9000" \
  minio/mc:latest mb -p "local/${RAVEL_S3_BUCKET}" >/dev/null 2>&1 || true

log "building ravel-server and ravel-cli"
cargo build --quiet -p ravel-server -p ravel-cli

# ADR-0050 section 6 (EC7): server startup on a non-Memory store
# refuses unless `sys/qualification` is already present, and there is
# deliberately no bootstrap-and-continue path for it (unlike the tenancy
# marker and gc-config objects). A bucket `mc mb`'d fresh above has no such
# record yet, so qualify it before starting the server or it refuses to start.
log "qualifying store backend (ravel-cli store qualify)"
cargo run --quiet -p ravel-cli -- --store s3 store qualify

log "generating fresh OTLP fixture at ${FIXTURE_PATH}"
mkdir -p examples
cargo run --quiet -p ravel-server --example gen_otlp_fixture > "$FIXTURE_PATH"

log "starting ravel-server on ${HTTP_ADDR}"
cargo run --quiet -p ravel-server -- \
  --store s3 \
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
