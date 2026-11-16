#!/usr/bin/env bash
# End-to-end round trip against the kind development environment (ADR-0034
# decision 6): push one OTLP metric export through the gateway Service, read it
# back through the query Service, and assert the exact value that went in.
#
# Same shape and same assertion discipline as scripts/demo.sh: every failure
# exits nonzero, so CI needs no grep over the output to prove it really ran.
# The differences from demo.sh are that the server is not a local process but
# pods the operator reconciled from a RavelCluster, and that ingest and query
# go to two different Services (two tiers), which is what makes this a test of
# the operator rather than of ravel-server.
#
# The OTLP fixture is generated fresh on every run (via the ravel-server
# `gen_otlp_fixture` example) rather than checked in, because OTLP ingest
# rejects points more than 2 hours old and a stale timestamp would make the
# demo fail non-deterministically.
#
# Requires scripts/kind-up.sh to have completed.
#
# Environment:
#   RAVEL_KIND_CLUSTER   cluster name (default ravel-dev)
#   RAVEL_TENANT_NAME    tenant provisioned by kind-up.sh (default demo-tenant)
#   RAVEL_TENANT_TOKEN   its bearer token (default demo-token)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${RAVEL_KIND_CLUSTER:-ravel-dev}"
NAMESPACE="ravel-system"
CLUSTER_CR="dev"
GATEWAY_SVC="${CLUSTER_CR}-gateway"
QUERY_SVC="${CLUSTER_CR}-query"
TENANT_TOKEN="${RAVEL_TENANT_TOKEN:-demo-token}"

# Local ports for the two port-forwards. Deliberately not 4318 on both, since
# the gateway and the query tier both listen on 4318 inside the cluster.
GATEWAY_PORT="${RAVEL_KIND_GATEWAY_PORT:-14318}"
QUERY_PORT="${RAVEL_KIND_QUERY_PORT:-14319}"

FIXTURE_PATH="examples/otlp_metrics_fixture.pb"
METRIC_NAME="demo_requests_total"
# The value gen_otlp_fixture writes into its single gauge data point. Asserting
# the value, not just the series name, is what makes this a data round trip
# rather than a liveness check.
EXPECTED_VALUE="7"

GATEWAY_PF_PID=""
QUERY_PF_PID=""

log() {
  echo "[kind-demo] $*" >&2
}

die() {
  log "$*"
  exit 1
}

cleanup() {
  local pid
  for pid in "$GATEWAY_PF_PID" "$QUERY_PF_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

wait_for() {
  local description="$1"
  shift
  local attempt
  for attempt in $(seq 1 60); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done
  log "timed out waiting for ${description}"
  return 1
}

for tool in kind kubectl cargo curl; do
  command -v "$tool" >/dev/null 2>&1 || die "${tool} is required but not on PATH"
done

kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME" ||
  die "kind cluster ${CLUSTER_NAME} does not exist; run scripts/kind-up.sh first"
kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null

# Fail fast with a clear reason if the environment is not actually ready, rather
# than with a confusing connection error from the first curl.
log "checking RavelCluster ${CLUSTER_CR} is Available"
kubectl wait --namespace "$NAMESPACE" --for=condition=Available --timeout=120s \
  "ravelcluster/${CLUSTER_CR}" ||
  die "RavelCluster ${CLUSTER_CR} is not Available; run scripts/kind-up.sh first"

log "generating fresh OTLP fixture at ${FIXTURE_PATH}"
mkdir -p examples
cargo run --quiet -p ravel-server --example gen_otlp_fixture >"$FIXTURE_PATH"
[[ -s "$FIXTURE_PATH" ]] || die "fixture generation produced an empty file"

log "port-forwarding svc/${GATEWAY_SVC} to 127.0.0.1:${GATEWAY_PORT}"
kubectl port-forward --namespace "$NAMESPACE" "svc/${GATEWAY_SVC}" \
  "${GATEWAY_PORT}:4318" >/dev/null 2>&1 &
GATEWAY_PF_PID=$!
log "port-forwarding svc/${QUERY_SVC} to 127.0.0.1:${QUERY_PORT}"
kubectl port-forward --namespace "$NAMESPACE" "svc/${QUERY_SVC}" \
  "${QUERY_PORT}:4318" >/dev/null 2>&1 &
QUERY_PF_PID=$!

# /healthz needs no Authorization header, so this waits on the tunnel and the
# pod, not on tenant configuration.
gateway_reachable() {
  curl --silent --fail --max-time 2 "http://127.0.0.1:${GATEWAY_PORT}/healthz" >/dev/null 2>&1
}
query_reachable() {
  curl --silent --fail --max-time 2 "http://127.0.0.1:${QUERY_PORT}/healthz" >/dev/null 2>&1
}
wait_for "the gateway port-forward" gateway_reachable
wait_for "the query port-forward" query_reachable

log "sending OTLP metrics export to the gateway Service"
export_headers_file="$(mktemp)"
curl --silent --show-error --fail \
  --dump-header "$export_headers_file" \
  --output /dev/null \
  -X POST "http://127.0.0.1:${GATEWAY_PORT}/v1/metrics" \
  -H "Authorization: Bearer ${TENANT_TOKEN}" \
  -H "Content-Type: application/x-protobuf" \
  --data-binary "@${FIXTURE_PATH}"

commit_token="$(grep -i '^x-ravel-commit-token:' "$export_headers_file" |
  sed 's/^[^:]*:[[:space:]]*//' | tr -d '\r')"
rm -f "$export_headers_file"

if [[ -z "$commit_token" ]]; then
  die "export succeeded but no commit token header was returned"
fi
log "export accepted, commit token: ${commit_token}"

# The query goes to the query tier's Service, not back to the gateway: the
# point of the round trip is that a separate set of pods, sharing nothing but
# the bucket, can read what the gateway just wrote. min_commit_token makes that
# read-your-write, not a poll.
log "querying ${METRIC_NAME} through the query Service with min_commit_token"
query_response="$(curl --silent --show-error --fail \
  -H "Authorization: Bearer ${TENANT_TOKEN}" \
  --get "http://127.0.0.1:${QUERY_PORT}/api/v1/query" \
  --data-urlencode "query=${METRIC_NAME}" \
  --data-urlencode "min_commit_token=${commit_token}")"

echo "export result: commit_token=${commit_token}"
echo "query result: ${query_response}"

if ! grep -q '"status":"success"' <<<"$query_response"; then
  die "query response did not report success"
fi
if ! grep -q "$METRIC_NAME" <<<"$query_response"; then
  die "query response did not contain the ingested series"
fi
# The instant-query result renders a sample as ["<timestamp>","<value>"], so
# the value is a quoted string in the JSON.
if ! grep -q "\"${EXPECTED_VALUE}\"" <<<"$query_response"; then
  die "query response did not contain the expected value ${EXPECTED_VALUE}"
fi

log "round trip asserted: ${METRIC_NAME} = ${EXPECTED_VALUE} via svc/${QUERY_SVC}"
log "demo complete"
