#!/usr/bin/env bash
# Tear down the kind development environment (ADR-0034 decision 6).
#
# Deleting the cluster deletes everything in it, including the fake S3
# backend's data: nothing in the kind environment is durable by design, so
# there is no bucket to preserve and no ordered shutdown to perform. The
# locally built ravel-server and ravel-operator images are left in the host
# docker daemon, so a later scripts/kind-up.sh can skip the build.
#
# Idempotent: deleting a cluster that does not exist is reported and succeeds,
# so this is safe in a CI `if: always()` step.
#
# Environment:
#   RAVEL_KIND_CLUSTER   cluster name (default ravel-dev)
set -euo pipefail

CLUSTER_NAME="${RAVEL_KIND_CLUSTER:-ravel-dev}"

log() {
  echo "[kind-down] $*" >&2
}

command -v kind >/dev/null 2>&1 || {
  log "kind is required but not on PATH"
  exit 1
}

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "kind cluster ${CLUSTER_NAME} does not exist; nothing to do"
  exit 0
fi

log "deleting kind cluster ${CLUSTER_NAME}"
kind delete cluster --name "$CLUSTER_NAME"
log "done"
