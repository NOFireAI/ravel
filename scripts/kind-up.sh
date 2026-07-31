#!/usr/bin/env bash
# Bring up the kind development environment (ADR-0034 decision 6): a local
# Kubernetes cluster running the Ravel operator, a fake S3 backend, and one
# reconciled RavelCluster.
#
# What it does, in order:
#   1. create a kind cluster from a pinned node image (idempotent: an existing
#      cluster of the same name is reused)
#   2. build the `server` and `operator` targets of the root Dockerfile, unless
#      prebuilt tags are supplied (see RAVEL_SKIP_IMAGE_BUILD below)
#   3. `kind load docker-image` both, so the cluster needs no registry and the
#      operator's IfNotPresent pull policy resolves locally
#   4. deploy the fake S3 backend selected by RAVEL_FAKE_S3_BACKEND and wait
#      for it to serve S3, then create the bucket
#   5. install the CRD, RBAC, and operator Deployment from deploy/k8s/operator/
#   6. apply a RavelCluster pointed at that backend and those image tags
#   7. wait for the operator to report `Available`, which by construction means
#      the gateway and query Deployments have ready replicas -- i.e. the images
#      really run and the pods really pass /readyz, not merely that objects
#      were created
#
# Run scripts/kind-demo.sh next for the ingest/query round trip, and
# scripts/kind-down.sh to tear the cluster down.
#
# Environment:
#   RAVEL_KIND_CLUSTER        cluster name (default ravel-dev)
#   RAVEL_KIND_NODE_IMAGE     pinned kind node image (default below)
#   RAVEL_SERVER_IMAGE        server image tag (default ravel-server:kind-dev)
#   RAVEL_OPERATOR_IMAGE      operator image tag (default ravel-operator:kind-dev)
#   RAVEL_SKIP_IMAGE_BUILD    1 to skip `docker build` and use the tags as-is.
#                             For CI (ADR-0034 decision 7), which builds the
#                             binaries on the host with a warm cargo cache and
#                             assembles the images itself.
#   RAVEL_FAKE_S3_BACKEND     floci (default) or minio (ADR-0034 decision 8)
#   RAVEL_TENANT_NAME         tenant to provision (default demo-tenant)
#   RAVEL_TENANT_TOKEN        its bearer token (default demo-token)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${RAVEL_KIND_CLUSTER:-ravel-dev}"
# Pinned by tag and digest. kind node images are tied to a kind release; a
# floating tag would silently change the control-plane version under a
# reproduction. This digest is the one the kind v0.32.0 release body attests
# for its default node image, Kubernetes v1.36.1, so the CLI version pinned in
# the k8s-integration CI job (helm/kind-action version: v0.32.0) and this node
# image come from the same release. The digest is the multi-arch index digest,
# so it resolves on both amd64 and arm64 hosts. Do not swap this for whatever a
# tag currently resolves to: pin the digest the release itself published.
NODE_IMAGE="${RAVEL_KIND_NODE_IMAGE:-kindest/node:v1.36.1@sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5}"
SERVER_IMAGE="${RAVEL_SERVER_IMAGE:-ravel-server:kind-dev}"
OPERATOR_IMAGE="${RAVEL_OPERATOR_IMAGE:-ravel-operator:kind-dev}"
SKIP_IMAGE_BUILD="${RAVEL_SKIP_IMAGE_BUILD:-0}"
BACKEND="${RAVEL_FAKE_S3_BACKEND:-floci}"

NAMESPACE="ravel-system"
CLUSTER_CR="dev"
# Must match the bucket the backend manifests' create-bucket Jobs make and the
# bucket deploy/k8s/examples/ravelcluster-dev.yaml references.
BUCKET="ravel"
SHARDS="4"
TENANT_NAME="${RAVEL_TENANT_NAME:-demo-tenant}"
TENANT_TOKEN="${RAVEL_TENANT_TOKEN:-demo-token}"
S3_CREDENTIALS_SECRET="ravel-s3-credentials"
TENANT_TOKENS_SECRET="ravel-tenant-tokens"

log() {
  echo "[kind-up] $*" >&2
}

die() {
  log "$*"
  exit 1
}

# On any failure after the cluster exists, dump what a human would ask for
# first. Without this the script's last line is a bare `kubectl wait` timeout,
# which says nothing about why a pod never went ready.
#
# Installed only once the cluster is up (see step 1), not here: on a pre-flight
# failure such as an unknown RAVEL_FAKE_S3_BACKEND there is no cluster to dump,
# and running these kubectl calls anyway buries the one-line real error under a
# screen of connection-refused noise and a false "cluster left running" claim.
diagnostics() {
  local status=$?
  [[ "$status" -eq 0 ]] && return 0
  log "failed (exit ${status}); dumping cluster state"
  kubectl get -n "$NAMESPACE" all 2>&1 | sed 's/^/[state] /' >&2 || true
  kubectl get -n "$NAMESPACE" ravelcluster "$CLUSTER_CR" -o yaml 2>&1 |
    sed 's/^/[cr] /' >&2 || true
  kubectl describe -n "$NAMESPACE" pods 2>&1 | sed 's/^/[pods] /' >&2 || true
  kubectl logs -n "$NAMESPACE" deploy/ravel-operator --tail=80 2>&1 |
    sed 's/^/[operator] /' >&2 || true
  log "cluster ${CLUSTER_NAME} left running for inspection; scripts/kind-down.sh removes it"
  return "$status"
}

case "$BACKEND" in
  floci)
    BACKEND_MANIFEST="deploy/k8s/floci.yaml"
    BACKEND_DEPLOYMENT="floci"
    BACKEND_JOB="floci-create-bucket"
    S3_ENDPOINT="http://floci.${NAMESPACE}.svc:4566"
    # floci accepts any credentials and verifies no signatures; these match
    # what the floci_contract test uses.
    S3_ACCESS_KEY="test"
    S3_SECRET_KEY="test"
    ;;
  minio)
    BACKEND_MANIFEST="deploy/k8s/minio.yaml"
    BACKEND_DEPLOYMENT="minio"
    BACKEND_JOB="minio-create-bucket"
    S3_ENDPOINT="http://minio.${NAMESPACE}.svc:9000"
    # Same values as deploy/docker-compose/minio.yml and deploy/k8s/minio.yaml.
    S3_ACCESS_KEY="ravel"
    S3_SECRET_KEY="ravel-dev-secret"
    ;;
  *)
    die "RAVEL_FAKE_S3_BACKEND must be floci or minio, got '${BACKEND}'"
    ;;
esac

for tool in docker kind kubectl; do
  command -v "$tool" >/dev/null 2>&1 || die "${tool} is required but not on PATH"
done

# ---- 1. cluster --------------------------------------------------------------
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "reusing existing kind cluster ${CLUSTER_NAME}"
else
  log "creating kind cluster ${CLUSTER_NAME} from ${NODE_IMAGE}"
  kind create cluster --name "$CLUSTER_NAME" --image "$NODE_IMAGE" --wait 120s
fi
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
kubectl config use-context "$KUBE_CONTEXT" >/dev/null
# From here on there is a cluster worth dumping on failure.
trap diagnostics EXIT

# ---- 2. images ---------------------------------------------------------------
if [[ "$SKIP_IMAGE_BUILD" == "1" ]]; then
  log "RAVEL_SKIP_IMAGE_BUILD=1: using prebuilt ${SERVER_IMAGE} and ${OPERATOR_IMAGE}"
  for image in "$SERVER_IMAGE" "$OPERATOR_IMAGE"; do
    docker image inspect "$image" >/dev/null 2>&1 ||
      die "prebuilt image ${image} not present in the local docker daemon"
  done
else
  # Two `docker build` invocations, not one: they share the builder stage, so
  # the second is a cache hit on everything expensive.
  log "building ${SERVER_IMAGE} (Dockerfile target: server)"
  docker build --target server -t "$SERVER_IMAGE" .
  log "building ${OPERATOR_IMAGE} (Dockerfile target: operator)"
  docker build --target operator -t "$OPERATOR_IMAGE" .
fi

log "loading images into kind cluster ${CLUSTER_NAME}"
kind load docker-image --name "$CLUSTER_NAME" "$SERVER_IMAGE" "$OPERATOR_IMAGE"

# ---- 3. namespace and secrets ------------------------------------------------
# The namespace is created here rather than taken from operator.yaml so that
# the backend manifests (which deliberately do not define it) can be applied
# before the operator, in the order ADR-0034 decision 6 describes.
log "ensuring namespace ${NAMESPACE}"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Secrets are created by the script, not committed as manifests: committing a
# Secret manifest -- even a dev one -- puts credentials in git and invites
# copying into a real cluster.
log "creating Secrets ${S3_CREDENTIALS_SECRET} and ${TENANT_TOKENS_SECRET}"
kubectl create secret generic "$S3_CREDENTIALS_SECRET" \
  --namespace "$NAMESPACE" \
  --from-literal="accessKeyId=${S3_ACCESS_KEY}" \
  --from-literal="secretAccessKey=${S3_SECRET_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
# Keys are tenant names, values are that tenant's bearer token (ADR-0034
# decision 2).
kubectl create secret generic "$TENANT_TOKENS_SECRET" \
  --namespace "$NAMESPACE" \
  --from-literal="${TENANT_NAME}=${TENANT_TOKEN}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# ---- 4. fake S3 backend ------------------------------------------------------
log "deploying fake S3 backend '${BACKEND}' from ${BACKEND_MANIFEST}"
kubectl apply -f "$BACKEND_MANIFEST"

log "waiting for ${BACKEND_DEPLOYMENT} to have a ready replica"
kubectl wait --namespace "$NAMESPACE" --for=condition=Available --timeout=300s \
  "deployment/${BACKEND_DEPLOYMENT}"

# The Job is the real proof the backend serves S3: it retries until the S3
# handler answers, creates the bucket, then HEADs it to confirm.
log "waiting for bucket-create job ${BACKEND_JOB}"
if ! kubectl wait --namespace "$NAMESPACE" --for=condition=Complete --timeout=300s \
  "job/${BACKEND_JOB}"; then
  kubectl logs --namespace "$NAMESPACE" "job/${BACKEND_JOB}" 2>&1 |
    sed 's/^/[bucket-job] /' >&2 || true
  die "bucket-create job ${BACKEND_JOB} did not complete"
fi
log "bucket ${BUCKET} ready on ${S3_ENDPOINT}"

# ---- 5. operator -------------------------------------------------------------
# CRD before the operator Deployment: the operator's watch fails until the
# RavelCluster kind is served. RBAC before it too, or its API calls 403.
log "installing CRD, RBAC, and operator Deployment"
kubectl apply -f deploy/k8s/operator/crd.yaml
kubectl apply -f deploy/k8s/operator/rbac.yaml
kubectl apply -f deploy/k8s/operator/operator.yaml
# operator.yaml carries a placeholder image tag (ravel-operator:latest); point
# it at the tag actually loaded into this cluster.
kubectl set image --namespace "$NAMESPACE" deployment/ravel-operator \
  "ravel-operator=${OPERATOR_IMAGE}" >/dev/null

log "waiting for the operator to become available"
kubectl wait --namespace "$NAMESPACE" --for=condition=Available --timeout=300s \
  deployment/ravel-operator

# ---- 6. RavelCluster ---------------------------------------------------------
# Rendered here rather than applying deploy/k8s/examples/ravelcluster-dev.yaml
# directly: that example is the documentation shape (a MinIO endpoint and a
# `ravel-server:latest` image tag), and this needs the endpoint of the backend
# just deployed and the image tag just built. Every other field is kept
# identical to the example on purpose, so the two do not drift in meaning.
log "applying RavelCluster ${CLUSTER_CR} (backend ${BACKEND}, image ${SERVER_IMAGE})"
kubectl apply -f - <<YAML
apiVersion: ravel.nofire.ai/v1alpha1
kind: RavelCluster
metadata:
  name: ${CLUSTER_CR}
  namespace: ${NAMESPACE}
spec:
  image: ${SERVER_IMAGE}
  imagePullPolicy: IfNotPresent
  shards: ${SHARDS}
  storage:
    s3:
      bucket: ${BUCKET}
      region: us-east-1
      endpoint: ${S3_ENDPOINT}
      credentialsSecretRef:
        name: ${S3_CREDENTIALS_SECRET}
  tenantTokensSecretRef:
    name: ${TENANT_TOKENS_SECRET}
  gateway:
    replicas: 1
  query:
    replicas: 1
  maintain:
    enabled: true
    intervalSecs: 300
  retention:
    default: 30d
YAML

# ---- 7. readiness ------------------------------------------------------------
# The operator sets Available=True only when both the gateway and the query
# Deployment report ready replicas, so this waits on real pods passing /readyz
# against the real backend, not on objects existing.
log "waiting for RavelCluster ${CLUSTER_CR} to report Available"
kubectl wait --namespace "$NAMESPACE" --for=condition=Available --timeout=300s \
  "ravelcluster/${CLUSTER_CR}"

log "cluster up. Deployments:"
kubectl get --namespace "$NAMESPACE" deployments
log "RavelCluster status:"
kubectl get --namespace "$NAMESPACE" "ravelcluster/${CLUSTER_CR}" \
  -o jsonpath='{.status}' | sed 's/^/[status] /' >&2
echo >&2
log "ready. Next: RAVEL_KIND_CLUSTER=${CLUSTER_NAME} scripts/kind-demo.sh"
