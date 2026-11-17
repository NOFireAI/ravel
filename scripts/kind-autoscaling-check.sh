#!/usr/bin/env bash
# Proves ADR-4314's replicas handover on a real apiserver (issue #4324): run
# after scripts/kind-demo.sh, against the cluster scripts/kind-up.sh brought
# up, to show that enabling autoscaling on the demo RavelCluster's gateway
# and query tiers keeps the replica count instead of resetting it to the
# declared value, that the operator hands `spec.replicas` to a second field
# manager instead of fighting the scaler, and that turning autoscaling off
# keeps the live count until `spec.<tier>.replicas` changes.
#
# The operator's fake-apiserver tests (services/ravel-operator/src/
# autoscaling.rs) prove the request sequence; this script is what ADR-4314's
# Consequences names as the thing that proves the outcome on a real
# apiserver (field-manager semantics the fake apiserver does not emulate).
#
# Same assertion discipline as scripts/kind-demo.sh: every failure exits
# nonzero with a message naming what was checked, so CI needs no grep over
# the output to prove this ran. Every wait is bounded; a timeout names what
# was awaited and the last value seen. Every kubectl read whose output is
# asserted is checked for emptiness first: an empty read is always a
# failure, never a silent pass.
#
# Requires scripts/kind-up.sh and scripts/kind-demo.sh to have completed
# (the demo RavelCluster "dev" exists, Available, with ready gateway and
# query Deployments).
#
# Environment:
#   RAVEL_KIND_CLUSTER                    cluster name (default ravel-dev)
#   RAVEL_AUTOSCALING_WAIT_TIMEOUT_SECONDS bound on every wait (default 180)
#   RAVEL_AUTOSCALING_WAIT_INTERVAL_SECONDS poll interval (default 3)
# The two *_SECONDS variables exist so scripts/kind-autoscaling-check.test.sh
# can run every wait against a stub kubectl in a couple of seconds instead of
# the real timeout.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${RAVEL_KIND_CLUSTER:-ravel-dev}"
NAMESPACE="ravel-system"
CLUSTER_CR="dev"
GATEWAY_DEPLOY="${CLUSTER_CR}-gateway"
QUERY_DEPLOY="${CLUSTER_CR}-query"
GATEWAY_HPA="${CLUSTER_CR}-gateway"

WAIT_TIMEOUT="${RAVEL_AUTOSCALING_WAIT_TIMEOUT_SECONDS:-180}"
WAIT_INTERVAL="${RAVEL_AUTOSCALING_WAIT_INTERVAL_SECONDS:-3}"

# The original demo shape (scripts/kind-up.sh step 6): both tiers at 1
# replica, no autoscaling block. Restored at the end so later lane steps see
# the demo cluster as kind-up.sh left it.
ORIGINAL_GATEWAY_REPLICAS=1
ORIGINAL_QUERY_REPLICAS=1

ANNOTATION_KEY="ravel.nofire.ai/autoscaling-check-nonce"
RECONCILE_NONCE=0

log() {
  echo "[kind-autoscaling-check] $*" >&2
}

die() {
  log "$*"
  exit 1
}

# Reads a value with "$@" (a kubectl invocation) and fails the whole script
# with "could not read <description>" on an empty result -- an empty read is
# never a silent pass (issue #4324's own gate requirement).
read_now() {
  local desc="$1"
  shift
  local val
  val="$("$@" 2>/dev/null)" || val=""
  if [[ -z "${val}" ]]; then
    die "could not read ${desc}"
  fi
  printf '%s' "${val}"
}

# Compares an already-read value against what was expected, printing one
# line either way so a passing run still shows every assertion's expected
# and actual value. Exact string equality only: a substring match would let
# "13" satisfy an expectation of "3" (ADR-4314's own failure mode for this
# check), which is exactly the bug scripts/kind-autoscaling-check.test.sh's
# distinguishing cases exist to catch.
assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [[ -z "${actual}" ]]; then
    die "could not read ${desc}"
  fi
  if [[ "${actual}" != "${expected}" ]]; then
    die "ASSERT FAIL: ${desc}: expected '${expected}', got '${actual}'"
  fi
  log "ASSERT OK: ${desc}: expected '${expected}', got '${actual}'"
}

# Asserts actual != forbidden (the managedFields ownership checks: a field
# manager that must NOT be the one named).
assert_ne() {
  local desc="$1" forbidden="$2" actual="$3"
  if [[ -z "${actual}" ]]; then
    die "could not read ${desc}"
  fi
  if [[ "${actual}" == "${forbidden}" ]]; then
    die "ASSERT FAIL: ${desc}: must not be '${forbidden}', got '${actual}'"
  fi
  log "ASSERT OK: ${desc}: not '${forbidden}' (got '${actual}')"
}

# Polls "$@" until it prints exactly $expected, bounded by WAIT_TIMEOUT. A
# timeout names what was awaited and the last value actually seen, never a
# bare `kubectl wait` timeout.
wait_field_equals() {
  local desc="$1" expected="$2"
  shift 2
  local deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
  local val=""
  while true; do
    val="$("$@" 2>/dev/null)" || val=""
    if [[ "${val}" == "${expected}" ]]; then
      log "ASSERT OK: ${desc}: expected '${expected}', got '${val}'"
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      die "timed out after ${WAIT_TIMEOUT}s waiting for ${desc}: expected '${expected}', last value seen '${val:-<empty>}'"
    fi
    sleep "${WAIT_INTERVAL}"
  done
}

# Same as wait_field_equals but succeeds once the read is merely non-empty
# (used for "does this list now contain the tier" reads where the expected
# text is the tier name itself read back).
wait_field_nonempty() {
  local desc="$1"
  shift
  local deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
  local val=""
  while true; do
    val="$("$@" 2>/dev/null)" || val=""
    if [[ -n "${val}" ]]; then
      log "ASSERT OK: ${desc}: got '${val}'"
      printf '%s' "${val}"
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      die "timed out after ${WAIT_TIMEOUT}s waiting for ${desc}: last value seen '<empty>'"
    fi
    sleep "${WAIT_INTERVAL}"
  done
}

# Polls until "$@" fails (used for "the HPA is gone").
wait_absent() {
  local desc="$1"
  shift
  local deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
  while true; do
    if ! "$@" >/dev/null 2>&1; then
      log "ASSERT OK: ${desc}: absent"
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      die "timed out after ${WAIT_TIMEOUT}s waiting for ${desc} to become absent"
    fi
    sleep "${WAIT_INTERVAL}"
  done
}

# A spec change the operator's own resync (every 300s) would eventually
# pick up anyway; an annotation forces a reconcile pass well inside the
# bounded waits below instead of waiting out that resync.
force_reconcile() {
  RECONCILE_NONCE=$((RECONCILE_NONCE + 1))
  kubectl annotate -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --overwrite \
    "${ANNOTATION_KEY}=${RECONCILE_NONCE}" >/dev/null
  log "forced reconcile pass ${RECONCILE_NONCE} (annotation ${ANNOTATION_KEY}=${RECONCILE_NONCE})"
}

deployment_replicas() {
  local dep="$1"
  kubectl get -n "${NAMESPACE}" "deployment/${dep}" -o jsonpath='{.spec.replicas}'
}

deployment_ready() {
  local dep="$1"
  kubectl get -n "${NAMESPACE}" "deployment/${dep}" -o jsonpath='{.status.readyReplicas}'
}

# The field manager owning spec.replicas on a Deployment, or empty if none
# does (ADR-4314 decision 2's handover: ravel-operator before the handover,
# ravel-operator-handover after it).
replicas_field_manager() {
  local dep="$1"
  kubectl get -n "${NAMESPACE}" "deployment/${dep}" -o json |
    jq -r '[.metadata.managedFields[]? |
      select(.fieldsV1 != null
        and (.fieldsV1 | has("f:spec"))
        and (.fieldsV1["f:spec"] | has("f:replicas"))) |
      .manager] | join(",")'
}

hpa_label() {
  local name="$1" label="$2"
  kubectl get -n "${NAMESPACE}" "hpa/${name}" -o json |
    jq -r --arg l "${label}" '.metadata.labels[$l] // ""'
}

hpa_owner_uid() {
  local name="$1"
  kubectl get -n "${NAMESPACE}" "hpa/${name}" -o json |
    jq -r '.metadata.ownerReferences[0].uid // ""'
}

hpa_exists() {
  local name="$1"
  kubectl get -n "${NAMESPACE}" "hpa/${name}" >/dev/null 2>&1
}

autoscaled_tiers_lists() {
  local tier="$1"
  kubectl get -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" \
    -o jsonpath="{.status.autoscaledTiers[?(@.tier==\"${tier}\")].tier}"
}

replica_adoption_adopted() {
  local tier="$1"
  kubectl get -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" \
    -o jsonpath="{.status.replicaAdoptions[?(@.tier==\"${tier}\")].adoptedReplicas}"
}

cluster_uid() {
  kubectl get -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" -o jsonpath='{.metadata.uid}'
}

cleanup() {
  local status=$?
  log "restoring the demo cluster: both tiers back to ${ORIGINAL_GATEWAY_REPLICAS}/${ORIGINAL_QUERY_REPLICAS} replicas, no autoscaling block"
  kubectl patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
    "{\"spec\":{\"gateway\":{\"replicas\":${ORIGINAL_GATEWAY_REPLICAS},\"autoscaling\":null},\"query\":{\"replicas\":${ORIGINAL_QUERY_REPLICAS},\"autoscaling\":null}}}" \
    >/dev/null 2>&1 || true
  kubectl annotate -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" "${ANNOTATION_KEY}-" \
    >/dev/null 2>&1 || true
  exit "${status}"
}

for tool in kubectl jq; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required but not on PATH"
done

kubectl config current-context >/dev/null 2>&1 ||
  die "no current kubectl context; run scripts/kind-up.sh first"

log "reading the RavelCluster uid (for HPA ownerReference assertions)"
CLUSTER_UID="$(read_now "RavelCluster ${CLUSTER_CR} uid" cluster_uid)"

trap cleanup EXIT

###############################################################################
# a. Scale the gateway to 3 replicas through the RavelCluster spec.
###############################################################################
log "step a: scaling spec.gateway.replicas to 3"
kubectl patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge \
  -p '{"spec":{"gateway":{"replicas":3}}}' >/dev/null

wait_field_equals "gateway Deployment spec.replicas is 3" "3" deployment_replicas "${GATEWAY_DEPLOY}"
wait_field_equals "gateway Deployment status.readyReplicas is 3" "3" deployment_ready "${GATEWAY_DEPLOY}"

###############################################################################
# b. Managed autoscaling on the gateway.
###############################################################################
log "step b: enabling Managed autoscaling on the gateway"
gateway_cpu_request="$(kubectl get -n "${NAMESPACE}" "deployment/${GATEWAY_DEPLOY}" \
  -o jsonpath='{.spec.template.spec.containers[0].resources.requests.cpu}' 2>/dev/null || true)"
if [[ -z "${gateway_cpu_request}" ]]; then
  # The demo gateway (scripts/kind-up.sh) sets requests.cpu: 100m, so this
  # branch should not run against the real demo cluster; it exists because a
  # Managed HPA refuses (AutoscalingWithheld) without a CPU request, and this
  # check must not depend on that manifest detail never changing.
  log "gateway has no CPU request; setting one (100m) in the same patch that enables Managed autoscaling"
  gateway_patch='{"spec":{"gateway":{"resources":{"requests":{"cpu":"100m"}},"autoscaling":{"mode":"Managed","minReplicas":2,"maxReplicas":4,"targetCPUUtilizationPercentage":80}}}}'
else
  log "gateway already has a CPU request (${gateway_cpu_request}); Managed mode can read it"
  gateway_patch='{"spec":{"gateway":{"autoscaling":{"mode":"Managed","minReplicas":2,"maxReplicas":4,"targetCPUUtilizationPercentage":80}}}}'
fi
kubectl patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p "${gateway_patch}" >/dev/null

wait_field_equals "status.autoscaledTiers lists gateway" "gateway" autoscaled_tiers_lists gateway

for pass in 1 2; do
  force_reconcile
  wait_field_equals "gateway Deployment spec.replicas after forced reconcile ${pass}" "3" \
    deployment_replicas "${GATEWAY_DEPLOY}"
  manager="$(read_now "gateway Deployment spec.replicas field manager (pass ${pass})" replicas_field_manager "${GATEWAY_DEPLOY}")"
  assert_ne "gateway Deployment spec.replicas field manager (pass ${pass})" "ravel-operator" "${manager}"
done

hpa_exists "${GATEWAY_HPA}" || die "HPA ${GATEWAY_HPA} does not exist after enabling Managed autoscaling"
log "ASSERT OK: HPA ${GATEWAY_HPA} exists"
managed_by="$(read_now "HPA ${GATEWAY_HPA} managed-by label" hpa_label "${GATEWAY_HPA}" "app.kubernetes.io/managed-by")"
assert_eq "HPA ${GATEWAY_HPA} managed-by label" "ravel-operator" "${managed_by}"
instance_label="$(read_now "HPA ${GATEWAY_HPA} instance label" hpa_label "${GATEWAY_HPA}" "app.kubernetes.io/instance")"
assert_eq "HPA ${GATEWAY_HPA} instance label" "${CLUSTER_CR}" "${instance_label}"
owner_uid="$(read_now "HPA ${GATEWAY_HPA} ownerReference uid" hpa_owner_uid "${GATEWAY_HPA}")"
assert_eq "HPA ${GATEWAY_HPA} ownerReference uid" "${CLUSTER_UID}" "${owner_uid}"

###############################################################################
# c. External autoscaling on the query tier.
###############################################################################
log "step c: enabling External autoscaling on the query tier"
kubectl patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  '{"spec":{"query":{"autoscaling":{"mode":"External","minReplicas":1,"maxReplicas":4}}}}' >/dev/null

wait_field_equals "status.autoscaledTiers lists query" "query" autoscaled_tiers_lists query

kubectl scale -n "${NAMESPACE}" "deployment/${QUERY_DEPLOY}" --replicas=3 >/dev/null
wait_field_equals "query Deployment spec.replicas after external scale" "3" deployment_replicas "${QUERY_DEPLOY}"

force_reconcile
wait_field_equals "query Deployment spec.replicas after forced reconcile" "3" deployment_replicas "${QUERY_DEPLOY}"

###############################################################################
# d. Turn-off keeps the live count.
###############################################################################
log "step d: raising the gateway's bounds to 4/4 so the HPA enforces 4 with no metrics-server"
kubectl patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  '{"spec":{"gateway":{"autoscaling":{"mode":"Managed","minReplicas":4,"maxReplicas":4,"targetCPUUtilizationPercentage":80}}}}' \
  >/dev/null
wait_field_equals "gateway Deployment spec.replicas is 4 (HPA bounds enforced)" "4" deployment_replicas "${GATEWAY_DEPLOY}"

log "step d: removing the gateway's autoscaling block (spec.gateway.replicas stays 3, unchanged)"
kubectl patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  '{"spec":{"gateway":{"autoscaling":null}}}' >/dev/null

for pass in 1 2; do
  force_reconcile
  wait_field_equals "gateway Deployment spec.replicas stays at the adopted count (pass ${pass})" "4" \
    deployment_replicas "${GATEWAY_DEPLOY}"
done

wait_absent "HPA ${GATEWAY_HPA} deleted after turn-off" hpa_exists "${GATEWAY_HPA}"

adopted="$(wait_field_nonempty "status.replicaAdoptions records gateway" replica_adoption_adopted gateway)"
assert_eq "status.replicaAdoptions gateway adoptedReplicas" "4" "${adopted}"

###############################################################################
# e. Leave the cluster as kind-up.sh and kind-demo.sh left it.
###############################################################################
log "step e: restoring both tiers to their original shape"
# The query tier is still External: its live replicas (3, set directly on
# the Deployment in step c) is not the RavelCluster's declared value, which
# never changed from ${ORIGINAL_QUERY_REPLICAS} (step c only added the
# autoscaling block). Per ADR-4314's declared-count amendment, removing
# autoscaling without a declared-value change in the same edit adopts the
# live count rather than forcing the declared one -- so the live count is
# brought back down directly first, exactly as the external scaler that
# raised it would, before the autoscaling block is removed.
kubectl scale -n "${NAMESPACE}" "deployment/${QUERY_DEPLOY}" --replicas="${ORIGINAL_QUERY_REPLICAS}" >/dev/null
kubectl patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  "{\"spec\":{\"gateway\":{\"replicas\":${ORIGINAL_GATEWAY_REPLICAS}},\"query\":{\"replicas\":${ORIGINAL_QUERY_REPLICAS},\"autoscaling\":null}}}" \
  >/dev/null

wait_field_equals "gateway Deployment spec.replicas restored to ${ORIGINAL_GATEWAY_REPLICAS}" \
  "${ORIGINAL_GATEWAY_REPLICAS}" deployment_replicas "${GATEWAY_DEPLOY}"
wait_field_equals "gateway Deployment status.readyReplicas restored to ${ORIGINAL_GATEWAY_REPLICAS}" \
  "${ORIGINAL_GATEWAY_REPLICAS}" deployment_ready "${GATEWAY_DEPLOY}"
wait_field_equals "query Deployment spec.replicas restored to ${ORIGINAL_QUERY_REPLICAS}" \
  "${ORIGINAL_QUERY_REPLICAS}" deployment_replicas "${QUERY_DEPLOY}"
wait_field_equals "query Deployment status.readyReplicas restored to ${ORIGINAL_QUERY_REPLICAS}" \
  "${ORIGINAL_QUERY_REPLICAS}" deployment_ready "${QUERY_DEPLOY}"

log "every ADR-4314 replicas-handover assertion held"
