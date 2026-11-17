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
# Forcing a reconcile pass and ordering reads against it (the heart of this
# check). The operator's primary RavelCluster watch drops every event whose
# `metadata.generation` did not change: it filters on `predicates::generation`
# (services/ravel-operator/src/controller.rs around line 4141, pinned by the
# test `generation_predicate_drops_status_only_events`). An annotation or any
# status-only write leaves `metadata.generation` untouched (Kubernetes
# upstream: the apiserver bumps `metadata.generation` only on a spec change),
# so it forces NO pass. This script forces a pass with a real spec change that
# bumps the generation but changes nothing the following assertion rests on,
# then waits until the operator has COMPLETED a SUCCESSFUL pass at that
# generation before it reads: the Available condition True and StoreQualified
# True, each with its own observedGeneration equal to `metadata.generation`, in
# one snapshot (wait_reconcile_complete has the operator lines this rests on).
# `status.observedGeneration` alone is not that signal: the operator's error
# path and its qualification hold stamp it too, without applying the tiers.
# This is the per-condition check scripts/kind-up.sh's `wait_cluster_condition`
# uses. Only after a pass is proven complete is a "the operator did NOT change this"
# assertion made (via a single read + exact-equality assert, not a poll that
# would return on the value the script itself just set).
#
# Requires scripts/kind-up.sh and scripts/kind-demo.sh to have completed
# (the demo RavelCluster "dev" exists, Available, with ready gateway and
# query Deployments).
#
# Environment:
#   RAVEL_AUTOSCALING_WAIT_TIMEOUT_SECONDS  bound on every wait (default 180)
#   RAVEL_AUTOSCALING_WAIT_INTERVAL_SECONDS poll interval (default 3)
#   RAVEL_AUTOSCALING_RESTORE_ON_FAILURE    when set, restore the cluster even
#                                           on a failed run (default: on
#                                           failure the cluster state is left
#                                           intact for the lane's diagnostics
#                                           dump, which runs after this script
#                                           exits; the lane deletes the cluster
#                                           unconditionally afterward)
# The two *_SECONDS variables exist so scripts/kind-autoscaling-check.test.sh
# can run every wait against a stub kubectl in a couple of seconds instead of
# the real timeout.
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

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

# Monotonic value written into a neutral spec field to force each reconcile
# pass. It must change every time or the apiserver records no generation
# change and the forced pass is a no-op (see force_reconcile).
RECONCILE_NONCE=0

log() {
  echo "[kind-autoscaling-check] $*" >&2
}

die() {
  log "$*"
  exit 1
}

# Runs a mutating kubectl call ("$@" after the verb), failing the whole script
# at once with the command and its stderr if the apiserver rejects it. A
# rejected patch/scale/annotate that went unchecked would leave the next read
# asserting against stale state and reporting a false pass.
kc() {
  local err
  # 2>&1 >/dev/null routes stderr to the captured stream and stdout to the
  # bin; a rejected call's message is then in ${err}.
  if ! err="$(kubectl "$@" 2>&1 >/dev/null)"; then
    die "kubectl $* failed: ${err}"
  fi
}

# Reads a value with "$@" (a kubectl invocation). Prints it on stdout and
# returns 0 when non-empty; logs "could not read <description>" and returns 1
# (NOT exit: this runs in a command substitution, where exit would leave only
# the subshell and the caller would read an empty value as success) on an
# empty result. The caller checks the status on the same line and stops.
read_now() {
  local desc="$1"
  shift
  local val
  val="$("$@" 2>/dev/null)" || val=""
  if [[ -z "${val}" ]]; then
    log "could not read ${desc}"
    return 1
  fi
  printf '%s' "${val}"
}

# Compares an already-read value against what was expected, printing one
# line either way so a passing run still shows every assertion's expected
# and actual value. Exact string equality only: a substring match would let
# "13" satisfy an expectation of "3" (ADR-4314's own failure mode for this
# check), which is exactly the bug scripts/kind-autoscaling-check.test.sh's
# distinguishing case A exists to catch.
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

# Asserts that spec.replicas has been handed off a horizontal scaler and that
# ravel-operator no longer owns it (ADR-4314 decision 2's handover). ${owners}
# is the comma-joined list of every field manager that owns spec.replicas on
# the Deployment. The check fails when:
#   - the list is empty: after the handover the field must have an owner; no
#     owner means the server-side apply handover never took effect, and
#   - ravel-operator is ANY element of the list: co-ownership means the
#     operator is still applying spec.replicas beside the scaler (fighting it),
#     not the equality test "the single owner is literally ravel-operator",
#     which a co-owning operator would slip past.
assert_replicas_handed_over() {
  local desc="$1" owners="$2"
  if [[ -z "${owners}" ]]; then
    die "ASSERT FAIL: ${desc}: no field manager owns spec.replicas (handover did not take effect)"
  fi
  local saved_ifs="${IFS}" owner
  IFS=','
  for owner in ${owners}; do
    if [[ "${owner}" == "ravel-operator" ]]; then
      IFS="${saved_ifs}"
      die "ASSERT FAIL: ${desc}: ravel-operator must not own spec.replicas, it co-owns or owns it (owners: ${owners})"
    fi
  done
  IFS="${saved_ifs}"
  log "ASSERT OK: ${desc}: handed over (owners: ${owners})"
}

# Asserts that ravel-operator is one of spec.replicas' field managers: after a
# turn-off the operator applies the count itself again. It applies with
# server-side apply and force (PatchParams::apply("ravel-operator").force(),
# services/ravel-operator/src/controller.rs around line 1403, setting
# spec.replicas around line 2887), and an applier owns every field it applies:
# force takes a conflicting field from its other managers, and an equal value
# is shared with them (Kubernetes server-side apply), so membership, not sole
# ownership, is the claim.
assert_replicas_owned_by_operator() {
  local desc="$1" owners="$2"
  local saved_ifs="${IFS}" owner
  IFS=','
  for owner in ${owners}; do
    if [[ "${owner}" == "ravel-operator" ]]; then
      IFS="${saved_ifs}"
      log "ASSERT OK: ${desc}: ravel-operator owns spec.replicas (owners: ${owners})"
      return 0
    fi
  done
  IFS="${saved_ifs}"
  die "ASSERT FAIL: ${desc}: ravel-operator must own spec.replicas after turn-off (owners: ${owners:-<none>})"
}

# Polls "$@" until it prints exactly $expected, bounded by WAIT_TIMEOUT. Used
# for genuine convergence waits (a Deployment the operator or an HPA is still
# rolling toward a count), NOT for "the operator left this unchanged"
# assertions, which must read once after a pass is proven complete. A timeout
# names what was awaited and the last value actually seen.
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
# (used for "does this status list now record the tier" reads where the
# expected text is the tier name itself read back).
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

# One jsonpath over the RavelCluster: metadata.generation, then status, own
# observedGeneration and reason of the Available, StoreQualified and Degraded
# conditions, joined by '|' (no value can contain one: integers, True/False/
# Unknown, CamelCase reasons). Applied by kubectl to one fetched object, so all
# nine fields come from the same snapshot.
COMPLETED_PASS_JSONPATH='{.metadata.generation}'
for _ctype in Available StoreQualified Degraded; do
  COMPLETED_PASS_JSONPATH+="|{.status.conditions[?(@.type==\"${_ctype}\")].status}"
  COMPLETED_PASS_JSONPATH+="|{.status.conditions[?(@.type==\"${_ctype}\")].observedGeneration}"
  COMPLETED_PASS_JSONPATH+="|{.status.conditions[?(@.type==\"${_ctype}\")].reason}"
done
unset _ctype

# Waits until the operator has completed a SUCCESSFUL reconcile pass at the
# current spec generation, i.e. a pass that ran every tier apply. Mirrors
# scripts/kind-up.sh's wait_cluster_condition (not sourced: kind-up.sh runs the
# whole bring-up at top level). status.observedGeneration alone proves nothing:
# all three status writers in services/ravel-operator/src/controller.rs stamp
# it with the pass's generation, including the error path (reconcile_timed,
# write_degraded_status around line 1762, which build_degraded_status turns
# into Available=False and Degraded=True, also after a failure partway through
# the tier applies) and the store-qualification hold (write_status around line
# 2003, which returns before any tier apply yet reports Available=True when a
# prior pass recorded ready replicas). So, from the single snapshot above, this
# requires:
#   - Available=True with its own observedGeneration == metadata.generation.
#     Only the success path writes that after its tier applies: the apply loop
#     around line 2457 propagates any error with `?`, then write_status around
#     line 2586 stamps obj.metadata.generation, and build_status (around line
#     3441) emits Available=True at that generation when both request-serving
#     tiers report ready replicas.
#   - StoreQualified=True at the same generation. The qualification hold writes
#     StoreQualified=False at the current generation (hold_for_qualification,
#     around line 1556); the success path passes the gate with
#     StoreQualified=True at it (around line 1888).
# A snapshot that does not satisfy both keeps the wait polling until the
# deadline, so a transient error the operator retries does not fail a correct
# operator. A timeout names the generation awaited and the last Available,
# StoreQualified and Degraded status and reason seen.
wait_reconcile_complete() {
  local deadline=$(( $(date +%s) + WAIT_TIMEOUT ))
  local snap gen a_status a_gen a_reason q_status q_gen q_reason d_status d_gen d_reason
  while true; do
    snap="$(kubectl get -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" \
      -o jsonpath="${COMPLETED_PASS_JSONPATH}" 2>/dev/null || true)"
    IFS='|' read -r gen a_status a_gen a_reason q_status q_gen q_reason d_status d_gen d_reason <<<"${snap}"
    if [[ -n "${gen}" && "${a_status}" == "True" && "${a_gen}" == "${gen}" && "${q_status}" == "True" && "${q_gen}" == "${gen}" ]]; then
      log "ASSERT OK: operator completed a successful reconcile pass at generation ${gen} (Available=True, StoreQualified=True at generation ${gen})"
      return 0
    fi
    if (( $(date +%s) >= deadline )); then
      die "timed out after ${WAIT_TIMEOUT}s waiting for the operator to complete a successful reconcile pass at generation ${gen:-?}: last seen Available=${a_status:-<none>} (reason ${a_reason:-<none>}, generation ${a_gen:-<none>}), StoreQualified=${q_status:-<none>} (reason ${q_reason:-<none>}, generation ${q_gen:-<none>}), Degraded=${d_status:-<none>} (reason ${d_reason:-<none>}, generation ${d_gen:-<none>})"
    fi
    sleep "${WAIT_INTERVAL}"
  done
}

# Forces a reconcile pass and waits until it has completed. It bumps
# metadata.generation by writing a new value into the named tier's
# autoscaling.scaleDownStabilizationSeconds. The operator reads that field in
# one place, desired_hpa (services/ravel-operator/src/autoscaling.rs, around
# line 620), as the Managed HPA's behavior.scaleDown.stabilizationWindowSeconds;
# External mode renders no HPA (desired_hpa returns None for it), and the
# per-tier replica plan never reads the field. So the operator's own replica
# decision and its spec.replicas ownership do not depend on it. On a Managed
# tier the HPA does read it, and that is neutral here too: kind runs no
# metrics-server, so the HPA computes no metric recommendation, which is the
# only input the upstream HPA controller passes through the stabilization
# window; it scales only to bring the count inside [minReplicas,
# maxReplicas], and that path does not consult the window. This is why step b
# can force through the gateway and then read the gateway (count 3, inside
# 2..4). The value is the monotonic RECONCILE_NONCE: it MUST differ from the
# field's previous value, or the apiserver records no generation change and
# wait_reconcile_complete would return on a prior pass. $1 is the tier whose
# (already-present) autoscaling block carries the field.
force_reconcile() {
  local tier="$1"
  RECONCILE_NONCE=$((RECONCILE_NONCE + 1))
  kc patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
    "{\"spec\":{\"${tier}\":{\"autoscaling\":{\"scaleDownStabilizationSeconds\":${RECONCILE_NONCE}}}}}"
  log "forced reconcile ${RECONCILE_NONCE} via spec.${tier}.autoscaling.scaleDownStabilizationSeconds"
  wait_reconcile_complete
}

deployment_replicas() {
  local dep="$1"
  kubectl get -n "${NAMESPACE}" "deployment/${dep}" -o jsonpath='{.spec.replicas}'
}

deployment_ready() {
  local dep="$1"
  kubectl get -n "${NAMESPACE}" "deployment/${dep}" -o jsonpath='{.status.readyReplicas}'
}

# Every field manager that owns spec.replicas on a Deployment, comma-joined,
# or empty if none does (ADR-4314 decision 2's handover: ravel-operator before
# the handover, a second manager after it). Joined rather than reduced to one
# so assert_replicas_handed_over can reject co-ownership.
replicas_field_managers() {
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

# Number of status.replicaAdoptions entries; "0" when the field is absent (the
# operator writes it as null once empty, crd.rs replica_adoptions), so a
# successful read is never empty.
replica_adoptions_count() {
  kubectl get -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" -o json |
    jq -r '(.status.replicaAdoptions // []) | length'
}

cluster_uid() {
  kubectl get -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" -o jsonpath='{.metadata.uid}'
}

cleanup() {
  local status=$?
  if (( status != 0 )) && [[ -z "${RAVEL_AUTOSCALING_RESTORE_ON_FAILURE:-}" ]]; then
    # Leave the failed cluster state intact. The lane's "Dump cluster
    # diagnostics on failure" step runs AFTER this script exits; restoring
    # here would overwrite exactly the state that step needs to capture. The
    # lane deletes the cluster unconditionally afterward, so skipping the
    # restore strands nothing. Set RAVEL_AUTOSCALING_RESTORE_ON_FAILURE to
    # restore even on failure (for a cluster you keep across runs).
    log "check failed (exit ${status}); leaving cluster state for the lane's diagnostics dump, skipping restore"
    exit "${status}"
  fi
  log "restoring the demo cluster: both tiers back to ${ORIGINAL_GATEWAY_REPLICAS}/${ORIGINAL_QUERY_REPLICAS} replicas, no autoscaling block"
  kubectl patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
    "{\"spec\":{\"gateway\":{\"replicas\":${ORIGINAL_GATEWAY_REPLICAS},\"autoscaling\":null},\"query\":{\"replicas\":${ORIGINAL_QUERY_REPLICAS},\"autoscaling\":null}}}" \
    >/dev/null 2>&1 || true
  exit "${status}"
}

for tool in kubectl jq; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required but not on PATH"
done

kubectl config current-context >/dev/null 2>&1 ||
  die "no current kubectl context; run scripts/kind-up.sh first"

log "reading the RavelCluster uid (for HPA ownerReference assertions)"
CLUSTER_UID="$(read_now "RavelCluster ${CLUSTER_CR} uid" cluster_uid)" || exit 1

trap cleanup EXIT

###############################################################################
# a. Scale the gateway to 3 replicas through the RavelCluster spec.
###############################################################################
log "step a: scaling spec.gateway.replicas to 3"
kc patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge \
  -p '{"spec":{"gateway":{"replicas":3}}}'

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
kc patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p "${gateway_patch}"

wait_field_equals "status.autoscaledTiers lists gateway" "gateway" autoscaled_tiers_lists gateway

# Each pass: force a real reconcile and wait until it has completed, THEN read
# once and assert the operator left spec.replicas at 3 and handed the field
# off. Reading only after wait_reconcile_complete is what makes "the operator
# did not reset it" a real claim: a poll would return on the value already
# there before any pass ran.
for pass in 1 2; do
  force_reconcile gateway
  reps="$(read_now "gateway Deployment spec.replicas after forced reconcile ${pass}" deployment_replicas "${GATEWAY_DEPLOY}")" || exit 1
  assert_eq "gateway Deployment spec.replicas after forced reconcile ${pass}" "3" "${reps}"
  owners="$(replicas_field_managers "${GATEWAY_DEPLOY}")" || owners=""
  assert_replicas_handed_over "gateway Deployment spec.replicas owners (pass ${pass})" "${owners}"
done

hpa_exists "${GATEWAY_HPA}" || die "HPA ${GATEWAY_HPA} does not exist after enabling Managed autoscaling"
log "ASSERT OK: HPA ${GATEWAY_HPA} exists"
managed_by="$(read_now "HPA ${GATEWAY_HPA} managed-by label" hpa_label "${GATEWAY_HPA}" "app.kubernetes.io/managed-by")" || exit 1
assert_eq "HPA ${GATEWAY_HPA} managed-by label" "ravel-operator" "${managed_by}"
instance_label="$(read_now "HPA ${GATEWAY_HPA} instance label" hpa_label "${GATEWAY_HPA}" "app.kubernetes.io/instance")" || exit 1
assert_eq "HPA ${GATEWAY_HPA} instance label" "${CLUSTER_CR}" "${instance_label}"
owner_uid="$(read_now "HPA ${GATEWAY_HPA} ownerReference uid" hpa_owner_uid "${GATEWAY_HPA}")" || exit 1
assert_eq "HPA ${GATEWAY_HPA} ownerReference uid" "${CLUSTER_UID}" "${owner_uid}"

###############################################################################
# c. External autoscaling on the query tier.
###############################################################################
log "step c: enabling External autoscaling on the query tier"
kc patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  '{"spec":{"query":{"autoscaling":{"mode":"External","minReplicas":1,"maxReplicas":4}}}}'

wait_field_equals "status.autoscaledTiers lists query" "query" autoscaled_tiers_lists query

kc scale -n "${NAMESPACE}" "deployment/${QUERY_DEPLOY}" --replicas=3
wait_field_equals "query Deployment spec.replicas after external scale" "3" deployment_replicas "${QUERY_DEPLOY}"

# Force a completed pass, then read once: an External tier the operator must
# not reset to its declared count (1). The poll above only proved the external
# scale landed; this proves the operator left it alone across a reconcile.
force_reconcile query
reps="$(read_now "query Deployment spec.replicas after forced reconcile" deployment_replicas "${QUERY_DEPLOY}")" || exit 1
assert_eq "query Deployment spec.replicas after forced reconcile" "3" "${reps}"
owners="$(replicas_field_managers "${QUERY_DEPLOY}")" || owners=""
assert_replicas_handed_over "query Deployment spec.replicas owners after forced reconcile" "${owners}"
listed="$(read_now "status.autoscaledTiers lists query after forced reconcile" autoscaled_tiers_lists query)" || exit 1
assert_eq "status.autoscaledTiers lists query after forced reconcile" "query" "${listed}"

###############################################################################
# d. Turn-off keeps the live count.
###############################################################################
log "step d: raising the gateway's bounds to 4/4 so the HPA enforces 4 with no metrics-server"
kc patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  '{"spec":{"gateway":{"autoscaling":{"mode":"Managed","minReplicas":4,"maxReplicas":4,"targetCPUUtilizationPercentage":80}}}}'
wait_field_equals "gateway Deployment spec.replicas is 4 (HPA bounds enforced)" "4" deployment_replicas "${GATEWAY_DEPLOY}"

log "step d: removing the gateway's autoscaling block (spec.gateway.replicas stays 3, unchanged)"
kc patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  '{"spec":{"gateway":{"autoscaling":null}}}'

# Wait until the turn-off pass has completed, then read once: the operator
# must have adopted the live count (4), not snapped back to the declared value
# (3). This single read right after the pass completes is what catches an
# operator that records the adoption but applies the wrong number; a later
# re-apply pass would hide that by converging it.
wait_reconcile_complete
reps="$(read_now "gateway Deployment spec.replicas after turn-off" deployment_replicas "${GATEWAY_DEPLOY}")" || exit 1
assert_eq "gateway Deployment spec.replicas after turn-off (adopted count)" "4" "${reps}"

wait_absent "HPA ${GATEWAY_HPA} deleted after turn-off" hpa_exists "${GATEWAY_HPA}"

adopted="$(wait_field_nonempty "status.replicaAdoptions records gateway" replica_adoption_adopted gateway)"
assert_eq "status.replicaAdoptions gateway adoptedReplicas" "4" "${adopted}"
owners="$(replicas_field_managers "${GATEWAY_DEPLOY}")" || owners=""
assert_replicas_owned_by_operator "gateway Deployment spec.replicas owners after turn-off" "${owners}"

# Prove the adopted count HOLDS across a further completed pass. gateway has no
# autoscaling block now, so force the pass through the query tier (still
# External); re-read gateway and assert 4 only after the pass is complete.
for pass in 1 2; do
  force_reconcile query
  reps="$(read_now "gateway Deployment spec.replicas stays at adopted count (pass ${pass})" deployment_replicas "${GATEWAY_DEPLOY}")" || exit 1
  assert_eq "gateway Deployment spec.replicas stays at adopted count (pass ${pass})" "4" "${reps}"
done

###############################################################################
# e. Leave the cluster as kind-up.sh and kind-demo.sh left it.
###############################################################################
log "step e: restoring both tiers to their original shape"
# Every count below is applied by the operator, never by this script, and is
# read once after a completed pass, so each assertion is about the operator.
#
# The query tier is still External, declared ${ORIGINAL_QUERY_REPLICAS} with
# a live count of 3. Removing the autoscaling block without changing the
# declared value would adopt the live count and leave a status.replicaAdoptions
# entry for query until spec.query.replicas next changes
# (services/ravel-operator/src/autoscaling.rs: the turn-off branch around line
# 365 adopts unless the same edit changed `replicas`; the record holds while
# `declaredReplicas` equals the declared value, around line 383). Restoring
# the original value cannot change it, since the original is the value the
# entry recorded. So the restore takes two edits:
#   e1. remove the block while declaring ${ORIGINAL_QUERY_REPLICAS} + 1: a
#       `replicas` edited in the same change wins at once and records no
#       adoption. The gateway's declared value goes back to
#       ${ORIGINAL_GATEWAY_REPLICAS} in the same edit, which differs from its
#       adoption's declaredReplicas (3), so the operator applies it and drops
#       that adoption.
#   e2. declare ${ORIGINAL_QUERY_REPLICAS}: no autoscaledTiers entry and no
#       adoption, so the operator applies the declared value.
QUERY_STEP_REPLICAS=$((ORIGINAL_QUERY_REPLICAS + 1))
log "step e1: removing query autoscaling with spec.query.replicas ${QUERY_STEP_REPLICAS}, spec.gateway.replicas ${ORIGINAL_GATEWAY_REPLICAS}"
kc patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  "{\"spec\":{\"gateway\":{\"replicas\":${ORIGINAL_GATEWAY_REPLICAS}},\"query\":{\"replicas\":${QUERY_STEP_REPLICAS},\"autoscaling\":null}}}"
wait_reconcile_complete
reps="$(read_now "query Deployment spec.replicas after step e1" deployment_replicas "${QUERY_DEPLOY}")" || exit 1
assert_eq "query Deployment spec.replicas after step e1 (same-edit declared value)" "${QUERY_STEP_REPLICAS}" "${reps}"
reps="$(read_now "gateway Deployment spec.replicas after step e1" deployment_replicas "${GATEWAY_DEPLOY}")" || exit 1
assert_eq "gateway Deployment spec.replicas after step e1 (declared value changed)" "${ORIGINAL_GATEWAY_REPLICAS}" "${reps}"

log "step e2: spec.query.replicas back to ${ORIGINAL_QUERY_REPLICAS}"
kc patch -n "${NAMESPACE}" "ravelcluster/${CLUSTER_CR}" --type merge -p \
  "{\"spec\":{\"query\":{\"replicas\":${ORIGINAL_QUERY_REPLICAS}}}}"
wait_reconcile_complete
reps="$(read_now "query Deployment spec.replicas after restore" deployment_replicas "${QUERY_DEPLOY}")" || exit 1
assert_eq "query Deployment spec.replicas after restore" "${ORIGINAL_QUERY_REPLICAS}" "${reps}"
reps="$(read_now "gateway Deployment spec.replicas after restore" deployment_replicas "${GATEWAY_DEPLOY}")" || exit 1
assert_eq "gateway Deployment spec.replicas after restore" "${ORIGINAL_GATEWAY_REPLICAS}" "${reps}"
adoptions="$(read_now "status.replicaAdoptions entry count after restore" replica_adoptions_count)" || exit 1
assert_eq "status.replicaAdoptions entry count after restore" "0" "${adoptions}"

# Readiness is genuine convergence (pods starting or terminating), so these
# poll.
wait_field_equals "gateway Deployment status.readyReplicas restored to ${ORIGINAL_GATEWAY_REPLICAS}" \
  "${ORIGINAL_GATEWAY_REPLICAS}" deployment_ready "${GATEWAY_DEPLOY}"
wait_field_equals "query Deployment status.readyReplicas restored to ${ORIGINAL_QUERY_REPLICAS}" \
  "${ORIGINAL_QUERY_REPLICAS}" deployment_ready "${QUERY_DEPLOY}"

log "every ADR-4314 replicas-handover assertion held"
