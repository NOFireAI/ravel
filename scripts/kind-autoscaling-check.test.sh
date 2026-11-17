#!/usr/bin/env bash
# Cases for kind-autoscaling-check.sh (issue #4324), in the pattern of
# scripts/guards/check-quick-xml-shipped-reachability.test.sh: run the real
# script against a stub `kubectl` on PATH instead of a real apiserver.
#
# The stub keeps a small stateful "world" (a JSON file: the RavelCluster spec,
# its metadata.generation and status.observedGeneration, each Deployment's live
# replicas/field-manager, and each HPA) and models the operator's reconcile
# loop coarsely but with the two properties this check turns on:
#
#   - A reconcile happens ONLY when the RavelCluster's generation changes (or
#     as the requeued retry of a failed pass). A `patch` that changes the spec
#     bumps metadata.generation, runs a reconcile and stamps
#     status.observedGeneration and the Available/StoreQualified/Degraded
#     conditions with the new generation, the way whichever of the operator's
#     three status writers ran would (the real operator's primary watch
#     filters on predicates::generation, so a pass runs only on a generation
#     change). A `scale` on a Deployment (the external scaler) changes
#     that Deployment's replicas WITHOUT a reconcile or a generation bump. An
#     `annotate` changes nothing: it neither bumps the generation nor runs a
#     pass (the real script no longer annotates to force passes for exactly
#     this reason, but the stub still accepts the verb harmlessly).
#   - Injectable misbehaving operator models, each a stub env knob, so a case
#     can prove the real script FAILS when the operator does the wrong thing:
#     an External tier reset to its declared count, ravel-operator co-owning
#     spec.replicas after the handover, a turn-off that records the right
#     adoption but applies the wrong count or does not take spec.replicas
#     back, a later External pass that drops the tier's autoscaledTiers entry,
#     an operator that never advances observedGeneration, a pass that stamps
#     the generation on the error path (Degraded) without applying the query
#     tier, and a qualification hold that stamps it without applying any tier.
#
# It does not re-prove the operator's own reconcile loop; that is
# services/ravel-operator/src/autoscaling.rs's test suite's job. This suite
# exists to prove the real script's assertions are correct and non-vacuous
# given eventually-consistent, generation-ordered reads.
#
# RAVEL_AUTOSCALING_WAIT_TIMEOUT_SECONDS / _INTERVAL_SECONDS (read by the
# real script) are set short here so a case that must time out does so in
# seconds, not minutes.
#
# Run: bash scripts/kind-autoscaling-check.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REAL_SCRIPT="${HERE}/kind-autoscaling-check.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/kind-autoscaling-check-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0
CLUSTER_UID="00000000-0000-0000-0000-000000000001"

# --- the stub kubectl ------------------------------------------------------
STUB_BIN="${TMP}/stub-bin"
mkdir -p "${STUB_BIN}"

# The reconcile/mergepatch logic, as a jq library sourced by the stub. Kept
# in its own file (rather than inlined per-call) so it is written once.
#
# reconcile_tier reads these injected-model variables, defaulted to "" by the
# stub when the knob is off (pass, below, reads the rest):
#   $external_resets  -- non-empty: an External tier is (wrongly) reset to its
#                        declared spec.replicas on every pass.
#   $turnoff_applies  -- a number: on turn-off, record adoptedReplicas as the
#                        live count but APPLY this number to the Deployment
#                        (empty: apply the live count, the correct behavior).
#   $turnoff_keeps_handover -- non-empty: on turn-off, adopt the live count but
#                        leave spec.replicas with the handover manager.
#   $drop_external_entry -- non-empty: an External pass that finds the tier
#                        already listed drops its autoscaledTiers entry.
#   $turnoff_ignores_same_edit -- non-empty: a turn-off adopts the live count
#                        even when the same edit changed spec.replicas.
#   $keep_adoptions   -- non-empty: a changed declared value is applied but
#                        the tier's replicaAdoptions entry is never dropped.
JQ_LIB="${TMP}/reconcile.jq"
cat >"${JQ_LIB}" <<'JQ'
# RFC 7396 JSON Merge Patch: a null value deletes the key, unlike jq's own
# `*` operator (which keeps null as a value). Only object-shaped patches are
# needed here (the real script never merge-patches a scalar at the top).
def mergepatch(target; patch):
  if (patch | type) != "object" then patch
  else
    reduce (patch | keys_unsorted[]) as $k
      (target // {};
        if (patch[$k] == null) then del(.[$k])
        elif ((.[$k] // null) | type) == "object" and (patch[$k] | type) == "object"
        then .[$k] = mergepatch(.[$k]; patch[$k])
        else .[$k] = patch[$k]
        end)
  end;

def reconcile_tier(tierKey; depName):
  (.status.replicaAdoptions // [] | map(select(.tier == tierKey)) | first) as $adoption
  | (.status.autoscaledTiers // [] | map(select(.tier == tierKey)) | first) as $declaredEntry
  | .cluster[tierKey] as $spec
  | .deployments[depName] as $dep
  | ($spec.autoscaling) as $asc
  | if $asc == null then
      (if $adoption != null then
         # Autoscaling has been off for a while and an adoption is recorded:
         # hold the adopted count while the declared value is unchanged, drop
         # the adoption and force the declared value once it changes.
         (if $spec.replicas == $adoption.declaredReplicas then
            .deployments[depName].replicas = $adoption.adoptedReplicas
          else
            .deployments[depName].replicas = $spec.replicas
            | .deployments[depName].manager = "ravel-operator"
            | (if ($keep_adoptions // "") == "" then
                 .status.replicaAdoptions = (.status.replicaAdoptions | map(select(.tier != tierKey)))
               else . end)
          end)
       elif $dep.manager == "ravel-operator-handover" then
         # Just turned off. ADR-4314's declared-count amendment: compare the
         # NEW declared value (this patch's spec.replicas) against the
         # declaredReplicas the autoscaledTiers entry held the moment before
         # removal, not against the live count. A same-edit change of the
         # declared value forces it immediately; otherwise the live count
         # (wherever the HPA or external scaler left it) is adopted and held
         # until spec.replicas actually changes.
         (if ($declaredEntry == null)
             or ($spec.replicas != $declaredEntry.declaredReplicas and ($turnoff_ignores_same_edit // "") == "") then
            .deployments[depName].replicas = $spec.replicas
            | .deployments[depName].manager = "ravel-operator"
          else
            ($dep.replicas) as $live
            | (if ($turnoff_applies // "") == "" then $live else ($turnoff_applies | tonumber) end) as $applied
            | .deployments[depName].replicas = $applied
            | .status.replicaAdoptions = (.status.replicaAdoptions + [{"tier": tierKey, "adoptedReplicas": $live, "declaredReplicas": $spec.replicas}])
            | .deployments[depName].manager = (if ($turnoff_keeps_handover // "") == "" then "ravel-operator" else "ravel-operator-handover" end)
          end)
       else
         .deployments[depName].replicas = $spec.replicas
         | .deployments[depName].manager = "ravel-operator"
       end)
      | .status.autoscaledTiers = (.status.autoscaledTiers | map(select(.tier != tierKey)))
      | .hpas = (.hpas | with_entries(select(.key != depName)))
    elif $asc.mode == "External" and ($drop_external_entry // "") != "" and $declaredEntry != null then
      # Injected misbehaving model: a later External pass drops the tier's
      # autoscaledTiers entry (and otherwise leaves the tier alone).
      .status.autoscaledTiers = (.status.autoscaledTiers | map(select(.tier != tierKey)))
    else
      .status.autoscaledTiers = ((.status.autoscaledTiers | map(select(.tier != tierKey))) + [{"tier": tierKey, "declaredReplicas": $spec.replicas}])
      | .deployments[depName].manager = "ravel-operator-handover"
      | .status.replicaAdoptions = (.status.replicaAdoptions | map(select(.tier != tierKey)))
      | (if $asc.mode == "Managed" then
           .hpas[depName] = {"labels": {"app.kubernetes.io/managed-by": "ravel-operator", "app.kubernetes.io/instance": "dev"}, "ownerUid": .uid}
           | .deployments[depName].replicas = (
               if .deployments[depName].replicas < $asc.minReplicas then $asc.minReplicas
               elif .deployments[depName].replicas > $asc.maxReplicas then $asc.maxReplicas
               else .deployments[depName].replicas end)
         else
           # External: the operator renders no HPA and does not touch
           # spec.replicas -- unless the injected misbehaving model resets it
           # to the declared count.
           .hpas = (.hpas | with_entries(select(.key != depName)))
           | (if ($external_resets // "") == "" then . else .deployments[depName].replicas = $spec.replicas end)
         end)
    end
    # Pods become ready instantly in this model: there is no real kubelet to
    # wait on, and the real script's readiness waits exist for a genuine
    # apiserver, not for this stub.
    | .deployments[depName].ready = .deployments[depName].replicas;

# The status conditions each operator status writer leaves, keyed by type,
# each stamped with the pass's generation like the real operator's
# (services/ravel-operator/src/controller.rs): the success path's write_status
# (Available=True, StoreQualified=True, no Degraded), the error path's
# write_degraded_status (Available=False and Degraded=True on one reason,
# StoreQualified carried as the gate left it), and the qualification hold's
# write_status (StoreQualified=False; Available stays True on the prior ready
# replicas).
def mark_success:
  .conditions.Available = {"status": "True", "observedGeneration": .generation, "reason": "MinimumReplicasAvailable"}
  | .conditions.StoreQualified = {"status": "True", "observedGeneration": .generation, "reason": "Succeeded"}
  | del(.conditions.Degraded);
def mark_degraded:
  .conditions.Available = {"status": "False", "observedGeneration": .generation, "reason": "ReconcileError"}
  | .conditions.Degraded = {"status": "True", "observedGeneration": .generation, "reason": "ReconcileError"}
  | .conditions.StoreQualified.observedGeneration = .generation;
def mark_qualify_hold:
  .conditions.Available = {"status": "True", "observedGeneration": .generation, "reason": "MinimumReplicasAvailable"}
  | .conditions.StoreQualified = {"status": "False", "observedGeneration": .generation, "reason": "Pending"};

def query_external: (.cluster.query.autoscaling.mode // "") == "External";

# One reconcile pass at the current generation. $first is true for the pass a
# generation change triggers and false for a retry the operator requeues
# after an error. Injected misbehaving models:
#   $qualify_hold     -- non-empty: once the query tier is External and listed
#                        in autoscaledTiers (every pass after the first
#                        External one), every pass is a qualification hold (no
#                        tier applied).
#   $degraded_tier    -- "query": once the query tier is External, the pass
#                        applies the gateway, records the query tier's
#                        handover bookkeeping, then fails before writing the
#                        query Deployment (Degraded): the operator's error path
#                        keeps a handover already applied. $degraded_mode
#                        "once" fails only the first pass at each generation
#                        and lets the requeued retry succeed; anything else
#                        fails every pass.
#   $no_observed      -- non-empty: the pass runs but writes no status at all.
def pass($first):
  (.observedGeneration) as $prev_obs
  | (.conditions) as $prev_conditions
  | (if ($qualify_hold // "") != "" and query_external
        and ((.status.autoscaledTiers // []) | map(.tier) | index("query")) != null then
       mark_qualify_hold
     else
       reconcile_tier("gateway"; "dev-gateway")
       | if ($degraded_tier // "") == "query" and query_external
            and (($degraded_mode // "") != "once" or $first) then
           .deployments["dev-query"] as $query_before
           | reconcile_tier("query"; "dev-query")
           | .deployments["dev-query"].replicas = $query_before.replicas
           | .deployments["dev-query"].ready = $query_before.ready
           | mark_degraded
           | .retryPending = (($degraded_mode // "") == "once")
         else
           reconcile_tier("query"; "dev-query") | mark_success | .retryPending = false
         end
     end)
  | if ($no_observed // "") == "" then .observedGeneration = .generation
    else .observedGeneration = $prev_obs | .conditions = $prev_conditions end;
JQ

cat >"${STUB_BIN}/kubectl" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail

WORLD="${STUB_WORLD_FILE:?STUB_WORLD_FILE must be set}"
LIB="${STUB_JQ_LIB:?STUB_JQ_LIB must be set}"

die_stub() { echo "stub kubectl: $*" >&2; exit 1; }

# Runs a jq filter over the world with the reconcile library spliced in front
# and every injected-model knob bound, writing the result back. The library
# text is spliced directly into the filter rather than loaded with
# `-L`/`include`: jq 1.6's include mechanism silently drops the trailing
# `.ready = .replicas` mutation at the end of reconcile_tier (confirmed by
# diffing identical filter text inlined vs. included), so a library consumed
# through include cannot be trusted to apply its own last pipe stage.
world_jq() {
  local filter="$1"
  shift
  local tmp="${WORLD}.tmp"
  jq "$@" \
     --arg external_resets "${STUB_EXTERNAL_RESETS:-}" \
     --arg turnoff_applies "${STUB_TURNOFF_APPLIES:-}" \
     --arg turnoff_keeps_handover "${STUB_TURNOFF_KEEPS_HANDOVER:-}" \
     --arg drop_external_entry "${STUB_DROP_EXTERNAL_ENTRY:-}" \
     --arg turnoff_ignores_same_edit "${STUB_TURNOFF_IGNORES_SAME_EDIT:-}" \
     --arg keep_adoptions "${STUB_KEEP_ADOPTIONS:-}" \
     --arg no_observed "${STUB_NO_OBSERVED_GEN:-}" \
     --arg degraded_tier "${STUB_DEGRADED_TIER:-}" \
     --arg degraded_mode "${STUB_DEGRADED_MODE:-}" \
     --arg qualify_hold "${STUB_QUALIFY_HOLD:-}" \
     "$(cat "${LIB}")
     ${filter}" "${WORLD}" >"${tmp}" || return 1
  mv "${tmp}" "${WORLD}"
}

# Strips a leading "-n"/"--namespace" (and its value) and the bare
# ravel-system namespace token: the real script always passes one, so the stub
# ignores it.
args=()
skip_next=0
for a in "$@"; do
  if [[ "${skip_next}" == "1" ]]; then skip_next=0; continue; fi
  case "${a}" in
    -n|--namespace) skip_next=1; continue ;;
    ravel-system) continue ;;
  esac
  args+=("${a}")
done

verb="${args[0]:-}"

case "${verb}" in
  config)
    [[ "${args[1]:-}" == "current-context" ]] || die_stub "unsupported config subcommand: ${args[1]:-}"
    echo "kind-ravel-dev"
    exit 0
    ;;

  get)
    resource="${args[1]:-}"
    output=""
    # kubectl's own `-o jsonpath=...` arrives as one token (-o then the
    # value), and `-o json` the same way; scan positionally.
    for ((i=2; i<${#args[@]}; i++)); do
      if [[ "${args[$i]}" == "-o" ]]; then
        nextv="${args[$((i+1))]:-}"
        if [[ "${nextv}" == jsonpath=* ]]; then
          output="jsonpath:${nextv#jsonpath=}"
        elif [[ "${nextv}" == "json" ]]; then
          output="json"
        fi
      fi
    done

    case "${resource}" in
      ravelcluster/dev)
        if [[ -n "${STUB_EMPTY_UID:-}" && "${output}" == 'jsonpath:{.metadata.uid}' ]]; then
          echo ""
          exit 0
        fi
        # The operator requeues a failed pass and retries it; the retry lands
        # before the next read of the RavelCluster. Each retry is logged, with
        # the Available status the failed pass left, so a case can prove the
        # Degraded passes happened.
        if [[ "$(jq -r '.retryPending // false' "${WORLD}")" == "true" ]]; then
          jq -r '"retry generation=\(.generation) prior Available=\(.conditions.Available.status)"' "${WORLD}" \
            >>"${WORLD}.retries"
          world_jq 'pass(false)' || die_stub "retry reconcile failed"
        fi
        case "${output}" in
          'jsonpath:{.metadata.uid}')
            jq -r '.uid' "${WORLD}"
            ;;
          jsonpath:'{.metadata.generation}|{.status.conditions[?(@.type=="Available")].status}'*)
            # The real script's completed-pass snapshot: generation, then
            # status|observedGeneration|reason of Available, StoreQualified and
            # Degraded. An absent condition prints empty fields, as kubectl's
            # jsonpath does for a filter that matches nothing.
            expected='{.metadata.generation}'
            for ctype in Available StoreQualified Degraded; do
              for field in status observedGeneration reason; do
                expected+="|{.status.conditions[?(@.type==\"${ctype}\")].${field}}"
              done
            done
            [[ "${output}" == "jsonpath:${expected}" ]] || die_stub "unexpected completed-pass jsonpath: ${output}"
            jq -r '[(.generation | tostring)] + ([("Available", "StoreQualified", "Degraded") as $t
                | .conditions[$t] as $c
                | ($c.status // ""), (($c.observedGeneration // "") | tostring), ($c.reason // "")])
              | join("|")' "${WORLD}"
            ;;
          # The pre-fix script's snapshot (status.observedGeneration only).
          # Kept so this suite can be run against that script to show which
          # cases it wrongly passes.
          'jsonpath:{.metadata.generation}|{.status.observedGeneration}')
            jq -r '"\(.generation)|\(.observedGeneration)"' "${WORLD}"
            ;;
          'jsonpath:{.metadata.generation}')
            jq -r '.generation | tostring' "${WORLD}"
            ;;
          jsonpath:'{.status.autoscaledTiers[?(@.tier=="'*'")].tier}')
            tier="$(sed -E 's/.*@\.tier==\"([a-z]+)\".*/\1/' <<<"${output}")"
            jq -r --arg t "${tier}" '[.status.autoscaledTiers[] | select(.tier == $t) | .tier] | join("")' "${WORLD}"
            ;;
          jsonpath:'{.status.replicaAdoptions[?(@.tier=="'*'")].adoptedReplicas}')
            tier="$(sed -E 's/.*@\.tier==\"([a-z]+)\".*/\1/' <<<"${output}")"
            jq -r --arg t "${tier}" '[.status.replicaAdoptions[] | select(.tier == $t) | .adoptedReplicas | tostring] | join("")' "${WORLD}"
            ;;
          json)
            jq '{"metadata": {"generation": .generation, "uid": .uid}, "status": .status}' "${WORLD}"
            ;;
          *)
            die_stub "unsupported ravelcluster/dev jsonpath: ${output}"
            ;;
        esac
        ;;

      deployment/*)
        dep="${resource#deployment/}"
        # Only misreports during the step b Managed window (minReplicas 2):
        # the point of this knob is to simulate the handover itself reading
        # back the wrong value right where the real script checks it, not to
        # freeze every later read -- step d changes the bounds to 4/4, which
        # turns this knob back off so the rest of the script still runs
        # truthfully.
        if [[ -n "${STUB_FREEZE_REPLICAS_DEP:-}" && "${dep}" == "${STUB_FREEZE_REPLICAS_DEP}" && "${output}" == 'jsonpath:{.spec.replicas}' ]]; then
          tier_key="gateway"
          [[ "${dep}" == *query* ]] && tier_key="query"
          in_window="$(jq -r --arg t "${tier_key}" '(.cluster[$t].autoscaling.minReplicas // -1) == 2' "${WORLD}")"
          if [[ "${in_window}" == "true" ]]; then
            echo "${STUB_FREEZE_REPLICAS_VALUE:-1}"
            exit 0
          fi
        fi
        case "${output}" in
          'jsonpath:{.spec.replicas}')
            jq -r --arg d "${dep}" '.deployments[$d].replicas | tostring' "${WORLD}"
            ;;
          'jsonpath:{.status.readyReplicas}')
            jq -r --arg d "${dep}" '.deployments[$d].ready | tostring' "${WORLD}"
            ;;
          'jsonpath:{.spec.template.spec.containers[0].resources.requests.cpu}')
            # cpuRequest lives on the tier spec (.cluster[tier]), matching how
            # init_world seeds it; read it from there, mapping dev-gateway ->
            # gateway and dev-query -> query.
            tier_key="gateway"
            [[ "${dep}" == *query* ]] && tier_key="query"
            jq -r --arg t "${tier_key}" '.cluster[$t].cpuRequest // ""' "${WORLD}"
            ;;
          json)
            # The field managers owning spec.replicas on the Deployment.
            # Default: a single manager (the world's current one). The knobs
            # inject the two failure shapes assert_replicas_handed_over must
            # catch: ravel-operator co-owning the field beside the handover
            # manager, and no manager owning it at all.
            base_manager="$(jq -r --arg d "${dep}" '.deployments[$d].manager' "${WORLD}")"
            if [[ -n "${STUB_NO_REPLICAS_OWNER:-}" && "${dep}" == "${STUB_NO_REPLICAS_OWNER}" ]]; then
              # A managedFields entry that owns something other than
              # spec.replicas: the join of replicas owners is empty.
              jq -n '{"metadata":{"managedFields":[{"manager":"kubectl","fieldsV1":{"f:metadata":{"f:labels":{}}}}]}}'
            elif [[ -n "${STUB_KEEP_MANAGER:-}" && "${dep}" == "${STUB_KEEP_MANAGER}" ]]; then
              jq -n '{"metadata":{"managedFields":[{"manager":"ravel-operator","fieldsV1":{"f:spec":{"f:replicas":{}}}}]}}'
            elif [[ -n "${STUB_COOWN_MANAGER:-}" && "${dep}" == "${STUB_COOWN_MANAGER}" ]]; then
              jq -n --arg b "${base_manager}" '{"metadata":{"managedFields":[
                {"manager":$b,"fieldsV1":{"f:spec":{"f:replicas":{}}}},
                {"manager":"ravel-operator","fieldsV1":{"f:spec":{"f:replicas":{}}}}]}}'
            else
              jq -n --arg m "${base_manager}" \
                '{"metadata":{"managedFields":[{"manager":$m,"fieldsV1":{"f:spec":{"f:replicas":{}}}}]}}'
            fi
            ;;
          *)
            die_stub "unsupported deployment jsonpath: ${output}"
            ;;
        esac
        ;;

      hpa/*)
        name="${resource#hpa/}"
        exists="$(jq -r --arg n "${name}" 'if .hpas[$n] then "1" else "0" end' "${WORLD}")"
        if [[ "${output}" == "" ]]; then
          [[ "${exists}" == "1" ]] || exit 1
          exit 0
        fi
        [[ "${exists}" == "1" ]] || die_stub "hpa ${name} does not exist"
        case "${output}" in
          json)
            jq --arg n "${name}" '.hpas[$n] + {"uid": .uid}' "${WORLD}" |
              jq '{"metadata":{"labels": .labels, "ownerReferences": [{"uid": .uid}]}}'
            ;;
          *)
            die_stub "unsupported hpa jsonpath: ${output}"
            ;;
        esac
        ;;

      *)
        die_stub "unsupported get resource: ${resource}"
        ;;
    esac
    exit 0
    ;;

  patch)
    resource="${args[1]:-}"
    [[ "${resource}" == "ravelcluster/dev" ]] || die_stub "unsupported patch resource: ${resource}"
    patch_body=""
    for ((i=2; i<${#args[@]}; i++)); do
      if [[ "${args[$i]}" == "-p" ]]; then
        patch_body="${args[$((i+1))]:-}"
      fi
    done
    [[ -n "${patch_body}" ]] || die_stub "patch called with no -p body"
    # A reconcile runs ONLY when the merge patch actually changes the spec
    # (which bumps metadata.generation, as the apiserver would). An identical
    # patch is a no-op, matching the generation filter the operator's watch
    # applies.
    world_jq '(.cluster) as $before_cluster
      | .cluster.gateway = mergepatch(.cluster.gateway; ($patch.spec.gateway // {}))
      | .cluster.query = mergepatch(.cluster.query; ($patch.spec.query // {}))
      | if .cluster != $before_cluster then
          .generation += 1 | pass(true)
        else . end' --argjson patch "${patch_body}" || die_stub "patch/reconcile failed"
    exit 0
    ;;

  scale)
    resource="${args[1]:-}"
    dep="${resource#deployment/}"
    replicas=""
    for a in "${args[@]:2}"; do
      [[ "${a}" == --replicas=* ]] && replicas="${a#--replicas=}"
    done
    [[ -n "${replicas}" ]] || die_stub "scale called with no --replicas"
    tmp="${WORLD}.tmp"
    # The external scaler sets a Deployment's replicas directly. It does NOT
    # bump the RavelCluster's generation and does NOT run a reconcile; the
    # operator sees the new count only on the next generation-bumping pass.
    jq --arg d "${dep}" --argjson r "${replicas}" \
      '.deployments[$d].replicas = $r | .deployments[$d].ready = $r' "${WORLD}" >"${tmp}" \
      || die_stub "scale failed"
    mv "${tmp}" "${WORLD}"
    exit 0
    ;;

  annotate)
    resource="${args[1]:-}"
    [[ "${resource}" == "ravelcluster/dev" ]] || die_stub "unsupported annotate resource: ${resource}"
    # An annotation changes neither the spec nor metadata.generation, so it
    # triggers no reconcile (the operator's watch drops it). The real script no
    # longer annotates to force a pass; this handler stays only so the verb is
    # not an error if a caller uses it.
    exit 0
    ;;

  *)
    die_stub "unsupported verb: ${verb}"
    ;;
esac
STUB
chmod +x "${STUB_BIN}/kubectl"

init_world() {
  # ${N-default} (no colon) so an explicitly empty CPU arg (the no-CPU-request
  # case) is preserved rather than replaced by the default.
  local out="$1" gateway_cpu="${2-100m}" query_cpu="${3-200m}"
  jq -n --arg uid "${CLUSTER_UID}" --arg gcpu "${gateway_cpu}" --arg qcpu "${query_cpu}" '{
    uid: $uid,
    generation: 1,
    observedGeneration: 1,
    retryPending: false,
    conditions: {
      Available: {status: "True", observedGeneration: 1, reason: "MinimumReplicasAvailable"},
      StoreQualified: {status: "True", observedGeneration: 1, reason: "Succeeded"}
    },
    cluster: {
      gateway: {replicas: 1, autoscaling: null, cpuRequest: (if $gcpu == "" then null else $gcpu end)},
      query: {replicas: 1, autoscaling: null, cpuRequest: (if $qcpu == "" then null else $qcpu end)}
    },
    deployments: {
      "dev-gateway": {replicas: 1, ready: 1, manager: "ravel-operator"},
      "dev-query": {replicas: 1, ready: 1, manager: "ravel-operator"}
    },
    hpas: {},
    status: {autoscaledTiers: [], replicaAdoptions: []}
  }' >"${out}"
}

# run_case <dir-suffix> [extra env assignment ...] -- runs the real script
# (or, with RUN_SCRIPT_OVERRIDE set, a mutated copy of it) against a fresh
# world.
run_case() {
  local dir="${TMP}/$1"
  shift
  mkdir -p "${dir}"
  local world="${dir}/world.json"
  # Use ${VAR-default} (no colon) so a case that sets GATEWAY_CPU="" to drive
  # the no-CPU-request fallback keeps the empty string; ${VAR:-default} would
  # substitute the default for an empty value and never exercise that branch.
  init_world "${world}" "${GATEWAY_CPU-100m}" "${QUERY_CPU-200m}"
  local script="${RUN_SCRIPT_OVERRIDE:-${REAL_SCRIPT}}"
  # The remaining args are dynamic "VAR=val" tokens (per-case stub knobs);
  # bash only recognizes assignment-prefix syntax when it is written
  # literally in the source, not when it arrives through "$@" expansion, so
  # they are threaded through the external `env` instead.
  PATH="${STUB_BIN}:${PATH}" \
    STUB_WORLD_FILE="${world}" \
    STUB_JQ_LIB="${JQ_LIB}" \
    RAVEL_AUTOSCALING_WAIT_TIMEOUT_SECONDS="${RAVEL_AUTOSCALING_WAIT_TIMEOUT_SECONDS:-2}" \
    RAVEL_AUTOSCALING_WAIT_INTERVAL_SECONDS="${RAVEL_AUTOSCALING_WAIT_INTERVAL_SECONDS:-1}" \
    env "$@" \
    bash "${script}"
}

check() {
  local name="$1" want_rc="$2" want_sub="$3"
  shift 3
  local out rc=0
  out="$("$@" 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing %s\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- case 1: happy path --------------------------------------------------
# The default world gives the gateway a CPU request (100m), so this also
# exercises the "Managed mode can read it" branch (see case 1c for the pin).
check "happy path exits 0 against a well-behaved cluster" \
  0 "every ADR-4314 replicas-handover assertion held" \
  run_case happy-path

# --- case 1b: gateway starts with no CPU request (fallback branch) -------
GATEWAY_CPU="" check "gateway with no CPU request takes the fallback branch and still succeeds" \
  0 "gateway has no CPU request; setting one" \
  run_case no-cpu-request
unset GATEWAY_CPU

# --- case 1c: gateway with a CPU request takes the Managed-can-read branch
# Pins item 5's fix: the stub reads cpuRequest from the tier spec, so the
# default (100m) world drives the "already has a CPU request" branch rather
# than the fallback.
check "gateway with a CPU request takes the Managed-can-read-it branch" \
  0 "gateway already has a CPU request (100m)" \
  run_case cpu-request-present

# --- case 2: replicas reset to 1 after the handover must fail, naming it -
check "a replica count of 1 after the handover fails, naming the field" \
  1 "gateway Deployment spec.replicas after forced reconcile" \
  run_case replicas-reset-to-one \
  STUB_FREEZE_REPLICAS_DEP=dev-gateway STUB_FREEZE_REPLICAS_VALUE=1

# --- case 3: an empty read must fail with "could not read", not pass -----
check "an empty read fails with 'could not read', not a silent pass" \
  1 "could not read" \
  run_case empty-read \
  STUB_EMPTY_UID=1

# --- case 4: spec.replicas still solely owned by ravel-operator must fail -
check "spec.replicas still owned by ravel-operator after enabling autoscaling fails" \
  1 "ravel-operator must not own spec.replicas" \
  run_case manager-not-handed-over \
  STUB_KEEP_MANAGER=dev-gateway

# --- case 4b: ravel-operator CO-OWNING spec.replicas must fail (item 2) ---
# The old ownership check joined the owners and failed only on an exact match
# with "ravel-operator"; a co-owning operator ("ravel-operator-handover,
# ravel-operator") slipped past it. assert_replicas_handed_over must reject
# ravel-operator appearing anywhere in the owner list.
check "ravel-operator co-owning spec.replicas after the handover fails" \
  1 "ravel-operator must not own spec.replicas" \
  run_case manager-co-owned \
  STUB_COOWN_MANAGER=dev-gateway

# --- case 4c: NO owner of spec.replicas must fail (item 2) ----------------
# After the handover the field must have an owner; an empty owner list means
# the server-side apply handover never took, and must fail rather than pass.
check "no field manager owning spec.replicas after the handover fails" \
  1 "no field manager owns spec.replicas" \
  run_case manager-no-owner \
  STUB_NO_REPLICAS_OWNER=dev-gateway

# --- case 5a: External tier reset to the declared count must fail step c --
# The operator model resets the External query tier to its declared
# spec.replicas (1) on a pass; the real script forces a pass and then reads,
# so it must catch the reset.
check "an External tier reset to its declared count fails step c" \
  1 "query Deployment spec.replicas after forced reconcile" \
  run_case external-resets \
  STUB_EXTERNAL_RESETS=1

# --- case 5b: turn-off that applies the wrong count must fail step d ------
# The operator records adoptedReplicas 4 but applies 3 to the Deployment. The
# read right after the turn-off pass (before any re-apply) must catch it.
check "a turn-off that records 4 but applies 3 fails step d" \
  1 "gateway Deployment spec.replicas after turn-off (adopted count)" \
  run_case turnoff-applies-wrong \
  STUB_TURNOFF_APPLIES=3

# --- case 5c: operator that never advances observedGeneration times out ---
# force_reconcile bumps the generation but the operator never advances
# observedGeneration, so wait_reconcile_complete must time out naming the
# generation it awaited.
check "an operator that never advances observedGeneration times out naming the generation" \
  1 "waiting for the operator to complete a successful reconcile pass at generation" \
  run_case no-observed-generation \
  STUB_NO_OBSERVED_GEN=1

# --- case 5d: a pass that stamps the generation but fails must not count ---
# Once the query tier is External, every pass applies the gateway and then
# fails before the query tier: the operator's error path writes
# observedGeneration at the new generation with Available=False and
# Degraded=True. The first forced pass of step c must time out naming the
# generation and the last Available/Degraded status and reason. (The pre-fix
# script, which waited on status.observedGeneration alone, passed this case.)
check "a query pass that is always Degraded once External fails step c, naming the conditions" \
  1 "complete a successful reconcile pass at generation 7: last seen Available=False (reason ReconcileError, generation 7), StoreQualified=True (reason Succeeded, generation 7), Degraded=True (reason ReconcileError, generation 7)" \
  run_case degraded-always \
  STUB_DEGRADED_TIER=query

# --- case 5e: a Degraded pass the operator's retry then completes passes ---
# The first pass at every generation with the query tier External is
# Degraded; the requeued retry succeeds. The wait must keep polling through
# the Degraded snapshot rather than fail a correct operator on a transient
# error.
check "a Degraded first pass at a generation followed by a successful retry passes" \
  0 "every ADR-4314 replicas-handover assertion held" \
  run_case degraded-once \
  STUB_DEGRADED_TIER=query STUB_DEGRADED_MODE=once
# Non-vacuity: every generation with the query tier External (6: the External
# patch; 7: step c's forced pass; 8 and 9: step d's bounds and turn-off; 10
# and 11: step d's two forced passes) had a Degraded first pass. Each was
# retried exactly once, except 8: after that patch the script reads only the
# gateway Deployment, so no RavelCluster read lands the retry before the
# turn-off patch starts generation 9. Step e1 removes the query block, so later
# generations have none.
want_retries="$(printf 'retry generation=%s prior Available=False\n' 6 7 9 10 11)"
got_retries="$(cat "${TMP}/degraded-once/world.json.retries" 2>/dev/null || true)"
if [[ "${got_retries}" == "${want_retries}" ]]; then
  printf 'ok    case 5e retried a Degraded pass once at each of generations 6, 7, 9, 10 and 11\n'
  passes=$((passes + 1))
else
  printf 'FAIL  case 5e retries: wanted\n%s\n      got\n%s\n' "${want_retries}" "${got_retries:-<none>}"
  fails=$((fails + 1))
fi

# --- case 5f: a qualification hold must not count as a completed pass -----
# The hold stamps the generation with Available=True (prior ready replicas)
# but applies no tier; StoreQualified=False at the generation must keep the
# wait from accepting it.
check "a qualification hold (Available=True, StoreQualified=False) fails step c" \
  1 "StoreQualified=False (reason Pending, generation 7)" \
  run_case qualify-hold \
  STUB_QUALIFY_HOLD=1

# --- case 5g: ravel-operator co-owning the External query tier's replicas --
check "ravel-operator co-owning the query tier's spec.replicas after the forced pass fails step c" \
  1 "query Deployment spec.replicas owners after forced reconcile: ravel-operator must not own spec.replicas" \
  run_case query-co-owned \
  STUB_COOWN_MANAGER=dev-query

# --- case 5h: a forced External pass that drops the autoscaledTiers entry --
check "an External pass that drops query from status.autoscaledTiers fails step c" \
  1 "could not read status.autoscaledTiers lists query after forced reconcile" \
  run_case external-entry-dropped \
  STUB_DROP_EXTERNAL_ENTRY=1

# --- case 5i: turn-off that leaves spec.replicas with the handover manager -
check "a turn-off that does not take spec.replicas back fails step d" \
  1 "gateway Deployment spec.replicas owners after turn-off: ravel-operator must own spec.replicas after turn-off (owners: ravel-operator-handover)" \
  run_case turnoff-keeps-handover \
  STUB_TURNOFF_KEEPS_HANDOVER=1

# --- case 5j: step e's query count must come from the operator ------------
# The operator adopts the live count on turn-off even when the same edit
# changed spec.replicas. Step e1 declares 2 while the live count is 3, so the
# read after the completed pass must see 3 and fail. (The pre-fix step e
# scaled the Deployment to 1 itself first, so its own scale met its waits.)
check "a turn-off that ignores a same-edit replicas change fails step e1" \
  1 "query Deployment spec.replicas after step e1 (same-edit declared value): expected '2', got '3'" \
  run_case turnoff-ignores-same-edit \
  STUB_TURNOFF_IGNORES_SAME_EDIT=1

# --- case 5k: step e must leave no replicaAdoptions entry -----------------
# The operator applies a changed declared value but never drops the adoption:
# the gateway's entry from step d survives the restore.
check "an adoption left in status.replicaAdoptions after the restore fails step e" \
  1 "status.replicaAdoptions entry count after restore: expected '0', got '1'" \
  run_case adoption-kept \
  STUB_KEEP_ADOPTIONS=1

# --- distinguishing test C: observedGeneration-only wait must wrongly pass -
# Mutates wait_reconcile_complete's predicate so it accepts any Available and
# StoreQualified status as long as their observedGeneration matches (what the
# pre-fix status.observedGeneration wait amounted to), then runs case 5d's
# always-Degraded operator: the real script fails, the mutant reports success.
MUTANT_C="${TMP}/mutant-observed-generation-only.sh"
sed -E -e 's/"\$\{a_status\}" == "True"/-n "${a_status}"/' \
  -e 's/"\$\{q_status\}" == "True"/-n "${q_status}"/' \
  "${REAL_SCRIPT}" >"${MUTANT_C}"
chmod +x "${MUTANT_C}"
if ! diff -q "${REAL_SCRIPT}" "${MUTANT_C}" >/dev/null; then
  printf 'ok    mutant C (observedGeneration-only wait) differs textually from the real script\n'
  passes=$((passes + 1))
else
  printf 'FAIL  mutant C: sed produced no change -- the pattern no longer matches wait_reconcile_complete\n'
  fails=$((fails + 1))
fi

RUN_SCRIPT_OVERRIDE="${MUTANT_C}" \
  check "distinguishing test C: the observedGeneration-only mutant wrongly PASSES an always-Degraded query tier" \
  0 "every ADR-4314 replicas-handover assertion held" \
  run_case distinguishing-c-mutant \
  STUB_DEGRADED_TIER=query
unset RUN_SCRIPT_OVERRIDE

# --- distinguishing test A: substring-match mutant must wrongly pass -----
# The "operator did not change this" replica assertions go through assert_eq
# (a single read after a proven-complete pass, not a poll), so this mutates
# assert_eq's own exact-equality test into a substring check, then proves it on
# a fixture where the live replica count reads back as 13 but 3 was expected
# ("13" contains "3"): the real script must fail that case, and the mutant must
# wrongly accept "13" as a match for "3" and report success.
MUTANT_A="${TMP}/mutant-substring-match.sh"
sed -E 's/\[\[ "\$\{actual\}" != "\$\{expected\}" \]\]/[[ "${actual}" != *"${expected}"* ]]/' \
  "${REAL_SCRIPT}" >"${MUTANT_A}"
chmod +x "${MUTANT_A}"
if ! diff -q "${REAL_SCRIPT}" "${MUTANT_A}" >/dev/null; then
  printf 'ok    mutant A (substring match) differs textually from the real script\n'
  passes=$((passes + 1))
else
  printf 'FAIL  mutant A: sed produced no change -- the pattern no longer matches assert_eq\n'
  fails=$((fails + 1))
fi

check "distinguishing test A: the real script fails a 13-for-3 mismatch, naming the value" \
  1 "got '13'" \
  run_case distinguishing-a-real \
  STUB_FREEZE_REPLICAS_DEP=dev-gateway STUB_FREEZE_REPLICAS_VALUE=13

RUN_SCRIPT_OVERRIDE="${MUTANT_A}" \
  check "distinguishing test A: the substring-match mutant wrongly PASSES the same 13-for-3 mismatch" \
  0 "every ADR-4314 replicas-handover assertion held" \
  run_case distinguishing-a-mutant \
  STUB_FREEZE_REPLICAS_DEP=dev-gateway STUB_FREEZE_REPLICAS_VALUE=13
unset RUN_SCRIPT_OVERRIDE

# --- distinguishing test B: empty-read-as-pass mutant must wrongly pass --
# Mutates read_now (the uid read is its first call) by deleting its emptiness
# check, then proves it on a fixture that returns an empty uid: the real script
# must fail that case with "could not read", and the mutant must NOT die there
# (it fails later, once the empty uid flows into the CLUSTER_UID compared to the
# HPA ownerReference uid -- but it must not die with read_now's own "could not
# read" message at the uid read).
MUTANT_B="${TMP}/mutant-empty-as-pass.sh"
awk '
  /^read_now\(\) \{/ { in_fn = 1 }
  in_fn && /if \[\[ -z "\$\{val\}" \]\]; then/ { skipping = 1 }
  in_fn && skipping && /^  fi$/ { skipping = 0; next }
  skipping { next }
  { print }
' "${REAL_SCRIPT}" >"${MUTANT_B}"
chmod +x "${MUTANT_B}"
if ! diff -q "${REAL_SCRIPT}" "${MUTANT_B}" >/dev/null; then
  printf 'ok    mutant B (empty-read-as-pass) differs textually from the real script\n'
  passes=$((passes + 1))
else
  printf 'FAIL  mutant B: awk produced no change -- read_now no longer matches the expected shape\n'
  fails=$((fails + 1))
fi

check "distinguishing test B: the real script fails an empty uid read with 'could not read'" \
  1 "could not read" \
  run_case distinguishing-b-real \
  STUB_EMPTY_UID=1

RUN_SCRIPT_OVERRIDE="${MUTANT_B}" \
  check "distinguishing test B: the empty-read-as-pass mutant does not die with 'could not read'" \
  1 "" \
  run_case distinguishing-b-mutant \
  STUB_EMPTY_UID=1
unset RUN_SCRIPT_OVERRIDE
# The mutant case above asserts only that the exit is nonzero (it still fails
# later, comparing an empty uid to a real one) -- the distinguishing point is
# proven separately: its output must NOT contain "could not read".
mutant_b_out="$(RUN_SCRIPT_OVERRIDE="${MUTANT_B}" run_case distinguishing-b-mutant-check STUB_EMPTY_UID=1 2>&1)" || true
if [[ "${mutant_b_out}" != *"could not read"* ]]; then
  printf 'ok    distinguishing test B: mutant output never says could not read\n'
  passes=$((passes + 1))
else
  printf 'FAIL  distinguishing test B: mutant output unexpectedly says could not read\n'
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
