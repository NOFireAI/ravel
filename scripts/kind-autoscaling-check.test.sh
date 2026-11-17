#!/usr/bin/env bash
# Cases for kind-autoscaling-check.sh (issue #4324), in the pattern of
# scripts/guards/check-quick-xml-shipped-reachability.test.sh: run the real
# script against a stub `kubectl` on PATH instead of a real apiserver.
#
# The stub keeps a small stateful "world" (a JSON file: the RavelCluster
# spec, each Deployment's live replicas/field-manager, and each HPA) and
# reconciles it on every mutating call (patch/scale/annotate), applying a
# simplified version of ADR-4314's handover rules (docs/adrs/4314-operator-
# autoscaling.md, decision 2 and its "declared-count" and "refuse to take
# over a user HPA" amendments). It does not simulate real apiserver timing:
# every reconcile converges immediately, because this suite exists to prove
# the real script's assertions are correct and non-vacuous given eventually-
# consistent reads, not to re-prove the operator's own reconcile loop (that
# is services/ravel-operator/src/autoscaling.rs's own test suite's job).
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
  (.status.replicaAdoptions | map(select(.tier == tierKey)) | first) as $adoption
  | (.status.autoscaledTiers | map(select(.tier == tierKey)) | first) as $declaredEntry
  | .cluster[tierKey] as $spec
  | .deployments[depName] as $dep
  | ($spec.autoscaling) as $asc
  | if $asc == null then
      (if $adoption != null then
         (if $spec.replicas == $adoption.declaredReplicas then
            .
          else
            .deployments[depName].replicas = $spec.replicas
            | .deployments[depName].manager = "ravel-operator"
            | .status.replicaAdoptions = (.status.replicaAdoptions | map(select(.tier != tierKey)))
          end)
       elif $dep.manager == "ravel-operator-handover" then
         # Just turned off. ADR-4314's declared-count amendment: compare the
         # NEW declared value (this patch's spec.replicas) against the
         # declaredReplicas the autoscaledTiers entry held the moment
         # before removal, not against the live count. A same-edit change
         # of the declared value forces it immediately; otherwise the live
         # count (wherever the HPA or external scaler left it) is adopted
         # and held until spec.replicas actually changes.
         (if ($declaredEntry == null) or ($spec.replicas != $declaredEntry.declaredReplicas) then
            .deployments[depName].replicas = $spec.replicas
            | .deployments[depName].manager = "ravel-operator"
          else
            .status.replicaAdoptions = (.status.replicaAdoptions + [{"tier": tierKey, "adoptedReplicas": $dep.replicas, "declaredReplicas": $spec.replicas}])
            | .deployments[depName].manager = "ravel-operator"
          end)
       else
         .deployments[depName].replicas = $spec.replicas
         | .deployments[depName].manager = "ravel-operator"
       end)
      | .status.autoscaledTiers = (.status.autoscaledTiers | map(select(.tier != tierKey)))
      | .hpas = (.hpas | with_entries(select(.key != depName)))
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
           .hpas = (.hpas | with_entries(select(.key != depName)))
         end)
    end
    # Pods become ready instantly in this model: there is no real kubelet to
    # wait on, and the real script's readiness waits exist for a genuine
    # apiserver, not for this stub.
    | .deployments[depName].ready = .deployments[depName].replicas;

def reconcile:
  reconcile_tier("gateway"; "dev-gateway") | reconcile_tier("query"; "dev-query");
JQ

cat >"${STUB_BIN}/kubectl" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail

WORLD="${STUB_WORLD_FILE:?STUB_WORLD_FILE must be set}"
LIB="${STUB_JQ_LIB:?STUB_JQ_LIB must be set}"

die_stub() { echo "stub kubectl: $*" >&2; exit 1; }

# Strips a leading "-n"/"--namespace" (and its value) and a trailing
# ">" null-ns ravel-system assumption: the real script always passes one, so
# the stub does not bother keeping it, it only needs to ignore it.
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
    for a in "${args[@]:2}"; do
      case "${a}" in
        -o) ;;
        jsonpath=*) output="jsonpath:${a#jsonpath=}" ;;
        -o=*) ;;
        json) output="json" ;;
      esac
    done
    # kubectl's own `-o jsonpath=...` arrives as one token (-o then the
    # value), and `-o json` the same way; scan positionally instead.
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
        case "${output}" in
          'jsonpath:{.metadata.uid}')
            jq -r '.uid' "${WORLD}"
            ;;
          jsonpath:'{.status.autoscaledTiers[?(@.tier=="'*'")].tier}')
            tier="$(sed -E 's/.*@\.tier==\"([a-z]+)\".*/\1/' <<<"${output}")"
            jq -r --arg t "${tier}" '[.status.autoscaledTiers[] | select(.tier == $t) | .tier] | join("")' "${WORLD}"
            ;;
          jsonpath:'{.status.replicaAdoptions[?(@.tier=="'*'")].adoptedReplicas}')
            tier="$(sed -E 's/.*@\.tier==\"([a-z]+)\".*/\1/' <<<"${output}")"
            jq -r --arg t "${tier}" '[.status.replicaAdoptions[] | select(.tier == $t) | .adoptedReplicas | tostring] | join("")' "${WORLD}"
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
        # back the wrong value right where the real script checks it, not
        # to freeze every later read -- step d changes the bounds to 4/4,
        # which is what turns this knob back off so the rest of the script
        # still runs truthfully.
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
            jq -r --arg d "${dep}" '.deployments[$d].cpuRequest // ""' "${WORLD}"
            ;;
          json)
            if [[ -n "${STUB_KEEP_MANAGER:-}" && "${dep}" == "${STUB_KEEP_MANAGER}" ]]; then
              manager="ravel-operator"
            else
              manager="$(jq -r --arg d "${dep}" '.deployments[$d].manager' "${WORLD}")"
            fi
            jq -n --arg m "${manager}" \
              '{"metadata":{"managedFields":[{"manager":$m,"fieldsV1":{"f:spec":{"f:replicas":{}}}}]}}'
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
    tmp="${WORLD}.tmp"
    # The library text is spliced directly into the filter rather than
    # loaded with `-L`/`include`: jq 1.6's include mechanism silently drops
    # the trailing `.ready = .replicas` mutation at the end of
    # reconcile_tier (confirmed by diffing identical filter text inlined
    # vs. included -- inlined reports the correct post-reconcile ready
    # count, included reports the pre-reconcile one every time), so a
    # library consumed through include cannot be trusted to apply its own
    # last pipe stage.
    jq --argjson patch "${patch_body}" "$(cat "${LIB}")
      .cluster.gateway = mergepatch(.cluster.gateway; (\$patch.spec.gateway // {}))
      | .cluster.query = mergepatch(.cluster.query; (\$patch.spec.query // {}))
      | reconcile" "${WORLD}" >"${tmp}" || die_stub "patch/reconcile failed"
    mv "${tmp}" "${WORLD}"
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
    jq --arg d "${dep}" --argjson r "${replicas}" "$(cat "${LIB}")
      .deployments[\$d].replicas = \$r | reconcile" "${WORLD}" >"${tmp}" || die_stub "scale/reconcile failed"
    mv "${tmp}" "${WORLD}"
    exit 0
    ;;

  annotate)
    resource="${args[1]:-}"
    [[ "${resource}" == "ravelcluster/dev" ]] || die_stub "unsupported annotate resource: ${resource}"
    # Annotating is how the real script forces a reconcile pass; re-running
    # reconcile (idempotently) is exactly what that models here. A removal
    # (the trailing-dash form used only during cleanup) needs no reconcile.
    for a in "${args[@]:2}"; do
      [[ "${a}" == *- ]] && exit 0
    done
    tmp="${WORLD}.tmp"
    jq "$(cat "${LIB}")
      reconcile" "${WORLD}" >"${tmp}" || die_stub "annotate/reconcile failed"
    mv "${tmp}" "${WORLD}"
    exit 0
    ;;

  *)
    die_stub "unsupported verb: ${verb}"
    ;;
esac
STUB
chmod +x "${STUB_BIN}/kubectl"

init_world() {
  local out="$1" gateway_cpu="${2:-100m}" query_cpu="${3:-200m}"
  jq -n --arg uid "${CLUSTER_UID}" --arg gcpu "${gateway_cpu}" --arg qcpu "${query_cpu}" '{
    uid: $uid,
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
  init_world "${world}" "${GATEWAY_CPU:-100m}" "${QUERY_CPU:-200m}"
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
check "happy path exits 0 against a well-behaved cluster" \
  0 "every ADR-4314 replicas-handover assertion held" \
  run_case happy-path

# --- case 1b: gateway starts with no CPU request (fallback branch) -------
GATEWAY_CPU="" check "gateway with no CPU request still succeeds (fallback sets one)" \
  0 "gateway has no CPU request; setting one" \
  run_case no-cpu-request
unset GATEWAY_CPU

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

# --- case 4: spec.replicas still owned by ravel-operator must fail -------
check "spec.replicas still owned by ravel-operator after enabling autoscaling fails" \
  1 "must not be 'ravel-operator'" \
  run_case manager-not-handed-over \
  STUB_KEEP_MANAGER=dev-gateway

# --- case 5: a wait that never converges fails within the stub timeout ---
check "a wait that never converges fails within the stubbed timeout" \
  1 "timed out after" \
  run_case never-converges \
  STUB_FREEZE_REPLICAS_DEP=dev-gateway STUB_FREEZE_REPLICAS_VALUE=999

# --- distinguishing test A: substring-match mutant must wrongly pass -----
# Every replica-count check in the real script goes through
# wait_field_equals, not assert_eq (replica counts are polled, not read
# once), so this mutates wait_field_equals's own exact-equality test into a
# substring check, then proves it on a fixture where the live replica
# count is 13 but 3 was expected ("13" contains "3"): the real script must
# fail that case (it is never satisfied, so it runs out its wait and times
# out), and the mutant must wrongly accept "13" as a match for "3" and
# report success.
MUTANT_A="${TMP}/mutant-substring-match.sh"
sed -E 's/\[\[ "\$\{val\}" == "\$\{expected\}" \]\]/[[ "${val}" == *"${expected}"* ]]/' \
  "${REAL_SCRIPT}" >"${MUTANT_A}"
chmod +x "${MUTANT_A}"
if ! diff -q "${REAL_SCRIPT}" "${MUTANT_A}" >/dev/null; then
  printf 'ok    mutant A (substring match) differs textually from the real script\n'
  passes=$((passes + 1))
else
  printf 'FAIL  mutant A: sed produced no change -- the pattern no longer matches wait_field_equals\n'
  fails=$((fails + 1))
fi

check "distinguishing test A: the real script fails a 13-for-3 mismatch, naming the value" \
  1 "last value seen '13'" \
  run_case distinguishing-a-real \
  STUB_FREEZE_REPLICAS_DEP=dev-gateway STUB_FREEZE_REPLICAS_VALUE=13

RUN_SCRIPT_OVERRIDE="${MUTANT_A}" \
  check "distinguishing test A: the substring-match mutant wrongly PASSES the same 13-for-3 mismatch" \
  0 "every ADR-4314 replicas-handover assertion held" \
  run_case distinguishing-a-mutant \
  STUB_FREEZE_REPLICAS_DEP=dev-gateway STUB_FREEZE_REPLICAS_VALUE=13
unset RUN_SCRIPT_OVERRIDE

# --- distinguishing test B: empty-read-as-pass mutant must wrongly pass --
# Mutates read_now (the uid read is its first call) by deleting its
# emptiness check, then proves it on a fixture that returns an empty uid:
# the real script must fail that case, and the mutant must not die there
# (it will fail further on, once the empty uid flows into `cluster_uid`
# compared to the HPA ownerReference uid -- but it still must not die with
# the real script's own "could not read" message at that point).
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
# The mutant case above asserts only that the exit is nonzero (it still
# fails later, comparing an empty uid to a real one) -- the distinguishing
# point is proven separately: its output must NOT contain "could not read".
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
