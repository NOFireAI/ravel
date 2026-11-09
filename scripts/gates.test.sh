#!/usr/bin/env bash
# Cases for the scoped selection in scripts/gates.sh and scripts/affected-tests.sh
# (issue #2877): scoped mode gates the reverse-dependency closure of the named
# crates, and the sql, agents, mcp, flight-sql and esql lanes follow that
# closure. The SDK gate runs in workspace mode only.
#
# Run by the doc-scripts job in .github/workflows/ci.yml, and by hand with
#   bash scripts/gates.test.sh
#
# No toolchain: a stub `cargo` first on PATH answers `cargo metadata` with a
# synthetic workspace whose edges mirror the real ones these cases are about
# (ravel-sql and ravel-server depend on ravel-query and ravel-esql, ravel-server
# also on the agents crates, ravel-agents-sim on ravel-agents-runtime, ravel-cli
# on ravel-sql,
# ravel-query, ravel-logseg, ravel-ingest and, as a dev-dependency,
# ravel-server, ravel-ingest on ravel-logseg, ravel-ingest-router on
# ravel-affinity), logs every other call
# and builds nothing. gates.sh runs with --dry-run, which prints each cargo
# command a real run would execute without running it.
set -uo pipefail

SCRIPTS="$(cd "$(dirname "$0")" && pwd)"
pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

# count <file> <fixed string>: lines of <file> equal to the string.
count() { grep -cxF -- "$2" "$1"; }
# count_matching <file> <regex>: lines of <file> matching the regex.
count_matching() { grep -cE -- "$2" "$1"; }

if ! command -v jq >/dev/null 2>&1; then
  echo "FAIL  jq is not installed; affected-tests.sh needs it to read cargo metadata" >&2
  exit 1
fi

tmp="$(mktemp -d "${TMPDIR:-/tmp}/gates-test.XXXXXX")"
trap 'rm -rf "${tmp}"' EXIT
mkdir -p "${tmp}/bin"

# pkg <name> <dep...>: one package object for the synthetic metadata.
pkg() {
  local name="$1" deps="" d
  shift
  for d in "$@"; do
    deps="${deps:+${deps},}{\"name\":\"${d}\"}"
  done
  printf '{"name":"%s","dependencies":[%s]}' "${name}" "${deps}"
}
{
  printf '{"packages":['
  pkg ravel-types
  printf ','
  pkg ravel-query ravel-types
  printf ','
  pkg ravel-esql ravel-types
  printf ','
  pkg ravel-sql ravel-query ravel-esql
  printf ','
  pkg ravel-agents-core ravel-types
  printf ','
  pkg ravel-agents-store ravel-agents-core
  printf ','
  pkg ravel-agents-runtime ravel-agents-core ravel-agents-store
  printf ','
  pkg ravel-agents-sim ravel-agents-core ravel-agents-store ravel-agents-runtime
  printf ','
  pkg ravel-server ravel-sql ravel-query ravel-esql ravel-agents-core ravel-agents-store ravel-agents-runtime
  printf ','
  pkg ravel-logseg ravel-types
  printf ','
  pkg ravel-ingest ravel-logseg ravel-types
  printf ','
  pkg ravel-bench ravel-server ravel-ingest
  printf ','
  pkg ravel-cli ravel-sql ravel-query ravel-logseg ravel-ingest ravel-server
  printf ','
  pkg ravel-affinity
  printf ','
  pkg ravel-ingest-router ravel-affinity
  printf ']}\n'
} >"${tmp}/metadata.json"

# The stub cargo. CARGO_STUB_FAIL_ON, when set, is a fixed string: a call
# whose arguments contain it exits 3, so the exit-code case can fail one run.
# A call made with RAVEL_AGENTS_PAGE_ENTRIES set logs it after the arguments.
# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' \
  'if [[ "$1" == "metadata" ]]; then echo "cargo $*" >>"${CARGO_STUB_LOG}"; cat "${CARGO_STUB_METADATA}"; exit 0; fi' \
  'echo "cargo $*${RAVEL_AGENTS_PAGE_ENTRIES:+ [RAVEL_AGENTS_PAGE_ENTRIES=${RAVEL_AGENTS_PAGE_ENTRIES}]}" >>"${CARGO_STUB_LOG}"' \
  'if [[ -n "${CARGO_STUB_FAIL_ON:-}" && "$*" == *"${CARGO_STUB_FAIL_ON}"* ]]; then exit 3; fi' \
  'exit 0' >"${tmp}/bin/cargo"
printf '#!/usr/bin/env bash\nexit 0\n' >"${tmp}/bin/cargo-nextest"
chmod +x "${tmp}/bin/cargo" "${tmp}/bin/cargo-nextest"

# run_stubbed <name> <command...>: run with the stub cargo first on PATH; the
# command's output lands in ${tmp}/<name>.out, the stub's call log in
# ${tmp}/<name>.calls, and the exit code in rc.
run_stubbed() {
  local name="$1"
  shift
  : >"${tmp}/${name}.calls"
  rc=0
  PATH="${tmp}/bin:${PATH}" CARGO_STUB_LOG="${tmp}/${name}.calls" \
    CARGO_STUB_METADATA="${tmp}/metadata.json" "$@" >"${tmp}/${name}.out" 2>&1 || rc=$?
}

# Every non-metadata cargo call the stub saw. A dry run must make none.
builds() { grep -vc '^cargo metadata ' "$1"; }

sql_clippy='==> cargo clippy --locked -p ravel-server --features sql --all-targets -- -D warnings'
sql_test='==> cargo nextest run --locked -p ravel-server --features sql'
flight_clippy='==> cargo clippy --locked -p ravel-server -p ravel-sql --features flight-sql --all-targets -- -D warnings'
flight_test='==> cargo nextest run --locked -p ravel-server -p ravel-sql --features flight-sql'
esql_clippy='==> cargo clippy --locked -p ravel-server --features esql --all-targets -- -D warnings'
esql_test='==> cargo nextest run --locked -p ravel-server --features esql'
stage_clippy='==> cargo clippy --locked -p ravel-logseg -p ravel-ingest --features stage-timing --all-targets -- -D warnings'
cli_explain_clippy='==> cargo clippy --locked -p ravel-cli --features sql-explain --all-targets -- -D warnings'
cli_explain_test='==> cargo nextest run --locked -p ravel-cli --features sql-explain'
ingest_shipping_clippy='==> cargo clippy --locked -p ravel-ingest --all-targets -- -D warnings'
agents_clippy='==> cargo clippy --locked -p ravel-server --features agents --all-targets -- -D warnings'
# The agents and mcp lanes select tests by binary: a test-name filter alone
# (the *_name_filter forms) never runs a test whose path omits the keyword.
agents_test='==> cargo nextest run --locked -p ravel-server --features agents --test agents_seam --test agents_effects_e2e --test agents_e2e --test agents_timers_e2e --test agents_projection_e2e --test agents_real_store'
agents_lib_test='==> cargo nextest run --locked -p ravel-server --features agents --lib'
agents_name_filter='==> cargo nextest run --locked -p ravel-server --features agents agents'
agents_classification='==> cargo nextest run --locked -p ravel-catalog --test sys_proto_format_version_classification'
agents_small_pages='==> env RAVEL_AGENTS_PAGE_ENTRIES=2 cargo nextest run --locked -p ravel-agents-runtime -p ravel-agents-sim'
mcp_clippy='==> cargo clippy --locked -p ravel-server --features mcp --all-targets -- -D warnings'
mcp_test='==> cargo nextest run --locked -p ravel-server --features mcp --test mcp_e2e --test trace_by_id --test mcp_conformance --test mcp_escape_routes --test mcp_audit --test mcp_adversarial_isolation --test mcp_adversarial_replay --test mcp_adversarial_hostile --test mcp_adversarial_precision --test mcp_adversarial_paging'
mcp_lib_test='==> cargo nextest run --locked -p ravel-server --features mcp --lib'
mcp_agents_clippy='==> cargo clippy --locked -p ravel-server --features mcp,agents --all-targets -- -D warnings'
mcp_agents_test='==> cargo nextest run --locked -p ravel-server --features mcp,agents --test mcp_agents_e2e'
mcp_name_filter='==> cargo nextest run --locked -p ravel-server --features mcp mcp'
sdk_gate_re='^==> .*/gates-sdk\.sh$'
sdk_skip='==> SDK gate skipped (RAVEL_GATES_SKIP_SDK=1): CI'"'"'s sdk job is the gate for sdk/typescript'

# --- gates.sh --dry-run -------------------------------------------------

# -p ravel-query: ravel-sql and ravel-server depend on it, so both lanes run,
# and the scoped clippy covers them too.
run_stubbed query bash "${SCRIPTS}/gates.sh" -p ravel-query --dry-run
out="${tmp}/query.out"
check_eq "gates -p ravel-query: exit 0" "0" "${rc}"
check_eq "gates -p ravel-query: sql lane clippy" "1" "$(count "${out}" "${sql_clippy}")"
check_eq "gates -p ravel-query: sql lane tests" "1" "$(count "${out}" "${sql_test}")"
check_eq "gates -p ravel-query: flight-sql lane clippy" "1" "$(count "${out}" "${flight_clippy}")"
check_eq "gates -p ravel-query: flight-sql lane tests" "1" "$(count "${out}" "${flight_test}")"
check_eq "gates -p ravel-query: esql lane clippy" "1" "$(count "${out}" "${esql_clippy}")"
check_eq "gates -p ravel-query: esql lane tests" "1" "$(count "${out}" "${esql_test}")"
check_eq "gates -p ravel-query: agents lane clippy" "1" "$(count "${out}" "${agents_clippy}")"
check_eq "gates -p ravel-query: agents lane seam and effects binaries" "1" "$(count "${out}" "${agents_test}")"
check_eq "gates -p ravel-query: agents lane lib tests" "1" "$(count "${out}" "${agents_lib_test}")"
check_eq "gates -p ravel-query: agents lane has no bare name filter" "0" \
  "$(count "${out}" "${agents_name_filter}")"
check_eq "gates -p ravel-query: agents lane classification test" "1" \
  "$(count "${out}" "${agents_classification}")"
check_eq "gates -p ravel-query: no page-size-2 run (no agents crate in the closure)" "0" \
  "$(count_matching "${out}" 'RAVEL_AGENTS_PAGE_ENTRIES')"
check_eq "gates -p ravel-query: mcp lane clippy" "1" "$(count "${out}" "${mcp_clippy}")"
check_eq "gates -p ravel-query: mcp lane binaries" "1" "$(count "${out}" "${mcp_test}")"
check_eq "gates -p ravel-query: mcp lane lib tests" "1" "$(count "${out}" "${mcp_lib_test}")"
check_eq "gates -p ravel-query: mcp lane agents clippy" "1" "$(count "${out}" "${mcp_agents_clippy}")"
check_eq "gates -p ravel-query: mcp lane agents binary" "1" "$(count "${out}" "${mcp_agents_test}")"
check_eq "gates -p ravel-query: mcp lane has no bare name filter" "0" \
  "$(count "${out}" "${mcp_name_filter}")"
check_eq "gates -p ravel-query: no SDK gate in scoped mode" "0" \
  "$(count_matching "${out}" "${sdk_gate_re}")"
check_eq "gates -p ravel-query: scoped clippy covers ravel-sql and ravel-server" "1" \
  "$(count_matching "${out}" '^==> cargo clippy --locked (-p [a-z-]+ )*-p ravel-sql (-p [a-z-]+ )*--all-targets' )"
check_eq "gates -p ravel-query: scoped clippy names ravel-server" "1" \
  "$(count_matching "${out}" '^==> cargo clippy --locked (-p [a-z-]+ )*-p ravel-server (-p [a-z-]+ )*--all-targets')"
check_eq "gates -p ravel-query: builds nothing" "0" "$(builds "${tmp}/query.calls")"

# Unscoped under the fleet work root: refused with exit 64 naming the hatch,
# and allowed with RAVEL_GATES_ALLOW_UNSCOPED=1. The scripts are copied into
# a fleet-shaped path, since gates.sh judges its own checkout.
fleet_root="${tmp}/var/lib/fleet/work/task-1"
mkdir -p "${fleet_root}/scripts"
cp "${SCRIPTS}/gates.sh" "${SCRIPTS}/affected-tests.sh" "${fleet_root}/scripts/"
run_stubbed fleet_unscoped bash "${fleet_root}/scripts/gates.sh" --dry-run
check_eq "gates unscoped in a fleet clone: exit 64" "64" "${rc}"
check_eq "gates unscoped in a fleet clone: names the hatch" "1" \
  "$(count_matching "${tmp}/fleet_unscoped.out" 'RAVEL_GATES_ALLOW_UNSCOPED=1')"
check_eq "gates unscoped in a fleet clone: builds nothing" "0" "$(builds "${tmp}/fleet_unscoped.calls")"
RAVEL_GATES_ALLOW_UNSCOPED=1 run_stubbed fleet_hatch bash "${fleet_root}/scripts/gates.sh" --dry-run
check_eq "gates unscoped in a fleet clone with the hatch: exit 0" "0" "${rc}"
check_eq "gates unscoped with the hatch: dry run reached" "1" \
  "$(count_matching "${tmp}/fleet_hatch.out" '^gates.sh: dry run')"
run_stubbed fleet_scoped bash "${fleet_root}/scripts/gates.sh" -p ravel-query --dry-run
check_eq "gates -p in a fleet clone: exit 0" "0" "${rc}"

# -p ravel-logseg: ravel-ingest depends on it, so it is in the closure.
run_stubbed logseg bash "${SCRIPTS}/gates.sh" -p ravel-logseg --dry-run
out="${tmp}/logseg.out"
check_eq "gates -p ravel-logseg: exit 0" "0" "${rc}"
check_eq "gates -p ravel-logseg: ravel-ingest in the closure" "1" \
  "$(count_matching "${out}" '^==> closure of -p ravel-logseg: (.* )?ravel-ingest( |$)')"
check_eq "gates -p ravel-logseg: scoped clippy names ravel-ingest" "1" \
  "$(count_matching "${out}" '^==> cargo clippy --locked -p ravel-logseg (-p [a-z-]+ )*-p ravel-ingest (-p [a-z-]+ )*--all-targets')"
check_eq "gates -p ravel-logseg: stage-timing lane" "1" "$(count "${out}" "${stage_clippy}")"
check_eq "gates -p ravel-logseg: ravel-ingest shipping-features clippy" "1" \
  "$(count "${out}" "${ingest_shipping_clippy}")"

# -p ravel-cli: the sql-explain lane, and no lane of a crate outside it.
run_stubbed cli bash "${SCRIPTS}/gates.sh" -p ravel-cli --dry-run
out="${tmp}/cli.out"
check_eq "gates -p ravel-cli: exit 0" "0" "${rc}"
check_eq "gates -p ravel-cli: sql-explain lane clippy" "1" "$(count "${out}" "${cli_explain_clippy}")"
check_eq "gates -p ravel-cli: sql-explain lane tests" "1" "$(count "${out}" "${cli_explain_test}")"
check_eq "gates -p ravel-cli: only that feature lane" "1" \
  "$(count_matching "${out}" '^==> cargo clippy --locked .*--features ')"

# -p ravel-sql: ravel-cli depends on it, so the sql-explain lane runs too.
run_stubbed sql bash "${SCRIPTS}/gates.sh" -p ravel-sql --dry-run
check_eq "gates -p ravel-sql: exit 0" "0" "${rc}"
check_eq "gates -p ravel-sql: sql-explain lane clippy" "1" \
  "$(count "${tmp}/sql.out" "${cli_explain_clippy}")"

# -p ravel-logseg: ravel-cli depends on it directly, so the sql-explain lane
# runs here too.
check_eq "gates -p ravel-logseg: sql-explain lane clippy" "1" \
  "$(count "${tmp}/logseg.out" "${cli_explain_clippy}")"
check_eq "gates -p ravel-logseg: sql-explain lane tests" "1" \
  "$(count "${tmp}/logseg.out" "${cli_explain_test}")"

# -p ravel-affinity: nothing that carries a feature lane is in its closure.
run_stubbed affinity bash "${SCRIPTS}/gates.sh" -p ravel-affinity --dry-run
out="${tmp}/affinity.out"
check_eq "gates -p ravel-affinity: exit 0" "0" "${rc}"
check_eq "gates -p ravel-affinity: closure" "1" \
  "$(count "${out}" '==> closure of -p ravel-affinity: ravel-affinity ravel-ingest-router')"
check_eq "gates -p ravel-affinity: scoped clippy" "1" \
  "$(count "${out}" '==> cargo clippy --locked -p ravel-affinity -p ravel-ingest-router --all-targets -- -D warnings')"
check_eq "gates -p ravel-affinity: no feature lane" "0" "$(count_matching "${out}" '--features')"
check_eq "gates -p ravel-affinity: no ravel-ingest shipping-features clippy" "0" \
  "$(count "${out}" "${ingest_shipping_clippy}")"

# -p ravel-esql: ravel-sql and ravel-server depend on it, so the esql lane
# runs, and with it the lanes that follow ravel-server.
run_stubbed esql bash "${SCRIPTS}/gates.sh" -p ravel-esql --dry-run
out="${tmp}/esql.out"
check_eq "gates -p ravel-esql: exit 0" "0" "${rc}"
check_eq "gates -p ravel-esql: esql lane clippy" "1" "$(count "${out}" "${esql_clippy}")"
check_eq "gates -p ravel-esql: esql lane tests" "1" "$(count "${out}" "${esql_test}")"
check_eq "gates -p ravel-esql: sql lane clippy" "1" "$(count "${out}" "${sql_clippy}")"

# -p ravel-types: every crate depends on it, ravel-server included, so the
# agents and mcp lanes run.
run_stubbed types bash "${SCRIPTS}/gates.sh" -p ravel-types --dry-run
out="${tmp}/types.out"
check_eq "gates -p ravel-types: exit 0" "0" "${rc}"
check_eq "gates -p ravel-types: agents lane clippy" "1" "$(count "${out}" "${agents_clippy}")"
check_eq "gates -p ravel-types: mcp lane clippy" "1" "$(count "${out}" "${mcp_clippy}")"

# -p ravel-agents-core: ravel-agents-runtime, ravel-agents-sim and ravel-server
# depend on it, so the agents lane runs with its page-size-2 run, once.
run_stubbed agents_core bash "${SCRIPTS}/gates.sh" -p ravel-agents-core --dry-run
out="${tmp}/agents_core.out"
check_eq "gates -p ravel-agents-core: exit 0" "0" "${rc}"
check_eq "gates -p ravel-agents-core: agents lane clippy" "1" "$(count "${out}" "${agents_clippy}")"
check_eq "gates -p ravel-agents-core: page-size-2 run" "1" "$(count "${out}" "${agents_small_pages}")"
check_eq "gates -p ravel-agents-core: no other page-size run" "1" \
  "$(count_matching "${out}" 'RAVEL_AGENTS_PAGE_ENTRIES')"
check_eq "gates -p ravel-agents-core: builds nothing" "0" "$(builds "${tmp}/agents_core.calls")"

# -p ravel-server: the server depends on the agents crates, not the reverse,
# so the agents lane runs without the page-size-2 run.
run_stubbed server bash "${SCRIPTS}/gates.sh" -p ravel-server --dry-run
check_eq "gates -p ravel-server: agents lane clippy" "1" \
  "$(count "${tmp}/server.out" "${agents_clippy}")"
check_eq "gates -p ravel-server: no page-size-2 run" "0" \
  "$(count_matching "${tmp}/server.out" 'RAVEL_AGENTS_PAGE_ENTRIES')"

# -p ravel-logseg: ravel-cli and ravel-bench depend on it, ravel-server does
# not, so neither the agents nor the mcp lane runs.
check_eq "gates -p ravel-logseg: no agents lane" "0" \
  "$(count "${tmp}/logseg.out" "${agents_clippy}")"
check_eq "gates -p ravel-logseg: no mcp lane" "0" \
  "$(count "${tmp}/logseg.out" "${mcp_clippy}")"

# -p ravel-logseg reaches no crate the esql lane covers.
check_eq "gates -p ravel-logseg: no esql lane" "0" \
  "$(count "${tmp}/logseg.out" "${esql_clippy}")"

# Workspace mode: every lane, and no cargo call at all. The hatch is set
# because this checkout may itself sit under the fleet work root (the
# orchestrator host does), and the refusal has its own cases above.
RAVEL_GATES_ALLOW_UNSCOPED=1 run_stubbed workspace bash "${SCRIPTS}/gates.sh" --dry-run
out="${tmp}/workspace.out"
check_eq "gates --dry-run: exit 0" "0" "${rc}"
check_eq "gates --dry-run: workspace clippy" "1" \
  "$(count "${out}" '==> cargo clippy --locked --workspace --all-targets -- -D warnings')"
check_eq "gates --dry-run: sql lane" "1" "$(count "${out}" "${sql_clippy}")"
check_eq "gates --dry-run: flight-sql lane" "1" "$(count "${out}" "${flight_clippy}")"
check_eq "gates --dry-run: esql lane" "1" "$(count "${out}" "${esql_clippy}")"
check_eq "gates --dry-run: sql-explain lane" "1" "$(count "${out}" "${cli_explain_clippy}")"
check_eq "gates --dry-run: ravel-ingest shipping-features clippy" "1" \
  "$(count "${out}" "${ingest_shipping_clippy}")"
check_eq "gates --dry-run: agents lane clippy" "1" "$(count "${out}" "${agents_clippy}")"
check_eq "gates --dry-run: agents lane seam and effects binaries" "1" "$(count "${out}" "${agents_test}")"
check_eq "gates --dry-run: agents lane lib tests" "1" "$(count "${out}" "${agents_lib_test}")"
check_eq "gates --dry-run: agents lane classification test" "1" \
  "$(count "${out}" "${agents_classification}")"
check_eq "gates --dry-run: agents page-size-2 run" "1" "$(count "${out}" "${agents_small_pages}")"
check_eq "gates --dry-run: mcp lane clippy" "1" "$(count "${out}" "${mcp_clippy}")"
check_eq "gates --dry-run: mcp lane binaries" "1" "$(count "${out}" "${mcp_test}")"
check_eq "gates --dry-run: mcp lane lib tests" "1" "$(count "${out}" "${mcp_lib_test}")"
check_eq "gates --dry-run: mcp lane agents clippy" "1" "$(count "${out}" "${mcp_agents_clippy}")"
check_eq "gates --dry-run: mcp lane agents binary" "1" "$(count "${out}" "${mcp_agents_test}")"
check_eq "gates --dry-run: SDK gate" "1" "$(count_matching "${out}" "${sdk_gate_re}")"
check_eq "gates --dry-run: no SDK skip line without the variable" "0" "$(count "${out}" "${sdk_skip}")"
check_eq "gates --dry-run: walk-expiry lane" "1" \
  "$(count "${out}" '==> cargo clippy --locked -p ravel-bench --features walk-expiry --all-targets -- -D warnings')"
check_eq "gates --dry-run: feature lane clippy runs" "11" \
  "$(count_matching "${out}" '^==> cargo clippy --locked .*--features ')"
check_eq "gates --dry-run: feature lane nextest runs" "13" \
  "$(count_matching "${out}" '^==> cargo nextest run --locked .*--features ')"

# RAVEL_GATES_SKIP_SDK=1: the SDK step becomes one skip line, and nothing else
# in the run changes.
RAVEL_GATES_ALLOW_UNSCOPED=1 RAVEL_GATES_SKIP_SDK=1 run_stubbed workspace_nosdk \
  bash "${SCRIPTS}/gates.sh" --dry-run
out="${tmp}/workspace_nosdk.out"
check_eq "gates --dry-run, SDK skipped: exit 0" "0" "${rc}"
check_eq "gates --dry-run, SDK skipped: skip line" "1" "$(count "${out}" "${sdk_skip}")"
check_eq "gates --dry-run, SDK skipped: no SDK gate" "0" "$(count_matching "${out}" "${sdk_gate_re}")"
check_eq "gates --dry-run, SDK skipped: the rest unchanged" "1" \
  "$(diff <(grep -v -e "${sdk_gate_re}" "${tmp}/workspace.out") \
    <(grep -vxF -e "${sdk_skip}" "${out}") >/dev/null && echo 1 || echo 0)"
check_eq "gates --dry-run: no cargo call" "0" "$(wc -l <"${tmp}/workspace.calls" | tr -d ' ')"

# A crate that is not a workspace member: the closure step's own exit 2, and
# no cargo command listed.
run_stubbed unknown bash "${SCRIPTS}/gates.sh" -p ravel-nope --dry-run
check_eq "gates -p ravel-nope: exit 2" "2" "${rc}"
check_eq "gates -p ravel-nope: no command listed" "0" "$(count_matching "${tmp}/unknown.out" '^==> cargo ')"

# The documentation claims registry check CI's doc-scripts job runs is in the
# ordinary gate run too (issue #3188): an invocation on a non-comment line,
# after the dry-run exit, so a real run reaches it. The source guards are
# skipped under --dry-run, so this reads the script instead of running it.
dry_exit_line="$(grep -n 'gates.sh: dry run;' "${SCRIPTS}/gates.sh" | head -1 | cut -d: -f1)"
claims_line="$(grep -n '^python3 .*check-doc-claims\.py"$' "${SCRIPTS}/gates.sh" | head -1 | cut -d: -f1)"
check_eq "gates: runs check-doc-claims.py" "1" \
  "$(grep -c '^python3 .*check-doc-claims\.py"$' "${SCRIPTS}/gates.sh")"
check_eq "gates: check-doc-claims.py runs after the dry-run exit" "1" \
  "$([[ -n "${dry_exit_line}" && -n "${claims_line}" && ${claims_line} -gt ${dry_exit_line} ]] && echo 1 || echo 0)"

# --- gates.sh --tier (ADR-4034 D1) ---------------------------------------

# Every line that starts a cargo invocation, an `env VAR=value` prefix included.
cargo_lines() { grep -E '^==> (env [A-Z_]+=[^ ]+ )?cargo ' "$1"; }
cargo_count() { grep -cE '^==> (env [A-Z_]+=[^ ]+ )?cargo ' "$1"; }

# tier_executor_ravel_query_checks_dependents_tests_only_edited: ravel-query's
# dependents are compiled by one `cargo check` carrying every lane feature the
# closure reaches, and nothing but ravel-query is tested.
run_stubbed exec_query bash "${SCRIPTS}/gates.sh" --tier executor -p ravel-query --dry-run
out="${tmp}/exec_query.out"
check_eq "tier executor -p ravel-query: exit 0" "0" "${rc}"
check_eq "tier executor -p ravel-query: first line names the tier" "1" \
  "$(head -1 "${out}" | grep -c '^gates.sh: dry run; tier executor:')"
check_eq "tier executor -p ravel-query: clippy of the edited crate" "1" \
  "$(count "${out}" '==> cargo clippy --locked -p ravel-query --all-targets -- -D warnings')"
check_eq "tier executor -p ravel-query: tests of the edited crate, ci profile" "1" \
  "$(count "${out}" '==> cargo nextest run --locked -p ravel-query --cargo-profile ci')"
check_eq "tier executor -p ravel-query: doctests of the edited crate" "1" \
  "$(count "${out}" '==> cargo test --locked --doc --profile ci -p ravel-query')"
check_eq "tier executor -p ravel-query: the test commands name only ravel-query" "-p ravel-query" \
  "$(cargo_lines "${out}" | grep -E '^==> (env [^ ]+ )?cargo (nextest run|test) ' |
    grep -oE -- '-p [a-z-]+' | sort -u | paste -sd' ' -)"
check_eq "tier executor -p ravel-query: one cargo check" "1" \
  "$(count_matching "${out}" '^==> cargo check ')"
check_eq "tier executor -p ravel-query: ravel-server only in the check line" "1" \
  "$(cargo_lines "${out}" | grep -c 'ravel-server')"
check_eq "tier executor -p ravel-query: ravel-sql only in the check line" "1" \
  "$(cargo_lines "${out}" | grep -c 'ravel-sql')"
check_eq "tier executor -p ravel-query: the check line compiles ravel-server and ravel-sql" "1" \
  "$(count_matching "${out}" '^==> cargo check --locked (-p [a-z-]+ )*-p ravel-sql (-p [a-z-]+ )*-p ravel-server (-p [a-z-]+ )*--all-targets --features ')"
check_eq "tier executor -p ravel-query: the check line leaves ravel-query out" "0" \
  "$(count_matching "${out}" '^==> cargo check .*-p ravel-query ')"
for f in ravel-server/sql ravel-server/esql ravel-server/flight-sql ravel-server/agents \
  ravel-server/mcp ravel-sql/flight-sql ravel-bench/saturation ravel-cli/sql-explain; do
  check_eq "tier executor -p ravel-query: check line carries ${f}" "1" \
    "$(count_matching "${out}" "^==> cargo check .*--features ([a-z/,-]+,)?${f}(,|$)")"
done
check_eq "tier executor -p ravel-query: no lane reached only through a dependent runs" "0" \
  "$(count_matching "${out}" '^==> cargo (clippy|nextest run|test) .*--features ')"
check_eq "tier executor -p ravel-query: builds nothing" "0" "$(builds "${tmp}/exec_query.calls")"

# The executor tier for an edited crate that carries lanes: each lane's clippy
# restricted to it, and one test run with the lane features unified.
run_stubbed exec_server bash "${SCRIPTS}/gates.sh" --tier executor -p ravel-server --dry-run
out="${tmp}/exec_server.out"
check_eq "tier executor -p ravel-server: exit 0" "0" "${rc}"
for l in "${sql_clippy}" "${agents_clippy}" "${mcp_clippy}" "${mcp_agents_clippy}" "${esql_clippy}" \
  '==> cargo clippy --locked -p ravel-server --features flight-sql --all-targets -- -D warnings'; do
  check_eq "tier executor -p ravel-server: lane clippy ${l#==> cargo clippy --locked }" "1" "$(count "${out}" "${l}")"
done
check_eq "tier executor -p ravel-server: one lane test run, features unified" "1" \
  "$(count "${out}" '==> cargo nextest run --locked -p ravel-server --features ravel-server/sql,ravel-server/agents,ravel-server/mcp,ravel-server/flight-sql,ravel-server/esql --cargo-profile ci')"
check_eq "tier executor -p ravel-server: no test of a dependent" "0" \
  "$(count_matching "${out}" '^==> cargo (nextest run|test) .*-p ravel-(bench|cli|catalog)( |$)')"

# -p ravel-ingest: its shipping-features clippy is the edited crate's own
# clippy, listed once; its stage-timing lane runs restricted to it.
run_stubbed exec_ingest bash "${SCRIPTS}/gates.sh" --tier executor -p ravel-ingest --dry-run
out="${tmp}/exec_ingest.out"
check_eq "tier executor -p ravel-ingest: exit 0" "0" "${rc}"
check_eq "tier executor -p ravel-ingest: shipping clippy once" "1" "$(count "${out}" "${ingest_shipping_clippy}")"
check_eq "tier executor -p ravel-ingest: stage-timing lane clippy restricted" "1" \
  "$(count "${out}" '==> cargo clippy --locked -p ravel-ingest --features stage-timing --all-targets -- -D warnings')"
check_eq "tier executor -p ravel-ingest: no ravel-logseg anywhere" "0" "$(cargo_lines "${out}" | grep -c 'ravel-logseg')"

# tier_merge_is_default_unchanged: no --tier and --tier merge print the same
# output, and merge's own command list is unchanged apart from the dry-run
# header line (which now names the tier); the merge list for -p ravel-query
# is pinned in full, so a changed merge command fails here and not only in
# the per-lane cases above.
cat >"${tmp}/query_merge.want" <<'EOF'
==> closure of -p ravel-query: ravel-query ravel-sql ravel-server ravel-bench ravel-cli
==> cargo clippy --locked -p ravel-query -p ravel-sql -p ravel-server -p ravel-bench -p ravel-cli --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-query -p ravel-sql -p ravel-server -p ravel-bench -p ravel-cli --cargo-profile ci
==> cargo test --locked --doc --profile ci -p ravel-query -p ravel-sql -p ravel-server -p ravel-bench -p ravel-cli
==> cargo clippy --locked -p ravel-server --features sql --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-server --features sql
==> cargo clippy --locked -p ravel-server --features agents --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-server --features agents --test agents_seam --test agents_effects_e2e --test agents_e2e --test agents_timers_e2e --test agents_projection_e2e --test agents_real_store
==> cargo nextest run --locked -p ravel-server --features agents --lib
==> cargo nextest run --locked -p ravel-catalog --test sys_proto_format_version_classification
==> cargo clippy --locked -p ravel-server --features mcp --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-server --features mcp --test mcp_e2e --test trace_by_id --test mcp_conformance --test mcp_escape_routes --test mcp_audit --test mcp_adversarial_isolation --test mcp_adversarial_replay --test mcp_adversarial_hostile --test mcp_adversarial_precision --test mcp_adversarial_paging
==> cargo nextest run --locked -p ravel-server --features mcp --lib
==> cargo clippy --locked -p ravel-server --features mcp,agents --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-server --features mcp,agents --test mcp_agents_e2e
==> cargo clippy --locked -p ravel-server -p ravel-sql --features flight-sql --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-server -p ravel-sql --features flight-sql
==> cargo clippy --locked -p ravel-server --features esql --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-server --features esql
==> cargo clippy --locked -p ravel-bench --features sql-latency,profiling,flight-lane --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-bench --features sql-latency,profiling,flight-lane
==> cargo clippy --locked -p ravel-bench --features saturation --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-bench --features saturation
==> cargo clippy --locked -p ravel-bench --features walk-expiry --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-bench --features walk-expiry
==> cargo check --locked -p ravel-bench --features stage-timing --all-targets
==> cargo test --locked -p ravel-bench --features stage-timing
==> cargo clippy --locked -p ravel-logseg -p ravel-ingest --features stage-timing --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-logseg -p ravel-ingest --features stage-timing
==> cargo clippy --locked -p ravel-cli --features sql-explain --all-targets -- -D warnings
==> cargo nextest run --locked -p ravel-cli --features sql-explain
EOF
for c in ravel-query ravel-server; do
  run_stubbed "merge_default_${c}" bash "${SCRIPTS}/gates.sh" -p "${c}" --dry-run
  run_stubbed "merge_named_${c}" bash "${SCRIPTS}/gates.sh" --tier merge -p "${c}" --dry-run
  check_eq "tier merge -p ${c}: exit 0" "0" "${rc}"
  check_eq "tier merge -p ${c}: identical to no --tier" "1" \
    "$(cmp -s "${tmp}/merge_default_${c}.out" "${tmp}/merge_named_${c}.out" && echo 1 || echo 0)"
  check_eq "tier merge -p ${c}: first line names the tier" "1" \
    "$(head -1 "${tmp}/merge_named_${c}.out" | grep -c '^gates.sh: dry run; tier merge:')"
done
check_eq "tier merge -p ravel-query: the pinned command list" "1" \
  "$(tail -n +2 "${tmp}/merge_default_ravel-query.out" | cmp -s - "${tmp}/query_merge.want" && echo 1 || echo 0)"

# tier_executor_unscoped_refused: no -p is refused before any work, with a
# one-line reason. The hatch is set so the fleet-clone refusal cannot be the
# one that answers.
RAVEL_GATES_ALLOW_UNSCOPED=1 run_stubbed exec_unscoped bash "${SCRIPTS}/gates.sh" --tier executor --dry-run
check_eq "tier executor unscoped: exit 64" "64" "${rc}"
check_eq "tier executor unscoped: one line of output" "1" "$(wc -l <"${tmp}/exec_unscoped.out" | tr -d ' ')"
check_eq "tier executor unscoped: says why" "1" \
  "$(count_matching "${tmp}/exec_unscoped.out" '^gates.sh: --tier executor needs at least one -p CRATE')"
check_eq "tier executor unscoped: builds nothing" "0" "$(wc -l <"${tmp}/exec_unscoped.calls" | tr -d ' ')"

# tier_executor_lane_listing_failure_refused: run_executor_gates reads the
# lane list from run_feature_lanes' own --dry-run output by capturing it into
# a variable first and checking its exit status explicitly. This rules out
# the prior implementation, a `while read` loop fed by process substitution
# (`done < <(...)`), which discards that subshell's exit status: under it, a
# lane listing that failed partway would print nothing, exit 0, and silently
# drop every lane clippy and test line after the failure instead of refusing.
# Nothing in the listing runs a real command under --dry-run, so
# RAVEL_GATES_TEST_FORCE_LANE_LISTING_FAILURE=1 is the only way to exercise
# this path: it makes the listing subshell exit 3 before producing any output.
RAVEL_GATES_TEST_FORCE_LANE_LISTING_FAILURE=1 run_stubbed exec_lane_listing_fail \
  bash "${SCRIPTS}/gates.sh" --tier executor -p ravel-query --dry-run
check_eq "tier executor lane listing failure: exit 70" "70" "${rc}"
check_eq "tier executor lane listing failure: refuses with a reason" "1" \
  "$(count_matching "${tmp}/exec_lane_listing_fail.out" '^gates.sh: could not list the executor tier')"
check_eq "tier executor lane listing failure: builds nothing" "0" "$(builds "${tmp}/exec_lane_listing_fail.calls")"

# tier_executor_command_count_bar: ADR-4034 D1's bar, at most 40 percent of the
# merge tier's cargo commands, for the three closures it names.
for c in ravel-query ravel-server ravel-ingest; do
  run_stubbed "bar_merge_${c}" bash "${SCRIPTS}/gates.sh" -p "${c}" --dry-run
  run_stubbed "bar_exec_${c}" bash "${SCRIPTS}/gates.sh" --tier executor -p "${c}" --dry-run
  merge_n="$(cargo_count "${tmp}/bar_merge_${c}.out")"
  exec_n="$(cargo_count "${tmp}/bar_exec_${c}.out")"
  check_eq "tier executor -p ${c}: ${exec_n} of ${merge_n} cargo commands is at most 40 percent" "1" \
    "$([[ ${merge_n} -gt 0 && $((exec_n * 100)) -le $((merge_n * 40)) ]] && echo 1 || echo 0)"
done

# tier_unknown_value_refused: an unknown tier, and --tier with no value.
run_stubbed tier_fast bash "${SCRIPTS}/gates.sh" --tier fast -p ravel-query --dry-run
check_eq "tier fast: exit 64" "64" "${rc}"
check_eq "tier fast: names the value" "1" "$(count_matching "${tmp}/tier_fast.out" '^gates.sh: unknown tier: fast ')"
check_eq "tier fast: builds nothing" "0" "$(wc -l <"${tmp}/tier_fast.calls" | tr -d ' ')"
run_stubbed tier_empty bash "${SCRIPTS}/gates.sh" -p ravel-query --tier
check_eq "tier with no value: exit 64" "64" "${rc}"

# --- affected-tests.sh --------------------------------------------------

# -n prints the closure line and nothing else, and runs no test.
run_stubbed at-dry bash "${SCRIPTS}/affected-tests.sh" -n -p ravel-query
check_eq "affected-tests -n: exit 0" "0" "${rc}"
check_eq "affected-tests -n: one line of output" "1" "$(wc -l <"${tmp}/at-dry.out" | tr -d ' ')"
check_eq "affected-tests -n: it is the closure line" "1" \
  "$(count_matching "${tmp}/at-dry.out" '^affected crates: ravel-query ')"
check_eq "affected-tests -n: runs no test" "0" "$(builds "${tmp}/at-dry.calls")"

# ravel-server in the closure: the sql, flight-sql and esql test runs, ci
# profile.
run_stubbed at-query bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
calls="${tmp}/at-query.calls"
check_eq "affected-tests -p ravel-query: exit 0" "0" "${rc}"
check_eq "affected-tests -p ravel-query: sql tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features sql')"
check_eq "affected-tests -p ravel-query: flight-sql tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server -p ravel-sql --features flight-sql')"
check_eq "affected-tests -p ravel-query: esql tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features esql')"
check_eq "affected-tests -p ravel-query: agents seam, effects and timers binaries" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features agents --test agents_seam --test agents_effects_e2e --test agents_e2e --test agents_timers_e2e --test agents_projection_e2e --test agents_real_store')"
check_eq "affected-tests -p ravel-query: agents lib tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features agents --lib')"
check_eq "affected-tests -p ravel-query: no bare agents name filter" "0" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features agents agents')"
check_eq "affected-tests -p ravel-query: classification test" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-catalog --test sys_proto_format_version_classification')"
check_eq "affected-tests -p ravel-query: mcp binaries" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features mcp --test mcp_e2e --test trace_by_id --test mcp_conformance --test mcp_escape_routes --test mcp_audit --test mcp_adversarial_isolation --test mcp_adversarial_replay --test mcp_adversarial_hostile --test mcp_adversarial_precision --test mcp_adversarial_paging')"
check_eq "affected-tests -p ravel-query: mcp lib tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features mcp --lib')"
check_eq "affected-tests -p ravel-query: mcp agents binary" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features mcp,agents --test mcp_agents_e2e')"
check_eq "affected-tests -p ravel-query: no bare mcp name filter" "0" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features mcp mcp')"

check_eq "affected-tests -p ravel-query: no page-size-2 run" "0" \
  "$(count_matching "${calls}" 'RAVEL_AGENTS_PAGE_ENTRIES')"

# ravel-agents-runtime in the closure: its and the sim's tests run again with
# RAVEL_AGENTS_PAGE_ENTRIES=2 in the environment, once.
run_stubbed at-agents bash "${SCRIPTS}/affected-tests.sh" -p ravel-agents-core
calls="${tmp}/at-agents.calls"
check_eq "affected-tests -p ravel-agents-core: exit 0" "0" "${rc}"
check_eq "affected-tests -p ravel-agents-core: page-size-2 run" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-agents-runtime -p ravel-agents-sim [RAVEL_AGENTS_PAGE_ENTRIES=2]')"
check_eq "affected-tests -p ravel-agents-core: only that run sets the page size" "1" \
  "$(count_matching "${calls}" 'RAVEL_AGENTS_PAGE_ENTRIES')"

# A failing page-size-2 run fails the script, after the flight-sql run still
# ran.
CARGO_STUB_FAIL_ON="--cargo-profile ci -p ravel-agents-runtime -p ravel-agents-sim" run_stubbed at-pages-fail \
  bash "${SCRIPTS}/affected-tests.sh" -p ravel-agents-core
check_eq "affected-tests, page-size-2 run fails: exit 3" "3" "${rc}"
check_eq "affected-tests, page-size-2 run fails: flight-sql still ran" "1" \
  "$(count_matching "${tmp}/at-pages-fail.calls" '--features flight-sql$')"

# Neither crate in the closure: no feature run.
run_stubbed at-affinity bash "${SCRIPTS}/affected-tests.sh" -p ravel-affinity
check_eq "affected-tests -p ravel-affinity: exit 0" "0" "${rc}"
check_eq "affected-tests -p ravel-affinity: no feature run" "0" \
  "$(count_matching "${tmp}/at-affinity.calls" '--features')"
check_eq "affected-tests -p ravel-affinity: no classification test" "0" \
  "$(count_matching "${tmp}/at-affinity.calls" 'sys_proto_format_version_classification')"

# A failing sql run fails the script, after the flight-sql run still ran.
CARGO_STUB_FAIL_ON="--features sql" run_stubbed at-fail bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
check_eq "affected-tests, sql run fails: exit 3" "3" "${rc}"
check_eq "affected-tests, sql run fails: flight-sql still ran" "1" \
  "$(count_matching "${tmp}/at-fail.calls" '--features flight-sql$')"

# A failing esql run fails the script too.
CARGO_STUB_FAIL_ON="--features esql" run_stubbed at-esql-fail bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
check_eq "affected-tests, esql run fails: exit 3" "3" "${rc}"
check_eq "affected-tests, esql run fails: sql still ran" "1" \
  "$(count_matching "${tmp}/at-esql-fail.calls" '--features sql$')"

# A failing agents run fails the script too, after the mcp run still ran.
CARGO_STUB_FAIL_ON="--features agents" run_stubbed at-agents-fail bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
check_eq "affected-tests, agents run fails: exit 3" "3" "${rc}"
check_eq "affected-tests, agents run fails: mcp still ran" "1" \
  "$(count_matching "${tmp}/at-agents-fail.calls" '--features mcp --lib$')"

# A failing agents lib run fails the script though the seam binary passed.
CARGO_STUB_FAIL_ON="--features agents --lib" run_stubbed at-agents-lib-fail \
  bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
check_eq "affected-tests, agents lib run fails: exit 3" "3" "${rc}"

# A failing mcp run fails the script.
CARGO_STUB_FAIL_ON="--features mcp" run_stubbed at-mcp-fail bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
check_eq "affected-tests, mcp run fails: exit 3" "3" "${rc}"

# A failing mcp lib run fails the script though the binaries passed.
CARGO_STUB_FAIL_ON="--features mcp --lib" run_stubbed at-mcp-lib-fail \
  bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
check_eq "affected-tests, mcp lib run fails: exit 3" "3" "${rc}"

# A failing mcp,agents run fails the script though the mcp runs passed.
CARGO_STUB_FAIL_ON="--features mcp,agents" run_stubbed at-mcp-agents-fail \
  bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
check_eq "affected-tests, mcp,agents run fails: exit 3" "3" "${rc}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
