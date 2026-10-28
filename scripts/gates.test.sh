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
# (ravel-sql and ravel-server depend on ravel-query and ravel-esql, ravel-cli
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
  pkg ravel-server ravel-sql ravel-query ravel-esql
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
# shellcheck disable=SC2016
printf '%s\n' '#!/usr/bin/env bash' \
  'if [[ "$1" == "metadata" ]]; then echo "cargo $*" >>"${CARGO_STUB_LOG}"; cat "${CARGO_STUB_METADATA}"; exit 0; fi' \
  'echo "cargo $*" >>"${CARGO_STUB_LOG}"' \
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
agents_test='==> cargo nextest run --locked -p ravel-server --features agents --test agents_seam --test agents_effects_e2e --test agents_e2e --test agents_timers_e2e'
agents_lib_test='==> cargo nextest run --locked -p ravel-server --features agents --lib'
agents_name_filter='==> cargo nextest run --locked -p ravel-server --features agents agents'
agents_classification='==> cargo nextest run --locked -p ravel-catalog --test sys_proto_format_version_classification'
mcp_clippy='==> cargo clippy --locked -p ravel-server --features mcp --all-targets -- -D warnings'
mcp_test='==> cargo nextest run --locked -p ravel-server --features mcp --test mcp_e2e --test trace_by_id'
mcp_lib_test='==> cargo nextest run --locked -p ravel-server --features mcp --lib'
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
check_eq "gates -p ravel-query: mcp lane clippy" "1" "$(count "${out}" "${mcp_clippy}")"
check_eq "gates -p ravel-query: mcp lane binaries" "1" "$(count "${out}" "${mcp_test}")"
check_eq "gates -p ravel-query: mcp lane lib tests" "1" "$(count "${out}" "${mcp_lib_test}")"
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
check_eq "gates --dry-run: mcp lane clippy" "1" "$(count "${out}" "${mcp_clippy}")"
check_eq "gates --dry-run: mcp lane binaries" "1" "$(count "${out}" "${mcp_test}")"
check_eq "gates --dry-run: mcp lane lib tests" "1" "$(count "${out}" "${mcp_lib_test}")"
check_eq "gates --dry-run: SDK gate" "1" "$(count_matching "${out}" "${sdk_gate_re}")"
check_eq "gates --dry-run: no SDK skip line without the variable" "0" "$(count "${out}" "${sdk_skip}")"
check_eq "gates --dry-run: feature lane clippy runs" "9" \
  "$(count_matching "${out}" '^==> cargo clippy --locked .*--features ')"
check_eq "gates --dry-run: feature lane nextest runs" "11" \
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
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features agents --test agents_seam --test agents_effects_e2e --test agents_e2e --test agents_timers_e2e')"
check_eq "affected-tests -p ravel-query: agents lib tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features agents --lib')"
check_eq "affected-tests -p ravel-query: no bare agents name filter" "0" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features agents agents')"
check_eq "affected-tests -p ravel-query: classification test" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-catalog --test sys_proto_format_version_classification')"
check_eq "affected-tests -p ravel-query: mcp binaries" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features mcp --test mcp_e2e --test trace_by_id')"
check_eq "affected-tests -p ravel-query: mcp lib tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features mcp --lib')"
check_eq "affected-tests -p ravel-query: no bare mcp name filter" "0" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features mcp mcp')"

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

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
