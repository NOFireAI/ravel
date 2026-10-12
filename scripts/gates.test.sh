#!/usr/bin/env bash
# Cases for the scoped selection in scripts/gates.sh and scripts/affected-tests.sh
# (issue #2877): scoped mode gates the reverse-dependency closure of the named
# crates, and the sql and flight-sql lanes follow that closure.
#
# Run by the doc-scripts job in .github/workflows/ci.yml, and by hand with
#   bash scripts/gates.test.sh
#
# No toolchain: a stub `cargo` first on PATH answers `cargo metadata` with a
# synthetic workspace whose edges mirror the real ones these cases are about
# (ravel-sql and ravel-server depend on ravel-query, ravel-ingest on
# ravel-logseg, ravel-ingest-router on ravel-affinity), logs every other call
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
  pkg ravel-sql ravel-query
  printf ','
  pkg ravel-server ravel-sql ravel-query
  printf ','
  pkg ravel-logseg ravel-types
  printf ','
  pkg ravel-ingest ravel-logseg ravel-types
  printf ','
  pkg ravel-bench ravel-server ravel-ingest
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
stage_clippy='==> cargo clippy --locked -p ravel-logseg -p ravel-ingest --features stage-timing --all-targets -- -D warnings'

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
  "$(count_matching "${out}" '^==> cargo clippy --locked (-p [a-z-]+ )*-p ravel-ingest (-p [a-z-]+ )*--all-targets')"
check_eq "gates -p ravel-logseg: stage-timing lane" "1" "$(count "${out}" "${stage_clippy}")"

# -p ravel-affinity: nothing that carries a feature lane is in its closure.
run_stubbed affinity bash "${SCRIPTS}/gates.sh" -p ravel-affinity --dry-run
out="${tmp}/affinity.out"
check_eq "gates -p ravel-affinity: exit 0" "0" "${rc}"
check_eq "gates -p ravel-affinity: closure" "1" \
  "$(count "${out}" '==> closure of -p ravel-affinity: ravel-affinity ravel-ingest-router')"
check_eq "gates -p ravel-affinity: scoped clippy" "1" \
  "$(count "${out}" '==> cargo clippy --locked -p ravel-affinity -p ravel-ingest-router --all-targets -- -D warnings')"
check_eq "gates -p ravel-affinity: no feature lane" "0" "$(count_matching "${out}" '--features')"

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
check_eq "gates --dry-run: feature lane clippy runs" "5" \
  "$(count_matching "${out}" '^==> cargo clippy --locked .*--features ')"
check_eq "gates --dry-run: no cargo call" "0" "$(wc -l <"${tmp}/workspace.calls" | tr -d ' ')"

# A crate that is not a workspace member: the closure step's own exit 2, and
# no cargo command listed.
run_stubbed unknown bash "${SCRIPTS}/gates.sh" -p ravel-nope --dry-run
check_eq "gates -p ravel-nope: exit 2" "2" "${rc}"
check_eq "gates -p ravel-nope: no command listed" "0" "$(count_matching "${tmp}/unknown.out" '^==> cargo ')"

# --- affected-tests.sh --------------------------------------------------

# -n prints the closure line and nothing else, and runs no test.
run_stubbed at-dry bash "${SCRIPTS}/affected-tests.sh" -n -p ravel-query
check_eq "affected-tests -n: exit 0" "0" "${rc}"
check_eq "affected-tests -n: one line of output" "1" "$(wc -l <"${tmp}/at-dry.out" | tr -d ' ')"
check_eq "affected-tests -n: it is the closure line" "1" \
  "$(count_matching "${tmp}/at-dry.out" '^affected crates: ravel-query ')"
check_eq "affected-tests -n: runs no test" "0" "$(builds "${tmp}/at-dry.calls")"

# ravel-server in the closure: the sql and flight-sql test runs, ci profile.
run_stubbed at-query bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
calls="${tmp}/at-query.calls"
check_eq "affected-tests -p ravel-query: exit 0" "0" "${rc}"
check_eq "affected-tests -p ravel-query: sql tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server --features sql')"
check_eq "affected-tests -p ravel-query: flight-sql tests" "1" \
  "$(count "${calls}" 'cargo nextest run --cargo-profile ci -p ravel-server -p ravel-sql --features flight-sql')"

# Neither crate in the closure: no feature run.
run_stubbed at-affinity bash "${SCRIPTS}/affected-tests.sh" -p ravel-affinity
check_eq "affected-tests -p ravel-affinity: exit 0" "0" "${rc}"
check_eq "affected-tests -p ravel-affinity: no feature run" "0" \
  "$(count_matching "${tmp}/at-affinity.calls" '--features')"

# A failing sql run fails the script, after the flight-sql run still ran.
CARGO_STUB_FAIL_ON="--features sql" run_stubbed at-fail bash "${SCRIPTS}/affected-tests.sh" -p ravel-query
check_eq "affected-tests, sql run fails: exit 3" "3" "${rc}"
check_eq "affected-tests, sql run fails: flight-sql still ran" "1" \
  "$(count_matching "${tmp}/at-fail.calls" '--features flight-sql$')"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
