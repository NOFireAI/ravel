#!/usr/bin/env bash
# Cases for build-pgo.sh (issue #4527). Runs the real script with stub perf,
# llvm-profgen, llvm-profdata, rustc, cargo, git and uname first on PATH,
# each writing canned output and recording its argv, so no case needs a PMU,
# an LLVM install or a build.
#
# Run: bash scripts/build-pgo.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/build-pgo.sh"
TMP="$(cd "$(mktemp -d "${TMPDIR:-/tmp}/build-pgo-test.XXXXXX")" && pwd)"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0
REAL_UNAME="$(command -v uname)"
HEAD_SHA="1111111111111111111111111111111111111111"
OTHER_SHA="2222222222222222222222222222222222222222"

STUB_BIN="${TMP}/stub-bin"
SYSROOT="${TMP}/sysroot"
SYSROOT_BIN="${SYSROOT}/lib/rustlib/x86_64-unknown-linux-gnu/bin"
mkdir -p "${STUB_BIN}" "${SYSROOT_BIN}" "${TMP}/empty-sysroot"

cat >"${STUB_BIN}/rustc" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_LOG}/rustc.argv"
case "$*" in
  -vV) printf 'rustc 1.100.0 (stub)\nbinary: rustc\nhost: x86_64-unknown-linux-gnu\nrelease: 1.100.0\nLLVM version: %s\n' "${STUB_RUSTC_LLVM:-23.1.5}" ;;
  "--print sysroot") printf '%s\n' "${STUB_SYSROOT}" ;;
  -V) echo "rustc 1.100.0 (stub)" ;;
  *) exit 1 ;;
esac
EOF

cat >"${STUB_BIN}/llvm-profgen" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_LOG}/llvm-profgen.argv"
if [[ "${1:-}" == "--version" ]]; then
  printf 'LLVM (http://llvm.org/):\n  LLVM version %s\n' "${STUB_PROFGEN_LLVM:-23.1.5}"
  exit 0
fi
for a in "$@"; do
  case "$a" in --output=*) out=${a#--output=} ;; esac
done
printf 'stub sample profile\n' >"${out}"
EOF

cat >"${STUB_BIN}/llvm-profdata" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_LOG}/llvm-profdata.argv"
cat "${STUB_PROFDATA_SHOW}"
EOF

cat >"${STUB_BIN}/perf" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_LOG}/perf.argv"
case "$1" in
  record)
    while [[ $# -gt 0 ]]; do
      case "$1" in
        -e) ev=$2; shift ;;
        -o) out=$2; shift ;;
      esac
      shift
    done
    if [[ " ${STUB_PERF_FAIL_EVENTS:-} " == *" ${ev} "* ]]; then
      : >"${out}"
    else
      printf 'stub perf data\n' >"${out}"
    fi
    ;;
  report) cat "${STUB_PERF_REPORT}" ;;
  *) exit 1 ;;
esac
EOF

cat >"${STUB_BIN}/git" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_LOG}/git.argv"
case "$*" in
  *"rev-parse HEAD"*) printf '%s\n' "${STUB_GIT_HEAD}" ;;
  *) exit 1 ;;
esac
EOF

cat >"${STUB_BIN}/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s | RUSTFLAGS=%s\n' "$*" "${RUSTFLAGS:-}" >>"${STUB_LOG}/cargo.calls"
sub=$1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-dir) td=$2; shift ;;
    -p) pkg=$2; shift ;;
  esac
  shift
done
case "${sub}" in
  build) mkdir -p "${td}/release" && printf 'stub binary\n' >"${td}/release/ravel-server" ;;
  rustc) mkdir -p "${td}/release/deps" && cp "${STUB_IR}" "${td}/release/deps/${pkg//-/_}-0123abcd.ll" ;;
  *) exit 1 ;;
esac
EOF

cat >"${STUB_BIN}/uname" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == "-s" && -n "${STUB_UNAME_S:-}" ]]; then
  printf '%s\n' "${STUB_UNAME_S}"
  exit 0
fi
exec "${REAL_UNAME}" "$@"
EOF

chmod +x "${STUB_BIN}"/*
ln -s "${STUB_BIN}/llvm-profgen" "${SYSROOT_BIN}/llvm-profgen"
ln -s "${STUB_BIN}/llvm-profdata" "${SYSROOT_BIN}/llvm-profdata"

# --- fixtures --------------------------------------------------------------

# Twenty hot symbols of the binary. The first is DataFusion's, the second
# names ravel_query, so ravel_query is the hottest Ravel crate.
SYMS=()
SYMS+=("_ZN10datafusion8physical4hash17h0000000000000001E")
SYMS+=("_ZN11ravel_query4scan4fold17h0000000000000002E")
for i in $(seq 3 20); do
  SYMS+=("_ZN9ravel_sql4exec5step${i}17h00000000000000${i}E")
done

# perf report with trailing "-  -" columns and clone suffixes on the symbols.
REPORT="${TMP}/perf-report.txt"
{
  echo "# Samples: 120K of event 'cpu/branch-brs/'"
  echo "#"
  echo "# Overhead  Shared Object  Symbol"
  echo "#"
  for i in "${!SYMS[@]}"; do
    suffix=""
    case "$i" in
      0) suffix=".llvm.1234" ;;
      1) suffix=".constprop.0" ;;
      2) suffix=".isra.0.part.1" ;;
      3) suffix=".cold" ;;
      4) suffix=".llvm.987654321" ;;
    esac
    printf '%8.2f%%  ravel-server  [.] %s%s  -  -\n' "$((30 - i)).5" "${SYMS[$i]}" "${suffix}"
    if [[ "$i" == 5 ]]; then
      printf '    1.00%%  libc.so.6     [.] __memcpy_avx_unaligned_erms  -  -\n'
    fi
  done
  printf '    0.10%%  ravel-server  [.] _ZN9ravel_sql4cold9neverseen17h0000000000000099E  -  -\n'
} >"${REPORT}"

# llvm-profdata show listings: every symbol, every symbol but one, none.
SHOW_ALL="${TMP}/show-all.txt"
SHOW_MISSING="${TMP}/show-missing.txt"
SHOW_NONE="${TMP}/show-none.txt"
MISSING_SYM="${SYMS[6]}"
{
  for s in "${SYMS[@]}"; do
    printf 'Function: %s: 12345, 67, 8 sampled lines\n' "$s"
  done
  printf 'Function: %s.__uniq.42: 10, 1, 1 sampled lines\n' "${SYMS[2]}"
} >"${SHOW_ALL}"
grep -vF "Function: ${MISSING_SYM}:" "${SHOW_ALL}" >"${SHOW_MISSING}"
printf 'Functions sampled: 0\nMaximum function count: 0\n' >"${SHOW_NONE}"

IR_GOOD="${TMP}/ir-good.ll"
IR_NO_SUMMARY="${TMP}/ir-no-summary.ll"
IR_ZERO_COUNTS="${TMP}/ir-zero.ll"
cat >"${IR_GOOD}" <<'EOF'
define void @f() !prof !1 { ret void }
!1 = !{!"function_entry_count", i64 0}
!2 = !{!"function_entry_count", i64 4521}
!3 = !{i32 1, !"ProfileSummary", !4}
!4 = !{!"ProfileFormat", !"SampleProfile"}
EOF
cat >"${IR_NO_SUMMARY}" <<'EOF'
define void @f() { ret void }
!1 = !{!"function_entry_count", i64 4521}
EOF
cat >"${IR_ZERO_COUNTS}" <<'EOF'
define void @f() !prof !1 { ret void }
!1 = !{!"function_entry_count", i64 0}
!3 = !{i32 1, !"ProfileSummary", !4}
!4 = !{!"ProfileFormat", !"SampleProfile"}
EOF

RECORD_KEYS=(source_sha binary_sha256 rustc_version llvm_major llvm_profgen_version
  perf_event seconds host cpu_model recorded_at profile_sha256 hottest_ravel_crate)
BUILD_KEYS=(source_sha binary_sha256 rustc_version features rustflags)
for k in "${RECORD_KEYS[@]}"; do BUILD_KEYS+=("profile_${k}"); done

DOCKER_FEATURES="$(sed -n 's/.*cargo build .*-p ravel-server .*--features \([^ ]*\).*/\1/p' "${HERE}/../Dockerfile")"

# --- harness ---------------------------------------------------------------

CASE_N=0
LOG=""
OUT=""
CODE=0

# run <env assignments...> -- <script args...>; sets OUT, CODE, LOG.
run() {
  CASE_N=$((CASE_N + 1))
  LOG="${TMP}/log-${CASE_N}"
  mkdir -p "${LOG}"
  local envs=()
  while [[ $# -gt 0 && "$1" != "--" ]]; do
    envs+=("$1")
    shift
  done
  shift
  CODE=0
  OUT=$(cd "${RUN_CWD:-${TMP}}" && env -u LLVM_PROFGEN -u LLVM_PROFDATA -u RUSTFLAGS -u CARGO_TARGET_DIR \
    PATH="${STUB_BIN}:${PATH}" STUB_LOG="${LOG}" STUB_SYSROOT="${SYSROOT}" \
    STUB_GIT_HEAD="${HEAD_SHA}" STUB_PERF_REPORT="${REPORT}" \
    STUB_PROFDATA_SHOW="${SHOW_ALL}" STUB_IR="${IR_GOOD}" REAL_UNAME="${REAL_UNAME}" \
    "${envs[@]}" bash "${SCRIPT}" "$@" 2>&1) || CODE=$?
}

ok() {
  passes=$((passes + 1))
  echo "ok   $1"
}

fail() {
  fails=$((fails + 1))
  echo "FAIL $1"
  shift
  printf '     %s\n' "$@"
  printf '%s\n' "${OUT}" | sed 's/^/     | /'
}

expect_code() {
  local name=$1 want=$2
  if [[ "${CODE}" == "${want}" ]]; then
    return 0
  fi
  fail "${name}" "exit ${CODE}, want ${want}"
  return 1
}

expect_out() {
  local name=$1 needle=$2
  if [[ "${OUT}" == *"${needle}"* ]]; then
    return 0
  fi
  fail "${name}" "output lacks: ${needle}"
  return 1
}

# every key exactly once and nothing else
expect_keys() {
  local name=$1 file=$2
  shift 2
  local k n bad=()
  [[ -f "${file}" ]] || { fail "${name}" "no ${file}"; return 1; }
  for k in "$@"; do
    n=$(grep -c "^${k}=" "${file}")
    [[ "${n}" == 1 ]] || bad+=("${k} appears ${n} times")
  done
  n=$(wc -l <"${file}")
  [[ "${n}" == "$#" ]] || bad+=("${n} lines, want $#")
  if [[ ${#bad[@]} -gt 0 ]]; then
    fail "${name}" "${bad[@]}"
    return 1
  fi
}

record_args() {
  printf '%s\n' --pid 4242 --binary "$1" --seconds 30 --out "$2"
}

BINARY="${TMP}/bin/ravel-server"
mkdir -p "${TMP}/bin"
printf 'unstripped stub binary\n' >"${BINARY}"

# --- check-tools -----------------------------------------------------------

n="check-tools: matching majors pass"
run -- check-tools
expect_code "$n" 0 && expect_out "$n" "llvm-profgen: ${SYSROOT_BIN}/llvm-profgen" \
  && expect_out "$n" "llvm-profdata: ${SYSROOT_BIN}/llvm-profdata" && ok "$n"

n="check-tools: mismatched majors exit 3 naming both"
run STUB_PROFGEN_LLVM=22.1.8 -- check-tools
expect_code "$n" 3 && expect_out "$n" "is LLVM 22, rustc is LLVM 23" && ok "$n"

n="check-tools: LLVM_PROFGEN wins over the sysroot"
run LLVM_PROFGEN="${STUB_BIN}/llvm-profgen" -- check-tools
expect_code "$n" 0 && expect_out "$n" "llvm-profgen: ${STUB_BIN}/llvm-profgen" && ok "$n"

n="check-tools: no llvm-profgen anywhere exits 3"
run STUB_SYSROOT="${TMP}/empty-sysroot" STUB_RUSTC_LLVM=99.0.1 -- check-tools
expect_code "$n" 3 && expect_out "$n" "no llvm-profgen" && expect_out "$n" "llvm-99" && ok "$n"

# --- record ----------------------------------------------------------------

n="record: trailing columns and clone suffixes resolve 20 of 20"
out_dir="${TMP}/rec-ok"
mapfile -t args < <(record_args "${BINARY}" "${out_dir}")
run -- record "${args[@]}"
if expect_code "$n" 0 && expect_out "$n" "resolution 20 of 20"; then
  if grep -q '^perf_event=cpu/branch-brs/$' "${out_dir}/provenance.txt" \
    && grep -q '^hottest_ravel_crate=ravel_query$' "${out_dir}/provenance.txt" \
    && grep -q "^source_sha=${HEAD_SHA}\$" "${out_dir}/provenance.txt" \
    && grep -q '^seconds=30$' "${out_dir}/provenance.txt" \
    && grep -q '^llvm_major=23$' "${out_dir}/provenance.txt" \
    && grep -qx -- "--perfdata=${out_dir}/perf.data --binary=${BINARY} --output=${out_dir}/profile.prof" "${LOG}/llvm-profgen.argv"; then
    ok "$n"
  else
    fail "$n" "provenance or llvm-profgen argv wrong:" "$(cat "${out_dir}/provenance.txt" "${LOG}/llvm-profgen.argv")"
  fi
fi

n="record: provenance.txt has every key exactly once"
expect_keys "$n" "${out_dir}/provenance.txt" "${RECORD_KEYS[@]}" && ok "$n"

n="record: a symbol absent from the profile exits 4 naming it"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-missing")
run STUB_PROFDATA_SHOW="${SHOW_MISSING}" -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "1 of 20 top symbols missing" && expect_out "$n" "  ${MISSING_SYM}" \
  && { [[ ! -e "${TMP}/rec-missing/provenance.txt" ]] || { fail "$n" "provenance.txt written on failure"; false; }; } && ok "$n"

n="record: an empty perf.data falls through to the next event"
out_dir="${TMP}/rec-fallback"
mapfile -t args < <(record_args "${BINARY}" "${out_dir}")
run STUB_PERF_FAIL_EVENTS="cpu/branch-brs/" -- record "${args[@]}"
if expect_code "$n" 0 && expect_out "$n" "event cpu/branch-brs/ wrote no samples"; then
  if grep -q '^perf_event=br_inst_retired.near_taken:u$' "${out_dir}/provenance.txt" \
    && [[ "$(grep -c '^record ' "${LOG}/perf.argv")" == 2 ]]; then
    ok "$n"
  else
    fail "$n" "perf_event not the second event, or not exactly two perf record calls:" "$(cat "${out_dir}/provenance.txt" "${LOG}/perf.argv")"
  fi
fi

n="record: no event writes samples exits 6"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-nopmu")
run STUB_PERF_FAIL_EVENTS="cpu/branch-brs/ br_inst_retired.near_taken:u cycles:u" -- record "${args[@]}"
expect_code "$n" 6 && expect_out "$n" "no perf event wrote samples" && ok "$n"

n="record: --event is the only event tried"
out_dir="${TMP}/rec-event"
mapfile -t args < <(record_args "${BINARY}" "${out_dir}")
run -- record "${args[@]}" --event cycles:u
if expect_code "$n" 0 && grep -q '^perf_event=cycles:u$' "${out_dir}/provenance.txt" \
  && [[ "$(grep -c '^record ' "${LOG}/perf.argv")" == 1 ]]; then
  ok "$n"
else
  [[ "${CODE}" == 0 ]] && fail "$n" "event or perf call count wrong"
fi

n="record: a non-Linux host exits 6"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-darwin")
run STUB_UNAME_S=Darwin -- record "${args[@]}"
expect_code "$n" 6 && expect_out "$n" "needs Linux x86_64" \
  && { [[ ! -e "${LOG}/perf.argv" ]] || { fail "$n" "perf ran"; false; }; } && ok "$n"

# --- build -----------------------------------------------------------------

PROFILE_DIR="${TMP}/rec-ok"
TARGET="${TMP}/target"

n="build: missing profile exits 4"
run -- build --profile "${TMP}/nope/profile.prof" --target-dir "${TARGET}"
expect_code "$n" 4 && expect_out "$n" "does not exist" && ok "$n"

n="build: empty profile exits 4"
: >"${TMP}/empty.prof"
run -- build --profile "${TMP}/empty.prof" --target-dir "${TARGET}"
expect_code "$n" 4 && expect_out "$n" "is empty" && ok "$n"

n="build: a profile with no functions exits 4"
run STUB_PROFDATA_SHOW="${SHOW_NONE}" -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TARGET}"
expect_code "$n" 4 && expect_out "$n" "lists no functions" \
  && { [[ ! -e "${LOG}/cargo.calls" ]] || { fail "$n" "cargo ran"; false; }; } && ok "$n"

n="build: IR with no SampleProfile summary exits 5"
run STUB_IR="${IR_NO_SUMMARY}" -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-nosum"
expect_code "$n" 5 && expect_out "$n" "no SampleProfile summary" && expect_out "$n" "do not use" \
  && { [[ ! -e "${TMP}/target-nosum/release/ravel-server.provenance.txt" ]] || { fail "$n" "provenance written"; false; }; } && ok "$n"

n="build: IR with only zero entry counts exits 5"
run STUB_IR="${IR_ZERO_COUNTS}" -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-zero"
expect_code "$n" 5 && expect_out "$n" "no nonzero function_entry_count" && ok "$n"

n="build: caller RUSTFLAGS reach cargo with the profile flag appended"
# A relative --profile, so the case also proves the flag carries an absolute path.
RUN_CWD="${TMP}" run RUSTFLAGS="-Cfoo" -- build --profile rec-ok/profile.prof --target-dir "${TMP}/target-flags"
want="RUSTFLAGS=-Cfoo -Cprofile-sample-use=${PROFILE_DIR}/profile.prof"
if expect_code "$n" 0; then
  if [[ "$(grep -c "^build .* | ${want}\$" "${LOG}/cargo.calls")" == 1 ]] \
    && [[ "$(grep -c "^rustc .* | ${want}\$" "${LOG}/cargo.calls")" == 1 ]]; then
    ok "$n"
  else
    fail "$n" "want both cargo calls with: ${want}" "$(cat "${LOG}/cargo.calls")"
  fi
fi

n="build: happy path exits 0 and writes every provenance key once"
TGT="${TMP}/target-ok"
run -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TGT}"
if expect_code "$n" 0 && expect_keys "$n" "${TGT}/release/ravel-server.provenance.txt" "${BUILD_KEYS[@]}"; then
  pv="${TGT}/release/ravel-server.provenance.txt"
  if [[ "${OUT}" == *warning* ]]; then
    fail "$n" "warned with matching source_sha"
  elif ! grep -qx "^build --release --locked -p ravel-server --features ${DOCKER_FEATURES} --target-dir ${TGT} | RUSTFLAGS=-Cprofile-sample-use=${PROFILE_DIR}/profile.prof" "${LOG}/cargo.calls"; then
    fail "$n" "cargo build call wrong (want Dockerfile features ${DOCKER_FEATURES}):" "$(cat "${LOG}/cargo.calls")"
  elif ! grep -q "^rustc --release --locked -p ravel-query --lib --target-dir ${TGT} -- --emit=llvm-ir | " "${LOG}/cargo.calls"; then
    fail "$n" "IR not emitted for the provenance's hottest crate:" "$(cat "${LOG}/cargo.calls")"
  elif ! grep -qx "features=${DOCKER_FEATURES}" "${pv}" || ! grep -qx "profile_hottest_ravel_crate=ravel_query" "${pv}" \
    || ! grep -qx "binary_sha256=$(sha256sum "${TGT}/release/ravel-server" | cut -d ' ' -f 1)" "${pv}"; then
    fail "$n" "provenance values wrong:" "$(cat "${pv}")"
  else
    ok "$n"
  fi
fi

n="build: --features overrides the Dockerfile set"
run -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-feat" --features sql
expect_code "$n" 0 && { grep -q "^build .* --features sql --target-dir " "${LOG}/cargo.calls" || { fail "$n" "features not passed"; false; }; } && ok "$n"

n="build: a stale provenance source_sha warns and still exits 0"
run STUB_GIT_HEAD="${OTHER_SHA}" -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-drift"
expect_code "$n" 0 && expect_out "$n" "warning: profile recorded at source ${HEAD_SHA}, building ${OTHER_SHA}" \
  && [[ -f "${TMP}/target-drift/release/ravel-server.provenance.txt" ]] && ok "$n"

# --- usage -----------------------------------------------------------------

n="usage: unknown subcommand exits 2"
run -- frobnicate
expect_code "$n" 2 && expect_out "$n" "unknown subcommand frobnicate" && ok "$n"

n="usage: record without --out exits 2"
run -- record --pid 1 --binary "${BINARY}" --seconds 5
expect_code "$n" 2 && expect_out "$n" "missing required --out" && ok "$n"

n="usage: build without --profile exits 2"
run -- build
expect_code "$n" 2 && expect_out "$n" "missing required --profile" && ok "$n"

n="usage: each subcommand's --help exits 0"
helps=0
for sub in check-tools record build; do
  run -- "${sub}" --help
  [[ "${CODE}" == 0 && "${OUT}" == *"Usage: scripts/build-pgo.sh ${sub}"* ]] && helps=$((helps + 1))
done
if [[ "${helps}" == 3 ]]; then ok "$n"; else fail "$n" "${helps} of 3 help texts"; fi

echo
echo "${passes} passed, ${fails} failed"
[[ "${fails}" == 0 ]]
