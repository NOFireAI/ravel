#!/usr/bin/env bash
# Cases for build-pgo.sh (issue #4527). Runs the real script with stub perf,
# llvm-profgen, llvm-profdata, rustc, cargo, git and uname first on PATH,
# each writing canned output and recording its argv, so no case needs a PMU,
# an LLVM install or a build, and every case sees Linux x86_64 unless it
# stubs another host.
#
# Run: bash scripts/build-pgo.test.sh
# BUILD_PGO_SCRIPT=<path> runs the cases against another copy of the script
# (a mutant for a mutation check); it must sit in scripts/ so that its repo
# root is this one.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${BUILD_PGO_SCRIPT:-${HERE}/build-pgo.sh}"
TMP="$(cd "$(mktemp -d "${TMPDIR:-/tmp}/build-pgo-test.XXXXXX")" && pwd)"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0
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
[[ -z "${STUB_PROFGEN_FAIL:-}" ]] || exit 1
if [[ -n "${STUB_PROFGEN_EMPTY:-}" ]]; then
  : >"${out}"
else
  printf 'stub sample profile\n' >"${out}"
fi
EOF

cat >"${STUB_BIN}/llvm-profdata" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_LOG}/llvm-profdata.argv"
cat "${STUB_PROFDATA_SHOW}"
EOF

# perf record without -p is the script's event probe; with -p it is the
# recording. STUB_PERF_UNSUPPORTED_EVENTS fails the probe as a host without
# that event does, STUB_PERF_PARANOID fails it as a perf_event_paranoid
# refusal does, STUB_PERF_RECORD_RC fails the recording, and
# STUB_PERF_SAMPLES sets the SAMPLE events count of perf report --stats.
# The recording always writes a non-empty perf.data, as real perf writes its
# header with zero samples.
cat >"${STUB_BIN}/perf" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_LOG}/perf.argv"
case "$1" in
  record)
    pid=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        -e) ev=$2; shift ;;
        -o) out=$2; shift ;;
        -p) pid=$2; shift ;;
      esac
      shift
    done
    if [[ -z "${pid}" ]]; then
      if [[ -n "${STUB_PERF_PARANOID:-}" ]]; then
        printf 'Access to performance monitoring and observability operations is limited.\nperf_event_paranoid setting is 4:\nError:\nFailure to open any events for recording.\n' >&2
        exit 255
      fi
      if [[ " ${STUB_PERF_UNSUPPORTED_EVENTS:-} " == *" ${ev} "* ]]; then
        printf 'event syntax error: %s\n' "${ev}" >&2
        exit 129
      fi
      printf 'probe perf data\n' >"${out}"
      exit 0
    fi
    printf 'stub perf data header\n' >"${out}"
    if [[ -n "${STUB_PERF_RECORD_RC:-}" ]]; then
      printf "Error:\nCouldn't attach to pid %s\n" "${pid}" >&2
      exit "${STUB_PERF_RECORD_RC}"
    fi
    ;;
  report)
    if [[ " $* " == *" --stats "* ]]; then
      printf 'Aggregated stats:\n           TOTAL events:     %s\n            MMAP events:         12\n          SAMPLE events:     %s\n' \
        "$(( ${STUB_PERF_SAMPLES:-120000} + 12 ))" "${STUB_PERF_SAMPLES:-120000}"
    else
      cat "${STUB_PERF_REPORT}"
    fi
    ;;
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
  build)
    [[ -z "${STUB_CARGO_BUILD_FAIL:-}" ]] || exit 101
    mkdir -p "${td}/release" && printf 'stub binary\n' >"${td}/release/ravel-server"
    ;;
  rustc)
    [[ -z "${STUB_CARGO_RUSTC_FAIL:-}" ]] || exit 101
    [[ -n "${STUB_CARGO_NO_IR:-}" ]] && exit 0
    mkdir -p "${td}/release/deps" && cp "${STUB_IR}" "${td}/release/deps/${pkg//-/_}-0123abcd.ll"
    ;;
  *) exit 1 ;;
esac
EOF

cat >"${STUB_BIN}/uname" <<'EOF'
#!/usr/bin/env bash
s=${STUB_UNAME_S:-Linux}
m=${STUB_UNAME_M:-x86_64}
case "$*" in
  -s) printf '%s\n' "${s}" ;;
  -m) printf '%s\n' "${m}" ;;
  -srm) printf '%s 6.0.0-stub %s\n' "${s}" "${m}" ;;
  *) exit 1 ;;
esac
EOF

chmod +x "${STUB_BIN}"/*
ln -s "${STUB_BIN}/llvm-profgen" "${SYSROOT_BIN}/llvm-profgen"
ln -s "${STUB_BIN}/llvm-profdata" "${SYSROOT_BIN}/llvm-profdata"

# --- fixtures --------------------------------------------------------------

# write_report <file> <symbol...>: a perf report of the binary's symbols,
# hottest first, with trailing "-  -" columns, clone suffixes on the first
# five, a libc row among them and a cold ravel-server row past the top 20.
write_report() {
  local file=$1
  shift
  local syms=("$@") i suffix
  {
    echo "# Samples: 120K of event 'cpu/branch-brs/'"
    echo "#"
    echo "# Overhead  Shared Object  Symbol"
    echo "#"
    for i in "${!syms[@]}"; do
      suffix=""
      case "$i" in
        0) suffix=".llvm.1234" ;;
        1) suffix=".constprop.0" ;;
        2) suffix=".isra.0.part.1" ;;
        3) suffix=".cold" ;;
        4) suffix=".llvm.987654321" ;;
      esac
      printf '%8.2f%%  ravel-server  [.] %s%s  -  -\n' "$((30 - i)).5" "${syms[$i]}" "${suffix}"
      if [[ "$i" == 5 ]]; then
        printf '    1.00%%  libc.so.6     [.] __memcpy_avx_unaligned_erms  -  -\n'
      fi
    done
    printf '    0.10%%  ravel-server  [.] _ZN9ravel_sql4cold9neverseen17h0000000000000099E  -  -\n'
  } >"${file}"
}

# write_show <file> <symbol...>: an llvm-profdata listing with every symbol
# sampled.
write_show() {
  local file=$1 s
  shift
  for s in "$@"; do
    printf 'Function: %s: 12345, 67, 8 sampled lines\n' "$s"
  done >"${file}"
}

# Twenty hot symbols of the binary. The first is DataFusion's, the second
# names ravel_query, so ravel_query is the hottest Ravel crate.
SYMS=()
SYMS+=("_ZN10datafusion8physical4hash17h0000000000000001E")
SYMS+=("_ZN11ravel_query4scan4fold17h0000000000000002E")
for i in $(seq 3 20); do
  SYMS+=("_ZN9ravel_sql4exec5step${i}17h00000000000000${i}E")
done

REPORT="${TMP}/perf-report.txt"
write_report "${REPORT}" "${SYMS[@]}"

# The main listing names SYMS[2] only with a profile-side suffix of its own
# (.__uniq.42), which must still resolve.
SHOW_ALL="${TMP}/show-all.txt"
{
  for i in "${!SYMS[@]}"; do
    if [[ "$i" == 2 ]]; then
      printf 'Function: %s.__uniq.42: 10, 1, 1 sampled lines\n' "${SYMS[$i]}"
    else
      printf 'Function: %s: 12345, 67, 8 sampled lines\n' "${SYMS[$i]}"
    fi
  done
} >"${SHOW_ALL}"

# Every symbol but one.
SHOW_MISSING="${TMP}/show-missing.txt"
MISSING_SYM="${SYMS[6]}"
grep -vF "Function: ${MISSING_SYM}:" "${SHOW_ALL}" >"${SHOW_MISSING}"

# One symbol listed with zero total samples.
SHOW_ZERO_SYM="${TMP}/show-zero-sym.txt"
ZERO_SYM="${SYMS[9]}"
{
  grep -vF "Function: ${ZERO_SYM}:" "${SHOW_ALL}"
  printf 'Function: %s: 0, 0, 0 sampled lines\n' "${ZERO_SYM}"
} >"${SHOW_ZERO_SYM}"

# One symbol present only as a longer name that starts with it.
SHOW_PREFIX="${TMP}/show-prefix.txt"
PREFIX_SYM="${SYMS[7]}"
{
  grep -vF "Function: ${PREFIX_SYM}:" "${SHOW_ALL}"
  printf 'Function: %sx: 12345, 67, 8 sampled lines\n' "${PREFIX_SYM}"
} >"${SHOW_PREFIX}"

SHOW_NONE="${TMP}/show-none.txt"
printf 'Functions sampled: 0\nMaximum function count: 0\n' >"${SHOW_NONE}"

# A listing of the size a real --all-functions listing of ravel-server has:
# the matching lines come first, then 200,000 more.
SHOW_BIG="${TMP}/show-big.txt"
{
  cat "${SHOW_ALL}"
  awk 'BEGIN { for (i = 1; i <= 200000; i++) printf "Function: _ZN9ravel_sql6filler%dE: 1, 0, 1 sampled lines\n", i }'
} >"${SHOW_BIG}"

# The top 20 hold no DataFusion symbol.
NODF_SYMS=("_ZN9ravel_sql4exec5step017h0000000000000000E" "${SYMS[@]:1}")
REPORT_NODF="${TMP}/perf-report-nodf.txt"
SHOW_NODF="${TMP}/show-nodf.txt"
write_report "${REPORT_NODF}" "${NODF_SYMS[@]}"
write_show "${SHOW_NODF}" "${NODF_SYMS[@]}"

# The top 20 hold no Ravel symbol.
NORAVEL_SYMS=()
for i in $(seq 1 20); do
  NORAVEL_SYMS+=("_ZN10datafusion8physical5step${i}17h00000000000000${i}E")
done
REPORT_NORAVEL="${TMP}/perf-report-noravel.txt"
SHOW_NORAVEL="${TMP}/show-noravel.txt"
write_report "${REPORT_NORAVEL}" "${NORAVEL_SYMS[@]}"
write_show "${SHOW_NORAVEL}" "${NORAVEL_SYMS[@]}"

# The hottest Ravel symbol is a trait impl naming ravel_sql and ravel_query;
# the longer crate name, ravel_query, is the one taken.
GEN_SYMS=("${SYMS[@]}")
GEN_SYMS[1]='_ZN93_$LT$9ravel_sql4exec4Plan$u20$as$u20$11ravel_query4scan4Fold$GT$4fold17h0000000000000002E'
REPORT_GEN="${TMP}/perf-report-gen.txt"
SHOW_GEN="${TMP}/show-gen.txt"
write_report "${REPORT_GEN}" "${GEN_SYMS[@]}"
write_show "${SHOW_GEN}" "${GEN_SYMS[@]}"

# IR for ravel_query. TOP_DEF is the define of SYMS[1], a top-20 symbol.
TOP_DEF="define void @${SYMS[1]}() #0 !prof !1 { ret void }"
TOP_DEF_NOPROF="define void @${SYMS[1]}() #0 { ret void }"
OTHER_DEF="define void @_ZN11ravel_query5other17h00000000000000ffE() #0 !prof !2 { ret void }"
SUMMARY='!3 = !{i32 1, !"ProfileSummary", !4}
!4 = !{!"ProfileFormat", !"SampleProfile"}'
IR_GOOD="${TMP}/ir-good.ll"
IR_NO_SUMMARY="${TMP}/ir-no-summary.ll"
IR_ZERO_COUNTS="${TMP}/ir-zero.ll"
IR_NO_TOP_PROF="${TMP}/ir-no-top-prof.ll"
printf '%s\n' "${OTHER_DEF}" "${TOP_DEF}" '!1 = !{!"function_entry_count", i64 0}' \
  '!2 = !{!"function_entry_count", i64 4521}' "${SUMMARY}" >"${IR_GOOD}"
printf '%s\n' "${TOP_DEF}" '!1 = !{!"function_entry_count", i64 4521}' >"${IR_NO_SUMMARY}"
printf '%s\n' "${TOP_DEF}" '!1 = !{!"function_entry_count", i64 0}' "${SUMMARY}" >"${IR_ZERO_COUNTS}"
printf '%s\n' "${TOP_DEF_NOPROF}" "${OTHER_DEF}" '!2 = !{!"function_entry_count", i64 4521}' \
  "${SUMMARY}" >"${IR_NO_TOP_PROF}"

RECORD_KEYS=(source_sha binary_sha256 rustc_version llvm_major llvm_profgen_version
  perf_event seconds host cpu_model recorded_at profile_sha256 hottest_ravel_crate top_symbols)
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
    STUB_PROFDATA_SHOW="${SHOW_ALL}" STUB_IR="${IR_GOOD}" \
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
  [[ "${n// /}" == "$#" ]] || bad+=("${n// /} lines, want $#")
  if [[ ${#bad[@]} -gt 0 ]]; then
    fail "${name}" "${bad[@]}"
    return 1
  fi
}

# No record output a later build could pick up.
expect_no_record_output() {
  local name=$1 dir=$2
  if [[ -e "${dir}/provenance.txt" || -e "${dir}/top-symbols.txt" ]]; then
    fail "${name}" "provenance.txt or top-symbols.txt written on failure"
    return 1
  fi
}

# After an exit 1 or 5 past the build, no binary is left at the plain path.
expect_moved_aside() {
  local name=$1 tgt=$2
  if [[ -e "${tgt}/release/ravel-server" ]]; then
    fail "${name}" "${tgt}/release/ravel-server still exists"
    return 1
  fi
  if [[ ! -f "${tgt}/release/ravel-server.pgo-unproven" ]]; then
    fail "${name}" "no ${tgt}/release/ravel-server.pgo-unproven"
    return 1
  fi
  if [[ -e "${tgt}/release/ravel-server.provenance.txt" ]]; then
    fail "${name}" "provenance written"
    return 1
  fi
  expect_out "${name}" "moved the binary to ${tgt}/release/ravel-server.pgo-unproven"
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

n="check-tools: LLVM_PROFDATA wins over the sysroot"
run LLVM_PROFDATA="${STUB_BIN}/llvm-profdata" -- check-tools
expect_code "$n" 0 && expect_out "$n" "llvm-profdata: ${STUB_BIN}/llvm-profdata" && ok "$n"

n="check-tools: no llvm-profdata anywhere exits 3"
run LLVM_PROFGEN="${STUB_BIN}/llvm-profgen" STUB_SYSROOT="${TMP}/empty-sysroot" -- check-tools
expect_code "$n" 3 && expect_out "$n" "no llvm-profdata" && ok "$n"

n="check-tools: no llvm-profgen anywhere exits 3"
run STUB_SYSROOT="${TMP}/empty-sysroot" STUB_RUSTC_LLVM=99.0.1 -- check-tools
expect_code "$n" 3 && expect_out "$n" "no llvm-profgen" && expect_out "$n" "llvm-99" && ok "$n"

n="check-tools: a non-Linux host exits 6"
run STUB_UNAME_S=Darwin -- check-tools
expect_code "$n" 6 && expect_out "$n" "check-tools: needs Linux x86_64, this host is Darwin x86_64" \
  && { [[ ! -e "${LOG}/rustc.argv" ]] || { fail "$n" "rustc ran"; false; }; } && ok "$n"

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
    && grep -q '^host=Linux 6.0.0-stub x86_64$' "${out_dir}/provenance.txt" \
    && grep -q '^top_symbols=top-symbols.txt$' "${out_dir}/provenance.txt" \
    && grep -qx -- "--perfdata=${out_dir}/perf.data --binary=${BINARY} --output=${out_dir}/profile.prof" "${LOG}/llvm-profgen.argv"; then
    ok "$n"
  else
    fail "$n" "provenance or llvm-profgen argv wrong:" "$(cat "${out_dir}/provenance.txt" "${LOG}/llvm-profgen.argv")"
  fi
fi

n="record: provenance.txt has every key exactly once"
expect_keys "$n" "${out_dir}/provenance.txt" "${RECORD_KEYS[@]}" && ok "$n"

n="record: top-symbols.txt is the 20 stripped symbols, hottest first"
if [[ "$(cat "${out_dir}/top-symbols.txt" 2>/dev/null)" == "$(printf '%s\n' "${SYMS[@]}")" ]]; then
  ok "$n"
else
  fail "$n" "top-symbols.txt:" "$(cat "${out_dir}/top-symbols.txt" 2>&1)"
fi

n="record: a symbol absent from the profile exits 4 naming it"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-missing")
run STUB_PROFDATA_SHOW="${SHOW_MISSING}" -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "1 of 20 top symbols missing" && expect_out "$n" "  ${MISSING_SYM}" \
  && expect_no_record_output "$n" "${TMP}/rec-missing" && ok "$n"

n="record: a top symbol with zero samples in the profile exits 4 naming it"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-zero-sym")
run STUB_PROFDATA_SHOW="${SHOW_ZERO_SYM}" -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "1 of 20 top symbols have zero samples" && expect_out "$n" "  ${ZERO_SYM}" \
  && expect_no_record_output "$n" "${TMP}/rec-zero-sym" && ok "$n"

n="record: a profile function that only starts with a top symbol does not resolve it"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-prefix")
run STUB_PROFDATA_SHOW="${SHOW_PREFIX}" -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "1 of 20 top symbols missing" && expect_out "$n" "  ${PREFIX_SYM}" && ok "$n"

n="record: a top 20 with no datafusion symbol exits 4"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-nodf")
run STUB_PERF_REPORT="${REPORT_NODF}" STUB_PROFDATA_SHOW="${SHOW_NODF}" -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "hold no DataFusion (datafusion) symbol" \
  && expect_no_record_output "$n" "${TMP}/rec-nodf" && ok "$n"

n="record: a top 20 with no ravel_ symbol exits 4"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-noravel")
run STUB_PERF_REPORT="${REPORT_NORAVEL}" STUB_PROFDATA_SHOW="${SHOW_NORAVEL}" -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "hold no Ravel (ravel_) symbol" \
  && expect_no_record_output "$n" "${TMP}/rec-noravel" && ok "$n"

n="record: llvm-profgen failing exits 4"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-pgfail")
run STUB_PROFGEN_FAIL=1 -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "llvm-profgen failed" && expect_no_record_output "$n" "${TMP}/rec-pgfail" && ok "$n"

n="record: llvm-profgen writing an empty profile exits 4"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-pgempty")
run STUB_PROFGEN_EMPTY=1 -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "llvm-profgen wrote an empty profile" \
  && expect_no_record_output "$n" "${TMP}/rec-pgempty" && ok "$n"

n="record: a symbol naming two Ravel crates takes the longer name"
out_dir="${TMP}/rec-gen"
mapfile -t args < <(record_args "${BINARY}" "${out_dir}")
run STUB_PERF_REPORT="${REPORT_GEN}" STUB_PROFDATA_SHOW="${SHOW_GEN}" -- record "${args[@]}"
expect_code "$n" 0 && { grep -qx 'hottest_ravel_crate=ravel_query' "${out_dir}/provenance.txt" \
  || { fail "$n" "hottest crate:" "$(grep hottest "${out_dir}/provenance.txt")"; false; }; } && ok "$n"

n="record: an event perf cannot open falls through to the next"
out_dir="${TMP}/rec-fallback"
mapfile -t args < <(record_args "${BINARY}" "${out_dir}")
run STUB_PERF_UNSUPPORTED_EVENTS="cpu/branch-brs/" -- record "${args[@]}"
if expect_code "$n" 0 && expect_out "$n" "event cpu/branch-brs/ cannot be opened on this host"; then
  if grep -q '^perf_event=br_inst_retired.near_taken:u$' "${out_dir}/provenance.txt" \
    && [[ "$(grep -c '^record .* -p ' "${LOG}/perf.argv")" == 1 ]] \
    && [[ "$(grep -c '^record -b -e br_inst_retired.near_taken:u -p 4242 ' "${LOG}/perf.argv")" == 1 ]]; then
    ok "$n"
  else
    fail "$n" "perf_event not the second event, or not one recording with it:" "$(cat "${out_dir}/provenance.txt" "${LOG}/perf.argv")"
  fi
fi

n="record: no event perf can open exits 6"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-nopmu")
run STUB_PERF_UNSUPPORTED_EVENTS="cpu/branch-brs/ br_inst_retired.near_taken:u cycles:u" -- record "${args[@]}"
expect_code "$n" 6 && expect_out "$n" "no perf event can be opened" \
  && { [[ "$(grep -c ' -p ' "${LOG}/perf.argv")" == 0 ]] || { fail "$n" "perf recorded the pid"; false; }; } && ok "$n"

n="record: a perf_event_paranoid refusal exits 4, not 6"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-paranoid")
run STUB_PERF_PARANOID=1 -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "perf_event_paranoid" && expect_out "$n" "not a missing PMU" && ok "$n"

n="record: perf record exiting non-zero exits 4 with perf's stderr"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-perffail")
run STUB_PERF_RECORD_RC=255 -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "exited 255" && expect_out "$n" "Couldn't attach to pid 4242" \
  && { [[ ! -e "${LOG}/llvm-profgen.argv" ]] || [[ "$(grep -vc -- '--version' "${LOG}/llvm-profgen.argv")" == 0 ]] \
    || { fail "$n" "llvm-profgen ran"; false; }; } \
  && expect_no_record_output "$n" "${TMP}/rec-perffail" && ok "$n"

n="record: a non-empty perf.data with zero samples exits 4"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-nosamples")
run STUB_PERF_SAMPLES=0 -- record "${args[@]}"
expect_code "$n" 4 && expect_out "$n" "perf recorded zero samples on pid 4242" \
  && { [[ -s "${TMP}/rec-nosamples/perf.data" ]] || { fail "$n" "perf.data empty, case proves nothing"; false; }; } \
  && expect_no_record_output "$n" "${TMP}/rec-nosamples" && ok "$n"

n="record: --event is the only event tried"
out_dir="${TMP}/rec-event"
mapfile -t args < <(record_args "${BINARY}" "${out_dir}")
run -- record "${args[@]}" --event cycles:u
if expect_code "$n" 0 && grep -q '^perf_event=cycles:u$' "${out_dir}/provenance.txt" \
  && [[ "$(grep -c '^record ' "${LOG}/perf.argv")" == 2 ]] \
  && [[ "$(grep -c '^record -b -e cycles:u ' "${LOG}/perf.argv")" == 2 ]]; then
  ok "$n"
else
  [[ "${CODE}" == 0 ]] && fail "$n" "event or perf call count wrong" "$(cat "${LOG}/perf.argv")"
fi

n="record: a non-Linux host exits 6"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-darwin")
run STUB_UNAME_S=Darwin -- record "${args[@]}"
expect_code "$n" 6 && expect_out "$n" "needs Linux x86_64" \
  && { [[ ! -e "${LOG}/perf.argv" ]] || { fail "$n" "perf ran"; false; }; } && ok "$n"

n="record: an aarch64 host exits 6"
mapfile -t args < <(record_args "${BINARY}" "${TMP}/rec-arm")
run STUB_UNAME_M=aarch64 -- record "${args[@]}"
expect_code "$n" 6 && expect_out "$n" "this host is Linux aarch64" \
  && { [[ ! -e "${LOG}/perf.argv" ]] || { fail "$n" "perf ran"; false; }; } && ok "$n"

# --- build -----------------------------------------------------------------

PROFILE_DIR="${TMP}/rec-ok"
TARGET="${TMP}/target"

n="build: a non-Linux host exits 6"
run STUB_UNAME_S=Darwin -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TARGET}"
expect_code "$n" 6 && expect_out "$n" "build: needs Linux x86_64" \
  && { [[ ! -e "${LOG}/cargo.calls" ]] || { fail "$n" "cargo ran"; false; }; } && ok "$n"

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

n="build: a 200,000-line listing is accepted"
run STUB_PROFDATA_SHOW="${SHOW_BIG}" -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-big"
expect_code "$n" 0 && expect_out "$n" "profile used" && ok "$n"

n="build: a profile with no top-symbols.txt beside it exits 4"
mkdir -p "${TMP}/notop"
cp "${PROFILE_DIR}/profile.prof" "${TMP}/notop/profile.prof"
run -- build --profile "${TMP}/notop/profile.prof" --target-dir "${TARGET}"
expect_code "$n" 4 && expect_out "$n" "no ${TMP}/notop/top-symbols.txt beside the profile" \
  && { [[ ! -e "${LOG}/cargo.calls" ]] || { fail "$n" "cargo ran"; false; }; } && ok "$n"

n="build: IR with no SampleProfile summary exits 5 and moves the binary aside"
run STUB_IR="${IR_NO_SUMMARY}" -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-nosum"
expect_code "$n" 5 && expect_out "$n" "no SampleProfile summary" && expect_out "$n" "do not use" \
  && expect_moved_aside "$n" "${TMP}/target-nosum" && ok "$n"

n="build: IR with only zero entry counts exits 5 and moves the binary aside"
run STUB_IR="${IR_ZERO_COUNTS}" -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-zero"
expect_code "$n" 5 && expect_out "$n" "no nonzero function_entry_count" \
  && expect_moved_aside "$n" "${TMP}/target-zero" && ok "$n"

n="build: IR with no top-20 define carrying !prof exits 5 and moves the binary aside"
run STUB_IR="${IR_NO_TOP_PROF}" -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-noprof"
expect_code "$n" 5 && expect_out "$n" "no top-20 symbol" \
  && expect_moved_aside "$n" "${TMP}/target-noprof" && ok "$n"

n="build: no IR file exits 5 and moves the binary aside"
run STUB_CARGO_NO_IR=1 -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-noir"
expect_code "$n" 5 && expect_out "$n" "no ravel_query-*.ll" \
  && expect_moved_aside "$n" "${TMP}/target-noir" && ok "$n"

n="build: a failed cargo rustc exits 1 and moves the binary aside"
run STUB_CARGO_RUSTC_FAIL=1 -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-rustcfail"
expect_code "$n" 1 && expect_out "$n" "cargo rustc --emit=llvm-ir failed for ravel-query" \
  && expect_moved_aside "$n" "${TMP}/target-rustcfail" && ok "$n"

n="build: a failed cargo build exits 1"
run STUB_CARGO_BUILD_FAIL=1 -- build --profile "${PROFILE_DIR}/profile.prof" --target-dir "${TMP}/target-buildfail"
expect_code "$n" 1 && expect_out "$n" "build: cargo build failed" \
  && { [[ "$(grep -c '^rustc ' "${LOG}/cargo.calls")" == 0 ]] || { fail "$n" "cargo rustc ran"; false; }; } && ok "$n"

n="build: CARGO_TARGET_DIR is the target without --target-dir"
run CARGO_TARGET_DIR="${TMP}/target-env" -- build --profile "${PROFILE_DIR}/profile.prof"
expect_code "$n" 0 && { [[ -f "${TMP}/target-env/release/ravel-server.provenance.txt" ]] \
  && grep -q "^build .* --target-dir ${TMP}/target-env | " "${LOG}/cargo.calls" \
  || { fail "$n" "not built under CARGO_TARGET_DIR" "$(cat "${LOG}/cargo.calls")"; false; }; } && ok "$n"

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
  elif [[ "${OUT}" != *"top-20 symbol ${SYMS[1]} is defined with !prof"* ]]; then
    fail "$n" "did not name the top-20 define with !prof"
  elif ! grep -qx "^build --release --locked -p ravel-server --features ${DOCKER_FEATURES} --target-dir ${TGT} | RUSTFLAGS=-Cprofile-sample-use=${PROFILE_DIR}/profile.prof" "${LOG}/cargo.calls"; then
    fail "$n" "cargo build call wrong (want Dockerfile features ${DOCKER_FEATURES}):" "$(cat "${LOG}/cargo.calls")"
  elif ! grep -q "^rustc --release --locked -p ravel-query --lib --target-dir ${TGT} -- --emit=llvm-ir | " "${LOG}/cargo.calls"; then
    fail "$n" "IR not emitted for the provenance's hottest crate:" "$(cat "${LOG}/cargo.calls")"
  elif ! grep -qx "features=${DOCKER_FEATURES}" "${pv}" || ! grep -qx "profile_hottest_ravel_crate=ravel_query" "${pv}" \
    || ! grep -qx "binary_sha256=$(sha256sum "${TGT}/release/ravel-server" | cut -d ' ' -f 1)" "${pv}"; then
    fail "$n" "provenance values wrong:" "$(cat "${pv}")"
  elif [[ -e "${TGT}/release/ravel-server.pgo-unproven" ]]; then
    fail "$n" "binary moved aside on success"
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
