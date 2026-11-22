#!/usr/bin/env bash
# Opt-in sample-PGO build of ravel-server (ADR-4523 decision 4, issue #4527).
#
# R3 measured sample PGO and deferred it (docs/internal/pgo-runbook.md):
# release images keep the ordinary build, and no release or image path calls
# this script (only its own test suite and that suite's ci.yml step do). It
# exists as the reproduction of R3 and for an operator following the
# runbook. It never touches publish-images, a Dockerfile or any release path;
# it only reads the root Dockerfile for its default feature list.
#
# Linux x86_64 only: every subcommand exits 6 on any other host.
#
# Subcommands (each takes --help):
#   check-tools   find llvm-profgen and llvm-profdata, refuse an LLVM major
#                 that differs from rustc's
#   record        perf-record a running ravel-server, convert the samples to
#                 a sample profile, check that perf's top 20 symbols are in it
#                 with samples, and write top-symbols.txt and provenance.txt
#                 beside it
#   build         cargo build --release with -Cprofile-sample-use, then prove
#                 from emitted LLVM IR that the profile was used
#
# Exit codes:
#   0  success
#   1  a cargo command failed (cargo build, or the cargo rustc that emits the
#      IR; after the latter the binary is moved aside as for 5)
#   2  usage error
#   3  a tool is missing, or llvm-profgen's LLVM major differs from rustc's
#   4  record: perf record failed or was refused by perf_event_paranoid,
#      perf recorded zero samples, llvm-profgen failed or wrote an empty
#      profile, a top-20 symbol is missing from the profile or has zero
#      samples in it, or the top 20 hold no ravel_ or no datafusion symbol;
#      build: the profile is missing, empty, lists no functions, or has no
#      top-symbols.txt beside it
#   5  the build could not be shown to use the profile (no IR file, no
#      SampleProfile summary, no nonzero function_entry_count, or no top-20
#      function defined with !prof); the binary is moved to
#      <target>/release/ravel-server.pgo-unproven
#   6  unsupported host: not Linux x86_64, or (record) no perf event in the
#      list can be opened on this host
#
# Environment:
#   LLVM_PROFGEN   llvm-profgen to use, ahead of the rustup sysroot and
#                  /usr/lib/llvm-<major>/bin
#   LLVM_PROFDATA  llvm-profdata to use, ahead of the rustup sysroot
#                  (rustup's llvm-tools component)
#   RUSTFLAGS      build appends -Cprofile-sample-use=<abs> to it
#   CARGO_TARGET_DIR  where build looks for its output without --target-dir
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROG="build-pgo.sh"
DEFAULT_EVENTS=("cpu/branch-brs/" "br_inst_retired.near_taken:u" "cycles:u")
TOP_N=20

die() {
  local code=$1
  shift
  echo "${PROG}: $*" >&2
  exit "${code}"
}

usage_main() {
  cat <<'EOF'
Usage: scripts/build-pgo.sh <check-tools|record|build> [flags]
       scripts/build-pgo.sh <subcommand> --help

Opt-in sample-PGO build of ravel-server. No release build uses it
(ADR-4523 decision 4, DEFER). See docs/internal/pgo-runbook.md.
Linux x86_64 only.

Exit codes: 0 success, 1 a cargo command failed, 2 usage, 3 tool missing
or LLVM major mismatch, 4 perf failed or recorded no samples, or the
profile is empty or unresolved, 5 profile not shown used by the build
(binary moved to ravel-server.pgo-unproven), 6 unsupported host or no
usable perf event.
EOF
}

usage_check_tools() {
  cat <<'EOF'
Usage: scripts/build-pgo.sh check-tools

Linux x86_64 only (exit 6 elsewhere). Reads the LLVM major from
`rustc -vV` and finds:
  llvm-profgen   $LLVM_PROFGEN, then <rustc sysroot>/lib/rustlib/
                 x86_64-unknown-linux-gnu/bin, then /usr/lib/llvm-<major>/bin
  llvm-profdata  $LLVM_PROFDATA, then <rustc sysroot>/lib/rustlib/
                 x86_64-unknown-linux-gnu/bin (rustup's llvm-tools component)
Exits 3 if either is missing, or if llvm-profgen's LLVM major differs from
rustc's (both majors are named).
EOF
}

usage_record() {
  cat <<'EOF'
Usage: scripts/build-pgo.sh record --pid <ravel-server pid>
         --binary <unstripped ravel-server> --seconds <n> --out <dir>
         [--event <perf event>]

Linux x86_64 only (exit 6 elsewhere). Without --event it probes
cpu/branch-brs/ (AMD BRS), then br_inst_retired.near_taken:u (Intel LBR),
then cycles:u with `perf record -b -e <event> -- true`, and keeps the first
that perf can open (exit 6 if none can; exit 4 if perf names
perf_event_paranoid). It then runs `perf record -b -e <event>` on the pid
for <n> seconds. Exit 4 if perf exits non-zero (its stderr tail is shown)
or if `perf report --stats` counts zero SAMPLE events. Converts with
llvm-profgen into <dir>/profile.prof (exit 4 if empty).

Resolution check: the top 20 self-time symbols of the binary in
`perf report --sort dso,symbol`, after stripping .llvm.N, .constprop.N,
.isra.N, .part.N and .cold(.N) suffixes, must each be a function of
`llvm-profdata show --sample --all-functions` with nonzero total samples,
and must include a ravel_ symbol and a datafusion symbol. Exit 4 names the
missing and zero-sample ones.

Writes <dir>/top-symbols.txt (the 20 stripped symbols, hottest first) and
<dir>/provenance.txt: source_sha, binary_sha256, rustc_version, llvm_major,
llvm_profgen_version, perf_event, seconds, host, cpu_model, recorded_at,
profile_sha256, hottest_ravel_crate, top_symbols.
EOF
}

usage_build() {
  cat <<'EOF'
Usage: scripts/build-pgo.sh build --profile <file> [--features <list>]
         [--target-dir <dir>]

Linux x86_64 only (exit 6 elsewhere). Runs
`cargo build --release --locked -p ravel-server --features <list>` with
-Cprofile-sample-use=<absolute profile path> appended to RUSTFLAGS (the
caller's RUSTFLAGS are kept, never replaced).

--features defaults to the release image's set, read from the
`cargo build ... -p ravel-server --features` line of the root Dockerfile.

Refuses a missing or empty profile, one in which llvm-profdata lists no
functions, or one with no top-symbols.txt beside it (exit 4). After the
build it emits LLVM IR for the hottest Ravel crate named in the profile's
provenance.txt (else ravel-sql) with the same RUSTFLAGS and requires a
SampleProfile summary, a nonzero function_entry_count, and a top-20
symbol defined with !prof. Otherwise it exits 5 (1 if that cargo rustc
fails) and moves the binary to <target>/release/ravel-server.pgo-unproven.
A provenance source_sha that differs from HEAD prints a warning naming both
and the build continues.

Writes <binary>.provenance.txt: the profile's provenance lines prefixed
profile_, plus source_sha, binary_sha256, rustc_version, features,
rustflags.
EOF
}

# --- shared helpers --------------------------------------------------------

require_linux_x86_64() {
  local os arch
  os=$(uname -s)
  arch=$(uname -m)
  [[ "${os}" == "Linux" && "${arch}" == "x86_64" ]] \
    || die 6 "$1: needs Linux x86_64, this host is ${os} ${arch}"
}

rustc_llvm_major() {
  local vv
  vv=$(rustc -vV) || die 3 "rustc -vV failed"
  local major
  major=$(awk '/^LLVM version: [0-9]/ { sub(/^LLVM version: /, ""); sub(/[^0-9].*/, ""); print; exit }' <<<"${vv}")
  [[ -n "${major}" ]] || die 3 "rustc -vV printed no LLVM version line"
  printf '%s\n' "${major}"
}

sysroot_bin() {
  local sysroot
  sysroot=$(rustc --print sysroot 2>/dev/null) || return 0
  printf '%s/lib/rustlib/x86_64-unknown-linux-gnu/bin\n' "${sysroot}"
}

find_profgen() {
  local major=$1 cand dir
  if [[ -n "${LLVM_PROFGEN:-}" ]]; then
    [[ -x "${LLVM_PROFGEN}" ]] || die 3 "LLVM_PROFGEN=${LLVM_PROFGEN} is not an executable"
    printf '%s\n' "${LLVM_PROFGEN}"
    return 0
  fi
  dir=$(sysroot_bin)
  for cand in "${dir:+${dir}/llvm-profgen}" "/usr/lib/llvm-${major}/bin/llvm-profgen"; do
    if [[ -n "${cand}" && -x "${cand}" ]]; then
      printf '%s\n' "${cand}"
      return 0
    fi
  done
  die 3 "no llvm-profgen: set LLVM_PROFGEN, or install one for LLVM ${major} (rustup sysroot or apt.llvm.org llvm-${major})"
}

find_profdata() {
  local dir
  if [[ -n "${LLVM_PROFDATA:-}" ]]; then
    [[ -x "${LLVM_PROFDATA}" ]] || die 3 "LLVM_PROFDATA=${LLVM_PROFDATA} is not an executable"
    printf '%s\n' "${LLVM_PROFDATA}"
    return 0
  fi
  dir=$(sysroot_bin)
  if [[ -n "${dir}" && -x "${dir}/llvm-profdata" ]]; then
    printf '%s\n' "${dir}/llvm-profdata"
    return 0
  fi
  die 3 "no llvm-profdata: set LLVM_PROFDATA, or run: rustup component add llvm-tools"
}

profgen_version_line() {
  local out
  out=$("$1" --version 2>&1) || die 3 "$1 --version failed"
  awk '/LLVM version/ { sub(/^[[:space:]]*/, ""); print; exit }' <<<"${out}"
}

# Sets PROFGEN, PROFDATA, LLVM_MAJOR and PROFGEN_VERSION.
resolve_tools() {
  LLVM_MAJOR=$(rustc_llvm_major)
  PROFGEN=$(find_profgen "${LLVM_MAJOR}")
  PROFDATA=$(find_profdata)
  PROFGEN_VERSION=$(profgen_version_line "${PROFGEN}")
  local pg_major=""
  if [[ "${PROFGEN_VERSION}" =~ LLVM\ version\ ([0-9]+) ]]; then
    pg_major=${BASH_REMATCH[1]}
  fi
  [[ -n "${pg_major}" ]] || die 3 "${PROFGEN} --version printed no LLVM version"
  if [[ "${pg_major}" != "${LLVM_MAJOR}" ]]; then
    die 3 "llvm-profgen ${PROFGEN} is LLVM ${pg_major}, rustc is LLVM ${LLVM_MAJOR}; use an llvm-profgen for LLVM ${LLVM_MAJOR}"
  fi
}

# Strips the compiler clone suffixes until none is left.
strip_suffixes() {
  local sym=$1 prev=""
  while [[ "${sym}" != "${prev}" ]]; do
    prev=${sym}
    sym=$(printf '%s\n' "${sym}" | sed -E 's/\.(llvm|constprop|isra|part)\.[0-9]+$//; s/\.cold(\.[0-9]+)?$//')
  done
  printf '%s\n' "${sym}"
}

# The value of key in a key=value file, first occurrence.
kv_value() {
  awk -v k="$1=" 'index($0, k) == 1 { print substr($0, length(k) + 1); exit }' "$2"
}

# Workspace crate names (ravel_sql), longest first. A generic or trait-impl
# symbol can name several Ravel crates; the longest name is the one taken.
workspace_crates() {
  local manifest name
  for manifest in "${REPO_ROOT}"/crates/*/Cargo.toml "${REPO_ROOT}"/services/*/Cargo.toml; do
    [[ -f "${manifest}" ]] || continue
    name=$(awk -F '"' '/^name = "ravel-[a-z0-9-]*"$/ { print $2; exit }' "${manifest}")
    [[ -n "${name}" ]] && printf '%s\n' "${name//-/_}"
  done | awk '{ print length($0) "\t" $0 }' | sort -rn | cut -f 2
}

# The first ravel crate named in the symbol arguments, by its mangled
# length-prefixed form (9ravel_sql), which both mangling schemes carry.
hottest_crate() {
  local crates sym crate
  crates=$(workspace_crates)
  for sym in "$@"; do
    while IFS= read -r crate; do
      [[ -n "${crate}" ]] || continue
      if [[ "${sym}" == *"${#crate}${crate}"* ]]; then
        printf '%s\n' "${crate}"
        return 0
      fi
    done <<<"${crates}"
  done
  printf 'ravel_sql\n'
}

# Prints the last lines of a captured stderr file, indented.
stderr_tail() {
  [[ -s "$1" ]] || return 0
  tail -n 5 "$1" | sed 's/^/    /' >&2
}

# --- check-tools -----------------------------------------------------------

cmd_check_tools() {
  if [[ $# -gt 0 ]]; then
    case "$1" in
      -h | --help) usage_check_tools; exit 0 ;;
      *) usage_check_tools >&2; die 2 "check-tools: unknown argument $1" ;;
    esac
  fi
  require_linux_x86_64 check-tools
  resolve_tools
  echo "rustc LLVM major: ${LLVM_MAJOR}"
  echo "llvm-profgen: ${PROFGEN} (${PROFGEN_VERSION})"
  echo "llvm-profdata: ${PROFDATA}"
}

# --- record ----------------------------------------------------------------

cmd_record() {
  local pid="" binary="" seconds="" out="" event=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h | --help) usage_record; exit 0 ;;
      --pid | --binary | --seconds | --out | --event)
        [[ $# -ge 2 ]] || { usage_record >&2; die 2 "record: $1 needs a value"; }
        case "$1" in
          --pid) pid=$2 ;;
          --binary) binary=$2 ;;
          --seconds) seconds=$2 ;;
          --out) out=$2 ;;
          --event) event=$2 ;;
        esac
        shift 2
        ;;
      *) usage_record >&2; die 2 "record: unknown argument $1" ;;
    esac
  done
  local missing=()
  [[ -n "${pid}" ]] || missing+=(--pid)
  [[ -n "${binary}" ]] || missing+=(--binary)
  [[ -n "${seconds}" ]] || missing+=(--seconds)
  [[ -n "${out}" ]] || missing+=(--out)
  if [[ ${#missing[@]} -gt 0 ]]; then
    usage_record >&2
    die 2 "record: missing required ${missing[*]}"
  fi
  [[ "${pid}" =~ ^[0-9]+$ ]] || die 2 "record: --pid must be a process id, got ${pid}"
  [[ "${seconds}" =~ ^[1-9][0-9]*$ ]] || die 2 "record: --seconds must be a positive integer, got ${seconds}"
  [[ -f "${binary}" ]] || die 2 "record: --binary ${binary} is not a file"

  require_linux_x86_64 record

  command -v perf >/dev/null 2>&1 || die 3 "record: perf is not installed"
  resolve_tools

  mkdir -p "${out}"
  rm -f "${out}/top-symbols.txt" "${out}/provenance.txt"
  local perf_data="${out}/perf.data" events=() ev kept=""
  local probe_data="${out}/perf-probe.data" probe_err="${out}/perf-probe.stderr"
  if [[ -n "${event}" ]]; then
    events=("${event}")
  else
    events=("${DEFAULT_EVENTS[@]}")
  fi
  for ev in "${events[@]}"; do
    rm -f "${probe_data}"
    if perf record -b -e "${ev}" -o "${probe_data}" -- true >/dev/null 2>"${probe_err}"; then
      kept=${ev}
      break
    fi
    if grep -q 'perf_event_paranoid' "${probe_err}"; then
      stderr_tail "${probe_err}"
      die 4 "record: perf refused event ${ev} under kernel.perf_event_paranoid; lower it or run with CAP_PERFMON (a permission refusal, not a missing PMU)"
    fi
    echo "record: event ${ev} cannot be opened on this host" >&2
    stderr_tail "${probe_err}"
  done
  rm -f "${probe_data}" "${probe_err}"
  [[ -n "${kept}" ]] || die 6 "record: no perf event can be opened (tried: ${events[*]}); this host exposes no usable branch sampling"

  local record_err="${out}/perf-record.stderr" rc=0
  rm -f "${perf_data}"
  echo "record: perf record -b -e ${kept} on pid ${pid} for ${seconds}s" >&2
  perf record -b -e "${kept}" -p "${pid}" -o "${perf_data}" -- sleep "${seconds}" >/dev/null 2>"${record_err}" || rc=$?
  if [[ "${rc}" != 0 ]]; then
    stderr_tail "${record_err}"
    die 4 "record: perf record -e ${kept} -p ${pid} exited ${rc} (wrong --pid, or perf refused it; see perf's stderr above)"
  fi

  local stats="${out}/perf-stats.txt" samples
  perf report -i "${perf_data}" --stats >"${stats}" 2>&1 || die 4 "record: perf report --stats failed on ${perf_data}"
  samples=$(awk '$1 == "SAMPLE" && $2 == "events:" { print $3; exit }' "${stats}")
  [[ "${samples}" =~ ^[0-9]+$ ]] || die 4 "record: perf report --stats printed no SAMPLE events count for ${perf_data}"
  [[ "${samples}" -gt 0 ]] \
    || die 4 "record: perf recorded zero samples on pid ${pid} with event ${kept}; is --pid the server, and is it under load?"
  echo "record: ${samples} samples with event ${kept}" >&2

  local profile="${out}/profile.prof"
  rm -f "${profile}"
  "${PROFGEN}" --perfdata="${perf_data}" --binary="${binary}" --output="${profile}" >&2 \
    || die 4 "record: llvm-profgen failed"
  [[ -s "${profile}" ]] || die 4 "record: llvm-profgen wrote an empty profile ${profile}"

  local functions="${out}/profile-functions.txt"
  "${PROFDATA}" show --sample --all-functions "${profile}" >"${functions}" \
    || die 4 "record: llvm-profdata show failed on ${profile}"

  local report="${out}/perf-report.txt" dso
  dso=$(basename "${binary}")
  perf report -i "${perf_data}" --stdio --no-branch-stack --no-children \
    --sort dso,symbol --no-demangle >"${report}" 2>/dev/null \
    || die 4 "record: perf report failed"

  # Field after "[.]": perf can print trailing "-  -" columns after the
  # symbol, so the last field is not the symbol.
  local top
  top=$(awk -v dso="${dso}" -v max="${TOP_N}" '
    /^#/ || NF == 0 || n >= max { next }
    {
      hit = 0
      for (i = 1; i <= NF; i++) if ($i == dso) hit = 1
      if (!hit) next
      for (i = 1; i < NF; i++) if ($i == "[.]") { print $(i + 1); n++; break }
    }' "${report}")
  [[ -n "${top}" ]] || die 4 "record: perf report shows no samples in ${dso}"

  local sym symbols=() top_file="${out}/top-symbols.txt"
  while IFS= read -r sym; do
    symbols+=("$(strip_suffixes "${sym}")")
  done <<<"${top}"
  printf '%s\n' "${symbols[@]}" >"${top_file}.tmp"

  # A profile function matches a symbol when its name is the symbol, or the
  # symbol followed by a "." suffix of its own (.__uniq.N).
  local verdicts
  verdicts=$(awk '
    FNR == NR { want[++n] = $0; next }
    /^Function: / {
      line = substr($0, 11)
      if (!match(line, /: [0-9]+, [0-9]+, [0-9]+ sampled lines/)) next
      name = substr(line, 1, RSTART - 1)
      total = substr(line, RSTART + 2) + 0
      for (i = 1; i <= n; i++) {
        s = want[i]
        if (name == s || index(name, s ".") == 1) {
          found[i] = 1
          if (total > 0) hot[i] = 1
        }
      }
    }
    END { for (i = 1; i <= n; i++) print (hot[i] ? "ok" : (found[i] ? "zero" : "absent")) "\t" want[i] }
    ' "${top_file}.tmp" "${functions}")
  local verdict absent=() zero=() n=${#symbols[@]}
  while IFS=$'\t' read -r verdict sym; do
    case "${verdict}" in
      absent) absent+=("${sym}") ;;
      zero) zero+=("${sym}") ;;
    esac
  done <<<"${verdicts}"
  if [[ ${#absent[@]} -gt 0 || ${#zero[@]} -gt 0 ]]; then
    if [[ ${#absent[@]} -gt 0 ]]; then
      printf '%s: record: %d of %d top symbols missing from the profile:\n' "${PROG}" "${#absent[@]}" "${n}" >&2
      printf '  %s\n' "${absent[@]}" >&2
    fi
    if [[ ${#zero[@]} -gt 0 ]]; then
      printf '%s: record: %d of %d top symbols have zero samples in the profile:\n' "${PROG}" "${#zero[@]}" "${n}" >&2
      printf '  %s\n' "${zero[@]}" >&2
    fi
    exit 4
  fi
  local has_ravel=0 has_df=0
  for sym in "${symbols[@]}"; do
    [[ "${sym}" == *ravel_* ]] && has_ravel=1
    [[ "${sym}" == *datafusion* ]] && has_df=1
  done
  [[ "${has_ravel}" == 1 ]] || die 4 "record: perf's top ${n} symbols hold no Ravel (ravel_) symbol; the training load did not reach Ravel code"
  [[ "${has_df}" == 1 ]] || die 4 "record: perf's top ${n} symbols hold no DataFusion (datafusion) symbol; the training load did not reach the SQL engine"
  mv -f "${top_file}.tmp" "${top_file}"
  echo "record: resolution ${n} of ${n} top symbols in the profile with samples" >&2

  local crate cpu_model
  crate=$(hottest_crate "${symbols[@]}")
  cpu_model=$(awk -F ': ' '/^model name/ { print $2; exit }' /proc/cpuinfo 2>/dev/null) || cpu_model=""
  {
    echo "source_sha=$(git -C "${REPO_ROOT}" rev-parse HEAD)"
    echo "binary_sha256=$(sha256sum "${binary}" | cut -d ' ' -f 1)"
    echo "rustc_version=$(rustc -V)"
    echo "llvm_major=${LLVM_MAJOR}"
    echo "llvm_profgen_version=${PROFGEN_VERSION}"
    echo "perf_event=${kept}"
    echo "seconds=${seconds}"
    echo "host=$(uname -srm)"
    echo "cpu_model=${cpu_model:-unknown}"
    echo "recorded_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "profile_sha256=$(sha256sum "${profile}" | cut -d ' ' -f 1)"
    echo "hottest_ravel_crate=${crate}"
    echo "top_symbols=top-symbols.txt"
  } >"${out}/provenance.txt"
  echo "record: wrote ${profile}, ${top_file} and ${out}/provenance.txt" >&2
}

# --- build -----------------------------------------------------------------

dockerfile_features() {
  local line
  line=$(awk '/cargo build .*-p ravel-server .*--features / { print; exit }' "${REPO_ROOT}/Dockerfile") || line=""
  [[ -n "${line}" ]] || die 2 "build: no ravel-server --features line in ${REPO_ROOT}/Dockerfile; pass --features"
  [[ "${line}" =~ --features[\ =]([^ ]+) ]] || die 2 "build: cannot read the --features value in ${REPO_ROOT}/Dockerfile; pass --features"
  printf '%s\n' "${BASH_REMATCH[1]}"
}

# Moves the unproven binary aside so nothing picks it up as a plain build,
# then exits with the given code.
unproven() {
  local code=$1 binary=$2
  shift 2
  if mv -f "${binary}" "${binary}.pgo-unproven"; then
    die "${code}" "$*; moved the binary to ${binary}.pgo-unproven, do not use it"
  fi
  die "${code}" "$*; could not move ${binary} aside, delete it and do not use it"
}

cmd_build() {
  local profile="" features="" target_dir=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h | --help) usage_build; exit 0 ;;
      --profile | --features | --target-dir)
        [[ $# -ge 2 ]] || { usage_build >&2; die 2 "build: $1 needs a value"; }
        case "$1" in
          --profile) profile=$2 ;;
          --features) features=$2 ;;
          --target-dir) target_dir=$2 ;;
        esac
        shift 2
        ;;
      *) usage_build >&2; die 2 "build: unknown argument $1" ;;
    esac
  done
  [[ -n "${profile}" ]] || { usage_build >&2; die 2 "build: missing required --profile"; }
  require_linux_x86_64 build
  [[ -e "${profile}" ]] || die 4 "build: profile ${profile} does not exist"
  [[ -s "${profile}" ]] || die 4 "build: profile ${profile} is empty"
  profile="$(cd "$(dirname "${profile}")" && pwd)/$(basename "${profile}")"

  PROFDATA=$(find_profdata)
  local listing
  listing=$("${PROFDATA}" show --sample --all-functions "${profile}") \
    || die 4 "build: llvm-profdata cannot read ${profile}"
  grep -q '^Function: ' <<<"${listing}" \
    || die 4 "build: llvm-profdata lists no functions in ${profile}"

  local prof_dir prov="" top_file
  prof_dir=$(dirname "${profile}")
  [[ -f "${prof_dir}/provenance.txt" ]] && prov="${prof_dir}/provenance.txt"
  top_file="${prof_dir}/top-symbols.txt"
  if [[ -n "${prov}" ]]; then
    local t
    t=$(kv_value top_symbols "${prov}")
    [[ -n "${t}" ]] && top_file="${prof_dir}/${t}"
  fi
  [[ -s "${top_file}" ]] \
    || die 4 "build: no ${top_file} beside the profile; record writes it and the !prof check needs it"

  [[ -n "${features}" ]] || features=$(dockerfile_features)
  target_dir=${target_dir:-${CARGO_TARGET_DIR:-${REPO_ROOT}/target}}
  mkdir -p "${target_dir}"
  target_dir="$(cd "${target_dir}" && pwd)"

  local head prof_sha="" crate="ravel_sql"
  head=$(git -C "${REPO_ROOT}" rev-parse HEAD)
  if [[ -n "${prov}" ]]; then
    prof_sha=$(kv_value source_sha "${prov}")
    local c
    c=$(kv_value hottest_ravel_crate "${prov}")
    [[ "${c}" =~ ^ravel_[a-z0-9_]+$ ]] && crate=${c}
  fi
  if [[ -n "${prof_sha}" && "${prof_sha}" != "${head}" ]]; then
    echo "${PROG}: warning: profile recorded at source ${prof_sha}, building ${head}; see the runbook's Refreshing section" >&2
  fi

  local flags="-Cprofile-sample-use=${profile}"
  if [[ -n "${RUSTFLAGS:-}" ]]; then
    flags="${RUSTFLAGS} ${flags}"
  fi

  local binary="${target_dir}/release/ravel-server"
  rm -f "${binary}.provenance.txt"
  echo "build: cargo build --release -p ravel-server --features ${features}" >&2
  RUSTFLAGS="${flags}" cargo build --release --locked -p ravel-server \
    --features "${features}" --target-dir "${target_dir}" >&2 \
    || die 1 "build: cargo build failed"
  [[ -f "${binary}" ]] || die 1 "build: cargo build left no ${binary}"

  local pkg=${crate//_/-}
  echo "build: emitting LLVM IR for ${pkg} to prove the profile was used" >&2
  RUSTFLAGS="${flags}" cargo rustc --release --locked -p "${pkg}" --lib \
    --target-dir "${target_dir}" -- --emit=llvm-ir >&2 \
    || unproven 1 "${binary}" "build: cargo rustc --emit=llvm-ir failed for ${pkg}"
  local ir="" f
  for f in "${target_dir}/release/deps/${crate}"-*.ll; do
    [[ -f "${f}" ]] || continue
    if [[ -z "${ir}" || "${f}" -nt "${ir}" ]]; then
      ir=${f}
    fi
  done
  [[ -n "${ir}" ]] || unproven 5 "${binary}" "build: no ${crate}-*.ll under ${target_dir}/release/deps, so the profile's use is unproven"
  grep -qF '!"SampleProfile"' "${ir}" \
    || unproven 5 "${binary}" "build: ${ir} carries no SampleProfile summary; the profile was not used"
  grep -qE '!"function_entry_count", i64 [1-9]' "${ir}" \
    || unproven 5 "${binary}" "build: ${ir} has no nonzero function_entry_count; the profile matched nothing"
  local hot_def
  hot_def=$(awk '
    FNR == NR { if ($0 != "") { want["@" $0 "("] = $0; want["@\"" $0 "\"("] = $0 } ; next }
    /^define / && index($0, "!prof") {
      for (w in want) if (index($0, w)) { print want[w]; exit }
    }' "${top_file}" "${ir}")
  [[ -n "${hot_def}" ]] \
    || unproven 5 "${binary}" "build: no top-20 symbol of ${top_file} is defined with !prof in ${ir}; the profile did not reach the hot code"
  echo "build: top-20 symbol ${hot_def} is defined with !prof" >&2

  {
    if [[ -n "${prov}" ]]; then
      sed -n 's/^\([a-z_0-9]*=\)/profile_\1/p' "${prov}"
    fi
    echo "source_sha=${head}"
    echo "binary_sha256=$(sha256sum "${binary}" | cut -d ' ' -f 1)"
    echo "rustc_version=$(rustc -V)"
    echo "features=${features}"
    echo "rustflags=${flags}"
  } >"${binary}.provenance.txt"
  echo "build: profile used; wrote ${binary} and ${binary}.provenance.txt" >&2
}

main() {
  [[ $# -gt 0 ]] || { usage_main >&2; exit 2; }
  local sub=$1
  shift
  case "${sub}" in
    -h | --help) usage_main ;;
    check-tools) cmd_check_tools "$@" ;;
    record) cmd_record "$@" ;;
    build) cmd_build "$@" ;;
    *) usage_main >&2; die 2 "unknown subcommand ${sub}" ;;
  esac
}

main "$@"
