#!/usr/bin/env bash
# Opt-in sample-PGO build of ravel-server (ADR-4523 decision 4, issue #4527).
#
# R3 measured sample PGO and deferred it (docs/internal/pgo-runbook.md):
# release images keep the ordinary build, and nothing in this repository
# calls this script. It exists as the reproduction of R3 and for an operator
# following the runbook. It never touches publish-images, a Dockerfile or
# any release path; it only reads the root Dockerfile for its default
# feature list.
#
# Subcommands (each takes --help):
#   check-tools   find llvm-profgen and llvm-profdata, refuse an LLVM major
#                 that differs from rustc's
#   record        perf-record a running ravel-server, convert the samples to
#                 a sample profile, check that perf's hottest symbols resolve
#                 in it, and write provenance.txt beside it
#   build         cargo build --release with -Cprofile-sample-use, then prove
#                 from emitted LLVM IR that the profile was used
#
# Exit codes:
#   0  success
#   2  usage error
#   3  a tool is missing, or llvm-profgen's LLVM major differs from rustc's
#   4  the profile is empty, lists no functions, or perf's top symbols do not
#      resolve in it
#   5  the build did not use the profile; the binary must not be used
#   6  unsupported host (not Linux x86_64, or no perf event records samples)
#   1  cargo build itself failed
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

Exit codes: 0 success, 2 usage, 3 tool missing or LLVM major mismatch,
4 empty or unresolved profile, 5 profile not used by the build,
6 unsupported host, 1 cargo build failed.
EOF
}

usage_check_tools() {
  cat <<'EOF'
Usage: scripts/build-pgo.sh check-tools

Reads the LLVM major from `rustc -vV` and finds:
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

Linux x86_64 only (exit 6 elsewhere). Runs `perf record -b -e <event>` on
the pid for <n> seconds. Without --event it tries cpu/branch-brs/ (AMD BRS),
then br_inst_retired.near_taken:u (Intel LBR), then cycles:u, and keeps the
first that writes a non-empty perf.data (exit 6 if none does). Converts
with llvm-profgen into <dir>/profile.prof (exit 4 if empty).

Resolution check: the top 20 self-time symbols of the binary in
`perf report --sort dso,symbol` must each appear as a function in
`llvm-profdata show --sample --all-functions`, after stripping .llvm.N,
.constprop.N, .isra.N, .part.N and .cold.N suffixes. Exit 4 names the
missing ones.

Writes <dir>/provenance.txt: source_sha, binary_sha256, rustc_version,
llvm_major, llvm_profgen_version, perf_event, seconds, host, cpu_model,
recorded_at, profile_sha256, hottest_ravel_crate.
EOF
}

usage_build() {
  cat <<'EOF'
Usage: scripts/build-pgo.sh build --profile <file> [--features <list>]
         [--target-dir <dir>]

Runs `cargo build --release --locked -p ravel-server --features <list>`
with -Cprofile-sample-use=<absolute profile path> appended to RUSTFLAGS
(the caller's RUSTFLAGS are kept, never replaced).

--features defaults to the release image's set, read from the
`cargo build ... -p ravel-server --features` line of the root Dockerfile.

Refuses a missing or empty profile, or one in which llvm-profdata lists no
functions (exit 4). After the build it emits LLVM IR for the hottest Ravel
crate named in the profile's provenance.txt (else ravel-sql) with the same
RUSTFLAGS and requires a SampleProfile summary and a nonzero
function_entry_count; otherwise it exits 5 and the binary must not be used.
A provenance source_sha that differs from HEAD prints a warning naming both
and the build continues.

Writes <binary>.provenance.txt: the profile's provenance lines prefixed
profile_, plus source_sha, binary_sha256, rustc_version, features,
rustflags.
EOF
}

# --- shared helpers --------------------------------------------------------

rustc_llvm_major() {
  local vv
  vv=$(rustc -vV) || die 3 "rustc -vV failed"
  local major
  major=$(printf '%s\n' "${vv}" | sed -n 's/^LLVM version: \([0-9][0-9]*\).*/\1/p;T;q')
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
  printf '%s\n' "${out}" | sed -n 's/^[[:space:]]*\(.*LLVM version.*\)/\1/p;T;q'
}

# Sets PROFGEN, PROFDATA, LLVM_MAJOR and PROFGEN_VERSION.
resolve_tools() {
  LLVM_MAJOR=$(rustc_llvm_major)
  PROFGEN=$(find_profgen "${LLVM_MAJOR}")
  PROFDATA=$(find_profdata)
  PROFGEN_VERSION=$(profgen_version_line "${PROFGEN}")
  local pg_major
  pg_major=$(printf '%s\n' "${PROFGEN_VERSION}" | sed -n 's/.*LLVM version \([0-9][0-9]*\).*/\1/p')
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

# Workspace crate names (ravel_sql), longest first, so a crate whose name is
# a prefix of another's never wins over it.
workspace_crates() {
  local manifest name
  for manifest in "${REPO_ROOT}"/crates/*/Cargo.toml "${REPO_ROOT}"/services/*/Cargo.toml; do
    [[ -f "${manifest}" ]] || continue
    name=$(sed -n 's/^name = "\(ravel-[a-z0-9-]*\)"$/\1/p;T;q' "${manifest}")
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

# --- check-tools -----------------------------------------------------------

cmd_check_tools() {
  if [[ $# -gt 0 ]]; then
    case "$1" in
      -h | --help) usage_check_tools; exit 0 ;;
      *) usage_check_tools >&2; die 2 "check-tools: unknown argument $1" ;;
    esac
  fi
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

  local os arch
  os=$(uname -s)
  arch=$(uname -m)
  [[ "${os}" == "Linux" && "${arch}" == "x86_64" ]] \
    || die 6 "record: needs Linux x86_64, this host is ${os} ${arch}"

  command -v perf >/dev/null 2>&1 || die 3 "record: perf is not installed"
  resolve_tools

  mkdir -p "${out}"
  local perf_data="${out}/perf.data" events=() ev kept=""
  if [[ -n "${event}" ]]; then
    events=("${event}")
  else
    events=("${DEFAULT_EVENTS[@]}")
  fi
  for ev in "${events[@]}"; do
    rm -f "${perf_data}"
    echo "record: perf record -b -e ${ev} on pid ${pid} for ${seconds}s" >&2
    if perf record -b -e "${ev}" -p "${pid}" -o "${perf_data}" -- sleep "${seconds}" >&2 \
      && [[ -s "${perf_data}" ]]; then
      kept=${ev}
      break
    fi
    echo "record: event ${ev} wrote no samples" >&2
  done
  [[ -n "${kept}" ]] || die 6 "record: no perf event wrote samples (tried: ${events[*]}); this host exposes no usable branch sampling"

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

  local sym stripped absent=() n=0 symbols=()
  while IFS= read -r sym; do
    stripped=$(strip_suffixes "${sym}")
    symbols+=("${stripped}")
    n=$((n + 1))
    if ! grep -qF -e "Function: ${stripped}:" -e "Function: ${stripped}." "${functions}"; then
      absent+=("${stripped}")
    fi
  done <<<"${top}"
  if [[ ${#absent[@]} -gt 0 ]]; then
    printf '%s: record: %d of %d top symbols missing from the profile:\n' "${PROG}" "${#absent[@]}" "${n}" >&2
    printf '  %s\n' "${absent[@]}" >&2
    exit 4
  fi
  echo "record: resolution ${n} of ${n} top symbols in the profile" >&2

  local crate cpu_model
  crate=$(hottest_crate "${symbols[@]}")
  cpu_model=$(sed -n 's/^model name[[:space:]]*: //p;T;q' /proc/cpuinfo 2>/dev/null) || cpu_model=""
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
  } >"${out}/provenance.txt"
  echo "record: wrote ${profile} and ${out}/provenance.txt" >&2
}

# --- build -----------------------------------------------------------------

dockerfile_features() {
  local line
  line=$(sed -n '/cargo build .*-p ravel-server .*--features /{p;q}' "${REPO_ROOT}/Dockerfile") || line=""
  [[ -n "${line}" ]] || die 2 "build: no ravel-server --features line in ${REPO_ROOT}/Dockerfile; pass --features"
  printf '%s\n' "${line}" | sed -E 's/.*--features[ =]([^ ]+).*/\1/'
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
  [[ -e "${profile}" ]] || die 4 "build: profile ${profile} does not exist"
  [[ -s "${profile}" ]] || die 4 "build: profile ${profile} is empty"
  profile="$(cd "$(dirname "${profile}")" && pwd)/$(basename "${profile}")"

  PROFDATA=$(find_profdata)
  local listing
  listing=$("${PROFDATA}" show --sample --all-functions "${profile}") \
    || die 4 "build: llvm-profdata cannot read ${profile}"
  printf '%s\n' "${listing}" | grep -q '^Function: ' \
    || die 4 "build: llvm-profdata lists no functions in ${profile}"

  [[ -n "${features}" ]] || features=$(dockerfile_features)
  target_dir=${target_dir:-${CARGO_TARGET_DIR:-${REPO_ROOT}/target}}
  mkdir -p "${target_dir}"
  target_dir="$(cd "${target_dir}" && pwd)"

  local prov="" head prof_sha="" crate="ravel_sql"
  [[ -f "$(dirname "${profile}")/provenance.txt" ]] && prov="$(dirname "${profile}")/provenance.txt"
  head=$(git -C "${REPO_ROOT}" rev-parse HEAD)
  if [[ -n "${prov}" ]]; then
    prof_sha=$(sed -n 's/^source_sha=//p;T;q' "${prov}")
    local c
    c=$(sed -n 's/^hottest_ravel_crate=//p;T;q' "${prov}")
    [[ "${c}" =~ ^ravel_[a-z0-9_]+$ ]] && crate=${c}
  fi
  if [[ -n "${prof_sha}" && "${prof_sha}" != "${head}" ]]; then
    echo "${PROG}: warning: profile recorded at source ${prof_sha}, building ${head}; see the runbook's Refreshing section" >&2
  fi

  local flags="-Cprofile-sample-use=${profile}"
  if [[ -n "${RUSTFLAGS:-}" ]]; then
    flags="${RUSTFLAGS} ${flags}"
  fi

  echo "build: cargo build --release -p ravel-server --features ${features}" >&2
  RUSTFLAGS="${flags}" cargo build --release --locked -p ravel-server \
    --features "${features}" --target-dir "${target_dir}" >&2 \
    || die 1 "build: cargo build failed"
  local binary="${target_dir}/release/ravel-server"
  [[ -f "${binary}" ]] || die 1 "build: cargo build left no ${binary}"

  local pkg=${crate//_/-}
  echo "build: emitting LLVM IR for ${pkg} to prove the profile was used" >&2
  RUSTFLAGS="${flags}" cargo rustc --release --locked -p "${pkg}" --lib \
    --target-dir "${target_dir}" -- --emit=llvm-ir >&2 \
    || die 5 "build: cargo rustc --emit=llvm-ir failed for ${pkg}; do not use ${binary}"
  local ir
  ir=$(find "${target_dir}/release/deps" -maxdepth 1 -name "${crate}-*.ll" -printf '%T@ %p\n' 2>/dev/null \
    | sort -rn | awk 'NR == 1 { sub(/^[^ ]* /, ""); print }') || ir=""
  [[ -n "${ir}" ]] || die 5 "build: no ${crate}-*.ll under ${target_dir}/release/deps; do not use ${binary}"
  grep -qF '!"SampleProfile"' "${ir}" \
    || die 5 "build: ${ir} carries no SampleProfile summary; the profile was not used, do not use ${binary}"
  grep -qE '!"function_entry_count", i64 [1-9]' "${ir}" \
    || die 5 "build: ${ir} has no nonzero function_entry_count; the profile matched nothing, do not use ${binary}"

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
