#!/usr/bin/env bash
# Cases for check-quick-xml-shipped-reachability.sh (issue #1718 round 2), in
# the pattern of check-quick-xml-entry-points.test.sh: add a case here before
# changing the guard.
#
# The real guard takes no arguments and always runs `cargo tree` against the
# real workspace, so these cases build a throwaway fixture "repo" (a copy of
# the guard at <fixture>/scripts/guards/... plus a fixture Cargo.lock at
# <fixture>/Cargo.lock) and run it with a stub `cargo` shadowing the real one
# on PATH. The guard's own repo-root detection (`dirname "$0"/../..`) is what
# makes the fixture directory read as "the workspace", with no argument
# parsing added to the guard just for testability.
#
# The stub cargo answers `cargo tree ...` calls from a responses file
# (tab-separated: key, exit status, optional stderr/stdout message), keyed on
# the argument list after "tree". Anything not listed defaults to the real
# cargo's own "version not present anywhere" shape (exit 101, "did not match
# any packages"), which is what makes the positive-control-failure case
# realistic: round 1 of this guard read exactly that shape, on every one of
# eight independent per-build checks, as a clean pass for a typo'd affected
# version that never resolved anywhere.
#
# No GNU-only sed/grep flags: this suite (and the guard it drives) must run
# the same way on macOS.
#
# Run: bash scripts/guards/check-quick-xml-shipped-reachability.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REAL_GUARD="${HERE}/check-quick-xml-shipped-reachability.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-quick-xml-reach-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# --- the stub cargo ----------------------------------------------------
STUB_BIN="${TMP}/stub-bin"
mkdir -p "${STUB_BIN}"
cat >"${STUB_BIN}/cargo" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail

if [[ "${FAKE_CARGO_MISSING:-0}" == "1" ]]; then
  echo "bash: cargo: command not found" >&2
  exit 127
fi

if [[ "${1:-}" != "tree" ]]; then
  echo "stub cargo: unsupported subcommand: ${1:-}" >&2
  exit 1
fi
shift
key="$*"

resp="${FAKE_CARGO_RESPONSES:-}"
if [[ -n "${resp}" && -f "${resp}" ]]; then
  while IFS=$'\t' read -r rkey rstatus rmsg; do
    [[ -z "${rkey:-}" ]] && continue
    if [[ "${key}" == "${rkey}" ]]; then
      if [[ -n "${rmsg:-}" ]]; then
        printf '%s\n' "${rmsg}"
      fi
      exit "${rstatus}"
    fi
  done <"${resp}"
fi

# Default: the real cargo's own shape for "this version is not in the
# resolved graph at all".
ver="$(printf '%s' "${key}" | grep -oE 'quick-xml@[^[:space:]]+' | head -1)"
echo "error: package ID specification \`${ver}\` did not match any packages" >&2
exit 101
STUB
chmod +x "${STUB_BIN}/cargo"

# check <name> <want-exit> <want-substring-or-empty> -- <lock-content> [responses-content] [missing=1]
run_guard() {
  local fixture="$1" lock_content="$2" responses_content="${3:-}" missing="${4:-0}"
  mkdir -p "${fixture}/scripts/guards"
  cp "${REAL_GUARD}" "${fixture}/scripts/guards/check-quick-xml-shipped-reachability.sh"
  chmod +x "${fixture}/scripts/guards/check-quick-xml-shipped-reachability.sh"
  printf '%s' "${lock_content}" >"${fixture}/Cargo.lock"
  local resp_file=""
  if [[ -n "${responses_content}" ]]; then
    resp_file="${fixture}/responses.tsv"
    # A trailing newline is required: the stub's `while read` loop silently
    # drops a last line that has none.
    printf '%s\n' "${responses_content}" >"${resp_file}"
  fi
  PATH="${STUB_BIN}:${PATH}" FAKE_CARGO_RESPONSES="${resp_file}" FAKE_CARGO_MISSING="${missing}" \
    bash "${fixture}/scripts/guards/check-quick-xml-shipped-reachability.sh"
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

lock_one_affected() {
  local ver="$1"
  cat <<LOCK
version = 4

[[package]]
name = "quick-xml"
version = "${ver}"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = [
 "memchr",
]
LOCK
}

lock_two_affected() {
  cat <<'LOCK'
version = 4

[[package]]
name = "quick-xml"
version = "0.26.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = [
 "memchr",
]

[[package]]
name = "quick-xml"
version = "0.39.4"
source = "registry+https://github.com/rust-lang/crates.io-index"
dependencies = [
 "memchr",
 "serde",
]
LOCK
}

# --- case 1: a version that resolves nowhere must FAIL, not pass ----------
# (round 1's bug: a typo'd/absent affected version reads every per-build
# "not reachable" as clean instead of refusing to vouch for the scan.)
d="${TMP}/positive-control-fails"
mkdir -p "${d}"
responses=$'--locked --workspace --all-features -i quick-xml@0.26.0\t101\terror: package ID specification `quick-xml@0.26.0` did not match any packages'
check "a version that resolves nowhere fails rather than passing" \
  70 "positive control failed" \
  run_guard "${d}" "$(lock_one_affected 0.26.0)" "${responses}"

# --- case 2: a version reachable from a shipped build is a real finding --
d="${TMP}/reachable-finding"
mkdir -p "${d}"
responses=$'--locked --workspace --all-features -i quick-xml@0.39.4\t0\tquick-xml v0.39.4\n--locked -p ravel-cli -i quick-xml@0.39.4\t0\tquick-xml v0.39.4\n    └── ravel-cli v0.15.0'
check "a version reachable from a shipped build's default graph is a finding" \
  1 "FINDING: quick-xml 0.39.4 is reachable from ravel-cli" \
  run_guard "${d}" "$(lock_one_affected 0.39.4)" "${responses}"

# --- case 3: cargo itself unusable must not read as clean -----------------
d="${TMP}/cargo-absent"
mkdir -p "${d}"
check "cargo being unusable exits 70, not 0" \
  70 "" \
  run_guard "${d}" "$(lock_one_affected 0.39.4)" "" 1

# --- case 4: the clean case: both affected versions resolve somewhere, ----
# --- neither reachable from any of the four shipped builds ---------------
d="${TMP}/clean"
mkdir -p "${d}"
responses=$'--locked --workspace --all-features -i quick-xml@0.26.0\t0\tquick-xml v0.26.0\n--locked --workspace --all-features -i quick-xml@0.39.4\t0\tquick-xml v0.39.4'
check "neither affected version reachable from any shipped build is clean" \
  0 "clean (8 build/version combination(s) checked, none reachable)" \
  run_guard "${d}" "$(lock_two_affected)" "${responses}"

# --- usage / self-check ----------------------------------------------------

d="${TMP}/help"
mkdir -p "${d}/scripts/guards"
cp "${REAL_GUARD}" "${d}/scripts/guards/check-quick-xml-shipped-reachability.sh"
chmod +x "${d}/scripts/guards/check-quick-xml-shipped-reachability.sh"
out="$(bash "${d}/scripts/guards/check-quick-xml-shipped-reachability.sh" --help 2>&1)"
rc=$?
if [[ "${rc}" == "0" && "${out}" == *"quick-xml"* ]]; then
  printf 'ok    --help prints the header and exits 0\n'
  passes=$((passes + 1))
else
  printf 'FAIL  --help prints the header and exits 0: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
