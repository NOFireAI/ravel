#!/usr/bin/env bash
# Published binaries are built optimised (issue #2528).
#
# The images the project publishes come from the root Dockerfile, which builds
# every binary with `cargo build --release`, on a release profile with LTO and
# one codegen unit. Both halves were convention: an edit to either passed every
# gate. CI build profiles are tuned for test speed elsewhere, so this pins the
# one profile and the one build path that must not follow them.
#
# Checks, all text scans, no build:
#   1. `[profile.release]` in the workspace Cargo.toml sets `lto` to "thin",
#      "fat" or true, sets `codegen-units = 1`, keeps debug info on (the
#      published debug-symbols image is split out of these binaries), and sets
#      no `opt-level` other than 3. The same `opt-level` rule holds for every
#      per-package release override, in either spelling: a
#      `[profile.release.package.NAME]` table or an inline table under
#      `[profile.release.package]`.
#   2. Every `cargo build` in the root Dockerfile carries `--release` and no
#      `--profile`. Backslash continuations are joined first, and a line that
#      chains several builds is checked per build.
#   3. Neither the Dockerfile nor publish-images.yml sets a
#      `CARGO_PROFILE_RELEASE_*` variable, which overrides the manifest.
#   4. publish-images.yml builds from the root Dockerfile: it names no other
#      Dockerfile.
#
# Exit 0 clean, 1 on a finding, 2 when an anchor is missing (no
# `[profile.release]` table, no `cargo build` line, a file that is not there):
# a scan that found nothing to check is not a pass.
#
# Run: scripts/guards/check-release-profile.sh
# Cases: scripts/guards/check-release-profile.test.sh
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 2

manifest="Cargo.toml"
dockerfile="Dockerfile"
publish=".github/workflows/publish-images.yml"

findings=0
finding() {
  printf 'FINDING: %s\n' "$1"
  findings=$((findings + 1))
}
missing() {
  printf 'cannot check: %s\n' "$1" >&2
  exit 2
}

for f in "${manifest}" "${dockerfile}" "${publish}"; do
  [[ -f "${f}" ]] || missing "${f} does not exist"
done

# --- 1. the release profile --------------------------------------------------

# Lines of one TOML table, comments and blank lines dropped. Prints nothing
# when the table is absent.
table_body() {
  awk -v want="[$1]" '
    /^\[/ { in_table = ($0 == want); next }
    in_table && !/^[[:space:]]*(#|$)/ { print }
  ' "${manifest}"
}

# The value of one key in a table body on stdin, quotes and spaces stripped.
key_value() {
  awk -v key="$1" '
    {
      line = $0
      sub(/[[:space:]]*#.*$/, "", line)
      n = index(line, "=")
      if (n == 0) next
      k = substr(line, 1, n - 1); v = substr(line, n + 1)
      gsub(/[[:space:]]/, "", k); gsub(/[[:space:]"]/, "", v)
      if (k == key) { print v; exit }
    }
  '
}

grep -qx '\[profile\.release\]' "${manifest}" \
  || missing "${manifest} has no [profile.release] table"

release_body="$(table_body profile.release)"

lto="$(printf '%s\n' "${release_body}" | key_value lto)"
case "${lto}" in
  thin | fat | true) ;;
  "") finding "${manifest}: [profile.release] sets no lto; published binaries need \"thin\" or \"fat\"" ;;
  *) finding "${manifest}: [profile.release] lto = ${lto}; published binaries need \"thin\" or \"fat\"" ;;
esac

units="$(printf '%s\n' "${release_body}" | key_value codegen-units)"
if [[ "${units}" != "1" ]]; then
  finding "${manifest}: [profile.release] codegen-units = ${units:-<unset>}; published binaries need 1"
fi

check_opt_level() {
  local table="$1" level
  level="$(table_body "${table}" | key_value opt-level)"
  if [[ -n "${level}" && "${level}" != "3" ]]; then
    finding "${manifest}: [${table}] opt-level = ${level}; published binaries need 3"
  fi
}

check_opt_level profile.release
while IFS= read -r header; do
  table="${header#[}"
  check_opt_level "${table%]}"
done < <(grep -E '^\[profile\.release\.package\..*\]$' "${manifest}" || true)

# The same override spelled as an inline table: `name = { opt-level = 1 }`
# under a bare [profile.release.package].
while IFS= read -r entry; do
  [[ -n "${entry}" ]] || continue
  finding "${manifest}: [profile.release.package] lowers opt-level: ${entry}"
done < <(table_body profile.release.package \
  | grep -E 'opt-level[[:space:]]*=' \
  | grep -vE 'opt-level[[:space:]]*=[[:space:]]*3([^0-9]|$)' || true)

# The debug-symbols image is split out of these binaries, so a release profile
# without debug info publishes an empty symbol set.
debug="$(printf '%s\n' "${release_body}" | key_value debug)"
case "${debug}" in
  "" | 0 | false | none)
    finding "${manifest}: [profile.release] debug = ${debug:-<unset>}; the published debug-symbols image needs debug info" ;;
esac

# --- 2. the Dockerfile builds on that profile --------------------------------

# Logical lines: a backslash continuation is joined to the line that starts
# it, and the number reported is that first line's. Comment lines are dropped
# before joining so a commented-out continuation cannot swallow a real line.
logical_lines() {
  awk '
    /^[[:space:]]*#/ { next }
    {
      if (start == 0) start = NR
      line = $0
      if (sub(/\\[[:space:]]*$/, "", line)) { buf = buf line " "; next }
      print start ":" buf line
      buf = ""; start = 0
    }
    END { if (start != 0) print start ":" buf }
  ' "${dockerfile}"
}

build_lines="$(logical_lines | grep -E 'cargo[[:space:]]+build' || true)"
[[ -n "${build_lines}" ]] || missing "${dockerfile} has no cargo build line"
builds=0

while IFS= read -r line; do
  number="${line%%:*}"
  text="${line#*:}"
  # One line can chain several builds; test each `cargo build ...` segment.
  while IFS= read -r segment; do
    [[ -n "${segment}" ]] || continue
    builds=$((builds + 1))
    if [[ "${segment}" != *"--release"* ]]; then
      finding "${dockerfile}:${number}: cargo build without --release: ${segment}"
    fi
    if [[ "${segment}" == *"--profile"* ]]; then
      finding "${dockerfile}:${number}: cargo build with --profile: ${segment}"
    fi
  done < <(printf '%s\n' "${text}" | awk '{
    n = split($0, parts, /cargo[[:space:]]+build/)
    for (i = 2; i <= n; i++) print "cargo build" parts[i]
  }')
done <<<"${build_lines}"

# --- 3. no environment override of the release profile -----------------------

for f in "${dockerfile}" "${publish}"; do
  while IFS= read -r hit; do
    [[ -n "${hit}" ]] || continue
    finding "${f}:${hit%%:*}: sets a CARGO_PROFILE_RELEASE_* override: ${hit#*:}"
  done < <(grep -nE 'CARGO_PROFILE_RELEASE_' "${f}" | grep -vE '^[0-9]+:[[:space:]]*#' || true)
done

# --- 4. the publish workflow builds the root Dockerfile -----------------------

grep -qE 'docker/build-push-action' "${publish}" \
  || missing "${publish} has no build-push-action step"

while IFS= read -r hit; do
  [[ -n "${hit}" ]] || continue
  finding "${publish}:${hit%%:*}: builds from a Dockerfile other than the root one: ${hit#*:}"
done < <(grep -nE '^[[:space:]]*file:[[:space:]]*' "${publish}" \
  | grep -vE '^[0-9]+:[[:space:]]*file:[[:space:]]*(\./)?Dockerfile[[:space:]]*$' || true)

if [[ "${findings}" -gt 0 ]]; then
  printf '%s finding(s): published binaries would not be built on the optimised release profile\n' "${findings}"
  exit 1
fi
printf 'release profile guard: clean (lto=%s, codegen-units=%s, debug=%s, %s cargo build(s) in %s)\n' \
  "${lto}" "${units}" "${debug}" "${builds}" "${dockerfile}"
