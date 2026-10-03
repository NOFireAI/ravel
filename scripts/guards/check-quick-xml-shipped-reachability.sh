#!/usr/bin/env bash
# quick-xml shipped-reachability guard (issue #1718).
#
# deny.toml ignores RUSTSEC-2026-0194/-0195 on the premise that no shipped
# binary's default feature set turns on the object_store/inferno paths that
# pull an affected (older than 0.41.0) quick-xml version; only ravel-bench's
# off-by-default `profiling` and `parquet-baseline` features do. This checks
# that premise mechanically against the four release builds the root
# Dockerfile actually ships, using `cargo tree -i` (invert: show what would
# depend on the named package) to ask "is this version reachable from this
# build's dependency graph at all", rather than trusting the feature list by
# eye.
#
# The affected set is derived from Cargo.lock, not hardcoded: every distinct
# quick-xml version present that is older than the fixed 0.41.0 release. A
# bump to a still-affected 0.40.x is caught the same way 0.26.0/0.39.4 are
# today.
#
# `cargo tree -i quick-xml@<version>` exits 101 with "did not match any
# packages" on stderr when that exact version is absent from the package's
# resolved graph -- the pass case. It exits 0 and prints the path(s) up to
# the built package when the version IS reachable -- the finding case. That
# same "did not match any packages" shape is also what a MISTYPED version
# (one that never resolves ANYWHERE, in any build) produces, so before
# trusting any per-build "not reachable" result this script first runs a
# positive control per affected version: `cargo tree --locked --workspace
# --all-features -i quick-xml@<version>` must exit 0, proving the version
# genuinely resolves somewhere in this workspace's graph. A version that
# fails its positive control is not "clean" (round 1 of this guard read a
# typo'd AFFECTED_VERSIONS entry as a clean pass across all eight
# build/version checks, because every one of them independently produced the
# same "did not match any packages" a real absence would); it is a scan that
# cannot vouch for anything.
#
# Any outcome other than the two shapes above (a different exit code, or
# exit 101 with different stderr) means cargo itself could not answer the
# question, which this script must not read as either a pass or a fail: see
# NOTE below.
#
# Usage:
#   scripts/guards/check-quick-xml-shipped-reachability.sh
#     no arguments; runs against the workspace this script lives in.
#
# Exit 0 clean (no affected version reachable from any shipped build), 1 when
# an affected version IS reachable from a shipped build's graph (a real
# finding -- the path prints), 70 when a positive control fails (an affected
# version does not resolve anywhere in the workspace at all -- a stale or
# mistyped affected-version entry, or the version genuinely dropped out of
# Cargo.lock without this script's derivation noticing) or when a cargo tree
# invocation fails for a reason other than the expected "did not match any
# packages" (cargo itself is broken, or the package ID spec no longer parses
# -- NOT the same as a clean pass, and must not be read as one).
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 70

case "${1:-}" in
  -h | --help)
    sed -n '2,47p' "$0"
    exit 0
    ;;
esac

lock="${repo_root}/Cargo.lock"
if [[ ! -f "${lock}" ]]; then
  echo "check-quick-xml-shipped-reachability.sh: no such file: ${lock}" >&2
  exit 70
fi

# The four release builds the root Dockerfile ships (Dockerfile ~53-56), each
# as "package:features" (features may be empty for a default build).
BUILDS=(
  "ravel-server:sql,flight-sql,otap"
  "ravel-cli:"
  "ravel-operator:"
  "ravel-ingest-router:"
)

# quick-xml 0.41.0 fixes both advisories; every earlier version is affected.
# This is a fact about the advisories, not about this repo's lock, so it
# stays a literal here even though the SET of affected versions present is
# derived from Cargo.lock below.
FIXED_VERSION="0.41.0"

versions_status=0
affected_raw="$(awk -v fixed="${FIXED_VERSION}" '
{ raw[NR] = $0 }
function pkg_field(line, key,    t) {
  t = line
  sub("^" key " = \"", "", t)
  sub("\".*", "", t)
  return t
}
function ver_cmp(a, b,    an, bn, av, bv, i, ai, bi) {
  an = split(a, av, ".")
  bn = split(b, bv, ".")
  for (i = 1; i <= 3; i++) {
    ai = (i <= an) ? av[i] + 0 : 0
    bi = (i <= bn) ? bv[i] + 0 : 0
    if (ai < bi) return -1
    if (ai > bi) return 1
  }
  return 0
}
END {
  nl = NR
  nqx = 0
  i = 1
  while (i <= nl) {
    if (raw[i] ~ /^\[\[package\]\]/) {
      name = ""; ver = ""
      j = i + 1
      while (j <= nl && raw[j] !~ /^\[\[package\]\]/) {
        if (name == "" && raw[j] ~ /^name = "/) name = pkg_field(raw[j], "name")
        if (ver == "" && raw[j] ~ /^version = "/) ver = pkg_field(raw[j], "version")
        j++
      }
      if (name == "quick-xml" && !(ver in seen)) { seen[ver] = 1; nqx++; qxvers[nqx] = ver }
      i = j
    } else {
      i++
    }
  }
  naff = 0
  for (k = 1; k <= nqx; k++) {
    if (ver_cmp(qxvers[k], fixed) < 0) { naff++; aff[naff] = qxvers[k] }
  }
  if (naff == 0) {
    print "ZERO_AFFECTED" > "/dev/stderr"
    exit 2
  }
  for (k = 1; k <= naff; k++) print aff[k]
}
' "${lock}")" || versions_status=$?

if [[ ${versions_status} -ne 0 ]]; then
  echo "check-quick-xml-shipped-reachability.sh: found zero quick-xml versions" >&2
  echo "  older than ${FIXED_VERSION} in ${lock}. Refusing to report a result:" >&2
  echo "  a Cargo.lock format change, or every affected version dropping out" >&2
  echo "  of the graph, must not read as clean." >&2
  exit 70
fi

AFFECTED_VERSIONS=()
while IFS= read -r v; do
  [[ -n "${v}" ]] && AFFECTED_VERSIONS+=("${v}")
done <<<"${affected_raw}"

findings=0
errors=0
checked=0

out_file="$(mktemp "${TMPDIR:-/tmp}/quick-xml-reach.XXXXXX")"
trap 'rm -f "${out_file}"' EXIT

# --- positive control: every affected version must resolve SOMEWHERE in ---
# --- the workspace, or a per-build "not reachable" below is meaningless ---
for ver in "${AFFECTED_VERSIONS[@]}"; do
  status=0
  cargo tree --locked --workspace --all-features -i "quick-xml@${ver}" >"${out_file}" 2>&1 || status=$?
  if [[ ${status} -ne 0 ]]; then
    echo "check-quick-xml-shipped-reachability.sh: positive control failed for" >&2
    echo "  quick-xml ${ver}: \`cargo tree --locked --workspace --all-features" >&2
    echo "  -i quick-xml@${ver}\` did not exit 0 (exit ${status}). This version" >&2
    echo "  does not resolve anywhere in the workspace, so every per-build" >&2
    echo "  \"not reachable\" result below would be vacuous rather than clean:" >&2
    sed 's/^/  /' "${out_file}" >&2
    exit 70
  fi
done

for build in "${BUILDS[@]}"; do
  pkg="${build%%:*}"
  features="${build#*:}"
  for ver in "${AFFECTED_VERSIONS[@]}"; do
    checked=$((checked + 1))
    args=(tree --locked -p "${pkg}")
    if [[ -n "${features}" ]]; then
      args+=(--features "${features}")
    fi
    args+=(-i "quick-xml@${ver}")

    status=0
    cargo "${args[@]}" >"${out_file}" 2>&1 || status=$?

    if [[ ${status} -eq 0 ]]; then
      findings=$((findings + 1))
      echo "FINDING: quick-xml ${ver} is reachable from ${pkg} (features: ${features:-default}):" >&2
      sed 's/^/  /' "${out_file}" >&2
      continue
    fi

    if [[ ${status} -eq 101 ]] && grep -qF "did not match any packages" "${out_file}"; then
      continue
    fi

    # NOTE: neither the pass shape nor the finding shape. cargo tree failed
    # for some other reason (a bad feature name, cargo itself broken, the
    # package no longer exists) and this must not be silently treated as a
    # clean result.
    errors=$((errors + 1))
    echo "ERROR: cargo tree could not determine reachability for quick-xml ${ver} from ${pkg} (features: ${features:-default}), exit ${status}:" >&2
    sed 's/^/  /' "${out_file}" >&2
  done
done

if [[ ${errors} -gt 0 ]]; then
  echo "check-quick-xml-shipped-reachability.sh: ${errors} of ${checked} check(s) could not be determined" >&2
  exit 70
fi

if [[ ${findings} -gt 0 ]]; then
  echo "check-quick-xml-shipped-reachability.sh: ${findings} of ${checked} check(s) found an affected quick-xml version reachable from a shipped build" >&2
  exit 1
fi

echo "check-quick-xml-shipped-reachability.sh: clean (${checked} build/version combination(s) checked, none reachable)"
exit 0
