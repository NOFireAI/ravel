#!/usr/bin/env bash
# quick-xml shipped-reachability guard (issue #1718).
#
# deny.toml ignores RUSTSEC-2026-0194/-0195 on the premise that no shipped
# binary's default feature set turns on the object_store/inferno paths that
# pull an affected quick-xml version (0.26.0 or 0.39.4); only ravel-bench's
# off-by-default `profiling` and `parquet-baseline` features do. This checks
# that premise mechanically against the four release builds the root
# Dockerfile actually ships, using `cargo tree -i` (invert: show what would
# depend on the named package) to ask "is this version reachable from this
# build's dependency graph at all", rather than trusting the feature list by
# eye.
#
# `cargo tree -i quick-xml@<version>` exits 101 with "did not match any
# packages" on stderr when that exact version is absent from the package's
# resolved graph -- the pass case. It exits 0 and prints the path(s) up to
# the built package when the version IS reachable -- the finding case. Any
# other outcome (a different exit code, or exit 101 with different stderr)
# means cargo itself could not answer the question, which this script must
# not read as either a pass or a fail: see NOTE below.
#
# Usage:
#   scripts/guards/check-quick-xml-shipped-reachability.sh
#     no arguments; runs against the workspace this script lives in.
#
# Exit 0 clean (no affected version reachable from any shipped build), 1 when
# an affected version IS reachable from a shipped build's graph (a real
# finding -- the path prints), 70 when a cargo tree invocation fails for a
# reason other than the expected "did not match any packages" (cargo itself
# is broken, or the package ID spec no longer parses -- NOT the same as a
# clean pass, and must not be read as one).
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 70

case "${1:-}" in
  -h | --help)
    sed -n '2,26p' "$0"
    exit 0
    ;;
esac

# The four release builds the root Dockerfile ships (Dockerfile ~53-56), each
# as "package:features" (features may be empty for a default build).
BUILDS=(
  "ravel-server:sql,flight-sql,otap"
  "ravel-cli:"
  "ravel-operator:"
  "ravel-ingest-router:"
)

# Both affected versions; quick-xml 0.41.0 fixes both advisories.
AFFECTED_VERSIONS=(0.26.0 0.39.4)

findings=0
errors=0
checked=0

out_file="$(mktemp "${TMPDIR:-/tmp}/quick-xml-reach.XXXXXX")"
trap 'rm -f "${out_file}"' EXIT

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
