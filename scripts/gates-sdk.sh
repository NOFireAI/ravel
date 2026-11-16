#!/usr/bin/env bash
# The TypeScript SDK gate (ADR-3331 decision 14): from sdk/typescript, run
# `npm ci`, `npm run lint`, `npm run typecheck` and `npm test`, in that order,
# and stop at the first that fails, naming it.
#
# Usage: scripts/gates-sdk.sh
#
# CI's `sdk` job runs this script and nothing else, so CI and a local or
# executor run execute the same four commands. scripts/gates.sh calls it in
# workspace mode only: scoped mode (-p CRATE) selects cargo packages, and the
# SDK is not one, so no -p argument can name it.
#
# `npm test` runs `npm test --workspaces`, and each package's own test script
# runs vitest, which discovers `*.test.ts` by pattern: a new suite under a
# package's src/ is enrolled by being there. A new package without a test
# script fails `npm test --workspaces` with "Missing script" rather than being
# skipped. What can be orphaned is this script itself, which is why
# scripts/guards/check-test-suites-run.sh requires a workflow to name it.
#
# Node: this script checks the major against sdk/typescript/.node-version,
# the file CI's actions/setup-node reads. The binding floor is stricter and
# lives in sdk/typescript/package.json's `engines` range, which `npm ci`
# enforces because sdk/typescript/.npmrc sets engine-strict: a node of the
# right major below that floor passes this check and fails at `npm ci` with
# EBADENGINE. A missing node or npm, or a different major, is refused with
# exit 64 before any npm command runs. A host without node 22 that needs an
# unscoped scripts/gates.sh run sets RAVEL_GATES_SKIP_SDK=1 there, and CI's
# `sdk` job stays the gate for sdk/typescript.
#
# Exit codes:
#   0   all four steps passed
#   64  node or npm is absent, the node major does not match, or
#       sdk/typescript/.node-version is missing or unreadable
#   *   the failing step's own exit code
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"
sdk_dir="${repo_root}/sdk/typescript"
version_file="${sdk_dir}/.node-version"

if [[ ! -r "${version_file}" ]]; then
  echo "gates-sdk.sh: refused: ${version_file} is missing or unreadable; it pins the node major" >&2
  exit 64
fi
want_major="$(tr -d '[:space:]' <"${version_file}")"
want_major="${want_major#v}"
want_major="${want_major%%.*}"
if [[ ! "${want_major}" =~ ^[0-9]+$ ]]; then
  echo "gates-sdk.sh: refused: ${version_file} does not start with a node major version" >&2
  exit 64
fi

if ! command -v node >/dev/null 2>&1; then
  echo "gates-sdk.sh: refused: node is not on PATH (need node ${want_major}, per sdk/typescript/.node-version); for an unscoped scripts/gates.sh run on this host set RAVEL_GATES_SKIP_SDK=1 and let CI's sdk job be the gate" >&2
  exit 64
fi
if ! command -v npm >/dev/null 2>&1; then
  echo "gates-sdk.sh: refused: npm is not on PATH (need the npm that ships with node ${want_major}); for an unscoped scripts/gates.sh run on this host set RAVEL_GATES_SKIP_SDK=1 and let CI's sdk job be the gate" >&2
  exit 64
fi
node_version="$(node --version 2>/dev/null)" || node_version=""
have_major="${node_version#v}"
have_major="${have_major%%.*}"
if [[ "${have_major}" != "${want_major}" ]]; then
  echo "gates-sdk.sh: refused: node ${node_version:-of unknown version} is on PATH, sdk/typescript/.node-version pins major ${want_major} (and npm ci enforces package.json's engines floor); for an unscoped scripts/gates.sh run on this host set RAVEL_GATES_SKIP_SDK=1 and let CI's sdk job be the gate" >&2
  exit 64
fi

cd "${sdk_dir}" || {
  echo "gates-sdk.sh: cannot enter ${sdk_dir}" >&2
  exit 1
}

echo "gates-sdk.sh: node ${node_version}, npm $(npm --version 2>/dev/null), in ${sdk_dir}"

step() {
  local label="$1" code=0
  shift
  echo "==> $*"
  "$@" || code=$?
  if [[ ${code} -ne 0 ]]; then
    echo "gates-sdk.sh: step '${label}' failed with exit ${code}: $*" >&2
    exit "${code}"
  fi
}

step "npm ci" npm ci
step "lint" npm run lint
step "typecheck" npm run typecheck
step "test" npm test

echo "gates-sdk.sh: all SDK gates passed."
