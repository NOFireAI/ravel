#!/usr/bin/env bash
# Cases for check-workflow-permissions.sh, in the pattern of
# check-test-hygiene.test.sh: add a case here before changing a rule.
#
# Each case builds a throwaway tree under $TMPDIR with the guard copied into
# its scripts/guards/, so the guard's own `cd repo_root` lands on the fixture
# and nothing here reads the real .github/workflows.
#
# Run: bash scripts/guards/check-workflow-permissions.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-workflow-permissions.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-workflow-permissions-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_tree <name>: a scratch tree with the guard installed. Prints its path.
new_tree() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/.github/workflows"
  cp "${GUARD}" "${dir}/scripts/guards/check-workflow-permissions.sh"
  printf '%s\n' "${dir}"
}

# check <name> <tree> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-workflow-permissions.sh 2>&1)" || rc=$?
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

# --- no-permissions --------------------------------------------------------

d="$(new_tree missing)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
YML
check "flags a workflow with no top-level permissions block" "${d}" 1 \
  ".github/workflows/w.yml:1: no-permissions"

d="$(new_tree present)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
permissions:
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
YML
check "a contents: read floor is clean" "${d}" 0 "clean"

d="$(new_tree empty-map)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
permissions: {}
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YML
check "an explicit empty permissions map is a declaration" "${d}" 0 "clean"

d="$(new_tree inline-read)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
permissions: read-all
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YML
check "the read-all shorthand is a declaration" "${d}" 0 "clean"

# A job-level block is not a substitute: a job added later inherits the
# repository default, which is the whole failure this guard exists for.
d="$(new_tree job-level-only)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - run: cargo build
YML
check "a job-level block does not satisfy the top-level rule" "${d}" 1 \
  "no-permissions"

# `permissions:` appears in the prose above the key in several real workflows
# here. A comment is not a declaration.
d="$(new_tree commented)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
# The workflow-level permissions floor is contents: read.
# permissions:
on:
  push:
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
YML
check "a commented-out permissions key does not count" "${d}" 1 "no-permissions"

d="$(new_tree yaml-ext)"
cat >"${d}/.github/workflows/w.yaml" <<'YML'
name: w
on:
  push:
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
YML
check "the .yaml extension is scanned too" "${d}" 1 ".yaml:1: no-permissions"

d="$(new_tree many)"
cat >"${d}/.github/workflows/a.yml" <<'YML'
name: a
on:
  push:
permissions:
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YML
cat >"${d}/.github/workflows/b.yml" <<'YML'
name: b
on:
  push:
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YML
check "one bad file among good ones is still reported" "${d}" 1 \
  ".github/workflows/b.yml:1: no-permissions"

# --- top-level-write -------------------------------------------------------

d="$(new_tree top-write)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
permissions:
  contents: read
  packages: write
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
YML
check "flags a write scope on the workflow-level floor" "${d}" 1 \
  ".github/workflows/w.yml:4: top-level-write"

d="$(new_tree top-write-all)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
permissions: write-all
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
YML
check "flags the write-all shorthand" "${d}" 1 "top-level-write"

# A job-level write is the shape the fix asks for and must stay clean.
d="$(new_tree job-write)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
permissions:
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
  report:
    runs-on: ubuntu-latest
    permissions:
      issues: write
    steps:
      - run: gh issue create
YML
check "a job-level write grant is clean" "${d}" 0 "clean"

# Prose inside the block is prose. This is the shape every workflow in this
# repo uses to say where the write grant actually lives.
d="$(new_tree prose)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
permissions:
  # issues: write is NOT granted here; it lives on the report job alone.
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
YML
check "a comment naming a write scope inside the block is not a grant" "${d}" 0 \
  "clean"

d="$(new_tree allow-marker)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
# workflow-permissions-allow: top-level-write -- single-job workflow whose only
# job is the one that files the issue, so the floor and the grant are the same
# thing and there is no later job to inherit it.
permissions:
  issues: write
jobs:
  report:
    runs-on: ubuntu-latest
    steps:
      - run: gh issue create
YML
check "an allow marker in the comment block above suppresses top-level-write" \
  "${d}" 0 "clean"

d="$(new_tree allow-detached)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
# workflow-permissions-allow: top-level-write -- detached from the key by a
# blank line and a real key, so it must not apply.
on:
  push:
permissions:
  issues: write
jobs:
  report:
    runs-on: ubuntu-latest
    steps:
      - run: gh issue create
YML
check "an allow marker not in the block above the key does not apply" "${d}" 1 \
  "top-level-write"

# --- the anchor ------------------------------------------------------------
#
# The rule the ticket asks for by name: a scan that finds nothing must fail
# rather than pass everything. A renamed or moved workflow directory otherwise
# turns this guard into a no-op that reports a clean tree forever.

d="$(new_tree anchor-empty)"
out="$(cd "${d}" && bash scripts/guards/check-workflow-permissions.sh 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"no workflow files"* ]]; then
  printf 'ok    an empty workflow directory exits 64\n'
  passes=$((passes + 1))
else
  printf 'FAIL  an empty workflow directory exits 64: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

d="$(new_tree anchor-gone)"
rmdir "${d}/.github/workflows"
out="$(cd "${d}" && bash scripts/guards/check-workflow-permissions.sh 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"no such directory"* ]]; then
  printf 'ok    a missing workflow directory exits 64\n'
  passes=$((passes + 1))
else
  printf 'FAIL  a missing workflow directory exits 64: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

# --- usage -----------------------------------------------------------------

d="$(new_tree usage)"
out="$(cd "${d}" && bash scripts/guards/check-workflow-permissions.sh --nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"unknown option"* ]]; then
  printf 'ok    an unknown option exits 64\n'
  passes=$((passes + 1))
else
  printf 'FAIL  an unknown option exits 64: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

out="$(cd "${d}" && bash scripts/guards/check-workflow-permissions.sh --help 2>&1)"
rc=$?
if [[ "${rc}" == "0" && "${out}" == *"no-permissions"* ]]; then
  printf 'ok    --help prints the header and exits 0\n'
  passes=$((passes + 1))
else
  printf 'FAIL  --help prints the header and exits 0: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

# An explicit root argument is the documented usage the CI step and gates.sh
# rely on staying equivalent to the default.
d="$(new_tree explicit-root)"
cat >"${d}/.github/workflows/w.yml" <<'YML'
name: w
on:
  push:
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: cargo build
YML
out="$(cd "${d}" && bash scripts/guards/check-workflow-permissions.sh .github/workflows 2>&1)"
rc=$?
if [[ "${rc}" == "1" && "${out}" == *"no-permissions"* ]]; then
  printf 'ok    an explicit root argument scans the same files\n'
  passes=$((passes + 1))
else
  printf 'FAIL  an explicit root argument scans the same files: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
