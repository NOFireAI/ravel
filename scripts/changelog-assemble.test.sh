#!/usr/bin/env bash
# Cases for changelog-assemble.sh (issue #2323). Each case builds a scratch
# repository layout under $TMPDIR holding a copy of the script, a small
# CHANGELOG.md and a changelog.d/, runs the script there, and compares the
# result byte for byte. Nothing here reads or writes the real repository.
#
# Run: bash scripts/changelog-assemble.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/changelog-assemble.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/changelog-assemble-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

PASSED=0
FAILED=0

ok() {
  printf 'ok    %s\n' "$1"
  PASSED=$((PASSED + 1))
}
fail() {
  printf 'FAIL  %s: %s\n' "$1" "$2"
  FAILED=$((FAILED + 1))
}

# Heading order is first appearance in the file: Changed, Fixed, Added,
# Security here, with Removed (never used) last.
BASE_CHANGELOG='# Changelog

Intro.

## [Unreleased]

### Changed

- existing change

### Fixed

- existing fix

## [0.1.0] - 2026-01-01

### Added

- old addition

### Security

- old security note
'

# new_tree <name> [changelog-text]: a scratch tree with the script and a
# CHANGELOG.md. Prints its path.
new_tree() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts" "${dir}/changelog.d"
  cp "${SCRIPT}" "${dir}/scripts/changelog-assemble.sh"
  printf '%s' "${2-${BASE_CHANGELOG}}" >"${dir}/CHANGELOG.md"
  printf '# Changelog fragments\n' >"${dir}/changelog.d/README.md"
  printf '%s\n' "${dir}"
}

# run <dir> [args]: run the script in <dir>; sets RC and OUT.
run() {
  local dir="$1"
  shift
  RC=0
  # The interpreter running this suite runs the script, so running the suite
  # with /bin/bash on a Mac really tests bash 3.2.
  OUT="$("${BASH}" "${dir}/scripts/changelog-assemble.sh" "$@" 2>&1)" || RC=$?
}

# expect_rc <name> <want>
expect_rc() {
  if [[ "${RC}" == "$2" ]]; then
    ok "$1"
  else
    fail "$1" "exit ${RC}, wanted $2; output: ${OUT}"
  fi
}

# expect_file <name> <path> <want-text>
expect_file() {
  local got
  got="$(cat "$2"; printf x)"
  got="${got%x}"
  if [[ "${got}" == "$3" ]]; then
    ok "$1"
  else
    fail "$1" "content differs"
    diff <(printf '%s' "$3") "$2" | sed 's/^/      /'
  fi
}

# snapshot <dir>: every file under it with its content, for "changed nothing".
# cksum is POSIX, so it exists on every host. An empty snapshot would make
# every "changed nothing" comparison pass, so a failed one prints a value
# unique to the call, which no other snapshot can equal.
snapshot() {
  local s
  s="$(cd "$1" && find . -type f -print0 | sort -z | xargs -0 cksum)"
  if [[ -z "${s}" ]]; then
    printf 'changelog-assemble.test.sh: empty snapshot of %s\n' "$1" >&2
    s="snapshot-failed-${RANDOM}-${RANDOM}"
  fi
  printf '%s\n' "${s}"
}

# --- a fragment under an existing heading lands after its bullets ----------

d="$(new_tree existing-heading)"
printf -- '- new fix\n' >"${d}/changelog.d/10.fixed.md"
run "${d}"
expect_rc "fold into an existing heading exits 0" 0
expect_file "fold into an existing heading appends after the existing bullets" \
  "${d}/CHANGELOG.md" '# Changelog

Intro.

## [Unreleased]

### Changed

- existing change

### Fixed

- existing fix
- new fix

## [0.1.0] - 2026-01-01

### Added

- old addition

### Security

- old security note
'

# --- the fragment is deleted after folding, README.md is kept ---------------

if [[ ! -e "${d}/changelog.d/10.fixed.md" ]]; then
  ok "a folded fragment is deleted"
else
  fail "a folded fragment is deleted" "10.fixed.md still exists"
fi
if [[ -f "${d}/changelog.d/README.md" ]]; then
  ok "changelog.d/README.md survives a fold"
else
  fail "changelog.d/README.md survives a fold" "README.md was removed"
fi

# --- two fragments in one section land in ascending NUMBER order -----------
# 100 sorts before 9 as text; numeric order puts 9 first. 9-2 follows 9.

d="$(new_tree number-order)"
printf -- '- change 100\n' >"${d}/changelog.d/100.changed.md"
printf -- '- change 9\n  wrapped line\n\n\n' >"${d}/changelog.d/9.changed.md"
printf -- '- change 9 again\n' >"${d}/changelog.d/9-2.changed.md"
run "${d}"
expect_rc "several fragments in one section exit 0" 0
expect_file "fragments in one section fold in ascending NUMBER then suffix order" \
  "${d}/CHANGELOG.md" '# Changelog

Intro.

## [Unreleased]

### Changed

- existing change
- change 9
  wrapped line
- change 9 again
- change 100

### Fixed

- existing fix

## [0.1.0] - 2026-01-01

### Added

- old addition

### Security

- old security note
'

# --- a heading [Unreleased] lacks is created, in the file's heading order ---
# Order of first appearance: Changed, Fixed, Added, Security; Removed never
# appears, so it goes last.

d="$(new_tree absent-heading)"
printf -- '- new security\n' >"${d}/changelog.d/5.security.md"
printf -- '- new addition\n' >"${d}/changelog.d/6.added.md"
printf -- '- new removal\n' >"${d}/changelog.d/7.removed.md"
run "${d}"
expect_rc "fragments whose heading is absent exit 0" 0
expect_file "absent headings are created in the order CHANGELOG.md uses" \
  "${d}/CHANGELOG.md" '# Changelog

Intro.

## [Unreleased]

### Changed

- existing change

### Fixed

- existing fix

### Added

- new addition

### Security

- new security

### Removed

- new removal

## [0.1.0] - 2026-01-01

### Added

- old addition

### Security

- old security note
'

# The file's order decides, not Keep a Changelog's: Security first appears
# before Changed here, so the created Security heading precedes Changed.
d="$(new_tree file-order-wins)"
printf '# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n- existing fix\n\n## [0.1.0]\n\n### Security\n\n- old security\n\n### Changed\n\n- old change\n' \
  >"${d}/CHANGELOG.md"
printf -- '- new change\n' >"${d}/changelog.d/3.changed.md"
printf -- '- new security\n' >"${d}/changelog.d/4.security.md"
run "${d}"
expect_rc "headings ordered by the file exit 0" 0
expect_file "created headings follow the file's first-appearance order" \
  "${d}/CHANGELOG.md" '# Changelog

## [Unreleased]

### Fixed

- existing fix

### Security

- new security

### Changed

- new change

## [0.1.0]

### Security

- old security

### Changed

- old change
'

# An empty [Unreleased] (just after a release) gains its first heading.
d="$(new_tree empty-unreleased)"
printf '# Changelog\n\n## [Unreleased]\n\n## [0.1.0]\n\n### Fixed\n\n- old fix\n' \
  >"${d}/CHANGELOG.md"
printf -- '- first fix\n' >"${d}/changelog.d/1.fixed.md"
run "${d}"
expect_rc "an empty [Unreleased] exits 0" 0
expect_file "an empty [Unreleased] gains the heading and its bullet" \
  "${d}/CHANGELOG.md" '# Changelog

## [Unreleased]

### Fixed

- first fix

## [0.1.0]

### Fixed

- old fix
'

# A heading straight after [Unreleased] leaves nothing between them, the
# empty-array case bash 3.2 rejects under set -u unless it is guarded.
d="$(new_tree no-preamble)"
printf '# Changelog\n\n## [Unreleased]\n### Fixed\n\n- existing fix\n' >"${d}/CHANGELOG.md"
printf -- '- next fix\n' >"${d}/changelog.d/3.fixed.md"
run "${d}"
expect_rc "a heading directly under [Unreleased] exits 0" 0
expect_file "a heading directly under [Unreleased] gains the bullet and a blank line" \
  "${d}/CHANGELOG.md" '# Changelog

## [Unreleased]

### Fixed

- existing fix
- next fix
'

# [Unreleased] as the last section of the file.
d="$(new_tree unreleased-last)"
printf '# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n- existing fix\n' >"${d}/CHANGELOG.md"
printf -- '- tail fix\n' >"${d}/changelog.d/2.fixed.md"
run "${d}"
expect_rc "an [Unreleased] at the end of the file exits 0" 0
expect_file "an [Unreleased] at the end of the file is appended to" \
  "${d}/CHANGELOG.md" '# Changelog

## [Unreleased]

### Fixed

- existing fix
- tail fix
'

# --- no fragments: nothing changes, exit 0 ----------------------------------

d="$(new_tree no-fragments)"
before="$(snapshot "${d}")"
run "${d}"
expect_rc "no fragments exits 0" 0
if [[ "$(snapshot "${d}")" == "${before}" ]]; then
  ok "no fragments changes nothing"
else
  fail "no fragments changes nothing" "the tree changed"
fi

# --- refusals: each exits 1 and changes nothing ----------------------------

# refuse <name> <file> <content> <want-substring>
refuse() {
  local name="$1" file="$2" content="$3" want="$4" d before
  d="$(new_tree "refuse-${name// /-}")"
  printf -- '- a valid fragment\n' >"${d}/changelog.d/1.fixed.md"
  if [[ "${file}" == */ ]]; then
    mkdir -p "${d}/changelog.d/${file}"
  else
    printf '%s' "${content}" >"${d}/changelog.d/${file}"
  fi
  before="$(snapshot "${d}")"
  run "${d}"
  expect_rc "${name}: exits 1" 1
  if [[ "$(snapshot "${d}")" == "${before}" ]]; then
    ok "${name}: changes nothing, valid fragments included"
  else
    fail "${name}: changes nothing, valid fragments included" "the tree changed"
  fi
  if [[ "${OUT}" == *"${want}"* ]]; then
    ok "${name}: says why"
  else
    fail "${name}: says why" "output missing '${want}': ${OUT}"
  fi
}

refuse "bad name, no number" "fix.fixed.md" '- x
' "name does not match"
refuse "bad name, missing .md" "12.fixed" '- x
' "name does not match"
refuse "bad name, leading zero" "012.fixed.md" '- x
' "name does not match"
refuse "bad name, capitalized section" "12.Fixed.md" '- x
' "name does not match"
refuse "bad name, stray file" "notes.txt" 'x
' "name does not match"
refuse "bad name, directory" "12.fixed.md/" '' "name does not match"
refuse "bad section" "12.bugfix.md" '- x
' "unknown section 'bugfix'"
refuse "deprecated is not a supported section" "12.deprecated.md" '- x
' "unknown section 'deprecated'"
refuse "empty file" "12.fixed.md" '' "empty fragment"
refuse "whitespace-only file" "12.fixed.md" '

' "empty fragment"
refuse "file not starting with a bullet" "12.fixed.md" 'A sentence, not a bullet.
' "must start with '- '"
refuse "bullet without its space" "12.fixed.md" '-x
' "must start with '- '"
refuse "suffix 1 collides with the unsuffixed name" "12-1.fixed.md" '- x
' "name does not match"
refuse "a release heading inside a fragment" "12.fixed.md" '- x
## [9.9.9]
' "heading line is not allowed"
refuse "a subsection heading inside a fragment" "12.fixed.md" '- x
### Added
' "heading line is not allowed"
refuse "CRLF line endings" "12.fixed.md" $'- x\r\n' "CRLF line endings"

# --- a fold keeps CHANGELOG.md's file mode ----------------------------------

d="$(new_tree keeps-mode)"
chmod 0644 "${d}/CHANGELOG.md"
printf -- '- new fix\n' >"${d}/changelog.d/10.fixed.md"
run "${d}"
expect_rc "a fold on a 0644 CHANGELOG.md exits 0" 0
if [[ -z "$(find "${d}/CHANGELOG.md" -perm 0644)" ]]; then
  fail "a fold keeps CHANGELOG.md at mode 0644" "$(ls -l "${d}/CHANGELOG.md")"
else
  ok "a fold keeps CHANGELOG.md at mode 0644"
fi

# --- every supported section name is accepted -------------------------------

d="$(new_tree all-sections)"
for s in added changed removed fixed security; do
  printf -- '- %s entry\n' "${s}" >"${d}/changelog.d/4.${s}.md"
done
run "${d}" --check
expect_rc "every supported section passes --check" 0

# --- --check validates and changes nothing ----------------------------------

d="$(new_tree check-valid)"
printf -- '- valid\n' >"${d}/changelog.d/10.fixed.md"
printf -- '- valid too\n' >"${d}/changelog.d/10-2.added.md"
before="$(snapshot "${d}")"
run "${d}" --check
expect_rc "--check on valid fragments exits 0" 0
if [[ "$(snapshot "${d}")" == "${before}" ]]; then
  ok "--check on valid fragments changes nothing"
else
  fail "--check on valid fragments changes nothing" "the tree changed"
fi

d="$(new_tree check-invalid)"
printf -- '- valid\n' >"${d}/changelog.d/10.fixed.md"
printf -- 'not a bullet\n' >"${d}/changelog.d/11.fixed.md"
before="$(snapshot "${d}")"
run "${d}" --check
expect_rc "--check on a bad fragment exits 1" 1
if [[ "$(snapshot "${d}")" == "${before}" ]]; then
  ok "--check on a bad fragment changes nothing"
else
  fail "--check on a bad fragment changes nothing" "the tree changed"
fi

# --- could not run: exit 2 ---------------------------------------------------

d="$(new_tree no-changelog)"
rm "${d}/CHANGELOG.md"
printf -- '- valid\n' >"${d}/changelog.d/10.fixed.md"
run "${d}"
expect_rc "no CHANGELOG.md exits 2" 2
if [[ -f "${d}/changelog.d/10.fixed.md" ]]; then
  ok "no CHANGELOG.md leaves the fragment"
else
  fail "no CHANGELOG.md leaves the fragment" "fragment deleted"
fi

d="$(new_tree no-unreleased)"
printf '# Changelog\n\n## [0.1.0]\n\n### Fixed\n\n- old\n' >"${d}/CHANGELOG.md"
printf -- '- valid\n' >"${d}/changelog.d/10.fixed.md"
before="$(snapshot "${d}")"
run "${d}"
expect_rc "no [Unreleased] section exits 2" 2
if [[ "$(snapshot "${d}")" == "${before}" ]]; then
  ok "no [Unreleased] section changes nothing"
else
  fail "no [Unreleased] section changes nothing" "the tree changed"
fi
run "${d}" --check
expect_rc "--check with no [Unreleased] section exits 2" 2

d="$(new_tree bad-arg)"
run "${d}" --fold
expect_rc "an unknown argument exits 2" 2

# --- result ----------------------------------------------------------------

printf '\nchangelog-assemble.test.sh: %s passed, %s failed\n' "${PASSED}" "${FAILED}"
if [[ "${FAILED}" -ne 0 ]]; then
  exit 1
fi
if [[ "${PASSED}" -lt 60 ]]; then
  printf 'changelog-assemble.test.sh: only %s cases ran; a suite that shrank silently is not a pass\n' \
    "${PASSED}" >&2
  exit 1
fi
exit 0
