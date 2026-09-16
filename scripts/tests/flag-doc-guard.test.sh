#!/usr/bin/env bash
# Coverage for gates.sh's `--flag-doc-guard-only` mode (issue #1297): a
# known-good run against the real, committed config.rs, and a known-bad run
# against a temporary copy with the gzip qualification stripped from one
# flag's doc block, each asserting the guard's exit code.
#
# Run by the doc-scripts job in .github/workflows/ci.yml, and by hand with
#   bash scripts/tests/flag-doc-guard.test.sh
#
# It was wired in by issue #1834, which found it running in no job at all --
# and RED, because its fixture had been pinned to a sentence in config.rs
# that was later reworded. The sentence this comment replaces said the wiring
# was "out of scope for the change that added this file", which was a
# reasonable scope call that then went stale and kept the suite invisible.
# scripts/guards/check-test-suites-run.sh now fails when any tracked
# *.test.sh runs nowhere, so that state cannot return quietly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
GATES="${SCRIPT_DIR}/gates.sh"
REAL_CONFIG="${SCRIPT_DIR}/../services/ravel-server/src/config.rs"

pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

# === (a) known-good: the guard passes against the real, committed config.rs
rc=0
bash "${GATES}" --flag-doc-guard-only "${REAL_CONFIG}" >/dev/null 2>&1 || rc=$?
check_eq "the real config.rs passes the flag-doc guard" "0" "${rc}"

# === (b) known-bad: a copy with the gzip qualification stripped from the
#     FIRST doc block that carries it. The doc still claims a memory bound,
#     and other blocks keep their own qualification, so this still pins the
#     guard's per-block scan rather than a file-wide one: a file-wide check
#     would pass on the remaining blocks' wording.
#
#     Found by the word, not by one exact sentence. The previous version
#     sed'd a literal `OTLP HTTP gzip inflate, which`; config.rs was reworded
#     to `the OTLP HTTP gzip inflate and the Remote`, the sed became a no-op,
#     and the fixture equalled the original. The suite's own
#     fixture-differs assertion caught that and had been failing ever since --
#     unseen, because the suite ran in no CI job (issue #1834). A fixture
#     pinned to prose rots the first time the prose is edited.
tmproot="$(mktemp -d "${TMPDIR:-/tmp}/flag-doc-guard-test.XXXXXX")"
if [[ ! -d "${tmproot}" ]]; then
  echo "FAIL  could not create a temp dir for the fixture" >&2
  exit 1
fi
trap 'rm -rf "${tmproot}"' EXIT
bad_config="${tmproot}/config.rs"
cp "${REAL_CONFIG}" "${bad_config}" || exit 1
if [[ ! -f "${bad_config}" ]]; then
  echo "FAIL  cp did not produce the fixture at ${bad_config}" >&2
  exit 1
fi
# Strip `gzip` from the first doc block that pairs it with inflate or
# decompress, leaving every later block intact.
awk '
  /^[[:space:]]*\/\/\// { in_doc = 1 }
  !/^[[:space:]]*\/\/\// { if (in_doc && done_block) done = 1; in_doc = 0; done_block = 0 }
  {
    # One condition for both: latch the block as done only when a
    # substitution actually happened. Latching on a `tolower` match while
    # substituting only two exact cases meant a title-case `Gzip` marked the
    # block done without editing it, and no later block was stripped either.
    # The bracket form covers every casing, which is what the latch accepts.
    if (!done && in_doc && gsub(/[Gg][Zz][Ii][Pp]/, "compressed", $0) > 0) {
      done_block = 1
    }
    print
  }
' "${REAL_CONFIG}" >"${bad_config}"
if cmp -s "${REAL_CONFIG}" "${bad_config}"; then
  echo "FAIL  fixture is identical to ${REAL_CONFIG}; the sed edit did not \
strip the gzip qualification, so the known-bad case would test nothing" >&2
  exit 1
fi

rc=0
bash "${GATES}" --flag-doc-guard-only "${bad_config}" >/dev/null 2>&1 || rc=$?
check_eq "stripping the gzip qualification from one flag's doc fails the guard" "1" "${rc}"

echo
echo "passed: ${pass}  failed: ${fail}"
[[ ${fail} -eq 0 ]]
