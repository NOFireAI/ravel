#!/usr/bin/env bash
# Coverage for gates.sh's `--flag-doc-guard-only` mode (issue #1297): a
# known-good run against the real, committed config.rs, and a known-bad run
# against a temporary copy with the gzip qualification stripped from one
# flag's doc block, each asserting the guard's exit code.
#
# Not wired into any CI job: the workflow that runs the other scripts/
# guard-test cases (the doc-scripts job in .github/workflows/ci.yml) is out
# of scope for the change that added this file. Run by hand:
#   bash scripts/tests/flag-doc-guard.test.sh
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

# === (b) known-bad: a copy with "gzip" removed from the one sentence that
#     pairs it with "inflate" in max_inflight_ingest_requests's doc block.
#     The doc still claims a memory bound (it still says "resident memory"
#     and still contains the unrelated word "decompression" from Remote
#     Write's cap, in a different sentence with no "gzip" nearby), but the
#     gzip-qualified inflate/decompress sentence the guard requires is gone.
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
sed -i.bak 's/OTLP HTTP gzip inflate, which/OTLP HTTP transient, which/' "${bad_config}"
rm -f "${bad_config}.bak"
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
