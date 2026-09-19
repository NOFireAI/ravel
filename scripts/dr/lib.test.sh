#!/usr/bin/env bash
# Usage: scripts/dr/lib.test.sh [--help]
#
# Cases for the assertion helpers in scripts/dr/lib.sh (issue #814).
#
# The rehearsal's whole claim rests on these five functions: a figure that is
# absent, printed twice, out of band, or too large for bash's arithmetic must
# fail exactly as a wrong one does, and the key classifiers must tell an L0
# commit record apart from a compaction record that shares its prefix. A bug
# in any of them turns every band in the harness into decoration, and it would
# show up as a rehearsal that passes.
#
# The fixture strings are the real output shapes: verify-custody's indented
# summary block, `commit reconstruct`'s one-line summary, and the object key
# layout in docs/catalog-and-mvcc.md.
#
# Exit 0 when every case passes, 1 when one fails, 64 on bad usage.
set -uo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  printf 'Usage: scripts/dr/lib.test.sh\n\nRuns the cases for the figure-assertion helpers in scripts/dr/lib.sh.\n'
  exit 0
fi
if [[ $# -gt 0 ]]; then
  printf 'lib.test.sh: unexpected argument: %s\n' "$1" >&2
  exit 64
fi

# shellcheck source=scripts/dr/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

PASSED=0
FAILED=0

check() {
  local what="$1" want="$2" got="$3"
  if [[ "${want}" == "${got}" ]]; then
    PASSED=$((PASSED + 1))
    printf 'ok   %s\n' "${what}"
  else
    FAILED=$((FAILED + 1))
    printf 'FAIL %s: want "%s", got "%s"\n' "${what}" "${want}" "${got}"
  fi
}

# Run a helper and report only its exit code, so a case can assert a refusal.
rc_of() {
  local code=0
  "$@" >/dev/null 2>&1 || code=$?
  printf '%s\n' "${code}"
}

# --- dr_field_once ---------------------------------------------------------

CUSTODY='verify-custody summary:
  live data objects verified (content hash matches key): 6
  compaction inputs resolved and verified: 0
  compaction inputs no longer present (expected; legitimately swept): 0
  content-hash mismatches (ANOMALY): 0
  live objects missing from store (ANOMALY): 0
  total anomalies: 0'

check "field: the indented verify-custody figure" \
  "6" "$(dr_field_once '  live data objects verified (content hash matches key)' "${CUSTODY}")"
check "field: the anomaly total" \
  "0" "$(dr_field_once '  total anomalies' "${CUSTODY}")"
check "field: the one-line reconstruct summary" \
  "reconstructed=0 already_present_skipped=0 failed=0" \
  "$(dr_field_once 'reconstruct summary' \
    'reconstruct summary: reconstructed=0 already_present_skipped=0 failed=0')"
check "field: a figure printed twice is refused" \
  "1" "$(rc_of dr_field_once 'entry_count' 'entry_count: 1
entry_count: 2')"
check "field: a figure that is absent is refused" \
  "1" "$(rc_of dr_field_once 'entry_count' 'nothing was printed')"
# The label must anchor at the start of the line: a longer label that merely
# contains the short one must not answer for it.
check "field: a label is matched at the line start, not anywhere in it" \
  "1" "$(rc_of dr_field_once 'count' 'entry_count: 3')"

# --- dr_assert_figure ------------------------------------------------------

check "figure: inside the band passes" "0" "$(rc_of dr_assert_figure f 5 1 6)"
check "figure: at the low edge passes" "0" "$(rc_of dr_assert_figure f 1 1 6)"
check "figure: at the high edge passes" "0" "$(rc_of dr_assert_figure f 6 1 6)"
check "figure: below the band is refused" "1" "$(rc_of dr_assert_figure f 0 1 6)"
check "figure: above the band is refused" "1" "$(rc_of dr_assert_figure f 7 1 6)"
check "figure: a non-integer is refused" "1" "$(rc_of dr_assert_figure f 'many' 1 6)"
check "figure: an empty value is refused" "1" "$(rc_of dr_assert_figure f '' 1 6)"
# Bash arithmetic wraps past 64 bits, so an absurd value would otherwise
# compare as a negative number and land inside a band that starts at zero.
check "figure: a value too large for 64-bit arithmetic is refused" \
  "1" "$(rc_of dr_assert_figure f 99000000000000000000 0 6)"

# --- dr_assert_timing_lines ------------------------------------------------

TIMING='phase seed seconds=1.000
phase replicate seconds=2.000'
check "timing: one line per expected phase passes" \
  "0" "$(rc_of dr_assert_timing_lines "${TIMING}" seed replicate)"
check "timing: a phase that emitted nothing is refused" \
  "1" "$(rc_of dr_assert_timing_lines "${TIMING}" seed replicate start)"
check "timing: a phase that emitted twice is refused" \
  "1" "$(rc_of dr_assert_timing_lines "${TIMING}
phase seed seconds=3.000" seed replicate)"
check "timing: an unexpected extra phase line is refused" \
  "1" "$(rc_of dr_assert_timing_lines "${TIMING}
phase start seconds=4.000" seed replicate)"

# --- key classifiers -------------------------------------------------------

KEYS='t/ab12/m/l0/0/w1.1.1.aaaaaaaaaaaaaaaa.rseg
t/ab12/m/l0/1/w1.1.2.bbbbbbbbbbbbbbbb.rseg
t/ab12/m/c/0/2026091920/w1.1.1.cmt
t/ab12/m/c/1/2026091920/w1.1.2.cmt
t/ab12/m/c/0/2026091920/l1.deadbeefdeadbeef.cmt
t/ab12/m/c/0/2026091920/rw.deadbeefdeadbeef.cmt
t/ab12/m/c/0/2026091920/retire.tmb
t/ab12/config
t/ab12/metrics/prov
sys/qualification'

check "keys: L0 data objects" "2" "$(dr_count_lines "$(dr_l0_data_keys "${KEYS}")")"
# The l1./rw. compaction records and the retire tombstone share the commit
# prefix and are not L0 records; counting them would make the pairing check
# report a dangling record on a healthy bucket.
check "keys: L0 commit records exclude compaction records and tombstones" \
  "2" "$(dr_count_lines "$(dr_l0_commit_keys "${KEYS}")")"
check "keys: the single tenant prefix" "ab12" "$(dr_tenant_hash_from_keys "${KEYS}")"
check "keys: two tenant prefixes are refused" \
  "1" "$(rc_of dr_tenant_hash_from_keys "${KEYS}
t/cd34/m/l0/0/w2.1.1.cccccccccccccccc.rseg")"
check "keys: identity from a data object key" \
  "w1.1.1" "$(dr_l0_identity 't/ab12/m/l0/0/w1.1.1.aaaaaaaaaaaaaaaa.rseg')"
check "keys: identity from a commit record key" \
  "w1.1.1" "$(dr_l0_identity 't/ab12/m/c/0/2026091920/w1.1.1.cmt')"
check "keys: an empty listing counts zero" "0" "$(dr_count_lines '')"

# --- real-S3 endpoint handling ---------------------------------------------
#
# "Real S3" is the absence of an endpoint override, not a special value:
# S3Config.endpoint is Option<String> and None selects AWS's regional
# endpoint. An exported empty RAVEL_S3_ENDPOINT would not be None.

(
  DR_ENDPOINT=""
  dr_export_s3_env "some-bucket"
  if [[ -n "${RAVEL_S3_ENDPOINT+set}" ]]; then
    printf 'FAIL endpoint: an empty DR_ENDPOINT still exported RAVEL_S3_ENDPOINT\n'
    exit 1
  fi
  exit 0
)
check "endpoint: an empty DR_ENDPOINT exports no RAVEL_S3_ENDPOINT" "0" "$?"

(
  DR_ENDPOINT="http://127.0.0.1:9000"
  dr_export_s3_env "some-bucket"
  [[ "${RAVEL_S3_ENDPOINT:-}" == "http://127.0.0.1:9000" ]]
)
check "endpoint: a set DR_ENDPOINT is exported unchanged" "0" "$?"

# --- result ----------------------------------------------------------------

printf '\nlib.test.sh: %s passed, %s failed\n' "${PASSED}" "${FAILED}"
if [[ "${FAILED}" -ne 0 ]]; then
  exit 1
fi
if [[ "${PASSED}" -lt 25 ]]; then
  printf 'lib.test.sh: only %s cases ran; a suite that shrank silently is not a pass\n' \
    "${PASSED}" >&2
  exit 1
fi
exit 0
