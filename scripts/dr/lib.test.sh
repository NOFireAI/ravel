#!/usr/bin/env bash
# Usage: scripts/dr/lib.test.sh [--help]
#
# Cases for the assertion helpers in scripts/dr/lib.sh (issue #814).
#
# The rehearsal's whole claim rests on these functions: a figure that is
# absent, printed twice, out of band, or too large for bash's arithmetic must
# fail exactly as a wrong one does; the key classifiers must tell an L0 commit
# record apart from a compaction record that shares its prefix; the forged-seq
# arithmetic must stay in base 10 and keep the key layout's width; the custody
# items must refuse an undeclared value; and the marker ordering must refuse a
# marker older than the restore that is supposed to have written it. A bug in
# any of them turns a band in the harness into decoration, and it would show up
# as a rehearsal that passes.
#
# The fixture strings are the real output shapes: verify-custody's indented
# summary block, `commit reconstruct`'s one-line summary, and the object key
# layout in docs/catalog-and-mvcc.md, padded exactly as
# crates/ravel-commit/src/keys.rs formats it.
#
# Exit 0 when every case passes, 1 when one fails, 64 on bad usage.
#
# shellcheck disable=SC2016  # the `bash -c` bodies below must not expand here
set -uo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  printf 'Usage: scripts/dr/lib.test.sh\n\nRuns the cases for the figure-assertion helpers in scripts/dr/lib.sh.\n'
  exit 0
fi
if [[ $# -gt 0 ]]; then
  printf 'lib.test.sh: unexpected argument: %s\n' "$1" >&2
  exit 64
fi

DR_LIB_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# shellcheck source=scripts/dr/lib.sh
source "${DR_LIB_PATH}"

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

# The same, in a subshell, for helpers that refuse by calling dr_die (which
# exits). Without the subshell a refusal would end this suite.
rc_sub() {
  local code=0
  ("$@") >/dev/null 2>&1 || code=$?
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
#
# Real keys, in the exact shape crates/ravel-commit/src/keys.rs writes and its
# parser accepts: a 32-hex tenant hash, a four-digit shard, a YYYYMMDDTHH
# ingest hour, and a TWENTY-DIGIT zero-padded sequence number. A fixture with
# a short unpadded seq cannot catch a forged key that is the wrong width, and
# one with no leading zeros cannot catch base-8 arithmetic.

TH='0123456789abcdef0123456789abcdef'
W='1b4e28ba-2fa1-11d2-883f-0016d3cca427'
KEYS="t/${TH}/m/l0/0000/${W}.7.00000000000000000001.aaaaaaaaaaaaaaaa.rseg
t/${TH}/m/l0/0001/${W}.7.00000000000000000008.bbbbbbbbbbbbbbbb.rseg
t/${TH}/m/c/0000/20260919T20/${W}.7.00000000000000000001.cmt
t/${TH}/m/c/0001/20260919T20/${W}.7.00000000000000000008.cmt
t/${TH}/m/c/0000/20260919T20/l1.deadbeefdeadbeef.cmt
t/${TH}/m/c/0000/20260919T20/rw.deadbeefdeadbeef.cmt
t/${TH}/m/c/0000/20260919T20/retire.tmb
t/${TH}/config
t/${TH}/metrics/prov
sys/qualification"

check "keys: L0 data objects" "2" "$(dr_count_lines "$(dr_l0_data_keys "${KEYS}")")"
# The l1./rw. compaction records and the retire tombstone share the commit
# prefix and are not L0 records; counting them would make the pairing check
# report a dangling record on a healthy bucket.
check "keys: L0 commit records exclude compaction records and tombstones" \
  "2" "$(dr_count_lines "$(dr_l0_commit_keys "${KEYS}")")"
check "keys: the single tenant prefix" "${TH}" "$(dr_tenant_hash_from_keys "${KEYS}")"
check "keys: two tenant prefixes are refused" \
  "1" "$(rc_of dr_tenant_hash_from_keys "${KEYS}
t/ffffffffffffffffffffffffffffffff/m/l0/0000/${W}.7.00000000000000000001.cccccccccccccccc.rseg")"
check "keys: identity from a data object key" \
  "${W}.7.00000000000000000001" \
  "$(dr_l0_identity "t/${TH}/m/l0/0000/${W}.7.00000000000000000001.aaaaaaaaaaaaaaaa.rseg")"
check "keys: identity from a commit record key" \
  "${W}.7.00000000000000000001" \
  "$(dr_l0_identity "t/${TH}/m/c/0000/20260919T20/${W}.7.00000000000000000001.cmt")"
check "keys: an empty listing counts zero" "0" "$(dr_count_lines '')"

# The harness's own bookkeeping is not corpus. A bucket holding only its
# creation marker is an empty restore target, and a marker counted as an
# object would put every mirrored-object band off by one.
HARNESS_KEYS="dr/rehearsal-bucket.json
dr/reconciled.json
dr/restore-start.json
${KEYS}"
check "keys: the dr/ harness prefix is dropped from a listing" \
  "10" "$(dr_count_lines "$(dr_strip_harness_keys "${HARNESS_KEYS}")")"
check "keys: a dr/ key is not counted as an L0 data object" \
  "2" "$(dr_count_lines "$(dr_l0_data_keys "$(dr_strip_harness_keys "${HARNESS_KEYS}")")")"

# --- finding 3: the forged sequence number ---------------------------------
#
# The dangling-commit-record fault offsets a commit key's seq. Two bugs live
# here and this fixture catches both. A zero-padded value inside $(( )) is
# parsed as OCTAL, so a seq containing an 8 aborts with "value too great for
# base" and every other seq is read in base 8; and the sum is unpadded, so the
# forged key carries a nine-digit seq that the key parser rejects as malformed,
# which would make the fault pass for the wrong reason.

check "seq: a padded seq containing an 8 is read in base 10, not octal" \
  "00000000000900000009" "$(dr_forged_seq 00000000000000000008)"
check "seq: a padded seq containing a 9 is read in base 10, not octal" \
  "00000000000900000010" "$(dr_forged_seq 00000000000000000009)"
FORGED="$(dr_forged_seq 00000000000000000001)"
check "seq: the forged seq is exactly 20 digits wide" "20" "${#FORGED}"
check "seq: the forged seq is the base-10 sum" "00000000000900000002" "${FORGED}"
check "seq: a seq of the wrong width is refused" \
  "1" "$(rc_of dr_forged_seq 1)"
check "seq: a 19-digit seq is refused" \
  "1" "$(rc_of dr_forged_seq 0000000000000000001)"
check "seq: a non-numeric seq component is refused" \
  "1" "$(rc_of dr_forged_seq 0000000000000000000x)"
check "seq: a seq too large for bash arithmetic is refused" \
  "1" "$(rc_of dr_forged_seq 99999999999999999999)"

# --- finding 4: the custody items are declared, never defaulted ------------
#
# "Unset" and "deliberately none" are different answers and only one of them
# is a restore-ready deployment. A default of `none` (or of `unkeyed`) makes
# restore-check's custody phase report the declaration the default made on the
# operator's behalf, which is a check that cannot fail.

check "custody: an unset value is refused" \
  "1" "$(rc_of dr_custody_declared DR_TENANT_KMS_CONFIG '')"
check "custody: the literal none is accepted" \
  "0" "$(rc_of dr_custody_declared DR_TENANT_KMS_CONFIG none)"
check "custody: a readable file is accepted" \
  "0" "$(rc_of dr_custody_declared DR_ADMIN_CREDENTIAL_FILE "${DR_LIB_PATH}")"
check "custody: a named file that is not readable is refused" \
  "1" "$(rc_of dr_custody_declared DR_ADMIN_CREDENTIAL_FILE /nonexistent/dr-admin-cred)"

# The defaults themselves, re-read in a subshell with the variables unset.
(
  unset DR_TENANT_KMS_CONFIG DR_ADMIN_CREDENTIAL_FILE DR_TENANT_HASH_MODE
  # shellcheck source=scripts/dr/lib.sh
  source "${DR_LIB_PATH}"
  [[ -z "${DR_TENANT_KMS_CONFIG}" ]] || exit 1
  [[ -z "${DR_ADMIN_CREDENTIAL_FILE}" ]] || exit 1
  [[ -z "${DR_TENANT_HASH_MODE}" ]] || exit 1
  exit 0
)
check "custody: the three custody variables default to empty, not to a declaration" \
  "0" "$?"

check "custody: dr_init refuses an undeclared tenant hash mode" \
  "64" "$(rc_sub bash -c '
    source "$1"
    DR_BUCKET_PRIMARY=a-primary
    DR_BUCKET_REPLICA=a-replica
    DR_ACCESS_KEY=k
    DR_SECRET_KEY=s
    DR_TENANT_HASH_MODE=""
    dr_init' _ "${DR_LIB_PATH}")"
check "custody: dr_init accepts a declared tenant hash mode" \
  "0" "$(rc_sub bash -c '
    source "$1"
    DR_BUCKET_PRIMARY=a-primary
    DR_BUCKET_REPLICA=a-replica
    DR_ACCESS_KEY=k
    DR_SECRET_KEY=s
    DR_TENANT_HASH_MODE=unkeyed
    dr_init' _ "${DR_LIB_PATH}")"

# --- finding 1: the buckets are named, never guessed -----------------------

(
  unset DR_BUCKET_PRIMARY DR_BUCKET_REPLICA
  # shellcheck source=scripts/dr/lib.sh
  source "${DR_LIB_PATH}"
  [[ -z "${DR_BUCKET_PRIMARY}" ]] || exit 1
  [[ -z "${DR_BUCKET_REPLICA}" ]] || exit 1
  [[ -z "${DR_ACCESS_KEY}" ]] || exit 1
  [[ -z "${DR_SECRET_KEY}" ]] || exit 1
  exit 0
)
check "buckets: neither bucket and neither credential carries a default" "0" "$?"

check "buckets: an unset primary is refused" \
  "64" "$(rc_sub bash -c '
    source "$1"
    DR_BUCKET_PRIMARY=""
    DR_BUCKET_REPLICA="a-replica"
    dr_require_buckets' _ "${DR_LIB_PATH}")"
check "buckets: an unset replica is refused" \
  "64" "$(rc_sub bash -c '
    source "$1"
    DR_BUCKET_PRIMARY="a-primary"
    DR_BUCKET_REPLICA=""
    dr_require_buckets' _ "${DR_LIB_PATH}")"
# A single bucket name for both would make the "restore into an empty bucket"
# step a recursive delete of the primary.
check "buckets: the same name for both is refused" \
  "64" "$(rc_sub bash -c '
    source "$1"
    DR_BUCKET_PRIMARY="same-bucket"
    DR_BUCKET_REPLICA="same-bucket"
    dr_require_buckets' _ "${DR_LIB_PATH}")"
check "buckets: two distinct names are accepted" \
  "0" "$(rc_sub bash -c '
    source "$1"
    DR_BUCKET_PRIMARY="a-primary"
    DR_BUCKET_REPLICA="a-replica"
    dr_require_buckets' _ "${DR_LIB_PATH}")"
check "buckets: an unusable bucket name is refused" \
  "64" "$(rc_sub bash -c '
    source "$1"
    DR_BUCKET_PRIMARY="Not A Bucket"
    DR_BUCKET_REPLICA="a-replica"
    dr_require_buckets' _ "${DR_LIB_PATH}")"
check "credentials: an unset access key is refused" \
  "64" "$(rc_sub bash -c '
    source "$1"
    DR_ACCESS_KEY=""
    DR_SECRET_KEY="s"
    dr_require_credentials' _ "${DR_LIB_PATH}")"
check "credentials: an unset secret key is refused" \
  "64" "$(rc_sub bash -c '
    source "$1"
    DR_ACCESS_KEY="k"
    DR_SECRET_KEY=""
    dr_require_credentials' _ "${DR_LIB_PATH}")"

# --- finding 2: the mc credential path carries a session token -------------
#
# A secret access key routinely contains `/` and `+`. An unencoded `/` in the
# userinfo truncates the host, so the URL names a different endpoint with no
# error to read, and an STS credential with no session-token component 403s on
# every mc call while the ravel binaries (which get the token through
# RAVEL_S3_SESSION_TOKEN) succeed.

check "credentials: a secret containing a slash is percent-encoded" \
  "a%2Fb%2Bc%3Dd" "$(dr_urlencode 'a/b+c=d')"
check "credentials: an unreserved value is left alone" \
  "Abc-123._~" "$(dr_urlencode 'Abc-123._~')"
check "credentials: the host URL carries a session token when one is set" \
  "http://key:sec%2Fret:tok%2Fen@127.0.0.1:9000" \
  "$(DR_MC_HOST_URL="" DR_ENDPOINT="http://127.0.0.1:9000" DR_ACCESS_KEY="key" \
    DR_SECRET_KEY="sec/ret" DR_SESSION_TOKEN="tok/en" dr_mc_host_url)"
check "credentials: with no session token the URL has two components" \
  "http://key:sec%2Fret@127.0.0.1:9000" \
  "$(DR_MC_HOST_URL="" DR_ENDPOINT="http://127.0.0.1:9000" DR_ACCESS_KEY="key" \
    DR_SECRET_KEY="sec/ret" DR_SESSION_TOKEN="" dr_mc_host_url)"
check "credentials: no endpoint means the regional S3 host" \
  "https://key:sec@s3.eu-west-2.amazonaws.com" \
  "$(DR_MC_HOST_URL="" DR_ENDPOINT="" DR_REGION="eu-west-2" DR_ACCESS_KEY="key" \
    DR_SECRET_KEY="sec" DR_SESSION_TOKEN="" dr_mc_host_url)"
check "credentials: a session token with a token-less DR_MC_HOST_URL is refused" \
  "64" "$(rc_sub bash -c '
    source "$1"
    DR_SESSION_TOKEN="tok"
    DR_MC_HOST_URL="https://key:secret@s3.amazonaws.com"
    dr_assert_mc_can_carry_session_token' _ "${DR_LIB_PATH}")"
check "credentials: a session token with a three-component URL is accepted" \
  "0" "$(rc_sub bash -c '
    source "$1"
    DR_SESSION_TOKEN="tok"
    DR_MC_HOST_URL="https://key:secret:tok@s3.amazonaws.com"
    dr_assert_mc_can_carry_session_token' _ "${DR_LIB_PATH}")"

# --- finding 6: the marker must post-date the restore it belongs to --------
#
# restore-check.sh stamps the bucket before its first check and start.sh
# requires the reconciled marker to post-date that stamp, so a marker left by
# an EARLIER restore of the same bucket is refused on its timestamp alone.
# "Not stamped in the future" was the old check and it accepts any marker old
# enough.

check "marker: a marker written after the restore started is accepted" \
  "0" "$(rc_of dr_ns_not_after 1758300000000000000 1758300000000000001)"
check "marker: a marker written before the restore started is refused" \
  "1" "$(rc_of dr_ns_not_after 1758300000000000001 1758300000000000000)"
check "marker: an equal stamp is accepted" \
  "0" "$(rc_of dr_ns_not_after 1758300000000000000 1758300000000000000)"
# Compared as equal-width strings, never with bash arithmetic: a twenty-digit
# stamp wraps past the 64-bit range and would read as safely in the past.
check "marker: a twenty-digit stamp does not wrap into the past" \
  "1" "$(rc_of dr_ns_not_after 99999999999999999999 1758300000000000000)"
check "marker: stamps of different widths compare by value" \
  "0" "$(rc_of dr_ns_not_after 999999999 1758300000000000000)"
check "marker: a non-numeric stamp is refused with its own code" \
  "2" "$(rc_of dr_ns_not_after 'yesterday' 1758300000000000000)"

# --- real-S3 endpoint handling ---------------------------------------------
#
# "Real S3" is the absence of an endpoint override, not a special value:
# S3Config.endpoint is Option<String> and None selects AWS's regional
# endpoint. An exported empty RAVEL_S3_ENDPOINT would not be None.

(
  DR_ENDPOINT=""
  dr_export_s3_env "some-bucket"
  # shellcheck disable=SC2031  # the subshell is the point: it reads what dr_export_s3_env set
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
  # shellcheck disable=SC2031
  [[ "${RAVEL_S3_ENDPOINT:-}" == "http://127.0.0.1:9000" ]]
)
check "endpoint: a set DR_ENDPOINT is exported unchanged" "0" "$?"

(
  DR_SESSION_TOKEN="a-session-token"
  dr_export_s3_env "some-bucket"
  # shellcheck disable=SC2031
  [[ "${RAVEL_S3_SESSION_TOKEN:-}" == "a-session-token" ]]
)
check "endpoint: a session token reaches the ravel binaries too" "0" "$?"

# --- result ----------------------------------------------------------------

printf '\nlib.test.sh: %s passed, %s failed\n' "${PASSED}" "${FAILED}"
if [[ "${FAILED}" -ne 0 ]]; then
  exit 1
fi
if [[ "${PASSED}" -lt 60 ]]; then
  printf 'lib.test.sh: only %s cases ran; a suite that shrank silently is not a pass\n' \
    "${PASSED}" >&2
  exit 1
fi
exit 0
