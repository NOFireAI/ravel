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
DR_DIR="$(dirname "${DR_LIB_PATH}")"
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
  "10" "$(dr_count_lines "$(dr_strip_noncorpus_keys "${HARNESS_KEYS}")")"
check "keys: a dr/ key is not counted as an L0 data object" \
  "2" "$(dr_count_lines "$(dr_l0_data_keys "$(dr_strip_noncorpus_keys "${HARNESS_KEYS}")")")"

# `ravel-cli store qualify` writes scratch fixtures under sys/qualify/<run-id>/
# and deletes none of them, so they sit in bucket A for the rest of the
# rehearsal. They are tooling output, not corpus: counting them puts every band
# derived from the export count off by however many keys the listing probes
# happened to write.
QUALIFY_KEYS="sys/qualify/019265f4-0000-7000-8000-000000000001/list/key-0000
sys/qualify/019265f4-0000-7000-8000-000000000001/list/key-0001
sys/qualify/019265f4-0000-7000-8000-000000000001/order/a
${KEYS}"
check "keys: the sys/qualify/ probe prefix is dropped from a listing" \
  "10" "$(dr_count_lines "$(dr_strip_noncorpus_keys "${QUALIFY_KEYS}")")"
check "keys: a listing of only probe keys counts zero" \
  "0" "$(dr_count_lines "$(dr_strip_noncorpus_keys \
    "sys/qualify/019265f4-0000-7000-8000-000000000001/list/key-0000
sys/qualify/019265f4-0000-7000-8000-000000000001/order/a")")"
# sys/qualification is the DURABLE qualification record a non-Memory store
# refuses to serve without (ADR-0050 EC7), not scratch. The two names share ten
# characters and diverge at the eleventh, so a prefix test written against
# `sys/qualif` would swallow the control object the seed's own band counts on.
check "keys: sys/qualification survives the sys/qualify/ exclusion" \
  "1" "$(dr_count_lines "$(dr_strip_noncorpus_keys 'sys/qualification')")"
# Both families at once, since both reach a listing on bucket A at the same
# time once the seed has qualified the store.
check "keys: the dr/ and sys/qualify/ prefixes are dropped together" \
  "10" "$(dr_count_lines "$(dr_strip_noncorpus_keys "dr/rehearsal-bucket.json
sys/qualify/019265f4-0000-7000-8000-000000000001/list/key-0000
${KEYS}")")"

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
AUDIT_KEY_DIR="$(mktemp -d)"
AUDIT_KEY_OK="${AUDIT_KEY_DIR}/ok.key"
AUDIT_KEY_SHORT="${AUDIT_KEY_DIR}/short.key"
AUDIT_KEY_NOT_HEX="${AUDIT_KEY_DIR}/not-hex.key"
printf '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n' >"${AUDIT_KEY_OK}"
printf '000102030405060708090a0b0c0d0e0f\n' >"${AUDIT_KEY_SHORT}"
printf 'zz0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n' >"${AUDIT_KEY_NOT_HEX}"

# dr_init under a declared mode, in a fresh shell. $1 is the mode, $2 the
# audit key file, $3 the caller's own RAVEL_AUDIT_TOKEN_KEY (empty for none).
# Prints three fields: dr_init's exit code, the key the server launch will
# receive (DR_SERVER_AUDIT_KEY), and what a child process the harness starts
# afterwards sees in RAVEL_AUDIT_TOKEN_KEY ("unset" when nothing).
audit_init() {
  bash -c '
    unset RAVEL_AUDIT_TOKEN_KEY
    [[ -n "$4" ]] && export RAVEL_AUDIT_TOKEN_KEY="$4"
    source "$1"
    DR_BUCKET_PRIMARY=a-primary
    DR_BUCKET_REPLICA=a-replica
    DR_ACCESS_KEY=k
    DR_SECRET_KEY=s
    DR_TENANT_HASH_MODE="$2"
    DR_TENANT_HASH_KEY_FILE="$5"
    DR_AUDIT_TOKEN_KEY_FILE="$3"
    DR_LOG_DIR="$6"
    rc=0
    (dr_init >/dev/null 2>&1) || rc=$?
    [[ "${rc}" -eq 0 ]] && dr_init >/dev/null 2>&1
    child="$(bash -c "printf %s \"\${RAVEL_AUDIT_TOKEN_KEY:-unset}\"")"
    printf "%s|%s|%s\n" "${rc}" "${DR_SERVER_AUDIT_KEY:-}" "${child}"' \
    _ "${DR_LIB_PATH}" "$1" "$2" "$3" "${AUDIT_KEY_OK}" "${AUDIT_KEY_DIR}/log"
}
OK_KEY=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
CALLER_KEY=ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100

check "custody: unkeyed hands the file's key to the server launch and to no other child" \
  "0|${OK_KEY}|unset" "$(audit_init unkeyed "${AUDIT_KEY_OK}" "")"
check "custody: unkeyed without DR_AUDIT_TOKEN_KEY_FILE is refused" \
  "64" "$(audit_init unkeyed "" "" | cut -d'|' -f1)"
check "custody: unkeyed with an unreadable audit key file is refused" \
  "64" "$(audit_init unkeyed "${AUDIT_KEY_DIR}/absent.key" "" | cut -d'|' -f1)"
check "custody: an audit key shorter than 64 hex characters is refused" \
  "64" "$(audit_init unkeyed "${AUDIT_KEY_SHORT}" "" | cut -d'|' -f1)"
check "custody: an audit key with a non-hex character is refused" \
  "64" "$(audit_init unkeyed "${AUDIT_KEY_NOT_HEX}" "" | cut -d'|' -f1)"
check "custody: keyed mode needs no audit key file and hands the server none" \
  "0||unset" "$(audit_init keyed "" "")"
check "custody: keyed mode passes a caller's explicit audit key to the server" \
  "0|${CALLER_KEY}|${CALLER_KEY}" "$(audit_init keyed "" "${CALLER_KEY}")"
check "custody: a caller's explicit audit key that is not 64 hex characters is refused" \
  "64" "$(audit_init keyed "" "${CALLER_KEY%?}" | cut -d'|' -f1)"
check "custody: a directory as the audit key file is refused as not a readable file" \
  "64" "$(audit_init unkeyed "${AUDIT_KEY_DIR}" "" | cut -d'|' -f1)"

# Every server launch carries the key as a prefix assignment, so the server,
# and only the server, receives it. Counted per script: a launch added without
# it fails here.
# Both counts are pinned to the number of launches each script has, so a
# launch line that stops matching the pattern fails instead of reading 0 = 0.
for pair in seed.sh:1 start.sh:2; do
  launcher="${pair%%:*}"
  expected="${pair#*:}"
  launches="$(grep -c '"${SERVER_ARGV\[@\]}"' "${DR_DIR}/${launcher}")"
  keyed_launches="$(grep -c 'RAVEL_AUDIT_TOKEN_KEY="${DR_SERVER_AUDIT_KEY}" \(exec \)\{0,1\}"${SERVER_ARGV\[@\]}"' "${DR_DIR}/${launcher}")"
  check "custody: ${launcher} has ${expected} ravel-server launch(es), each passing the audit key" \
    "${expected}:${expected}" "${launches}:${keyed_launches}"
done

# --- finding 1: the buckets are named, never guessed -----------------------

# All four are unset before sourcing, so the case reads lib.sh rather than the
# caller: the rehearsal workflow exports both credentials for the whole job.
no_bucket_or_credential_default() (
  unset DR_BUCKET_PRIMARY DR_BUCKET_REPLICA DR_ACCESS_KEY DR_SECRET_KEY
  # shellcheck source=scripts/dr/lib.sh
  source "${DR_LIB_PATH}"
  [[ -z "${DR_BUCKET_PRIMARY}" ]] || exit 1
  [[ -z "${DR_BUCKET_REPLICA}" ]] || exit 1
  [[ -z "${DR_ACCESS_KEY}" ]] || exit 1
  [[ -z "${DR_SECRET_KEY}" ]] || exit 1
  exit 0
)
no_bucket_or_credential_default
check "buckets: neither bucket and neither credential carries a default" "0" "$?"
(
  export DR_BUCKET_PRIMARY=env-primary DR_BUCKET_REPLICA=env-replica
  export DR_ACCESS_KEY=env-access-key DR_SECRET_KEY=env-secret-key
  no_bucket_or_credential_default
)
check "buckets: the no-default case ignores all four set in the caller's environment" \
  "0" "$?"

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

# --- finding 2: the client credential path carries a session token ---------
#
# The AWS CLI receives its credentials through the environment and never in
# argv, and an EMPTY AWS_SESSION_TOKEN is not the same as an absent one: the
# signer then sends an empty x-amz-security-token and every call 403s while
# the ravel binaries (which get the token through RAVEL_S3_SESSION_TOKEN)
# succeed.

# A stub client that reports its own argv and the credential variables that
# reached it, so dr_aws is observable without a network call.
AWS_STUB="$(mktemp)"
{
  printf '#!/usr/bin/env bash\n'
  printf 'printf "argv=%%s\\n" "$*"\n'
  printf 'printf "token=%%s\\n" "${AWS_SESSION_TOKEN-<unset>}"\n'
  printf 'printf "key=%%s\\n" "${AWS_ACCESS_KEY_ID-<unset>}"\n'
  printf 'printf "region=%%s\\n" "${AWS_DEFAULT_REGION-<unset>}"\n'
} >"${AWS_STUB}"
chmod +x "${AWS_STUB}"

# $1 endpoint, $2 region, $3 session token, $4 the field to read back.
aws_probe() {
  DR_AWS="${AWS_STUB}" DR_ENDPOINT="$1" DR_REGION="$2" DR_ACCESS_KEY="key" \
    DR_SECRET_KEY="sec/ret" DR_SESSION_TOKEN="$3" \
    dr_aws s3api list-buckets \
    | awk -F= -v field="$4" '$1 == field { print $2 }'
}

check "credentials: a session token reaches the client when one is set" \
  "tok/en" "$(aws_probe "http://127.0.0.1:9000" us-east-1 "tok/en" token)"
check "credentials: with no session token the variable is unset, not empty" \
  "<unset>" "$(aws_probe "http://127.0.0.1:9000" us-east-1 "" token)"
check "credentials: the access key travels in the environment, not in argv" \
  "key" "$(aws_probe "http://127.0.0.1:9000" us-east-1 "" key)"
check "credentials: an endpoint override is passed to the client" \
  "--endpoint-url http://127.0.0.1:9000 s3api list-buckets" \
  "$(aws_probe "http://127.0.0.1:9000" us-east-1 "" argv)"
check "credentials: an empty endpoint passes no override at all" \
  "s3api list-buckets" "$(aws_probe "" eu-west-2 "" argv)"
check "credentials: the region reaches the client" \
  "eu-west-2" "$(aws_probe "" eu-west-2 "" region)"
rm -f "${AWS_STUB}"

# Every listing in the harness reads a `--output text` list query, which prints
# its values tab separated, one line per response page, and the literal `None`
# for a page that matched nothing. A split that kept either shape would count
# a whole page as one key, or an empty bucket as one.
check "listing: a tab-separated page becomes one key per line" \
  "t/a
t/b
t/c" "$(dr_text_list_lines "$(printf 't/a\tt/b\nt/c\n')")"
check "listing: a page that matched nothing contributes no key" \
  "" "$(dr_text_list_lines "None")"

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

# --- dr_reset_bucket listing status ----------------------------------------

# The emptiness checks around the recursive delete must read the LISTING's
# status, not the line counter's. Nesting one inside the other reports the
# counter, which always exits 0 and prints 0 for empty input, so a listing
# that failed would read as an empty bucket. The assertion is on ORDER, not
# only the exit code: a failed listing must stop the pass before any delete
# command is issued, which is the property that matters when the next step is
# a recursive delete.

# Runs dr_reset_bucket with the listing failing at the given stage and prints
# "<exit code>:<delete commands issued>".
reset_probe() {
  local fail_stage="$1" state delete_log
  state="$(mktemp)"
  delete_log="$(mktemp)"
  local code=0
  (
    DR_LOG_DIR="$(mktemp -d)"
    dr_bucket_marker_present() { return 0; }
    dr_write_bucket_marker() { return 0; }
    # Records every delete pass the reset issues, so a case can assert none ran.
    dr_delete_all_versions() { printf 'delete\n' >>"${delete_log}"; return 0; }
    # Each command substitution is its own subshell, so the call count lives
    # in a file; a variable would reset on every call.
    dr_list_all_versions() {
      local nth
      nth="$(wc -l <"${state}" | tr -d ' ')"
      printf 'call\n' >>"${state}"
      if [[ "${fail_stage}" == "before" && "${nth}" -eq 0 ]]; then return 1; fi
      if [[ "${fail_stage}" == "after" && "${nth}" -ge 1 ]]; then return 1; fi
      return 0
    }
    dr_reset_bucket b 0
  ) >/dev/null 2>&1 || code=$?
  printf '%s:%s\n' "${code}" "$(wc -l <"${delete_log}" | tr -d ' ')"
  rm -f "${state}" "${delete_log}"
}

# A listing that fails before the delete must refuse AND issue no delete.
# shellcheck disable=SC2031  # read in the parent; the probe's subshell only stubs
check "reset: a failed listing before the delete refuses and deletes nothing" \
  "${DR_EX_PRECONDITION}:0" "$(reset_probe before)"
# A listing that fails after the delete must refuse rather than report the
# bucket proven empty. One delete has already run by then.
# shellcheck disable=SC2031
check "reset: a failed listing after the delete refuses rather than proving empty" \
  "${DR_EX_PRECONDITION}:1" "$(reset_probe after)"

# --- dr_reset_bucket emptiness proof over non-corpus keys -------------------
#
# The reset's before and after counts go through the same exclusion as every
# other count, so a bucket whose only remaining keys are the harness marker and
# a qualification run's probe fixtures is PROVEN EMPTY rather than reported as
# still holding objects. Both stubs below leave the real dr_list_all_versions,
# dr_delete_all_versions and dr_strip_noncorpus_keys in the path and stub only
# the client call under them, which is what makes this a test of the proof and
# not of a fake listing.

# Runs dr_reset_bucket against a bucket whose every listing returns $1 (one key
# per line, as the text output prints them) and prints
# "<exit code>:<delete-object calls>".
reset_residue_probe() {
  local residue="$1" delete_log
  delete_log="$(mktemp)"
  local code=0
  (
    DR_LOG_DIR="$(mktemp -d)"
    dr_bucket_marker_present() { return 0; }
    dr_write_bucket_marker() { return 0; }
    dr_aws() {
      case "${2:-}" in
        delete-object) printf 'rm\n' >>"${delete_log}" ;;
        list-object-versions | list-objects-v2) printf '%s\n' "${residue}" ;;
      esac
      return 0
    }
    dr_reset_bucket b 0
  ) >/dev/null 2>&1 || code=$?
  printf '%s:%s\n' "${code}" "$(wc -l <"${delete_log}" | tr -d ' ')"
  rm -f "${delete_log}"
}

# shellcheck disable=SC2031  # read in the parent; the probe's subshell only stubs
check "reset: a bucket holding only qualification probe keys is proven empty" \
  "0:3" "$(reset_residue_probe \
    "sys/qualify/019265f4-0000-7000-8000-000000000001/list/key-0000
sys/qualify/019265f4-0000-7000-8000-000000000001/order/a
${DR_BUCKET_MARKER_KEY}")"
# The other side of the same proof: a corpus key that survived the delete still
# refuses, so the case above is passing because the probe keys are excluded and
# not because the emptiness check stopped looking.
# shellcheck disable=SC2031
check "reset: a corpus key surviving the delete still refuses" \
  "${DR_EX_PRECONDITION}:2" "$(reset_residue_probe \
    "sys/qualify/019265f4-0000-7000-8000-000000000001/list/key-0000
t/${TH}/m/l0/0000/${W}.7.00000000000000000001.aaaaaaaaaaaaaaaa.rseg")"

# --- dr_ensure_bucket and the bucket protection checks ---------------------
#
# A bucket carrying the rehearsal marker is configured and read back exactly. A
# bucket without one is never changed and is held only to what a
# --require-bucket-protection server refuses to start on, as `ravel-cli store
# verify-protection` reports it. Both clients are stubbed and every call is
# logged, so a case can assert what was and was not sent.

# vp_report [<condition>=<verdict> ...]: `store verify-protection` output, in
# its column layout, with every condition pass except those named.
vp_report() {
  local id verdict pair
  for id in versioning noncurrent-expiration expired-delete-marker abort-multipart \
    rule-scope no-foreign-rule delete-marker-replication object-lock; do
    verdict=pass
    for pair in "$@"; do
      if [[ "${pair%%=*}" == "${id}" ]]; then verdict="${pair#*=}"; fi
    done
    if [[ "${verdict}" == pass ]]; then
      printf '%-26s %s\n' "${id}" pass
    else
      printf '%-26s %-7s %s\n' "${id}" "${verdict}" "stub detail for ${id}"
    fi
  done
  printf '%-26s unknown not checked by this command, does not affect the exit code\n' \
    object-retention
  printf 'verify-protection: STUB\n'
}

# ensure_probe <dir> <marked 0|1>: runs dr_ensure_bucket on the existing
# replica bucket and prints its exit code. The stub aws answers Object Lock
# from <dir>/lock, versioning from <dir>/versioning, the whole-lifecycle
# read-back from <dir>/fields and the per-rule listing the unmarked check reads
# from <dir>/rules. The versioning and fields files hold one answer per line,
# the nth call answering line n and every later call the last line; a line
# FAIL fails that call, as does a rules file holding only FAIL. The stub ravel-cli prints <dir>/verify-<E_v>, else
# <dir>/verify, and exits with <dir>/cli-rc (default 0). ravel-cli or cargo is
# missing from PATH when <dir>/no-ravel-cli or <dir>/no-cargo exists. The bucket
# is absent when <dir>/absent exists; its create-bucket then fails when
# <dir>/create-fails exists and the marker upload when <dir>/marker-fails does,
# and DR_REGION is <dir>/region (default us-east-1). Calls are logged to
# <dir>/calls and the refusal reason is left in <dir>/error.
ensure_probe() {
  local dir="$1" marked="$2" code=0
  : >"${dir}/calls"
  : >"${dir}/error"
  rm -f "${dir}"/*.n
  (
    DR_BUCKET_PRIMARY=dr-primary
    DR_BUCKET_REPLICA=dr-replica
    DR_REGION="$(cat "${dir}/region" 2>/dev/null || echo us-east-1)"
    dr_bucket_exists() { [[ ! -e "${dir}/absent" ]]; }
    dr_bucket_marker_present() { [[ "${marked}" == 1 ]]; }
    dr_have_command() {
      case "$1" in
        ravel-cli | cargo) [[ ! -e "${dir}/no-$1" ]] ;;
        *) command -v "$1" >/dev/null 2>&1 ;;
      esac
    }
    answer() {
      local n line
      n=$(( $(cat "$1.n" 2>/dev/null || echo 0) + 1 ))
      printf '%s\n' "${n}" >"$1.n"
      line="$(sed -n "${n}p" "$1")"
      [[ -n "${line}" ]] || line="$(tail -n 1 "$1")"
      [[ "${line}" != FAIL ]] || return 254
      printf '%s\n' "${line}"
    }
    dr_aws() {
      printf '%s\n' "$*" >>"${dir}/calls"
      case "$2" in
        create-bucket) [[ ! -e "${dir}/create-fails" ]] ;;
        cp)
          cat >/dev/null
          [[ ! -e "${dir}/marker-fails" ]]
          ;;
        get-object-lock-configuration) cat "${dir}/lock" ;;
        get-bucket-versioning) answer "${dir}/versioning" ;;
        get-bucket-lifecycle-configuration)
          if [[ "$*" == *'length(Rules)'* ]]; then
            answer "${dir}/fields"
          elif [[ "$(cat "${dir}/rules")" == FAIL ]]; then
            return 254
          else
            cat "${dir}/rules"
          fi
          ;;
      esac
    }
    dr_ravel_cli() {
      printf 'ravel-cli %s\n' "$*" >>"${dir}/calls"
      cat "${dir}/verify-${*: -1}" 2>/dev/null || cat "${dir}/verify"
      return "$(cat "${dir}/cli-rc" 2>/dev/null || echo 0)"
    }
    dr_ensure_bucket dr-replica || {
      code=$?
      printf '%s\n' "${DR_BUCKET_ERROR}" >"${dir}/error"
      exit "${code}"
    }
  ) >/dev/null 2>&1 || code=$?
  printf '%s\n' "${code}"
}

# ensure_dir <name>: a fresh directory for one ensure_probe case, set up as a
# compliant bucket already carrying everything dr_ensure_bucket puts.
ensure_dir() {
  local dir="${ENSURE_TMP}/$1"
  mkdir -p "${dir}"
  printf 'Enabled\n' >"${dir}/lock"
  printf 'Enabled\n' >"${dir}/versioning"
  printf '1\tEnabled\t{"f":{"Prefix":""},"p":null}\ttrue\t1\t7\n' >"${dir}/fields"
  ensure_rules "${dir}" 'Enabled|{"f":{"Prefix":""},"p":null}|1'
  vp_report >"${dir}/verify"
  printf '%s\n' "${dir}"
}

# ensure_rules <dir> <rule>...: the per-rule listing, one "Status|scope|days"
# argument per rule.
ensure_rules() {
  local dir="$1" rule
  shift
  : >"${dir}/rules"
  for rule in "$@"; do printf '%s\n' "${rule//|/$'\t'}" >>"${dir}/rules"; done
}

# The ravel-cli calls a case made, "|" after each.
ensure_cli_calls() {
  grep '^ravel-cli' "$1/calls" | tr '\n' '|'
}

# The subcommands a case sent to aws, in order, space separated.
ensure_sequence() {
  awk '$1 == "s3api" { printf "%s ", $2 }' "$1/calls"
}

ensure_has() {
  if grep -qF -- "$2" "$1"; then printf 'yes\n'; else printf 'no\n'; fi
}

ENSURE_TMP="$(mktemp -d)"

# A fresh marked bucket is checked for Object Lock before versioning is
# switched on, then gets versioning and DR_BUCKET_LIFECYCLE, each read first and
# read back after its put. ravel-cli is never asked.
d="$(ensure_dir marked-fresh)"
printf 'None\nEnabled\n' >"${d}/versioning"
printf 'FAIL\n1\tEnabled\t{"f":{"Prefix":""},"p":null}\ttrue\t1\t7\n' >"${d}/fields"
check "ensure: a fresh marked bucket passes" "0" "$(ensure_probe "${d}" 1)"
check "ensure: a fresh marked bucket issues both puts, each between its read and read-back" \
  "get-object-lock-configuration get-bucket-versioning put-bucket-versioning get-bucket-versioning get-bucket-lifecycle-configuration put-bucket-lifecycle-configuration get-bucket-lifecycle-configuration " \
  "$(ensure_sequence "${d}")"
check "ensure: a marked bucket gets DR_BUCKET_LIFECYCLE" \
  "yes" "$(ensure_has "${d}/calls" "--lifecycle-configuration ${DR_BUCKET_LIFECYCLE}")"
check "ensure: a marked bucket never runs ravel-cli" "no" "$(ensure_has "${d}/calls" ravel-cli)"

# An existing marked bucket already Enabled and already carrying the rule
# issues no put: RustFS answers InternalError to a second PutBucketVersioning on
# an Object Lock bucket, so a rerun that put again would fail.
d="$(ensure_dir marked-settled)"
check "ensure: a settled marked bucket passes" "0" "$(ensure_probe "${d}" 1)"
check "ensure: a settled marked bucket issues no put and reads each setting once" \
  "get-object-lock-configuration get-bucket-versioning get-bucket-lifecycle-configuration " \
  "$(ensure_sequence "${d}")"

# Only the setting that does not already match is put.
d="$(ensure_dir marked-other-rule)"
printf '1\tEnabled\t{"f":{"Prefix":""},"p":null}\ttrue\t30\t7\n1\tEnabled\t{"f":{"Prefix":""},"p":null}\ttrue\t1\t7\n' \
  >"${d}/fields"
check "ensure: a marked bucket carrying another rule gets only the lifecycle put" "0" \
  "$(ensure_probe "${d}" 1)"
check "ensure: that bucket's versioning is read and not put" \
  "get-object-lock-configuration get-bucket-versioning get-bucket-lifecycle-configuration put-bucket-lifecycle-configuration get-bucket-lifecycle-configuration " \
  "$(ensure_sequence "${d}")"

# The read-back is exact for a marked bucket: the harness put NoncurrentDays 1,
# so a bucket still reading 30 did not take the configuration.
d="$(ensure_dir marked-other-noncurrent-days)"
printf '1\tEnabled\t{"f":{"Prefix":""},"p":null}\ttrue\t30\t7\n' >"${d}/fields"
check "ensure: a marked bucket reading back another NoncurrentDays is refused" \
  "1" "$(ensure_probe "${d}" 1)"
check "ensure: the exact read-back names what it read" \
  "yes" "$(ensure_has "${d}/error" "does not carry each action (read ExpiredObjectDeleteMarker=true NoncurrentDays=30")"
d="$(ensure_dir marked-versioning-not-taken)"
printf 'Suspended\n' >"${d}/versioning"
check "ensure: a marked bucket whose versioning reads back Suspended is refused" \
  "1" "$(ensure_probe "${d}" 1)"
check "ensure: that refusal names what it read" \
  "yes" "$(ensure_has "${d}/error" "does not have versioning enabled (read Suspended)")"

# A marked bucket without Object Lock is refused before versioning, which cannot
# be switched off again, is turned on.
d="$(ensure_dir marked-without-object-lock)"
printf 'None\n' >"${d}/lock"
check "ensure: a marked bucket without Object Lock is refused" "1" "$(ensure_probe "${d}" 1)"
check "ensure: a marked bucket without Object Lock never gets versioning" \
  "get-object-lock-configuration " "$(ensure_sequence "${d}")"
check "ensure: a marked bucket without Object Lock is told to delete and rerun" \
  "yes" "$(ensure_has "${d}/error" "delete the bucket and rerun")"

# An absent bucket is created with Object Lock, in us-east-1 with no
# LocationConstraint and elsewhere with one naming DR_REGION, then stamped with
# the marker before anything else is sent.
d="$(ensure_dir absent-us-east-1)"
: >"${d}/absent"
check "ensure: an absent bucket in us-east-1 is created and passes" "0" "$(ensure_probe "${d}" 1)"
check "ensure: the us-east-1 create carries Object Lock and no LocationConstraint" \
  "s3api create-bucket --bucket dr-replica --object-lock-enabled-for-bucket" \
  "$(grep ' create-bucket ' "${d}/calls")"
check "ensure: the created bucket is stamped before it is read" \
  "s3api create-bucket|s3 cp|s3api get-object-lock-configuration|" \
  "$(awk '{ printf "%s %s|", $1, $2 }' "${d}/calls" | cut -d'|' -f1-3)|"
check "ensure: the marker goes to the bucket just created" \
  "yes" "$(ensure_has "${d}/calls" "s3 cp - s3://dr-replica/${DR_BUCKET_MARKER_KEY}")"
d="$(ensure_dir absent-eu-west-2)"
: >"${d}/absent"
printf 'eu-west-2\n' >"${d}/region"
check "ensure: an absent bucket outside us-east-1 is created and passes" "0" \
  "$(ensure_probe "${d}" 1)"
check "ensure: that create names DR_REGION as its LocationConstraint and carries Object Lock" \
  "s3api create-bucket --bucket dr-replica --create-bucket-configuration LocationConstraint=eu-west-2 --object-lock-enabled-for-bucket" \
  "$(grep ' create-bucket ' "${d}/calls")"
# A create or marker upload that fails refuses, and nothing more is sent: a
# bucket with no marker is one this harness would never reconfigure or delete.
d="$(ensure_dir absent-create-fails)"
: >"${d}/absent"
: >"${d}/create-fails"
check "ensure: a failed create refuses" "1" "$(ensure_probe "${d}" 1)"
check "ensure: that refusal names the bucket and region" \
  "yes" "$(ensure_has "${d}/error" "could not create bucket dr-replica in region us-east-1")"
check "ensure: a failed create sends nothing after it" "create-bucket " "$(ensure_sequence "${d}")"
d="$(ensure_dir absent-marker-fails)"
: >"${d}/absent"
: >"${d}/marker-fails"
printf 'eu-west-2\n' >"${d}/region"
check "ensure: a failed marker write refuses" "1" "$(ensure_probe "${d}" 1)"
check "ensure: that refusal says the bucket was created without its marker" \
  "yes" "$(ensure_has "${d}/error" "created bucket dr-replica but could not write its ${DR_BUCKET_MARKER_KEY} rehearsal marker")"
check "ensure: a failed marker write sends nothing after the upload" \
  "s3api create-bucket|s3 cp|" "$(awk '{ printf "%s %s|", $1, $2 }' "${d}/calls")"

# An unmarked bucket with its own E_v and a second rule over logs/ passes the
# server's checks and is never reconfigured. verify-protection needs an E_v; it
# runs once, with the NoncurrentDays of the rule covering t/ (30), which is the
# reference the server measures other rules against. The logs/ rule's 7 does
# not cover t/ and is never tried.
d="$(ensure_dir unmarked-own-ev-and-logs-rule)"
ensure_rules "${d}" 'Enabled|{"f":{"Prefix":""},"p":null}|30' \
  'Enabled|{"f":{"Prefix":"logs/"},"p":null}|7'
vp_report noncurrent-expiration=fail >"${d}/verify-7"
vp_report >"${d}/verify-30"
check "ensure: an unmarked bucket with its own E_v and a logs/ rule passes" \
  "0" "$(ensure_probe "${d}" 0)"
check "ensure: an unmarked bucket is never written" "no" "$(ensure_has "${d}/calls" " put-")"
check "ensure: an unmarked bucket is never read back exactly" \
  "no" "$(ensure_has "${d}/calls" "length(Rules)")"
check "ensure: verify-protection runs once, with the covering rule's NoncurrentDays" \
  "ravel-cli --store s3 store verify-protection --expected-noncurrent-days 30|" \
  "$(ensure_cli_calls "${d}")"

# The reference is the server's, not any value that happens to pass. A covering
# rule at 30 and a t/a rule at 7 with versioning unknown: the server takes 30,
# fails no-foreign-rule on the t/a rule and refuses. A run with E_v 7 would show
# nothing fatal (noncurrent-expiration fails, but versioning is not pass), and
# must not be what the bucket is judged by.
d="$(ensure_dir unmarked-early-rule-versioning-unknown)"
ensure_rules "${d}" 'Enabled|{"f":{"Prefix":""},"p":null}|30' \
  'Enabled|{"f":{"Prefix":"t/a"},"p":null}|7'
vp_report noncurrent-expiration=fail versioning=unknown >"${d}/verify-7"
vp_report no-foreign-rule=fail versioning=unknown >"${d}/verify-30"
check "ensure: an early t/ rule under unknown versioning is refused" \
  "1" "$(ensure_probe "${d}" 0)"
check "ensure: that bucket is checked only with the server's reference, 30" \
  "ravel-cli --store s3 store verify-protection --expected-noncurrent-days 30|" \
  "$(ensure_cli_calls "${d}")"
check "ensure: that refusal names no-foreign-rule" \
  "yes" "$(ensure_has "${d}/error" "refuses to start on: no-foreign-rule (store verify-protection --expected-noncurrent-days 30,")"

# The covering rules can be a union of t/<d> rules over all sixteen hex digits,
# or a legacy t/ Prefix or an And holding only a prefix. Disabled, tagged and
# partial-union rules cover nothing.
d="$(ensure_dir unmarked-union-reference)"
union_rules=()
for digit in 0 1 2 3 4 5 6 7 8 9 a b c d e f; do
  union_rules+=("Enabled|{\"f\":{\"Prefix\":\"t/${digit}\"},\"p\":null}|14")
done
ensure_rules "${d}" "${union_rules[@]}" 'Disabled|{"f":{"Prefix":""},"p":null}|3' \
  'Enabled|{"f":{"And":{"Prefix":"","Tags":[{"Key":"k","Value":"v"}]}},"p":null}|2'
check "ensure: a complete t/<d> union gives the reference" "0" "$(ensure_probe "${d}" 0)"
check "ensure: that bucket is checked with the union's NoncurrentDays" \
  "ravel-cli --store s3 store verify-protection --expected-noncurrent-days 14|" \
  "$(ensure_cli_calls "${d}")"
d="$(ensure_dir unmarked-legacy-and-reference)"
ensure_rules "${d}" 'Enabled|{"f":null,"p":"t/"}|9' \
  'Enabled|{"f":{"And":{"Prefix":"t"}},"p":null}|9'
check "ensure: a legacy t/ Prefix and an And prefix agreeing give the reference" \
  "0" "$(ensure_probe "${d}" 0)"
check "ensure: that bucket is checked with their NoncurrentDays" \
  "ravel-cli --store s3 store verify-protection --expected-noncurrent-days 9|" \
  "$(ensure_cli_calls "${d}")"

# With no reference the bucket is refused, saying why, and ravel-cli never runs.
d="$(ensure_dir unmarked-partial-union)"
ensure_rules "${d}" "${union_rules[@]:1}"
check "ensure: an incomplete t/<d> union gives no reference and refuses" \
  "1" "$(ensure_probe "${d}" 0)"
check "ensure: that refusal says no covering rule carries NoncurrentDays" \
  "yes" "$(ensure_has "${d}/error" "no enabled lifecycle rule covering t/ carries NoncurrentDays, so the NoncurrentDays a --require-bucket-protection server measures other rules against cannot be determined")"
check "ensure: no reference never runs ravel-cli" "" "$(ensure_cli_calls "${d}")"
check "ensure: that refusal says the harness is stricter than the server" \
  "yes" "$(ensure_has "${d}/error" "A --require-bucket-protection server refuses to start on such a bucket, while versioning passes, where the rules covering all of t/ disagree, or where no rule carries NoncurrentDays over t/ at all (a rule narrowed by a tag or an object size does not count). It warns and starts instead where what carries it is a rule it cannot read (a Status other than Enabled or Disabled, a filter it does not recognise, a NoncurrentDays that does not parse) or rules on prefixes under t/ that do not form a complete union of t/0 to t/f")"
d="$(ensure_dir unmarked-ten-digit-reference)"
ensure_rules "${d}" 'Enabled|{"f":{"Prefix":""},"p":null}|1000000000'
check "ensure: a ten-digit NoncurrentDays gives the reference" "0" "$(ensure_probe "${d}" 0)"
check "ensure: that bucket is checked with it" \
  "ravel-cli --store s3 store verify-protection --expected-noncurrent-days 1000000000|" \
  "$(ensure_cli_calls "${d}")"

# ref_of <rule>...: the reference dr_reference_noncurrent_days finds in the
# per-rule lines ("Status|scope|days"), or "none: <reason>". It runs with
# globasciiranges off under the first installed UTF-8 locale whose collation
# puts A inside [a-f], so a bracket range follows a dictionary collation the
# way it does under bash 3.2.
UTF8_LOCALE=""
while IFS= read -r candidate; do
  case "$(printf '%s' "${candidate}" | tr '[:upper:]' '[:lower:]')" in
    c.utf8 | c.utf-8) continue ;;
    *.utf8 | *.utf-8) ;;
    *) continue ;;
  esac
  if (
    shopt -u globasciiranges 2>/dev/null || true
    LC_ALL="${candidate}"
    [[ A == [a-f] ]]
  ) 2>/dev/null; then
    UTF8_LOCALE="${candidate}"
    break
  fi
done < <(locale -a 2>/dev/null)
if [[ -z "${UTF8_LOCALE}" ]]; then
  printf 'note: no installed UTF-8 locale collates A into [a-f]; the hex-digit case below cannot exhibit that bug here\n'
fi
ref_of() {
  local listing="" rule
  for rule in "$@"; do listing+="${rule//|/$'\t'}"$'\n'; done
  (
    shopt -u globasciiranges 2>/dev/null || true
    if [[ -n "${UTF8_LOCALE}" ]]; then LC_ALL="${UTF8_LOCALE}"; fi
    if dr_reference_noncurrent_days "${listing}"; then
      printf '%s\n' "${DR_REFERENCE_DAYS}"
    else
      printf 'none: %s\n' "${DR_REFERENCE_ERROR}"
    fi
  )
}
whole_rule() { printf 'Enabled|{"f":{"Prefix":""},"p":null}|%s\n' "$1"; }
check "reference: the lowercase t/<d> union gives its NoncurrentDays" "14" \
  "$(ref_of "${union_rules[@]}")"
check "reference: a t/A rule in place of t/a completes no union" \
  "none: no enabled lifecycle rule covering t/ carries NoncurrentDays" \
  "$(ref_of "${union_rules[@]:0:10}" 'Enabled|{"f":{"Prefix":"t/A"},"p":null}|14' \
    "${union_rules[@]:11}")"
check "reference: NoncurrentDays 0 parses, as the server's parse_decimal reads it" "0" \
  "$(ref_of "$(whole_rule 0)")"
check "reference: a ten-digit NoncurrentDays parses" "1000000000" \
  "$(ref_of "$(whole_rule 1000000000)")"
check "reference: the u32 maximum parses" "4294967295" "$(ref_of "$(whole_rule 4294967295)")"
check "reference: one past the u32 maximum does not, as the server's does not" \
  "none: no enabled lifecycle rule covering t/ carries NoncurrentDays" \
  "$(ref_of "$(whole_rule 4294967296)")"
check "reference: an eleven-digit NoncurrentDays does not parse" \
  "none: no enabled lifecycle rule covering t/ carries NoncurrentDays" \
  "$(ref_of "$(whole_rule 10000000000)")"
check "reference: a leading zero does not parse, as the server's does not" \
  "none: no enabled lifecycle rule covering t/ carries NoncurrentDays" \
  "$(ref_of "$(whole_rule 007)")"
d="$(ensure_dir unmarked-covering-rules-disagree)"
ensure_rules "${d}" 'Enabled|{"f":{"Prefix":""},"p":null}|30' \
  'Enabled|{"f":{"Prefix":"t/"},"p":null}|7'
check "ensure: covering rules that disagree refuse" "1" "$(ensure_probe "${d}" 0)"
check "ensure: that refusal names both values" \
  "yes" "$(ensure_has "${d}/error" "the enabled lifecycle rules covering t/ disagree on NoncurrentDays (7 30)")"
d="$(ensure_dir unmarked-lifecycle-unreadable)"
printf 'FAIL\n' >"${d}/rules"
check "ensure: an unreadable lifecycle configuration refuses" "1" "$(ensure_probe "${d}" 0)"
check "ensure: that refusal says the reference cannot be determined" \
  "yes" "$(ensure_has "${d}/error" "could not read the lifecycle configuration of bucket dr-replica, so the NoncurrentDays a --require-bucket-protection server measures other rules against cannot be determined")"

# An unmarked bucket without Object Lock is refused, and told what to change
# rather than to delete a bucket the harness does not own.
d="$(ensure_dir unmarked-without-object-lock)"
printf 'None\n' >"${d}/lock"
vp_report object-lock=fail >"${d}/verify"
printf '1\n' >"${d}/cli-rc"
check "ensure: an unmarked bucket without Object Lock is refused" "1" "$(ensure_probe "${d}" 0)"
check "ensure: the refusal says Object Lock is set at creation and names the variable" \
  "yes" "$(ensure_has "${d}/error" "Object Lock can only be enabled when a bucket is created, so this bucket can never pass: set DR_BUCKET_REPLICA to a bucket created with Object Lock")"
check "ensure: the refusal does not tell the operator to delete the bucket" \
  "no" "$(ensure_has "${d}/error" "delete the bucket")"
check "ensure: an unmarked bucket without Object Lock is never written" \
  "no" "$(ensure_has "${d}/calls" " put-")"

# Checking an unmarked bucket needs ravel-cli, prebuilt or through cargo, and
# nothing else: with neither the bucket is refused and the message names both.
d="$(ensure_dir unmarked-no-ravel-cli-no-cargo)"
: >"${d}/no-ravel-cli"
: >"${d}/no-cargo"
check "ensure: no ravel-cli and no cargo refuses" "1" "$(ensure_probe "${d}" 0)"
check "ensure: that refusal names what is missing" \
  "yes" "$(ensure_has "${d}/error" "needs ravel-cli store verify-protection, but there is no ravel-cli on PATH and no cargo to build one")"
check "ensure: that bucket is never read" "" "$(ensure_sequence "${d}")"
d="$(ensure_dir unmarked-cargo-only)"
: >"${d}/no-ravel-cli"
check "ensure: cargo without a prebuilt ravel-cli is enough" "0" "$(ensure_probe "${d}" 0)"

# The server refuses on no-foreign-rule, abort-multipart, and on
# noncurrent-expiration only while versioning passes; every other failure and
# every unknown condition only warns.
d="$(ensure_dir unmarked-foreign-rule)"
vp_report no-foreign-rule=fail >"${d}/verify"
check "ensure: an unmarked bucket failing no-foreign-rule is refused" \
  "1" "$(ensure_probe "${d}" 0)"
check "ensure: the refusal names the condition and what to change" \
  "yes" "$(ensure_has "${d}/error" "refuses to start on: no-foreign-rule (store verify-protection --expected-noncurrent-days 1, detail above). It carries no dr/rehearsal-bucket.json rehearsal marker, so this harness did not create it and changes nothing on it: change the bucket's configuration")"
d="$(ensure_dir unmarked-abort-multipart)"
vp_report abort-multipart=fail >"${d}/verify"
check "ensure: an unmarked bucket failing abort-multipart is refused" \
  "1" "$(ensure_probe "${d}" 0)"
d="$(ensure_dir unmarked-noncurrent-versioned)"
vp_report noncurrent-expiration=fail >"${d}/verify"
check "ensure: an unmarked versioned bucket failing noncurrent-expiration is refused" \
  "1" "$(ensure_probe "${d}" 0)"
d="$(ensure_dir unmarked-noncurrent-unversioned)"
vp_report noncurrent-expiration=fail versioning=fail >"${d}/verify"
check "ensure: noncurrent-expiration without versioning only warns" \
  "0" "$(ensure_probe "${d}" 0)"
d="$(ensure_dir unmarked-non-fatal)"
vp_report rule-scope=fail expired-delete-marker=fail versioning=unknown >"${d}/verify"
check "ensure: rule-scope and expired-delete-marker failures only warn" \
  "0" "$(ensure_probe "${d}" 0)"
d="$(ensure_dir unmarked-unknown)"
vp_report object-lock=unknown abort-multipart=unknown no-foreign-rule=unknown \
  noncurrent-expiration=unknown >"${d}/verify"
printf '2\n' >"${d}/cli-rc"
check "ensure: unknown conditions only warn" "0" "$(ensure_probe "${d}" 0)"

# A ravel-cli that did not run, or printed no verdict for a condition the server
# refuses on, refuses: "could not check" is not a pass.
d="$(ensure_dir unmarked-cli-crashed)"
printf '101\n' >"${d}/cli-rc"
check "ensure: a ravel-cli that exits outside 0..2 refuses" "1" "$(ensure_probe "${d}" 0)"
check "ensure: that refusal says ravel-cli could not run" \
  "yes" "$(ensure_has "${d}/error" "could not run ravel-cli store verify-protection against bucket dr-replica (exit 101)")"
d="$(ensure_dir unmarked-missing-verdict)"
vp_report | grep -v '^abort-multipart' >"${d}/verify"
check "ensure: a report missing a condition the server refuses on refuses" \
  "1" "$(ensure_probe "${d}" 0)"
check "ensure: that refusal names the missing condition" \
  "yes" "$(ensure_has "${d}/error" "printed no single verdict for abort-multipart")"

rm -rf "${ENSURE_TMP}"

# --- result ----------------------------------------------------------------

printf '\nlib.test.sh: %s passed, %s failed\n' "${PASSED}" "${FAILED}"
if [[ "${FAILED}" -ne 0 ]]; then
  exit 1
fi
if [[ "${PASSED}" -lt 150 ]]; then
  printf 'lib.test.sh: only %s cases ran; a suite that shrank silently is not a pass\n' \
    "${PASSED}" >&2
  exit 1
fi
exit 0
