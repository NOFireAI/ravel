#!/usr/bin/env bash
# Shared library for the disaster-recovery rehearsal harness (issue #814).
#
# The harness restores a replica bucket into an empty bucket with writers
# stopped, runs the reconciliation and verification checks the restore
# checklist in docs/guides/disaster-recovery.md names, and only then lets
# ravel-server start. It is backend agnostic: MinIO in CI, real S3 when the
# environment points it there.
#
# This file is sourced, never executed. Every entry point under scripts/dr/
# sources it, sets `set -euo pipefail` itself, and calls `dr_init`.
#
# NOTHING HERE HAS A GUESSABLE BUCKET NAME OR A GUESSABLE CREDENTIAL. Both
# bucket names and both credentials are required with no default, and `dr_init`
# refuses when any of them is unset. A default bucket name is a recursive
# delete aimed at whatever bucket happens to bear that name in the account the
# credentials belong to, and a default credential pair silently turns a
# real-S3 misconfiguration into a run against someone's dev MinIO or the
# reverse.
#
# Environment:
#
#   DR_ENDPOINT          S3 endpoint override. Default http://127.0.0.1:9000.
#                        Set it to the EMPTY STRING for real S3: the scripts
#                        then export no RAVEL_S3_ENDPOINT at all, which is how
#                        the object store expresses "use AWS's regional
#                        endpoint" (S3Config.endpoint is Option<String>).
#   DR_REGION            S3 region. Default us-east-1. Passed to bucket
#                        creation too: a bucket created with no region lands
#                        in the endpoint's default one, which is a different
#                        bucket from the one the rest of the run addresses.
#   DR_BUCKET_PRIMARY    REQUIRED. Bucket A, the primary the corpus is written
#                        into.
#   DR_BUCKET_REPLICA    REQUIRED. Bucket B, the empty bucket the replica is
#                        restored into and the only bucket the restore checks
#                        read. Must differ from bucket A.
#   DR_ACCESS_KEY        REQUIRED. Access key id.
#   DR_SECRET_KEY        REQUIRED. Secret access key.
#   DR_SESSION_TOKEN     STS session token. Required in practice whenever the
#                        credentials come from an instance role or any other
#                        STS source; when it is set, every tool the harness
#                        drives must carry it or every call 403s.
#   DR_TENANT            Tenant name. Default dr-rehearsal-tenant.
#   DR_TENANT_TOKEN      Ingest bearer token for that tenant. Never placed in
#                        argv: ravel-server reads it from a
#                        `--tenant-token-file` and curl from a `--config`
#                        file, both written mode 600 under DR_LOG_DIR/private.
#   DR_SHARDS            Shard count passed to the ravel-cli subcommands.
#   DR_TENANT_HASH_MODE  REQUIRED. unkeyed | keyed. A fresh bucket refuses to
#                        be written until the deployment names one (ADR-0050
#                        section 3), and the custody manifest check asserts the
#                        choice was made explicitly rather than defaulted into.
#   DR_TENANT_HASH_KEY_FILE   Required when DR_TENANT_HASH_MODE=keyed.
#   DR_TENANT_KMS_CONFIG      REQUIRED. Per-tenant KMS configuration file, or
#                        the literal `none` to declare the deployment uses no
#                        per-tenant KMS. The custody manifest check refuses an
#                        unset value: "unset" and "deliberately none" are
#                        different answers and only one of them is a
#                        restore-ready deployment. It therefore has no default:
#                        a default of `none` would make an operator who
#                        declared nothing pass as one who declared none.
#   DR_ADMIN_CREDENTIAL_FILE  REQUIRED. File holding the admin credential the
#                        restore operator will use, or the literal `none` under
#                        the same rule and for the same reason.
#   DR_FOLD_SEAL_MARGIN_WAITED  0 | 1, default 0. Set it to 1 only when the run
#                        really did wait `max_flush_lifetime +
#                        clock_skew_allowance + fold_safety_margin` out after
#                        the last write. At 0 the catalog-fold phase reports
#                        itself a NO-OP (see restore-check.sh); at 1 a fold
#                        that publishes no snapshot HEAD is a failure.
#   DR_LOG_DIR           Where logs and the pre-registered figures live.
#                        Default <repo>/.gate-logs/dr (gitignored).
#   DR_MC                Path to an `mc` binary. When unset the scripts run
#                        the digest-pinned mc image under docker.
#   DR_MC_IMAGE          The mc image reference, digest pinned.
#   DR_MC_HOST_URL       Escape hatch: the whole MC_HOST_dr URL, built by the
#                        caller. When DR_SESSION_TOKEN is set this URL must
#                        carry a session-token component or startup refuses.
#   DR_HTTP_ADDR         host:port the seeding server listens on for HTTP.
#   DR_GRPC_ADDR         host:port the seeding server listens on for gRPC.
#
# A real S3 run needs: DR_ENDPOINT set to the empty string, DR_REGION,
# DR_BUCKET_PRIMARY, DR_BUCKET_REPLICA, DR_ACCESS_KEY, DR_SECRET_KEY,
# DR_SESSION_TOKEN (under STS, which an instance role always is), DR_TENANT,
# DR_TENANT_TOKEN, DR_TENANT_HASH_MODE (with DR_TENANT_HASH_KEY_FILE when
# keyed), DR_TENANT_KMS_CONFIG and DR_ADMIN_CREDENTIAL_FILE.
#
# Credentials are never placed in a command line. mc receives them through an
# MC_HOST_<alias> variable in the environment (for the containerised mc, via a
# bare `-e NAME` so the value never appears in the container's argv), and
# ravel-cli and ravel-server receive them through RAVEL_S3_ACCESS_KEY /
# RAVEL_S3_SECRET_KEY / RAVEL_S3_SESSION_TOKEN.

# shellcheck shell=bash

DR_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export DR_ROOT_DIR

DR_ENDPOINT="${DR_ENDPOINT-http://127.0.0.1:9000}"
DR_REGION="${DR_REGION:-us-east-1}"
DR_BUCKET_PRIMARY="${DR_BUCKET_PRIMARY:-}"
DR_BUCKET_REPLICA="${DR_BUCKET_REPLICA:-}"
DR_ACCESS_KEY="${DR_ACCESS_KEY:-}"
DR_SECRET_KEY="${DR_SECRET_KEY:-}"
DR_SESSION_TOKEN="${DR_SESSION_TOKEN:-}"
DR_TENANT="${DR_TENANT:-dr-rehearsal-tenant}"
DR_TENANT_TOKEN="${DR_TENANT_TOKEN:-dr-rehearsal-token}"
DR_SHARDS="${DR_SHARDS:-4}"
DR_TENANT_HASH_MODE="${DR_TENANT_HASH_MODE:-}"
DR_TENANT_HASH_KEY_FILE="${DR_TENANT_HASH_KEY_FILE:-}"
DR_TENANT_KMS_CONFIG="${DR_TENANT_KMS_CONFIG:-}"
DR_ADMIN_CREDENTIAL_FILE="${DR_ADMIN_CREDENTIAL_FILE:-}"
DR_FOLD_SEAL_MARGIN_WAITED="${DR_FOLD_SEAL_MARGIN_WAITED:-0}"
DR_LOG_DIR="${DR_LOG_DIR:-${DR_ROOT_DIR}/.gate-logs/dr}"
DR_MC="${DR_MC:-}"
# quay.io rather than Docker Hub: Docker Hub's anonymous pull allowance is
# per-IP and shared across every project on a runner.
DR_MC_IMAGE="${DR_MC_IMAGE:-quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727}"
DR_HTTP_ADDR="${DR_HTTP_ADDR:-127.0.0.1:8480}"
DR_GRPC_ADDR="${DR_GRPC_ADDR:-127.0.0.1:8481}"

# The harness's own keys. All three live under `dr/`, deliberately outside
# `t/` and `sys/`, the two key families docs/catalog-and-mvcc.md freezes, so
# they can never collide with a key Ravel itself writes. `dr_list_keys` drops
# the whole `dr/` prefix, so harness bookkeeping never counts as corpus and a
# bucket holding only its own creation marker still reads as empty.
DR_MARKER_KEY="dr/reconciled.json"
DR_RESTORE_START_KEY="dr/restore-start.json"
DR_BUCKET_MARKER_KEY="dr/rehearsal-bucket.json"
DR_HARNESS_PREFIX="dr/"

# The file the pre-registered figures live in. seed.sh and replicate.sh write
# it before any fault is injected; restore-check.sh reads it and refuses to
# run without it, so every figure it asserts has a band fixed before the
# thing being measured could have moved.
dr_expect_file() { printf '%s\n' "${DR_LOG_DIR}/dr-expect.env"; }

dr_log() { printf '%s %s\n' "[$(date -u +%H:%M:%S)]" "$*" >&2; }

dr_die() {
  local code="$1"
  shift
  printf 'dr: %s\n' "$*" >&2
  exit "$code"
}

# Exit codes. 64 is bad usage (sysexits EX_USAGE); 65 is a harness
# precondition the caller can fix; 10 and 11..14 are restore-check's and
# start.sh's, documented where they are raised.
DR_EX_USAGE=64
DR_EX_PRECONDITION=65

# A bucket name this harness is willing to address. Not a full S3 grammar:
# enough that a truncated or interpolation-mangled variable cannot reach a
# recursive delete.
dr_valid_bucket_name() {
  [[ "$1" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]
}

# Both buckets, named explicitly and distinct. Called by dr_init, so every
# entry point refuses the same way.
dr_require_buckets() {
  [[ -n "${DR_BUCKET_PRIMARY}" ]] || dr_die "${DR_EX_USAGE}" \
    "DR_BUCKET_PRIMARY is unset and has no default; name bucket A explicitly"
  [[ -n "${DR_BUCKET_REPLICA}" ]] || dr_die "${DR_EX_USAGE}" \
    "DR_BUCKET_REPLICA is unset and has no default; name bucket B explicitly"
  dr_valid_bucket_name "${DR_BUCKET_PRIMARY}" || dr_die "${DR_EX_USAGE}" \
    "DR_BUCKET_PRIMARY is not a usable bucket name: '${DR_BUCKET_PRIMARY}'"
  dr_valid_bucket_name "${DR_BUCKET_REPLICA}" || dr_die "${DR_EX_USAGE}" \
    "DR_BUCKET_REPLICA is not a usable bucket name: '${DR_BUCKET_REPLICA}'"
  [[ "${DR_BUCKET_PRIMARY}" != "${DR_BUCKET_REPLICA}" ]] || dr_die "${DR_EX_USAGE}" \
    "DR_BUCKET_PRIMARY and DR_BUCKET_REPLICA are both '${DR_BUCKET_PRIMARY}'; the restore target must be a different bucket from the primary"
}

dr_require_credentials() {
  [[ -n "${DR_ACCESS_KEY}" ]] || dr_die "${DR_EX_USAGE}" \
    "DR_ACCESS_KEY is unset and has no default; there is no dev fallback credential"
  [[ -n "${DR_SECRET_KEY}" ]] || dr_die "${DR_EX_USAGE}" \
    "DR_SECRET_KEY is unset and has no default; there is no dev fallback credential"
}

# One custody item, declared rather than defaulted. An empty value is refused:
# "unset" and "deliberately none" are different answers, and a default would
# collapse them.
dr_custody_declared() {
  local name="$1" value="$2"
  if [[ -z "${value}" ]]; then
    printf 'dr: %s is unset; declare a file path or the literal "none"\n' "${name}" >&2
    return 1
  fi
  if [[ "${value}" != "none" ]]; then
    if [[ ! -r "${value}" ]]; then
      printf 'dr: %s=%s is not readable\n' "${name}" "${value}" >&2
      return 1
    fi
  fi
  return 0
}

dr_init() {
  mkdir -p "${DR_LOG_DIR}"
  dr_require_buckets
  dr_require_credentials
  dr_assert_mc_can_carry_session_token
  case "${DR_TENANT_HASH_MODE}" in
    unkeyed | keyed) ;;
    "")
      dr_die "${DR_EX_USAGE}" \
        "DR_TENANT_HASH_MODE is unset and has no default; declare 'unkeyed' or 'keyed' (ADR-0050 section 3)"
      ;;
    *)
      dr_die "${DR_EX_USAGE}" \
        "DR_TENANT_HASH_MODE must be 'unkeyed' or 'keyed', got '${DR_TENANT_HASH_MODE}'"
      ;;
  esac
  if [[ "${DR_TENANT_HASH_MODE}" == "keyed" && -z "${DR_TENANT_HASH_KEY_FILE}" ]]; then
    dr_die "${DR_EX_USAGE}" \
      "DR_TENANT_HASH_MODE=keyed needs DR_TENANT_HASH_KEY_FILE"
  fi
  case "${DR_FOLD_SEAL_MARGIN_WAITED}" in
    0 | 1) ;;
    *)
      dr_die "${DR_EX_USAGE}" \
        "DR_FOLD_SEAL_MARGIN_WAITED must be 0 or 1, got '${DR_FOLD_SEAL_MARGIN_WAITED}'"
      ;;
  esac
}

# The shared tail of every usage message: the variables a real S3 run needs,
# in one place so the six scripts cannot drift apart on it.
dr_usage_environment() {
  cat <<'USAGE'
Environment. The four marked REQUIRED have no default; every script refuses
when one of them is unset.
  DR_ENDPOINT        endpoint override, default http://127.0.0.1:9000.
                     Set it to the EMPTY STRING for real S3: no endpoint
                     override is exported and the store uses AWS's regional
                     endpoint.
  DR_REGION          default us-east-1; also the region buckets are created in
  DR_BUCKET_PRIMARY  REQUIRED  bucket A (the primary)
  DR_BUCKET_REPLICA  REQUIRED  bucket B (the empty restore target), distinct
                               from bucket A
  DR_ACCESS_KEY      REQUIRED  access key id
  DR_SECRET_KEY      REQUIRED  secret access key
  DR_SESSION_TOKEN   STS session token; required whenever the credentials come
                     from an instance role or any other STS source
  DR_TENANT          tenant name, default dr-rehearsal-tenant
  DR_TENANT_TOKEN    ingest bearer token for that tenant (never in argv)
  DR_SHARDS          shard count for the ravel-cli subcommands, default 4
  DR_TENANT_HASH_MODE       REQUIRED  unkeyed | keyed
  DR_TENANT_HASH_KEY_FILE   deployment key file, required when keyed
  DR_TENANT_KMS_CONFIG      REQUIRED  per-tenant KMS config file, or `none`
  DR_ADMIN_CREDENTIAL_FILE  REQUIRED  admin credential file, or `none`
  DR_FOLD_SEAL_MARGIN_WAITED  0 | 1, default 0; 1 only when the run waited the
                     catalog seal margin out
  DR_LOG_DIR         logs and pre-registered figures, default
                     <repo>/.gate-logs/dr
  DR_MC / DR_MC_IMAGE       an mc binary, or the digest-pinned mc image
  DR_MC_HOST_URL     escape hatch: the whole MC_HOST_dr URL
  DR_HTTP_ADDR / DR_GRPC_ADDR  listen addresses for the seeding server

A real S3 run needs: DR_ENDPOINT="" plus DR_REGION, DR_BUCKET_PRIMARY,
DR_BUCKET_REPLICA, DR_ACCESS_KEY, DR_SECRET_KEY, DR_SESSION_TOKEN (under STS,
which an instance role always is), DR_TENANT, DR_TENANT_TOKEN,
DR_TENANT_HASH_MODE (with DR_TENANT_HASH_KEY_FILE when keyed),
DR_TENANT_KMS_CONFIG and DR_ADMIN_CREDENTIAL_FILE. Credentials are read from
the environment and are never passed on a command line.
USAGE
}

# ---------------------------------------------------------------------------
# Figures: pre-registration, extraction, and banded assertion.
#
# docs/guides/disaster-recovery.md publishes nothing a rehearsal did not
# measure, and the same discipline applies inside the harness: a number it
# prints and does not assert on is decoration. Every figure goes through
# dr_assert_figure, and every figure read out of a tool's output goes through
# dr_field_once first, so "absent" and "printed twice" fail exactly as
# "outside the band" does.
# ---------------------------------------------------------------------------

# Print the single value of `<label>: <value>` in $2. Fails when the label is
# absent or appears more than once.
dr_field_once() {
  local label="$1" text="$2" matches count
  matches="$(awk -v lab="${label}: " '
    index($0, lab) == 1 { print substr($0, length(lab) + 1) }
  ' <<<"${text}")"
  count="$(awk 'BEGIN { n = 0 } { n++ } END { print n }' <<<"${matches}")"
  if [[ -z "${matches}" ]]; then
    printf 'dr: figure "%s" was expected in the output and is absent\n' "${label}" >&2
    return 1
  fi
  if [[ "${count}" -ne 1 ]]; then
    printf 'dr: figure "%s" appears %s times; a figure present twice is as bad as one out of band\n' \
      "${label}" "${count}" >&2
    return 1
  fi
  printf '%s\n' "${matches}"
}

# Assert an integer figure is present exactly once and inside [lo, hi].
dr_assert_figure() {
  local label="$1" value="$2" lo="$3" hi="$4"
  if [[ ! "${value}" =~ ^-?[0-9]+$ ]]; then
    printf 'dr: figure %s is not an integer: "%s"\n' "${label}" "${value}" >&2
    return 1
  fi
  # Bash compares with 64-bit arithmetic and wraps silently past its range, so
  # an absurd figure could land inside its band as a negative number. No count
  # this harness reports can reach eighteen digits.
  if [[ "${#value}" -gt 18 ]]; then
    printf 'dr: figure %s has %s digits and is not a plausible count: "%s"\n' \
      "${label}" "${#value}" "${value}" >&2
    return 1
  fi
  printf 'figure %s=%s band=[%s,%s]\n' "${label}" "${value}" "${lo}" "${hi}"
  if [[ "${value}" -lt "${lo}" || "${value}" -gt "${hi}" ]]; then
    printf 'dr: figure %s=%s is outside its band [%s,%s]\n' \
      "${label}" "${value}" "${lo}" "${hi}" >&2
    return 1
  fi
  return 0
}

# Read one pre-registered figure. Refuses a missing expectations file rather
# than defaulting: a band that was never registered is not a band.
dr_expect() {
  local name="$1" file
  file="$(dr_expect_file)"
  if [[ ! -f "${file}" ]]; then
    printf 'dr: no pre-registered figures at %s; run seed.sh and replicate.sh first\n' \
      "${file}" >&2
    return 1
  fi
  local value
  value="$(awk -v key="${name}=" '
    index($0, key) == 1 { print substr($0, length(key) + 1) }
  ' "${file}")"
  if [[ -z "${value}" ]]; then
    printf 'dr: pre-registered figure %s is missing from %s\n' "${name}" "${file}" >&2
    return 1
  fi
  printf '%s\n' "${value}"
}

dr_expect_write() {
  local name="$1" value="$2" file
  file="$(dr_expect_file)"
  printf '%s=%s\n' "${name}" "${value}" >>"${file}"
}

# ---------------------------------------------------------------------------
# Nanosecond stamps.
#
# These are compared as equal-width zero-padded strings, never with bash
# arithmetic: a stamp is input this harness does not always produce, and bash
# wraps past the 64-bit range, so a twenty-digit stamp would compare as a
# negative number and read as safely in the past.
# ---------------------------------------------------------------------------

dr_now_ns() { date -u +%s%N; }

# True when `a` is not after `b`, i.e. a <= b.
dr_ns_not_after() {
  local a="$1" b="$2"
  [[ "${a}" =~ ^[0-9]+$ && "${b}" =~ ^[0-9]+$ ]] || return 2
  while [[ "${#a}" -lt "${#b}" ]]; do a="0${a}"; done
  while [[ "${#b}" -lt "${#a}" ]]; do b="0${b}"; done
  if [[ "${a}" > "${b}" ]]; then
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# mc: the one backend-agnostic S3 tool. Same invocations against MinIO and
# against real S3; only the MC_HOST_dr URL differs.
# ---------------------------------------------------------------------------

dr_have_command() { command -v "$1" >/dev/null 2>&1; }

# Percent-encode one value for the userinfo part of a URL. A secret access key
# routinely contains `/` and `+`, and an unencoded `/` truncates the host: the
# URL then names a different endpoint entirely, with no error to read.
dr_urlencode() {
  local LC_ALL=C
  local s="$1" out="" i c
  for ((i = 0; i < ${#s}; i++)); do
    c="${s:i:1}"
    case "${c}" in
      [a-zA-Z0-9._~-]) out+="${c}" ;;
      *) out+="$(printf '%%%02X' "'${c}")" ;;
    esac
  done
  printf '%s\n' "${out}"
}

# Refuse at startup when a session token is set and the mc credential path in
# use cannot carry it. MC_HOST_<alias> carries temporary credentials as
# `scheme://<key>:<secret>:<token>@host`; a caller-supplied DR_MC_HOST_URL
# with only two components would send the STS key pair with no token, and
# every mc call would 403 while the ravel binaries (which get the token
# through RAVEL_S3_SESSION_TOKEN) succeeded.
dr_assert_mc_can_carry_session_token() {
  [[ -n "${DR_SESSION_TOKEN}" ]] || return 0
  [[ -n "${DR_MC_HOST_URL:-}" ]] || return 0
  local url="${DR_MC_HOST_URL}" userinfo colons
  if [[ "${url}" != *"@"* ]]; then
    dr_die "${DR_EX_USAGE}" \
      "DR_SESSION_TOKEN is set but DR_MC_HOST_URL carries no credentials at all; it must be scheme://<key>:<secret>:<session-token>@host"
  fi
  userinfo="${url#*://}"
  userinfo="${userinfo%%@*}"
  colons="${userinfo//[^:]/}"
  if [[ "${#colons}" -ne 2 ]]; then
    dr_die "${DR_EX_USAGE}" \
      "DR_SESSION_TOKEN is set but DR_MC_HOST_URL has no session-token component; it must be scheme://<key>:<secret>:<session-token>@host"
  fi
}

# The alias URL. Credentials go in here and this value is never echoed.
dr_mc_host_url() {
  if [[ -n "${DR_MC_HOST_URL:-}" ]]; then
    printf '%s\n' "${DR_MC_HOST_URL}"
    return 0
  fi
  local host scheme access secret token
  if [[ -n "${DR_ENDPOINT}" ]]; then
    scheme="${DR_ENDPOINT%%://*}"
    host="${DR_ENDPOINT#*://}"
  else
    scheme="https"
    host="s3.${DR_REGION}.amazonaws.com"
  fi
  access="$(dr_urlencode "${DR_ACCESS_KEY}")"
  secret="$(dr_urlencode "${DR_SECRET_KEY}")"
  if [[ -n "${DR_SESSION_TOKEN}" ]]; then
    token="$(dr_urlencode "${DR_SESSION_TOKEN}")"
    printf '%s://%s:%s:%s@%s\n' "${scheme}" "${access}" "${secret}" "${token}" "${host}"
  else
    printf '%s://%s:%s@%s\n' "${scheme}" "${access}" "${secret}" "${host}"
  fi
}

# Run one mc command against the `dr` alias. Prefers an mc on PATH, falls back
# to the digest-pinned image under docker with --network host so a MinIO
# published on localhost is reachable.
dr_mc() {
  local url
  url="$(dr_mc_host_url)"
  if [[ -n "${DR_MC}" ]] || dr_have_command mc; then
    local bin="${DR_MC:-mc}"
    MC_HOST_dr="${url}" "${bin}" --no-color "$@"
    return $?
  fi
  # The bare `-e MC_HOST_dr` form passes the value from this process's
  # environment; the credential never appears in the container's argv.
  MC_HOST_dr="${url}" docker run --rm --interactive --network host \
    -e MC_HOST_dr "${DR_MC_IMAGE}" --no-color "$@"
}

dr_mc_available() {
  if [[ -n "${DR_MC}" ]] || dr_have_command mc; then
    return 0
  fi
  dr_have_command docker
}

# Drop the harness's own bookkeeping keys from a listing. They are not corpus:
# a bucket holding only its creation marker is an empty restore target, and
# the marker must not inflate a count whose band was fixed from bucket A.
dr_strip_harness_keys() {
  awk -v pfx="${DR_HARNESS_PREFIX}" '
    NF > 0 && index($0, pfx) != 1 { print }
  ' <<<"$1"
}

# Every current object key in a bucket, one per line, bucket-relative.
# Listing always starts at the bucket root so the printed keys are full keys;
# the callers filter by prefix themselves.
dr_list_keys() {
  local bucket="$1" listing names
  listing="$(dr_mc ls --recursive "dr/${bucket}/")" || return 1
  names="$(awk 'NF > 0 { print $NF }' <<<"${listing}")"
  dr_strip_harness_keys "${names}"
}

# Every key in a bucket INCLUDING noncurrent versions and delete markers. The
# emptiness assertions use this one: `mc rm --recursive` on a versioned bucket
# writes delete markers and leaves every prior version in place, so a listing
# of current versions alone reports an emptied-looking bucket that still holds
# all of its data (and `maintain verify-custody --versioning-aware` will find
# it). Falls back to the current-version listing when the backend rejects
# `--versions`.
dr_list_all_versions() {
  local bucket="$1" listing names
  if ! listing="$(dr_mc ls --recursive --versions "dr/${bucket}/" 2>/dev/null)"; then
    listing="$(dr_mc ls --recursive "dr/${bucket}/")" || return 1
  fi
  names="$(awk 'NF > 0 { print $NF }' <<<"${listing}")"
  dr_strip_harness_keys "${names}"
}

dr_count_lines() {
  awk 'BEGIN { n = 0 } NF > 0 { n++ } END { print n }' <<<"$1"
}

# ---------------------------------------------------------------------------
# Bucket lifecycle. Creation stamps a rehearsal marker; a recursive delete
# refuses on a bucket that does not carry one.
# ---------------------------------------------------------------------------

dr_bucket_exists() {
  dr_mc ls "dr/$1/" >/dev/null 2>&1
}

dr_write_bucket_marker() {
  local bucket="$1"
  printf '{"rehearsal": "ravel-dr", "bucket": "%s", "created_at_unix_ns": %s}\n' \
    "${bucket}" "$(dr_now_ns)" \
    | dr_mc pipe "dr/${bucket}/${DR_BUCKET_MARKER_KEY}" >/dev/null
}

# True when the bucket carries a rehearsal marker this harness wrote FOR THIS
# BUCKET. The bucket name is checked inside the body as well as in the key, so
# a marker copied in from somewhere else does not authorise a delete here.
dr_bucket_marker_present() {
  local bucket="$1" body
  body="$(dr_mc cat "dr/${bucket}/${DR_BUCKET_MARKER_KEY}" 2>/dev/null)" || return 1
  [[ "${body}" == *'"rehearsal": "ravel-dr"'* ]] || return 1
  [[ "${body}" == *"\"bucket\": \"${bucket}\""* ]] || return 1
  return 0
}

# Create the bucket in DR_REGION if it is absent, and stamp it. A bucket
# created with no region lands in the endpoint's default region, which is a
# different bucket from the one the rest of the run addresses.
dr_ensure_bucket() {
  local bucket="$1"
  if dr_bucket_exists "${bucket}"; then
    return 0
  fi
  dr_log "creating bucket ${bucket} in region ${DR_REGION}"
  dr_mc mb --region "${DR_REGION}" "dr/${bucket}" >/dev/null || return 1
  dr_write_bucket_marker "${bucket}"
}

# Empty a bucket. Refuses unless the bucket carries this harness's creation
# marker, or the caller passed the explicit override; prints the bucket name
# and its object count (all versions) before deleting anything; deletes every
# version rather than writing delete markers; and proves the result is empty.
#
# $1 bucket, $2 1 when --i-know-this-bucket was passed.
dr_reset_bucket() {
  local bucket="$1" override="$2" count after
  if ! dr_bucket_marker_present "${bucket}"; then
    if [[ "${override}" -ne 1 ]]; then
      dr_die "${DR_EX_PRECONDITION}" \
        "refusing to empty bucket '${bucket}': it carries no ${DR_BUCKET_MARKER_KEY} rehearsal marker, so this harness did not create it. Pass --i-know-this-bucket to override."
    fi
    dr_log "bucket ${bucket} carries no rehearsal marker; proceeding under --i-know-this-bucket"
  fi
  # Two statements, not one: a command substitution nested inside another
  # reports the OUTER command's status, and dr_count_lines always exits 0, so
  # a failed listing would have read as an empty bucket.
  local listing
  listing="$(dr_list_all_versions "${bucket}")" || dr_die \
    "${DR_EX_PRECONDITION}" "could not list ${bucket} before emptying it"
  count="$(dr_count_lines "${listing}")"
  printf 'dr-reset: bucket=%s objects_to_delete=%s (all versions)\n' "${bucket}" "${count}"
  dr_log "emptying bucket ${bucket} (${count} object version(s))"
  if ! dr_mc rm --recursive --force --versions "dr/${bucket}/" \
    >"${DR_LOG_DIR}/reset-${bucket}.log" 2>&1; then
    dr_log "versioned delete refused by the backend; retrying without --versions"
    dr_mc rm --recursive --force "dr/${bucket}/" \
      >>"${DR_LOG_DIR}/reset-${bucket}.log" 2>&1 \
      || dr_die "${DR_EX_PRECONDITION}" "could not empty ${bucket}"
  fi
  local after_listing
  after_listing="$(dr_list_all_versions "${bucket}")" || dr_die \
    "${DR_EX_PRECONDITION}" \
    "could not list ${bucket} after emptying it, so the bucket is not proven empty"
  after="$(dr_count_lines "${after_listing}")"
  if [[ "${after}" -ne 0 ]]; then
    dr_die "${DR_EX_PRECONDITION}" \
      "bucket ${bucket} still holds ${after} object version(s) after the delete; on a versioned bucket a delete marker is not an empty bucket"
  fi
  dr_write_bucket_marker "${bucket}"
}

# ---------------------------------------------------------------------------
# Key classifiers. All of them read the layout docs/catalog-and-mvcc.md and
# ADR-0010 freeze, with no decode step.
# ---------------------------------------------------------------------------

# The tenant prefix hash, discovered from the bucket rather than recomputed.
# Asserts a single-tenant universe: every figure below is a whole-bucket count,
# and a second tenant's objects would inflate each one while the band stayed
# put.
dr_tenant_hash_from_keys() {
  local keys="$1" hashes count
  hashes="$(awk -F/ '$1 == "t" && NF > 2 { print $2 }' <<<"${keys}" | sort -u)"
  count="$(dr_count_lines "${hashes}")"
  if [[ "${count}" -ne 1 ]]; then
    printf 'dr: expected exactly one tenant prefix under t/, found %s\n' "${count}" >&2
    return 1
  fi
  printf '%s\n' "${hashes}"
}

# L0 data objects for the metrics signal:
#   t/<hash>/m/l0/<shard>/<writer>.<epoch>.<seq>.<hash16>.rseg
dr_l0_data_keys() {
  awk -F/ '$1 == "t" && $3 == "m" && $4 == "l0" && $0 ~ /\.rseg$/ { print }' <<<"$1"
}

# L0 commit records for the metrics signal:
#   t/<hash>/m/c/<shard>/<ingest_hour>/<writer>.<epoch>.<seq>.cmt
# The `l1.`/`rw.` compaction records and `retire.tmb` tombstones live in the
# same prefix and are not L0 records, so they are excluded by basename.
dr_l0_commit_keys() {
  awk -F/ '
    $1 == "t" && $3 == "m" && $4 == "c" && $0 ~ /\.cmt$/ {
      base = $NF
      if (base ~ /^l1\./ || base ~ /^rw\./) next
      print
    }
  ' <<<"$1"
}

# `<writer>.<epoch>.<seq>` out of an L0 data key or an L0 commit key. That
# triple is the identity a commit record and its data object share, and it is
# readable from the key alone, with no decode step.
dr_l0_identity() {
  local key="$1" base
  base="${key##*/}"
  if [[ "${key}" == *.rseg ]]; then
    # <writer>.<epoch>.<seq>.<hash16>.rseg: drop the content hash and suffix.
    base="${base%.rseg}"
    base="${base%.*}"
  else
    base="${base%.cmt}"
  fi
  printf '%s\n' "${base}"
}

# The `<seq>` component of an L0 identity, exactly as the key carries it:
# twenty decimal digits, zero padded (ravel-commit/src/keys.rs formats it
# `{seq:020}` and the parser rejects any other width).
DR_SEQ_WIDTH=20
# The offset the dangling-commit-record fault adds to reach a sequence number
# no writer epoch of this rehearsal produced.
DR_FORGED_SEQ_OFFSET=900000001

# Offset a padded seq component and re-pad the result to the key's own width.
#
# Two things go wrong without this. A zero-padded value inside `$(( ))` is
# parsed as OCTAL, so a seq containing an 8 or a 9 aborts the script with
# "value too great for base", and every other seq is silently read in base 8.
# And the sum of a padded value is unpadded, so the forged key carries a nine
# digit seq that the key parser rejects as malformed: the fault is then caught
# for the wrong reason, and would go on being "caught" if the corruption it
# models stopped being detected.
dr_forged_seq() {
  local seq="$1" stripped value
  if [[ ! "${seq}" =~ ^[0-9]{20}$ ]]; then
    printf 'dr: seq component "%s" is not %s digits; the key layout is frozen at that width\n' \
      "${seq}" "${DR_SEQ_WIDTH}" >&2
    return 1
  fi
  stripped="${seq#"${seq%%[!0]*}"}"
  [[ -n "${stripped}" ]] || stripped="0"
  if [[ "${#stripped}" -gt 18 ]]; then
    printf 'dr: seq component "%s" does not fit bash arithmetic; refusing to forge from it\n' \
      "${seq}" >&2
    return 1
  fi
  # 10# forces base 10 on the zero-padded value.
  value=$((10#${seq} + DR_FORGED_SEQ_OFFSET))
  printf '%0*d\n' "${DR_SEQ_WIDTH}" "${value}"
}

# ---------------------------------------------------------------------------
# ravel binaries. Prefer prebuilt binaries on PATH, fall back to cargo run so
# the same harness works on a runner with installed binaries and on a dev
# tree.
# ---------------------------------------------------------------------------

# A private directory for the two credential-bearing files below. Mode 700,
# and outside every path the CI artifact upload globs.
dr_private_dir() {
  local dir="${DR_LOG_DIR}/private"
  mkdir -p "${dir}"
  chmod 700 "${dir}"
  printf '%s\n' "${dir}"
}

# `<token>=<tenant>` in a file, for ravel-server's --tenant-token-file. The
# token is a credential and this file's own rule is that credentials never
# appear in a command line; --tenant-token would put it in argv, where every
# process listing on the host can read it.
dr_tenant_token_file() {
  local dir file
  dir="$(dr_private_dir)"
  file="${dir}/tenant-tokens"
  : >"${file}"
  chmod 600 "${file}"
  printf '%s=%s\n' "${DR_TENANT_TOKEN}" "${DR_TENANT}" >"${file}"
  printf '%s\n' "${file}"
}

# A curl config file carrying the tenant bearer header, for the same reason:
# `curl -H "Authorization: Bearer ..."` puts the token in argv.
dr_curl_auth_config() {
  local dir file
  dir="$(dr_private_dir)"
  file="${dir}/curl-auth.cfg"
  : >"${file}"
  chmod 600 "${file}"
  printf 'header = "Authorization: Bearer %s"\n' "${DR_TENANT_TOKEN}" >"${file}"
  printf '%s\n' "${file}"
}

# Export the RAVEL_S3_* fallbacks for one bucket. Against real S3 no
# RAVEL_S3_ENDPOINT is exported at all: S3Config.endpoint is Option<String>,
# and None is what selects AWS's regional endpoint.
dr_export_s3_env() {
  local bucket="$1"
  export RAVEL_S3_BUCKET="${bucket}"
  export RAVEL_S3_REGION="${DR_REGION}"
  export RAVEL_S3_ACCESS_KEY="${DR_ACCESS_KEY}"
  export RAVEL_S3_SECRET_KEY="${DR_SECRET_KEY}"
  if [[ -n "${DR_ENDPOINT}" ]]; then
    export RAVEL_S3_ENDPOINT="${DR_ENDPOINT}"
  else
    unset RAVEL_S3_ENDPOINT
  fi
  if [[ -n "${DR_SESSION_TOKEN}" ]]; then
    export RAVEL_S3_SESSION_TOKEN="${DR_SESSION_TOKEN}"
  else
    unset RAVEL_S3_SESSION_TOKEN
  fi
}

# The tenant-hash custody flags, as an array in DR_TENANCY_ARGS. A fresh
# bucket refuses every write until the deployment names one of these
# (ADR-0050 section 3), which is exactly the runbook's step-0 custody item.
dr_tenancy_args() {
  DR_TENANCY_ARGS=()
  if [[ "${DR_TENANT_HASH_MODE}" == "keyed" ]]; then
    DR_TENANCY_ARGS=(--tenant-hash-key-file "${DR_TENANT_HASH_KEY_FILE}")
  else
    DR_TENANCY_ARGS=(--tenant-hash-unkeyed)
  fi
}

dr_ravel_cli() {
  dr_tenancy_args
  if dr_have_command ravel-cli; then
    ravel-cli "${DR_TENANCY_ARGS[@]}" "$@"
  else
    cargo run --quiet -p ravel-cli -- "${DR_TENANCY_ARGS[@]}" "$@"
  fi
}

# argv for launching ravel-server, NUL delimited so a caller can background it
# and keep the PID.
dr_ravel_server_argv() {
  dr_tenancy_args
  if dr_have_command ravel-server; then
    printf '%s\0' ravel-server "${DR_TENANCY_ARGS[@]}" "$@"
  else
    printf '%s\0' cargo run --quiet -p ravel-server -- "${DR_TENANCY_ARGS[@]}" "$@"
  fi
}

dr_ravel_binaries_available() {
  if dr_have_command ravel-cli && dr_have_command ravel-server; then
    return 0
  fi
  dr_have_command cargo
}

# Run a command and return its exit code without ERREXIT unwinding the caller.
dr_run() {
  local code=0
  "$@" || code=$?
  return "${code}"
}

# ---------------------------------------------------------------------------
# Writers-stopped precondition. The restore checks run against a frozen
# bucket; a writer still flushing into B would move every figure under the
# assertion that reads it.
# ---------------------------------------------------------------------------

dr_writers_stopped() {
  if curl --silent --fail --max-time 2 "http://${DR_HTTP_ADDR}/metrics" >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Phase timing. rehearse.sh prints the wall clock of each phase and
# restore-check.sh prints its own four. The durations are reported, not
# asserted against a band: an asserted wall-clock band here would be an RTO
# claim, and docs/guides/disaster-recovery.md holds RPO and RTO unmeasured
# until a real rehearsal produces them. What IS asserted is that every phase
# emitted its timing line exactly once (dr_assert_timing_lines below), so a
# phase that silently did not run cannot pass as one that was fast.
# ---------------------------------------------------------------------------

dr_emit_phase_seconds() {
  local name="$1" start_ns="$2" end_ns="$3"
  awk -v n="${name}" -v a="${start_ns}" -v b="${end_ns}" \
    'BEGIN { printf "phase %s seconds=%.3f\n", n, (b - a) / 1000000000 }'
}

# Assert the captured output carries exactly one `phase <name> seconds=` line
# per expected phase, and no others.
dr_assert_timing_lines() {
  local text="$1"
  shift
  local expected=("$@") name seen total
  for name in "${expected[@]}"; do
    seen="$(awk -v n="phase ${name} seconds=" '
      BEGIN { c = 0 } index($0, n) == 1 { c++ } END { print c }
    ' <<<"${text}")"
    if [[ "${seen}" -ne 1 ]]; then
      printf 'dr: phase %s emitted %s timing lines, expected exactly 1\n' \
        "${name}" "${seen}" >&2
      return 1
    fi
  done
  total="$(awk 'BEGIN { c = 0 } /^phase .* seconds=/ { c++ } END { print c }' <<<"${text}")"
  if [[ "${total}" -ne "${#expected[@]}" ]]; then
    printf 'dr: %s phase timing lines for %s expected phases\n' \
      "${total}" "${#expected[@]}" >&2
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Dry run. Every script takes --dry-run and validates what it can without
# touching a bucket, a binary, or the network.
# ---------------------------------------------------------------------------

dr_dry_run_common() {
  local script_name="$1"
  printf 'dry run: %s\n' "${script_name}"
  printf '  endpoint: %s\n' "${DR_ENDPOINT:-<none: AWS regional endpoint>}"
  printf '  region: %s\n' "${DR_REGION}"
  printf '  bucket A (primary): %s\n' "${DR_BUCKET_PRIMARY}"
  printf '  bucket B (restore target): %s\n' "${DR_BUCKET_REPLICA}"
  printf '  tenant: %s (shards %s)\n' "${DR_TENANT}" "${DR_SHARDS}"
  printf '  tenant hash mode: %s\n' "${DR_TENANT_HASH_MODE}"
  printf '  credentials: from DR_ACCESS_KEY / DR_SECRET_KEY (not shown)\n'
  if [[ -n "${DR_SESSION_TOKEN}" ]]; then
    printf '  session token: set (mc receives it in the MC_HOST_dr URL)\n'
  else
    printf '  session token: not set\n'
  fi
  printf '  log dir: %s\n' "${DR_LOG_DIR}"
  printf '  reconciled marker key: %s\n' "${DR_MARKER_KEY}"
  printf '  restore-start stamp key: %s\n' "${DR_RESTORE_START_KEY}"
  printf '  bucket creation marker key: %s\n' "${DR_BUCKET_MARKER_KEY}"
  if dr_mc_available; then
    printf '  OK    mc available (binary on PATH or docker for the pinned image)\n'
  else
    printf '  WARN  no mc and no docker: a real run cannot reach the buckets\n'
  fi
  if dr_ravel_binaries_available; then
    printf '  OK    ravel binaries runnable (prebuilt or via cargo)\n'
  else
    printf '  WARN  no ravel-cli/ravel-server and no cargo: a real run cannot verify\n'
  fi
}
