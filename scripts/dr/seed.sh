#!/usr/bin/env bash
# Usage: scripts/dr/seed.sh [--exports N] [--reset] [--dry-run] [--help]
#
# Step 1 of the disaster-recovery rehearsal (issue #814): write a known corpus
# through a real ravel-server into bucket A, stop the writer, and pre-register
# the figures every later phase asserts against.
#
# The corpus is N OTLP metric exports, each one resource, one gauge series and
# one sample, POSTed under strict ack so an accepted export is a durable L0
# data object plus its commit record before the next one is sent. N defaults
# to 12, small enough for CI and large enough that the fault matrix has more
# than one L0 object to work with.
#
#   --exports N   corpus size (default 12, or DR_SEED_EXPORTS)
#   --reset       empty bucket A first (a rehearsal starts from a known
#                 corpus, so a non-empty bucket A is refused without this)
#   --dry-run     validate configuration and dependencies, touch nothing
#
# Exit 0 on success, 64 on bad usage, 65 on an unmet precondition, 1 when a
# seeded figure lands outside its band.
set -euo pipefail

# shellcheck source=scripts/dr/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/dr/seed.sh [--exports N] [--reset] [--dry-run] [--help]

Writes a known corpus through a real ravel-server into bucket A, stops the
writer, inventories the bucket, and pre-registers the expected figures other
phases assert against (DR_EXPECT_* in <DR_LOG_DIR>/dr-expect.env).

Options:
  --exports N   corpus size in OTLP exports (default 12, or DR_SEED_EXPORTS)
  --reset       empty bucket A before seeding
  --dry-run     validate configuration and dependencies without touching a
                bucket, a binary or the network
  --help, -h    this message

USAGE
  dr_usage_environment
}

EXPORTS="${DR_SEED_EXPORTS:-12}"
RESET=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exports)
      [[ $# -ge 2 ]] || dr_die "${DR_EX_USAGE}" "--exports needs a value"
      EXPORTS="$2"
      shift 2
      ;;
    --reset)
      RESET=1
      shift
      ;;
    --dry-run | --check)
      DRY_RUN=1
      shift
      ;;
    --help | -h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      dr_die "${DR_EX_USAGE}" "unknown argument: $1"
      ;;
  esac
done

[[ "${EXPORTS}" =~ ^[0-9]+$ ]] || dr_die "${DR_EX_USAGE}" "--exports must be an integer"
[[ "${EXPORTS}" -ge 2 ]] || dr_die "${DR_EX_USAGE}" \
  "--exports must be at least 2: the fault matrix needs more than one L0 object"

dr_init

if [[ "${DRY_RUN}" -eq 1 ]]; then
  dr_dry_run_common "seed.sh"
  printf '  corpus: %s OTLP metric exports, 1 sample and 1 series each\n' "${EXPORTS}"
  printf '  would: create %s, qualify the store, start ravel-server on %s,\n' \
    "${DR_BUCKET_PRIMARY}" "${DR_HTTP_ADDR}"
  printf '         POST %s exports under strict ack, stop the writer, and\n' "${EXPORTS}"
  printf '         pre-register DR_EXPECT_* into %s\n' "$(dr_expect_file)"
  printf '  bands: accepted exports [%s,%s]; L0 data objects [2,%s];\n' \
    "${EXPORTS}" "${EXPORTS}" "${EXPORTS}"
  printf '         L0 commit records == L0 data objects; samples [%s,%s];\n' \
    "${EXPORTS}" "${EXPORTS}"
  printf '         total objects [3,%s]\n' "$((2 * EXPORTS + 16))"
  exit 0
fi

dr_mc_available || dr_die "${DR_EX_PRECONDITION}" \
  "no mc binary and no docker: cannot reach the object store"
dr_ravel_binaries_available || dr_die "${DR_EX_PRECONDITION}" \
  "no ravel-server/ravel-cli and no cargo: cannot seed"

SEED_LOG="${DR_LOG_DIR}/seed-server.log"
FIXTURE="${DR_LOG_DIR}/otlp-fixture.bin"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

dr_export_s3_env "${DR_BUCKET_PRIMARY}"

dr_log "ensuring bucket ${DR_BUCKET_PRIMARY} exists"
dr_mc mb -p "dr/${DR_BUCKET_PRIMARY}" >/dev/null 2>&1 || true

existing="$(dr_list_keys "${DR_BUCKET_PRIMARY}")" || dr_die "${DR_EX_PRECONDITION}" \
  "could not list ${DR_BUCKET_PRIMARY}"
existing_count="$(dr_count_lines "${existing}")"
if [[ "${existing_count}" -gt 0 ]]; then
  if [[ "${RESET}" -eq 1 ]]; then
    dr_log "emptying bucket ${DR_BUCKET_PRIMARY} (${existing_count} object(s))"
    dr_mc rm --recursive --force "dr/${DR_BUCKET_PRIMARY}/" >/dev/null
  else
    dr_die "${DR_EX_PRECONDITION}" \
      "bucket ${DR_BUCKET_PRIMARY} already holds ${existing_count} object(s); pass --reset to empty it"
  fi
fi

# A fresh expectations file: a stale band from an earlier run is worse than no
# band, because it would still pass an assertion.
: >"$(dr_expect_file)"

# ADR-0050 EC7: a non-Memory store refuses to serve until `sys/qualification`
# exists, and there is no bootstrap-and-continue path.
dr_log "qualifying the store backend"
dr_ravel_cli --store s3 store qualify

dr_log "generating the OTLP fixture generator (first build may be slow)"
gen_fixture() {
  if dr_have_command ravel-server-gen-otlp-fixture; then
    ravel-server-gen-otlp-fixture >"${FIXTURE}"
  else
    cargo run --quiet -p ravel-server --example gen_otlp_fixture >"${FIXTURE}"
  fi
}
gen_fixture

dr_log "starting ravel-server against ${DR_BUCKET_PRIMARY}"
declare -a SERVER_ARGV=()
mapfile -d '' -t SERVER_ARGV < <(dr_ravel_server_argv \
  --store s3 \
  --listen-http "${DR_HTTP_ADDR}" \
  --listen-grpc "${DR_GRPC_ADDR}" \
  --tenant-token "${DR_TENANT_TOKEN}=${DR_TENANT}")
"${SERVER_ARGV[@]}" >"${SEED_LOG}" 2>&1 &
SERVER_PID=$!

server_ready() {
  curl --silent --fail --max-time 2 \
    -H "Authorization: Bearer ${DR_TENANT_TOKEN}" \
    "http://${DR_HTTP_ADDR}/api/v1/query?query=up" >/dev/null 2>&1
}

ready=0
for _ in $(seq 1 60); do
  if server_ready; then
    ready=1
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    break
  fi
  sleep 1
done
if [[ "${ready}" -ne 1 ]]; then
  dr_log "ravel-server did not become ready; last 40 log lines follow"
  tail -n 40 "${SEED_LOG}" >&2 || true
  dr_die "${DR_EX_PRECONDITION}" "ravel-server did not become ready on ${DR_HTTP_ADDR}"
fi

# One export per iteration, each with a freshly generated fixture so the
# samples carry distinct event timestamps. An export is counted only when the
# server returned a strict-ack commit token: an accepted-but-untokened export
# is not durable and must not be counted as corpus.
dr_log "POSTing ${EXPORTS} export(s) under strict ack"
accepted=0
header_file="${DR_LOG_DIR}/seed-export-headers.txt"
for _ in $(seq 1 "${EXPORTS}"); do
  gen_fixture
  code=0
  curl --silent --show-error --fail \
    --dump-header "${header_file}" \
    --output /dev/null \
    -X POST "http://${DR_HTTP_ADDR}/v1/metrics" \
    -H "Authorization: Bearer ${DR_TENANT_TOKEN}" \
    -H "Content-Type: application/x-protobuf" \
    --data-binary "@${FIXTURE}" || code=$?
  if [[ "${code}" -ne 0 ]]; then
    dr_log "export rejected (curl exit ${code})"
    continue
  fi
  headers="$(cat "${header_file}")"
  token="$(awk 'BEGIN { IGNORECASE = 1 } /^x-ravel-commit-token:/ { print $2 }' \
    <<<"${headers}" | tr -d '\r')"
  if [[ -z "${token}" ]]; then
    dr_log "export carried no strict-ack commit token; not counted"
    continue
  fi
  accepted=$((accepted + 1))
done
rm -f "${header_file}"

dr_log "stopping the writer"
cleanup
SERVER_PID=""
for _ in $(seq 1 30); do
  if dr_writers_stopped; then
    break
  fi
  sleep 1
done
dr_writers_stopped || dr_die "${DR_EX_PRECONDITION}" \
  "a writer is still answering on ${DR_HTTP_ADDR} after the stop"

dr_log "inventorying ${DR_BUCKET_PRIMARY}"
keys="$(dr_list_keys "${DR_BUCKET_PRIMARY}")" || dr_die "${DR_EX_PRECONDITION}" \
  "could not list ${DR_BUCKET_PRIMARY}"
tenant_hash="$(dr_tenant_hash_from_keys "${keys}")"
data_keys="$(dr_l0_data_keys "${keys}")"
commit_keys="$(dr_l0_commit_keys "${keys}")"
total_objects="$(dr_count_lines "${keys}")"
data_objects="$(dr_count_lines "${data_keys}")"
commit_records="$(dr_count_lines "${commit_keys}")"

# Bands, all derived from the corpus:
#   accepted exports        exactly the number requested. An export that was
#                           not acked is not corpus, and a corpus short of
#                           what was asked for is a failed seed, not a
#                           smaller rehearsal.
#   L0 data objects         [2, exports]. Each accepted export is durable at
#                           ack time, so it contributes at most one L0 object,
#                           and several exports may share a flush. The lower
#                           bound of 2 is the rehearsal's own precondition:
#                           the fault matrix removes one whole L0 pair and
#                           still needs the tenant to hold data afterwards.
#   L0 commit records       exactly the number of L0 data objects. A record
#                           without an object, or an object without a record,
#                           is the custody anomaly this whole harness exists
#                           to catch, and bucket A must not start with one.
#   samples                 exactly the number of accepted exports: the
#                           fixture is one resource, one gauge, one data
#                           point, so one export is exactly one sample. Every
#                           export carries the SAME labels, so the series
#                           count is per segment rather than per export and
#                           gets a [1, exports] band at the canary instead.
#   total objects           [3, 2 * exports + 16]. Two objects per flush plus
#                           the bounded control set (sys/qualification,
#                           sys/tenancy, sys/t/<hash>, t/<hash>/config,
#                           t/<hash>/m/meta, t/<hash>/enc,
#                           t/<hash>/metrics/prov and room to spare).
failures=0
dr_assert_figure "seed.accepted_exports" "${accepted}" "${EXPORTS}" "${EXPORTS}" || failures=1
dr_assert_figure "seed.l0_data_objects" "${data_objects}" 2 "${EXPORTS}" || failures=1
dr_assert_figure "seed.l0_commit_records" "${commit_records}" \
  "${data_objects}" "${data_objects}" || failures=1
dr_assert_figure "seed.total_objects" "${total_objects}" 3 "$((2 * EXPORTS + 16))" || failures=1
if [[ "${failures}" -ne 0 ]]; then
  dr_die 1 "seeded figures outside their bands; bucket A is not a known corpus"
fi

{
  printf 'DR_EXPECT_EXPORTS=%s\n' "${accepted}"
  printf 'DR_EXPECT_SAMPLES=%s\n' "${accepted}"
  printf 'DR_EXPECT_DATA_OBJECTS=%s\n' "${data_objects}"
  printf 'DR_EXPECT_COMMIT_RECORDS=%s\n' "${commit_records}"
  printf 'DR_EXPECT_TOTAL_OBJECTS=%s\n' "${total_objects}"
  printf 'DR_EXPECT_TENANT_HASH=%s\n' "${tenant_hash}"
} >>"$(dr_expect_file)"

dr_log "seeded ${accepted} export(s) into ${DR_BUCKET_PRIMARY}; figures pre-registered"
printf 'seed: bucket=%s tenant_hash=%s exports=%s data_objects=%s commit_records=%s total_objects=%s\n' \
  "${DR_BUCKET_PRIMARY}" "${tenant_hash}" "${accepted}" "${data_objects}" \
  "${commit_records}" "${total_objects}"
