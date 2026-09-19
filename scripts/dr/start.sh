#!/usr/bin/env bash
# Usage: scripts/dr/start.sh [--background] [--dry-run] [--help]
#
# Step 5 of the disaster-recovery rehearsal (issue #814): start ravel-server
# against the restored bucket B, and refuse to start it at all unless the
# reconciled marker restore-check.sh writes is present in B and names B.
#
# This is the ordering the rehearsal exists to prove, made mechanical rather
# than procedural: the restore checklist says the custody, reconstruction,
# catalog and canary checks run BEFORE anything serves, and the only way to
# satisfy this script is to have passed all four. The marker names the bucket
# it was written for, so a marker left in place by a restore into a different
# bucket cannot satisfy a start against this one.
#
# A marker in the right bucket is still not enough. restore-check.sh writes a
# restore-start stamp OBJECT into the bucket before its first check, and this
# script requires the marker to post-date that stamp and to carry the same
# value. A marker from an EARLIER restore of the same bucket is therefore
# refused: "not stamped in the future" is a clock check, not an ordering one,
# and on its own it accepts any marker old enough.
#
#   --background  start the server, wait for it to answer, and return, leaving
#                 it running with its pid in <DR_LOG_DIR>/dr-server.pid. The
#                 default runs it in the foreground.
#   --dry-run     report whether the marker would allow a start, start nothing
#
# Exit 0 when the server started (or, under --dry-run, when the configuration
# is usable), 64 on bad usage, 65 on an unmet precondition, and 10 when the
# start is REFUSED because the marker is absent, unreadable, names another
# bucket, or is not older than this start.
set -euo pipefail

# shellcheck source=scripts/dr/lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

DR_EX_REFUSED=10

usage() {
  cat <<'USAGE'
Usage: scripts/dr/start.sh [--background] [--dry-run] [--help]

Starts ravel-server against bucket B, and only if the reconciled marker
written by restore-check.sh exists in B, names B, and belongs to the restore
whose restore-start stamp is currently in B.

A refusal prints `dr-start-refused: <reason>` and exits 10. A start prints
`dr-start-ok: bucket=<B> restore_start_ns=<n> marker_ns=<n> started_ns=<n>`
and records the start time in <DR_LOG_DIR>/dr-server-started-at, so a caller
can assert the marker was written during this restore and before the server
was started rather than taking either on trust.

Options:
  --background  return once the server answers, leaving it running
  --dry-run     report whether the marker would allow a start, start nothing
  --help, -h    this message

USAGE
  dr_usage_environment
}

BACKGROUND=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --background)
      BACKGROUND=1
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

dr_init

MARKER_FILE="${DR_LOG_DIR}/reconciled.json"
RESTORE_START_FILE="${DR_LOG_DIR}/restore-start.json"

# Fetch the marker from bucket B. Returns 1 when it is not there at all, which
# is the ordinary "the restore checks have not passed yet" case rather than an
# error to report as a failure of this script.
fetch_marker() {
  dr_mc cat "dr/${DR_BUCKET_REPLICA}/${DR_MARKER_KEY}" \
    >"${MARKER_FILE}" 2>"${DR_LOG_DIR}/start-marker-fetch.log"
}

fetch_restore_start() {
  dr_mc cat "dr/${DR_BUCKET_REPLICA}/${DR_RESTORE_START_KEY}" \
    >"${RESTORE_START_FILE}" 2>"${DR_LOG_DIR}/start-restore-start-fetch.log"
}

# The single value of a JSON field in one of the two objects, quotes and
# trailing comma removed. Both are written one field per line, so this needs no
# JSON parser, and it goes through dr_field_once so a field that appears twice
# is refused exactly as a missing one is.
json_field() {
  local file="$1" raw
  raw="$(dr_field_once "  \"$2\"" "$(cat "${file}")")" || return 1
  raw="${raw%,}"
  raw="${raw#\"}"
  raw="${raw%\"}"
  printf '%s\n' "${raw}"
}

marker_field() { json_field "${MARKER_FILE}" "$1"; }

if [[ "${DRY_RUN}" -eq 1 ]]; then
  dr_dry_run_common "start.sh"
  printf '  would: refuse unless %s/%s exists and its "bucket" field is %s\n' \
    "${DR_BUCKET_REPLICA}" "${DR_MARKER_KEY}" "${DR_BUCKET_REPLICA}"
  printf '  would: refuse unless %s/%s exists and the marker carries and\n' \
    "${DR_BUCKET_REPLICA}" "${DR_RESTORE_START_KEY}"
  printf '         post-dates its restore_started_at_unix_ns\n'
  if [[ "${BACKGROUND}" -eq 1 ]]; then
    printf '  would: start ravel-server in the background on %s\n' "${DR_HTTP_ADDR}"
  else
    printf '  would: start ravel-server in the foreground on %s\n' "${DR_HTTP_ADDR}"
  fi
  if dr_mc_available && fetch_marker; then
    printf '  marker present, names bucket: %s\n' "$(marker_field bucket || printf '<unreadable>')"
  else
    printf '  no marker reachable: a real run would refuse with exit 10\n'
  fi
  exit 0
fi

dr_mc_available || dr_die "${DR_EX_PRECONDITION}" \
  "no mc binary and no docker: cannot read the reconciled marker"
dr_ravel_binaries_available || dr_die "${DR_EX_PRECONDITION}" \
  "no ravel-server and no cargo: cannot start"

refuse() {
  printf 'dr-start-refused: %s\n' "$*"
  exit "${DR_EX_REFUSED}"
}

rm -f "${MARKER_FILE}"
fetch_marker || refuse \
  "no reconciled marker at ${DR_BUCKET_REPLICA}/${DR_MARKER_KEY}; the restore checks have not passed"
[[ -s "${MARKER_FILE}" ]] || refuse \
  "the reconciled marker at ${DR_BUCKET_REPLICA}/${DR_MARKER_KEY} is empty"

marker_bucket="$(marker_field bucket)" || refuse \
  "the reconciled marker carries no single \"bucket\" field"
[[ "${marker_bucket}" == "${DR_BUCKET_REPLICA}" ]] || refuse \
  "the reconciled marker names bucket ${marker_bucket}, not ${DR_BUCKET_REPLICA}"

marker_ns="$(marker_field reconciled_at_unix_ns)" || refuse \
  "the reconciled marker carries no single \"reconciled_at_unix_ns\" field"
[[ "${marker_ns}" =~ ^[0-9]+$ ]] || refuse \
  "the reconciled marker's reconciled_at_unix_ns is not an integer: ${marker_ns}"

# The marker must belong to the restore that is running now, not to any
# earlier restore of the same bucket. restore-check.sh writes this stamp into
# the bucket before its first check and copies it into the marker it writes
# after the last one, so a marker that post-dates the stamp AND carries its
# value can only have been written by the run that wrote the stamp.
rm -f "${RESTORE_START_FILE}"
fetch_restore_start || refuse \
  "no restore-start stamp at ${DR_BUCKET_REPLICA}/${DR_RESTORE_START_KEY}; restore-check.sh has not run against this bucket"
[[ -s "${RESTORE_START_FILE}" ]] || refuse \
  "the restore-start stamp at ${DR_BUCKET_REPLICA}/${DR_RESTORE_START_KEY} is empty"

restore_start_ns="$(json_field "${RESTORE_START_FILE}" restore_started_at_unix_ns)" || refuse \
  "the restore-start stamp carries no single \"restore_started_at_unix_ns\" field"
[[ "${restore_start_ns}" =~ ^[0-9]+$ ]] || refuse \
  "the restore-start stamp's restore_started_at_unix_ns is not an integer: ${restore_start_ns}"

marker_start_ns="$(marker_field restore_started_at_unix_ns)" || refuse \
  "the reconciled marker carries no single \"restore_started_at_unix_ns\" field; it predates the restore-start stamp requirement"
[[ "${marker_start_ns}" == "${restore_start_ns}" ]] || refuse \
  "the reconciled marker belongs to the restore started at ${marker_start_ns}, but the bucket's restore-start stamp is ${restore_start_ns}"

dr_ns_not_after "${restore_start_ns}" "${marker_ns}" || refuse \
  "the reconciled marker is stamped ${marker_ns}, before the restore started at ${restore_start_ns}"

started_ns="$(dr_now_ns)"
dr_ns_not_after "${marker_ns}" "${started_ns}" || refuse \
  "the reconciled marker is stamped ${marker_ns}, after this start at ${started_ns}"

printf '%s\n' "${started_ns}" >"${DR_LOG_DIR}/dr-server-started-at"
printf 'dr-start-ok: bucket=%s restore_start_ns=%s marker_ns=%s started_ns=%s\n' \
  "${DR_BUCKET_REPLICA}" "${restore_start_ns}" "${marker_ns}" "${started_ns}"

dr_export_s3_env "${DR_BUCKET_REPLICA}"

# The tenant token reaches the server through a mode-600 file, not through
# argv: a credential on a command line is readable by every process listing on
# the host, and this harness's own rule forbids it.
TOKEN_FILE="$(dr_tenant_token_file)"
CURL_AUTH="$(dr_curl_auth_config)"

declare -a SERVER_ARGV=()
mapfile -d '' -t SERVER_ARGV < <(dr_ravel_server_argv \
  --store s3 \
  --listen-http "${DR_HTTP_ADDR}" \
  --listen-grpc "${DR_GRPC_ADDR}" \
  --tenant-token-file "${TOKEN_FILE}")

if [[ "${BACKGROUND}" -eq 0 ]]; then
  dr_log "starting ravel-server against ${DR_BUCKET_REPLICA} (foreground)"
  exec "${SERVER_ARGV[@]}"
fi

SERVER_LOG="${DR_LOG_DIR}/start-server.log"
dr_log "starting ravel-server against ${DR_BUCKET_REPLICA} (background)"
"${SERVER_ARGV[@]}" >"${SERVER_LOG}" 2>&1 &
server_pid=$!
printf '%s\n' "${server_pid}" >"${DR_LOG_DIR}/dr-server.pid"

ready=0
for _ in $(seq 1 60); do
  if curl --silent --fail --max-time 2 \
    --config "${CURL_AUTH}" \
    "http://${DR_HTTP_ADDR}/api/v1/query?query=up" >/dev/null 2>&1; then
    ready=1
    break
  fi
  if ! kill -0 "${server_pid}" 2>/dev/null; then
    break
  fi
  sleep 1
done
if [[ "${ready}" -ne 1 ]]; then
  dr_log "ravel-server did not become ready; last 40 log lines follow"
  tail -n 40 "${SERVER_LOG}" >&2 || true
  dr_die "${DR_EX_PRECONDITION}" \
    "ravel-server did not become ready on ${DR_HTTP_ADDR} after the marker allowed the start"
fi

printf 'dr-start-serving: bucket=%s pid=%s addr=%s\n' \
  "${DR_BUCKET_REPLICA}" "${server_pid}" "${DR_HTTP_ADDR}"
