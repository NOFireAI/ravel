#!/usr/bin/env bash
# Usage: scripts/chaos/lib.test.sh [--help]
#
# Cases for the helpers in scripts/chaos/lib.sh (issue #534), runnable with no
# object store, no server and no cluster.
#
# Every oracle in the chaos lane reads its evidence through three helpers: the
# /metrics parser, the commit-token extraction from an export's response
# headers, and the read-your-write query. A parser that never matches reports
# every metric as absent, a token mangled on the way to `min_commit_token`
# makes every acked write look lost, and both show up as a nightly run that
# fails for the wrong reason or passes for none. These cases pin each helper
# against the real output shapes:
#
#   * the /metrics text is in the exact form services/ravel-server/src/
#     metrics.rs renders (`write_header` + `write_sample`: every family this
#     lane reads carries a `mode` label, and the per-signal families a
#     `signal` label too), plus the exposition-format cases the renderer is
#     allowed to grow into (an optional timestamp, a label value holding a
#     space or a brace);
#   * the commit token is `CommitToken::encode()`'s shape (URL-safe base64,
#     no padding, of `v2:<shard>:<writer-uuid>:<epoch>:<seq>:<hour>`), and a
#     multi-shard write returns them comma-joined in one
#     `x-ravel-commit-token` header (services/ravel-server/src/otlp_http.rs,
#     `encode_commit_tokens`).
#
# `curl` is replaced by a shell function for the helpers that scrape or POST,
# so the network is never touched.
#
# Exit 0 when every case passes, 1 when one fails, 64 on bad usage.
set -uo pipefail

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  printf 'Usage: scripts/chaos/lib.test.sh\n\nRuns the cases for the helpers in scripts/chaos/lib.sh.\n'
  exit 0
fi
if [[ $# -gt 0 ]]; then
  printf 'lib.test.sh: unexpected argument: %s\n' "$1" >&2
  exit 64
fi

CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${CHAOS_DIR}/../.." && pwd)"
# shellcheck source=scripts/chaos/lib.sh
source "${CHAOS_DIR}/lib.sh"

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

rc_of() {
  local code=0
  "$@" >/dev/null 2>&1 || code=$?
  printf '%s\n' "${code}"
}

SCRATCH="$(mktemp -d)"
trap 'rm -rf "${SCRATCH}"' EXIT

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------

METRICS_SAMPLE='# HELP ravel_ingest_flushes_by_size_total Flushes opened because the tenant buffer reached target_bytes, by signal.
# TYPE ravel_ingest_flushes_by_size_total counter
ravel_ingest_flushes_by_size_total{mode="all",signal="metrics"} 5
ravel_ingest_flushes_by_size_total{mode="all",signal="logs"} 6
ravel_ingest_flushes_by_size_total{mode="all",signal="traces"} 0
# HELP ravel_ingest_flushes_by_age_total Flushes opened because the oldest buffered entry reached max_age, by signal.
# TYPE ravel_ingest_flushes_by_age_total counter
ravel_ingest_flushes_by_age_total{mode="all",signal="metrics"} 9
# HELP ravel_maintain_workers_live In-process maintenance workers this supervisor currently sees as live (ADR-0065 decision 1).
# TYPE ravel_maintain_workers_live gauge
ravel_maintain_workers_live{mode="maintain"} 2
# HELP ravel_maintain_units_owned Owned (tenant, signal, shard) units this process is currently maintaining (ADR-0065 decision 2).
# TYPE ravel_maintain_units_owned gauge
ravel_maintain_units_owned{mode="maintain"} 4
ravel_maintain_units_owned_total{mode="maintain"} 99
# HELP ravel_maintain_units_stalled Owned units with consecutive failing ticks past the configured threshold (ADR-0065 decision 2).
# TYPE ravel_maintain_units_stalled gauge
ravel_maintain_units_stalled{mode="maintain"} 0
# TYPE ravel_maintain_conservation_aborts_total counter
ravel_maintain_conservation_aborts_total{mode="maintain",signal="metrics"} 1
ravel_maintain_conservation_aborts_total{mode="maintain",signal="logs"} 0
# TYPE ravel_maintain_orphan_breaker_tripped_total counter
ravel_maintain_orphan_breaker_tripped_total{mode="maintain",signal="metrics"} 2
ravel_maintain_orphan_breaker_tripped_total{mode="maintain",signal="logs"} 0
# TYPE ravel_maintain_orphans_present gauge
ravel_maintain_orphans_present{mode="maintain",signal="metrics"} 7
# TYPE ravel_maintain_orphans_withheld gauge
ravel_maintain_orphans_withheld{mode="maintain",signal="metrics"} 7
# TYPE ravel_exposition_edge gauge
ravel_exposition_edge{route="/a b}",signal="metrics"} 3 1695800000000
ravel_exposition_edge{route="q\"x",signal="logs"} 4
# TYPE ravel_build_up gauge
ravel_build_up 1'

# CommitToken::encode(): URL-safe base64, no padding, over the v2 text form.
# Two shards of one write, as a multi-shard export returns them.
b64url() {
  printf '%s' "$1" | base64 -w0 | tr '+/' '-_' | tr -d '='
}
TOKEN_A="$(b64url 'v2:0:3f6c1b2e-9a7d-4c1e-bf00-7e5a9d2c4b11:1:42:494387')"
TOKEN_B="$(b64url 'v2:3:3f6c1b2e-9a7d-4c1e-bf00-7e5a9d2c4b11:1:43:494387')"

# ---------------------------------------------------------------------------
# metric_value_from_body: label-aware parse of a captured /metrics body.
# ---------------------------------------------------------------------------

mv() { metric_value_from_body "${METRICS_SAMPLE}" "$@"; }

check "labeled gauge, single sample" "4" "$(mv ravel_maintain_units_owned)"
check "family summed across signals with no selector" "11" \
  "$(mv ravel_ingest_flushes_by_size_total)"
check "selector signal=metrics picks one sample" "5" \
  "$(mv ravel_ingest_flushes_by_size_total signal=metrics)"
check "two selectors both applied" "6" \
  "$(mv ravel_ingest_flushes_by_size_total mode=all signal=logs)"
check "selector matching no sample reads as absent" "" \
  "$(mv ravel_ingest_flushes_by_size_total mode=maintain signal=metrics)"
check "selector on a label the family lacks reads as absent" "" \
  "$(mv ravel_maintain_units_owned signal=metrics)"
check "zero-valued sample is present, not absent" "0" \
  "$(mv ravel_maintain_units_stalled)"
check "name prefix of a longer family does not match it" "4" \
  "$(mv ravel_maintain_units_owned mode=maintain)"
check "longer family read on its own" "99" "$(mv ravel_maintain_units_owned_total)"
check "absent family reads as empty" "" "$(mv ravel_not_a_metric)"
check "HELP/TYPE comment lines are not samples" "" "$(mv HELP)"
check "unlabeled sample" "1" "$(mv ravel_build_up)"
check "value is read before an optional timestamp" "3" \
  "$(mv ravel_exposition_edge signal=metrics)"
check "label value with space and brace" "3" \
  "$(mv ravel_exposition_edge 'route=/a b}')"
check "escaped quote in a label value" "4" \
  "$(mv ravel_exposition_edge 'route=q"x')"
check "conservation aborts summed across signals" "1" \
  "$(mv ravel_maintain_conservation_aborts_total)"
check "orphan gauges" "7/7" \
  "$(mv ravel_maintain_orphans_present)/$(mv ravel_maintain_orphans_withheld)"

# ---------------------------------------------------------------------------
# metric_value / wait_for_metric_at_least / wait_for_flush_started, over a
# stubbed curl that serves the sample.
# ---------------------------------------------------------------------------

CURL_MODE="serve-metrics"
CURL_ARGS_FILE="${SCRATCH}/curl-args"
curl() {
  printf '%s\n' "$@" >"${CURL_ARGS_FILE}"
  case "${CURL_MODE}" in
    serve-metrics) printf '%s\n' "${METRICS_SAMPLE}" ;;
    unreachable) return 7 ;;
    export)
      local dump="" prev=""
      local arg
      for arg in "$@"; do
        [[ "${prev}" == "--dump-header" ]] && dump="${arg}"
        prev="${arg}"
      done
      printf '%s' "${EXPORT_HEADERS}" >"${dump}"
      ;;
    query) printf '%s' "${QUERY_BODY}" ;;
  esac
}

check "metric_value over the stub" "4" \
  "$(metric_value http://stub ravel_maintain_units_owned)"
check "metric_value forwards selectors" "6" \
  "$(metric_value http://stub ravel_ingest_flushes_by_size_total signal=logs)"
check "metric_value scrape failure is rc 1" "1" \
  "$(CURL_MODE=unreachable rc_of metric_value http://stub ravel_maintain_units_owned)"
check "wait_for_metric_at_least reaches threshold" "0" \
  "$(rc_of wait_for_metric_at_least http://stub ravel_maintain_units_owned 4 2)"
check "wait_for_metric_at_least honours a selector" "1" \
  "$(rc_of wait_for_metric_at_least http://stub ravel_ingest_flushes_by_size_total 6 1 signal=metrics)"
# The load is metrics-only, so only the metrics pipeline's flushes count:
# baseline 4 -> 5 is a rise, baseline 5 is not, even though the logs pipeline
# alone holds 6 and the unselected total is 11.
check "flush trigger fires on a metrics-signal rise" "0" \
  "$(rc_of wait_for_flush_started http://stub 4 2)"
check "flush trigger ignores other signals' flushes" "1" \
  "$(rc_of wait_for_flush_started http://stub 5 1)"

# ---------------------------------------------------------------------------
# Commit tokens: opaque strings, extracted verbatim.
# ---------------------------------------------------------------------------

CRLF=$'\r\n'
HDR_ONE="HTTP/1.1 200 OK${CRLF}content-type: application/x-protobuf${CRLF}x-ravel-commit-token: ${TOKEN_A}${CRLF}${CRLF}"
HDR_MIXED_CASE="HTTP/1.1 200 OK${CRLF}X-Ravel-Commit-Token: ${TOKEN_A}${CRLF}${CRLF}"
HDR_TWO="HTTP/1.1 200 OK${CRLF}x-ravel-commit-token: ${TOKEN_A},${TOKEN_B}${CRLF}${CRLF}"
HDR_NONE="HTTP/1.1 200 OK${CRLF}content-type: application/x-protobuf${CRLF}${CRLF}"

check "token fixture is the encoded v2 form" "v2:0:3f6c1b2e-9a7d-4c1e-bf00-7e5a9d2c4b11:1:42:494387" \
  "$(printf '%s' "${TOKEN_A}" | base64 -d 2>/dev/null)"
check "single token extracted verbatim" "${TOKEN_A}" \
  "$(commit_tokens_from_headers "${HDR_ONE}")"
check "header name matched case-insensitively" "${TOKEN_A}" \
  "$(commit_tokens_from_headers "${HDR_MIXED_CASE}")"
check "comma-joined multi-shard tokens split one per line" "${TOKEN_A}"$'\n'"${TOKEN_B}" \
  "$(commit_tokens_from_headers "${HDR_TWO}")"
check "no token header prints nothing" "" "$(commit_tokens_from_headers "${HDR_NONE}")"
check "commit_tokens_from_headers refuses when no token" "1" \
  "$(rc_of commit_tokens_from_headers "${HDR_NONE}")"

FIXTURE="${SCRATCH}/fixture.pb"
printf 'x' >"${FIXTURE}"
check "drive_one_export echoes every token" "${TOKEN_A}"$'\n'"${TOKEN_B}" \
  "$(CURL_MODE=export EXPORT_HEADERS="${HDR_TWO}" drive_one_export 127.0.0.1:1 "${FIXTURE}")"
check "drive_one_export with no token is rc 1" "1" \
  "$(CURL_MODE=export EXPORT_HEADERS="${HDR_NONE}" rc_of drive_one_export 127.0.0.1:1 "${FIXTURE}")"
check "drive_one_export export failure is nonzero" "7" \
  "$(CURL_MODE=unreachable rc_of drive_one_export 127.0.0.1:1 "${FIXTURE}")"

# ---------------------------------------------------------------------------
# Read-your-write query.
# ---------------------------------------------------------------------------

BODY_HIT='{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"demo_requests_total","job":"demo"},"value":[1695800000,"7"]}]}}'
BODY_EMPTY='{"status":"success","data":{"resultType":"vector","result":[]}}'
BODY_ERR='{"status":"error","errorType":"bad_data","error":"invalid min_commit_token: \"demo_requests_total\""}'

check "query body with the series is visible" "0" \
  "$(rc_of query_body_shows_series "${BODY_HIT}" demo_requests_total)"
check "empty result is not visible" "1" \
  "$(rc_of query_body_shows_series "${BODY_EMPTY}" demo_requests_total)"
check "error body naming the series is not visible" "1" \
  "$(rc_of query_body_shows_series "${BODY_ERR}" demo_requests_total)"
check "other series is not visible" "1" \
  "$(rc_of query_body_shows_series "${BODY_HIT}" demo_requests)"

CURL_MODE=query QUERY_BODY="${BODY_HIT}" query_series_visible 127.0.0.1:1 demo_requests_total "${TOKEN_A}" >/dev/null 2>&1
check "query passes the token verbatim as min_commit_token" "1" \
  "$(grep -cxF "min_commit_token=${TOKEN_A}" "${CURL_ARGS_FILE}")"

check "oracle passes on base64 tokens that are visible" "0" \
  "$(CURL_MODE=query QUERY_BODY="${BODY_HIT}" rc_of oracle_strict_ack_implies_durable \
      127.0.0.1:1 demo_requests_total "${TOKEN_A}" "${TOKEN_B}")"
check "oracle fails when a token's write is not visible" "1" \
  "$(CURL_MODE=query QUERY_BODY="${BODY_EMPTY}" rc_of oracle_strict_ack_implies_durable \
      127.0.0.1:1 demo_requests_total "${TOKEN_A}")"
check "oracle fails with no acked tokens" "1" \
  "$(rc_of oracle_strict_ack_implies_durable 127.0.0.1:1 demo_requests_total)"

# ---------------------------------------------------------------------------
# Exit-code contract the nightly workflow relies on.
# ---------------------------------------------------------------------------

summary_rc() {
  ORACLE_PASS=("$1")
  ORACLE_FAIL=()
  [[ -n "$2" ]] && ORACLE_FAIL=("$2")
  rc_of print_oracle_summary scenario "$3"
}
check "summary: all held is 0" "0" "$(summary_rc a "" blocking)"
check "summary: ordinary failure is 1" "1" "$(summary_rc a "b: detail" normal)"
check "summary: release-blocking failure is 2" "2" "$(summary_rc a "b: detail" blocking)"

# ---------------------------------------------------------------------------
# The scenarios against the fixture and each other.
# ---------------------------------------------------------------------------

fixture_series="$(sed -n 's/^ *name: "\([a-z_]*\)"\.to_string(),$/\1/p' \
  "${REPO_ROOT}/services/ravel-server/examples/gen_otlp_fixture.rs")"
scenario1_series="$(sed -n 's/^SERIES="\(.*\)"$/\1/p' "${CHAOS_DIR}/kill-ingest-flush.sh")"
check "fixture emits exactly one metric" "1" "$(printf '%s\n' "${fixture_series}" | grep -c .)"
check "scenario 1 queries the series the fixture emits" "${fixture_series}" "${scenario1_series}"

scenario2_body="$(cat "${CHAOS_DIR}/kill-maintain-worker.sh")"
check "scenario 2 POSTs the generated fixture to its ingest server" "yes" \
  "$([[ "${scenario2_body}" == *'drive_one_export "$INGEST_HTTP" "$FIXTURE_PATH"'* ]] && echo yes || echo no)"

check "scenario 1 --check runs with no store" "0" \
  "$(rc_of bash "${CHAOS_DIR}/kill-ingest-flush.sh" --check)"
check "scenario 2 --check runs with no store" "0" \
  "$(rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check "unknown argument is usage error 64" "64" \
  "$(rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --bogus)"

printf '\n%d passed, %d failed\n' "${PASSED}" "${FAILED}"
[[ "${FAILED}" -eq 0 ]]
