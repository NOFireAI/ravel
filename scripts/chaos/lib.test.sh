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
# The cases below pin the default; an operator's exported value must not flip them.
unset CHAOS_KEEP_LOGS
# chaos_aws/rustfs_up cases below pin the no-host-binary and host-binary paths
# explicitly; an operator's exported CHAOS_AWS must not flip which path runs.
unset CHAOS_AWS
# The conservation oracle waits up to the takeover bound for a survivor
# publish; every case reads the log once unless it sets its own wait.
CHAOS_CONSERVATION_WAIT_SECONDS=0
CHAOS_CONSERVATION_POLL_SECONDS=0.1

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
  printf '%s' "$1" | base64 | tr -d '\n' | tr '+/' '-_' | tr -d '='
}
# Inverse of b64url: restore the standard alphabet and the padding the encoder
# strips, since strict decoders (macOS) truncate an unpadded last group.
b64url_decode() {
  local s
  s="$(printf '%s' "$1" | tr -- '-_' '+/')"
  while [ $(( ${#s} % 4 )) -ne 0 ]; do s="${s}="; done
  printf '%s' "$s" | base64 --decode
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

# Scenario 2's symmetric trigger scrapes two URLs (worker A's and worker B's)
# per round; serve-metrics-ab picks a body by a "stub-a"/"stub-b" substring
# in the last argument (the scraped URL) so A and B can answer differently.
# wait_for_compaction_in_flight_any reads each round's body via
# body="$(curl ...)" -- a command substitution, which forks a subshell per
# call. A round index kept in a shell variable would be reset on every call
# instead of advancing, since the mutation never survives past that subshell.
# The index is kept in a file under SCRATCH instead, which both persists
# across the subshell boundary and lets the two workers' sequences step
# independently. ab_body takes a fixed "a"/"b" selector rather than an array
# name passed through a `local -n` nameref: nameref requires bash 4.3, which
# macOS's bash 3.2 does not have, and this file otherwise only needs 3.2.
AB_INFLIGHT_A=(0); AB_UNITS_A=(0)
AB_INFLIGHT_B=(0); AB_UNITS_B=(0)
AB_IDX_FILE_A="${SCRATCH}/ab-idx-a"
AB_IDX_FILE_B="${SCRATCH}/ab-idx-b"
AB_ORDER_FILE="${SCRATCH}/ab-order"
ab_body() { # which(a|b) idx_file
  local which="$1" idx_file="$2"
  local i infl units len
  i="$(cat "${idx_file}" 2>/dev/null)"
  [[ "${i}" =~ ^[0-9]+$ ]] || i=0
  # A units sequence shorter than its in-flight sequence holds its last entry.
  if [[ "${which}" == "a" ]]; then
    infl="${AB_INFLIGHT_A[i]}"; len="${#AB_INFLIGHT_A[@]}"
    units="${AB_UNITS_A[i]:-${AB_UNITS_A[${#AB_UNITS_A[@]}-1]}}"
  else
    infl="${AB_INFLIGHT_B[i]}"; len="${#AB_INFLIGHT_B[@]}"
    units="${AB_UNITS_B[i]:-${AB_UNITS_B[${#AB_UNITS_B[@]}-1]}}"
  fi
  printf '# TYPE ravel_maintain_compactions_in_flight gauge\nravel_maintain_compactions_in_flight{mode="maintain"} %s\n# TYPE ravel_maintain_units_owned gauge\nravel_maintain_units_owned{mode="maintain"} %s\n' \
    "${infl}" "${units}"
  if (( i < len - 1 )); then
    printf '%s' "$(( i + 1 ))" >"${idx_file}"
  fi
}

CURL_MODE="serve-metrics"
CURL_ARGS_FILE="${SCRATCH}/curl-args"
curl() {
  printf '%s\n' "$@" >"${CURL_ARGS_FILE}"
  case "${CURL_MODE}" in
    serve-metrics) printf '%s\n' "${METRICS_SAMPLE}" ;;
    serve-metrics-ab)
      printf '%s\n' "${@: -1}" >>"${AB_ORDER_FILE}"
      case "${@: -1}" in
        *stub-a*) ab_body a "${AB_IDX_FILE_A}" ;;
        *stub-b*) ab_body b "${AB_IDX_FILE_B}" ;;
      esac
      ;;
    unreachable) return 7 ;;
    slow-metrics) sleep 2; printf '%s\n' "${METRICS_SAMPLE}" ;;
    metrics-file) cat "${METRICS_FILE}" ;;
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
# The load is metrics-only, so only the metrics pipeline's flushes count, and
# every trigger's family counts: size 5 + age 9 = 14 attempts. Baseline 13 is
# a rise, baseline 14 is not, even though the logs pipeline holds 6 more and
# the unselected total is 20.
check "flush attempts sum every trigger family on the metrics signal" "14" \
  "$(flush_attempts_from_body "${METRICS_SAMPLE}")"
check "flush trigger fires on a metrics-signal rise" "0" \
  "$(rc_of wait_for_flush_started http://stub 13 2)"
check "flush trigger ignores other signals' flushes" "1" \
  "$(rc_of wait_for_flush_started http://stub 14 1)"

# A run where the only flush was time-triggered: the size family stays at 0,
# which is what left the size-only trigger waiting out its whole budget.
METRICS_TIME_ONLY='# TYPE ravel_ingest_flushes_by_size_total counter
ravel_ingest_flushes_by_size_total{mode="all",signal="metrics"} 0
ravel_ingest_flushes_by_size_total{mode="all",signal="logs"} 0
# TYPE ravel_ingest_flushes_by_age_total counter
ravel_ingest_flushes_by_age_total{mode="all",signal="metrics"} 1
ravel_ingest_flushes_by_age_total{mode="all",signal="logs"} 0
# TYPE ravel_ingest_flushes_by_age_floor_total counter
ravel_ingest_flushes_by_age_floor_total{mode="all",signal="metrics"} 0
# TYPE ravel_ingest_flushes_manual_total counter
ravel_ingest_flushes_manual_total{mode="all",signal="metrics"} 0'
check "time-triggered flush: all-flushes figure is 1" "1" \
  "$(flush_attempts_from_body "${METRICS_TIME_ONLY}")"
check "time-triggered flush: the trigger fires from baseline 0" "0" \
  "$(METRICS_SAMPLE="${METRICS_TIME_ONLY}" rc_of wait_for_flush_started http://stub 0 1)"
check "time-triggered flush: no rise past baseline 1" "1" \
  "$(METRICS_SAMPLE="${METRICS_TIME_ONLY}" rc_of wait_for_flush_started http://stub 1 1)"
METRICS_ADAPTIVE_MANUAL='ravel_ingest_flushes_by_size_total{mode="all",signal="metrics"} 0
ravel_ingest_flushes_by_age_total{mode="all",signal="metrics"} 0
ravel_ingest_flushes_by_age_adaptive_total{mode="all",signal="metrics"} 2
ravel_ingest_flushes_by_age_floor_total{mode="all",signal="metrics"} 4
ravel_ingest_flushes_manual_total{mode="all",signal="metrics"} 1'
check "adaptive, floor and manual flushes count too" "7" \
  "$(flush_attempts_from_body "${METRICS_ADAPTIVE_MANUAL}")"
check "no flush family at all reads as absent" "" \
  "$(flush_attempts_from_body 'ravel_build_up 1')"

check "kill timing: flush seen, export unacked is mid-flush=yes" \
  "KILL-TIMING: mid-flush=yes (flush attempts 3 -> 4, in-flight export unacknowledged at the kill)" \
  "$(kill_timing_line 1 0 3 4)"
check "kill timing: flush seen but export acked is mid-flush=no" "KILL-TIMING: mid-flush=no" \
  "$(kill_timing_line 1 1 3 4 | cut -d' ' -f1-2)"
check "kill timing: no flush seen is mid-flush=no" "KILL-TIMING: mid-flush=no" \
  "$(kill_timing_line 0 0 3 3 | cut -d' ' -f1-2)"
timing_for_rc() { kill_timing_line 1 "$(chaos_inflight_answered "$1")" 3 4 | cut -d' ' -f1-2; }
check "kill timing: curl exit 22 (HTTP error answered) is mid-flush=no" "KILL-TIMING: mid-flush=no" \
  "$(timing_for_rc 22)"
check "kill timing: curl exit 52 (empty reply) is mid-flush=yes" "KILL-TIMING: mid-flush=yes" \
  "$(timing_for_rc 52)"
check "kill timing: curl exit 56 (connection reset) is mid-flush=yes" "KILL-TIMING: mid-flush=yes" \
  "$(timing_for_rc 56)"
check "kill timing: curl exit 0 (acknowledged) is mid-flush=no" "KILL-TIMING: mid-flush=no" \
  "$(timing_for_rc 0)"
check "kill timing: no commit token (exit 1, answered) is mid-flush=no" "KILL-TIMING: mid-flush=no" \
  "$(timing_for_rc 1)"
check "unanswered curl codes" "7 18 28 52 55 56" "${CHAOS_CURL_UNANSWERED_CODES[*]}"
scenario1_line() { grep -n -m1 -F -- "$1" "${CHAOS_DIR}/kill-ingest-flush.sh" | cut -d: -f1; }
detect_line="$(scenario1_line 'if wait_for_flush_started')"
first_kill_line="$(scenario1_line 'sigkill_pid "$SERVER_PID"')"
# Between the detecting poll and the kill: nothing but comments, blank lines
# and the flag or epoch the branch records.
only_bookkeeping_between() { # file from to
  awk -v a="$2" -v b="$3" 'NR > a && NR < b && $0 !~ /^[[:space:]]*(#.*)?$/ && $0 !~ /^[[:space:]]*(FLUSH_OBSERVED=1|KILL_EPOCH="\$\(date \+%s\)"|VICTIM_PID_VAR="WORKER_\$\{CHAOS_VICTIM\^\^\}_PID")[[:space:]]*$/ { n++ } END { print n + 0 }' "$1"
}
check "scenario 1 sends the SIGKILL with nothing but bookkeeping after the detecting poll" "0" \
  "$([[ -n "${detect_line}" && "${first_kill_line:-0}" -gt "${detect_line}" ]] && only_bookkeeping_between "${CHAOS_DIR}/kill-ingest-flush.sh" "${detect_line}" "${first_kill_line}" || echo missing)"
check "scenario 1 scrapes no flush count after the detection" "0" \
  "$(awk -v d="${detect_line:-0}" 'NR > d && index($0, "flush_attempts \"$BASE_URL\"") { n++ } END { print n + 0 }' \
    "${CHAOS_DIR}/kill-ingest-flush.sh")"
scenario1_body="$(cat "${CHAOS_DIR}/kill-ingest-flush.sh")"
check "scenario 1 prints the kill-timing line on stdout" "yes" \
  "$([[ "${scenario1_body}" == *$'\necho "$KILL_TIMING"\n'* ]] && echo yes || echo no)"

# ---------------------------------------------------------------------------
# Seal wait (scenario 2).
# ---------------------------------------------------------------------------

# 2026-10-10T03:10:00Z, 03:59:59Z and 04:00:00Z as unix seconds; the seal is
# the end of the hour + 4800 s margin + 120 s safety.
ACK_0310=1791601800
ACK_0359=1791604799
ACK_0400=1791604800
SEALED_0522=1791609720  # 2026-10-10T05:22:00Z = 04:00 + 4800 + 120
SEALED_0622=1791613320  # 2026-10-10T06:22:00Z = 05:00 + 4800 + 120
check "sealed_at: last ack 03:10:00 seals at 05:22:00" "${SEALED_0522}" \
  "$(chaos_sealed_at_unix_s "${ACK_0310}" 4800 120)"
check "sealed_at: last ack 03:59:59 seals at 05:22:00" "${SEALED_0522}" \
  "$(chaos_sealed_at_unix_s "${ACK_0359}" 4800 120)"
check "sealed_at: last ack 04:00:00 seals an hour later, 06:22:00" "${SEALED_0622}" \
  "$(chaos_sealed_at_unix_s "${ACK_0400}" 4800 120)"
check "sealed_at: safety defaults to 120 s" "${SEALED_0522}" \
  "$(chaos_sealed_at_unix_s "${ACK_0310}" 4800)"
check "sealed_at: a non-integer margin is refused with 64" "64" \
  "$(rc_of chaos_sealed_at_unix_s "${ACK_0310}" 48x0)"

# Modelled on tracing's default fmt output for log_resolved_request_budget
# (services/ravel-server/src/lib.rs), plain and with ANSI field colouring.
SEAL_LOG_LINE='2026-10-10T03:00:01.123456Z  INFO ravel_server: per-query S3 request budget resolved max_s3_requests=4096 source="derived" covered_span_secs=8400 seal_margin_secs=4800'
SEAL_LOG_ANSI=$'2026-10-10T03:00:01.123456Z \e[32m INFO\e[0m \e[2mravel_server\e[0m\e[2m:\e[0m per-query S3 request budget resolved \e[3mmax_s3_requests\e[0m\e[2m=\e[0m4096 \e[3mseal_margin_secs\e[0m\e[2m=\e[0m4800'
SEAL_LOG="2026-10-10T03:00:00.000001Z  INFO ravel_server: starting
${SEAL_LOG_LINE}
2026-10-10T03:00:02.000001Z  INFO ravel_server: listening"
check "seal margin read from the server's log line" "4800" \
  "$(chaos_seal_margin_from_log "${SEAL_LOG}")"
check "seal margin read through ANSI colouring" "4800" \
  "$(chaos_seal_margin_from_log "${SEAL_LOG_ANSI}")"
check "seal margin: a log without the field is refused" "1" \
  "$(rc_of chaos_seal_margin_from_log '2026-10-10T03:00:00Z  INFO ravel_server: starting')"
check "seal margin: a non-integer value is refused" "1" \
  "$(rc_of chaos_seal_margin_from_log "${SEAL_LOG_LINE/seal_margin_secs=4800/seal_margin_secs=48.5}")"
check "seal margin: an empty value is refused" "1" \
  "$(rc_of chaos_seal_margin_from_log "${SEAL_LOG_LINE/seal_margin_secs=4800/seal_margin_secs=}")"

# The wait loop against a fake clock: 1500 s to go sleeps 600, 600, 300, and
# logs the target once and a progress line after each sleep that leaves time.
seal_wait_trace() {
  local clock="${SCRATCH}/clock" sleeps="${SCRATCH}/sleeps"
  printf '%s\n' 1000 >"${clock}"
  : >"${sleeps}"
  chaos_now_unix_s() { cat "${clock}"; }
  chaos_sleep() {
    printf '%s ' "$1" >>"${sleeps}"
    printf '%s\n' $(( $(cat "${clock}") + $1 )) >"${clock}"
  }
  local out
  out="$(chaos_wait_until_sealed 2500 2>&1)"
  printf 'sleeps=%s targets=%s progress=%s\n' "$(cat "${sleeps}")" \
    "$(grep -c 'sealed_at=2500 (' <<<"${out}")" "$(grep -c 's left until' <<<"${out}")"
}
check "seal wait: one target line, a progress line per 10 min" \
  "sleeps=600 600 300  targets=1 progress=2" "$(seal_wait_trace)"

scenario2_line() { grep -n -m1 -F -- "$1" "${CHAOS_DIR}/kill-maintain-worker.sh" | cut -d: -f1; }
stop_line="$(scenario2_line 'kill "$INGEST_PID"')"
margin_line="$(scenario2_line 'chaos_seal_margin_from_log "$(cat "$INGEST_LOG")"')"
wait_line="$(scenario2_line 'chaos_wait_until_sealed "$SEALED_AT"')"
workers_line="$(scenario2_line 'start_worker "$WORKER_A_HTTP"')"
check "scenario 2 stops ingest, reads the margin, waits, then starts workers" "yes" \
  "$([[ -n "${stop_line}" && "${stop_line}" -lt "${margin_line:-0}" && "${margin_line}" -lt "${wait_line:-0}" && "${wait_line}" -lt "${workers_line:-0}" ]] && echo yes || echo no)"

# The seal wait's input is the time the ingest server has exited, not the last
# successful ack: a write answered 503 Abandoned, or the shutdown flush, can
# land after that ack. A last ack at 03:59:50 and an exit at 04:00:05 seal an
# hour later than the ack alone would.
ACK_035950=1791604790
EXIT_040005=1791604805
check "seal input: last ack 03:59:50 alone would seal at 05:22:00" "${SEALED_0522}" \
  "$(chaos_sealed_at_unix_s "${ACK_035950}" 4800 120)"
check "seal input: exit 04:00:05 seals at 06:22:00" "${SEALED_0622}" \
  "$(chaos_sealed_at_unix_s "${EXIT_040005}" 4800 120)"
reaped_line="$(scenario2_line 'wait "$INGEST_PID"')"
exit_stamp_line="$(scenario2_line 'INGEST_EXIT_UNIX_S="$(chaos_now_unix_s)"')"
sealed_from_exit_line="$(scenario2_line 'SEALED_AT="$(chaos_sealed_at_unix_s "$INGEST_EXIT_UNIX_S"')"
check "scenario 2 stamps the ingest exit after reaping it and seals from that stamp" "yes" \
  "$([[ -n "${reaped_line}" && "${reaped_line}" -lt "${exit_stamp_line:-0}" && "${exit_stamp_line}" -lt "${sealed_from_exit_line:-0}" ]] && echo yes || echo no)"
check "scenario 2 never seals from the last ack" "0" \
  "$(grep -c -F 'chaos_sealed_at_unix_s "$LAST_ACK_UNIX_S"' "${CHAOS_DIR}/kill-maintain-worker.sh")"

# ---------------------------------------------------------------------------
# Scenario 2's conservation verdict, with the could-not-measure guard. The
# stubbed /metrics serves conservation aborts 1: a baseline of 1 is no rise,
# a baseline of 0 is a rise.
# ---------------------------------------------------------------------------

# Modelled on tracing's default fmt output for MergeInFlight::start and its
# Drop in crates/ravel-maintain/src/compact.rs, the publish line in
# publish.rs, and the pass line in services/ravel-server/src/maintain.rs. The
# bucket= / key= relation and the end line's outcomes are pinned on the Rust
# side by crates/ravel-maintain/tests/compactions_in_flight_gauge.rs.
BUCKET_X='t/0123456789abcdef/m/c/0001/2026101004/'
BUCKET_Y='t/0123456789abcdef/m/c/0002/2026101004/'
BUCKET_Z='t/0123456789abcdef/m/c/0003/2026101004/'
start_line() { printf '2026-10-10T05:30:00Z  INFO ravel_maintain::compact: compaction merge started bucket=%s\n' "$1"; }
pub_line() { printf '2026-10-10T05:30:01Z  INFO ravel_maintain::publish: compaction record published key=%sl1.00112233445566ff.cmt parts=1\n' "$1"; }
# Args: bucket [outcome, default published].
end_line() { printf '2026-10-10T05:30:01Z  INFO ravel_maintain::compact: compaction merge ended bucket=%s outcome=%s\n' "$1" "${2:-published}"; }
PUB_LINE="$(pub_line "${BUCKET_X}")"
OTHER_LINE='2026-10-10T05:29:00Z  INFO ravel_server: other work'
pass_line() { printf '2026-10-10T05:30:0%sZ  INFO ravel_server::maintain: maintenance: retention + compaction pass complete tenant=ab signal=Metrics shard=%s retired=0 compacted=%s already_done=0 not_sealed=0\n' "$1" "$1" "$2"; }
# A published X, then started Y and was killed merging it.
LOG_A_MERGING="${SCRATCH}/a-merging.log"
{ start_line "${BUCKET_X}"; pub_line "${BUCKET_X}"; end_line "${BUCKET_X}"; start_line "${BUCKET_Y}"; } >"${LOG_A_MERGING}"
# A was killed in its first merge, Y, having published nothing.
LOG_A_FIRST="${SCRATCH}/a-first.log"
{ printf '%s\n' "${OTHER_LINE}"; start_line "${BUCKET_Y}"; } >"${LOG_A_FIRST}"
# A ended every merge it started.
LOG_A_ALL_DONE="${SCRATCH}/a-all-done.log"
{ start_line "${BUCKET_X}"; pub_line "${BUCKET_X}"; end_line "${BUCKET_X}"; pass_line 0 1; } >"${LOG_A_ALL_DONE}"
# A started no merge at all.
LOG_A_NO_START="${SCRATCH}/a-no-start.log"
{ printf '%s\n' "${OTHER_LINE}"; pass_line 0 0; } >"${LOG_A_NO_START}"
# B's logs carry one line before the kill (line count 1), then: nothing; a
# publish of Z, its own bucket; a publish of Y, A's interrupted bucket.
LOG_B_SILENT="${SCRATCH}/b-silent.log"
printf '%s\n' "${OTHER_LINE}" >"${LOG_B_SILENT}"
LOG_B_OTHER="${SCRATCH}/b-other.log"
{ printf '%s\n' "${OTHER_LINE}"; pub_line "${BUCKET_Z}"; } >"${LOG_B_OTHER}"
LOG_B_SAME="${SCRATCH}/b-same.log"
{ printf '%s\n' "${OTHER_LINE}"; pub_line "${BUCKET_Y}"; } >"${LOG_B_SAME}"
# B published Y only before the kill.
LOG_B_PRE_KILL="${SCRATCH}/b-pre-kill.log"
{ pub_line "${BUCKET_Y}"; printf '%s\n' "${OTHER_LINE}"; } >"${LOG_B_PRE_KILL}"
# B started Y after the kill and has not published it.
LOG_B_STARTED="${SCRATCH}/b-started.log"
{ printf '%s\n' "${OTHER_LINE}"; start_line "${BUCKET_Y}"; } >"${LOG_B_STARTED}"

interrupted() { local out rc=0; out="$(chaos_interrupted_buckets "$@")" || rc=$?; printf 'rc=%s %s\n' "${rc}" "$(printf '%s' "${out}" | tr '\n' ' ')"; }
LOG_A_TWO_OPEN="${SCRATCH}/a-two-open.log"
{ start_line "${BUCKET_X}"; start_line "${BUCKET_Y}"; } >"${LOG_A_TWO_OPEN}"
check "interrupted: two merges open at the kill are both printed, in start order" \
  "rc=0 ${BUCKET_X} ${BUCKET_Y}" "$(interrupted "${LOG_A_TWO_OPEN}")"
check "interrupted: a bucket open twice is printed once" "rc=0 ${BUCKET_X}" \
  "$(interrupted <({ start_line "${BUCKET_X}"; start_line "${BUCKET_X}"; }))"
check "interrupted: the last start with no end line after it" "rc=0 ${BUCKET_Y}" \
  "$(interrupted "${LOG_A_MERGING}")"
check "interrupted: a first merge with no end line at all" "rc=0 ${BUCKET_Y}" \
  "$(interrupted "${LOG_A_FIRST}")"
check "interrupted: every start ended is none" "rc=1 " "$(interrupted "${LOG_A_ALL_DONE}")"
check "interrupted: no start line is none" "rc=1 " "$(interrupted "${LOG_A_NO_START}")"
check "interrupted: a missing log is none" "rc=1 " "$(interrupted "${SCRATCH}/no-such.log")"
LOG_A_CONCURRENT="${SCRATCH}/a-concurrent.log"
{ start_line "${BUCKET_X}"; start_line "${BUCKET_Y}"; pub_line "${BUCKET_Y}"; end_line "${BUCKET_Y}"; } >"${LOG_A_CONCURRENT}"
check "interrupted: concurrent merges, the later one ended, the earlier is interrupted" \
  "rc=0 ${BUCKET_X}" "$(interrupted "${LOG_A_CONCURRENT}")"
LOG_A_RESTART="${SCRATCH}/a-restart.log"
{ start_line "${BUCKET_X}"; pub_line "${BUCKET_X}"; end_line "${BUCKET_X}"; start_line "${BUCKET_X}"; } >"${LOG_A_RESTART}"
check "interrupted: an end before a later start of the same bucket does not end it" \
  "rc=0 ${BUCKET_X}" "$(interrupted "${LOG_A_RESTART}")"
# A merge of X that started after the one of Y and ended without a publish
# must not shadow Y, which was still open at the kill.
LOG_A_LATER_FAILED="${SCRATCH}/a-later-failed.log"
{ start_line "${BUCKET_Y}"; start_line "${BUCKET_X}"; end_line "${BUCKET_X}" failed; } >"${LOG_A_LATER_FAILED}"
check "interrupted: a later merge that ended failed does not shadow an earlier unended one" \
  "rc=0 ${BUCKET_Y}" "$(interrupted "${LOG_A_LATER_FAILED}")"
for outcome in published converged abandoned skipped held failed cancelled dry-run dropped; do
  check "interrupted: an end line with outcome=${outcome} ends its merge" "rc=1 " \
    "$(interrupted <({ start_line "${BUCKET_X}"; end_line "${BUCKET_X}" "${outcome}"; }))"
done
check "interrupted: a publish with no end line leaves the merge open (killed after the publish)" \
  "rc=0 ${BUCKET_X}" "$(interrupted <({ start_line "${BUCKET_X}"; pub_line "${BUCKET_X}"; }))"
check "interrupted: another bucket's end line does not end it" "rc=0 ${BUCKET_X}" \
  "$(interrupted <({ start_line "${BUCKET_X}"; end_line "${BUCKET_Y}"; }))"
check "interrupted: one end closes one of two starts of the same bucket" "rc=0 ${BUCKET_X}" \
  "$(interrupted <({ start_line "${BUCKET_X}"; start_line "${BUCKET_X}"; end_line "${BUCKET_X}"; }))"
LOG_A_ANSI="${SCRATCH}/a-ansi.log"
printf '\e[32m INFO\e[0m \e[2mravel_maintain::compact\e[0m\e[2m:\e[0m compaction merge started \e[3mbucket\e[0m\e[2m=\e[0m%s\n' \
  "${BUCKET_Y}" >"${LOG_A_ANSI}"
check "interrupted: a start line read through ANSI colouring" "rc=0 ${BUCKET_Y}" \
  "$(interrupted "${LOG_A_ANSI}")"
published() { rc_of chaos_published_bucket_since "$@"; }
check "published: a publish of the bucket past the line count" "0" \
  "$(published "${LOG_B_SAME}" 1 "${BUCKET_Y}")"
check "published: a publish of another bucket does not count" "1" \
  "$(published "${LOG_B_OTHER}" 1 "${BUCKET_Y}")"
check "published: a publish before the line count does not count" "1" \
  "$(published "${LOG_B_PRE_KILL}" 1 "${BUCKET_Y}")"
check "published: a prefix of a longer bucket does not count" "1" \
  "$(published "${LOG_B_SAME}" 1 't/0123456789abcdef/m/c/')"
check "published: an empty bucket never counts" "1" "$(published "${LOG_B_SAME}" 0 "")"
started() { rc_of chaos_started_bucket_since "$@"; }
check "started: a start of the bucket past the line count" "0" \
  "$(started "${LOG_B_STARTED}" 1 "${BUCKET_Y}")"
check "started: a start before the line count does not count" "1" \
  "$(started "${LOG_B_STARTED}" 2 "${BUCKET_Y}")"
check "started: a start of another bucket does not count" "1" \
  "$(started "${LOG_B_STARTED}" 1 "${BUCKET_X}")"
check "started: a publish is not a start" "1" "$(started "${LOG_B_SAME}" 1 "${BUCKET_Y}")"

# Merge durations, paired per log, from tracing's UTC timestamps.
LOG_DUR_A="${SCRATCH}/dur-a.log"
{
  printf '2026-10-10T05:30:00.000000Z  INFO x: compaction merge started bucket=%s\n' "${BUCKET_X}"
  printf '2026-10-10T05:30:01Z  INFO x: compaction merge started bucket=%s\n' "${BUCKET_Y}"
  printf '2026-10-10T05:30:02.500000Z  INFO x: compaction merge ended bucket=%s outcome=published\n' "${BUCKET_X}"
  printf '2026-10-10T05:30:11Z  INFO x: compaction merge ended bucket=%s outcome=failed\n' "${BUCKET_Y}"
  printf '2026-10-10T05:31:00Z  INFO x: compaction merge started bucket=%s\n' "${BUCKET_Z}"
} >"${LOG_DUR_A}"
LOG_DUR_B="${SCRATCH}/dur-b.log"
{
  printf '2026-10-31T23:59:59Z  INFO x: compaction merge started bucket=%s\n' "${BUCKET_Z}"
  printf '2026-11-01T00:00:03Z  INFO x: compaction merge ended bucket=%s outcome=published\n' "${BUCKET_Z}"
} >"${LOG_DUR_B}"
check "durations: each log paired on its own, an unended start left out" \
  "2.500 10.000 4.000" "$(chaos_merge_durations "${LOG_DUR_A}" "${LOG_DUR_B}" | tr '\n' ' ' | sed 's/ $//')"
check "durations: the summary line, nearest-rank p50" "merge durations: n=3 p50=4.000 max=10.000" \
  "$(chaos_merge_durations_line "${LOG_DUR_A}" "${LOG_DUR_B}")"
check "durations: nothing paired" "merge durations: n=0 p50=none max=none" \
  "$(chaos_merge_durations_line "${LOG_A_FIRST}" "${SCRATCH}/no-such.log")"
check "durations: a line with no timestamp pairs nothing" "merge durations: n=0 p50=none max=none" \
  "$(chaos_merge_durations_line <(printf 'INFO compaction merge started bucket=a/\nINFO compaction merge ended bucket=a/ outcome=failed\n'))"

# The store LIST behind the conservation verdict goes through chaos_aws,
# replaced here like curl. AWS_MODE: absent (no key under the prefix, which
# the AWS CLI prints as None), present (a record and a non-record key under
# it), error (the CLI's exit 255), or keys (AWS_KEYS verbatim, None when
# empty).
AWS_ARGS_FILE="${SCRATCH}/aws-args"
AWS_MODE="absent"
chaos_aws() {
  printf '%s\n' "$@" >"${AWS_ARGS_FILE}"
  local prefix="" prev="" arg
  for arg in "$@"; do
    [[ "${prev}" == "--prefix" ]] && prefix="${arg}"
    prev="${arg}"
  done
  case "${AWS_MODE}" in
    absent) printf 'None\n' ;;
    present) printf '%s00112233445566ff.cmt\t%s00112233445566ff.tmp\n' "${prefix}" "${prefix}" ;;
    error) return 255 ;;
    keys) printf '%s\n' "${AWS_KEYS:-None}" ;;
    # A record only under AWS_ONLY_BUCKET; the LIST of any other bucket is
    # empty (or fails, with AWS_OTHERS=error).
    only)
      if [[ "${prefix}" == "${AWS_ONLY_BUCKET}l1." ]]; then
        printf '%s00112233445566ff.cmt\n' "${prefix}"
      elif [[ "${AWS_OTHERS:-absent}" == error ]]; then
        return 255
      else
        printf 'None\n'
      fi
      ;;
  esac
}
records() { local out rc=0; out="$(chaos_compaction_records_under "$@")" || rc=$?; printf 'rc=%s %s\n' "${rc}" "${out}"; }
check "records: a .cmt key under the prefix counts, another key does not" "rc=0 1" \
  "$(AWS_MODE=present records "${BUCKET_Y}")"
check "records: an empty LIST is 0" "rc=0 0" "$(AWS_MODE=absent records "${BUCKET_Y}")"
check "records: a LIST that fails returns the CLI's code and prints nothing" "rc=255 " \
  "$(AWS_MODE=error records "${BUCKET_Y}")"
AWS_MODE=absent records "${BUCKET_Y}" >/dev/null
check "records: the LIST is of <bucket>l1. in the chaos bucket" "yes" \
  "$(grep -qxF -- "${BUCKET_Y}l1." "${AWS_ARGS_FILE}" && grep -qxF -- "${RAVEL_S3_BUCKET}" "${AWS_ARGS_FILE}" && grep -qxF list-objects-v2 "${AWS_ARGS_FILE}" && echo yes || echo no)"

# The scenario-2 trigger scrapes BOTH workers every round and SIGKILLs the
# PREFERRED worker as soon as its own
# ravel_maintain_compactions_in_flight>=1; the other worker is never the
# victim, and a budget with no reading from the preferred worker ends in the
# no-gauge path (wait_for_compaction_in_flight_preferred). The old race -- whichever
# gauge fired first, with no preference -- picked worker A about 15 nights out
# of 16, since A starts its first merge 7-11s before B in every run since
# #3019 (issue #3037); a kill that only ever watched one worker is the
# earlier, more severe form of the same bug (nightly run 38674293885, issue
# #2666), where the unwatched worker owned every data-bearing unit. ab_reset
# loads a sequence per worker; wait_for_compaction_in_flight_preferred steps
# one entry per scrape, holding the last entry once a sequence runs out.
ab_reset() { # infl_a units_a infl_b units_b -- each a space-separated sequence
  read -r -a AB_INFLIGHT_A <<<"$1"
  read -r -a AB_UNITS_A <<<"$2"
  read -r -a AB_INFLIGHT_B <<<"$3"
  read -r -a AB_UNITS_B <<<"$4"
  printf '0' >"${AB_IDX_FILE_A}"
  printf '0' >"${AB_IDX_FILE_B}"
  : >"${AB_ORDER_FILE}"
}
ab_run() { CURL_MODE=serve-metrics-ab CHAOS_COMPACTION_POLL_SECONDS=0.01 wait_for_compaction_in_flight_preferred "$1" http://stub-a http://stub-b "${@:2}" >/dev/null 2>&1; }
ab_wait() { CURL_MODE=serve-metrics-ab CHAOS_COMPACTION_POLL_SECONDS=0.01 rc_of wait_for_compaction_in_flight_preferred "$1" http://stub-a http://stub-b "${@:2}"; }

check "preferred trigger: preferred=a, A fires first, returns 0" "0" \
  "$(ab_reset "1" "2" "0" "3"; ab_wait a 1)"
check "preferred trigger: preferred=a, A fires first, picks victim a, reason preferred" "a preferred" \
  "$(ab_reset "1" "2" "0" "3"; ab_run a 1; echo "${CHAOS_VICTIM} ${CHAOS_VICTIM_REASON}")"
check "preferred trigger: preferred=b, B fires first, picks victim b, reason preferred" "b preferred" \
  "$(ab_reset "0" "3" "1" "2"; ab_run b 1; echo "${CHAOS_VICTIM} ${CHAOS_VICTIM_REASON}")"

check "preferred trigger: neither fires within budget returns nonzero" "1" \
  "$(ab_reset "0" "1" "0" "4"; ab_wait a 0)"
check "preferred trigger: neither fires within budget leaves victim at preferred a, reason no-gauge" "a no-gauge" \
  "$(ab_reset "0" "1" "0" "4"; ab_run a 0; echo "${CHAOS_VICTIM} ${CHAOS_VICTIM_REASON}")"
check "preferred trigger: neither fires within budget leaves victim at preferred b, reason no-gauge" "b no-gauge" \
  "$(ab_reset "0" "1" "0" "4"; ab_run b 0; echo "${CHAOS_VICTIM} ${CHAOS_VICTIM_REASON}")"
check "preferred trigger: does not fire on an unreachable pair" "1" \
  "$(CURL_MODE=unreachable rc_of wait_for_compaction_in_flight_preferred a http://stub-a http://stub-b 0)"

# DISTINGUISHING(a): a mutant that restores the old "first gauge wins" race
# (no preference) would pick whichever worker's gauge fires chronologically
# first. Here B fires on round 0 and A (the preferred worker) only fires on
# round 2; a first-gauge-wins race reports victim b, but the real, preferred-
# respecting victim is a, reason preferred -- CHAOS_PREFERRED_VICTIM is never
# overridden by a sibling firing earlier.
check "DISTINGUISHING(a) preferred worker wins even when the sibling fired first chronologically" "a preferred" \
  "$(ab_reset "0 0 1" "2 2 2" "1 1 1" "5 5 5"; ab_run a 2; echo "${CHAOS_VICTIM} ${CHAOS_VICTIM_REASON}")"

check "preferred trigger: per-worker peak units_owned is kept separately for A" "2" \
  "$(ab_reset "0 1" "2 2" "0 0" "5 5"; ab_run a 2; echo "${CHAOS_A_MAX_UNITS_OWNED}")"
check "preferred trigger: per-worker peak units_owned is kept separately for B" "5" \
  "$(ab_reset "0 1" "2 2" "0 0" "5 5"; ab_run a 2; echo "${CHAOS_B_MAX_UNITS_OWNED}")"
check "preferred trigger: total scrape count sums both workers' polls in the one round before a zero-second deadline breaks" "2" \
  "$(ab_reset "0 0" "1 1" "0 0" "1 1"; ab_run a 0; echo "${CHAOS_COMPACTION_POLLS}")"

# Nit (#3037): the round in which the preferred worker's gauge fires still
# scrapes its sibling, before the preferred worker. A fires on round 0 and the
# loop returns that same round; B's distinctive units_owned value (7) can
# only land in CHAOS_B_MAX_UNITS_OWNED if B was scraped during that round.
check "preferred trigger: B is scraped in the very round A's gauge fires, not skipped (nit: scrape both before returning)" "7" \
  "$(ab_reset "1" "2" "0" "7"; ab_run a 1; echo "${CHAOS_B_MAX_UNITS_OWNED}")"
check "preferred trigger: the survivor's last units_owned reading before the return is kept" "6" \
  "$(ab_reset "0 1" "2 2" "0 0" "5 6"; ab_run a 2; echo "${CHAOS_B_LAST_UNITS_OWNED}")"

# Detection to kill (ADR-0077: nothing but bookkeeping between them): every
# round scrapes the sibling first and the preferred worker last, so the
# preferred worker's detecting read is the last scrape before the return and
# no HTTP scrape of the sibling sits between it and the kill.
ab_order() { local o; o="$(tr '\n' ' ' <"${AB_ORDER_FILE}")"; echo "${o% }"; }
check "detection order: preferred=a scrapes B then A each round and returns on A's detecting read" \
  "http://stub-b/metrics http://stub-a/metrics http://stub-b/metrics http://stub-a/metrics" \
  "$(ab_reset "0 1" "2 2" "0" "3"; ab_run a 5; ab_order)"
check "detection order: preferred=b scrapes A then B each round and returns on B's detecting read" \
  "http://stub-a/metrics http://stub-b/metrics http://stub-a/metrics http://stub-b/metrics" \
  "$(ab_reset "0" "3" "0 1" "2 2"; ab_run b 5; ab_order)"

# The preferred worker is killed on its first reading >= 1, and only the
# preferred worker is ever the victim: a budget that passes with no reading
# from it ends in the no-gauge path, whatever the other worker reads. The
# clock is replaced by one that advances one second per read, so round r of
# the wait reads elapsed r + 1 however fast the stub answers. Each line
# reports the elapsed second of the round that ended the wait (at).
ab_tick() { # preferred budget
  local rc=0
  printf '1000\n' >"${SCRATCH}/ab-clock"
  chaos_now_unix_s() {
    local t
    t="$(cat "${SCRATCH}/ab-clock")"
    printf '%s\n' "$(( t + 1 ))" >"${SCRATCH}/ab-clock"
    echo "${t}"
  }
  CURL_MODE=serve-metrics-ab CHAOS_COMPACTION_POLL_SECONDS=0 \
    wait_for_compaction_in_flight_preferred "$1" http://stub-a http://stub-b "$2" >/dev/null 2>&1 || rc=$?
  echo "rc=${rc} victim=${CHAOS_VICTIM} reason=${CHAOS_VICTIM_REASON} at=${CHAOS_WAIT_ELAPSED_S:-} polls=${CHAOS_COMPACTION_POLLS}"
}
ab_last_scrape() { tail -n 1 "${AB_ORDER_FILE}"; }
# The other worker reads 1 from the first round; the preferred worker first
# reads 1 on its third scrape, at 3 s, and is killed on that reading.
check "first live reading: preferred=a is killed on its first reading >= 1, the other worker's earlier readings ignored" \
  "rc=0 victim=a reason=preferred at=3 polls=6 last=http://stub-a/metrics" \
  "$(ab_reset "0 0 1" "1" "1" "4"; echo "$(ab_tick a 420) last=$(ab_last_scrape)")"
check "first live reading: preferred=b is killed on its first reading >= 1, the other worker's earlier readings ignored" \
  "rc=0 victim=b reason=preferred at=3 polls=6 last=http://stub-b/metrics" \
  "$(ab_reset "1" "4" "0 0 1" "1"; echo "$(ab_tick b 420) last=$(ab_last_scrape)")"
check "first live reading: both reading 1 in the first round kills the preferred worker" \
  "rc=0 victim=a reason=preferred at=1 polls=2 last=http://stub-a/metrics" \
  "$(ab_reset "1" "1" "1" "4"; echo "$(ab_tick a 420) last=$(ab_last_scrape)")"
# The other worker reads 1 throughout while the preferred worker never does:
# the wait runs to its 10 s budget and names the preferred worker, reason
# no-gauge. The other worker is never the victim.
check "no-gauge: preferred=a never reads 1 while b does; the budget expires with victim a" \
  "rc=1 victim=a reason=no-gauge at=10 polls=20" \
  "$(ab_reset "0" "1" "1" "4"; ab_tick a 10)"
check "no-gauge: preferred=b never reads 1 while a does; the budget expires with victim b" \
  "rc=1 victim=b reason=no-gauge at=10 polls=20" \
  "$(ab_reset "1" "4" "0" "1"; ab_tick b 10)"
# Nightly run 88's shape: the preferred worker owns no data unit and the
# other merges from 311 s to 380 s, inside the default 420 s budget (one
# tick plus 120 s). The other worker is not killed mid-merge; the wait runs
# to the budget and takes the no-gauge path.
run88_b="$(for (( e = 1; e <= 420; e++ )); do
  if (( e >= 311 && e <= 380 )); then printf '1 '; else printf '0 '; fi
done)"
check "no-gauge: the run-88 shape never kills the other worker and expires at the 420 s budget" \
  "rc=1 victim=a reason=no-gauge at=420 polls=840" \
  "$(ab_reset "0" "0" "${run88_b% }" "4"; ab_tick a 420)"

# Shard coverage after the load (chaos_metrics_shard_coverage). shard_keys
# prints a LIST of metrics commit records for shards 0 to N-1, two per
# shard, plus keys that must not count: metrics L0 data and a logs commit
# record, both under shard 0099.
shard_keys() { # N
  local s
  for (( s = 0; s < $1; s++ )); do
    printf 't/ab12/m/c/%04d/493001/w1.1.1.cmt\tt/ab12/m/c/%04d/493002/w1.1.2.cmt\t' "${s}" "${s}"
  done
  printf 't/ab12/m/l0/0099/w1.1.1.00112233445566ff.rseg\tt/ab12/l/c/0099/493001/w1.1.1.cmt\n'
}
coverage() { # want keys
  local rc=0
  AWS_MODE=keys AWS_KEYS="$2" chaos_metrics_shard_coverage "$1" 2>"${SCRATCH}/coverage-log" || rc=$?
  printf 'rc=%s %s\n' "${rc}" "$(sed 's/^\[chaos\] //' "${SCRATCH}/coverage-log")"
}
check "shard keys: metrics commit records count once per shard, L0 data and another signal not at all" "7" \
  "$(shard_keys 7 | chaos_metrics_shards_from_keys)"
check "shard keys: an empty LIST (None) is 0" "0" \
  "$(printf 'None\n' | chaos_metrics_shards_from_keys)"
check "shard coverage: 7 of 8 metrics shards holding data fails" \
  "rc=1 metrics shards holding data after the load: 7 of 8" \
  "$(coverage 8 "$(shard_keys 7)")"
check "shard coverage: 8 of 8 metrics shards holding data passes" \
  "rc=0 metrics shards holding data after the load: 8 of 8" \
  "$(coverage 8 "$(shard_keys 8)")"
check "shard coverage: a LIST that fails returns 2" "2" \
  "$(AWS_MODE=error rc_of chaos_metrics_shard_coverage 8)"
AWS_MODE=keys AWS_KEYS="" chaos_metrics_shard_coverage 8 >/dev/null 2>&1
check "shard coverage: the LIST is of t/ in the chaos bucket" "yes" \
  "$(grep -qxF -- "t/" "${AWS_ARGS_FILE}" && grep -qxF -- "${RAVEL_S3_BUCKET}" "${AWS_ARGS_FILE}" && grep -qxF list-objects-v2 "${AWS_ARGS_FILE}" && echo yes || echo no)"

# chaos_default_preferred_victim: alternates by GITHUB_RUN_NUMBER parity so
# successive nightly runs exercise both workers (issue #3037), defaulting to
# "a" with no run number or a non-numeric one.
check "chaos_default_preferred_victim: no GITHUB_RUN_NUMBER defaults to a" "a" \
  "$(unset GITHUB_RUN_NUMBER; chaos_default_preferred_victim)"
check "chaos_default_preferred_victim: even GITHUB_RUN_NUMBER picks a" "a" \
  "$(GITHUB_RUN_NUMBER=38770264 chaos_default_preferred_victim)"
check "chaos_default_preferred_victim: odd GITHUB_RUN_NUMBER picks b" "b" \
  "$(GITHUB_RUN_NUMBER=38770265 chaos_default_preferred_victim)"
check "chaos_default_preferred_victim: non-numeric GITHUB_RUN_NUMBER defaults to a" "a" \
  "$(GITHUB_RUN_NUMBER=not-a-number chaos_default_preferred_victim)"
check "chaos_default_preferred_victim: alternates across successive run numbers" "a b a b" \
  "$(out=""
     for n in 10 11 12 13; do out+="$(GITHUB_RUN_NUMBER=$n chaos_default_preferred_victim) "; done
     echo "${out% }")"

# Args: A's log, B's log, aborts baseline, CHAOS_KILL_ON_GAUGE. B's line
# count at the kill is 1, and the wait is 0 (one read). The store LIST reads
# AWS_MODE, absent unless a case sets it: a case where B published sets
# present too, as the store would be after that publish.
cons() {
  CHAOS_KILL_ON_GAUGE="${4:-1}" rc_of oracle_conservation_or_unmeasured "$1" http://stub "${3:-1}" "$2" 1
}
check "classifier: B published another bucket after the kill, not A's, fails" "1" \
  "$(cons "${LOG_A_MERGING}" "${LOG_B_OTHER}")"
check "classifier: B published A's interrupted bucket after the kill passes" "0" \
  "$(AWS_MODE=present cons "${LOG_A_MERGING}" "${LOG_B_SAME}")"
check "classifier: gauge seen, A killed in its first merge, B silent, no record fails" "1" \
  "$(cons "${LOG_A_FIRST}" "${LOG_B_SILENT}")"
check "classifier: gauge seen, A killed in its first merge, B published it passes" "0" \
  "$(AWS_MODE=present cons "${LOG_A_FIRST}" "${LOG_B_SAME}")"
check "classifier: the store holds the bucket's record and B is silent passes" "0" \
  "$(AWS_MODE=present cons "${LOG_A_FIRST}" "${LOG_B_SILENT}")"
check "classifier: B's publish line with no record in the store fails" "1" \
  "$(AWS_MODE=absent cons "${LOG_A_FIRST}" "${LOG_B_SAME}")"
check "classifier: a LIST that fails is could-not-measure (3), even with a B publish" "3" \
  "$(AWS_MODE=error cons "${LOG_A_FIRST}" "${LOG_B_SAME}")"
check "classifier: a LIST that fails never hides risen aborts" "1" \
  "$(AWS_MODE=error cons "${LOG_A_FIRST}" "${LOG_B_SAME}" 0)"
check "classifier: a later failed merge in A's log does not shadow the interrupted one" "0" \
  "$(AWS_MODE=present cons "${LOG_A_LATER_FAILED}" "${LOG_B_SAME}")"
# The FAIL detail says whether B started the bucket, and carries the
# durations line. Args: B's log.
cons_detail() {
  (
    ORACLE_FAIL=()
    CHAOS_KILL_ON_GAUGE=1 oracle_conservation_or_unmeasured "${LOG_A_FIRST}" http://stub 1 "$1" 1 >/dev/null 2>&1
    printf '%s\n' "${ORACLE_FAIL[*]}"
  )
}
check "classifier: a FAIL with B having started the bucket says so" "yes" \
  "$(d="$(cons_detail "${LOG_B_STARTED}")"; [[ "${d}" == *"${BUCKET_Y}: no record, survivor start yes, publish no"* && "${d}" == *"merge durations: n=0 p50=none max=none"* ]] && echo yes || echo no)"
check "classifier: a FAIL with B never having started the bucket says so" "yes" \
  "$(d="$(cons_detail "${LOG_B_SILENT}")"; [[ "${d}" == *"${BUCKET_Y}: no record, survivor start no, publish no"* ]] && echo yes || echo no)"
check "classifier: the oracle logs the durations line" "yes" \
  "$(d="$(CHAOS_KILL_ON_GAUGE=1 oracle_conservation_or_unmeasured "${LOG_DUR_A}" http://stub 1 "${LOG_DUR_B}" 0 2>&1 >/dev/null)"; [[ "${d}" == *'conservation: merge durations: n=3 p50=4.000 max=10.000'* ]] && echo yes || echo no)"
check "classifier: gauge never seen is could-not-measure (3)" "3" \
  "$(cons "${LOG_A_FIRST}" "${LOG_B_SILENT}" 1 0)"
check "classifier: gauge never seen is could-not-measure even with a B publish" "3" \
  "$(cons "${LOG_A_FIRST}" "${LOG_B_SAME}" 1 0)"
check "classifier: A's log has no unmatched start is could-not-measure (3)" "3" \
  "$(cons "${LOG_A_ALL_DONE}" "${LOG_B_SILENT}")"
check "classifier: A's log has no start at all is could-not-measure (3)" "3" \
  "$(cons "${LOG_A_NO_START}" "${LOG_B_OTHER}")"
check "classifier: B published A's bucket only before the kill fails" "1" \
  "$(cons "${LOG_A_FIRST}" "${LOG_B_PRE_KILL}")"
check "classifier: B aborts rose (1 against 0), gauge never seen, still fails" "1" \
  "$(cons "${LOG_A_FIRST}" "${LOG_B_SILENT}" 0 0)"
check "classifier: B aborts rose (1 against 0), B published A's bucket still fails" "1" \
  "$(cons "${LOG_A_FIRST}" "${LOG_B_SAME}" 0)"
check "classifier: unreadable aborts counter fails even when unmeasurable" "1" \
  "$(CURL_MODE=unreachable cons "${LOG_A_ALL_DONE}" "${LOG_B_SILENT}")"
check "classifier: a non-integer line count fails conservation" "1" \
  "$(CHAOS_KILL_ON_GAUGE=1 rc_of oracle_conservation_or_unmeasured "${LOG_A_FIRST}" http://stub 1 "${LOG_B_SAME}" x)"

LOG_ACTIVITY="${SCRATCH}/activity.log"
{ printf '%s\n' "${PUB_LINE}"; pass_line 0 1; pass_line 1 0; printf '%s\n' "${PUB_LINE}"; pass_line 2 3; } >"${LOG_ACTIVITY}"
check "activity: whole log counts publishes, passes and summed compacted/not_sealed" \
  "WORKER-ACTIVITY: w publishes=2 passes=3 compacted=4 not_sealed=0" \
  "$(chaos_worker_activity_line w "${LOG_ACTIVITY}")"
check "activity: only the lines past the given count" \
  "WORKER-ACTIVITY: w publishes=1 passes=1 compacted=3 not_sealed=0" \
  "$(chaos_worker_activity_line w "${LOG_ACTIVITY}" 3)"
check "activity: a missing log reports zeros" \
  "WORKER-ACTIVITY: w publishes=0 passes=0 compacted=0 not_sealed=0" \
  "$(chaos_worker_activity_line w "${SCRATCH}/no-such.log")"
check "activity: not_sealed is summed" "WORKER-ACTIVITY: w publishes=0 passes=1 compacted=0 not_sealed=2" \
  "$(chaos_worker_activity_line w <(printf 'x maintenance: retention + compaction pass complete compacted=0 not_sealed=2\n'))"

# The scenarios run under set -eE and an ERR trap that exits 3; the in-flight
# export's own exit code must still reach `wait`.
bg_export_rc() {
  (
    set -eEuo pipefail
    trap 'exit 3' ERR
    drive_one_export() { return "$1"; }
    chaos_start_background_export "$1" fixture "${SCRATCH}/bg-out"
    rc=0
    wait "$CHAOS_BG_EXPORT_PID" || rc=$?
    echo "$rc"
  )
}
check "background export: curl 52 survives the ERR trap" "52" "$(bg_export_rc 52)"
check "background export: curl 22 survives the ERR trap" "22" "$(bg_export_rc 22)"
check "background export: success is 0" "0" "$(bg_export_rc 0)"
check "background export: 52 reads as unanswered" "0" "$(chaos_inflight_answered "$(bg_export_rc 52)")"

# wait_for_flush_started's deadline is wall-clock: a scrape that takes 2 s
# against a 1 s deadline returns after one scrape, not after 5 polls of it.
flush_wait_seconds() {
  local t0 t1
  t0="$(date +%s)"
  CURL_MODE=slow-metrics METRICS_SAMPLE="${METRICS_TIME_ONLY}" wait_for_flush_started http://stub 999 1 >/dev/null 2>&1
  t1="$(date +%s)"
  echo $(( t1 - t0 ))
}
check "flush wait: a slow /metrics stops at the deadline (at most 4 s for a 1 s deadline)" "yes" \
  "$([[ "$(flush_wait_seconds)" -le 4 ]] && echo yes || echo no)"

check "chaos_line_count counts lines" "2" "$(chaos_line_count "${LOG_B_PRE_KILL}")"
check "chaos_line_count of a missing file is 0" "0" "$(chaos_line_count "${SCRATCH}/no-such.log")"
printf 'a\nb' >"${SCRATCH}/no-final-newline.log"
check "chaos_line_count counts a last line with no newline, as tail -n +N reads it" "2" \
  "$(chaos_line_count "${SCRATCH}/no-final-newline.log")"
scenario1_body="$(cat "${CHAOS_DIR}/kill-ingest-flush.sh")"
check "scenario 1 refuses an unreadable flush baseline rather than reading it as 0" "yes" \
  "$([[ "${scenario1_body}" != *'FLUSH_BASELINE=0'* && "${scenario1_body}" == *'could not read the flush-attempt baseline'* ]] && echo yes || echo no)"
check "scenario 2 waits at least one maintenance tick for A to be mid-compaction" "yes" \
  "$([[ "$(cat "${CHAOS_DIR}/kill-maintain-worker.sh")" == *'INFLIGHT_BUDGET_SECONDS=$(( CHAOS_MAINTAIN_TICK_SECONDS + 120 ))'* ]] && echo yes || echo no)"
scenario2_summary() {
  # Args: A's log, B's log, aborts baseline, an extra failure or "".
  ORACLE_PASS=(other)
  ORACLE_FAIL=()
  ORACLE_UNMEASURED=()
  [[ -n "$4" ]] && ORACLE_FAIL=("$4")
  CHAOS_KILL_ON_GAUGE=1 oracle_conservation_or_unmeasured "$1" http://stub "$3" "$2" 1 >/dev/null 2>&1
  local rc=0
  print_oracle_summary scenario blocking >"${SCRATCH}/summary.out" 2>&1 || rc=$?
  printf 'rc=%s conservation-fail=%s\n' "${rc}" \
    "$(grep -c '^  FAIL  conservation-holds' "${SCRATCH}/summary.out")"
}
check "summary: could-not-measure with nothing failed exits 3" "rc=3 conservation-fail=0" \
  "$(scenario2_summary "${LOG_A_ALL_DONE}" "${LOG_B_SILENT}" 1 "")"
check "summary: could-not-measure never hides a failure (exit 2)" "rc=2 conservation-fail=0" \
  "$(scenario2_summary "${LOG_A_ALL_DONE}" "${LOG_B_SILENT}" 1 "no-orphaned-lease: detail")"
check "summary: B aborts rose while A finished and published is exit 2, conservation FAIL" \
  "rc=2 conservation-fail=1" "$(scenario2_summary "${LOG_A_ALL_DONE}" "${LOG_B_SILENT}" 0 "")"
check "summary: A interrupted in Y, B published only Z is exit 2" "rc=2 conservation-fail=1" \
  "$(scenario2_summary "${LOG_A_MERGING}" "${LOG_B_OTHER}" 1 "")"
check "summary: A interrupted in Y, B published Y is exit 0" "rc=0 conservation-fail=0" \
  "$(AWS_MODE=present scenario2_summary "${LOG_A_MERGING}" "${LOG_B_SAME}" 1 "")"
check "summary: A interrupted in Y, Y's record in the store, B silent is exit 0" "rc=0 conservation-fail=0" \
  "$(AWS_MODE=present scenario2_summary "${LOG_A_MERGING}" "${LOG_B_SILENT}" 1 "")"
check "summary: a LIST that fails with nothing else failed exits 3" "rc=3 conservation-fail=0" \
  "$(AWS_MODE=error scenario2_summary "${LOG_A_MERGING}" "${LOG_B_SAME}" 1 "")"
# A had X and Y open at the kill. A survivor that recovers only Y must not
# pass: conservation has to hold for every merge the kill interrupted.
check "summary: two buckets open at the kill, a record only for the later one is exit 2" \
  "rc=2 conservation-fail=1" \
  "$(AWS_MODE=only AWS_ONLY_BUCKET="${BUCKET_Y}" scenario2_summary "${LOG_A_TWO_OPEN}" "${LOG_B_SILENT}" 1 "")"
check "summary: two buckets open at the kill, a record only for the earlier one is exit 2" \
  "rc=2 conservation-fail=1" \
  "$(AWS_MODE=only AWS_ONLY_BUCKET="${BUCKET_X}" scenario2_summary "${LOG_A_TWO_OPEN}" "${LOG_B_SILENT}" 1 "")"
check "summary: two buckets open at the kill, records for both is exit 0" "rc=0 conservation-fail=0" \
  "$(AWS_MODE=present scenario2_summary "${LOG_A_TWO_OPEN}" "${LOG_B_SILENT}" 1 "")"
check "summary: two buckets open, one recorded and the other's LIST fails is exit 3" \
  "rc=3 conservation-fail=0" \
  "$(AWS_MODE=only AWS_ONLY_BUCKET="${BUCKET_Y}" AWS_OTHERS=error scenario2_summary "${LOG_A_TWO_OPEN}" "${LOG_B_SILENT}" 1 "")"
two_open_detail() {
  ORACLE_PASS=()
  ORACLE_FAIL=()
  ORACLE_UNMEASURED=()
  AWS_MODE=only AWS_ONLY_BUCKET="${BUCKET_Y}" CHAOS_KILL_ON_GAUGE=1 \
    oracle_conservation_or_unmeasured "${LOG_A_TWO_OPEN}" http://stub 1 "${LOG_B_SILENT}" 1 >/dev/null 2>&1 || true
  printf '%s\n' "${ORACLE_FAIL[@]}"
}
check "conservation: the FAIL detail names each open bucket's verdict" "yes" \
  "$(d="$(two_open_detail)"; [[ "${d}" == *"1 of 2 interrupted bucket(s)"* && "${d}" == *"${BUCKET_X}: no record"* && "${d}" == *"${BUCKET_Y}: 1 record(s)"* ]] && echo yes || echo no)"
check "scenario 2 stops its load at a deadline, not only at the export count" "yes" \
  "$([[ "$(cat "${CHAOS_DIR}/kill-maintain-worker.sh")" == *'>= CHAOS_MAINTAIN_LOAD_MAX_SECONDS'* ]] && echo yes || echo no)"
check "scenario 2 goes on after a partly rejected load and stops only below the minimum" "yes" \
  "$(b="$(cat "${CHAOS_DIR}/kill-maintain-worker.sh")"; [[ "${b}" == *'"$SENT" -lt "$CHAOS_MAINTAIN_MIN_EXPORTS"'* && "${b}" != *'"$SENT" -lt "$EXPORT_COUNT"'* ]] && echo yes || echo no)"

scenario2_body="$(cat "${CHAOS_DIR}/kill-maintain-worker.sh")"
universe_line="$(scenario2_line 'assert_single_tenant_universe || true')"
inflight_wait_line="$(scenario2_line 'if wait_for_compaction_in_flight_preferred')"
first_kill_line="$(grep -n -F 'sigkill_pid "${!VICTIM_PID_VAR}"' "${CHAOS_DIR}/kill-maintain-worker.sh" | head -1 | cut -d: -f1)"
check "scenario 2 checks the universe before the wait, and kills right after it" "yes" \
  "$([[ -n "${universe_line}" && "${universe_line}" -lt "${inflight_wait_line:-0}" && "${first_kill_line:-0}" -gt "${inflight_wait_line:-0}" && "$(only_bookkeeping_between "${CHAOS_DIR}/kill-maintain-worker.sh" "${inflight_wait_line}" "${first_kill_line}")" == 0 ]] && echo yes || echo no)"
survivor_count_line="$(scenario2_line 'SURVIVOR_LOG_LINES_AT_KILL="$(chaos_line_count "$SURVIVOR_LOG")"')"
last_kill_line="$(grep -n -F 'sigkill_pid "${!VICTIM_PID_VAR}"' "${CHAOS_DIR}/kill-maintain-worker.sh" | tail -1 | cut -d: -f1)"
cons_call_line="$(scenario2_line '"$SURVIVOR_URL" "$CONS_BASELINE" "$SURVIVOR_LOG" "$SURVIVOR_LOG_LINES_AT_KILL" "$KILL_EPOCH" || true')"
check "scenario 2 counts the survivor's log after the last kill and passes it to the conservation oracle" "yes" \
  "$([[ -n "${survivor_count_line}" && -n "${cons_call_line}" && "${survivor_count_line}" -gt "${last_kill_line:-999999}" && "${survivor_count_line}" -lt "${cons_call_line}" ]] && echo yes || echo no)"
check "scenario 2 headers make no 'provably' claim" "no" \
  "$([[ "${scenario2_body}" == *provabl* ]] && echo yes || echo no)"
check "scenario 2 triggers on the preferred worker's gauge only, not on a fixed worker A" "yes" \
  "$([[ "${scenario2_body}" == *'wait_for_compaction_in_flight_preferred "$CHAOS_PREFERRED_VICTIM" "$WORKER_A_URL" "$WORKER_B_URL" "$INFLIGHT_BUDGET_SECONDS"; then'* ]] && echo yes || echo no)"
check "scenario 2 bounds the whole in-flight wait at one maintenance tick plus 120 s, with no extra phase" "yes" \
  "$([[ "${scenario2_body}" == *$'\nINFLIGHT_BUDGET_SECONDS=$(( CHAOS_MAINTAIN_TICK_SECONDS + 120 ))\n'* && "${scenario2_body}" != *FALLBACK_LIVE_WAIT* ]] && echo yes || echo no)"
check "scenario 2 logs the no-gauge path when the preferred worker never reads 1" "yes" \
  "$([[ "${scenario2_body}" == *'(no-gauge, no reading >= 1 from it within ${INFLIGHT_BUDGET_SECONDS}s)'* ]] && echo yes || echo no)"
check "no fallback decision window, sibling-first timestamp or fallback reason is left in scenario 2 or the library" "0" \
  "$(cat "${CHAOS_DIR}/lib.sh" "${CHAOS_DIR}/kill-maintain-worker.sh" "${REPO_ROOT}/.github/workflows/k8s-nightly.yml" | grep -cE 'CHAOS_FALLBACK_DECIDE_SECONDS|CHAOS_SIBLING_FIRST_LIVE_S|fallback-no-data-shard')"
check "scenario 2 defaults to 8 shards" "yes" \
  "$([[ "${scenario2_body}" == *$'\nCHAOS_SHARD_COUNT="${CHAOS_SHARD_COUNT:-8}"\n'* ]] && echo yes || echo no)"
check "the nightly scenario 2 step sets CHAOS_SHARD_COUNT to 8" "1" \
  "$(grep -cE '^ +CHAOS_SHARD_COUNT: "8"$' "${REPO_ROOT}/.github/workflows/k8s-nightly.yml")"
check "scenario 2 --check refuses a non-integer CHAOS_SHARD_COUNT" "64" \
  "$(CHAOS_SHARD_COUNT=1.5 rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check "scenario 2 --check refuses CHAOS_SHARD_COUNT=0" "64" \
  "$(CHAOS_SHARD_COUNT=0 rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check "scenario 2 --check derives 24 expected units from 8 shards" "1" \
  "$(env -u CHAOS_SHARD_COUNT -u CHAOS_EXPECTED_TOTAL_UNITS bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check 2>&1 | grep -cF 'expected total units: 24 (3 signals x 8 shards')"
check "scenario 2 --check follows an overridden shard count into the expected units" "1" \
  "$(env -u CHAOS_EXPECTED_TOTAL_UNITS CHAOS_SHARD_COUNT=5 bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check 2>&1 | grep -cF 'expected total units: 15 (3 signals x 5 shards')"
coverage_line="$(scenario2_line 'chaos_metrics_shard_coverage "$CHAOS_SHARD_COUNT" || coverage_rc=$?')"
check "scenario 2 checks shard coverage after the load and before the seal wait, and exits 3 below the count" "yes" \
  "$([[ -n "${coverage_line}" && "${coverage_line}" -gt "${stop_line:-999999}" && "${coverage_line}" -lt "${wait_line:-0}" && "$(sed -n "$(( coverage_line + 1 )),$(( coverage_line + 4 ))p" "${CHAOS_DIR}/kill-maintain-worker.sh")" == *'exit 3'* ]] && echo yes || echo no)"
check "scenario 2 logs the survivor's last units_owned before the kill against the expected total" "yes" \
  "$([[ "${scenario2_body}" == *'${SURVIVOR_LAST_UNITS_OWNED} of ${EXPECTED_TOTAL_UNITS}'* ]] && echo yes || echo no)"
check "scenario 2 prints the exact VICTIM line, via echo and not log (log prepends '[chaos] ')" "yes" \
  "$([[ "${scenario2_body}" == *'echo "VICTIM preferred=${CHAOS_PREFERRED_VICTIM} chosen=${CHAOS_VICTIM} reason=${CHAOS_VICTIM_REASON}"'* ]] && echo yes || echo no)"
check "scenario 2 prints the VICTIM line exactly once" "1" \
  "$(grep -cF 'echo "VICTIM preferred=${CHAOS_PREFERRED_VICTIM} chosen=${CHAOS_VICTIM} reason=${CHAOS_VICTIM_REASON}"' "${CHAOS_DIR}/kill-maintain-worker.sh")"
check "scenario 2 gates both takeover oracles on chaos_takeover_measurable" "yes" \
  "$([[ "${scenario2_body}" == *'if chaos_takeover_measurable "$SURVIVOR_LAST_UNITS_OWNED" "$EXPECTED_TOTAL_UNITS"; then'* ]] && echo yes || echo no)"
check "scenario 2 --check refuses CHAOS_MAINTAINED_SIGNALS=0" "64" \
  "$(CHAOS_MAINTAINED_SIGNALS=0 rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check "scenario 2 --check refuses a non-integer CHAOS_MAINTAINED_SIGNALS" "64" \
  "$(CHAOS_MAINTAINED_SIGNALS=x rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check "scenario 2 --check refuses CHAOS_EXPECTED_TOTAL_UNITS=0" "64" \
  "$(CHAOS_EXPECTED_TOTAL_UNITS=0 rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check "takeover measurable: survivor held 12 of 24 before the kill" "0" \
  "$(rc_of chaos_takeover_measurable 12 24)"
check "takeover measurable: survivor held 0 of 24 before the kill" "0" \
  "$(rc_of chaos_takeover_measurable 0 24)"
check "takeover not measurable: survivor already held all 24" "1" \
  "$(rc_of chaos_takeover_measurable 24 24)"
check "takeover not measurable: survivor held more than the total" "1" \
  "$(rc_of chaos_takeover_measurable 25 24)"
check "takeover not measurable: survivor's reading unknown" "1" \
  "$(rc_of chaos_takeover_measurable unknown 24)"
check "takeover not measurable: total of 0" "1" \
  "$(rc_of chaos_takeover_measurable 0 0)"
check "scenario 2 passes --shards \$CHAOS_SHARD_COUNT to both server launch sites" "2" \
  "$(grep -cF -- '--shards "$CHAOS_SHARD_COUNT")' "${CHAOS_DIR}/kill-maintain-worker.sh")"
check "scenario 2 validates an invalid CHAOS_PREFERRED_VICTIM" "64" \
  "$(CHAOS_PREFERRED_VICTIM=c rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check "scenario 2 --check runs with a valid CHAOS_PREFERRED_VICTIM=a" "0" \
  "$(CHAOS_PREFERRED_VICTIM=a rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check "scenario 2 --check runs with a valid CHAOS_PREFERRED_VICTIM=b" "0" \
  "$(CHAOS_PREFERRED_VICTIM=b rc_of bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check)"
check_output_file="${SCRATCH}/scenario2-check-output"
CHAOS_PREFERRED_VICTIM=a bash "${CHAOS_DIR}/kill-maintain-worker.sh" --check >"${check_output_file}" 2>&1
check "--check names the CHAOS_PREFERRED_VICTIM entry point" "yes" \
  "$(grep -qF 'CHAOS_PREFERRED_VICTIM' "${check_output_file}" && echo yes || echo no)"
check "--check names the CHAOS_SHARD_COUNT entry point" "yes" \
  "$(grep -qF 'CHAOS_SHARD_COUNT' "${check_output_file}" && echo yes || echo no)"

# Nit (#3037): CHAOS_MAINTAINED_SIGNALS' literal default must track the
# maintain loop's own MAINTAINED_SIGNALS array length; a chaos-lane total that
# silently drifts from the real signal count reintroduces exactly the
# vacuous-pass bug deliverable 2 removed (a wrong EXPECTED_TOTAL_UNITS).
maintain_signals_decl="$(grep -oE 'const MAINTAINED_SIGNALS: \[Signal; [0-9]+\]' \
  "${REPO_ROOT}/services/ravel-server/src/maintain.rs")"
maintain_signals_rust_count="$(grep -oE '[0-9]+' <<<"${maintain_signals_decl}")"
chaos_maintained_signals_default="$(grep -oE 'CHAOS_MAINTAINED_SIGNALS:-[0-9]+' \
  "${CHAOS_DIR}/kill-maintain-worker.sh" | grep -oE '[0-9]+')"
check "MAINTAINED_SIGNALS extraction from maintain.rs found a count" "yes" \
  "$([[ -n "${maintain_signals_rust_count}" ]] && echo yes || echo no)"
check "CHAOS_MAINTAINED_SIGNALS default extraction found a count" "yes" \
  "$([[ -n "${chaos_maintained_signals_default}" ]] && echo yes || echo no)"
check "CHAOS_MAINTAINED_SIGNALS default matches maintain.rs's MAINTAINED_SIGNALS length" "yes" \
  "$([[ "${chaos_maintained_signals_default}" == "${maintain_signals_rust_count}" ]] && echo yes || echo no)"

# DISTINGUISHING(b): a script that picks victim b (CHAOS_VICTIM=b) but still
# hands the takeover and leak oracles worker B's URL/log, unconditionally,
# instead of the survivor's -- here, worker A's -- would call
# oracle_sibling_takeover_within_bound/oracle_no_orphaned_lease/
# oracle_no_partial_output_leak with "$WORKER_B_URL" hardcoded in place of
# "$SURVIVOR_URL". Every flipped line contains "$WORKER_B_URL" literally,
# which a correct caller never does: it always reads $SURVIVOR_URL, which is
# worker A's URL when B is the victim.
sibling_call_text="$(grep -F 'oracle_sibling_takeover_within_bound' "${CHAOS_DIR}/kill-maintain-worker.sh")"
orphan_call_text="$(grep -F 'oracle_no_orphaned_lease' "${CHAOS_DIR}/kill-maintain-worker.sh")"
leak_call_text="$(grep -F 'oracle_no_partial_output_leak' "${CHAOS_DIR}/kill-maintain-worker.sh")"
check "DISTINGUISHING(b) the takeover and leak oracles read \$SURVIVOR_URL, never a hardcoded \$WORKER_A_URL/\$WORKER_B_URL" "yes" \
  "$([[ "${sibling_call_text}" == *'"$SURVIVOR_URL"'* && "${orphan_call_text}" == *'"$SURVIVOR_URL"'* && "${leak_call_text}" == *'"$SURVIVOR_URL"'* \
     && "${sibling_call_text}" != *'WORKER_A_URL'* && "${sibling_call_text}" != *'WORKER_B_URL'* \
     && "${orphan_call_text}" != *'WORKER_A_URL'* && "${orphan_call_text}" != *'WORKER_B_URL'* \
     && "${leak_call_text}" != *'WORKER_A_URL'* && "${leak_call_text}" != *'WORKER_B_URL'* ]] && echo yes || echo no)"

# chaos_select_survivor: the victim-to-survivor mapping DISTINGUISHING(b)
# above only pins at the text level (the oracle calls read $SURVIVOR_URL, not
# a hardcoded worker). This covers the mapping itself behaviourally: for each
# victim letter, every SURVIVOR_*/VICTIM_* output and baseline must come from
# the OTHER worker (and the victim's own label/log for the VICTIM_* outputs),
# and the victim's own PID global must be cleared to "" while the survivor's
# is left untouched.
select_survivor_case() { # victim(a|b)
  WORKER_A_LOG="log-a"; WORKER_B_LOG="log-b"
  WORKER_A_URL="http://a"; WORKER_B_URL="http://b"
  CHAOS_A_MAX_UNITS_OWNED=3; CHAOS_B_MAX_UNITS_OWNED=9
  CONS_BASELINE_A=1; CONS_BASELINE_B=2
  BREAKER_BASELINE_A=5; BREAKER_BASELINE_B=6
  WORKER_A_PID=111; WORKER_B_PID=222
  chaos_select_survivor "$1"
  printf 'VICTIM_LABEL=%s VICTIM_LOG=%s SURVIVOR_LABEL=%s SURVIVOR_URL=%s SURVIVOR_LOG=%s CONS_BASELINE=%s BREAKER_BASELINE=%s WORKER_A_PID=%s WORKER_B_PID=%s' \
    "${VICTIM_LABEL}" "${VICTIM_LOG}" "${SURVIVOR_LABEL}" "${SURVIVOR_URL}" "${SURVIVOR_LOG}" \
    "${CONS_BASELINE}" "${BREAKER_BASELINE}" "${WORKER_A_PID}" "${WORKER_B_PID}"
}
check "chaos_select_survivor a: survivor is worker-b with B's URL/log/peak/baselines, A's pid cleared" \
  "VICTIM_LABEL=worker-a VICTIM_LOG=log-a SURVIVOR_LABEL=worker-b SURVIVOR_URL=http://b SURVIVOR_LOG=log-b CONS_BASELINE=2 BREAKER_BASELINE=6 WORKER_A_PID= WORKER_B_PID=222" \
  "$(select_survivor_case a)"
check "chaos_select_survivor b: survivor is worker-a with A's URL/log/peak/baselines, B's pid cleared" \
  "VICTIM_LABEL=worker-b VICTIM_LOG=log-b SURVIVOR_LABEL=worker-a SURVIVOR_URL=http://a SURVIVOR_LOG=log-a CONS_BASELINE=1 BREAKER_BASELINE=5 WORKER_A_PID=111 WORKER_B_PID=" \
  "$(select_survivor_case b)"
check "chaos_select_survivor: the survivor's last units_owned comes from the other worker" "a:8 b:4" \
  "$(select_survivor_case a >/dev/null
     CHAOS_A_LAST_UNITS_OWNED=4; CHAOS_B_LAST_UNITS_OWNED=8
     chaos_select_survivor a; o="a:${SURVIVOR_LAST_UNITS_OWNED}"
     chaos_select_survivor b; echo "${o} b:${SURVIVOR_LAST_UNITS_OWNED}")"
check "chaos_select_survivor: an unread last units_owned is unknown" "unknown" \
  "$(select_survivor_case a >/dev/null; unset CHAOS_B_LAST_UNITS_OWNED
     chaos_select_survivor a; echo "${SURVIVOR_LAST_UNITS_OWNED}")"

# DISTINGUISHING(c), part 2: a script that substitutes a bare literal 4 back
# in for the derived signals x shards total, or for CHAOS_SHARD_COUNT in the
# custody oracle call, reintroduces the vacuous-pass bug this task fixes
# (deliverable 2). The flipped lines are EXPECTED_TOTAL_UNITS's default
# falling back to "${CHAOS_EXPECTED_TOTAL_UNITS:-4}" instead of the
# signals*shards product, and
# oracle_custody_and_catalog_verify_clean's second argument reading a bare
# " 4 " instead of "$CHAOS_SHARD_COUNT".
expected_units_line="$(grep -F 'EXPECTED_TOTAL_UNITS="${CHAOS_EXPECTED_TOTAL_UNITS:-' "${CHAOS_DIR}/kill-maintain-worker.sh")"
custody_call_text="$(grep -F 'oracle_custody_and_catalog_verify_clean' "${CHAOS_DIR}/kill-maintain-worker.sh")"
check "DISTINGUISHING(c) EXPECTED_TOTAL_UNITS derives from signals x shards, not a literal 4" "yes" \
  "$([[ "${expected_units_line}" == *'CHAOS_MAINTAINED_SIGNALS * CHAOS_SHARD_COUNT'* && "${expected_units_line}" != *':-4}'* ]] && echo yes || echo no)"
check "DISTINGUISHING(c) the custody oracle gets \$CHAOS_SHARD_COUNT, not a literal 4" "yes" \
  "$([[ "${custody_call_text}" == *'"$CHAOS_SHARD_COUNT"'* && "${custody_call_text}" != *' 4 '* ]] && echo yes || echo no)"

# The conservation oracle waits, from the kill, until the bound or a survivor
# publish of A's interrupted bucket that lands after the kill line, then
# lists the store. Args: A's log, B's log, wait seconds. B's log starts as one
# pre-kill line, so the kill line count is 1. Each case sets
# CHAOS_KILL_ON_GAUGE, and AWS_MODE when the store holds the record.
LOG_B_WAIT="${SCRATCH}/b-wait.log"
cons_wait() {
  CHAOS_CONSERVATION_WAIT_SECONDS="$3" \
    rc_of oracle_conservation_or_unmeasured "$1" http://stub 1 "$2" 1 "$(date +%s)"
}
printf '%s\n' "${OTHER_LINE}" >"${LOG_B_WAIT}"
( sleep 1; pub_line "${BUCKET_Y}" >>"${LOG_B_WAIT}" ) &
check "conservation wait: a publish of A's bucket landing during the wait passes" "0" \
  "$(AWS_MODE=present CHAOS_KILL_ON_GAUGE=1 cons_wait "${LOG_A_FIRST}" "${LOG_B_WAIT}" 5)"
wait
cons_wait_timed() {
  local t0 rc
  t0="$(date +%s)"
  rc="$(cons_wait "$1" "$2" "$3")"
  printf 'rc=%s waited=%s\n' "${rc}" "$([[ $(( $(date +%s) - t0 )) -ge "$3" ]] && echo yes || echo no)"
}
printf '%s\n' "${OTHER_LINE}" >"${LOG_B_WAIT}"
check "conservation wait: no record of A's bucket by the bound fails after waiting" "rc=1 waited=yes" \
  "$(AWS_MODE=absent CHAOS_KILL_ON_GAUGE=1 cons_wait_timed "${LOG_A_FIRST}" "${LOG_B_WAIT}" 2)"
check "conservation wait: a record in the store with B silent passes after the bound" "rc=0 waited=yes" \
  "$(AWS_MODE=present CHAOS_KILL_ON_GAUGE=1 cons_wait_timed "${LOG_A_FIRST}" "${LOG_B_WAIT}" 2)"
check "conservation wait: a LIST that fails after the bound is could-not-measure" "rc=3 waited=yes" \
  "$(AWS_MODE=error CHAOS_KILL_ON_GAUGE=1 cons_wait_timed "${LOG_A_FIRST}" "${LOG_B_WAIT}" 2)"
check "conservation wait: B's publish of A's bucket ends the wait early" "rc=0 waited=no" \
  "$(AWS_MODE=present CHAOS_KILL_ON_GAUGE=1 cons_wait_timed "${LOG_A_FIRST}" "${LOG_B_SAME}" 3)"
check "conservation wait: B's own publishes do not end the wait" "rc=1 waited=yes" \
  "$(CHAOS_KILL_ON_GAUGE=1 cons_wait_timed "${LOG_A_MERGING}" "${LOG_B_OTHER}" 2)"
check "conservation wait: a pre-kill publish does not end the wait" "rc=1 waited=yes" \
  "$(CHAOS_KILL_ON_GAUGE=1 cons_wait_timed "${LOG_A_FIRST}" "${LOG_B_PRE_KILL}" 2)"
check "conservation wait: could-not-measure does not wait" "rc=3 waited=no" \
  "$(CHAOS_KILL_ON_GAUGE=0 cons_wait_timed "${LOG_A_FIRST}" "${LOG_B_WAIT}" 2)"
aborts_first() {
  local t0 rc
  t0="$(date +%s)"
  rc="$(CHAOS_KILL_ON_GAUGE=1 CHAOS_CONSERVATION_WAIT_SECONDS=3 rc_of oracle_conservation_or_unmeasured \
    "${LOG_A_FIRST}" http://stub 0 "${LOG_B_WAIT}" 1 "$(date +%s)")"
  printf 'rc=%s waited=%s\n' "${rc}" "$([[ $(( $(date +%s) - t0 )) -ge 3 ]] && echo yes || echo no)"
}
check "conservation wait: aborts that rose before the wait fail without waiting" "rc=1 waited=no" \
  "$(aborts_first)"
METRICS_FILE="${SCRATCH}/metrics-file"
printf '%s\n' "${METRICS_SAMPLE}" >"${METRICS_FILE}"
printf '%s\n' "${OTHER_LINE}" >"${LOG_B_WAIT}"
(
  sleep 1
  sed 's/^\(ravel_maintain_conservation_aborts_total{mode="maintain",signal="metrics"}\) 1$/\1 2/' \
    "${METRICS_FILE}" >"${METRICS_FILE}.new"
  command mv "${METRICS_FILE}.new" "${METRICS_FILE}"
  pub_line "${BUCKET_Y}" >>"${LOG_B_WAIT}"
) &
check "conservation wait: an abort during the wait fails even with a publish" "1" \
  "$(CURL_MODE=metrics-file CHAOS_KILL_ON_GAUGE=1 cons_wait "${LOG_A_FIRST}" "${LOG_B_WAIT}" 5)"
wait
check "conservation wait: the default is the takeover bound plus one tick plus the merge allowance" \
  "1080" "$(CHAOS_H_SECONDS=60 CHAOS_MAINTAIN_TICK_SECONDS=300 CHAOS_CONSERVATION_MERGE_ALLOWANCE_SECONDS=300 chaos_conservation_wait_seconds)"
check "scenario 2 passes the gauge observation to the oracle" "yes" \
  "$([[ "${scenario2_body}" == *'CHAOS_KILL_ON_GAUGE=1'* && "${scenario2_body}" == *'CHAOS_KILL_ON_GAUGE=0'* ]] && echo yes || echo no)"
check "scenario 2 reaps the pacing sleep on exit" "yes" \
  "$([[ "${scenario2_body}" == *'"$INGEST_PID" "$PACE_PID"; do'* ]] && echo yes || echo no)"

# ---------------------------------------------------------------------------
# Fixture arguments reach gen_otlp_fixture, and none are added by default.
# ---------------------------------------------------------------------------

GEN_STUB_DIR="$(mktemp -d)"
printf '#!/usr/bin/env bash\nprintf "argc=%%s args=%%s\\n" "$#" "$*"\n' >"${GEN_STUB_DIR}/gen_otlp_fixture"
chmod +x "${GEN_STUB_DIR}/gen_otlp_fixture"
check "chaos_gen_fixture with no arguments passes none" "argc=0 args=" \
  "$(PATH="${GEN_STUB_DIR}:${PATH}" chaos_gen_fixture)"
check "chaos_gen_fixture passes SERIES POINTS through" "argc=2 args=1000 100" \
  "$(PATH="${GEN_STUB_DIR}:${PATH}" chaos_gen_fixture "${CHAOS_FIXTURE_SERIES}" "${CHAOS_FIXTURE_POINTS}")"
rm -rf "${GEN_STUB_DIR}"

# ---------------------------------------------------------------------------
# Commit tokens: opaque strings, extracted verbatim.
# ---------------------------------------------------------------------------

CRLF=$'\r\n'
HDR_ONE="HTTP/1.1 200 OK${CRLF}content-type: application/x-protobuf${CRLF}x-ravel-commit-token: ${TOKEN_A}${CRLF}${CRLF}"
HDR_MIXED_CASE="HTTP/1.1 200 OK${CRLF}X-Ravel-Commit-Token: ${TOKEN_A}${CRLF}${CRLF}"
HDR_TWO="HTTP/1.1 200 OK${CRLF}x-ravel-commit-token: ${TOKEN_A},${TOKEN_B}${CRLF}${CRLF}"
HDR_NONE="HTTP/1.1 200 OK${CRLF}content-type: application/x-protobuf${CRLF}${CRLF}"

check "token fixture is the encoded v2 form" "v2:0:3f6c1b2e-9a7d-4c1e-bf00-7e5a9d2c4b11:1:42:494387" \
  "$(b64url_decode "${TOKEN_A}" 2>/dev/null)"
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
BODY_WARN='{"status":"success","data":{"resultType":"vector","result":[]},"warnings":["demo_requests_total matched no samples"]}'

check "query body with the series is visible" "0" \
  "$(rc_of query_body_shows_series "${BODY_HIT}" demo_requests_total)"
check "empty result is not visible" "1" \
  "$(rc_of query_body_shows_series "${BODY_EMPTY}" demo_requests_total)"
check "error body naming the series is not visible" "1" \
  "$(rc_of query_body_shows_series "${BODY_ERR}" demo_requests_total)"
check "success body naming the series only in a warning is not visible" "1" \
  "$(rc_of query_body_shows_series "${BODY_WARN}" demo_requests_total)"
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

# Each scenario's own prologue (its set line and its ERR trap, read from the
# script) must make a setup failure inside a helper exit 3, and its cleanup's
# own guard lines must keep a pending verdict when a command in the EXIT trap
# fails. Reading the lines from the scripts is what makes dropping -E, the
# trap, or either guard line fail here.
contract_rc() {
  local script="${CHAOS_DIR}/$1" body="$2" out="${SCRATCH}/contract-$1-$3.sh"
  {
    grep -m1 -x 'set -eEuo pipefail' "${script}"
    grep -m1 -x "trap 'exit 3' ERR" "${script}"
    printf '%s\n' "${body}"
  } >"${out}"
  rc_of bash "${out}"
}
cleanup_guard() {
  sed -n '/^cleanup() {/,/^}/p' "${CHAOS_DIR}/$1" | grep -E -x '  (trap - ERR|set \+e)'
}
for s in kill-ingest-flush.sh kill-maintain-worker.sh; do
  check "${s}: setup failure inside a helper exits 3" "3" \
    "$(contract_rc "${s}" 'helper() { false; :; }
helper' helper)"
  check "${s}: failing cleanup keeps a pending exit 2" "2" \
    "$(contract_rc "${s}" "cleanup() {
$(cleanup_guard "${s}")
  false
  :
}
trap cleanup EXIT
exit 2" cleanup)"
done

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

# ---------------------------------------------------------------------------
# Process logs: kept on failure when CHAOS_LOG_DIR is set, else deleted.
# ---------------------------------------------------------------------------

# Each case runs in a subshell so the CHAOS_LOG_DIR it sets does not leak.
check "logfile: without CHAOS_LOG_DIR it is a temp file that exists" "yes" \
  "$(CHAOS_LOG_DIR="" f="$(chaos_logfile s1 server)"; [[ -f "${f}" ]] && echo yes || echo no; rm -f "${f}")"
check "logfile: with CHAOS_LOG_DIR it is the named file under it" \
  "${SCRATCH}/logs/s1-server.log" \
  "$(CHAOS_LOG_DIR="${SCRATCH}/logs" chaos_logfile s1 server)"
check "logfile: with CHAOS_LOG_DIR the directory is created" "yes" \
  "$([[ -d "${SCRATCH}/logs" ]] && echo yes || echo no)"

mkdir -p "${SCRATCH}/logs"
printf 'server refused: the reason\n' > "${SCRATCH}/logs/kept.log"
printf 'deleted on success\n' > "${SCRATCH}/logs/gone.log"
check "release: exit 0 deletes the log even with CHAOS_LOG_DIR set" "no" \
  "$(CHAOS_LOG_DIR="${SCRATCH}/logs" chaos_release_logs 0 "${SCRATCH}/logs/gone.log" 2>/dev/null; [[ -f "${SCRATCH}/logs/gone.log" ]] && echo yes || echo no)"
printf 'x\n' >"${SCRATCH}/logs/keep-on-pass.log"
check "chaos_release_logs keeps a passing run's logs under CHAOS_KEEP_LOGS=1" "yes" \
  "$(CHAOS_KEEP_LOGS=1 CHAOS_LOG_DIR="${SCRATCH}/logs" chaos_release_logs 0 "${SCRATCH}/logs/keep-on-pass.log" 2>/dev/null; [[ -f "${SCRATCH}/logs/keep-on-pass.log" ]] && echo yes || echo no)"
check "chaos_release_logs ignores CHAOS_KEEP_LOGS without a log directory" "no" \
  "$(CHAOS_KEEP_LOGS=1 CHAOS_LOG_DIR="" chaos_release_logs 0 "${SCRATCH}/logs/keep-on-pass.log" 2>/dev/null; [[ -f "${SCRATCH}/logs/keep-on-pass.log" ]] && echo yes || echo no)"
kept_tail="$(CHAOS_LOG_DIR="${SCRATCH}/logs" chaos_release_logs 3 "${SCRATCH}/logs/kept.log" 2>&1 >/dev/null)"
check "release: exit 3 with CHAOS_LOG_DIR keeps the log" "yes" \
  "$([[ -f "${SCRATCH}/logs/kept.log" ]] && echo yes || echo no)"
check "release: exit 3 with CHAOS_LOG_DIR prints the kept path and the log's tail" "yes" \
  "$([[ "${kept_tail}" == *"kept ${SCRATCH}/logs/kept.log"* && "${kept_tail}" == *'server refused: the reason'* ]] && echo yes || echo no)"
printf 'no dir, no keep\n' > "${SCRATCH}/logs/nodir.log"
check "release: exit 3 without CHAOS_LOG_DIR deletes the log" "no" \
  "$(CHAOS_LOG_DIR="" chaos_release_logs 3 "${SCRATCH}/logs/nodir.log" 2>/dev/null; [[ -f "${SCRATCH}/logs/nodir.log" ]] && echo yes || echo no)"
# The scenarios' real cleanup functions, extracted from each script and run
# as the EXIT trap of a shell exiting 3 with CHAOS_LOG_DIR set, must keep the
# log. A command inserted before `local code=$?` (or the read moved below the
# process reaping) makes the trap see 0 and delete it; the log that this lane
# fails without is then gone with a green run. The processes are stubbed:
# no PID is set, so nothing is signalled, and rustfs_down is a no-op.
cleanup_keeps_log_on_exit_3() {
  local script="$1"
  local kept="${SCRATCH}/logs/cleanup-$(basename "${script}" .sh).log"
  local body
  printf 'the reason the start was refused\n' > "${kept}"
  body="$(sed -n '/^cleanup() {/,/^}/p' "${script}")"
  [[ -n "${body}" ]] || { echo "no cleanup function in ${script}"; return; }
  CHAOS_LOG_DIR="${SCRATCH}/logs" bash -c '
    set -euo pipefail
    source "$1"
    rustfs_down() { :; }
    SERVER_PID=""; WORKER_A_PID=""; WORKER_B_PID=""; INGEST_PID=""
    SERVER_LOG="$3"; SERVER_RESTART_LOG="$3"
    WORKER_A_LOG="$3"; WORKER_B_LOG="$3"; INGEST_LOG="$3"
    FIXTURE_PATH="$4"
    eval "$2"
    trap cleanup EXIT
    exit 3' _ "${CHAOS_DIR}/lib.sh" "${body}" "${kept}" "${SCRATCH}/logs/fixture.pb" \
    >/dev/null 2>&1 || true
  [[ -f "${kept}" ]] && echo yes || echo no
}
check "scenario 1's cleanup keeps the log when the scenario exits 3" "yes" \
  "$(cleanup_keeps_log_on_exit_3 "${CHAOS_DIR}/kill-ingest-flush.sh")"
check "scenario 2's cleanup keeps the logs when the scenario exits 3" "yes" \
  "$(cleanup_keeps_log_on_exit_3 "${CHAOS_DIR}/kill-maintain-worker.sh")"
check "scenario 1 gives the restarted server its own log file" "yes" \
  "$([[ "$(cat "${CHAOS_DIR}/kill-ingest-flush.sh")" == *'start_server_bg "$SERVER_LOG"'* && "$(cat "${CHAOS_DIR}/kill-ingest-flush.sh")" == *'start_server_bg "$SERVER_RESTART_LOG"'* ]] && echo yes || echo no)"

# ---------------------------------------------------------------------------
# Tenant-hash declaration: a fresh bucket refuses a server that names neither
# scheme, and the lane empties its bucket before every scenario.
# ---------------------------------------------------------------------------

# Both launchers have two branches: a binary on PATH, which is what the
# nightly takes (it puts target/release on PATH), and the `cargo run`
# fallback. ARGV_BRANCH (path | cargo) pins one in the subshell by replacing
# chaos_have_command, and replaces `cargo` with a function that prints the
# arguments after `--`, so neither branch builds or runs anything.
PIN_BRANCH='
case "${ARGV_BRANCH:?}" in
  path) chaos_have_command() { return 0; } ;;
  cargo)
    chaos_have_command() { return 1; }
    cargo() { while [[ $# -gt 0 && "$1" != "--" ]]; do shift; done; shift; printf "%s\n" "$@"; }
    ;;
esac'

# One argv element per line, from a fresh shell that sources lib.sh with the
# tenancy variables taken from the arguments (VAR=value ...) and nothing else.
server_argv() {
  env -u CHAOS_TENANT_HASH_MODE -u CHAOS_TENANT_HASH_KEY_FILE "$@" \
    bash -c 'source "$1" && eval "$2" && ravel_server_cmd --store s3 | tr "\0" "\n"' \
    _ "${CHAOS_DIR}/lib.sh" "${PIN_BRANCH}" 2>/dev/null
}
server_cmd_rc() {
  env -u CHAOS_TENANT_HASH_MODE -u CHAOS_TENANT_HASH_KEY_FILE "$@" \
    bash -c 'source "$1"; eval "$2"; rc=0; ravel_server_cmd --store s3 >/dev/null || rc=$?; echo "${rc}"' \
    _ "${CHAOS_DIR}/lib.sh" "${PIN_BRANCH}" 2>/dev/null
}
tenant_hash_flags() {
  grep -- '^--tenant-hash-' || true
}

KEY_FILE="${SCRATCH}/deployment.key"
printf '%064d' 0 > "${KEY_FILE}"
for ARGV_BRANCH in path cargo; do
  export ARGV_BRANCH
  check "server argv (${ARGV_BRANCH}): exactly one tenant-hash flag, --tenant-hash-unkeyed by default" \
    "--tenant-hash-unkeyed" "$(server_argv | tenant_hash_flags)"
  check "server argv (${ARGV_BRANCH}): the caller's own flags are kept" "--store"$'\n'"s3" \
    "$(server_argv | grep -x -A1 -- '--store')"
  check "server argv (${ARGV_BRANCH}): keyed mode emits --tenant-hash-key-file and no other tenant-hash flag" \
    "--tenant-hash-key-file" \
    "$(server_argv CHAOS_TENANT_HASH_MODE=keyed CHAOS_TENANT_HASH_KEY_FILE="${KEY_FILE}" | tenant_hash_flags)"
  check "server argv (${ARGV_BRANCH}): keyed mode passes the key file path after the flag" "${KEY_FILE}" \
    "$(server_argv CHAOS_TENANT_HASH_MODE=keyed CHAOS_TENANT_HASH_KEY_FILE="${KEY_FILE}" \
        | grep -x -A1 -- '--tenant-hash-key-file' | tail -n 1)"
  check "server argv (${ARGV_BRANCH}): an unknown mode is refused with 64" "64" \
    "$(server_cmd_rc CHAOS_TENANT_HASH_MODE=bogus)"
  check "server argv (${ARGV_BRANCH}): keyed mode with no key file is refused with 64" "64" \
    "$(server_cmd_rc CHAOS_TENANT_HASH_MODE=keyed)"
  check "server argv (${ARGV_BRANCH}): a key file under unkeyed mode is refused with 64" "64" \
    "$(server_cmd_rc CHAOS_TENANT_HASH_KEY_FILE="${KEY_FILE}")"
  check "server argv (${ARGV_BRANCH}): keyed mode with an unreadable key file is refused with 64" "64" \
    "$(server_cmd_rc CHAOS_TENANT_HASH_MODE=keyed CHAOS_TENANT_HASH_KEY_FILE="${SCRATCH}/absent.key")"
done
for s in kill-ingest-flush.sh kill-maintain-worker.sh; do
  check "${s}: an unknown tenant-hash mode is refused with 64 before anything starts" "64" \
    "$(CHAOS_TENANT_HASH_MODE=bogus rc_of bash "${CHAOS_DIR}/${s}" --check)"
done

# With the audit enabled, `--audit-text redacted` refuses an `all`-mode server
# with no tokenization key, and an unkeyed bucket has no deployment key to
# derive one from.
audit_key() {
  env -u CHAOS_TENANT_HASH_MODE -u CHAOS_TENANT_HASH_KEY_FILE -u RAVEL_AUDIT_TOKEN_KEY "$@" \
    bash -c 'source "$1" && printf "%s" "${RAVEL_AUDIT_TOKEN_KEY:-unset}"' _ "${CHAOS_DIR}/lib.sh" 2>/dev/null
}
check "audit key: unkeyed mode exports a 64-hex RAVEL_AUDIT_TOKEN_KEY" "yes" \
  "$([[ "$(audit_key)" =~ ^[0-9a-f]{64}$ ]] && echo yes || echo no)"
check "audit key: a caller's own RAVEL_AUDIT_TOKEN_KEY is kept" "caller-key" \
  "$(audit_key RAVEL_AUDIT_TOKEN_KEY=caller-key)"
check "audit key: keyed mode leaves it to the deployment key" "unset" \
  "$(audit_key CHAOS_TENANT_HASH_MODE=keyed CHAOS_TENANT_HASH_KEY_FILE="${KEY_FILE}")"
compose_audit_key="$(sed -n 's/.*RAVEL_AUDIT_TOKEN_KEY: \${RAVEL_AUDIT_TOKEN_KEY:-\([0-9a-f]\{64\}\)}.*/\1/p' \
  "${REPO_ROOT}/deploy/docker-compose/ravel.yml")"
check "audit key: unkeyed mode uses the compose file's dev-only key" "${compose_audit_key:-<no key in ravel.yml>}" \
  "$(audit_key)"
# The query audit is off by default since #2791, so the compose key is read
# only because the compose file turns the audit on beside it.
check "audit mode: the compose file's query-serving service sets RAVEL_AUDIT_MODE: required" "1" \
  "$(grep -c '^ *RAVEL_AUDIT_MODE: required$' "${REPO_ROOT}/deploy/docker-compose/ravel.yml")"

# ravel_cli takes the same tenant-hash flags as the server, ahead of the
# subcommand (they are top-level flags of ravel-cli). A stub ravel-cli on PATH
# records its argv.
CLI_STUB_DIR="$(mktemp -d)"
printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$@"\n' >"${CLI_STUB_DIR}/ravel-cli"
chmod +x "${CLI_STUB_DIR}/ravel-cli"
cli_argv() {
  env -u CHAOS_TENANT_HASH_MODE -u CHAOS_TENANT_HASH_KEY_FILE PATH="${CLI_STUB_DIR}:${PATH}" "$@" \
    bash -c 'source "$1" && eval "$2" && ravel_cli store qualify' \
    _ "${CHAOS_DIR}/lib.sh" "${PIN_BRANCH}" 2>/dev/null | paste -sd' ' -
}
for ARGV_BRANCH in path cargo; do
  export ARGV_BRANCH
  check "ravel_cli (${ARGV_BRANCH}): unkeyed puts --tenant-hash-unkeyed before the subcommand" \
    "--tenant-hash-unkeyed store qualify" "$(cli_argv)"
  check "ravel_cli (${ARGV_BRANCH}): keyed puts --tenant-hash-key-file before the subcommand" \
    "--tenant-hash-key-file ${KEY_FILE} store qualify" \
    "$(cli_argv CHAOS_TENANT_HASH_MODE=keyed CHAOS_TENANT_HASH_KEY_FILE="${KEY_FILE}")"
done

# ---------------------------------------------------------------------------
# chaos_aws picks CHAOS_AWS, then an aws on PATH, then the pinned container
# image; rustfs_up starts only the rustfs service (issue #2666). The cases run
# the real functions in a fresh bash, because the cases above replace
# chaos_aws with a stub. PATH holds only the stub directory, so no aws that
# the host happens to have can leak into the no-aws case. Each stub records
# its argv in one file and the credential variables it received in another.
# ---------------------------------------------------------------------------

AWS_SEL_SECRET="s3cr3t-do-not-leak-0123456789"
AWS_SEL_DIR="${SCRATCH}/aws-select"
mkdir -p "${AWS_SEL_DIR}/host" "${AWS_SEL_DIR}/docker-ok" "${AWS_SEL_DIR}/docker-fail" \
  "${AWS_SEL_DIR}/override"

# stub_cmd DIR NAME EXIT: DIR/NAME records argv to $REC/NAME.argv and the
# credential variables in its environment to $REC/NAME.env, then exits EXIT.
stub_cmd() {
  printf '#!%s\nprintf "%%s\\n" "$@" >>"${REC}/%s.argv"\nprintf "%%s\\n" "KEY=${AWS_ACCESS_KEY_ID-}" "SECRET=${AWS_SECRET_ACCESS_KEY-}" "IMDS=${AWS_EC2_METADATA_DISABLED-}" >>"${REC}/%s.env"\nexit %s\n' \
    "${BASH}" "$2" "$2" "$3" >"$1/$2"
  chmod +x "$1/$2"
}
stub_cmd "${AWS_SEL_DIR}/host" aws 0
stub_cmd "${AWS_SEL_DIR}/override" my-aws 0
stub_cmd "${AWS_SEL_DIR}/docker-ok" docker 0
stub_cmd "${AWS_SEL_DIR}/docker-fail" docker 97

# sel_run PATHDIRS... -- ARGS...: chaos_aws ARGS in a fresh shell whose PATH is
# PATHDIRS, with a fresh record directory in $REC (also printed as REC_DIR).
# CHAOS_AWS is whatever the caller exports.
sel_run() {
  local dirs="" a
  for a in "$@"; do
    shift
    [[ "${a}" == "--" ]] && break
    dirs="${dirs:+${dirs}:}${a}"
  done
  REC_DIR="$(mktemp -d "${SCRATCH}/rec.XXXXXX")"
  REC="${REC_DIR}" PATH="${dirs}" RAVEL_S3_SECRET_KEY="${AWS_SEL_SECRET}" \
    "${BASH}" -c 'source "$1" && shift && chaos_aws "$@"' _ "${CHAOS_DIR}/lib.sh" "$@" \
    >/dev/null 2>&1
}
rec_lines() { # file: its lines joined by spaces, or "none" when never written
  if [[ -f "$1" ]]; then paste -sd' ' - <"$1"; else echo none; fi
}
no_secret_in() { # file...: yes when the secret value is in none of them
  if cat "$@" 2>/dev/null | grep -qF "${AWS_SEL_SECRET}"; then echo no; else echo yes; fi
}

# A host aws on PATH wins, and docker (stubbed to exit 97) is never invoked.
sel_run "${AWS_SEL_DIR}/host" "${AWS_SEL_DIR}/docker-fail" -- s3 ls s3://b/
check "chaos_aws: an aws on PATH runs with --endpoint-url and the arguments" \
  "--endpoint-url ${CHAOS_RUSTFS_ENDPOINT} s3 ls s3://b/" "$(rec_lines "${REC_DIR}/aws.argv")"
check "chaos_aws: an aws on PATH leaves docker uninvoked" "none" "$(rec_lines "${REC_DIR}/docker.argv")"
check "chaos_aws: the host aws gets the credentials and IMDS off in its environment" \
  "KEY=${RAVEL_S3_ACCESS_KEY} SECRET=${AWS_SEL_SECRET} IMDS=true" "$(rec_lines "${REC_DIR}/aws.env")"
check "chaos_aws: the host aws argv carries no credential" "yes" "$(no_secret_in "${REC_DIR}/aws.argv")"

# CHAOS_AWS names the binary, ahead of an aws on PATH.
CHAOS_AWS="${AWS_SEL_DIR}/override/my-aws" sel_run "${AWS_SEL_DIR}/host" "${AWS_SEL_DIR}/docker-fail" -- s3 ls
check "chaos_aws: CHAOS_AWS runs the named binary with --endpoint-url" \
  "--endpoint-url ${CHAOS_RUSTFS_ENDPOINT} s3 ls" "$(rec_lines "${REC_DIR}/my-aws.argv")"
check "chaos_aws: CHAOS_AWS takes precedence over an aws on PATH" "none" "$(rec_lines "${REC_DIR}/aws.argv")"
check "chaos_aws: CHAOS_AWS leaves docker uninvoked" "none" "$(rec_lines "${REC_DIR}/docker.argv")"
CHAOS_AWS="${AWS_SEL_DIR}/override/my-aws" sel_run "${AWS_SEL_DIR}/docker-fail" -- s3 ls
check "chaos_aws: CHAOS_AWS works with no aws on PATH" \
  "--endpoint-url ${CHAOS_RUSTFS_ENDPOINT} s3 ls" "$(rec_lines "${REC_DIR}/my-aws.argv")"

# A CHAOS_AWS that names nothing executable fails the call with 127 rather
# than falling back to the container, and --check warns about it.
check "chaos_aws: an unresolvable CHAOS_AWS exits 127" "127" \
  "$(REC="$(mktemp -d "${SCRATCH}/rec.XXXXXX")" CHAOS_AWS="${AWS_SEL_DIR}/override/no-such-aws" PATH="${AWS_SEL_DIR}/docker-ok" "${BASH}" -c 'source "$1"; chaos_aws s3 ls; echo $?' _ "${CHAOS_DIR}/lib.sh" 2>/dev/null)"
# Nit (#3037): the 127 case must say why, on stderr, naming the unresolvable
# path, not merely exit with a bare code a caller has to look up.
check "chaos_aws: an unresolvable CHAOS_AWS prints which path on stderr" \
  "chaos_aws: CHAOS_AWS=${AWS_SEL_DIR}/override/no-such-aws is not an executable" \
  "$(CHAOS_AWS="${AWS_SEL_DIR}/override/no-such-aws" "${BASH}" -c 'source "$1"; chaos_aws s3 ls' _ "${CHAOS_DIR}/lib.sh" 2>&1 >/dev/null)"
CHAOS_AWS="${AWS_SEL_DIR}/override/no-such-aws" sel_run "${AWS_SEL_DIR}/docker-ok" -- s3 ls
check "chaos_aws: an unresolvable CHAOS_AWS leaves docker uninvoked" "none" "$(rec_lines "${REC_DIR}/docker.argv")"

# The caller's own session token, profile and region do not reach the host
# aws: a stale AWS_SESSION_TOKEN signed with the chaos key makes RustFS 403.
mkdir -p "${AWS_SEL_DIR}/host-ambient"
printf '#!%s\nprintf "%%s\\n" "TOKEN=${AWS_SESSION_TOKEN-unset}" "PROFILE=${AWS_PROFILE-unset}" "REGION=${AWS_REGION-unset}" >>"${REC}/aws.ambient"\n' \
  "${BASH}" >"${AWS_SEL_DIR}/host-ambient/aws"
chmod +x "${AWS_SEL_DIR}/host-ambient/aws"
AWS_SESSION_TOKEN=stale-token AWS_PROFILE=someone AWS_REGION=eu-west-9 \
  sel_run "${AWS_SEL_DIR}/host-ambient" -- s3 ls
check "chaos_aws: the caller's session token, profile and region do not reach the host aws" \
  "TOKEN=unset PROFILE=unset REGION=unset" "$(rec_lines "${REC_DIR}/aws.ambient")"

# No aws anywhere: the pinned image under docker, credentials by name only.
sel_run "${AWS_SEL_DIR}/docker-ok" -- s3api create-bucket --bucket b
check "chaos_aws: with no aws, docker runs the pinned image then --endpoint-url and the arguments" \
  "run --rm --network host -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION -e AWS_EC2_METADATA_DISABLED ${CHAOS_AWS_CLI_IMAGE} --endpoint-url ${CHAOS_RUSTFS_ENDPOINT} s3api create-bucket --bucket b" \
  "$(rec_lines "${REC_DIR}/docker.argv")"
check "chaos_aws: the docker argv carries no credential" "yes" "$(no_secret_in "${REC_DIR}/docker.argv")"
check "chaos_aws: docker gets the credentials and IMDS off in its environment" \
  "KEY=${RAVEL_S3_ACCESS_KEY} SECRET=${AWS_SEL_SECRET} IMDS=true" "$(rec_lines "${REC_DIR}/docker.env")"
check "chaos_aws: the exit code of the container path is the command's" "97" \
  "$(REC="$(mktemp -d "${SCRATCH}/rec.XXXXXX")" PATH="${AWS_SEL_DIR}/docker-fail" "${BASH}" -c 'source "$1"; chaos_aws s3 ls; echo $?' _ "${CHAOS_DIR}/lib.sh" 2>/dev/null)"

# --check names the client the run will use.
dep_line() { # PATHDIRS: the "aws CLI:" line of check_dependencies
  PATH="$1" "${BASH}" -c 'source "$1"; check_dependencies' _ "${CHAOS_DIR}/lib.sh" 2>/dev/null |
    grep -F 'aws CLI:' | sed 's/^ *OK *//'
}
check "check_dependencies: names the host aws when one is on PATH" \
  "aws CLI: host (${AWS_SEL_DIR}/host/aws)" "$(dep_line "${AWS_SEL_DIR}/host")"
check "check_dependencies: names the container image when only docker is" \
  "aws CLI: container ${CHAOS_AWS_CLI_IMAGE}" "$(dep_line "${AWS_SEL_DIR}/docker-ok")"
check "check_dependencies: warns when CHAOS_AWS names nothing executable" \
  "  WARN  aws CLI: CHAOS_AWS=${AWS_SEL_DIR}/override/no-such-aws is not an executable" \
  "$(CHAOS_AWS="${AWS_SEL_DIR}/override/no-such-aws" dep_line "${AWS_SEL_DIR}/docker-ok")"

# rustfs_up names the rustfs service, so compose does not start createbucket
# (which pulls the AWS CLI image from ECR Public). The stubbed docker records
# its argv; the health probe fails once so the compose branch runs.
RUSTFS_UP_REC="$(mktemp -d "${SCRATCH}/rec.XXXXXX")"
REC="${RUSTFS_UP_REC}" PATH="${AWS_SEL_DIR}/docker-ok" "${BASH}" -c '
  source "$1"
  log() { :; }
  run_capture() { :; }
  chaos_aws() { :; }
  probes=0
  chaos_rustfs_healthy() { probes=$((probes + 1)); [[ "${probes}" -gt 1 ]]; }
  rustfs_up' _ "${CHAOS_DIR}/lib.sh" >/dev/null 2>&1
check "rustfs_up: compose is told to start only the rustfs service" \
  "compose -f ${CHAOS_RUSTFS_COMPOSE} up -d rustfs" "$(rec_lines "${RUSTFS_UP_REC}/docker.argv")"
check "rustfs_up: the compose file names a rustfs service" "1" \
  "$(grep -c '^  rustfs:$' "${REPO_ROOT}/${CHAOS_RUSTFS_COMPOSE}")"

# Nit (#3037): the container aws is pinned to CHAOS_AWS_CLI_IMAGE, but a host
# aws is whatever version happens to be on the runner; rustfs_up logs it once
# so a version-specific aws behavior is diagnosable from the run's own log.
RUSTFS_UP_VERSION_REC="$(mktemp -d "${SCRATCH}/rec.XXXXXX")"
REC="${RUSTFS_UP_VERSION_REC}" PATH="${AWS_SEL_DIR}/host:${AWS_SEL_DIR}/docker-ok" "${BASH}" -c '
  source "$1"
  log() { printf "%s\n" "$*" >>"${REC}/log.txt"; }
  run_capture() { :; }
  chaos_aws() { :; }
  probes=0
  chaos_rustfs_healthy() { probes=$((probes + 1)); [[ "${probes}" -gt 1 ]]; }
  rustfs_up' _ "${CHAOS_DIR}/lib.sh" >/dev/null 2>&1
check "rustfs_up: logs the host aws CLI version exactly once when a host aws is resolvable" "1" \
  "$(grep -c 'host aws CLI version' "${RUSTFS_UP_VERSION_REC}/log.txt")"

RUSTFS_UP_NOHOST_REC="$(mktemp -d "${SCRATCH}/rec.XXXXXX")"
REC="${RUSTFS_UP_NOHOST_REC}" PATH="${AWS_SEL_DIR}/docker-ok" "${BASH}" -c '
  source "$1"
  log() { printf "%s\n" "$*" >>"${REC}/log.txt"; }
  run_capture() { :; }
  chaos_aws() { :; }
  probes=0
  chaos_rustfs_healthy() { probes=$((probes + 1)); [[ "${probes}" -gt 1 ]]; }
  rustfs_up' _ "${CHAOS_DIR}/lib.sh" >/dev/null 2>&1
check "rustfs_up: logs no host aws CLI version when no host aws resolves (container fallback)" "0" \
  "$(grep -c 'host aws CLI version' "${RUSTFS_UP_NOHOST_REC}/log.txt")"

printf '\n%d passed, %d failed\n' "${PASSED}" "${FAILED}"
[[ "${FAILED}" -eq 0 ]]
