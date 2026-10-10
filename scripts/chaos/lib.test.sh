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
check "scenario 1 sends the SIGKILL on the line after the detecting poll" "yes" \
  "$([[ -n "${detect_line}" && "${first_kill_line:-0}" -eq $(( detect_line + 2 )) ]] && echo yes || echo no)"
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

# Modelled on tracing's default fmt output for publish.rs:331 and the pass
# line in services/ravel-server/src/maintain.rs.
PUB_LINE='2026-10-10T05:30:00Z  INFO ravel_maintain::publish: compaction record published key=k parts=1'
pass_line() { printf '2026-10-10T05:30:0%sZ  INFO ravel_server::maintain: maintenance: retention + compaction pass complete tenant=ab signal=Metrics shard=%s retired=0 compacted=%s already_done=0 not_sealed=0\n' "$1" "$1" "$2"; }
LOG_PUBLISHED="${SCRATCH}/published.log"
LOG_SILENT="${SCRATCH}/silent.log"
LOG_A_FINISHED="${SCRATCH}/a-finished.log"
LOG_A_UNIT2="${SCRATCH}/a-unit2.log"
printf '%s\n' "${PUB_LINE}" >"${LOG_PUBLISHED}"
printf '2026-10-10T05:30:00Z  INFO ravel_maintain: maintain pass compacted=0\n' >"${LOG_SILENT}"
# A published every unit it started: the pass ended after its publish.
{ printf '%s\n' "${PUB_LINE}"; pass_line 0 1; pass_line 1 0; } >"${LOG_A_FINISHED}"
# A published unit 1 and was still merging unit 2 at the kill: its pass had
# not ended, so no pass line follows the publish.
{ printf '%s\n' "${PUB_LINE}"; printf '2026-10-10T05:30:01Z  INFO ravel_server: other work\n'; } >"${LOG_A_UNIT2}"
LOG_A_LATER_PUBLISH="${SCRATCH}/a-later-publish.log"
{ printf '%s\n' "${PUB_LINE}"; pass_line 0 1; printf '%s\n' "${PUB_LINE}"; } >"${LOG_A_LATER_PUBLISH}"

unfinished() { rc_of compaction_unfinished_in_log "$1"; }
check "unfinished: no publish at all" "0" "$(unfinished "$(cat "${LOG_SILENT}")")"
check "unfinished: a publish with no pass line after it" "0" "$(unfinished "${PUB_LINE}")"
check "unfinished: a publish followed by a compacted>=1 pass line is finished" "1" \
  "$(unfinished "$(cat "${LOG_A_FINISHED}")")"
check "unfinished: a compacted=0 pass line after the publish does not finish it" "0" \
  "$(unfinished "${PUB_LINE}"$'\n'"$(pass_line 0 0)")"
check "unfinished: a publish with its pass still running" "0" \
  "$(unfinished "$(cat "${LOG_A_UNIT2}")")"
check "unfinished: a publish after the last finished pass" "0" \
  "$(unfinished "$(cat "${LOG_A_LATER_PUBLISH}")")"
check "unfinished: already_done=1 is not compacted=1" "0" \
  "$(unfinished "${PUB_LINE}"$'\n''x maintenance: retention + compaction pass complete compacted=0 already_done=1')"
check "unfinished: pass line read through ANSI colouring" "1" \
  "$(unfinished "${PUB_LINE}"$'\n\e[32m INFO\e[0m maintenance: retention + compaction pass complete \e[3mcompacted\e[0m\e[2m=\e[0m2')"

# Args: A's log, B's log, aborts baseline.
cons() { rc_of oracle_conservation_or_unmeasured "$1" http://stub "${3:-1}" "$2"; }
check "classifier: A finished its pass, B silent is could-not-measure (3)" "3" \
  "$(cons "${LOG_A_FINISHED}" "${LOG_SILENT}")"
check "classifier: B published passes conservation" "0" \
  "$(cons "${LOG_SILENT}" "${LOG_PUBLISHED}")"
check "classifier: nothing published fails conservation" "1" \
  "$(cons "${LOG_SILENT}" "${LOG_SILENT}")"
check "classifier: A published with no pass end, B silent fails conservation" "1" \
  "$(cons "${LOG_PUBLISHED}" "${LOG_SILENT}")"
check "classifier: A published unit 1, started unit 2, B silent fails conservation" "1" \
  "$(cons "${LOG_A_UNIT2}" "${LOG_SILENT}")"
check "classifier: B aborts rose (1 against 0), A finished, B silent fails" "1" \
  "$(cons "${LOG_A_FINISHED}" "${LOG_SILENT}" 0)"
check "classifier: B aborts rose (1 against 0), B published still fails" "1" \
  "$(cons "${LOG_SILENT}" "${LOG_PUBLISHED}" 0)"
check "classifier: unreadable aborts counter fails even when A finished" "1" \
  "$(CURL_MODE=unreachable cons "${LOG_A_FINISHED}" "${LOG_SILENT}")"
scenario2_summary() {
  # Args: A's log, B's log, aborts baseline, an extra failure or "".
  ORACLE_PASS=(other)
  ORACLE_FAIL=()
  ORACLE_UNMEASURED=()
  [[ -n "$4" ]] && ORACLE_FAIL=("$4")
  oracle_conservation_or_unmeasured "$1" http://stub "$3" "$2" >/dev/null 2>&1
  local rc=0
  print_oracle_summary scenario blocking >"${SCRATCH}/summary.out" 2>&1 || rc=$?
  printf 'rc=%s conservation-fail=%s\n' "${rc}" \
    "$(grep -c '^  FAIL  conservation-holds' "${SCRATCH}/summary.out")"
}
check "summary: could-not-measure with nothing failed exits 3" "rc=3 conservation-fail=0" \
  "$(scenario2_summary "${LOG_A_FINISHED}" "${LOG_SILENT}" 1 "")"
check "summary: could-not-measure never hides a failure (exit 2)" "rc=2 conservation-fail=0" \
  "$(scenario2_summary "${LOG_A_FINISHED}" "${LOG_SILENT}" 1 "no-orphaned-lease: detail")"
check "summary: B aborts rose while A finished and published is exit 2, conservation FAIL" \
  "rc=2 conservation-fail=1" "$(scenario2_summary "${LOG_A_FINISHED}" "${LOG_SILENT}" 0 "")"
check "summary: A started unit 2, B published nothing is exit 2" "rc=2 conservation-fail=1" \
  "$(scenario2_summary "${LOG_A_UNIT2}" "${LOG_SILENT}" 1 "")"
check "summary: A published every unit it started, B nothing is exit 3" "rc=3 conservation-fail=0" \
  "$(scenario2_summary "${LOG_A_FINISHED}" "${LOG_SILENT}" 1 "")"

scenario2_body="$(cat "${CHAOS_DIR}/kill-maintain-worker.sh")"
universe_line="$(scenario2_line 'assert_single_tenant_universe || true')"
inflight_wait_line="$(scenario2_line 'if wait_for_compaction_in_flight')"
first_a_kill_line="$(scenario2_line 'sigkill_pid "$WORKER_A_PID"')"
check "scenario 2 checks the universe before the wait, and kills right after it" "yes" \
  "$([[ -n "${universe_line}" && "${universe_line}" -lt "${inflight_wait_line:-0}" && "${first_a_kill_line:-0}" -eq $(( inflight_wait_line + 2 )) ]] && echo yes || echo no)"
check "scenario 2 headers make no 'provably' claim" "no" \
  "$([[ "${scenario2_body}" == *provabl* ]] && echo yes || echo no)"

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

# `--audit-text redacted` refuses an `all`-mode server with no tokenization
# key, and an unkeyed bucket has no deployment key to derive one from.
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

printf '\n%d passed, %d failed\n' "${PASSED}" "${FAILED}"
[[ "${FAILED}" -eq 0 ]]
