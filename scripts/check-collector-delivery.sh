#!/usr/bin/env bash
# Assert that the quickstart's OpenTelemetry Collector is actually delivering to
# Ravel, by reading the collector's own counters rather than inferring delivery
# from query results (#209).
#
# Why this exists. The `quickstart` CI job runs telemetrygen beside the
# collector and polls readiness on telemetrygen's series. That made a total
# collector failure invisible: the collector defaulted to gzip, Ravel's OTLP
# HTTP ingest rejected every export with HTTP 400 (#115), and the job stayed
# green while the quickstart's Grafana dashboard was empty from the day it
# shipped. Nothing asserted the collector's own delivery, so nothing failed.
#
# Querying for a specific series cannot fix that. ADR-0081 decision 5 is right
# that no `hostmetrics` series name is assertable across a CI runner and a
# developer's laptop. The collector's counters are, because they are about the
# collector rather than about what it happened to scrape.
#
# Assertions:
#   1. otelcol_exporter_sent_metric_points  > 0   (it delivered something)
#   2. otelcol_exporter_send_failed_metric_points == 0   (nothing was dropped)
#
# The first alone would pass a collector that delivers one point and drops a
# thousand. The second alone would pass a collector that is scraping nothing.
#
# Shell-safety (CLAUDE.md "Writing gate and poll shell"): exit codes are
# captured as `cmd || code=$?` on the same line; no variable is named `status`,
# `path`, `argv`, or `PWD`; no checked command is piped through grep/head/tail.
set -euo pipefail

COLLECTOR_METRICS_URL="${RAVEL_COLLECTOR_METRICS_URL:-http://127.0.0.1:18888/metrics}"
# The collector exports on a batch timer, so a scrape immediately after start-up
# legitimately shows zero sent points. Poll instead of sleeping a guessed
# duration, and fail on the budget rather than on the first empty read.
DELIVERY_TIMEOUT_SECONDS="${RAVEL_COLLECTOR_TIMEOUT_SECONDS:-120}"
DELIVERY_POLL_SECONDS="${RAVEL_COLLECTOR_POLL_SECONDS:-3}"
CONNECT_TIMEOUT_SECONDS="${RAVEL_CONNECT_TIMEOUT_SECONDS:-5}"
REQUEST_TIMEOUT_SECONDS="${RAVEL_REQUEST_TIMEOUT_SECONDS:-10}"

log() { echo "[collector-delivery] $*" >&2; }

fail() {
  log "FAILED: $*"
  exit 1
}

# Sum every sample of a counter family, ignoring its labels. The collector
# reports one series per exporter, so a deployment with more than one exporter
# must not have its first series mistaken for the total.
metric_total() {
  local body="$1" name="$2"
  # `python3 -c`, not `python3 -` with a heredoc: the heredoc would claim stdin,
  # leaving no way to pipe the metrics body in.
  printf '%s' "$body" | python3 -c '
import sys
name = sys.argv[1]
total = 0.0
found = False
for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    if not (line.startswith(name + " ") or line.startswith(name + "{")):
        continue
    value = line.rsplit(" ", 1)[-1]
    try:
        total += float(value)
    except ValueError:
        continue
    found = True
print(f"{total:.0f}" if found else "absent")
' "$name"
}

log "reading collector counters from $COLLECTOR_METRICS_URL"

deadline=$(( SECONDS + DELIVERY_TIMEOUT_SECONDS ))
sent="absent"
failed="absent"
body=""

while (( SECONDS < deadline )); do
  code=0
  body=$(curl --silent --show-error \
    --connect-timeout "$CONNECT_TIMEOUT_SECONDS" --max-time "$REQUEST_TIMEOUT_SECONDS" \
    "$COLLECTOR_METRICS_URL") || code=$?
  if (( code == 0 )); then
    sent=$(metric_total "$body" otelcol_exporter_sent_metric_points)
    failed=$(metric_total "$body" otelcol_exporter_send_failed_metric_points)
    # A drop is already a verdict. Stop polling rather than waiting out the
    # whole budget for a `sent` count that a rejecting server will never
    # produce: under a total rejection `sent` stays 0 forever, and burning the
    # full timeout only delays a failure that is already known.
    if [[ "$failed" != "absent" && "$failed" != "0" ]]; then
      break
    fi
    if [[ "$sent" != "absent" && "$sent" != "0" ]]; then
      break
    fi
  fi
  sleep "$DELIVERY_POLL_SECONDS"
done

# Check the drop count before the sent count. Under a total rejection both are
# wrong, and "Ravel refused these exports" is the diagnosis, while "nothing was
# delivered" is only the symptom.
if [[ "$failed" != "absent" && "$failed" != "0" ]]; then
  fail "the collector dropped $failed metric points (delivered $sent)." \
    "Ravel is rejecting its exports. Read the HTTP status it received with:" \
    "docker compose -f deploy/docker-compose/ravel.yml logs otel-collector"
fi

if [[ "$sent" == "absent" ]]; then
  fail "the collector never reported otelcol_exporter_sent_metric_points." \
    "Either its metrics endpoint is unreachable at $COLLECTOR_METRICS_URL," \
    "or the collector is not running at all."
fi

if [[ "$sent" == "0" ]]; then
  fail "the collector delivered 0 metric points within ${DELIVERY_TIMEOUT_SECONDS}s." \
    "It is running but nothing reached Ravel."
fi

# The collector omits the failure counter entirely until something fails, so
# "absent" is the healthy reading and means zero.
if [[ "$failed" == "absent" ]]; then
  failed=0
fi

log "PASSED: the collector delivered $sent metric points, $failed dropped."
