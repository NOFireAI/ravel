#!/usr/bin/env bash
# Run the marked runnable command blocks in a markdown file against an
# ALREADY-RUNNING stack and assert each block's documented outcome. This is the
# executable side of ADR-0081 decision 5.
#
# This script does NOT bring up the stack. Bringing MinIO, ravel-server, the
# collector, and Grafana up is the compose file's job and the CI job's job
# (ticket #175); this script assumes they are already reachable and only checks
# that the README's commands do what the README says.
#
# Usage:
#   scripts/check-readme-commands.sh <markdown-path>   # wait for readiness, run blocks
#   scripts/check-readme-commands.sh --wait            # readiness wait only (for #175's CI)
#
# Marker convention and expectation vocabulary are documented in the header of
# scripts/check_readme_commands.py, the pure logic this script drives. In short:
# a block is runnable when the line immediately above its opening fence is
#   <!-- ravel:run <expectation>[; <expectation> ...] -->
# with expectations drawn from: status=<code>, json:<path>=<value>,
# nonempty:<path>. An unparseable marker, a marker with no expectation, or an
# unknown keyword is a hard error, never a skip.
#
# Outcome, not exit code: `curl` exits 0 on an HTTP 401 and on a
# `{"status":"error"}` JSON envelope, so this script asserts the declared
# expectation (parsed and evaluated by check_readme_commands.py) rather than
# trusting the process exit status.
#
# Shell-safety (CLAUDE.md "Writing gate and poll shell"): exit codes are
# captured as `cmd || code=$?` on the same line; no variable is named `status`,
# `path`, `argv`, or `PWD`; no checked command is piped through grep/head/tail.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
py_module="$here/check_readme_commands.py"

# --- Configuration (env overrides, documented defaults) --------------------
# The compose stack from ADR-0081 binds loopback; #175 wires the real values.
RAVEL_HTTP_BASE=${RAVEL_HTTP_BASE:-http://127.0.0.1:4318}
RAVEL_TENANT_TOKEN=${RAVEL_TENANT_TOKEN:-demo-token}
RAVEL_HEALTH_PATH=${RAVEL_HEALTH_PATH:-/health}
# Query the readiness poll uses to decide the stack has data. It must be an
# outcome both the collector and telemetrygen produce (ADR-0081 D5); the
# default is a shape-only instant query whose result set is non-empty once any
# series exists.
RAVEL_READY_QUERY_URL=${RAVEL_READY_QUERY_URL:-$RAVEL_HTTP_BASE/api/v1/query?query=up}

# Readiness timeout is a NAMED CONSTANT, never a fixed sleep (ADR-0081 D5).
READINESS_TIMEOUT_SECONDS=${RAVEL_READINESS_TIMEOUT_SECONDS:-120}
READINESS_POLL_SECONDS=${RAVEL_READINESS_POLL_SECONDS:-2}

# --- Readiness ------------------------------------------------------------
# Poll health, then poll a first query until it returns a non-empty result,
# bounded by READINESS_TIMEOUT_SECONDS. Callable independently via `--wait` so
# #175's CI job can gate on it before running the blocks.
wait_for_ready() {
  local health_url="$RAVEL_HTTP_BASE$RAVEL_HEALTH_PATH"
  local deadline=$(( $(date +%s) + READINESS_TIMEOUT_SECONDS ))

  echo "readiness: waiting for health at $health_url"
  while true; do
    local http_code=000
    local curl_rc=0
    http_code=$(curl -s -o /dev/null -w '%{http_code}' "$health_url") || curl_rc=$?
    if [[ $curl_rc -eq 0 && $http_code == "200" ]]; then
      break
    fi
    if (( $(date +%s) >= deadline )); then
      echo "readiness: health not 200 within ${READINESS_TIMEOUT_SECONDS}s (last=$http_code)" >&2
      return 1
    fi
    sleep "$READINESS_POLL_SECONDS"
  done

  echo "readiness: waiting for first non-empty query at $RAVEL_READY_QUERY_URL"
  while true; do
    local body=""
    body=$(curl -s -H "Authorization: Bearer $RAVEL_TENANT_TOKEN" "$RAVEL_READY_QUERY_URL") || body=""
    local eval_rc=0
    printf '%s' "$body" | python3 "$py_module" evaluate 'nonempty:.data.result' \
      --exit 0 --http 200 >/dev/null 2>&1 || eval_rc=$?
    if [[ $eval_rc -eq 0 ]]; then
      break
    fi
    if (( $(date +%s) >= deadline )); then
      echo "readiness: query returned no data within ${READINESS_TIMEOUT_SECONDS}s" >&2
      return 1
    fi
    sleep "$READINESS_POLL_SECONDS"
  done

  echo "readiness: stack healthy and first query non-empty"
}

# --- Running one command --------------------------------------------------
# Runs `cmd`, writes its stdout to `body_file`, and prints two space-separated
# tokens on one line: the captured HTTP status (or "none") and the command's
# own exit code. For a curl command it appends a write-out directive so the
# HTTP status is captured even when curl exits 0 on a 4xx/5xx; the sentinel
# line is stripped from the body before evaluation.
run_one_command() {
  local cmd=$1
  local body_file=$2
  local run_code=0
  local captured="none"

  if [[ $cmd == curl* ]]; then
    bash -c "$cmd --silent --write-out '\nRAVEL_HTTP_STATUS=%{http_code}'" \
      >"$body_file" 2>/dev/null || run_code=$?
    local line
    line=$(sed -n 's/^RAVEL_HTTP_STATUS=//p' "$body_file")
    [[ -n $line ]] && captured=$line
    local cleaned
    cleaned=$(sed '/^RAVEL_HTTP_STATUS=/d' "$body_file")
    printf '%s' "$cleaned" >"$body_file"
  else
    bash -c "$cmd" >"$body_file" 2>/dev/null || run_code=$?
  fi
  printf '%s %s' "$captured" "$run_code"
}

# --- Running all marked blocks --------------------------------------------
run_blocks() {
  local md=$1
  local found=0
  local line_no exp_raw cmd_b64

  while IFS= read -r -d '' line_no \
     && IFS= read -r -d '' exp_raw \
     && IFS= read -r -d '' cmd_b64; do
    found=1
    local cmd
    cmd=$(printf '%s' "$cmd_b64" | base64 -d)

    echo "----------------------------------------------------------------"
    echo "block ${md}:${line_no}  expect: ${exp_raw}"
    printf 'command:\n%s\n' "$cmd"

    local body_file
    body_file=$(mktemp)
    local run_out http_status run_code
    run_out=$(run_one_command "$cmd" "$body_file")
    http_status=${run_out%% *}
    run_code=${run_out##* }
    echo "exit=${run_code} http=${http_status}"

    local eval_rc=0
    python3 "$py_module" evaluate "$exp_raw" \
      --exit "$run_code" --http "$http_status" <"$body_file" || eval_rc=$?
    rm -f "$body_file"

    if [[ $eval_rc -ne 0 ]]; then
      echo "FAIL: block at ${md}:${line_no} did not meet its expectation" >&2
      return 1
    fi
    echo "PASS: ${md}:${line_no}"
  done < <(python3 "$py_module" extract "$md")

  if [[ $found -eq 0 ]]; then
    echo "no marked (ravel:run) blocks found in $md" >&2
    return 1
  fi
  echo "================================================================"
  echo "all marked blocks in $md passed"
}

usage() {
  echo "usage: $0 [--wait] <markdown-path>" >&2
}

main() {
  local md=""
  local wait_only=0
  while [[ $# -gt 0 ]]; do
    case $1 in
      --wait) wait_only=1; shift ;;
      -h|--help) usage; exit 0 ;;
      --) shift; break ;;
      -*) echo "unknown option: $1" >&2; usage; exit 2 ;;
      *) md=$1; shift ;;
    esac
  done
  if [[ $# -gt 0 && -z $md ]]; then
    md=$1
  fi

  if [[ $wait_only -eq 1 ]]; then
    local rc=0
    wait_for_ready || rc=$?
    exit $rc
  fi

  if [[ -z $md ]]; then
    usage
    exit 2
  fi
  if [[ ! -r $md ]]; then
    echo "cannot read markdown file: $md" >&2
    exit 2
  fi

  wait_for_ready
  run_blocks "$md"
}

main "$@"
