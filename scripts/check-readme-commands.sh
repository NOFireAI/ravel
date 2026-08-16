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
# Liveness route. ravel-server registers /healthz, /readyz, /-/healthy, and
# /-/ready (services/ravel-server/src/health.rs); /healthz returns 200 as soon
# as the axum server can route, which is what the readiness wait needs. There is
# no /health route, so it is not a usable default.
RAVEL_HEALTH_PATH=${RAVEL_HEALTH_PATH:-/healthz}
# Query the readiness poll uses to decide the stack has data. It must return a
# non-empty result set for an outcome both the collector and telemetrygen
# produce (ADR-0081 D5). There is NO usable default: ravel-server has no scrape
# loop and never synthesizes a `up` series, the collector's hostmetrics receiver
# emits `system_*`, and telemetrygen emits `gen*`, so no single series name is
# non-empty across both generators. The caller (#175's CI job, wiring the real
# compose stack) MUST supply this explicitly; an empty value is a hard error
# when the readiness poll runs, never a silent skip.
RAVEL_READY_QUERY_URL=${RAVEL_READY_QUERY_URL:-}

# Readiness timeout is a NAMED CONSTANT, never a fixed sleep (ADR-0081 D5).
READINESS_TIMEOUT_SECONDS=${RAVEL_READINESS_TIMEOUT_SECONDS:-120}
READINESS_POLL_SECONDS=${RAVEL_READINESS_POLL_SECONDS:-2}
# Per-request bounds (ADR-0081 D5: a bounded poll, not just a bounded loop). A
# server that accepts a connection and never responds must not hang a single
# request past these, so the overall READINESS_TIMEOUT_SECONDS budget stays the
# real ceiling. These apply to every curl this script issues, including the ones
# that run marked README blocks: a hung server otherwise hangs the whole CI job,
# not just the readiness phase. Timing out is a failure, never a pass.
CONNECT_TIMEOUT_SECONDS=${RAVEL_CONNECT_TIMEOUT_SECONDS:-5}
REQUEST_TIMEOUT_SECONDS=${RAVEL_REQUEST_TIMEOUT_SECONDS:-10}

# Real curl path and the on-PATH shim dir that fronts it while a marked block
# runs. Resolved once, before any shim is on PATH. See setup_curl_shim.
REAL_CURL=$(command -v curl || true)
CURL_SHIM_DIR=""

# --- Readiness ------------------------------------------------------------
# Poll health, then poll a first query until it returns a non-empty result,
# bounded by READINESS_TIMEOUT_SECONDS. Callable independently via `--wait` so
# #175's CI job can gate on it before running the blocks.
wait_for_ready() {
  local health_url="$RAVEL_HTTP_BASE$RAVEL_HEALTH_PATH"
  local deadline=$(( $(date +%s) + READINESS_TIMEOUT_SECONDS ))

  if [[ -z $RAVEL_READY_QUERY_URL ]]; then
    echo "readiness: RAVEL_READY_QUERY_URL is unset; it has no working default" \
         "(no series is non-empty across both the collector and telemetrygen)." \
         "Set it to a query the running stack answers non-empty." >&2
    return 1
  fi

  echo "readiness: waiting for health at $health_url"
  while true; do
    local http_code=000
    local curl_rc=0
    http_code=$(curl -s -o /dev/null -w '%{http_code}' \
      --connect-timeout "$CONNECT_TIMEOUT_SECONDS" \
      --max-time "$REQUEST_TIMEOUT_SECONDS" "$health_url") || curl_rc=$?
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
    body=$(curl -s -H "Authorization: Bearer $RAVEL_TENANT_TOKEN" \
      --connect-timeout "$CONNECT_TIMEOUT_SECONDS" \
      --max-time "$REQUEST_TIMEOUT_SECONDS" "$RAVEL_READY_QUERY_URL") || body=""
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

# --- The curl shim --------------------------------------------------------
# Capturing the HTTP status must be OUT OF BAND: it may never be recoverable
# from the response body (the body is produced by the very thing being gated, so
# any status parsed out of it is forgeable), and appending flags to the tail of
# an arbitrary command string is unsafe (a trailing `#`, `;`, or `|` in a README
# line detaches them). Instead we front the real curl with a shim on PATH while
# a marked block runs. The block command is executed verbatim; when it invokes
# `curl` the shim receives the arguments as real argv, injects the timeouts and
# a `%{http_code}` write-out, sends the body to its own stdout, and writes the
# status to RAVEL_STATUS_FILE. Because the injected options are argv inside the
# shim rather than text appended to the command string, they survive a trailing
# `#` (the shim never sees it), a `;` (the shell runs `curl ...` as its own
# simple command, which resolves to the shim), and a `|` (curl is one stage of
# the pipeline; the shim still runs and still writes the status file). A 000
# code (connection refused, or a --max-time timeout with no response) is written
# as-is, so a timeout fails a `status=` assertion rather than passing it.
setup_curl_shim() {
  [[ -n $CURL_SHIM_DIR ]] && return 0
  if [[ -z $REAL_CURL ]]; then
    echo "curl not found on PATH" >&2
    return 1
  fi
  CURL_SHIM_DIR=$(mktemp -d)
  cat >"$CURL_SHIM_DIR/curl" <<'SHIM'
#!/usr/bin/env bash
# On-PATH shim for scripts/check-readme-commands.sh. Injects request timeouts
# and captures the HTTP status out of band into RAVEL_STATUS_FILE.
set -uo pipefail
body=$(mktemp)
code=$("$RAVEL_REAL_CURL" \
  --silent \
  --connect-timeout "$RAVEL_CURL_CONNECT_TIMEOUT_SECONDS" \
  --max-time "$RAVEL_CURL_MAX_TIME_SECONDS" \
  --output "$body" \
  --write-out '%{http_code}' \
  "$@")
rc=$?
printf '%s' "$code" >"$RAVEL_STATUS_FILE"
cat "$body"
rm -f "$body"
exit "$rc"
SHIM
  chmod +x "$CURL_SHIM_DIR/curl"
  # shellcheck disable=SC2064
  trap "rm -rf '$CURL_SHIM_DIR'" EXIT
}

# --- Running one command --------------------------------------------------
# Runs `cmd`, writes its stdout to `body_file`, and prints two space-separated
# tokens on one line: the captured HTTP status (or "none") and the command's
# own exit code. A curl command runs with the shim on PATH so the HTTP status is
# captured out of band even when curl exits 0 on a 4xx/5xx. If the status cannot
# be determined, "none" is reported and a `status=` assertion fails on it, never
# passes.
run_one_command() {
  local cmd=$1
  local body_file=$2
  local run_code=0
  local captured="none"

  if [[ $cmd == curl* ]]; then
    setup_curl_shim
    local status_file
    status_file=$(mktemp)
    : >"$status_file"
    PATH="$CURL_SHIM_DIR:$PATH" \
      RAVEL_REAL_CURL="$REAL_CURL" \
      RAVEL_STATUS_FILE="$status_file" \
      RAVEL_CURL_CONNECT_TIMEOUT_SECONDS="$CONNECT_TIMEOUT_SECONDS" \
      RAVEL_CURL_MAX_TIME_SECONDS="$REQUEST_TIMEOUT_SECONDS" \
      bash -c "$cmd" >"$body_file" 2>/dev/null || run_code=$?
    local code_read
    code_read=$(cat "$status_file")
    rm -f "$status_file"
    if [[ -n $code_read ]]; then
      captured=$code_read
    fi
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

  # Extract to a file and capture the extractor's exit code before looping.
  # `done < <(python3 ... )` would discard it: a producer that exits non-zero
  # would leave the loop at rc 0 (a fail-open channel, the exact shape CLAUDE.md
  # warns about). Unreachable from markdown today (a MarkerError emits zero
  # records, so found=0 already fails), but closed here regardless.
  local rec_file
  rec_file=$(mktemp)
  local extract_rc=0
  python3 "$py_module" extract "$md" >"$rec_file" || extract_rc=$?
  if [[ $extract_rc -ne 0 ]]; then
    rm -f "$rec_file"
    echo "extractor failed for $md (rc=$extract_rc)" >&2
    return "$extract_rc"
  fi

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
  done <"$rec_file"
  rm -f "$rec_file"

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
