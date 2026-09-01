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
# real ceiling. These reach the readiness curls (issued directly here) and every
# curl invocation the shim fronts while a marked block runs, because the shim
# injects them as argv. They do NOT reach a block that never resolves to the
# shim: a first token that is not the literal `curl` (`env curl ...`,
# `RAVEL_TOKEN=demo curl ...`, `/usr/bin/curl ...`, a leading comment) runs a
# bare curl with no --max-time. BLOCK_TIMEOUT_SECONDS below is the bound that
# covers those, so a hung request in any block shape still fails the job rather
# than hanging it. Timing out is a failure, never a pass.
CONNECT_TIMEOUT_SECONDS=${RAVEL_CONNECT_TIMEOUT_SECONDS:-5}
REQUEST_TIMEOUT_SECONDS=${RAVEL_REQUEST_TIMEOUT_SECONDS:-10}

# Wall-clock ceiling for one marked block, enforced with `timeout` regardless of
# the block's text: the per-request bounds above only reach curls the shim
# fronts, so a block whose first token is not `curl` (and therefore bypasses the
# shim) would otherwise run an unbounded bare curl and hang the whole CI job. It
# must exceed the sum of per-request budgets a legitimate block can incur (a
# handful of sequential requests, each up to REQUEST_TIMEOUT_SECONDS); the
# default leaves generous headroom over that. Hitting it is a hard failure that
# names the block, never a pass. `-k` escalates to SIGKILL if the block ignores
# SIGTERM, so a `trap '' TERM` in README text cannot outlast the bound.
BLOCK_TIMEOUT_SECONDS=${RAVEL_BLOCK_TIMEOUT_SECONDS:-90}
BLOCK_TIMEOUT_KILL_SECONDS=${RAVEL_BLOCK_TIMEOUT_KILL_SECONDS:-5}

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
#
# A block may issue MORE THAN ONE request (the README's ingest-then-query
# shape). One truncating status slot would keep only the last request's status,
# so a failing ingest followed by a healthy query would look healthy. The shim
# therefore APPENDS one status line per invocation to RAVEL_STATUS_FILE and one
# body-file path per invocation to RAVEL_BODY_INDEX, both in execution order, so
# run_one_command sees every request. Each body is kept in RAVEL_CAPTURE_DIR (not
# deleted here) for the driver to evaluate and reap.
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
body=$(mktemp "$RAVEL_CAPTURE_DIR/body.XXXXXX")
code=$("$RAVEL_REAL_CURL" \
  --silent \
  --connect-timeout "$RAVEL_CURL_CONNECT_TIMEOUT_SECONDS" \
  --max-time "$RAVEL_CURL_MAX_TIME_SECONDS" \
  --output "$body" \
  --write-out '%{http_code}' \
  "$@")
rc=$?
# Append (not overwrite): a block may make several requests, and every one must
# be accounted for. One status line and one body-file path per invocation.
printf '%s\n' "$code" >>"$RAVEL_STATUS_FILE"
printf '%s\n' "$body" >>"$RAVEL_BODY_INDEX"
cat "$body"
exit "$rc"
SHIM
  chmod +x "$CURL_SHIM_DIR/curl"
  # shellcheck disable=SC2064
  trap "rm -rf '$CURL_SHIM_DIR'" EXIT
}

# --- Running one command --------------------------------------------------
# Reduce the per-request statuses a block captured to a single token for the
# `status=` assertion. Reads one code per line (execution order) from
# `status_file` and prints:
#   "none"      no request was captured (a shim-bypassed or non-curl block)
#   "mismatch"  the block's requests returned two or more DIFFERENT codes, so a
#               single `status=` cannot map to them: fail closed, never guess
#   <code>      every request returned this one code; the assertion applies to
#               all of them, not to whichever ran last
resolve_block_status() {
  local status_file=$1
  local -a codes=()
  mapfile -t codes <"$status_file"
  if (( ${#codes[@]} == 0 )); then
    printf 'none'
    return 0
  fi
  local first=${codes[0]}
  local c
  for c in "${codes[@]}"; do
    if [[ $c != "$first" ]]; then
      printf 'mismatch'
      return 0
    fi
  done
  printf '%s' "$first"
}

# Runs `cmd`, writes the body to evaluate into `body_file`, and prints two
# space-separated tokens on one line: the block's status token (see
# resolve_block_status, or "timeout") and the command's own exit code. A curl
# command runs with the shim on PATH so every request's HTTP status is captured
# out of band even when curl exits 0 on a 4xx/5xx. When the shim fronted at least
# one request, `body_file` is overwritten with the LAST request's body, so a
# `json:`/`nonempty:` assertion evaluates one coherent response rather than the
# concatenation of every request's output (which let an empty 401 body followed
# by a valid 200 envelope satisfy the check vacuously). Every block, whatever its
# text, runs under a BLOCK_TIMEOUT_SECONDS wall-clock bound; a block that exceeds
# it reports "timeout" and fails.
run_one_command() {
  local cmd=$1
  local body_file=$2
  local run_code=0
  local captured="none"

  if [[ $cmd == curl* ]]; then
    setup_curl_shim
    local cap_dir
    cap_dir=$(mktemp -d)
    local status_file="$cap_dir/status"
    local body_index="$cap_dir/bodyindex"
    : >"$status_file"
    : >"$body_index"
    PATH="$CURL_SHIM_DIR:$PATH" \
      RAVEL_REAL_CURL="$REAL_CURL" \
      RAVEL_STATUS_FILE="$status_file" \
      RAVEL_BODY_INDEX="$body_index" \
      RAVEL_CAPTURE_DIR="$cap_dir" \
      RAVEL_CURL_CONNECT_TIMEOUT_SECONDS="$CONNECT_TIMEOUT_SECONDS" \
      RAVEL_CURL_MAX_TIME_SECONDS="$REQUEST_TIMEOUT_SECONDS" \
      timeout -k "$BLOCK_TIMEOUT_KILL_SECONDS" "$BLOCK_TIMEOUT_SECONDS" \
      bash -c "$cmd" >"$body_file" 2>/dev/null || run_code=$?
    if [[ $run_code -eq 124 || $run_code -eq 137 ]]; then
      captured="timeout"
    else
      captured=$(resolve_block_status "$status_file")
      # Evaluate the last request's body, not the concatenated block stdout.
      local -a body_paths=()
      mapfile -t body_paths <"$body_index"
      if (( ${#body_paths[@]} > 0 )); then
        cp "${body_paths[-1]}" "$body_file"
      fi
    fi
    rm -rf "$cap_dir"
  else
    timeout -k "$BLOCK_TIMEOUT_KILL_SECONDS" "$BLOCK_TIMEOUT_SECONDS" \
      bash -c "$cmd" >"$body_file" 2>/dev/null || run_code=$?
    if [[ $run_code -eq 124 || $run_code -eq 137 ]]; then
      captured="timeout"
    fi
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

    # Fail closed BEFORE evaluation on the two shapes the evaluator cannot judge:
    # a block that outran its wall-clock bound, and a block whose requests
    # returned differing statuses (a single `status=` cannot map to N requests,
    # and a body-only marker would otherwise pass on the last request while an
    # earlier one had already failed).
    if [[ $http_status == "timeout" ]]; then
      rm -f "$body_file"
      echo "FAIL: block at ${md}:${line_no} exceeded the ${BLOCK_TIMEOUT_SECONDS}s wall-clock bound" >&2
      return 1
    fi
    if [[ $http_status == "mismatch" ]]; then
      rm -f "$body_file"
      echo "FAIL: block at ${md}:${line_no} issued requests with differing HTTP statuses;" \
           "a single status= cannot map to them (fail closed, not a guess)" >&2
      return 1
    fi

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
