#!/usr/bin/env bash
# MCP agent evaluation runner (issue #1385, ADR-1374 D8). See README.md.
#
#   run.sh --check
#       Offline. Validates corpus/, expected/ and fixture/, prints every figure
#       once, and exits 1 unless each figure in bands.json and
#       fixture/fixture.json is present exactly once and equal.
#   run.sh --verify-figures FILE
#       The figure assertion alone, over a file of "figure KEY=N" lines.
#   run.sh --fixture URL --out DIR
#       Loads the fixture into an empty tenant over OTLP/HTTP, records T0 in
#       DIR/anchor.json, and queries the counts back over /api/v1/sql.
#   run.sh --run PROFILE URL --out DIR [--condition cold|warm] [--runs N]
#       Runs every case through $MCP_EVAL_AGENT_CMD and scores it. PROFILE is
#       baseline or full. Results go under DIR/results/, never into the repo.
#
# Environment: MCP_EVAL_TOKEN (bearer token, --fixture and --run),
# MCP_EVAL_AGENT_CMD, MCP_EVAL_RAVEL_SHA, MCP_EVAL_COLD_CMD (cold runs),
# MCP_EVAL_FAULT_CMD (optional, case F4), MCP_EVAL_ORIGIN (default
# http://mcp-eval.invalid), MCP_EVAL_DIR (corpus root, default this directory).
#
# Exit codes: 0 pass, 1 a finding or a failed step, 64 usage or a refused
# precondition, 69 a dependency or the server could not be reached.
#
# Bash 3.2: no associative arrays, no mapfile, no ${var,,}.

set -u

HERE=$(cd "$(dirname "$0")" && pwd -P)
ROOT=${MCP_EVAL_DIR:-$HERE}
TOOL="$HERE/lib/evaltool.py"
GEN="$ROOT/fixture/gen.py"
ORIGIN=${MCP_EVAL_ORIGIN:-http://mcp-eval.invalid}

die() {
  code=$1
  shift
  echo "run.sh: $*" >&2
  exit "$code"
}

usage() {
  sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 64
}

need() {
  command -v "$1" >/dev/null 2>&1 || die 69 "$1 is required and not on PATH"
}

TMP=""
cleanup() {
  if [ -n "$TMP" ]; then rm -rf "$TMP"; fi
}
trap cleanup EXIT
make_tmp() {
  TMP=$(mktemp -d "${TMPDIR:-/tmp}/mcp-eval.XXXXXX") || die 69 "mktemp failed"
}

py() {
  python3 "$TOOL" "$@"
}

# --- figures -----------------------------------------------------------------

verify_figures() {
  figures=$1
  make_tmp
  py want corpus "$ROOT" "$TMP/want.json" || die 1 "could not build the wanted figures"
  py verify "$TMP/want.json" "$figures" || return 1
  return 0
}

cmd_check() {
  need python3
  make_tmp
  check_code=0
  py check "$ROOT" "$TMP/figures" || check_code=$?
  if [ ! -s "$TMP/figures" ]; then
    die 1 "the corpus check wrote no figures (exit $check_code)"
  fi
  cat "$TMP/figures"
  py want corpus "$ROOT" "$TMP/want.json" || die 1 "could not build the wanted figures"
  verify_code=0
  py verify "$TMP/want.json" "$TMP/figures" || verify_code=$?
  if [ "$check_code" -ne 0 ] || [ "$verify_code" -ne 0 ]; then
    echo "run.sh --check: FAIL (corpus check exit $check_code, figure check exit $verify_code)" >&2
    exit 1
  fi
  echo "run.sh --check: PASS"
}

# --- server access -----------------------------------------------------------

require_token() {
  [ -n "${MCP_EVAL_TOKEN:-}" ] || die 64 "MCP_EVAL_TOKEN is not set; the server needs a bearer token"
}

# Refuse an output directory inside this repository, where results could be
# committed by accident.
check_out_dir() {
  out=$1
  [ -n "$out" ] || die 64 "--out DIR is required"
  parent=$(dirname "$out")
  parent_abs=$(cd "$parent" && pwd -P) || die 64 "the parent of $out does not exist"
  out_abs="$parent_abs/$(basename "$out")"
  repo=$(cd "$HERE" && git rev-parse --show-toplevel 2>/dev/null) || repo=""
  if [ -n "$repo" ]; then
    repo=$(cd "$repo" && pwd -P)
    case "$out_abs/" in
      "$repo"/*) die 64 "--out $out_abs is inside the repository $repo; name a directory outside it" ;;
    esac
  fi
  mkdir -p "$out_abs" || die 64 "cannot create $out_abs"
  OUT=$out_abs
}

# sql_count QUERY START_NS END_NS TOKENS -> prints the single integer cell
sql_count() {
  py sql-request "$1" "$2" "$3" "$4" >"$TMP/sql.req" || die 1 "could not build the SQL request"
  http=$(curl -sS -o "$TMP/sql.resp" -w '%{http_code}' -X POST "$URL/api/v1/sql" \
    -H "authorization: Bearer $MCP_EVAL_TOKEN" -H 'content-type: application/json' \
    --data-binary "@$TMP/sql.req") || die 69 "the SQL request to $URL did not complete"
  [ "$http" = "200" ] || die 1 "SQL request returned HTTP $http: $(head -c 400 "$TMP/sql.resp")"
  py sql-scalar "$TMP/sql.resp"
}

# ingest SIGNAL BODY_FILE -> prints the commit tokens, comma-separated
ingest() {
  http=$(curl -sS -o "$TMP/ingest.resp" -D "$TMP/ingest.hdr" -w '%{http_code}' -X POST \
    "$URL/v1/$1" -H "authorization: Bearer $MCP_EVAL_TOKEN" \
    -H 'content-type: application/x-protobuf' --data-binary "@$2") \
    || die 69 "the OTLP $1 export to $URL did not complete"
  [ "$http" = "200" ] || die 1 "OTLP $1 export returned HTTP $http"
  tokens=$(tr -d '\r' <"$TMP/ingest.hdr" | sed -n 's/^[Xx]-[Rr]avel-[Cc]ommit-[Tt]oken: *//p' | paste -sd, -)
  [ -n "$tokens" ] || die 1 "OTLP $1 export returned no x-ravel-commit-token; the fixture needs a strict ack"
  echo "$tokens"
}

# mcp_call TOOL ARGS_JSON OUT -> writes the result envelope to OUT
mcp_call() {
  py mcp-request "$1" "$2" >"$TMP/mcp.req" || die 1 "could not build the MCP request"
  http=$(curl -sS -o "$TMP/mcp.resp" -w '%{http_code}' -X POST "$URL/mcp" \
    -H "authorization: Bearer $MCP_EVAL_TOKEN" -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' -H 'MCP-Protocol-Version: 2026-07-28' \
    -H 'Mcp-Method: tools/call' -H "Mcp-Name: $1" -H "Origin: $ORIGIN" \
    --data-binary "@$TMP/mcp.req") || die 69 "the MCP request to $URL did not complete"
  [ "$http" = "200" ] || die 1 "MCP $1 returned HTTP $http: $(head -c 400 "$TMP/mcp.resp")"
  py mcp-envelope "$TMP/mcp.resp" >"$3" || die 1 "MCP $1 returned no envelope"
}

# --- fixture -----------------------------------------------------------------

cmd_fixture() {
  need python3
  need curl
  require_token
  check_out_dir "$OUT_ARG"
  [ ! -e "$OUT/anchor.json" ] || die 64 "$OUT/anchor.json exists; load each fixture into a fresh tenant and a fresh --out"
  make_tmp
  now_ns=$(py now) || die 1 "could not read the clock"
  far_end=$((now_ns + 3600 * 1000000000))
  for table in samples logs spans; do
    n=$(sql_count "SELECT count(*) FROM $table" 0 "$far_end" "") || exit $?
    [ "$n" = "0" ] || die 64 "table $table already holds $n rows; the exact counts need an empty tenant"
  done
  t0=$(( (now_ns / 60000000000) * 60000000000 - 3600 * 1000000000 ))
  py kv "$OUT/anchor.json" "t0_ns=$t0" "loaded_at_ns=$now_ns" "url=$URL" || die 1 "could not write anchor.json"
  for sig in metrics logs traces; do
    python3 "$GEN" encode "$sig" "$t0" >"$TMP/$sig.pb" || die 1 "gen.py encode $sig failed"
  done
  tok_metrics=$(ingest metrics "$TMP/metrics.pb") || exit $?
  tok_logs=$(ingest logs "$TMP/logs.pb") || exit $?
  tok_traces=$(ingest traces "$TMP/traces.pb") || exit $?
  end=$((t0 + 3600 * 1000000000))
  : >"$TMP/figures"
  # figure key|table tokens|statement
  while IFS='|' read -r key which stmt; do
    case "$which" in
      metrics) toks=$tok_metrics ;;
      logs) toks=$tok_logs ;;
      traces) toks=$tok_traces ;;
    esac
    n=$(sql_count "$stmt" "$t0" "$end" "$toks") || exit $?
    echo "figure fixture.$key=$n" >>"$TMP/figures"
  done <<'QUERIES'
samples|metrics|SELECT count(*) FROM samples
series|metrics|SELECT count(DISTINCT series_id) FROM samples
log_rows|logs|SELECT count(*) FROM logs
error_log_rows|logs|SELECT count(*) FROM logs WHERE severity_text = 'ERROR'
target_user_log_rows|logs|SELECT count(*) FROM logs WHERE "user.id" = 'u-7f3a'
spans|traces|SELECT count(*) FROM spans
traces|traces|SELECT count(DISTINCT trace_id) FROM spans
error_spans|traces|SELECT count(*) FROM spans WHERE status_code = 2
QUERIES
  cat "$TMP/figures"
  cp "$TMP/figures" "$OUT/fixture-figures.txt" || die 1 "could not save the figures"
  py want loaded "$ROOT" "$TMP/want.json" || die 1 "could not build the wanted figures"
  py verify "$TMP/want.json" "$TMP/figures" || die 1 "the loaded fixture does not hold the stated counts"
  echo "run.sh --fixture: PASS (T0=$t0 recorded in $OUT/anchor.json)"
}

# --- run ---------------------------------------------------------------------

# run_case PASS_DIR RUN_INDEX CASE_ID
run_case() {
  pass_dir=$1
  n=$2
  cid=$3
  cdir="$pass_dir/$cid"
  mkdir -p "$cdir" || die 1 "cannot create $cdir"
  setup=$(py field-opt "$ROOT/corpus/$cid.json" setup) || die 1 "cannot read case $cid"
  if [ "$setup" = "storage_fault" ] && [ -z "${MCP_EVAL_FAULT_CMD:-}" ]; then
    py kv "$cdir/record.json" "case=$cid" "run=$n" "profile=$PROFILE" "condition=$CONDITION" \
      "ravel_sha=$MCP_EVAL_RAVEL_SHA" "status=not_run" \
      "reason=MCP_EVAL_FAULT_CMD is not set, so storage cannot be made unavailable" \
      || die 1 "cannot write $cdir/record.json"
    echo "  $cid run $n: not_run (no MCP_EVAL_FAULT_CMD)"
    return 0
  fi
  if [ "$CONDITION" = "cold" ]; then
    "$MCP_EVAL_COLD_CMD" >"$cdir/cold.log" 2>&1 || die 1 "MCP_EVAL_COLD_CMD failed before $cid; see $cdir/cold.log"
  fi
  now_ns=$(py now) || die 1 "could not read the clock"
  label="mcp-eval-$now_ns-$PROFILE-$CONDITION-$n-$cid"
  prior="-"
  prior_snapshot=""
  if [ "$setup" = "late_batch" ]; then
    t_from=$(py offset "$T0" "$now_ns" T0) || die 1 "bad window"
    t_to=$(py offset "$T0" "$now_ns" now+1m) || die 1 "bad window"
    args="{\"query\": \"SELECT ts, body FROM logs WHERE attrs['eval.batch'] = '$label'\", \"time_range\": {\"start_ns\": \"$t_from\", \"end_ns\": \"$t_to\"}}"
    mcp_call ravel_query_sql "$args" "$cdir/prior_result.json"
    rows=$(py field "$cdir/prior_result.json" data.row_count) || die 1 "cannot read the prior result"
    [ "$rows" = "0" ] || die 1 "the prior query for $label returned $rows rows; want 0"
    prior_snapshot=$(py field "$cdir/prior_result.json" visibility.snapshot_id) || die 1 "cannot read the prior snapshot"
    prior="$cdir/prior_result.json"
    batch_ns=$(( (now_ns / 1000000000) * 1000000000 - 5000000000 ))
    [ "$batch_ns" -ge $((T0 + 3600 * 1000000000)) ] || die 1 "the fixture hour has not ended; wait until T0+60m before --run"
    python3 "$GEN" late "$batch_ns" "$label" >"$TMP/late.pb" || die 1 "gen.py late failed"
    tok=$(ingest logs "$TMP/late.pb") || exit $?
    seen=$(sql_count "SELECT count(*) FROM logs WHERE attrs['eval.batch'] = '$label'" "$t_from" "$t_to" "$tok") || exit $?
    [ "$seen" = "4" ] || die 1 "the late batch $label reads back $seen rows; want 4"
  fi
  py case-input "$ROOT" "$cid" "$T0" "$now_ns" "$PROFILE" "$label" "$prior" "$cdir/case.json" \
    || die 1 "cannot write the case input for $cid"
  py tools "$PROFILE" >"$cdir/tools.txt" || die 1 "cannot write the tool list"
  if [ "$setup" = "storage_fault" ]; then
    "$MCP_EVAL_FAULT_CMD" on >"$cdir/fault.log" 2>&1 || die 1 "MCP_EVAL_FAULT_CMD on failed; see $cdir/fault.log"
  fi
  t_start=$(py now) || die 1 "could not read the clock"
  agent_exit=0
  MCP_EVAL_URL=$URL MCP_EVAL_PROFILE=$PROFILE "$MCP_EVAL_AGENT_CMD" \
    "$cdir/case.json" "$cdir/tools.txt" "$cdir/transcript.json" >"$cdir/agent.log" 2>&1 || agent_exit=$?
  t_end=$(py now) || die 1 "could not read the clock"
  if [ "$setup" = "storage_fault" ]; then
    "$MCP_EVAL_FAULT_CMD" off >>"$cdir/fault.log" 2>&1 || die 1 "MCP_EVAL_FAULT_CMD off failed; see $cdir/fault.log"
  fi
  wall_ms=$(( (t_end - t_start) / 1000000 ))
  py kv "$cdir/context.json" "profile=$PROFILE" "condition=$CONDITION" "run=$n" \
    "ravel_sha=$MCP_EVAL_RAVEL_SHA" "wall_time_ms=$wall_ms" "agent_exit=$agent_exit" \
    "t0_ns=$T0" "now_ns=$now_ns" "late_label=$label" "prior_snapshot_id=$prior_snapshot" \
    || die 1 "cannot write the context for $cid"
  py score "$ROOT" "$cid" "$cdir/transcript.json" "$cdir/context.json" "$cdir/record.json" \
    || die 1 "cannot score $cid"
  echo "  $cid run $n: agent exit $agent_exit, ${wall_ms} ms"
}

run_pass() {
  pass_dir=$1
  n=$2
  for f in "$ROOT"/corpus/*.json; do
    cid=$(basename "$f" .json)
    run_case "$pass_dir" "$n" "$cid"
  done
}

cmd_run() {
  # The agent command is checked first: nothing else is worth asking about
  # when there is no agent to run.
  [ -n "${MCP_EVAL_AGENT_CMD:-}" ] || die 64 "MCP_EVAL_AGENT_CMD is not set; --run needs an agent command (README.md, Agent command contract)"
  [ -x "$MCP_EVAL_AGENT_CMD" ] || die 64 "MCP_EVAL_AGENT_CMD=$MCP_EVAL_AGENT_CMD is not an executable file"
  case "$PROFILE" in
    baseline | full) ;;
    *) die 64 "profile must be baseline or full, got '$PROFILE'" ;;
  esac
  case "$CONDITION" in
    cold | warm) ;;
    *) die 64 "--condition must be cold or warm, got '$CONDITION'" ;;
  esac
  case "$RUNS" in
    '' | *[!0-9]* | 0) die 64 "--runs must be a positive integer, got '$RUNS'" ;;
  esac
  [ -n "${MCP_EVAL_RAVEL_SHA:-}" ] || die 64 "MCP_EVAL_RAVEL_SHA is not set; every record names the Ravel version or main SHA"
  case "$MCP_EVAL_RAVEL_SHA" in
    *[!0-9A-Za-z.+_-]*) die 64 "MCP_EVAL_RAVEL_SHA holds characters outside [0-9A-Za-z.+_-]" ;;
  esac
  if [ "$CONDITION" = "cold" ]; then
    [ -n "${MCP_EVAL_COLD_CMD:-}" ] && [ -x "$MCP_EVAL_COLD_CMD" ] \
      || die 64 "a cold run needs MCP_EVAL_COLD_CMD, an executable that restarts the server with empty caches"
  fi
  if [ -n "${MCP_EVAL_FAULT_CMD:-}" ] && [ ! -x "$MCP_EVAL_FAULT_CMD" ]; then
    die 64 "MCP_EVAL_FAULT_CMD=$MCP_EVAL_FAULT_CMD is not an executable file"
  fi
  need python3
  need curl
  require_token
  check_out_dir "$OUT_ARG"
  [ -f "$OUT/anchor.json" ] || die 64 "$OUT/anchor.json is missing; run --fixture with the same --out first"
  T0=$(py field "$OUT/anchor.json" t0_ns) || die 1 "cannot read $OUT/anchor.json"
  case "$T0" in
    '' | *[!0-9]*) die 1 "$OUT/anchor.json holds no t0_ns" ;;
  esac
  results="$OUT/results/$PROFILE/$CONDITION"
  [ ! -e "$results" ] || die 64 "$results exists; results are never overwritten"
  make_tmp
  if [ "$CONDITION" = "warm" ]; then
    echo "priming pass (unrecorded) for $PROFILE/warm"
    run_pass "$OUT/prime/$PROFILE/warm" 0
  fi
  i=1
  while [ "$i" -le "$RUNS" ]; do
    echo "run $i of $RUNS for $PROFILE/$CONDITION"
    run_pass "$results/run-$i" "$i"
    i=$((i + 1))
  done
  py summarize "$results" "$results/summary.tsv" || die 1 "cannot write the summary"
}

# --- main --------------------------------------------------------------------

[ $# -ge 1 ] || usage
MODE=$1
shift
OUT_ARG=""
CONDITION=warm
RUNS=5
PROFILE=""
URL=""
case "$MODE" in
  --check)
    [ $# -eq 0 ] || usage
    cmd_check
    ;;
  --verify-figures)
    [ $# -eq 1 ] || usage
    need python3
    verify_figures "$1" || exit 1
    ;;
  --fixture)
    [ $# -ge 1 ] || usage
    URL=${1%/}
    shift
    while [ $# -gt 0 ]; do
      case "$1" in
        --out) [ $# -ge 2 ] || usage; OUT_ARG=$2; shift 2 ;;
        *) usage ;;
      esac
    done
    cmd_fixture
    ;;
  --run)
    [ $# -ge 2 ] || usage
    PROFILE=$1
    URL=${2%/}
    shift 2
    while [ $# -gt 0 ]; do
      case "$1" in
        --out) [ $# -ge 2 ] || usage; OUT_ARG=$2; shift 2 ;;
        --condition) [ $# -ge 2 ] || usage; CONDITION=$2; shift 2 ;;
        --runs) [ $# -ge 2 ] || usage; RUNS=$2; shift 2 ;;
        *) usage ;;
      esac
    done
    cmd_run
    ;;
  -h | --help) usage ;;
  *) usage ;;
esac
