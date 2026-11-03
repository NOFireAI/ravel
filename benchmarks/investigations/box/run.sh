#!/usr/bin/env bash
# box/run.sh: one selective-investigations run (issue #3699, ADR-3692
# decision 8), from an empty store to results.<lane>.json.
#
#   run.sh --lane local --run-toml FILE --out DIR --bin-dir DIR --rustfs PATH
#          [--pylib DIR] [--dry-run]
#   run.sh --lane s3 --run-toml FILE --out DIR [--dry-run]
#
# --lane local runs on the machine it is started on, against a loopback
# RustFS it starts itself (--rustfs), with ravel-cli and ravel-server release
# binaries already built (--bin-dir; ravel-server with --features sql). It
# never shuts anything down.
#
# --lane s3 runs only on the instance box/launch.sh started (it refuses
# without that box's /root/inv/RUN_ID): it builds both binaries at run.toml's
# box.sha, measures against the real S3 bucket box.store_bucket through the
# instance role, uploads out/ and terminates the instance, on success and on
# failure alike.
#
# --dry-run prints every step (STEP), command (DRY-RUN:) and stamp (STAMP) in
# order and executes nothing; it needs no binary, store or credential.
#
# Steps: bootstrap, store, host, corpus, declare, load-base, then one step per
# run.toml state (fold transitions inside them), then evaluate and finish.
# out/ holds run.toml, truth.json, calls.jsonl, stamps/ and evaluate.py's
# results.<lane>.json and summary.<lane>.txt; out/work/ holds logs, plans,
# the store's data and the run's secrets and is never uploaded.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INV_DIR="$(cd "$HERE/.." && pwd)"
LIB="$HERE/lib/inv.py"
BOX_ROOT=/root/inv
BOX_MARKER="$BOX_ROOT/RUN_ID"

LANE=""
RUN_TOML_IN=""
OUT=""
BIN_DIR=""
RUSTFS_BIN=""
PYLIB=""
DRY_RUN=0

usage() {
  sed -n '5,8p' "${BASH_SOURCE[0]}" >&2
  exit 64
}

while [ $# -gt 0 ]; do
  case "$1" in
    --lane) LANE="${2:-}"; shift 2 ;;
    --run-toml) RUN_TOML_IN="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    --bin-dir) BIN_DIR="${2:-}"; shift 2 ;;
    --rustfs) RUSTFS_BIN="${2:-}"; shift 2 ;;
    --pylib) PYLIB="${2:-}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) usage ;;
  esac
done
case "$LANE" in local | s3) ;; *) usage ;; esac
[ -n "$RUN_TOML_IN" ] && [ -f "$RUN_TOML_IN" ] && [ -n "$OUT" ] || usage

# The s3 lane terminates the machine it runs on. Outside a dry run it runs
# only where launch.sh's user-data wrote the box marker.
if [ "$LANE" = s3 ] && [ "$DRY_RUN" = 0 ] && [ ! -f "$BOX_MARKER" ]; then
  echo "run.sh: --lane s3 runs only on a box launch.sh started (no $BOX_MARKER); use --dry-run here" >&2
  exit 64
fi
if [ "$LANE" = local ] && [ "$DRY_RUN" = 0 ]; then
  [ -x "$BIN_DIR/ravel-cli" ] && [ -x "$BIN_DIR/ravel-server" ] \
    || { echo "run.sh: --lane local needs --bin-dir holding ravel-cli and ravel-server" >&2; exit 64; }
  [ -x "$RUSTFS_BIN" ] || { echo "run.sh: --lane local needs --rustfs PATH" >&2; exit 64; }
fi

# Amazon Linux 2023's python3 is 3.9, without tomllib; launch.sh's user-data
# installs python3.11 before it starts this script.
# HOST_PY runs what a dry run executes too (config reads, free ports), so a
# dry run of the s3 lane needs only the local python3.
PY=python3
[ "$LANE" = s3 ] && PY=python3.11
HOST_PY="$PY"
[ "$DRY_RUN" = 1 ] && HOST_PY=python3
# A dry run's DRY-RUN lines go to fd 3, the script's stdout, so a command
# whose output the real run sends to a log still prints its line.
exec 3>&1
STAMPS="$OUT/stamps"
WORK="$OUT/work"
RUN_TOML="$OUT/run.toml"
PROGRESS="$WORK/progress.log"
CALLS="$OUT/calls.jsonl"
STATEMENTS="$INV_DIR/statements.toml"
AUTH_DIR="$WORK/auth"
TOKEN_FILE="$WORK/auth/tokens"
CURRENT_STEP=""
FINISHED=0
SERVER_PID=""
SERVER_N=0
SERVER_LOG=""
RUSTFS_PID=""
VERDICT=none

say() { printf 'run.sh: %s\n' "$*" >&2; }
utc_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
now_hour() { echo $(($(date +%s) / 3600)); }

step() {
  CURRENT_STEP="$1"
  printf 'STEP %s\n' "$1"
  [ "$DRY_RUN" = 1 ] || printf '%s STEP %s\n' "$(utc_now)" "$1" >>"$PROGRESS"
}

run() {
  if [ "$DRY_RUN" = 1 ]; then
    {
      printf 'DRY-RUN:'
      printf ' %q' "$@"
      printf '\n'
    } >&3
    return 0
  fi
  "$@"
}

# stamp FILE FIELD...: write stamps/FILE (a .jsonl FILE is appended to).
stamp() {
  local file="$1" append=()
  shift
  printf 'STAMP stamps/%s\n' "$file"
  [ "$DRY_RUN" = 1 ] && return 0
  case "$file" in *.jsonl) append=(--append) ;; esac
  "$PY" "$LIB" stamp --file "$STAMPS/$file" "${append[@]}" "$@" || fail "stamp $file"
}

cfg() { "$HOST_PY" "$LIB" get "$RUN_TOML_IN" "$@"; }

# --- failure and the end of the run -----------------------------------------

stop_server() {
  [ -n "$SERVER_PID" ] || return 0
  local i
  kill -TERM "$SERVER_PID" 2>/dev/null
  for ((i = 1; i <= 600; i++)); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.1
  done
  kill -KILL "$SERVER_PID" 2>/dev/null
  wait "$SERVER_PID" 2>/dev/null
  SERVER_PID=""
}

stop_store() {
  [ -n "$RUSTFS_PID" ] || return 0
  kill -TERM "$RUSTFS_PID" 2>/dev/null
  wait "$RUSTFS_PID" 2>/dev/null
  RUSTFS_PID=""
}

upload_out() {
  [ "$LANE" = s3 ] && [ "$DRY_RUN" = 0 ] || return 0
  aws s3 cp --region "$REGION" --only-show-errors --recursive --exclude 'work/*' \
    "$OUT" "$OUT_URI/" || return 1
  aws s3 cp --region "$REGION" --only-show-errors "$PROGRESS" "$OUT_URI/progress.log"
}

# terminate_self: the s3 lane's last act, through the instance role, then
# shutdown whatever happened (launch.sh set shutdown behavior to terminate).
terminate_self() {
  [ "$LANE" = s3 ] && [ "$DRY_RUN" = 0 ] && [ -f "$BOX_MARKER" ] || return 0
  local token id
  trap - EXIT HUP INT TERM
  token="$(curl -sS --max-time 5 -X PUT http://169.254.169.254/latest/api/token \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" || token=""
  id="$(curl -sS --max-time 5 -H "X-aws-ec2-metadata-token: $token" \
    http://169.254.169.254/latest/meta-data/instance-id)" || id=""
  if [[ "$id" =~ ^i-[0-9a-z]+$ ]]; then
    aws ec2 terminate-instances --region "$REGION" --instance-ids "$id" --output text >/dev/null
  fi
  shutdown -h now
}

# fail MESSAGE: FAILED naming the step and the message, outputs kept (and on
# the s3 lane uploaded), every process this run started stopped.
fail() {
  FINISHED=1
  say "FAILED step=$CURRENT_STEP: $*"
  [ "$DRY_RUN" = 1 ] && exit 1
  stop_server
  stop_store
  mkdir -p "$OUT"
  printf 'FAILED step=%s at=%s: %s\n' "$CURRENT_STEP" "$(utc_now)" "$*" >"$OUT/FAILED"
  if [ "$LANE" = s3 ]; then
    upload_out
    terminate_self
  fi
  exit 1
}

on_exit() {
  [ "$FINISHED" = 1 ] || fail "unexpected exit"
}
trap on_exit EXIT
trap 'fail "signal"' HUP INT TERM

# --- servers -----------------------------------------------------------------

free_port() {
  "$HOST_PY" -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

start_server() {
  SERVER_N=$((SERVER_N + 1))
  SERVER_LOG="$WORK/server-$SERVER_N.log"
  # --mode query: --mode all runs a scheduled fold and compaction that would
  # change the state under measurement.
  "$SERVER_BIN" --mode query --store s3 --shards "$SHARDS" --tenant-hash-unkeyed \
    --tenant-token-file "$TOKEN_FILE" --listen-http "127.0.0.1:$HTTP_PORT" \
    --listen-health "127.0.0.1:$HEALTH_PORT" --listen-grpc "127.0.0.1:$GRPC_PORT" \
    --audit-mode off "${SERVER_EXTRA[@]}" </dev/null >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  local i
  for ((i = 1; i <= 600; i++)); do
    curl -sf --max-time 2 "http://127.0.0.1:$HEALTH_PORT/readyz" >/dev/null 2>&1 && return 0
    kill -0 "$SERVER_PID" 2>/dev/null || { say "server exited before ready; see $SERVER_LOG"; return 1; }
    sleep 0.2
  done
  say "server not ready after 120 s"
  return 1
}

# drop_page_cache: the s3 lane must drop it; the local lane drops it when it
# can and says so when it cannot (not root).
drop_page_cache() {
  if sync && echo 3 2>/dev/null >/proc/sys/vm/drop_caches; then
    CACHE_DROP=dropped
  elif [ "$LANE" = s3 ]; then
    return 1
  else
    CACHE_DROP="unavailable (not root)"
  fi
}

# restart_for LINE: the server restart and page-cache drop before one cold
# call, then the startup cache warmup's end, stamped with the call's key.
restart_for() {
  local line="$1" statement state try warm
  [[ "$line" =~ \"statement\":\ \"([^\"]+)\" ]] && statement="${BASH_REMATCH[1]}"
  [[ "$line" =~ \"state\":\ \"([^\"]+)\" ]] && state="${BASH_REMATCH[1]}"
  [[ "$line" =~ \"try\":\ ([0-9]+) ]] && try="${BASH_REMATCH[1]}"
  stop_server
  drop_page_cache || fail "page cache drop before $statement/$state/$try"
  start_server || fail "server start before $statement/$state/$try"
  warm="$("$PY" "$LIB" warmup --log "$SERVER_LOG" --timeout 300)" || fail "warmup of server $SERVER_N"
  "$PY" "$LIB" stamp --file "$STAMPS/restarts.jsonl" --append statement="$statement" state="$state" \
    try:="$try" server_n:="$SERVER_N" pid:="$SERVER_PID" cache_drop="$CACHE_DROP" \
    warmup:="$warm" at="$(utc_now)" || fail "stamp restarts.jsonl"
}

# --- steps -------------------------------------------------------------------

load_config() {
  # A dry run creates these (empty) too: the commands it prints redirect
  # into them.
  mkdir -p "$OUT" "$STAMPS" "$WORK" "$AUTH_DIR" || fail "create $OUT"
  [ "$DRY_RUN" = 1 ] || cp "$RUN_TOML_IN" "$RUN_TOML" || fail "copy run.toml"
  local toml_lane
  toml_lane="$(cfg run.lane)" || fail "run.toml run.lane"
  # evaluate.py names its outputs and judges timing by run.toml's lane.
  [ "$toml_lane" = "$LANE" ] || fail "run.toml run.lane is '$toml_lane', not --lane $LANE"
  STATES="$(cfg run.states)" || fail "run.toml run.states"
  COLD_RUNS="$(cfg run.cold_runs)" || fail "run.toml run.cold_runs"
  WARM_RUNS="$(cfg run.warm_runs --default 1)"
  HITS="$(cfg run.hits --default false)"
  MIXED_WAIT="$(cfg run.mixed_wait_max_minutes --default 70)"
  TRACES="$(cfg corpus.traces)" || fail "run.toml corpus.traces"
  HOURS="$(cfg corpus.hours)" || fail "run.toml corpus.hours"
  SEED="$(cfg corpus.seed)" || fail "run.toml corpus.seed"
  SPANS_PER="$(cfg corpus.spans_per_trace --default 10)"
  LOGS_PER="$(cfg corpus.logs_per_trace --default 5)"
  TENANT="$(cfg server.tenant --default inv)"
  SHARDS="$(cfg server.shards --default 8)"
  SERVER_EXTRA=()
  local budget
  budget="$(cfg server.memory_budget_bytes --default "")"
  [ -z "$budget" ] || SERVER_EXTRA+=(--memory-budget-bytes "$budget")
  RQ_STATS="${RQ_STATS:-1}"
  if [ "$LANE" = s3 ]; then
    SHA="$(cfg box.sha)" || fail "run.toml box.sha"
    REPO_URL="$(cfg box.repo_url)" || fail "run.toml box.repo_url"
    REGION="$(cfg box.region)" || fail "run.toml box.region"
    STORE_BUCKET="$(cfg box.store_bucket)" || fail "run.toml box.store_bucket"
    local bucket prefix run_id=DRY-RUN
    bucket="$(cfg box.bucket)" || fail "run.toml box.bucket"
    prefix="$(cfg box.prefix --default investigations)"
    [ "$DRY_RUN" = 1 ] || run_id="$(cat "$BOX_MARKER")"
    OUT_URI="s3://$bucket/$prefix/runs/$run_id/out"
    # user-data starts this script with no HOME; rustup installs under it.
    [ "$DRY_RUN" = 1 ] || export HOME="$BOX_ROOT"
    BIN_DIR="$BOX_ROOT/bin"
    SERVER_BIN="$BIN_DIR/ravel-server-$SHA"
    BIN="$BIN_DIR/ravel-cli-$SHA"
    PYLIB="$BOX_ROOT/pylib"
  else
    SERVER_BIN="$BIN_DIR/ravel-server"
    BIN="$BIN_DIR/ravel-cli"
  fi
  [ -z "$PYLIB" ] || export PYTHONPATH="$PYLIB${PYTHONPATH:+:$PYTHONPATH}"
  HITS_CORPUS="$INV_DIR/../clickbench/hits.corpus.json"
  HITS_MAPPING="$INV_DIR/../clickbench/hits.mapping.toml"
  CLI=("$BIN" --store s3 --tenant-hash-unkeyed)
}

step_bootstrap() {
  step bootstrap
  if [ "$LANE" = s3 ]; then
    local src="$BOX_ROOT/ravel" target="$BOX_ROOT/target-fp"
    run dnf install -y git gcc gcc-c++ make cmake pkgconfig openssl-devel perl jq python3.11 python3.11-pip \
      || fail "dnf install"
    run "$PY" -m pip install --quiet --target "$PYLIB" pyarrow==26.0.0 || fail "pip install pyarrow"
    run curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o "$BOX_ROOT/rustup-init.sh" || fail "rustup download"
    run sh "$BOX_ROOT/rustup-init.sh" -y --default-toolchain none || fail "rustup install"
    [ "$DRY_RUN" = 1 ] || . "$HOME/.cargo/env" || fail "cargo env"
    run rm -rf "$src"
    run git clone "$REPO_URL" "$src" || fail "git clone"
    run git -C "$src" checkout -q --detach "$SHA" || fail "checkout $SHA"
    if [ "$DRY_RUN" = 0 ]; then
      [ "$(git -C "$src" rev-parse HEAD)" = "$SHA" ] || fail "HEAD is not $SHA"
      [ -z "$(git -C "$src" status --porcelain)" ] || fail "checkout is dirty"
    fi
    # Two invocations, not one: a combined -p ravel-cli -p ravel-server
    # --features ravel-server/sql build left ravel-server without its SQL
    # routes (404) on this tree.
    run env CARGO_TARGET_DIR="$target" RUSTFLAGS="-C force-frame-pointers=yes" \
      cargo build --manifest-path "$src/Cargo.toml" --release --locked -p ravel-cli || fail "build ravel-cli"
    run env CARGO_TARGET_DIR="$target" RUSTFLAGS="-C force-frame-pointers=yes" \
      cargo build --manifest-path "$src/Cargo.toml" --release --locked -p ravel-server --features sql \
      || fail "build ravel-server"
    run install -D -m 0755 "$target/release/ravel-cli" "$BIN" || fail "install ravel-cli"
    run install -D -m 0755 "$target/release/ravel-server" "$SERVER_BIN" || fail "install ravel-server"
    stamp build.json lane=s3 sha="$SHA" build="separate cargo build --release --locked per package, RUSTFLAGS=-C force-frame-pointers=yes, ravel-server --features sql" \
      ravel_cli_sha256="$(sha256 "$BIN")" ravel_server_sha256="$(sha256 "$SERVER_BIN")" \
      rustc="$([ "$DRY_RUN" = 1 ] && echo dry-run || (cd "$src" && rustc --version))"
  else
    local sha dirty
    sha="$(git -C "$INV_DIR" rev-parse HEAD 2>/dev/null)" || sha=unknown
    dirty="$(git -C "$INV_DIR" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"
    stamp build.json lane=local sha="$sha" dirty_paths:="${dirty:-0}" \
      build="prebuilt binaries from --bin-dir; README.md \"Local lane\" gives the build" \
      ravel_cli_sha256="$(sha256 "$BIN")" ravel_server_sha256="$(sha256 "$SERVER_BIN")"
  fi
}

sha256() {
  [ "$DRY_RUN" = 1 ] && { echo dry-run; return 0; }
  sha256sum "$1" | cut -d' ' -f1
}

write_secrets() {
  [ "$DRY_RUN" = 1 ] && return 0
  local t1 t2
  t1="$("$PY" -c 'import secrets; print(secrets.token_hex(24))')" || return 1
  t2="$("$PY" -c 'import secrets; print(secrets.token_hex(24))')" || return 1
  (
    umask 077
    printf '%s=%s\n%s=%s\n' "$t1" "$TENANT" "$t2" hits >"$TOKEN_FILE"
    printf 'Authorization: Bearer %s\n' "$t1" >"$AUTH_DIR/$TENANT.auth"
    printf 'Authorization: Bearer %s\n' "$t2" >"$AUTH_DIR/hits.auth"
  )
}

step_store() {
  step store
  write_secrets || fail "secrets"
  HTTP_PORT="$(free_port)" HEALTH_PORT="$(free_port)" GRPC_PORT="$(free_port)" || fail "free ports"
  if [ "$LANE" = local ]; then
    local port key secret
    port="$(free_port)" || fail "free port"
    key="$("$HOST_PY" -c 'import secrets; print(secrets.token_hex(10))')"
    secret="$("$HOST_PY" -c 'import secrets; print(secrets.token_hex(20))')"
    export RAVEL_S3_ENDPOINT="http://127.0.0.1:$port" RAVEL_S3_BUCKET=ravel-inv RAVEL_S3_REGION=us-east-1 \
      RAVEL_S3_ACCESS_KEY="$key" RAVEL_S3_SECRET_KEY="$secret"
    if [ "$DRY_RUN" = 1 ]; then
      run "$RUSTFS_BIN" --address "127.0.0.1:PORT" "$WORK/rustfs-data"
    else
      mkdir -p "$WORK/rustfs-data"
      RUSTFS_ACCESS_KEY="$key" RUSTFS_SECRET_KEY="$secret" \
        "$RUSTFS_BIN" --address "127.0.0.1:$port" "$WORK/rustfs-data" </dev/null >"$WORK/rustfs.log" 2>&1 &
      RUSTFS_PID=$!
    fi
    run "$PY" "$LIB" s3 wait --timeout 120 || fail "RustFS did not answer"
    local i ok=0
    for i in 1 2 3 4 5 6 7 8 9 10; do
      run "$PY" "$LIB" s3 mkbucket && { ok=1; break; }
      sleep 1
    done
    [ "$ok" = 1 ] || fail "create bucket"
  else
    export RAVEL_S3_AUTH=instance-role RAVEL_S3_BUCKET="$STORE_BUCKET" RAVEL_S3_REGION="$REGION"
    local listing="$WORK/store-listing.json"
    run aws s3api list-objects-v2 --region "$REGION" --bucket "$STORE_BUCKET" --max-items 1 --output json \
      >"$listing" || fail "list store_bucket"
    if [ "$DRY_RUN" = 0 ] && grep -q '"Key"' "$listing"; then
      fail "store_bucket $STORE_BUCKET is not empty"
    fi
  fi
  run "${CLI[@]}" store qualify >"$WORK/qualify.out" 2>&1 || fail "store qualify"
  stamp store.json lane="$LANE" bucket="${RAVEL_S3_BUCKET:-}" qualified:=true empty_at_start:=true at="$(utc_now)"
}

step_host() {
  step host
  local itype=local
  if [ "$LANE" = s3 ] && [ "$DRY_RUN" = 0 ]; then
    local token
    token="$(curl -sS --max-time 5 -X PUT http://169.254.169.254/latest/api/token \
      -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')"
    itype="$(curl -sS --max-time 5 -H "X-aws-ec2-metadata-token: $token" \
      http://169.254.169.254/latest/meta-data/instance-type)" || itype=unknown
  fi
  [ "$DRY_RUN" = 1 ] || drop_page_cache || fail "page cache drop (the s3 lane needs root)"
  stamp host.json lane="$LANE" arch="$(uname -m)" nproc:="$(nproc)" kernel="$(uname -r)" \
    mem_total_kb:="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)" instance_type="$itype" \
    page_cache_drop="${CACHE_DROP:-dry-run}" disk="$(df -h "$OUT" 2>/dev/null | tail -n 1)" \
    rq_stats="$RQ_STATS" at="$(utc_now)"
}

step_corpus() {
  step corpus
  local h0 extra_traces
  h0="$(now_hour)"
  extra_traces=$((TRACES / HOURS))
  [ "$extra_traces" -ge 1 ] || extra_traces=1
  run "$PY" "$INV_DIR/gen_corpus.py" --traces "$TRACES" --hours "$HOURS" --seed "$SEED" \
    --spans-per-trace "$SPANS_PER" --logs-per-trace "$LOGS_PER" --start-hour $((h0 - HOURS)) \
    --format parquet --out "$WORK/corpus" >"$WORK/corpus.out" 2>&1 || fail "gen_corpus base"
  # The mixed state's extra hour: noise only, a different seed so no trace id
  # repeats, event time the hour after the base corpus.
  run "$PY" "$INV_DIR/gen_corpus.py" --traces "$extra_traces" --hours 1 --seed $((SEED + 1)) \
    --spans-per-trace "$SPANS_PER" --logs-per-trace "$LOGS_PER" --start-hour "$h0" --noise-only \
    --format parquet --out "$WORK/corpus-extra" >"$WORK/corpus-extra.out" 2>&1 || fail "gen_corpus extra"
  run cp "$WORK/corpus/truth.json" "$OUT/truth.json" || fail "copy truth.json"
  stamp corpus.json traces:="$TRACES" hours:="$HOURS" seed:="$SEED" start_hour:=$((h0 - HOURS)) \
    extra_traces:="$extra_traces" extra_seed:=$((SEED + 1)) extra_start_hour:="$h0" \
    truth_sha256="$(sha256 "$OUT/truth.json")"
}

step_declare() {
  step declare
  run "${CLI[@]}" typed-attr-column set "$TENANT" --from-mapping "$WORK/corpus/logs.mapping.toml" \
    --key-index request_id --index-trace-id true --readers-rolled-out >"$WORK/declare.out" 2>&1 \
    || fail "typed-attr-column set $TENANT"
  run "${CLI[@]}" typed-attr-column show "$TENANT" >"$WORK/declare-show.out" 2>&1 || fail "typed-attr-column show"
  if [ "$HITS" = true ]; then
    run "${CLI[@]}" typed-attr-column set hits --from-mapping "$HITS_MAPPING" >>"$WORK/declare.out" 2>&1 \
      || fail "typed-attr-column set hits"
  fi
  stamp declare.json tenant="$TENANT" key_index=request_id index_trace_id:=true \
    from_mapping=logs.mapping.toml show="$( [ "$DRY_RUN" = 1 ] || cat "$WORK/declare-show.out")"
}

# load_signal HOUR SIGNAL CORPUS_DIR
load_signal() {
  local hour="$1" sig="$2" dir="$3" log="$WORK/load-$1-$2.out" m="$WORK/load-$1-$2.measure.json" code=0
  run "$PY" "$LIB" measure --out "$m" --stdout "$log" -- "${CLI[@]}" load --signal "$sig" \
    --parquet "$dir/$sig.parquet" --tenant "$TENANT" --mapping "$dir/$sig.mapping.toml" --shards "$SHARDS" \
    || code=$?
  stamp loads.jsonl hour="$hour" signal="$sig" tenant="$TENANT" exit:="$code" ingest_hour:="$(now_hour)" \
    measure@="$m" report%="$log"
  [ "$code" = 0 ] || fail "load $hour/$sig exit $code"
}

load_hits() {
  local ds="$BOX_ROOT/hits.parquet" url sha rows m="$WORK/load-hits.measure.json" code=0
  url="$(cfg hits.dataset_url)" || fail "run.toml hits.dataset_url"
  sha="$(cfg hits.dataset_sha256)" || fail "run.toml hits.dataset_sha256"
  rows="$(cfg hits.rows_expected)" || fail "run.toml hits.rows_expected"
  run curl -sSfL "$url" -o "$ds" || fail "download $url"
  [ "$DRY_RUN" = 1 ] || [ "$(sha256 "$ds")" = "$sha" ] || fail "hits dataset sha256 is not $sha"
  run "$PY" "$LIB" measure --out "$m" --stdout "$WORK/load-hits.out" -- "${CLI[@]}" load \
    --parquet "$ds" --tenant hits --mapping "$HITS_MAPPING" --shards "$SHARDS" --read-cursors 8 \
    --batch-rows 65536 || code=$?
  stamp loads.jsonl hour=base signal=hits tenant=hits exit:="$code" rows_expected:="$rows" \
    measure@="$m" report%="$WORK/load-hits.out"
  [ "$code" = 0 ] || fail "load hits exit $code"
}

step_load_base() {
  step load-base
  load_signal base spans "$WORK/corpus"
  load_signal base logs "$WORK/corpus"
  [ "$HITS" != true ] || load_hits
}

# fold TRANSITION [--key-index-leaves]: one catalog fold per signal, sealing
# through the current hour (--writers-stopped: nothing else writes).
fold() {
  local transition="$1" sig code log m
  shift
  stop_server
  for sig in spans logs; do
    log="$WORK/fold-$transition-$sig.out"
    m="$WORK/fold-$transition-$sig.measure.json"
    code=0
    run "$PY" "$LIB" measure --out "$m" --stdout "$log" -- "${CLI[@]}" catalog fold --tenant "$TENANT" \
      --shards "$SHARDS" --signal "$sig" --writers-stopped --json "$@" || code=$?
    stamp folds.jsonl transition="$transition" signal="$sig" kind="fold${1:+ $1}" exit:="$code" \
      hour:="$(now_hour)" measure@="$m" report%="$log"
    [ "$code" = 0 ] || fail "fold $transition/$sig exit $code"
  done
  FOLD_HOUR="$(now_hour)"
}

# rebuild_leaves: attach key-index leaves to the parts already in HEAD. A
# --key-index-leaves fold right after a plain one has no hour left to seal, so
# it re-encodes nothing and writes no leaf (its report says no_op).
rebuild_leaves() {
  local sig code log m
  stop_server
  for sig in spans logs; do
    log="$WORK/fold-leaf_rebuild-$sig.out"
    m="$WORK/fold-leaf_rebuild-$sig.measure.json"
    code=0
    run "$PY" "$LIB" measure --out "$m" --stdout "$log" -- "${CLI[@]}" catalog fold --tenant "$TENANT" \
      --shards "$SHARDS" --signal "$sig" --rebuild-key-index --json || code=$?
    stamp folds.jsonl transition=leaf_rebuild signal="$sig" kind=rebuild-key-index exit:="$code" \
      hour:="$(now_hour)" measure@="$m" report%="$log"
    [ "$code" = 0 ] || fail "rebuild-key-index $sig exit $code"
  done
}

census() {
  local state="$1" f="$WORK/census-$1.json"
  if [ "$LANE" = local ]; then
    run "$PY" "$LIB" s3 census --out "$f" || fail "census $state"
  else
    run aws s3api list-objects-v2 --region "$REGION" --bucket "$STORE_BUCKET" --output json \
      >"$WORK/listing-$state.json" || fail "list store_bucket for census $state"
    run "$PY" "$LIB" census-listing --listing "$WORK/listing-$state.json" --bucket "$STORE_BUCKET" \
      --out "$f" || fail "census $state"
  fi
  stamp census.jsonl state="$state" census@="$f"
}

# statements STATE [UNSUPPORTED_REASON]: every planned call of STATE.
statements() {
  local state="$1" reason="${2:-}" plan="$WORK/plan-$1.jsonl" line flags=()
  local args=(plan --statements "$STATEMENTS" --state "$state" --runs "$COLD_RUNS" --warm-runs "$WARM_RUNS"
    --out "$plan")
  if [ -n "$reason" ]; then
    args+=(--unsupported-reason "$reason")
  else
    args+=(--truth "$OUT/truth.json")
  fi
  [ "$HITS" != true ] || args+=(--hits --hits-corpus "$HITS_CORPUS")
  run "$PY" "$LIB" "${args[@]}" >/dev/null || fail "plan $state"
  [ "$RQ_STATS" != 1 ] || flags=(--rq-stats)
  if [ "$DRY_RUN" = 1 ]; then
    if [ -n "$reason" ]; then
      printf 'DRY-RUN: per planned statement: inv.py call (outcome unsupported-on-this-build: %s)\n' "$reason"
    else
      printf 'DRY-RUN: per planned cold call: stop server, drop page cache, start server, wait for readyz and the cache warmup\n'
      stamp restarts.jsonl
      printf 'DRY-RUN: per planned call: RQ_STATS=%s inv.py call --rq-stats into calls.jsonl\n' "$RQ_STATS"
    fi
    return 0
  fi
  while IFS= read -r line; do
    case "$line" in
      *'"unsupported_reason"'*) ;;
      *'"temp": "cold"'*) restart_for "$line" ;;
    esac
    "$PY" "$LIB" call --line "$line" --server "http://127.0.0.1:$HTTP_PORT" --auth-dir "$AUTH_DIR" \
      --pid "${SERVER_PID:-0}" --calls "$CALLS" "${flags[@]}" || fail "call against an unreachable server"
  done <"$plan"
}

transition() {
  stamp transitions.jsonl state="$1" at="$(utc_now)" ingest_hour:="$(now_hour)" note="$2"
}

# wait_next_hour: the mixed state's extra hour must land in an ingest hour
# the last fold did not seal; a commit into a sealed hour stays invisible.
wait_next_hour() {
  local deadline=$(($(date +%s) + MIXED_WAIT * 60))
  if [ "$DRY_RUN" = 1 ]; then
    printf 'DRY-RUN: wait until the ingest hour is past the last fold hour (at most %s minutes)\n' "$MIXED_WAIT"
    return 0
  fi
  while [ "$(( ($(date +%s) - 5) / 3600 ))" -le "$FOLD_HOUR" ]; do
    [ "$(date +%s)" -lt "$deadline" ] || fail "the hour after $FOLD_HOUR did not start within $MIXED_WAIT minutes"
    sleep 20
  done
}

run_state() {
  local state="$1" reason
  step "state:$state"
  reason="$(cfg "unsupported_states.$state" --default "")"
  if [ -n "$reason" ]; then
    transition "$state" "unsupported-on-this-build: $reason"
    statements "$state" "$reason"
    return 0
  fi
  case "$state" in
    unfolded)
      transition unfolded "base hours loaded, no fold" ;;
    folded_no_leaves)
      fold folded_no_leaves
      transition folded_no_leaves "catalog fold --writers-stopped, no leaves" ;;
    folded_leaves)
      fold folded_leaves --key-index-leaves
      rebuild_leaves
      transition folded_leaves "catalog fold --writers-stopped --key-index-leaves, then catalog fold --rebuild-key-index" ;;
    mixed)
      wait_next_hour
      load_signal extra spans "$WORK/corpus-extra"
      load_signal extra logs "$WORK/corpus-extra"
      transition mixed "folded hours plus one unfolded extra hour" ;;
    *) fail "unknown state $state" ;;
  esac
  census "$state"
  statements "$state"
  if [ "$state" = mixed ]; then
    stop_server
    fold fold_advance --key-index-leaves
    transition fold_advance "the fold that seals the mixed state's extra hour"
    census fold_advance
  fi
}

step_evaluate() {
  step evaluate
  stop_server
  local code=0
  run "$PY" "$INV_DIR/evaluate.py" --run "$OUT" >"$WORK/evaluate.out" 2>&1 || code=$?
  case "$code" in
    0) VERDICT=pass ;;
    1) VERDICT=fail ;;
    *) fail "evaluate.py exit $code" ;;
  esac
  [ "$DRY_RUN" = 1 ] || cat "$WORK/evaluate.out"
}

finish() {
  step finish
  FINISHED=1
  stop_server
  stop_store
  if [ "$DRY_RUN" = 1 ]; then
    printf 'DRY-RUN: write DONE\n'
    [ "$LANE" = local ] || printf 'DRY-RUN: upload out/ to %s, then terminate the instance\n' "$OUT_URI"
    exit 0
  fi
  printf 'DONE verdict=%s at=%s\n' "$VERDICT" "$(utc_now)" >"$OUT/DONE"
  if [ "$LANE" = s3 ]; then
    upload_out || { FINISHED=0; fail "upload out/"; }
    terminate_self
  fi
  [ "$VERDICT" = pass ]
  exit $?
}

main() {
  load_config
  [ "$DRY_RUN" = 1 ] || : >"$CALLS"
  step_bootstrap
  step_store
  step_host
  step_corpus
  step_declare
  step_load_base
  local s
  for s in $STATES; do
    run_state "$s"
  done
  step_evaluate
  finish
}

main
