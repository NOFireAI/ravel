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
  "$PY" "$LIB" stamp --file "$STAMPS/$file" ${append[@]+"${append[@]}"} "$@" || fail "stamp $file"
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

# upload_out: every result except the DONE/FAILED marker, which
# upload_marker puts on its own once this has exited 0, so a prefix holding
# a marker holds the results it describes.
upload_out() {
  [ "$LANE" = s3 ] && [ "$DRY_RUN" = 0 ] || return 0
  aws s3 cp --region "$REGION" --only-show-errors --recursive --exclude 'work/*' --exclude DONE \
    --exclude FAILED "$OUT" "$OUT_URI/" || return 1
  aws s3 cp --region "$REGION" --only-show-errors "$PROGRESS" "$OUT_URI/progress.log"
}

upload_marker() {
  [ "$LANE" = s3 ] && [ "$DRY_RUN" = 0 ] || return 0
  aws s3 cp --region "$REGION" --only-show-errors "$OUT/$1" "$OUT_URI/$1"
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
    keep_run_log
    upload_out && upload_marker FAILED
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
    --audit-mode off ${SERVER_EXTRA[@]+"${SERVER_EXTRA[@]}"} </dev/null >"$SERVER_LOG" 2>&1 &
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
  # Regexes with groups live in variables: bash 3.2 cannot parse them inline.
  local re_statement='"statement": "([^"]+)"' re_state='"state": "([^"]+)"' re_try='"try": ([0-9]+)'
  [[ "$line" =~ $re_statement ]] && statement="${BASH_REMATCH[1]}"
  [[ "$line" =~ $re_state ]] && state="${BASH_REMATCH[1]}"
  [[ "$line" =~ $re_try ]] && try="${BASH_REMATCH[1]}"
  if [ "$DRY_RUN" = 1 ]; then
    printf 'DRY-RUN: restart the server for %s/%s, drop the page cache, wait for the warmup\n' "$statement" "$state"
    return 0
  fi
  local t0
  t0="$(date +%s.%N)"
  stop_server
  drop_page_cache || fail "page cache drop before $statement/$state/$try"
  start_server || fail "server start before $statement/$state/$try"
  warm="$("$PY" "$LIB" warmup --log "$SERVER_LOG" --timeout 300)" || fail "warmup of server $SERVER_N"
  "$PY" "$LIB" stamp --file "$STAMPS/restarts.jsonl" --append statement="$statement" state="$state" \
    try:="$try" server_n:="$SERVER_N" pid:="$SERVER_PID" cache_drop="$CACHE_DROP" \
    warmup:="$warm" restart_s:="$(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN { printf "%.3f", b - a }')" \
    at="$(utc_now)" || fail "stamp restarts.jsonl"
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
  # Set by fold(); empty until a fold ran, which the mixed state checks
  # before it waits (set -u would otherwise end the run there).
  FOLD_HOUR=""
  RUN_MAX_MINUTES="$(cfg run.max_minutes --default 540)"
  STEP_ESTIMATE_DEFAULT="$(cfg step_estimate_minutes.default --default 30)"
  NEEDLE_MAX="$(cfg run.needle_blocks_max_objects --default 0)"
  SPLIT_BASE="$(cfg run.split_base_load --default false)"
  MP_TPH="$(cfg many_parts.traces_per_hour --default 10)"
  MP_MAX_ENTRIES="$(cfg many_parts.snapshot_part_max_entries --default 1)"
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
  # tenant:store-signal:label for every fold, leaf rebuild and inspect stamp
  # of the shared states; a many_parts_N state folds its own tenant alone.
  TRIPLES="$TENANT:spans:spans $TENANT:logs:logs"
  [ "$HITS" != true ] || TRIPLES="$TRIPLES hits:logs:hits"
  TENANTS="$TENANT"
  [ "$HITS" != true ] || TENANTS="$TENANTS hits"
  MP_SIZES=""
  local s
  for s in $STATES; do
    case "$s" in
      many_parts_*)
        [[ "${s#many_parts_}" =~ ^[1-9][0-9]*$ ]] || fail "run.toml state $s: N must be a positive integer"
        MP_SIZES="$MP_SIZES ${s#many_parts_}"
        TENANTS="$TENANTS mp${s#many_parts_}" ;;
      unfolded | folded_no_leaves | folded_leaves | mixed | compaction_in_flight | erasure_in_flight) ;;
      *) fail "run.toml run.states: unknown state $s" ;;
    esac
    estimate_s "$s" >/dev/null || fail "run.toml step_estimate_minutes.$s"
  done
  SERVER_FLAGS="--mode query --store s3 --shards $SHARDS --tenant-hash-unkeyed --audit-mode off ${SERVER_EXTRA[*]+${SERVER_EXTRA[*]}}"
  RUN_START="$(date +%s)"
}

# build_pkg NAME CARGO_ARGS...: one release build of the clone from inside
# it, with frame pointers, into the shared target; its output goes to
# out/build-NAME.log so a failed build travels with the FAILED marker.
build_pkg() {
  local name="$1"
  shift
  # One command for the dry run and the real run, so the dry run shows the
  # directory the build runs in (env -C: coreutils 8.28 and later; Amazon
  # Linux 2023 ships 9.x).
  local cmd=(env -C "$BOX_ROOT/ravel" CARGO_TARGET_DIR="$BOX_ROOT/target-fp" RUSTFLAGS="-C force-frame-pointers=yes"
    cargo build --release --locked "$@")
  if [ "$DRY_RUN" = 1 ]; then
    run ${cmd[@]+"${cmd[@]}"}
    return 0
  fi
  mkdir -p "$OUT"
  run ${cmd[@]+"${cmd[@]}"} >"$OUT/build-$name.log" 2>&1
}

# keep_run_log: copy the box's own run.log (launch.sh's user-data redirects
# run.sh's output there, outside out/) into out/, so one upload carries
# whatever a FAILED marker names.
keep_run_log() {
  [ "$LANE" = s3 ] && [ "$DRY_RUN" = 0 ] || return 0
  [ -f "$BOX_ROOT/run.log" ] || return 0
  mkdir -p "$OUT"
  cp "$BOX_ROOT/run.log" "$OUT/run.log" 2>/dev/null || true
}

step_bootstrap() {
  step bootstrap
  if [ "$LANE" = s3 ]; then
    local src="$BOX_ROOT/ravel" target="$BOX_ROOT/target-fp"
    run dnf install -y git gcc gcc-c++ make cmake pkgconfig openssl-devel perl jq python3.11 python3.11-pip \
      || fail "dnf install"
    run "$PY" -m pip install --quiet --target "$PYLIB" pyarrow==17.0.0 || fail "pip install pyarrow"
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
    # routes (404) on this tree. Each runs inside the clone (build_pkg):
    # rustup reads rust-toolchain.toml from the current directory, not from
    # --manifest-path, and the installer above sets no default toolchain.
    build_pkg ravel-cli -p ravel-cli || fail "build ravel-cli (out/build-ravel-cli.log)"
    build_pkg ravel-server -p ravel-server --features sql || fail "build ravel-server (out/build-ravel-server.log)"
    run install -D -m 0755 "$target/release/ravel-cli" "$BIN" || fail "install ravel-cli"
    run install -D -m 0755 "$target/release/ravel-server" "$SERVER_BIN" || fail "install ravel-server"
    stamp build.json lane=s3 sha="$SHA" build="separate cargo build --release --locked per package, RUSTFLAGS=-C force-frame-pointers=yes, ravel-server --features sql" \
      ravel_cli_sha256="$(sha256 "$BIN")" ravel_server_sha256="$(sha256 "$SERVER_BIN")" \
      rustc="$([ "$DRY_RUN" = 1 ] && echo dry-run || (cd "$src" && rustc --version))"
  else
    local sha dirty porcelain
    sha="$(git -C "$INV_DIR" rev-parse HEAD 2>/dev/null)" || sha=unknown
    # A git status that fails is no count: null, which evaluate.py reports
    # missing.
    if porcelain="$(git -C "$INV_DIR" status --porcelain -- . 2>/dev/null)"; then
      dirty=0
      [ -z "$porcelain" ] || dirty="$(printf '%s\n' "$porcelain" | wc -l | tr -d ' ')"
    else
      dirty=null
    fi
    [ "$DRY_RUN" = 0 ] || printf 'DRY-RUN: build.json dirty_paths %s\n' "$dirty"
    stamp build.json lane=local sha="$sha" dirty_paths:="$dirty" \
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
  local t tok
  (
    umask 077
    : >"$TOKEN_FILE"
    for t in $TENANTS; do
      tok="$("$PY" -c 'import secrets; print(secrets.token_hex(24))')" || exit 1
      printf '%s=%s\n' "$tok" "$t" >>"$TOKEN_FILE"
      printf 'Authorization: Bearer %s\n' "$tok" >"$AUTH_DIR/$t.auth"
    done
  )
}

# stamp_tenants: every tenant's hash, for the per-tenant census.
stamp_tenants() {
  local t out fields=() re_hash='t/([0-9a-f]+)/'
  for t in $TENANTS; do
    if [ "$DRY_RUN" = 1 ]; then
      run "${CLI[@]}" tenancy resolve "$t"
      continue
    fi
    out="$("${CLI[@]}" tenancy resolve "$t" 2>&1)" || fail "tenancy resolve $t: $out"
    [[ "$out" =~ $re_hash ]] || fail "tenancy resolve $t printed no t/<hash>/: $out"
    fields+=("$t=${BASH_REMATCH[1]}")
  done
  stamp tenants.json ${fields[@]+"${fields[@]}"}
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
  stamp_tenants
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
  local h0 extra_traces n subs=() mp="{}"
  h0="$(now_hour)"
  CORPUS_H0="$h0"
  extra_traces=$((TRACES / HOURS))
  [ "$extra_traces" -ge 1 ] || extra_traces=1
  run "$PY" "$INV_DIR/gen_corpus.py" --traces "$TRACES" --hours "$HOURS" --seed "$SEED" \
    --spans-per-trace "$SPANS_PER" --logs-per-trace "$LOGS_PER" --start-hour $((h0 - HOURS)) \
    --format parquet --out "$WORK/corpus" >"$WORK/corpus.out" 2>&1 || fail "gen_corpus base"
  # The mixed state's extra hour: planted like the base corpus (its own saved
  # traces, request id, needles and failing cell), a different seed so no
  # trace id repeats, event time the hour after the base corpus.
  run "$PY" "$INV_DIR/gen_corpus.py" --traces "$extra_traces" --hours 1 --seed $((SEED + 1)) \
    --spans-per-trace "$SPANS_PER" --logs-per-trace "$LOGS_PER" --start-hour "$h0" --tail \
    --format parquet --out "$WORK/corpus-tail" >"$WORK/corpus-tail.out" 2>&1 || fail "gen_corpus tail"
  subs+=(--sub "tail=$WORK/corpus-tail/truth.json")
  # Each many_parts_N tenant's corpus: N event hours ending four hours before
  # this one, so its strided ingest hours (one later than the event hours)
  # all end below the hour the state runs in.
  for n in $MP_SIZES; do
    run "$PY" "$INV_DIR/gen_corpus.py" --traces $((n * MP_TPH)) --hours "$n" --seed $((SEED + n)) \
      --spans-per-trace "$SPANS_PER" --logs-per-trace "$LOGS_PER" --start-hour $((h0 - n - 4)) \
      --format parquet --out "$WORK/corpus-mp$n" >"$WORK/corpus-mp$n.out" 2>&1 || fail "gen_corpus many_parts_$n"
    subs+=(--sub "many_parts_$n=$WORK/corpus-mp$n/truth.json")
    mp="$("$HOST_PY" -c 'import json, sys; d = json.loads(sys.argv[1]); d[sys.argv[2]] = {"traces": int(sys.argv[3]), "hours": int(sys.argv[2]), "seed": int(sys.argv[4]), "start_hour": int(sys.argv[5]), "ingest_hour_start": int(sys.argv[5]) + 1}; print(json.dumps(d))' \
      "$mp" "$n" $((n * MP_TPH)) $((SEED + n)) $((h0 - n - 4)))" || fail "many_parts corpus record"
  done
  run "$PY" "$LIB" merge-truth --base "$WORK/corpus/truth.json" ${subs[@]+"${subs[@]}"} --out "$OUT/truth.json" \
    || fail "merge-truth"
  stamp corpus.json traces:="$TRACES" hours:="$HOURS" seed:="$SEED" start_hour:=$((h0 - HOURS)) \
    tail_traces:="$extra_traces" tail_seed:=$((SEED + 1)) tail_start_hour:="$h0" many_parts:="$mp" \
    truth_sha256="$(sha256 "$OUT/truth.json")"
}

# corpus_rows DIR SIGNAL: rows gen_corpus wrote (a dry run prints ROWS).
corpus_rows() {
  [ "$DRY_RUN" = 1 ] && { echo ROWS; return 0; }
  "$PY" -c 'import json, sys; print(json.load(open(sys.argv[1]))["rows_written"][sys.argv[2]])' \
    "$1/truth.json" "$2" || fail "rows of $1/$2"
}

# batch_rows ROWS PARTS: ceil(ROWS / PARTS), symbolic in a dry run.
batch_rows() {
  [ "$DRY_RUN" = 1 ] && { echo "ceil($1/$2)"; return 0; }
  echo $((($1 + $2 - 1) / $2))
}

step_declare() {
  step declare
  run "${CLI[@]}" typed-attr-column set "$TENANT" --from-mapping "$WORK/corpus/logs.mapping.toml" \
    --key-index request_id --index-trace-id true --readers-rolled-out >"$WORK/declare.out" 2>&1 \
    || fail "typed-attr-column set $TENANT"
  run "${CLI[@]}" typed-attr-column show "$TENANT" >"$WORK/declare-show.out" 2>&1 || fail "typed-attr-column show"
  local hits_show=""
  if [ "$HITS" = true ]; then
    run "${CLI[@]}" typed-attr-column set hits --from-mapping "$HITS_MAPPING" --key-index UserID \
      --readers-rolled-out >>"$WORK/declare.out" 2>&1 || fail "typed-attr-column set hits"
    run "${CLI[@]}" typed-attr-column show hits >"$WORK/declare-show-hits.out" 2>&1 \
      || fail "typed-attr-column show hits"
    [ "$DRY_RUN" = 1 ] || hits_show="$(cat "$WORK/declare-show-hits.out")"
  fi
  stamp declare.json tenant="$TENANT" key_index=request_id index_trace_id:=true \
    from_mapping=logs.mapping.toml show="$( [ "$DRY_RUN" = 1 ] || cat "$WORK/declare-show.out")" \
    hits_key_index="$([ "$HITS" = true ] && echo UserID)" hits_show="$hits_show"
}

# load_signal LABEL SIGNAL CORPUS_DIR TENANT [LOAD_FLAG...]
load_signal() {
  local label="$1" sig="$2" dir="$3" tenant="$4" log m code=0
  shift 4
  log="$WORK/load-$label-$sig.out"
  m="$WORK/load-$label-$sig.measure.json"
  run "$PY" "$LIB" measure --out "$m" --stdout "$log" -- "${CLI[@]}" load --signal "$sig" \
    --parquet "$dir/$sig.parquet" --tenant "$tenant" --mapping "$dir/$sig.mapping.toml" --shards "$SHARDS" \
    "$@" || code=$?
  stamp loads.jsonl hour="$label" signal="$sig" tenant="$tenant" exit:="$code" ingest_hour:="$(now_hour)" \
    flags="$*" measure@="$m" report%="$log"
  [ "$code" = 0 ] || fail "load $label/$sig exit $code"
}

load_hits() {
  local ds="$BOX_ROOT/hits.parquet" url sha rows m="$WORK/load-hits.measure.json" code=0
  url="$(cfg hits.dataset_url)" || fail "run.toml hits.dataset_url"
  sha="$(cfg hits.dataset_sha256)" || fail "run.toml hits.dataset_sha256"
  rows="$(cfg hits.rows_expected)" || fail "run.toml hits.rows_expected"
  run curl -sSfL "$url" -o "$ds" || fail "download $url"
  [ "$DRY_RUN" = 1 ] || [ "$(sha256 "$ds")" = "$sha" ] || fail "hits dataset sha256 is not $sha"
  # The T12 load recipe (#3692): 398 objects at the ADR's object size.
  run "$PY" "$LIB" measure --out "$m" --stdout "$WORK/load-hits.out" -- "${CLI[@]}" load \
    --parquet "$ds" --tenant hits --mapping "$HITS_MAPPING" --shards "$SHARDS" --batch-rows 100000 \
    --target-bytes 1850000000 --max-flush-delay 30s --pipeline-depth 32 --load-memory-bytes 6500000000 \
    || code=$?
  stamp loads.jsonl hour=base signal=hits tenant=hits exit:="$code" rows_expected:="$rows" \
    measure@="$m" report%="$WORK/load-hits.out"
  [ "$code" = 0 ] || fail "load hits exit $code"
}

step_load_base() {
  step load-base
  local flags=() sig
  for sig in spans logs; do
    flags=()
    if [ "$SPLIT_BASE" = true ]; then
      # Two batches, each flushed alone: two L0 objects per shard and hour,
      # the inputs compaction_in_flight's compaction needs.
      flags=(--batch-rows "$(batch_rows "$(corpus_rows "$WORK/corpus" "$sig")" 2)" --target-bytes 1)
    fi
    load_signal base "$sig" "$WORK/corpus" "$TENANT" ${flags[@]+"${flags[@]}"}
  done
  [ "$HITS" != true ] || load_hits
}

# fold TRANSITION [FOLD_FLAG...]: one catalog fold per $TRIPLES entry,
# sealing through the current hour (--writers-stopped: nothing else writes).
fold() {
  local transition="$1" triple tenant sig label code log m
  shift
  stop_server
  for triple in $TRIPLES; do
    IFS=: read -r tenant sig label <<<"$triple"
    log="$WORK/fold-$transition-$label.out"
    m="$WORK/fold-$transition-$label.measure.json"
    code=0
    run "$PY" "$LIB" measure --out "$m" --stdout "$log" -- "${CLI[@]}" catalog fold --tenant "$tenant" \
      --shards "$SHARDS" --signal "$sig" --writers-stopped --json "$@" || code=$?
    stamp folds.jsonl transition="$transition" tenant="$tenant" signal="$label" kind="fold${1:+ $*}" \
      exit:="$code" hour:="$(now_hour)" measure@="$m" report%="$log"
    [ "$code" = 0 ] || fail "fold $transition/$label exit $code"
  done
  FOLD_HOUR="$(now_hour)"
}

# rebuild_leaves TRANSITION: attach key-index leaves to the parts already in
# HEAD, per $TRIPLES entry. A --key-index-leaves fold right after a plain one
# has no hour left to seal, so it re-encodes nothing and writes no leaf (its
# report says no_op).
rebuild_leaves() {
  local transition="$1" triple tenant sig label code log m
  stop_server
  for triple in $TRIPLES; do
    IFS=: read -r tenant sig label <<<"$triple"
    log="$WORK/fold-$transition-$label.out"
    m="$WORK/fold-$transition-$label.measure.json"
    code=0
    run "$PY" "$LIB" measure --out "$m" --stdout "$log" -- "${CLI[@]}" catalog fold --tenant "$tenant" \
      --shards "$SHARDS" --signal "$sig" --rebuild-key-index --json || code=$?
    stamp folds.jsonl transition="$transition" tenant="$tenant" signal="$label" kind=rebuild-key-index \
      exit:="$code" hour:="$(now_hour)" measure@="$m" report%="$log"
    [ "$code" = 0 ] || fail "rebuild-key-index $label exit $code"
  done
}

# census STATE: the store's census (classes, and per tenant and signal) and,
# for every $TRIPLES entry, the state's inspect stamps; then the binaries and
# server flags the state's calls run against.
census() {
  local state="$1" f="$WORK/census-$1.json" keys="$WORK/keys-$1.tsv" triple tenant sig label ranges
  local tenants=(--tenants "$STAMPS/tenants.json" --keys-out "$keys")
  if [ "$LANE" = local ]; then
    run "$PY" "$LIB" s3 census "${tenants[@]}" --out "$f" || fail "census $state"
  else
    run aws s3api list-objects-v2 --region "$REGION" --bucket "$STORE_BUCKET" --output json \
      >"$WORK/listing-$state.json" || fail "list store_bucket for census $state"
    run "$PY" "$LIB" census-listing --listing "$WORK/listing-$state.json" --bucket "$STORE_BUCKET" \
      "${tenants[@]}" --out "$f" || fail "census $state"
  fi
  stamp census.jsonl state="$state" census@="$f"
  for triple in $TRIPLES; do
    IFS=: read -r tenant sig label <<<"$triple"
    printf 'STAMP stamps/%s.%s.{catalog,object,kidx}.json stamps/%s.%s.tenant.txt\n' "$state" "$label" "$state" "$label"
    # Row 3 divides probe wire by the unfolded window's candidate objects,
    # counted from every data object's event range.
    ranges=()
    if [ "$state" = unfolded ]; then
      ranges=(--ranges-keys "$keys")
      printf 'STAMP stamps/%s.%s.ranges.json\n' "$state" "$label"
    fi
    run "$PY" "$LIB" inspect-stamp --cli-bin "$BIN" --census "$f" --tenant "$tenant" --signal "$sig" \
      --label "$label" --state "$state" --out-dir "$STAMPS" ${ranges[@]+"${ranges[@]}"} \
      || fail "inspect stamps $state/$label"
  done
  stamp "$state.server.json" state="$state" sha="${SHA:-$(git -C "$INV_DIR" rev-parse HEAD 2>/dev/null)}" \
    ravel_cli_sha256="$(sha256 "$BIN")" ravel_server_sha256="$(sha256 "$SERVER_BIN")" \
    server_flags="$SERVER_FLAGS" \
    fold_off="--mode query runs no scheduled fold or compaction; ravel-server refuses --disable-fold in query mode, so the flag is not passed"
}

# statements STATE [PLAN_FLAG...]: every planned call of STATE. A cold call
# follows a server restart; an inflight call (--temp inflight) does not. Past
# the run deadline every remaining call is recorded not-run, unsent.
statements() {
  local state="$1" plan="$WORK/plan-$1.jsonl" line flags=() deadline=$((RUN_START + RUN_MAX_MINUTES * 60))
  shift
  local args=(plan --statements "$STATEMENTS" --state "$state" --runs "$COLD_RUNS" --warm-runs "$WARM_RUNS"
    --truth "$OUT/truth.json" --out "$plan" "$@")
  [ "$HITS" != true ] || args+=(--hits --hits-corpus "$HITS_CORPUS")
  run "$PY" "$LIB" "${args[@]}" >/dev/null || fail "plan $state"
  [ "$RQ_STATS" != 1 ] || flags=(--rq-stats)
  if [ "$DRY_RUN" = 1 ]; then
    printf 'DRY-RUN: per planned cold call: stop server, drop page cache, start server, wait for readyz and the cache warmup\n'
    stamp restarts.jsonl
    printf 'DRY-RUN: per planned call: RQ_STATS=%s inv.py call --rq-stats --deadline-unix-s RUN_START+%s min into calls.jsonl\n' \
      "$RQ_STATS" "$RUN_MAX_MINUTES"
    return 0
  fi
  while IFS= read -r line; do
    case "$line" in
      *'"unsupported_reason"'* | *'"not_run_reason"'*) ;;
      *'"temp": "cold"'*) [ "$(date +%s)" -ge "$deadline" ] || restart_for "$line" ;;
    esac
    "$PY" "$LIB" call --line "$line" --server "http://127.0.0.1:$HTTP_PORT" --auth-dir "$AUTH_DIR" \
      --pid "${SERVER_PID:-0}" --calls "$CALLS" --deadline-unix-s "$deadline" ${flags[@]+"${flags[@]}"} \
      || fail "call against an unreachable server"
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

not_run() {
  printf 'NOT-RUN %s: %s\n' "$1" "$2"
  stamp not_run.jsonl state="$1" reason="$2" at="$(utc_now)"
}

# estimate_s STATE: the seconds one load, fold, leaf rebuild or compaction of
# STATE is taken to need at most, from run.toml [step_estimate_minutes]
# (STATE, else default, else 30 minutes). load_config checks every state's.
estimate_s() {
  local m
  m="$(cfg "step_estimate_minutes.$1" --default "$STEP_ESTIMATE_DEFAULT")" || return 1
  [[ "$m" =~ ^[0-9]+$ ]] || { printf 'run.toml step_estimate_minutes.%s = %s: not whole minutes\n' "$1" "$m" >&2; return 1; }
  printf '%s\n' "$((m * 60))"
}

# fits STATE STEP BOUND_S: true when the run deadline leaves BOUND_S seconds
# for STEP; otherwise STATE is recorded not-run, so evaluate and the upload
# still run before the instance's own cap.
fits() {
  local left=$((RUN_START + RUN_MAX_MINUTES * 60 - $(date +%s)))
  [ "$left" -lt "$3" ] || return 0
  not_run "$1" "deadline: $2 needs up to $3 s, $left s left of run.max_minutes $RUN_MAX_MINUTES"
  return 1
}

# needle_blocks: the logs needles' block counts as rlog inspect reads them,
# beside the generator's own count in truth.json (README "Needles").
needle_blocks() {
  run "$PY" "$LIB" needle-blocks --cli-bin "$BIN" --census "$WORK/census-unfolded.json" \
    --keys "$WORK/keys-unfolded.tsv" --tenant "$TENANT" --truth "$OUT/truth.json" --max-objects "$NEEDLE_MAX" \
    --out "$STAMPS/needle_blocks.json" || fail "needle-blocks"
  printf 'STAMP stamps/needle_blocks.json\n'
}

# compact_child: maintain compact-tenant over every sealed hour of both
# signals, run in the background while the in-flight statements run. The two
# signals compact at once, so each signal's calls can overlap its own
# compaction whichever order the statements run in.
compact_child() {
  local sig pid pids=() code=0
  for sig in spans logs; do
    "$PY" "$LIB" measure --out "$WORK/compact-$sig.measure.json" --stdout "$WORK/compact-$sig.out" -- \
      "${CLI[@]}" maintain compact-tenant --tenant "$TENANT" --signal "$sig" --shards "$SHARDS" \
      --max-flush-lifetime 0s --no-claim &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do
    wait "$pid" || code=$?
  done
  return "$code"
}

# wait_sealed: compact-tenant seals an hour at its end plus
# --max-flush-lifetime (0s) plus the compactor's 300 s clock-skew allowance,
# so every hour before the current one is compactable 310 s into it.
WAIT_SEALED_MAX_S=310
wait_sealed() {
  if [ "$DRY_RUN" = 1 ]; then
    printf 'DRY-RUN: wait until 310 s past the current hour start, so every earlier hour is sealed\n'
    return 0
  fi
  while [ "$(( $(date +%s) % 3600 ))" -lt "$WAIT_SEALED_MAX_S" ]; do
    sleep 10
  done
}

state_compaction() {
  local code=0 sig
  # The sealed-hour wait and the compaction run back to back: both must fit.
  local compact_s
  compact_s="$(estimate_s compaction_in_flight)" || fail "run.toml step_estimate_minutes.compaction_in_flight"
  fits compaction_in_flight "wait_sealed and compaction" "$((WAIT_SEALED_MAX_S + compact_s))" || return 0
  stop_server
  wait_sealed
  # One restart before the in-flight calls; they run against this server.
  restart_for '{"statement": "compaction_in_flight", "state": "compaction_in_flight", "try": 0}'
  transition compaction_in_flight "maintain compact-tenant running in the background"
  if [ "$DRY_RUN" = 1 ]; then
    for sig in spans logs; do
      run "$PY" "$LIB" measure --out "$WORK/compact-$sig.measure.json" --stdout "$WORK/compact-$sig.out" -- \
        "${CLI[@]}" maintain compact-tenant --tenant "$TENANT" --signal "$sig" --shards "$SHARDS" \
        --max-flush-lifetime 0s --no-claim
    done
    printf 'DRY-RUN: the compactions above run as background children, both signals at once, while the in-flight calls run\n'
    statements compaction_in_flight --temp inflight
    printf 'DRY-RUN: wait for the compaction child before the next transition\n'
  else
    compact_child </dev/null &
    local child=$!
    statements compaction_in_flight --temp inflight
    wait "$child" || code=$?
  fi
  for sig in spans logs; do
    stamp compaction.jsonl signal="$sig" exit:="$code" measure@="$WORK/compact-$sig.measure.json" \
      report:="$(run "$PY" "$LIB" kv-report --log "$WORK/compact-$sig.out" || echo null)"
  done
  [ "$code" = 0 ] || fail "maintain compact-tenant exit $code"
  transition compaction_done "maintain compact-tenant finished"
  census compaction_done
  statements compaction_done
}

state_erasure() {
  local rid out="$WORK/erase-submit.out" code=0
  rid="$("$PY" -c 'import json, sys; v = json.load(open(sys.argv[1])).get("erasure_victim"); print(v["request_id"] if v else "")' \
    "$OUT/truth.json" 2>/dev/null)"
  [ "$DRY_RUN" = 0 ] || rid=VICTIM_REQUEST_ID
  if [ -z "$rid" ]; then
    not_run erasure_in_flight "the corpus holds no erasure victim at this scale"
    return 0
  fi
  stop_server
  run "${CLI[@]}" erase status --tenant "$TENANT" --signal logs >"$WORK/erase-status-before.out" 2>&1 \
    || fail "erase status before submit"
  run "${CLI[@]}" erase submit --tenant "$TENANT" --signal logs --matcher "request_id=$rid" \
    --reason "investigations erasure_in_flight" >"$out" 2>&1 || code=$?
  [ "$code" = 0 ] || fail "erase submit exit $code"
  run "${CLI[@]}" erase status --tenant "$TENANT" --signal logs >"$WORK/erase-status-after.out" 2>&1 \
    || fail "erase status after submit"
  # ravel-cli erase status prints "<request id>: PENDING" for a request with
  # a .dreq and no .done (services/ravel-cli/src/erase.rs print_state).
  if [ "$DRY_RUN" = 0 ] && ! grep -q ': PENDING$' "$WORK/erase-status-after.out"; then
    fail "erase status after submit shows no PENDING request"
  fi
  stamp erasure.jsonl victim_request_id="$rid" \
    submit:="$(run "$PY" "$LIB" kv-report --log "$out" || echo null)" \
    status_before:="$(run "$PY" "$LIB" kv-report --log "$WORK/erase-status-before.out" || echo null)" \
    status_after:="$(run "$PY" "$LIB" kv-report --log "$WORK/erase-status-after.out" || echo null)"
  transition erasure_in_flight "erase submit on the victim's request_id; erase status reads pending"
  census erasure_in_flight
  statements erasure_in_flight
}

# state_many_parts N: a fresh spans tenant mpN, one batch per strided ingest
# hour, folded into parts of at most MP_MAX_ENTRIES entries.
state_many_parts() {
  local n="$1" t="mp$1" dir="$WORK/corpus-mp$1" saved="$TRIPLES" start
  local est
  est="$(estimate_s "many_parts_$n")"
  start="$("$HOST_PY" -c 'import sys; print(int(sys.argv[1]) - int(sys.argv[2]) - 3)' "$CORPUS_H0" "$n")"
  fits "many_parts_$n" load "$est" || return 0
  stop_server
  load_signal "many_parts_$n" spans "$dir" "$t" --ingest-hour-stride 1 --ingest-hour-start "$start" \
    --batch-rows "$(batch_rows "$(corpus_rows "$dir" spans)" "$n")" --target-bytes 1
  TRIPLES="$t:spans:spans"
  fits "many_parts_$n" fold "$est" || { TRIPLES="$saved"; return 0; }
  fold "many_parts_$n" --snapshot-part-max-entries "$MP_MAX_ENTRIES"
  fits "many_parts_$n" "leaf rebuild" "$est" || { TRIPLES="$saved"; return 0; }
  rebuild_leaves "many_parts_${n}_leaf_rebuild"
  transition "many_parts_$n" "tenant $t: $n strided ingest hours from $start, parts of at most $MP_MAX_ENTRIES entries"
  census "many_parts_$n"
  statements "many_parts_$n" --tenant "$t" --sub "many_parts_$n"
  TRIPLES="$saved"
}

run_state() {
  local state="$1"
  step "state:$state"
  case "$state" in
    unfolded)
      transition unfolded "base hours loaded, no fold"
      census unfolded
      needle_blocks
      statements unfolded ;;
    folded_no_leaves)
      fits folded_no_leaves fold "$(estimate_s folded_no_leaves)" || return 0
      fold folded_no_leaves
      transition folded_no_leaves "catalog fold --writers-stopped, no leaves"
      census folded_no_leaves
      statements folded_no_leaves ;;
    folded_leaves)
      fits folded_leaves fold "$(estimate_s folded_leaves)" || return 0
      fold folded_leaves --key-index-leaves
      fits folded_leaves "leaf rebuild" "$(estimate_s folded_leaves)" || return 0
      rebuild_leaves leaf_rebuild
      transition folded_leaves "catalog fold --writers-stopped --key-index-leaves, then catalog fold --rebuild-key-index"
      census folded_leaves
      statements folded_leaves ;;
    mixed)
      fits mixed "the wait for the next ingest hour" "$((MIXED_WAIT * 60))" || return 0
      if [ -z "$FOLD_HOUR" ]; then
        not_run mixed "no fold ran before the mixed state (its fold states were skipped or not listed), so the tail hour has no sealed hour to follow"
        return 0
      fi
      wait_next_hour
      fits mixed load "$(estimate_s mixed)" || return 0
      stop_server
      load_signal tail spans "$WORK/corpus-tail" "$TENANT"
      load_signal tail logs "$WORK/corpus-tail" "$TENANT"
      transition mixed "folded hours plus one unfolded tail hour"
      census mixed
      statements mixed
      fits fold_advance fold "$(estimate_s mixed)" || return 0
      fold fold_advance --key-index-leaves
      transition fold_advance "the fold that seals the mixed state's tail hour"
      census fold_advance ;;
    compaction_in_flight) state_compaction ;;
    erasure_in_flight) state_erasure ;;
    many_parts_*) state_many_parts "${state#many_parts_}" ;;
    *) fail "unknown state $state" ;;
  esac
}

step_evaluate() {
  step evaluate
  stop_server
  local code=0
  run "$PY" "$INV_DIR/evaluate.py" --run "$OUT" >"$WORK/evaluate.out" 2>&1 || code=$?
  case "$code" in
    0) VERDICT=pass ;;
    1)
      # fail or incomplete: the results file names which.
      VERDICT=fail
      [ "$DRY_RUN" = 1 ] || VERDICT="$("$HOST_PY" -c 'import json, sys; print(json.load(open(sys.argv[1]))["verdict"])' \
        "$OUT/results.$LANE.json")" || fail "read the verdict from results.$LANE.json" ;;
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
    [ "$LANE" = local ] || printf 'DRY-RUN: upload out/ (no DONE, FAILED or work/) to %s\n' "$OUT_URI"
    printf 'DRY-RUN: write DONE\n'
    [ "$LANE" = local ] || printf 'DRY-RUN: put DONE to %s/DONE once the upload exited 0, then terminate the instance\n' "$OUT_URI"
    exit 0
  fi
  if [ "$LANE" = s3 ]; then
    keep_run_log
    upload_out || { FINISHED=0; fail "upload out/"; }
  fi
  printf 'DONE verdict=%s at=%s\n' "$VERDICT" "$(utc_now)" >"$OUT/DONE"
  if [ "$LANE" = s3 ]; then
    upload_marker DONE || { FINISHED=0; fail "put DONE"; }
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
  local s elapsed
  for s in $STATES; do
    elapsed=$(($(date +%s) - RUN_START))
    if [ "$elapsed" -ge $((RUN_MAX_MINUTES * 60)) ]; then
      not_run "$s" "deadline: run.max_minutes $RUN_MAX_MINUTES passed ($((elapsed / 60)) minutes elapsed)"
      continue
    fi
    run_state "$s"
  done
  step_evaluate
  finish
}

main
