#!/usr/bin/env bash
# Run one arm of a ClickBench kit run on its own box (ADR-2867 change 2).
#
# launch.sh's user-data starts this as root under nohup once it has verified
# the sha256 of every kit file. It reads the arm from /root/kit/ARM, the run
# id from /root/kit/RUN_ID and everything else from /root/kit/run.toml, and
# runs these steps in order, each stamped `<UTC> step=<name> state=...` in
# /root/kit/progress.log:
#
#   bootstrap   runbook section 6: dnf packages, rustup, a clone of the arm's
#               SHA, a frame-pointer release build of ravel-cli and
#               ravel-server (feature sql) copied to /root/bin/<name>-<sha>,
#               RustFS from run.toml's rustfs_url checked against
#               rustfs_sha256, and hits.parquet downloaded during the build
#   store       RustFS on 127.0.0.1:9000 with keys generated on the box, a
#               bucket, and `store qualify`
#   arm-env     /root/kit/arm.env: the binaries and every server flag, written
#               once; every server start sources it, so a restart runs the
#               same flags
#   load        runbook section 7: load, compact, fold with --writers-stopped
#               and declare, with store object counts before and after the
#               load; the fold must report seal_through_hour and an
#               entry_count of at least fold_min_entry_ratio (default 0.8) of
#               the objects the load wrote, and its figures are stamped
#   phases      each phase run.toml names, in order; phase NAME is the
#               function phase_NAME and writes /root/kit/out/NAME/
#   results     /root/kit/results.json (SqlLatencyReport's entries and failed
#               rows, which the runbook's analyse.py reads, plus the
#               provenance stamps and the absolute bands when run.toml's
#               band_sections selects them), evidence.tgz, then both and
#               progress.log uploaded to
#               s3://<bucket>/<prefix>/runs/<run-id>/out/<arm>/
#
# It ends by uploading DONE (or FAILED, naming the step that failed) beside
# those files and terminating its own instance through the instance role,
# falling back to `shutdown -h now`, which the launch's terminate behaviour
# turns into a termination. An absolute band miss is a verdict, not a
# failure: DONE records `bands=miss` and fetch.sh exits 1 on it.
#
# Every statement goes through run_query, which records one JSON line per call
# in out/<phase>/calls.jsonl: statement, arm, phase, start, duration, HTTP
# status, rows, outcome and refusal class. A 200 answer whose row count is not
# the expected one (the corpus's expected_rows, else the first try's) is an
# error of class wrong_answer, not a success. Every request body names its
# window: start window_hours (default 200000, the runbook's --window-hours)
# before end, the epoch second the phase began. A new phase is a phase_NAME
# function plus its name in kit.py's KNOWN_PHASES; wrapping run_query wraps
# every call.
#
# KIT_ROOT (default /root) relocates every path, and KIT_RUN_SH_LIB=1 defines
# the functions without running main; kit.test.sh uses both.

set -uo pipefail

KIT_ROOT="${KIT_ROOT:-/root}"
KIT="$KIT_ROOT/kit"
SRC="$KIT/kit"
PY_KIT="$SRC/lib/kit.py"
RUN_TOML="$KIT/run.toml"
OUT="$KIT/out"
STAMPS="$KIT/stamps"
PROGRESS="$KIT/progress.log"
ARM_ENV="$KIT/arm.env"
SECRETS_ENV="$KIT/secrets.env"
AUTH_HEADER="$KIT/auth.header"
TOKEN_FILE="$KIT/tenant-tokens"
RAVEL_DIR="$KIT_ROOT/ravel"
BIN_DIR="$KIT_ROOT/bin"
DATASET="$KIT_ROOT/hits.parquet"
RUSTFS_DATA="$KIT_ROOT/rustfs-data"
TARGET_DIR="$KIT_ROOT/target-fp"
IMDS="${KIT_IMDS:-http://169.254.169.254}"
DROP_CACHES_FILE="${KIT_DROP_CACHES_FILE:-/proc/sys/vm/drop_caches}"
READY_TRIES="${KIT_READY_TRIES:-600}"
RUSTFS_ENDPOINT="http://127.0.0.1:9000"
STORE_BUCKET="ravel-kit"
SQL_URL="http://127.0.0.1:4318/api/v1/sql"
READY_URL="http://127.0.0.1:4319/readyz"

CURRENT_STEP=""
FINISHED=0
SERVER_N=0
SERVER_PID=""
SAMPLER_PIDS=""
DATASET_PID=""
BANDS_VERDICT="none"

export HOME="$KIT_ROOT"

utc_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

stamp() {
  printf '%s %s\n' "$(utc_now)" "$*" >>"$PROGRESS"
}

say() {
  echo "kit-box: $*" >&2
}

kit_py() {
  python3 "$PY_KIT" "$@"
}

toml() {
  kit_py get "$RUN_TOML" "$1"
}

# toml_or KEY DEFAULT: the key's value, or DEFAULT when run.toml lacks it.
toml_or() {
  local v
  v="$(kit_py get "$RUN_TOML" "$1" 2>/dev/null)" || v=""
  [ -n "$v" ] || v="$2"
  printf '%s\n' "$v"
}

json_str() {
  local s
  s="$(printf '%s' "$1" | tr -d '\000-\010\013\014\016-\037')"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\t'/\\t}"
  s="${s//$'\r'/\\r}"
  printf '"%s"' "$s"
}

# json_num VALUE: VALUE when it is a number, null otherwise.
json_num() {
  if [[ "$1" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then printf '%s' "$1"; else printf 'null'; fi
}

sha256_of() {
  sha256sum "$1" | awk '{ print $1 }'
}

# step NAME CMD...: run CMD stamped in progress.log; a failure ends the run
# as FAILED naming NAME.
step() {
  local name="$1" code=0
  shift
  CURRENT_STEP="$name"
  stamp "step=$name state=start"
  "$@" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp "step=$name state=failed exit=$code"
    fail_run "$name"
  fi
  stamp "step=$name state=done"
  CURRENT_STEP=""
}

load_config() {
  ARM="$(cat "$KIT/ARM" 2>/dev/null)" || ARM=""
  RUN_ID="$(cat "$KIT/RUN_ID" 2>/dev/null)" || RUN_ID=""
  case "$ARM" in
    A) SHA="$(toml sha_a)" ;;
    B) SHA="$(toml sha_b)" ;;
    *) say "no arm A or B in $KIT/ARM: '$ARM'"; return 1 ;;
  esac
  [[ "$SHA" =~ ^[0-9a-f]{40}$ ]] || { say "arm $ARM has no full commit: '$SHA'"; return 1; }
  [ -n "$RUN_ID" ] || { say "no run id in $KIT/RUN_ID"; return 1; }
  REGION="$(toml region)" || return 1
  BUCKET="$(toml bucket)" || return 1
  PREFIX="$(toml prefix)" || return 1
  OUT_URI="s3://$BUCKET/${PREFIX%/}/runs/$RUN_ID/out/$ARM"
  REPO_URL="$(toml repo_url)" || return 1
  TENANT="$(toml tenant)" || return 1
  SHARDS="$(toml shards)" || return 1
  DATASET_URL="$(toml dataset_url)" || return 1
  DATASET_SHA256="$(toml_or dataset_sha256 '')"
  RUSTFS_URL="$(toml rustfs_url)" || return 1
  RUSTFS_SHA256="$(toml rustfs_sha256)" || return 1
  MEMORY_BUDGET="$(toml memory_budget_bytes)" || return 1
  AUDIT_MODE="$(toml audit_mode)" || return 1
  AUDIT_TEXT="$(toml_or audit_text redacted)"
  EXPECTED_STATEMENTS="$(toml_or expected_statements 43)"
  FOLD_MIN_RATIO="$(toml_or fold_min_entry_ratio 0.8)"
  QUERY_TIMEOUT_S="$(toml_or query_timeout_s 900)"
  WINDOW_HOURS="$(toml_or window_hours 200000)"
  BAND_SECTIONS="$(toml_or band_sections "$(printf 'relative\nabsolute')" | tr '\n' ' ')"
  PHASES="$(toml phases | tr '\n' ' ')" || return 1
  LANES="$(toml lanes | tr '\n' ' ')" || return 1
  CORPUS="$SRC/corpus.json"
  MAPPING="$RAVEL_DIR/benchmarks/clickbench/hits.mapping.toml"
}

# --- terminal handling -----------------------------------------------------

upload_out() {
  local f code=0
  for f in results.json evidence.tgz progress.log; do
    [ -f "$KIT/$f" ] || continue
    aws s3 cp --region "$REGION" --only-show-errors "$KIT/$f" "$OUT_URI/$f" || code=1
  done
  return "$code"
}

terminate_self() {
  local token id
  token="$(curl -sS --max-time 5 -X PUT "$IMDS/latest/api/token" \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" || token=""
  id="$(curl -sS --max-time 5 -H "X-aws-ec2-metadata-token: $token" \
    "$IMDS/latest/meta-data/instance-id")" || id=""
  if [[ "$id" =~ ^i-[0-9a-z]+$ ]] \
    && aws ec2 terminate-instances --region "$REGION" --instance-ids "$id" --output text >/dev/null; then
    stamp "terminate instance=$id"
    return 0
  fi
  stamp "terminate fallback=shutdown"
  shutdown -h now
}

# fail_run STEP: FAILED naming STEP, partial outputs uploaded, instance gone.
fail_run() {
  local name="$1"
  trap - EXIT
  FINISHED=1
  stop_samplers
  stop_server
  printf 'FAILED step=%s at=%s\n' "$name" "$(utc_now)" >"$KIT/FAILED"
  tail -n 20 "$KIT/run.log" >>"$KIT/FAILED" 2>/dev/null || true
  write_evidence || true
  if [ -n "${OUT_URI:-}" ]; then
    upload_out || true
    aws s3 cp --region "$REGION" --only-show-errors "$KIT/FAILED" "$OUT_URI/FAILED" || true
  fi
  terminate_self
  exit 1
}

on_exit() {
  [ "$FINISHED" = 1 ] || fail_run "${CURRENT_STEP:-unexpected-exit}"
}

finish() {
  trap - EXIT
  FINISHED=1
  printf 'DONE bands=%s at=%s\n' "$BANDS_VERDICT" "$(utc_now)" >"$KIT/DONE"
  aws s3 cp --region "$REGION" --only-show-errors "$KIT/DONE" "$OUT_URI/DONE" \
    || fail_run "upload-done"
  stamp "done bands=$BANDS_VERDICT"
  terminate_self
}

# --- provenance helpers ----------------------------------------------------

imds_get() {
  local token
  token="$(curl -sS --max-time 5 -X PUT "$IMDS/latest/api/token" \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" || return 1
  curl -sS --max-time 5 -H "X-aws-ec2-metadata-token: $token" "$IMDS/latest/meta-data/$1"
}

# record_burst start|end: EBS BurstBalance of the root volume. gp3 publishes
# none (its IOPS are provisioned, not credit-based), so a gp3 root records null
# with that reason rather than a figure.
record_burst() {
  local which="$1" id vol vtype value="" reason=""
  mkdir -p "$STAMPS"
  id="$(imds_get instance-id)" || id=""
  vol="$(aws ec2 describe-volumes --region "$REGION" \
    --filters "Name=attachment.instance-id,Values=$id" \
    --query 'Volumes[0].[VolumeId,VolumeType]' --output text 2>/dev/null)" || vol=""
  vtype="$(printf '%s' "$vol" | awk '{ print $2 }')"
  vol="$(printf '%s' "$vol" | awk '{ print $1 }')"
  if [ -z "$vol" ]; then
    reason="root volume could not be described (ec2:DescribeVolumes)"
  elif [ "$vtype" = gp3 ]; then
    reason="gp3 publishes no BurstBalance metric: its IOPS and throughput are provisioned"
  else
    value="$(aws cloudwatch get-metric-statistics --region "$REGION" --namespace AWS/EBS \
      --metric-name BurstBalance --dimensions "Name=VolumeId,Value=$vol" \
      --start-time "$(date -u -d '-10 minutes' +%Y-%m-%dT%H:%M:%SZ)" \
      --end-time "$(utc_now)" --period 300 --statistics Average \
      --query 'sort_by(Datapoints,&Timestamp)[-1].Average' --output text 2>/dev/null)" || value=""
    [[ "$value" =~ ^[0-9.]+$ ]] || { value=""; reason="cloudwatch returned no BurstBalance datapoint"; }
  fi
  printf '{"at": %s, "volume_id": %s, "volume_type": %s, "value": %s, "reason": %s}\n' \
    "$(json_str "$(utc_now)")" "$(json_str "$vol")" "$(json_str "$vtype")" \
    "$(json_num "$value")" "$(json_str "$reason")" >"$KIT/burst-$which.json"
}

# --- bootstrap (runbook section 6) -----------------------------------------

bootstrap_packages() {
  dnf install -y git gcc gcc-c++ make cmake pkgconfig openssl-devel perl wget jq \
    gperftools gperftools-libs gdb perf sysstat time unzip
}

bootstrap_toolchain() {
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o "$KIT_ROOT/rustup-init.sh" || return 1
  sh "$KIT_ROOT/rustup-init.sh" -y --default-toolchain none || return 1
  # shellcheck source=/dev/null
  . "$KIT_ROOT/.cargo/env"
}

bootstrap_source() {
  rm -rf "$RAVEL_DIR"
  git clone "$REPO_URL" "$RAVEL_DIR" || return 1
  git -C "$RAVEL_DIR" checkout -q --detach "$SHA" || return 1
  [ "$(git -C "$RAVEL_DIR" rev-parse HEAD)" = "$SHA" ] || { say "HEAD is not $SHA"; return 1; }
  [ -z "$(git -C "$RAVEL_DIR" status --porcelain)" ] || { say "checkout is dirty"; return 1; }
  # Resolves the toolchain rust-toolchain.toml pins.
  (cd "$RAVEL_DIR" && rustc --version && cargo --version) >"$STAMPS/toolchain" || return 1
}

start_dataset_download() {
  rm -f "$DATASET"
  wget -q "$DATASET_URL" -O "$DATASET" &
  DATASET_PID=$!
}

bootstrap_build() {
  (
    cd "$RAVEL_DIR" || exit 1
    export CARGO_TARGET_DIR="$TARGET_DIR"
    RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release --locked -p ravel-cli || exit 1
    RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release --locked -p ravel-server --features sql
  ) || return 1
  mkdir -p "$BIN_DIR"
  install -m 0755 "$TARGET_DIR/release/ravel-cli" "$BIN_DIR/ravel-cli-$SHA" || return 1
  install -m 0755 "$TARGET_DIR/release/ravel-server" "$BIN_DIR/ravel-server-$SHA"
}

wait_dataset() {
  wait "$DATASET_PID" || { say "download of $DATASET_URL failed"; return 1; }
  [ -s "$DATASET" ] || { say "$DATASET is empty"; return 1; }
  DATASET_DIGEST="$(sha256_of "$DATASET")"
  if [ -n "$DATASET_SHA256" ] && [ "$DATASET_DIGEST" != "$DATASET_SHA256" ]; then
    say "$DATASET has sha256 $DATASET_DIGEST, run.toml pins $DATASET_SHA256"
    return 1
  fi
  printf '%s\n' "$DATASET_DIGEST" >"$KIT/dataset.sha256"
}

# --- store -----------------------------------------------------------------

# secrets: RustFS keys and the tenant token, generated on the box once and
# never uploaded.
make_secrets() {
  if [ ! -s "$SECRETS_ENV" ]; then
    (
      umask 077
      printf 'RUSTFS_ACCESS_KEY=%s\nRUSTFS_SECRET_KEY=%s\nKIT_TENANT_TOKEN=%s\n' \
        "$(openssl rand -hex 12)" "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" >"$SECRETS_ENV"
    ) || return 1
  fi
  # shellcheck source=/dev/null
  . "$SECRETS_ENV" || return 1
  (
    umask 077
    printf '%s=%s\n' "$KIT_TENANT_TOKEN" "$TENANT" >"$TOKEN_FILE"
    printf 'Authorization: Bearer %s\n' "$KIT_TENANT_TOKEN" >"$AUTH_HEADER"
  ) || return 1
  export RAVEL_S3_ENDPOINT="$RUSTFS_ENDPOINT" RAVEL_S3_BUCKET="$STORE_BUCKET" \
    RAVEL_S3_REGION=us-east-1 RAVEL_S3_ACCESS_KEY="$RUSTFS_ACCESS_KEY" \
    RAVEL_S3_SECRET_KEY="$RUSTFS_SECRET_KEY"
}

rustfs_aws() {
  AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
    AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true \
    aws --endpoint-url "$RUSTFS_ENDPOINT" "$@"
}

bootstrap_rustfs() {
  local dl="$KIT_ROOT/rustfs.download" got i bin
  make_secrets || return 1
  curl -sSfL "$RUSTFS_URL" -o "$dl" || return 1
  got="$(sha256_of "$dl")"
  [ "$got" = "$RUSTFS_SHA256" ] || { say "RustFS download has sha256 $got, run.toml pins $RUSTFS_SHA256"; return 1; }
  mkdir -p "$BIN_DIR"
  case "$RUSTFS_URL" in
    *.zip)
      rm -rf "$KIT_ROOT/rustfs-unpack"
      unzip -q -o "$dl" -d "$KIT_ROOT/rustfs-unpack" || return 1
      bin="$(find "$KIT_ROOT/rustfs-unpack" -type f -name rustfs | head -n 1)"
      [ -n "$bin" ] || { say "no rustfs binary in $RUSTFS_URL"; return 1; }
      install -m 0755 "$bin" "$BIN_DIR/rustfs" || return 1 ;;
    *) install -m 0755 "$dl" "$BIN_DIR/rustfs" || return 1 ;;
  esac
  mkdir -p "$RUSTFS_DATA"
  RUSTFS_ACCESS_KEY="$RUSTFS_ACCESS_KEY" RUSTFS_SECRET_KEY="$RUSTFS_SECRET_KEY" \
    nohup "$BIN_DIR/rustfs" --address 127.0.0.1:9000 "$RUSTFS_DATA" >"$KIT/rustfs.log" 2>&1 &
  for ((i = 1; i <= 120; i++)); do
    rustfs_aws s3api list-buckets >/dev/null 2>&1 && break
    [ "$i" -lt 120 ] || { say "RustFS did not answer a signed call in 120 s"; return 1; }
    sleep 1
  done
  rustfs_aws s3api head-bucket --bucket "$STORE_BUCKET" >/dev/null 2>&1 \
    || rustfs_aws s3api create-bucket --bucket "$STORE_BUCKET" --object-lock-enabled-for-bucket >/dev/null \
    || return 1
  "$BIN_DIR/rustfs" --version >"$KIT/rustfs.version" 2>&1 || true
}

store_qualify() {
  "$CLI_BIN" --store s3 --tenant-hash-unkeyed store qualify
}

# count_objects LABEL: total objects in Ravel's bucket, kept for the evidence.
count_objects() {
  local n
  mkdir -p "$KIT/object-counts"
  rustfs_aws s3 ls "s3://$STORE_BUCKET/" --recursive --summarize >"$KIT/object-counts/$1.txt" || return 1
  n="$(sed -n 's/^[[:space:]]*Total Objects:[[:space:]]*//p' "$KIT/object-counts/$1.txt")"
  [[ "$n" =~ ^[0-9]+$ ]] || { say "no object total in the $1 listing"; return 1; }
  printf '%s\n' "$n" >"$KIT/object-counts/$1"
}

# --- arm.env and the server ------------------------------------------------

# resolve_arm_env: the binaries and server flags, written once. A restarted
# run.sh keeps the file it finds, so every server start uses the same flags.
resolve_arm_env() {
  if [ -s "$ARM_ENV" ]; then
    stamp "arm.env kept"
    return 0
  fi
  local preload="" flags
  preload="$(ls /usr/lib64/libtcmalloc.so.4 /usr/lib64/libtcmalloc.so 2>/dev/null | head -n 1)"
  flags=(--store s3 --shards "$SHARDS" --tenant-hash-unkeyed
    --tenant-token-file "$TOKEN_FILE"
    --listen-http 127.0.0.1:4318 --listen-health 127.0.0.1:4319
    --memory-budget-bytes "$MEMORY_BUDGET" --audit-mode "$AUDIT_MODE")
  if [ "$AUDIT_MODE" = required ]; then
    flags+=(--audit-text "$AUDIT_TEXT")
  fi
  {
    printf 'SERVER_BIN=%q\n' "$BIN_DIR/ravel-server-$SHA"
    printf 'CLI_BIN=%q\n' "$BIN_DIR/ravel-cli-$SHA"
    printf 'SERVER_PRELOAD=%q\n' "$preload"
    printf 'SERVER_FLAGS=('
    printf ' %q' "${flags[@]}"
    printf ' )\n'
  } >"$ARM_ENV.tmp" && mv "$ARM_ENV.tmp" "$ARM_ENV"
}

load_arm_env() {
  # shellcheck source=/dev/null
  . "$ARM_ENV"
}

start_server() {
  load_arm_env || return 1
  make_secrets || return 1
  if [ "$AUDIT_MODE" = required ] && [ "$AUDIT_TEXT" = redacted ]; then
    [ -n "${RAVEL_AUDIT_TOKEN_KEY:-}" ] || export RAVEL_AUDIT_TOKEN_KEY="$(openssl rand -hex 32)"
  fi
  SERVER_N=$((SERVER_N + 1))
  SERVER_LOG="$KIT/server-$SERVER_N.log"
  LD_PRELOAD="$SERVER_PRELOAD" "$SERVER_BIN" "${SERVER_FLAGS[@]}" </dev/null >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  printf '%s\n' "$SERVER_PID" >"$KIT/server.pid"
  stamp "server start n=$SERVER_N pid=$SERVER_PID log=$(basename "$SERVER_LOG")"
  local i
  for ((i = 1; i <= READY_TRIES; i++)); do
    curl -sf --max-time 2 "$READY_URL" >/dev/null 2>&1 && return 0
    kill -0 "$SERVER_PID" 2>/dev/null || { say "server exited before ready; see $SERVER_LOG"; return 1; }
    sleep 1
  done
  say "server not ready after $READY_TRIES s"
  return 1
}

stop_server() {
  [ -n "$SERVER_PID" ] || return 0
  local i
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  for ((i = 1; i <= 600; i++)); do
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.1
  done
  kill -KILL "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

drop_caches() {
  sync
  echo 3 >"$DROP_CACHES_FILE"
}

# start_samplers PHASE: 2 s samplers into out/<phase>/samplers/.
start_samplers() {
  local dir="$OUT/$1/samplers"
  mkdir -p "$dir"
  SAMPLER_PIDS=""
  if command -v vmstat >/dev/null; then
    vmstat -t 2 >"$dir/vmstat.log" 2>&1 &
    SAMPLER_PIDS="$SAMPLER_PIDS $!"
  fi
  if command -v iostat >/dev/null; then
    iostat -x -t 2 >"$dir/iostat.log" 2>&1 &
    SAMPLER_PIDS="$SAMPLER_PIDS $!"
  fi
  (
    while :; do
      printf '%s pid=%s rss_kb=%s\n' "$(utc_now)" "${SERVER_PID:-}" \
        "$(ps -o rss= -p "$(cat "$KIT/server.pid" 2>/dev/null || echo 0)" 2>/dev/null | tr -d ' ')"
      sleep 2
    done
  ) >"$dir/server-rss.log" 2>&1 &
  SAMPLER_PIDS="$SAMPLER_PIDS $!"
}

stop_samplers() {
  local pid
  for pid in $SAMPLER_PIDS; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  SAMPLER_PIDS=""
}

# --- load, compact, fold, declare (runbook section 7) ----------------------

do_load() {
  /usr/bin/time -f 'wall=%e rss_kb=%M user=%U sys=%S' -o "$KIT/load.time" \
    "$CLI_BIN" --store s3 --tenant-hash-unkeyed load \
    --parquet "$DATASET" \
    --tenant "$TENANT" \
    --mapping "$MAPPING" \
    --shards "$SHARDS" \
    --read-cursors 8 \
    --batch-rows 65536
}

do_compact() {
  /usr/bin/time -f 'wall=%e rss_kb=%M' -o "$KIT/compact.time" \
    "$CLI_BIN" --store s3 --tenant-hash-unkeyed maintain compact-tenant \
    --tenant "$TENANT" --signal logs --shards "$SHARDS"
}

fold_field() {
  sed -n "s/^[[:space:]]*$1:[[:space:]]*//p" "$KIT/fold.out" | head -n 1
}

# do_fold: the fold, then its precondition: a sealed hour, and entry_count at
# least FOLD_MIN_RATIO of the objects the load wrote.
do_fold() {
  "$CLI_BIN" --store s3 --tenant-hash-unkeyed catalog fold \
    --tenant "$TENANT" --shards "$SHARDS" --signal logs --writers-stopped \
    >"$KIT/fold.out" 2>&1 || return 1
  local seal entries watermark buckets part before after written ok
  seal="$(fold_field seal_through_hour)"
  watermark="$(fold_field watermark_hour)"
  entries="$(fold_field entry_count)"
  buckets="$(fold_field buckets_folded)"
  part="$(fold_field part_bytes)"
  before="$(cat "$KIT/object-counts/before-load")"
  after="$(cat "$KIT/object-counts/after-load")"
  written=$((after - before))
  [[ "$seal" =~ ^Some\(([0-9]+)\)$ ]] || { say "fold sealed nothing: seal_through_hour=$seal"; return 1; }
  [[ "$entries" =~ ^[0-9]+$ ]] || { say "fold printed no entry_count"; return 1; }
  ok="$(python3 -c 'import sys; print(int(int(sys.argv[1]) >= float(sys.argv[3]) * int(sys.argv[2])))' \
    "$entries" "$written" "$FOLD_MIN_RATIO")" || return 1
  mkdir -p "$STAMPS"
  printf '{"verified_at": %s, "seal_through_hour": %s, "watermark_hour": %s, "entry_count": %s, "buckets_folded": %s, "part_bytes": %s, "objects_written_by_load": %s, "min_entry_ratio": %s, "precondition": %s}\n' \
    "$(json_str "$(utc_now)")" "$(json_str "$seal")" "$(json_str "$watermark")" \
    "$(json_num "$entries")" "$(json_num "$buckets")" "$(json_num "$part")" \
    "$(json_num "$written")" "$(json_num "$FOLD_MIN_RATIO")" \
    "$([ "$ok" = 1 ] && echo '"met"' || echo '"failed"')" >"$STAMPS/fold"
  [ "$ok" = 1 ] || { say "fold entry_count $entries is under $FOLD_MIN_RATIO of the $written objects the load wrote"; return 1; }
}

do_declare() {
  "$CLI_BIN" --store s3 --tenant-hash-unkeyed typed-attr-column set "$TENANT" \
    --from-mapping "$MAPPING"
}

# --- queries ---------------------------------------------------------------

# run_query PHASE NUMBER STATEMENT TRY BODY_FILE [EXPECT_ROWS]: one SQL call,
# recorded as a line of out/<phase>/calls.jsonl. Sets RQ_OUTCOME (ok, error,
# timeout or unreachable), RQ_HTTP, RQ_DURATION, RQ_ROWS and RQ_CLASS. A 200
# answer with a row count other than EXPECT_ROWS is an error of class
# wrong_answer, and one whose rows cannot be read is unreadable_answer.
# Returns non-zero only when the record could not be written.
run_query() {
  local phase="$1" number="$2" statement="$3" try="$4" body="$5" expect="${6:-}"
  local dir="$OUT/$phase" resp w code=0 parsed err=""
  mkdir -p "$dir/responses" || return 1
  resp="$dir/responses/$number-$try.json"
  RQ_START="$(utc_now)"
  w="$(curl -sS -o "$resp" -w '%{http_code} %{time_total}' --max-time "$QUERY_TIMEOUT_S" \
    -H @"$AUTH_HEADER" -H 'Content-Type: application/json' \
    --data-binary @"$body" "$SQL_URL" 2>"$dir/responses/$number-$try.curl")" || code=$?
  RQ_HTTP="${w%% *}"
  RQ_DURATION="${w##* }"
  RQ_CLASS=""
  RQ_ROWS=""
  if [ "$code" -eq 28 ]; then
    RQ_OUTCOME=timeout
    RQ_CLASS=client_timeout
    err="no answer within ${QUERY_TIMEOUT_S} s"
  elif [ "$code" -ne 0 ] || [ "${RQ_HTTP:-000}" = 000 ]; then
    RQ_OUTCOME=unreachable
    RQ_CLASS=transport
    err="curl exit $code: $(head -c 300 "$dir/responses/$number-$try.curl" 2>/dev/null)"
  elif [ "$RQ_HTTP" = 200 ]; then
    RQ_ROWS="$(python3 -c 'import json, sys
print(len(json.load(open(sys.argv[1]))["data"]["rows"]))' "$resp" 2>/dev/null)" || RQ_ROWS=""
    if ! [[ "$RQ_ROWS" =~ ^[0-9]+$ ]]; then
      RQ_ROWS=""
      RQ_OUTCOME=error
      RQ_CLASS=unreadable_answer
      err="HTTP 200 without a readable data.rows array"
    elif [ -n "$expect" ] && [ "$RQ_ROWS" != "$expect" ]; then
      RQ_OUTCOME=error
      RQ_CLASS=wrong_answer
      err="answer has $RQ_ROWS rows, expected $expect"
    else
      RQ_OUTCOME=ok
    fi
  else
    RQ_OUTCOME=error
    parsed="$(python3 -c 'import json, sys
d = json.load(open(sys.argv[1]))
print("%s\t%s" % (d.get("errorType", ""), str(d.get("error", ""))[:300]))' "$resp" 2>/dev/null)" || parsed=""
    RQ_CLASS="${parsed%%$'\t'*}"
    err="${parsed#*$'\t'}"
    [ -n "$RQ_CLASS" ] || RQ_CLASS=unclassified
  fi
  # Keep the first try's answer and every error; later ok answers repeat it.
  [ "$RQ_OUTCOME" = ok ] && [ "$try" != 1 ] && rm -f "$resp"
  printf '{"statement": %s, "number": %s, "arm": %s, "phase": %s, "try": %s, "start": %s, "duration_s": %s, "http_status": %s, "rows": %s, "outcome": %s, "refusal_class": %s, "error": %s}\n' \
    "$(json_str "$statement")" "$(json_num "$((10#$number))")" "$(json_str "$ARM")" \
    "$(json_str "$phase")" "$(json_num "$try")" "$(json_str "$RQ_START")" \
    "$(json_num "$RQ_DURATION")" "$(json_num "${RQ_HTTP:-0}")" "$(json_num "$RQ_ROWS")" \
    "$(json_str "$RQ_OUTCOME")" "$(json_str "$RQ_CLASS")" "$(json_str "$err")" >>"$dir/calls.jsonl"
}

# --- phases ----------------------------------------------------------------

# phase_benchmark: every corpus statement three times. Try 1 is cold (server
# restarted and the page cache dropped first); tries 2 and 3 are hot. Tries 2
# and 3 must return try 1's row count when the corpus states none.
phase_benchmark() {
  local dir="$OUT/benchmark" count n id exp expect try end start
  end="$(date -u +%s)"
  start=$((end - WINDOW_HOURS * 3600))
  mkdir -p "$STAMPS"
  printf '{"start": %s, "end": %s, "window_hours": %s}\n' "$start" "$end" "$WINDOW_HOURS" \
    >"$STAMPS/query_window"
  count="$(kit_py request-bodies "$CORPUS" "$dir/requests" "$start" "$end")" || return 1
  [ "$count" = "$EXPECTED_STATEMENTS" ] \
    || { say "corpus holds $count statements, run.toml expects $EXPECTED_STATEMENTS"; return 1; }
  : >"$dir/calls.jsonl"
  start_samplers benchmark
  BENCHMARK_SERVER_LOGS=""
  while IFS=$'\t' read -r n id exp; do
    stop_server
    drop_caches || { stop_samplers; return 1; }
    start_server || { stop_samplers; return 1; }
    BENCHMARK_SERVER_LOGS="${BENCHMARK_SERVER_LOGS:+$BENCHMARK_SERVER_LOGS,}$SERVER_LOG"
    expect=""
    [ "$exp" = - ] || expect="$exp"
    for try in 1 2 3; do
      run_query benchmark "$n" "$id" "$try" "$dir/requests/$n.json" "$expect" \
        || { stop_samplers; return 1; }
      [ -n "$expect" ] || expect="$RQ_ROWS"
    done
  done <"$dir/requests/index.tsv"
  stop_server
  stop_samplers
  [ "$(wc -l <"$dir/calls.jsonl" | tr -d ' ')" = "$((count * 3))" ] \
    || { say "benchmark recorded $(wc -l <"$dir/calls.jsonl") calls, expected $((count * 3))"; return 1; }
}

run_phases() {
  local phase
  for phase in $PHASES; do
    declare -F "phase_$phase" >/dev/null || { say "no phase_$phase in run.sh"; fail_run "phase-$phase"; }
    mkdir -p "$OUT/$phase"
    step "phase-$phase" "phase_$phase"
  done
}

# --- results ---------------------------------------------------------------

write_stamps() {
  local cores mem cpu itype lane lanes_json="" tool flags
  mkdir -p "$STAMPS"
  printf '%s\n' "$ARM" >"$STAMPS/kit_arm"
  printf '%s\n' "$RUN_ID" >"$STAMPS/run_id"
  for lane in $LANES; do lanes_json="${lanes_json:+$lanes_json, }$(json_str "$lane")"; done
  printf '[%s]\n' "$lanes_json" >"$STAMPS/lanes"
  itype="$(imds_get instance-type 2>/dev/null)" || itype="unknown"
  cores="$(nproc)"
  mem="$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo)"
  cpu="$(awk -F': ' '/^model name/ { print $2; exit }' /proc/cpuinfo)"
  printf '{"instance_type": %s, "cores": %s, "mem_total_kb": %s, "cpu": %s, "arch": %s}\n' \
    "$(json_str "$itype")" "$(json_num "$cores")" "$(json_num "$mem")" \
    "$(json_str "$cpu")" "$(json_str "$(uname -m)")" >"$STAMPS/hardware"
  tool="$(cat "$STAMPS/toolchain" 2>/dev/null)" || tool=""
  [ -n "$tool" ] || { say "no toolchain stamp"; return 1; }
  printf '{"store": "rustfs", "endpoint": %s, "url": %s, "sha256": %s, "version": %s}\n' \
    "$(json_str "$RUSTFS_ENDPOINT")" "$(json_str "$RUSTFS_URL")" "$(json_str "$RUSTFS_SHA256")" \
    "$(json_str "$(head -n 1 "$KIT/rustfs.version" 2>/dev/null)")" >"$STAMPS/backend"
  printf '{"dataset_url": %s, "dataset_sha256": %s, "corpus_sha256": %s, "mapping_sha256": %s, "statements": %s}\n' \
    "$(json_str "$DATASET_URL")" "$(json_str "$(cat "$KIT/dataset.sha256" 2>/dev/null)")" \
    "$(json_str "$(sha256_of "$CORPUS")")" "$(json_str "$(sha256_of "$MAPPING" 2>/dev/null)")" \
    "$(json_num "$EXPECTED_STATEMENTS")" >"$STAMPS/corpus_digest"
  load_arm_env || return 1
  flags="$(printf '%q ' "${SERVER_FLAGS[@]}")"
  printf '{"binary": %s, "flags": %s, "ld_preload": %s}\n' "$(json_str "$SERVER_BIN")" \
    "$(json_str "${flags% }")" "$(json_str "$SERVER_PRELOAD")" >"$STAMPS/server_flags"
  if [ "$AUDIT_MODE" = required ]; then
    printf '%s\n' "required, audit text $AUDIT_TEXT" >"$STAMPS/audit_mode"
  else
    printf '%s\n' "off" >"$STAMPS/audit_mode"
  fi
  printf '{"start": %s, "end": %s}\n' "$(cat "$KIT/burst-start.json" 2>/dev/null || echo null)" \
    "$(cat "$KIT/burst-end.json" 2>/dev/null || echo null)" >"$STAMPS/burst_balance"
  printf '{"before_load": %s, "after_load": %s}\n' \
    "$(json_num "$(cat "$KIT/object-counts/before-load" 2>/dev/null)")" \
    "$(json_num "$(cat "$KIT/object-counts/after-load" 2>/dev/null)")" >"$STAMPS/object_counts"
}

write_results() {
  local lane bands="" sections code=0
  write_stamps || return 1
  for lane in $LANES; do bands="${bands:+$bands,}$lane:$SRC/bands/$lane.toml"; done
  # shellcheck disable=SC2086 # PHASES is a space-separated list of names
  kit_py phases "$OUT" $PHASES >"$KIT/phases.json" || return 1
  load_arm_env || return 1
  sections="$(printf '%s' "$BAND_SECTIONS" | tr -s ' ' ',')"
  kit_py assemble out="$KIT/results.json" calls="$OUT/benchmark/calls.jsonl" \
    index="$OUT/benchmark/requests/index.tsv" server_logs="$BENCHMARK_SERVER_LOGS" \
    bands="$bands" sections="${sections%,}" provenance="$STAMPS" arm="$ARM" sha="$SHA" \
    binary="$SERVER_BIN" tenant="$TENANT" phases_json="$KIT/phases.json" || return 1
  kit_py absolute "$ARM" "$KIT/results.json" >/dev/null || code=$?
  case "$code" in
    0) BANDS_VERDICT=pass ;;
    1) BANDS_VERDICT=miss ;;
    *) return 1 ;;
  esac
  stamp "bands=$BANDS_VERDICT"
}

write_evidence() {
  local items="" f g
  for f in server-*.log rustfs.log run.log progress.log arm.env fold.out load.time \
    compact.time object-counts stamps out phases.json burst-start.json burst-end.json; do
    # shellcheck disable=SC2086 # globs expand here on purpose
    for g in $(cd "$KIT" && ls -d $f 2>/dev/null); do items="$items $g"; done
  done
  [ -n "$items" ] || return 0
  # shellcheck disable=SC2086 # items is a space-separated list of names
  tar -czf "$KIT/evidence.tgz" -C "$KIT" --exclude='out/*/requests' $items
}

main() {
  mkdir -p "$OUT" "$STAMPS"
  : >>"$PROGRESS"
  trap on_exit EXIT
  trap 'exit 1' HUP INT TERM
  step config load_config
  step check-run kit_py check-run "$RUN_TOML"
  for lane in $LANES; do
    # shellcheck disable=SC2086 # BAND_SECTIONS is a space-separated list of names
    step "bands-$lane" kit_py check-bands "$lane" "$SRC/bands/$lane.toml" $BAND_SECTIONS
  done
  step burst-start record_burst start
  step packages bootstrap_packages
  step toolchain bootstrap_toolchain
  step source bootstrap_source
  step dataset-start start_dataset_download
  step build bootstrap_build
  step dataset wait_dataset
  step rustfs bootstrap_rustfs
  step arm-env resolve_arm_env
  step arm-env-load load_arm_env
  step qualify store_qualify
  step objects-before-load count_objects before-load
  step load do_load
  step objects-after-load count_objects after-load
  step compact do_compact
  step fold do_fold
  step declare do_declare
  run_phases
  step burst-end record_burst end
  step results write_results
  step evidence write_evidence
  step upload upload_out
  finish
}

if [ "${KIT_RUN_SH_LIB:-}" != 1 ]; then
  main "$@"
fi
