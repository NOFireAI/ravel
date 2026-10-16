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
#               load; the load must report rows_written equal to run.toml's
#               rows_expected, the fold must report seal_through_hour and an
#               entry_count of at least fold_min_entry_ratio (default 0.8) of
#               the objects the load wrote, and its figures are stamped
#   verify      the corpus's q01, SELECT COUNT(*) FROM logs, once through the
#               benchmark's endpoint and window; its one cell must equal
#               rows_expected, and the three counts are stamped as row_count
#   phases      each phase run.toml names, in order; phase NAME is the
#               function phase_NAME and writes /root/kit/out/NAME/:
#               benchmark (every statement cold, then hot twice), concurrent
#               (upstream ClickBench's bench_concurrent_qps, concurrent_rounds
#               times), q29_probe (q29 alone, warmed up, then timed) and perf
#               (folded perf stacks of one hot call per statement)
#   results     /root/kit/results.json (SqlLatencyReport's entries and failed
#               rows, which the runbook's analyse.py reads, plus the
#               provenance stamps and the absolute bands when run.toml's
#               band_sections selects them), evidence.tgz, then both and
#               progress.log uploaded to
#               s3://<bucket>/<prefix>/runs/<run-id>/out/<arm>/
#
# It ends by uploading DONE (or FAILED, naming the step that failed) beside
# those files and terminating its own instance through the instance role, then
# running `shutdown -h now` in every case (a run that failed before it knew its
# region uploads nothing and only shuts down), which the launch's terminate
# behaviour turns into a termination. An absolute band miss is a verdict, not a
# failure: DONE records `bands=miss`, and fetch.sh exits 1 from its own
# evaluation of the bands in results.json.
#
# Every statement goes through run_query, which records one JSON line per call
# in out/<phase>/calls.jsonl: statement, arm, phase, start, duration, HTTP
# status, rows, outcome and refusal class. A 200 answer whose row count is not
# the expected one (the corpus's expected_rows, else the first try's) is an
# error of class wrong_answer, not a success. The concurrent phase's figures
# follow upstream instead: a 2xx answer whose transfer completed (curl exit 0)
# succeeds, and a wrong_answer there is counted beside them as a diagnostic. Every request body names its
# window: start window_hours (default 200000, the runbook's --window-hours)
# before end, the epoch second the verify step began. A new phase is a phase_NAME
# function plus its name in kit.py's KNOWN_PHASES; wrapping run_query wraps
# every call.
#
# KIT_ROOT (default /root) relocates every path, and KIT_RUN_SH_LIB=1 defines
# the functions without running main; kit.test.sh uses both. It also sets
# KIT_READY_TRIES and KIT_READY_INTERVAL (the server readiness poll, default 600
# tries 1 s apart, which also bounds the concurrent watchdog's restart wait),
# KIT_WATCHDOG_INTERVAL (the concurrent phase's SELECT 1 check interval,
# default 5 s),
# KIT_PERF_SETTLE (seconds perf record gets before the profiled call, default
# 1) and KIT_CPUINFO and KIT_MEMINFO (the hardware stamp's sources, default
# /proc/cpuinfo and /proc/meminfo).

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
READY_INTERVAL="${KIT_READY_INTERVAL:-1}"
WATCHDOG_INTERVAL="${KIT_WATCHDOG_INTERVAL:-5}"
PERF_SETTLE="${KIT_PERF_SETTLE:-1}"
CPUINFO="${KIT_CPUINFO:-/proc/cpuinfo}"
MEMINFO="${KIT_MEMINFO:-/proc/meminfo}"
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

# utc_now_to VAR: utc_now assigned to VAR, through bash 4.2's printf %()T
# without forking date where bash has it.
utc_now_to() {
  if [ "${BASH_VERSINFO[0]}" -gt 4 ] || { [ "${BASH_VERSINFO[0]}" = 4 ] && [ "${BASH_VERSINFO[1]}" -ge 2 ]; }; then
    local -x TZ=UTC0
    printf -v "$1" '%(%Y-%m-%dT%H:%M:%SZ)T' -1
  else
    printf -v "$1" '%s' "$(utc_now)"
  fi
}

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

# The control characters json_str_to drops: every one below 0x20 but tab, line
# feed and carriage return, which it escapes. A list, not a range, so the
# locale's collation order cannot widen it.
JSON_DROP=$'[\001\002\003\004\005\006\007\010\013\014\016\017\020\021\022\023\024\025\026\027\030\031\032\033\034\035\036\037]'

# json_str_to VAR VALUE: VALUE as a JSON string, assigned to VAR without a
# subshell, so run_query can build its record line without forking.
json_str_to() {
  # A name no caller's VAR uses: printf -v would set this local instead.
  local __json_s="${2//$JSON_DROP/}"
  __json_s="${__json_s//\\/\\\\}"
  __json_s="${__json_s//\"/\\\"}"
  __json_s="${__json_s//$'\n'/\\n}"
  __json_s="${__json_s//$'\t'/\\t}"
  __json_s="${__json_s//$'\r'/\\r}"
  printf -v "$1" '"%s"' "$__json_s"
}

json_str() {
  local s
  json_str_to s "$1"
  printf '%s' "$s"
}

# json_num_to VAR VALUE: VALUE as a JSON number when it is a decimal number,
# its leading zeros dropped (curl's "000" is 0); null otherwise. Assigned to
# VAR without a subshell.
json_num_to() {
  if [[ "$2" =~ ^(-?)0*([0-9]+(\.[0-9]+)?)$ ]]; then
    printf -v "$1" '%s%s' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
  else
    printf -v "$1" 'null'
  fi
}

# json_num VALUE: json_num_to's value printed.
json_num() {
  local n
  json_num_to n "$1"
  printf '%s' "$n"
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
  ROWS_EXPECTED="$(toml rows_expected)" || return 1
  RUSTFS_URL="$(toml rustfs_url)" || return 1
  RUSTFS_SHA256="$(toml rustfs_sha256)" || return 1
  MEMORY_BUDGET="$(toml memory_budget_bytes)" || return 1
  AUDIT_MODE="$(toml audit_mode)" || return 1
  AUDIT_TEXT="$(toml_or audit_text redacted)"
  EXPECTED_STATEMENTS="$(toml_or expected_statements 43)"
  FOLD_MIN_RATIO="$(toml_or fold_min_entry_ratio 0.8)"
  QUERY_TIMEOUT_S="$(toml_or query_timeout_s 900)"
  CONC_ROUNDS="$(toml_or concurrent_rounds 3)"
  CONC_CONNECTIONS="$(toml_or concurrent_connections 10)"
  CONC_DURATION="$(toml_or concurrent_duration_s 600)"
  CONC_SEED="$(toml_or concurrent_seed clickbench)"
  Q29_WARMUP="$(toml_or q29_probe_warmup 1)"
  Q29_RUNS="$(toml_or q29_probe_runs 3)"
  PERF_RECORD="$(toml_or perf_record false)"
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

# terminate_self: terminate through the instance role when the region is
# known, then `shutdown -h now` whatever happened, so a run that failed before
# load_config set REGION still ends its instance.
terminate_self() {
  local token id
  if [ -n "${REGION:-}" ]; then
    token="$(curl -sS --max-time 5 -X PUT "$IMDS/latest/api/token" \
      -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')" || token=""
    id="$(curl -sS --max-time 5 -H "X-aws-ec2-metadata-token: $token" \
      "$IMDS/latest/meta-data/instance-id")" || id=""
    if [[ "$id" =~ ^i-[0-9a-z]+$ ]] \
      && aws ec2 terminate-instances --region "$REGION" --instance-ids "$id" --output text >/dev/null; then
      stamp "terminate instance=$id"
    else
      stamp "terminate fallback=shutdown"
    fi
  else
    stamp "terminate fallback=shutdown reason=no-region"
  fi
  shutdown_now
}

shutdown_now() {
  trap - EXIT HUP INT TERM
  shutdown -h now
}

# arm_shutdown: from here until shutdown_now, any exit or signal still shuts
# the box down. The signal traps shut down themselves because an EXIT trap set
# inside a running EXIT handler (fail_run from on_exit) never fires.
arm_shutdown() {
  trap shutdown_now EXIT
  trap 'shutdown_now; exit 1' HUP INT TERM
}

# fail_run STEP: FAILED naming STEP, partial outputs uploaded, instance gone.
# FAILED quotes run.log's last 20 lines with every secret redacted, or says it
# withheld them when a secret survives.
fail_run() {
  local name="$1" tail_file="$KIT/FAILED.tail"
  arm_shutdown
  FINISHED=1
  stop_samplers
  stop_server
  printf 'FAILED step=%s at=%s\n' "$name" "$(utc_now)" >"$KIT/FAILED"
  if tail -n 20 "$KIT/run.log" >"$tail_file" 2>/dev/null; then
    if scrub "$tail_file" && ! leaked "$tail_file"; then
      cat "$tail_file" >>"$KIT/FAILED"
    else
      echo "run.log tail withheld: a box secret survived redaction" >>"$KIT/FAILED"
    fi
  fi
  rm -f "$tail_file"
  write_evidence || true
  if [ -n "${OUT_URI:-}" ] && [ -n "${REGION:-}" ]; then
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
  arm_shutdown
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

# secrets: RustFS keys, the tenant token and the audit token key, generated on
# the box once and never uploaded. Every value is redacted from the evidence
# and from FAILED.
make_secrets() {
  if [ ! -s "$SECRETS_ENV" ]; then
    (
      umask 077
      printf 'RUSTFS_ACCESS_KEY=%s\nRUSTFS_SECRET_KEY=%s\nKIT_TENANT_TOKEN=%s\nRAVEL_AUDIT_TOKEN_KEY=%s\n' \
        "$(openssl rand -hex 12)" "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" \
        "$(openssl rand -hex 32)" >"$SECRETS_ENV"
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
  launch_server || return 1
  wait_server_ready
}

# launch_server: the server started in the background from arm.env, its pid in
# SERVER_PID and server.pid, without waiting for readiness.
launch_server() {
  load_arm_env || return 1
  make_secrets || return 1
  # The server reads the key only for redacted audit text and reports it set
  # under any other mode.
  if [ "$AUDIT_MODE" = required ] && [ "$AUDIT_TEXT" = redacted ]; then
    export RAVEL_AUDIT_TOKEN_KEY
  fi
  SERVER_N=$((SERVER_N + 1))
  SERVER_LOG="$KIT/server-$SERVER_N.log"
  LD_PRELOAD="$SERVER_PRELOAD" "$SERVER_BIN" "${SERVER_FLAGS[@]}" </dev/null >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  printf '%s\n' "$SERVER_PID" >"$KIT/server.pid"
  stamp "server start n=$SERVER_N pid=$SERVER_PID log=$(basename "$SERVER_LOG")"
}

# wait_server_ready [DEADLINE]: polls READY_CHECK (default readyz_check) up to
# READY_TRIES times READY_INTERVAL apart. Returns 1 when the server exits or
# never becomes ready, and 2 when DEADLINE (whole epoch seconds) passes first.
wait_server_ready() {
  local i
  for ((i = 1; i <= READY_TRIES; i++)); do
    "${READY_CHECK:-readyz_check}" && return 0
    kill -0 "$SERVER_PID" 2>/dev/null || { say "server exited before ready; see $SERVER_LOG"; return 1; }
    [ -z "${1:-}" ] || [ "$(date +%s)" -lt "$1" ] || return 2
    sleep "$READY_INTERVAL"
  done
  say "server not ready after $READY_TRIES tries ${READY_INTERVAL}s apart"
  return 1
}

# readyz_check: the health listener's readiness, the probe every step but the
# concurrent phase uses.
readyz_check() {
  curl -sf --max-time 2 "$READY_URL" >/dev/null 2>&1
}

# sql_check: upstream ClickBench's ravel/check, a POST of SELECT 1 to the SQL
# endpoint with curl --fail-with-body, its body naming the window every other
# call names (CHECK_BODY, written by phase_concurrent). It fails on a refusal
# of SELECT 1 that readyz would not see.
#
# The one deliberate difference from upstream, whose check has no timeout:
# CHECK_MAX_TIME (phase_concurrent sets it to 5 s or the watchdog interval,
# whichever is smaller) bounds it, so a server that accepts the connection and
# never answers fails the check and is restarted, instead of blocking the
# watchdog, and with it the round's end, forever. It is a health probe, not a
# measured call.
sql_check() {
  curl -sS --fail-with-body --max-time "${CHECK_MAX_TIME:-5}" -o /dev/null -H @"$AUTH_HEADER" \
    -H 'Content-Type: application/json' --data-binary @"$CHECK_BODY" "$SQL_URL" >/dev/null 2>&1
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

# timed FILE FORMAT CMD...: CMD under /usr/bin/time, its figures in FILE.
timed() {
  local file="$1" format="$2"
  shift 2
  /usr/bin/time -f "$format" -o "$file" "$@"
}

# out_field FILE KEY: the value of FILE's one `KEY: value` line. No such line,
# or more than one, is a failure naming the count.
out_field() {
  local n
  n="$(grep -c "^[[:space:]]*$2[[:space:]]*:" "$1" 2>/dev/null)"
  [ "$n" = 1 ] || { say "$(basename "$1") carries $2 ${n:-0} times, not once"; return 1; }
  sed -n "s/^[[:space:]]*$2[[:space:]]*:[[:space:]]*//p" "$1"
}

# do_load: the load, then its row count: rows_written must be run.toml's
# rows_expected, or every later figure measures other data.
do_load() {
  local rows
  timed "$KIT/load.time" 'wall=%e rss_kb=%M user=%U sys=%S' \
    "$CLI_BIN" --store s3 --tenant-hash-unkeyed load \
    --parquet "$DATASET" \
    --tenant "$TENANT" \
    --mapping "$MAPPING" \
    --shards "$SHARDS" \
    --read-cursors 8 \
    --batch-rows 65536 >"$KIT/load.out" || return 1
  rows="$(out_field "$KIT/load.out" rows_written)" || return 1
  [ "$rows" = "$ROWS_EXPECTED" ] \
    || { say "load wrote $rows rows, run.toml rows_expected is $ROWS_EXPECTED"; return 1; }
}

do_compact() {
  timed "$KIT/compact.time" 'wall=%e rss_kb=%M' \
    "$CLI_BIN" --store s3 --tenant-hash-unkeyed maintain compact-tenant \
    --tenant "$TENANT" --signal logs --shards "$SHARDS"
}

fold_field() {
  out_field "$KIT/fold.out" "$1"
}

# do_fold: the fold, then its precondition: a sealed hour, and entry_count at
# least FOLD_MIN_RATIO of the objects the load wrote.
do_fold() {
  "$CLI_BIN" --store s3 --tenant-hash-unkeyed catalog fold \
    --tenant "$TENANT" --shards "$SHARDS" --signal logs --writers-stopped \
    >"$KIT/fold.out" 2>&1 || return 1
  local seal entries watermark buckets part before after written ok
  seal="$(fold_field seal_through_hour)" || return 1
  watermark="$(fold_field watermark_hour)" || return 1
  entries="$(fold_field entry_count)" || return 1
  buckets="$(fold_field buckets_folded)" || return 1
  part="$(fold_field part_bytes)" || return 1
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

# set_query_window: the window every request body names, set once so the
# row-count check and every phase read the same data.
set_query_window() {
  WINDOW_END="$(date -u +%s)"
  WINDOW_START=$((WINDOW_END - WINDOW_HOURS * 3600))
  mkdir -p "$STAMPS"
  printf '{"start": %s, "end": %s, "window_hours": %s}\n' "$WINDOW_START" "$WINDOW_END" \
    "$WINDOW_HOURS" >"$STAMPS/query_window"
}

# do_verify: the corpus's q01, SELECT COUNT(*) FROM logs, once through the
# benchmark's endpoint and window. Its one cell must be run.toml's
# rows_expected; otherwise every answer the benchmark checks is checked
# against data the server does not see.
do_verify() {
  local dir="$KIT/verify" n id exp w code=0 count
  set_query_window
  rm -rf "$dir"
  kit_py request-bodies "$CORPUS" "$dir/requests" "$WINDOW_START" "$WINDOW_END" >/dev/null || return 1
  IFS=$'\t' read -r n id exp <"$dir/requests/index.tsv"
  python3 -c 'import json, sys
sys.exit(json.load(open(sys.argv[1]))["query"] != "SELECT COUNT(*) FROM logs")' "$dir/requests/$n.json" \
    || { say "corpus statement $id is not SELECT COUNT(*) FROM logs"; return 1; }
  start_server || return 1
  w="$(curl -sS -o "$dir/count.json" -w '%{http_code} %{time_total}' --max-time "$QUERY_TIMEOUT_S" \
    -H @"$AUTH_HEADER" -H 'Content-Type: application/json' \
    --data-binary @"$dir/requests/$n.json" "$SQL_URL" 2>"$dir/count.curl")" || code=$?
  stop_server
  if [ "$code" -ne 0 ] || [ "${w%% *}" != 200 ]; then
    say "row-count check: curl exit $code, HTTP ${w%% *}: $(head -c 300 "$dir/count.json" 2>/dev/null)"
    return 1
  fi
  count="$(python3 -c 'import json, sys
rows = json.load(open(sys.argv[1]))["data"]["rows"]
n = rows[0][0] if len(rows) == 1 and len(rows[0]) == 1 else None
if not isinstance(n, int) or isinstance(n, bool):
    sys.exit(1)
print(n)' "$dir/count.json" 2>/dev/null)" \
    || { say "row-count check: answer is not one whole-number cell: $(head -c 300 "$dir/count.json")"; return 1; }
  mkdir -p "$STAMPS"
  printf '{"rows_expected": %s, "load_rows_written": %s, "count_star": %s, "statement": %s, "verified_at": %s}\n' \
    "$(json_num "$ROWS_EXPECTED")" "$(json_num "$(out_field "$KIT/load.out" rows_written)")" \
    "$(json_num "$count")" "$(json_str "$id")" "$(json_str "$(utc_now)")" >"$STAMPS/row_count"
  [ "$count" = "$ROWS_EXPECTED" ] \
    || { say "SELECT COUNT(*) FROM logs answered $count, run.toml rows_expected is $ROWS_EXPECTED"; return 1; }
}

# --- queries ---------------------------------------------------------------

# run_query PHASE NUMBER STATEMENT TRY BODY_FILE [EXPECT_ROWS]: one SQL call,
# recorded as a line of out/<phase>/calls.jsonl. Sets RQ_OUTCOME (ok, error,
# timeout or unreachable), RQ_HTTP, RQ_DURATION, RQ_ROWS and RQ_CLASS. The
# line carries curl's exit code as curl_exit and the HTTP status as a number,
# 0 when nothing answered. An answer whose transfer failed after its status
# line is an error of class transfer. A 200 answer with a row count other
# than EXPECT_ROWS is an error of class wrong_answer, and one whose rows
# cannot be read is unreadable_answer.
# Returns non-zero only when the record could not be written.
#
# Optional globals: RQ_CALLS and RQ_RESP_DIR move the calls file and the
# responses directory, RQ_MAX_TIME replaces query_timeout_s (empty sets no
# client timeout), RQ_EXTRA is appended to the record as JSON members
# (`"round": 1, "connection": 3`), and RQ_STATS=1 records the response's
# stats object. RQ_DEFER=1 reads no answer: a call that completed with a
# status is recorded with a "pending" member naming its response file
# (relative to the calls file's directory) and EXPECT_ROWS, and kit.py
# concurrent-answers settles its rows, outcome, class and error later. With
# RQ_DEFER=1 and no RQ_STATS a call starts no interpreter and forks nothing
# but curl.
run_query() {
  local phase="$1" number="$2" statement="$3" try="$4" body="$5" expect="${6:-}"
  local dir="$OUT/$phase" resp w code=0 parsed err="" stats="" pending=""
  local calls="${RQ_CALLS:-$dir/calls.jsonl}" rdir="${RQ_RESP_DIR:-$dir/responses}"
  local limit="${RQ_MAX_TIME-$QUERY_TIMEOUT_S}" opts=()
  local j_statement j_arm j_phase j_start j_outcome j_class j_err j_dur j_http j_rows j_try j_resp j_exp
  [ -z "$limit" ] || opts=(--max-time "$limit")
  [ -d "$rdir" ] || mkdir -p "$rdir" || return 1
  resp="$rdir/$number-$try.json"
  utc_now_to RQ_START
  w="$(curl -sS -o "$resp" -w '%{http_code} %{time_total}' ${opts[@]+"${opts[@]}"} \
    -H @"$AUTH_HEADER" -H 'Content-Type: application/json' \
    --data-binary @"$body" "$SQL_URL" 2>"$rdir/$number-$try.curl")" || code=$?
  RQ_HTTP="${w%% *}"
  RQ_DURATION="${w##* }"
  RQ_CLASS=""
  RQ_ROWS=""
  if [ "$code" -eq 28 ]; then
    RQ_OUTCOME=timeout
    RQ_CLASS=client_timeout
    if [ -n "$limit" ]; then
      err="no answer within $limit s"
    else
      err="transfer timed out (curl exit 28) with no client time limit set"
    fi
  elif [ "${RQ_HTTP:-000}" = 000 ]; then
    RQ_OUTCOME=unreachable
    RQ_CLASS=transport
    err="curl exit $code: $(head -c 300 "$rdir/$number-$try.curl" 2>/dev/null)"
  elif [ "$code" -ne 0 ]; then
    RQ_OUTCOME=error
    RQ_CLASS=transfer
    err="HTTP $RQ_HTTP, then curl exit $code: $(head -c 300 "$rdir/$number-$try.curl" 2>/dev/null)"
  elif [ "${RQ_DEFER:-}" = 1 ]; then
    RQ_OUTCOME=error
    [ "$RQ_HTTP" != 200 ] || RQ_OUTCOME=ok
    json_str_to j_resp "${resp#"${calls%/*}/"}"
    json_num_to j_exp "$expect"
    pending=", \"pending\": {\"response\": $j_resp, \"expect\": $j_exp}"
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
  # A failed call's curl stderr is in err now; a call that exited 0 printed
  # none, and concurrent_round removes its empty file after the round.
  [ "$code" = 0 ] || rm -f "$rdir/$number-$try.curl"
  if [ "${RQ_STATS:-}" = 1 ]; then
    stats="$(python3 -c 'import json, sys
print(json.dumps(json.load(open(sys.argv[1])).get("stats")))' "$resp" 2>/dev/null)" || stats=""
    stats=", \"stats\": ${stats:-null}"
  fi
  # Keep the first try's answer and every error; later ok answers repeat it.
  # A deferred answer stays until concurrent-answers has read it.
  [ "$RQ_OUTCOME" = ok ] && [ "$try" != 1 ] && [ -z "$pending" ] && rm -f "$resp"
  json_str_to j_statement "$statement"
  json_str_to j_arm "$ARM"
  json_str_to j_phase "$phase"
  json_str_to j_start "$RQ_START"
  json_str_to j_outcome "$RQ_OUTCOME"
  json_str_to j_class "$RQ_CLASS"
  json_str_to j_err "$err"
  json_num_to j_try "$try"
  json_num_to j_dur "$RQ_DURATION"
  json_num_to j_http "${RQ_HTTP:-0}"
  json_num_to j_rows "$RQ_ROWS"
  printf '{"statement": %s, "number": %s, "arm": %s, "phase": %s, "try": %s, "start": %s, "duration_s": %s, "http_status": %s, "curl_exit": %s, "rows": %s, "outcome": %s, "refusal_class": %s, "error": %s%s%s%s}\n' \
    "$j_statement" "$((10#$number))" "$j_arm" "$j_phase" "$j_try" "$j_start" "$j_dur" "$j_http" \
    "$code" "$j_rows" "$j_outcome" "$j_class" "$j_err" "${RQ_EXTRA:+, $RQ_EXTRA}" "$stats" \
    "$pending" >>"$calls"
}

# --- phases ----------------------------------------------------------------

# phase_benchmark: every corpus statement three times. Try 1 is cold (server
# restarted and the page cache dropped first) and tries 2 and 3 are hot; the
# report's hot figure is the best of tries 2 and 3. Tries 2 and 3 must return
# try 1's row count when the corpus states none.
phase_benchmark() {
  local dir="$OUT/benchmark" count n id exp expect try
  [ -n "${WINDOW_END:-}" ] || set_query_window
  count="$(kit_py request-bodies "$CORPUS" "$dir/requests" "$WINDOW_START" "$WINDOW_END")" || return 1
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

# now_us: microseconds since the epoch, from bash 5's EPOCHREALTIME, else
# GNU date's nanoseconds, else python3 (macOS's bash 3.2 and BSD date).
now_us() {
  local t="${EPOCHREALTIME:-}"
  if [ -n "$t" ]; then
    printf '%s\n' "${t//[.,]/}"
    return 0
  fi
  t="$(date +%s%N)"
  if [[ "$t" =~ ^[0-9]{19}$ ]]; then
    printf '%s\n' "${t%???}"
    return 0
  fi
  python3 -c 'import time; print(time.time_ns() // 1000)'
}

# phase_requests DIR: the corpus's request bodies under DIR/requests, which
# must hold expected_statements of them.
phase_requests() {
  local count
  [ -n "${WINDOW_END:-}" ] || set_query_window
  count="$(kit_py request-bodies "$CORPUS" "$1/requests" "$WINDOW_START" "$WINDOW_END")" || return 1
  [ "$count" = "$EXPECTED_STATEMENTS" ] \
    || { say "corpus holds $count statements, run.toml expects $EXPECTED_STATEMENTS"; return 1; }
}

# phase_concurrent: upstream ClickBench's bench_concurrent_qps, run
# concurrent_rounds times back to back. Each round restarts the server on a
# dropped page cache, then concurrent_connections connections each loop over
# every statement in their own seeded order until concurrent_duration_s has
# passed, with no client timeout; a call in flight at the deadline completes
# and is counted. The round's deadline is whole seconds, `date +%s` plus the
# duration, as upstream computes it. A watchdog restarts a server that fails
# upstream's check (sql_check, also the pre-round readiness poll, bounded by
# CHECK_MAX_TIME where upstream's has no timeout) without telling the
# connections, so calls during the outage are errors.
# kit.py concurrent-summary turns calls.jsonl and rounds.jsonl into the
# results.json block.
phase_concurrent() {
  local dir="$OUT/concurrent" r
  phase_requests "$dir" || return 1
  CHECK_BODY="$dir/check.json"
  CHECK_MAX_TIME="$(awk -v s="$WATCHDOG_INTERVAL" 'BEGIN { print (s < 5 ? s : 5) }')"
  printf '{"query": "SELECT 1", "start": %s, "end": %s}\n' "$WINDOW_START" "$WINDOW_END" >"$CHECK_BODY"
  : >"$dir/calls.jsonl"
  : >"$dir/rounds.jsonl"
  start_samplers concurrent
  for ((r = 1; r <= CONC_ROUNDS; r++)); do
    concurrent_round "$r" || { stop_samplers; return 1; }
  done
  stop_server
  stop_samplers
  kit_py concurrent-summary "$dir" >"$dir/summary.json"
}

# concurrent_round ROUND: one round into out/concurrent/round-ROUND/, then,
# after its timed window, its answers read (kit.py concurrent-answers settles
# each deferred call's rows and wrong_answer), its calls appended to
# calls.jsonl in connection order and its line to rounds.jsonl.
concurrent_round() {
  local r="$1" dir="$OUT/concurrent" rdir n id exp c nq start deadline end last pids="" failed=0 answers
  local READY_CHECK=sql_check
  local numbers=() ids=() expects=() orders=()
  rdir="$dir/round-$r"
  rm -rf "$rdir"
  mkdir -p "$rdir" || return 1
  while IFS=$'\t' read -r n id exp; do
    numbers+=("$n")
    ids+=("$id")
    expects+=("$exp")
  done <"$dir/requests/index.tsv"
  nq="${#numbers[@]}"
  for ((c = 1; c <= CONC_CONNECTIONS; c++)); do
    orders[c]="$(kit_py permutation "$CONC_SEED" "$c" "$nq" | tr '\n' ' ')" || return 1
  done
  stop_server
  drop_caches || return 1
  start_server || return 1
  stamp "concurrent round=$r connections=$CONC_CONNECTIONS duration_s=$CONC_DURATION"
  start="$(now_us)" || return 1
  deadline=$(($(date +%s) + CONC_DURATION))
  for ((c = 1; c <= CONC_CONNECTIONS; c++)); do
    concurrent_worker "$r" "$c" "$deadline" "${orders[c]}" &
    pids="$pids $!"
  done
  concurrent_watchdog "$deadline" >"$rdir/watchdog.log" 2>&1 &
  pids="$pids $!"
  for c in $pids; do wait "$c" || failed=1; done
  end="$(now_us)" || return 1
  [ "$failed" = 0 ] || { say "a concurrent connection could not record its calls in round $r"; return 1; }
  answers="$(kit_py concurrent-answers "$rdir")" || return 1
  stamp "concurrent answers round=$r: $answers"
  SERVER_PID="$(cat "$KIT/server.pid")"
  last="$(sed -n 's/^.*restart n=\([0-9]*\) .*$/\1/p' "$rdir/watchdog.log" | tail -n 1)"
  [ -z "$last" ] || SERVER_N="$last"
  for ((c = 1; c <= CONC_CONNECTIONS; c++)); do
    [ ! -f "$rdir/conn-$c.jsonl" ] || cat "$rdir/conn-$c.jsonl" >>"$dir/calls.jsonl" || return 1
    rm -f "$rdir/conn-$c.jsonl"
  done
  find "$rdir" -name '*.curl' -size 0 -delete
  printf '{"round": %s, "duration_s": %s, "wall_s": %s, "connections": %s, "statements": %s, "seed": %s}\n' \
    "$r" "$(json_num "$CONC_DURATION")" \
    "$(awk -v a="$start" -v b="$end" 'BEGIN { printf "%.6f", (b - a) / 1000000 }')" \
    "$CONC_CONNECTIONS" "$nq" "$(json_str "$CONC_SEED")" >>"$dir/rounds.jsonl"
}

# concurrent_worker ROUND CONNECTION DEADLINE ORDER: connection CONNECTION's
# loop, reading numbers, ids and expects from concurrent_round. It checks the
# deadline, whole epoch seconds, before each call and never after one. Its
# calls are deferred (RQ_DEFER=1): no interpreter runs inside the round's
# timed window, and concurrent_round reads the answers after it.
concurrent_worker() {
  local r="$1" c="$2" deadline="$3" i=0 try=0 now expect
  # shellcheck disable=SC2206 # ORDER is a space-separated list of indexes
  local order=($4)
  local RQ_CALLS="$OUT/concurrent/round-$r/conn-$c.jsonl"
  local RQ_RESP_DIR="$OUT/concurrent/round-$r/conn-$c"
  local RQ_MAX_TIME="" RQ_DEFER=1 RQ_EXTRA="\"round\": $r, \"connection\": $c"
  local nq="${#order[@]}" k
  while :; do
    # bash 5's EPOCHSECONDS, so a call does not fork date; else date.
    now="${EPOCHSECONDS:-}"
    [ -n "$now" ] || now="$(date +%s)" || return 1
    [ "$now" -lt "$deadline" ] || return 0
    k="${order[i]}"
    try=$((try + 1))
    expect="${expects[k]}"
    [ "$expect" != - ] || expect=""
    run_query concurrent "${numbers[k]}" "${ids[k]}" "$try" \
      "$OUT/concurrent/requests/${numbers[k]}.json" "$expect" || return 1
    i=$(((i + 1) % nq))
  done
}

# concurrent_watchdog DEADLINE: upstream's watchdog loop. While the deadline
# (whole epoch seconds) is ahead it sleeps KIT_WATCHDOG_INTERVAL seconds
# (default 5), then runs sql_check; on a failure it restarts the server and
# polls sql_check up to KIT_READY_TRIES times (default 600) KIT_READY_INTERVAL
# apart, or until the deadline. Each restart is logged as `restart n=N pid=P`.
# It never fails the round.
#
# Two differences from upstream, on purpose. The last sleep is cut short at
# the deadline and no check runs after it, where upstream sleeps the full
# interval and checks once more, so the round ends at its deadline; a check
# upstream runs after the deadline affects no call. And the restart waits as
# long as the kit's own server start (READY_TRIES), where upstream polls 60
# times 1 s apart: a server that needs longer, as one on a page cache just
# dropped can, would otherwise be given up on and then stopped by the next
# failed check while still booting, again and again until the deadline, and
# the whole round would read as an outage. No check runs while the restart's
# wait is in progress, so the watchdog never stops a server it is waiting on.
concurrent_watchdog() {
  local deadline="$1" now nap code
  while :; do
    now="$(now_us)" || return 0
    [ "$now" -lt "$((deadline * 1000000))" ] || return 0
    nap="$(awk -v d="$deadline" -v n="$now" -v s="$WATCHDOG_INTERVAL" \
      'BEGIN { r = d - n / 1000000; printf "%.3f", (r < s ? r : s) }')"
    sleep "$nap"
    [ "$(date +%s)" -lt "$deadline" ] || return 0
    sql_check && continue
    stamp "concurrent watchdog: SELECT 1 check failed, restarting the server"
    stop_server
    launch_server || { echo "restart failed: server not started"; continue; }
    echo "restart n=$SERVER_N pid=$SERVER_PID"
    code=0
    wait_server_ready "$deadline" || code=$?
    case "$code" in
      1) echo "restart failed: server not ready" ;;
      2) echo "restart still starting at the round's deadline" ;;
    esac
  done
}

# phase_q29_probe: the corpus's q29 alone on a restarted server,
# q29_probe_warmup untimed calls then q29_probe_runs timed ones, each recorded
# with its duration and the response's stats. kit.py q29-summary takes min and
# median over the timed runs only.
phase_q29_probe() {
  local dir="$OUT/q29_probe" n id exp q="" expect i total probe
  phase_requests "$dir" || return 1
  while IFS=$'\t' read -r n id exp; do
    [[ "$id" =~ ^q29_ ]] || continue
    q="$n"
    break
  done <"$dir/requests/index.tsv"
  [ -n "$q" ] || { say "the corpus has no q29 statement"; return 1; }
  : >"$dir/calls.jsonl"
  stop_server
  start_server || return 1
  expect=""
  [ "$exp" = - ] || expect="$exp"
  total=$((Q29_WARMUP + Q29_RUNS))
  for ((i = 1; i <= total; i++)); do
    probe=run
    [ "$i" -gt "$Q29_WARMUP" ] || probe=warmup
    RQ_STATS=1 RQ_EXTRA="\"probe\": \"$probe\"" \
      run_query q29_probe "$q" "$id" "$i" "$dir/requests/$q.json" "$expect" || return 1
    [ -n "$expect" ] || expect="$RQ_ROWS"
  done
  stop_server
  kit_py q29-summary "$dir" >"$dir/summary.json"
}

# phase_perf: needs perf_record = true (check_run refuses it otherwise). Per
# statement, one warm call, then one hot call under
# `perf record -F 99 -g -p <server pid>`; the samples are kept as folded
# stacks in out/perf/NN.folded, never as perf.data: each statement's
# perf.data is removed whether or not its profile succeeded.
phase_perf() {
  local dir="$OUT/perf" n id exp code
  [ "$PERF_RECORD" = true ] || { say "phase perf needs perf_record = true"; return 1; }
  command -v perf >/dev/null || { say "phase perf needs perf on the box"; return 1; }
  phase_requests "$dir" || return 1
  : >"$dir/calls.jsonl"
  stop_server
  start_server || return 1
  while IFS=$'\t' read -r n id exp; do
    code=0
    perf_statement "$dir" "$n" "$id" "$exp" || code=$?
    rm -f "$dir/$n.data"
    [ "$code" = 0 ] || return 1
  done <"$dir/requests/index.tsv"
  stop_server
}

# perf_statement DIR NUMBER STATEMENT EXPECTED: phase_perf's warm call and
# profiled hot call for one statement, folded into DIR/NUMBER.folded. It
# leaves DIR/NUMBER.data for its caller to remove.
perf_statement() {
  local dir="$1" n="$2" id="$3" expect="$4" ppid
  [ "$expect" != - ] || expect=""
  RQ_EXTRA='"profiled": false' run_query perf "$n" "$id" 1 "$dir/requests/$n.json" "$expect" || return 1
  [ -n "$expect" ] || expect="$RQ_ROWS"
  perf record -F 99 -g -p "$SERVER_PID" -o "$dir/$n.data" >"$dir/$n.record.log" 2>&1 &
  ppid=$!
  sleep "$PERF_SETTLE"
  RQ_EXTRA='"profiled": true' run_query perf "$n" "$id" 2 "$dir/requests/$n.json" "$expect" \
    || { kill -TERM "$ppid" 2>/dev/null; wait "$ppid"; return 1; }
  kill -TERM "$ppid" 2>/dev/null
  wait "$ppid"
  perf script -i "$dir/$n.data" 2>"$dir/$n.script.log" | kit_py fold-perf >"$dir/$n.folded" \
    || { say "perf script for statement $id failed; see $dir/$n.script.log"; return 1; }
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
  mem="$(awk '/^MemTotal:/ { print $2 }' "$MEMINFO")"
  cpu="$(awk -F': ' '/^model name/ { print $2; exit }' "$CPUINFO")"
  # An arm64 /proc/cpuinfo has no model name line; lscpu still names the core.
  [ -n "$cpu" ] || cpu="$(lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -n 1)"
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
  local lane bands="" sections code=0 phase extra=()
  write_stamps || return 1
  for phase in $PHASES; do
    case "$phase" in
      concurrent|q29_probe) extra+=("$phase=$OUT/$phase/summary.json") ;;
    esac
  done
  for lane in $LANES; do bands="${bands:+$bands,}$lane:$SRC/bands/$lane.toml"; done
  # shellcheck disable=SC2086 # PHASES is a space-separated list of names
  kit_py phases "$OUT" $PHASES >"$KIT/phases.json" || return 1
  load_arm_env || return 1
  sections="$(printf '%s' "$BAND_SECTIONS" | tr -s ' ' ',')"
  kit_py assemble out="$KIT/results.json" calls="$OUT/benchmark/calls.jsonl" \
    index="$OUT/benchmark/requests/index.tsv" server_logs="$BENCHMARK_SERVER_LOGS" \
    bands="$bands" sections="${sections%,}" provenance="$STAMPS" arm="$ARM" sha="$SHA" \
    binary="$SERVER_BIN" tenant="$TENANT" phases_json="$KIT/phases.json" ${extra[@]+"${extra[@]}"} || return 1
  kit_py absolute "$ARM" "$KIT/results.json" >/dev/null || code=$?
  case "$code" in
    0) BANDS_VERDICT=pass ;;
    1) BANDS_VERDICT=miss ;;
    *) return 1 ;;
  esac
  stamp "bands=$BANDS_VERDICT"
}

# secret_values: every value in secrets.env, one per line; none before
# make_secrets has run.
secret_values() {
  [ ! -f "$SECRETS_ENV" ] || sed -n 's/^[A-Z_]*=//p' "$SECRETS_ENV"
}

# scrub FILE: every secrets.env value in FILE replaced by [redacted].
scrub() {
  local s
  for s in $(secret_values); do
    [[ "$s" =~ ^[0-9a-f]+$ ]] || { say "secrets.env holds a value that is not hex"; return 1; }
    if grep -qF -e "$s" "$1"; then
      sed -i.bak "s/$s/[redacted]/g" "$1" && rm -f "$1.bak" || return 1
    fi
  done
}

# leaked PATH: succeeds when any secrets.env value is still in PATH (a file or
# a directory, searched recursively).
leaked() {
  local s
  for s in $(secret_values); do
    grep -rqF -e "$s" "$1" && return 0
  done
  return 1
}

# write_evidence: evidence.tgz from a copy of the logs and outputs with every
# value in secrets.env replaced by [redacted]; RustFS or a server may log a key.
# A secret still present after that refuses the tarball.
write_evidence() {
  local items="" f g stage="$KIT/evidence-stage"
  rm -f "$KIT/evidence.tgz"
  for f in server-*.log rustfs.log run.log progress.log arm.env load.out fold.out load.time \
    compact.time object-counts stamps verify out phases.json burst-start.json burst-end.json; do
    # shellcheck disable=SC2086 # globs expand here on purpose
    for g in $(cd "$KIT" && ls -d $f 2>/dev/null); do items="$items $g"; done
  done
  [ -n "$items" ] || return 0
  rm -rf "$stage"
  mkdir -p "$stage" || return 1
  # shellcheck disable=SC2086 # items is a space-separated list of names
  (cd "$KIT" && cp -R $items "$stage/") || return 1
  # Per-call response files: the call lines already carry what they hold, and a
  # long outage leaves enough of them that scrubbing each one outlasts the box.
  rm -rf "$stage"/out/*/requests "$stage"/verify/requests "$stage"/out/concurrent/round-*/conn-*
  while IFS= read -r f; do
    scrub "$f" || return 1
  done < <(find "$stage" -type f)
  if leaked "$stage"; then
    say "a box secret is still in the evidence after redaction; evidence.tgz not written"
    return 1
  fi
  # shellcheck disable=SC2086 # items is a space-separated list of names
  tar -czf "$KIT/evidence.tgz" -C "$stage" $items || return 1
  rm -rf "$stage"
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
  step verify do_verify
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
