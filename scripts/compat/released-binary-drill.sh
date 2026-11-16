#!/usr/bin/env bash
# Released-binary fence drill (issue #4243, ADR-4233 decision 4).
#
# Runs the published v0.33.0 ravel-server against isolated buckets on a local
# RustFS process, fences each tenant/signal bucket the way `compat activate`
# does (crates/ravel-catalog/examples/compat_fence_bucket.rs), and checks that
# no v0.33.0 write path publishes into a fenced bucket after its grace
# horizon. Never point it at a shared or production bucket: it creates its
# own RustFS data directory and buckets under $DRILL_RUN.
#
# usage: released-binary-drill.sh PHASE
#   preflight    verify the assets (sha256, cosign) and print the stamps
#   start        start RustFS and create the buckets
#   warm-start   (a) for the warm bucket: v0.33.0 all + maintain, the driver
#   warm-fence   (b)+(d) fence the warm bucket while the driver runs
#   warm-finish  (d) after the horizon: listings, graceful stop, verdict
#   cold         (a)+(b)+(c) a second bucket, fenced, then a cold restart
#   read         (e) v0.33.0 and main read the unfenced v0.33.0 copy
#   verdict      fold every phase's verdict into the report; exit non-zero on
#                a post-horizon fenced publish, a zero positive control or a
#                read diff
#   stop         stop every process this drill started
#   drive DIR PORT   (internal) the request loop the warm and cold phases run
#
# Environment (defaults in brackets):
#   DRILL_RUN       run directory [.dd-tools/drill-run]
#   DRILL_V033      v0.33.0 asset directory [.dd-tools/v033]
#   DRILL_RUSTFS    RustFS binary [.dd-tools/rustfs/rustfs]
#   DRILL_FENCE     compat_fence_bucket example binary
#   DRILL_MAIN      main ravel-server binary (cargo build --release -p ravel-server --features sql)
#   DRILL_PORT      first of the local ports the drill binds [39410]
#   DRILL_COLD_SECS how long the cold restart is driven [240]
#   DRILL_COLD_BUCKET, DRILL_READ_BUCKET  fresh bucket names [drill-cold, drill-read]
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
HERE="$ROOT/scripts/compat"
# shellcheck source=scripts/compat/drill-lib.sh
. "$HERE/drill-lib.sh"

RUN=${DRILL_RUN:-$ROOT/.dd-tools/drill-run}
V033=${DRILL_V033:-$ROOT/.dd-tools/v033}
RUSTFS=${DRILL_RUSTFS:-$ROOT/.dd-tools/rustfs/rustfs}
TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
FENCE=${DRILL_FENCE:-$TARGET/debug/examples/compat_fence_bucket}
MAIN=${DRILL_MAIN:-$TARGET/release/ravel-server}
PORT=${DRILL_PORT:-39410}
COLD_SECS=${DRILL_COLD_SECS:-240}
# The cold and read phases each need a bucket nothing wrote before; a rerun
# names a new one.
COLD_BUCKET=${DRILL_COLD_BUCKET:-drill-cold}
READ_BUCKET=${DRILL_READ_BUCKET:-drill-read}
TENANT=acme
# Upper bound on the life of every process the drill starts, so nothing
# outlives a drill that was abandoned midway.
LIFE=4h

# v0.33.0 grace horizon inputs, from the v0.33.0 source (.dd-tools/src033):
# the provisioning refresh interval (60 s, fixed), max_flush_lifetime (3600 s,
# compiled in) and the alert lease TTL (LEASE_TTL_TICKS = 3 evaluation
# intervals). T_REQ bounds one in-flight store request past its deadline.
REFRESH_S=60
MAX_FLUSH_LIFETIME_S=3600
ALERT_INTERVAL_S=1
LEASE_TTL_TICKS=3
T_REQ_S=60
# The spec's budget: a horizon plus setup beyond 3 h 15 min is NOT RUN.
BUDGET_S=11700

V033_SERVER="$V033/ravel-server-linux-amd64"
V033_CLI="$V033/ravel-cli-linux-amd64"

say() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() {
  say "refused: $*" >&2
  exit 1
}

load_creds() {
  [ -f "$RUN/creds" ] || die "no $RUN/creds: run the start phase first"
  # shellcheck disable=SC1091
  . "$RUN/creds"
  export AWS_ACCESS_KEY_ID=$DRILL_KEY AWS_SECRET_ACCESS_KEY=$DRILL_SECRET AWS_REGION=us-east-1
  export S3_ENDPOINT="http://127.0.0.1:$PORT"
  export RAVEL_S3_ENDPOINT=$S3_ENDPOINT RAVEL_S3_REGION=us-east-1
  export RAVEL_S3_ACCESS_KEY=$DRILL_KEY RAVEL_S3_SECRET_KEY=$DRILL_SECRET
}

ls_bucket() {
  python3 "$HERE/drill_s3.py" ls "$1" > "$2.tmp" || die "listing $1 failed"
  sort "$2.tmp" > "$2"
  rm -f "$2.tmp"
}

# Start a daemon in its own session, bounded by LIFE, pid in $1.pid.
spawn() {
  local name=$1
  shift
  setsid timeout "$LIFE" "$@" > "$name.log" 2>&1 < /dev/null &
  echo $! > "$name.pid"
}

stop_pid() {
  local pidfile=$1 pid i
  [ -f "$pidfile" ] || return 0
  pid=$(cat "$pidfile")
  if kill -0 "$pid" 2> /dev/null; then
    # timeout forwards SIGTERM to the server: a graceful stop.
    kill -TERM "$pid" 2> /dev/null || true
    for i in $(seq 1 120); do
      kill -0 "$pid" 2> /dev/null || break
      sleep 1
    done
    if kill -0 "$pid" 2> /dev/null; then
      say "$pidfile did not stop in 120 s, killing"
      kill -KILL "$pid" 2> /dev/null || true
    fi
  fi
  rm -f "$pidfile"
}

wait_ready() {
  local url=$1 i
  for i in $(seq 1 60); do
    if curl -fsS -o /dev/null "$url/readyz" 2> /dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

write_rules() {
  cat > "$1" << EOF
{"rules":[{"tenant":"$TENANT","rule_id":"drill-always","promql":"vector(1)","condition":{"type":"threshold","op":"gt","value":0},"for":"0s","labels":{"severity":"drill"},"annotations":{"summary":"drill"},"repeat_interval":"1m"}]}
EOF
}

# v0.33.0 in deployment-style flags: S3 store, keyed tenant hash, fold,
# alerts and required audit in one all-mode process, maintain beside it.
start_v033() {
  local dir=$1 bucket=$2 http=$3 grpc=$4 mhttp=$5 mgrpc=$6
  write_rules "$dir/rules.json"
  RAVEL_S3_BUCKET=$bucket NO_COLOR=1 spawn "$dir/server" "$V033_SERVER" --mode all --store s3 \
    --tenant-hash-key-file "$RUN/tenant.key" --dev-insecure-tenant-header \
    --listen-http "127.0.0.1:$http" --listen-grpc "127.0.0.1:$grpc" \
    --fold-interval-secs 1 --alert-rules-file "$dir/rules.json" \
    --alert-eval-interval-secs "$ALERT_INTERVAL_S" --audit-mode required \
    --cache-dir "$dir/cache"
  RAVEL_S3_BUCKET=$bucket NO_COLOR=1 spawn "$dir/maintain" "$V033_SERVER" --mode maintain --store s3 \
    --tenant-hash-key-file "$RUN/tenant.key" --maintain-key-file "$RUN/maintain.key" \
    --maintain-interval-secs 1 --listen-http "127.0.0.1:$mhttp" --listen-grpc "127.0.0.1:$mgrpc" \
    --cache-dir "$dir/mcache"
  wait_ready "http://127.0.0.1:$http" || {
    say "v0.33.0 server on $bucket never became ready (see $dir/server.log)"
    return 1
  }
}

qualify() {
  RAVEL_S3_BUCKET=$1 "$V033_CLI" --store s3 --tenant-hash-key-file "$RUN/tenant.key" store qualify \
    > "$2/qualify.log" 2>&1 || die "store qualify on $1 failed (see $2/qualify.log)"
}

# The request loop: every write path v0.33.0 exposes over HTTP, plus a PromQL
# query (audit), once a second, one log line per request with its status.
drive() {
  local dir=$1 http=$2 seq=0 kind code url
  local u="http://127.0.0.1:$http"
  while :; do
    seq=$((seq + 1))
    for kind in otlp-metrics otlp-logs otlp-traces remote-write; do
      python3 "$HERE/drill_payload.py" "$kind" "$seq" > "$dir/body.$kind"
      case "$kind" in
        otlp-metrics) url=$u/v1/metrics ;;
        otlp-logs) url=$u/v1/logs ;;
        otlp-traces) url=$u/v1/traces ;;
        remote-write) url=$u/api/v1/write ;;
      esac
      if [ "$kind" = remote-write ]; then
        code=$(curl -sS -o /dev/null -w '%{http_code}' -m 30 -H "x-ravel-tenant: $TENANT" \
          -H 'content-type: application/x-protobuf' -H 'content-encoding: snappy' \
          -H 'X-Prometheus-Remote-Write-Version: 0.1.0' --data-binary "@$dir/body.$kind" "$url" || echo 000)
      else
        code=$(curl -sS -o /dev/null -w '%{http_code}' -m 30 -H "x-ravel-tenant: $TENANT" \
          -H 'content-type: application/x-protobuf' --data-binary "@$dir/body.$kind" "$url" || echo 000)
      fi
      printf '%s\t%s\t%s\n' "$(date -u +%s)" "$kind" "$code"
    done
    code=$(curl -sS -o /dev/null -w '%{http_code}' -m 30 -H "x-ravel-tenant: $TENANT" \
      "$u/api/v1/query?query=drill_rw_gauge" || echo 000)
    printf '%s\t%s\t%s\n' "$(date -u +%s)" promql "$code"
    sleep 1
  done
}

# Fence m, l and s of the tenant and park its alert lease. Prints the
# example's lines into $2/fence.log and the fence's Unix second into
# $2/fence.epoch (the second after the last fence write returned).
fence_bucket() {
  local bucket=$1 dir=$2 sig
  : > "$dir/fence.log"
  date -u +%s > "$dir/fence.start"
  for sig in metrics logs spans; do
    "$FENCE" --endpoint "$S3_ENDPOINT" --bucket "$bucket" --tenant "$TENANT" \
      --tenant-hash-key-file "$RUN/tenant.key" --epoch 1 --signal "$sig" >> "$dir/fence.log" 2>&1 \
      || die "fence $bucket $sig failed (see $dir/fence.log)"
  done
  "$FENCE" --endpoint "$S3_ENDPOINT" --bucket "$bucket" --tenant "$TENANT" \
    --tenant-hash-key-file "$RUN/tenant.key" --epoch 1 --park-alert-lease >> "$dir/fence.log" 2>&1 \
    || die "park lease on $bucket failed (see $dir/fence.log)"
  date -u +%s > "$dir/fence.epoch"
  [ "$(grep -c '^prov-after .* format_version=4 compat_epoch=1 ' "$dir/fence.log")" -eq 3 ] \
    || die "fence of $bucket did not leave three v4 records (see $dir/fence.log)"
  grep -q '^lease-outcome outcome=\(replaced\|created\)' "$dir/fence.log" \
    || die "lease park on $bucket did not replace a held lease (see $dir/fence.log)"
  grep -q '^tenant=' "$dir/fence.log" && sed -n 's/^tenant=[^ ]* tenant_hash=//p' "$dir/fence.log" | head -n 1 > "$RUN/tenant.hash"
}

# Late-write check over a before/after listing pair. Records every key the
# pair shows written, with LastModified, and the late subset.
judge() {
  local dir=$1 before=$2 after=$3 fenced_after=$4 alerts_after=$5 tag=$6 h
  h=$(cat "$RUN/tenant.hash")
  drill_written_keys "$before" "$after" > "$dir/written.$tag"
  drill_removed_keys "$before" "$after" > "$dir/removed.$tag"
  if drill_late_writes "$dir/written.$tag" "$h" "$fenced_after" "$alerts_after" > "$dir/late.$tag"; then
    echo "verdict $tag pass late=0 written=$(grep -c . "$dir/written.$tag" || true)"
  else
    echo "verdict $tag FAIL late=$(grep -c . "$dir/late.$tag") written=$(grep -c . "$dir/written.$tag" || true)"
  fi
}

phase_preflight() {
  mkdir -p "$RUN"
  {
    echo "stamp uname_m $(uname -m)"
    echo "stamp nproc $(nproc)"
    (cd "$V033" && sha256sum -c SHA256SUMS --ignore-missing) | sed 's/^/stamp asset_check /'
    sha256sum "$V033_SERVER" "$V033_CLI" | sed 's/^/stamp asset_sha256 /'
    echo "stamp rustfs_version $("$RUSTFS" --version 2>&1 | head -n 1)"
    sha256sum "$RUSTFS" | sed 's/^/stamp rustfs_sha256 /'
    echo "stamp main_sha $(git -C "$ROOT" rev-parse HEAD)"
    if [ -x "$ROOT/.dd-tools/bin/cosign" ]; then
      "$ROOT/.dd-tools/bin/cosign" verify-blob --signature "$V033/SHA256SUMS.sig" \
        --certificate "$V033/SHA256SUMS.pem" \
        --certificate-identity-regexp '^https://github.com/NOFireAI/ravel/' \
        --certificate-oidc-issuer https://token.actions.githubusercontent.com \
        "$V033/SHA256SUMS" 2>&1 | sed 's/^/stamp cosign /'
    fi
  } | tee "$RUN/stamps"
}

phase_start() {
  mkdir -p "$RUN/rustfs-data"
  if [ ! -f "$RUN/creds" ]; then
    umask 077
    printf 'DRILL_KEY=%s\nDRILL_SECRET=%s\n' "drill$(openssl rand -hex 6)" "$(openssl rand -hex 20)" > "$RUN/creds"
    openssl rand -hex 32 > "$RUN/tenant.key"
    openssl rand -hex 32 > "$RUN/maintain.key"
    umask 022
  fi
  load_creds
  RUSTFS_ACCESS_KEY=$DRILL_KEY RUSTFS_SECRET_KEY=$DRILL_SECRET \
    spawn "$RUN/rustfs" "$RUSTFS" server --address "127.0.0.1:$PORT" "$RUN/rustfs-data"
  local i
  for i in $(seq 1 30); do
    python3 "$HERE/drill_s3.py" mb drill-warm 2> /dev/null && break
    sleep 1
  done
  python3 "$HERE/drill_s3.py" ls drill-warm > /dev/null || die "RustFS on $PORT did not come up"
  say "RustFS on 127.0.0.1:$PORT, bucket drill-warm"
}

phase_warm_start() {
  load_creds
  local d=$RUN/warm i h
  mkdir -p "$d"
  qualify drill-warm "$d"
  start_v033 "$d" drill-warm $((PORT + 1)) $((PORT + 2)) $((PORT + 3)) $((PORT + 4)) || die "warm start"
  spawn "$d/drive" bash "$0" drive "$d" $((PORT + 1))
  say "warm: v0.33.0 up, driver running; waiting for the positive control"
  # The hash is only known once the example ran; derive it from the first
  # tenant key the server wrote under t/.
  for i in $(seq 1 90); do
    ls_bucket drill-warm "$d/pre.list"
    h=$(awk -F'\t' '$1 ~ /^t\/[0-9a-f]+\/m\/prov$/ { split($1, p, "/"); print p[2]; exit }' "$d/pre.list")
    if [ -n "$h" ] && pc_ready "$d/pre.list" "$h"; then
      break
    fi
    sleep 2
  done
  [ -n "$h" ] || die "warm: no provisioning record appeared"
  echo "$h" > "$RUN/tenant.hash"
  : > "$d/report"
  positive_control "$d/pre.list" "$h" "$d/report" | tee "$d/pc.out"
  say "warm: positive control done (tenant hash $h)"
}

phase_warm_fence() {
  load_creds
  local d=$RUN/warm w horizon alerts_h now
  ls_bucket drill-warm "$d/prefence.list"
  fence_bucket drill-warm "$d"
  ls_bucket drill-warm "$d/fenced.list"
  w=$(cat "$d/fence.epoch")
  horizon=$(drill_ingest_horizon "$w" "$T_REQ_S")
  alerts_h=$((w + LEASE_TTL_TICKS * ALERT_INTERVAL_S + T_REQ_S))
  now=$(date -u +%s)
  {
    echo "horizon fence_unix=$w fence_utc=$(date -u -d "@$w" +%FT%TZ)"
    echo "horizon refresh_s=$REFRESH_S max_flush_lifetime_s=$MAX_FLUSH_LIFETIME_S t_req_s=$T_REQ_S"
    echo "horizon ingest_unix=$horizon ingest_utc=$(date -u -d "@$horizon" +%FT%TZ) after_fence_s=$((horizon - w))"
    echo "horizon alerts_unix=$alerts_h after_fence_s=$((alerts_h - w))"
  } | tee "$d/horizon"
  echo "$horizon" > "$d/horizon.ingest"
  echo "$alerts_h" > "$d/horizon.alerts"
  if [ $((horizon - now)) -gt "$BUDGET_S" ]; then
    say "warm: horizon $((horizon - now)) s away exceeds the $BUDGET_S s budget: (d) NOT RUN"
    echo "NOT RUN" > "$d/status"
  else
    echo "RUNNING" > "$d/status"
  fi
}

phase_warm_finish() {
  load_creds
  local d=$RUN/warm horizon alerts_h now
  horizon=$(cat "$d/horizon.ingest")
  alerts_h=$(cat "$d/horizon.alerts")
  now=$(date -u +%s)
  [ "$now" -gt "$horizon" ] || die "warm: horizon $(date -u -d "@$horizon" +%T) not passed yet ($((horizon - now)) s left)"
  ls_bucket drill-warm "$d/after.list"
  stop_pid "$d/drive.pid"
  stop_pid "$d/server.pid"
  stop_pid "$d/maintain.pid"
  sleep 5
  ls_bucket drill-warm "$d/teardown.list"
  {
    judge "$d" "$d/fenced.list" "$d/after.list" "$horizon" "$alerts_h" warm
    judge "$d" "$d/after.list" "$d/teardown.list" "$horizon" "$alerts_h" teardown
  } | tee "$d/verdict"
  echo "DONE" > "$d/status"
}

phase_cold() {
  load_creds
  local d=$RUN/cold c=$RUN/cold/restart i h w
  rm -rf "$d"
  mkdir -p "$d" "$c"
  python3 "$HERE/drill_s3.py" mb "$COLD_BUCKET" || die "bucket $COLD_BUCKET exists or RustFS is down: name a fresh DRILL_COLD_BUCKET"
  qualify "$COLD_BUCKET" "$d"
  start_v033 "$d" "$COLD_BUCKET" $((PORT + 11)) $((PORT + 12)) $((PORT + 13)) $((PORT + 14)) || die "cold start"
  spawn "$d/drive" bash "$0" drive "$d" $((PORT + 11))
  h=$(cat "$RUN/tenant.hash")
  for i in $(seq 1 90); do
    ls_bucket "$COLD_BUCKET" "$d/pre.list"
    pc_ready "$d/pre.list" "$h" && break
    sleep 2
  done
  stop_pid "$d/drive.pid"
  stop_pid "$d/server.pid"
  stop_pid "$d/maintain.pid"
  ls_bucket "$COLD_BUCKET" "$d/pre.list"
  : > "$d/report"
  positive_control "$d/pre.list" "$h" "$d/report" | tee "$d/pc.out" || true
  fence_bucket "$COLD_BUCKET" "$d"
  w=$(cat "$d/fence.epoch")
  ls_bucket "$COLD_BUCKET" "$d/fenced.list"
  # (c) cold restart: fresh cache directories, so nothing cached survives.
  start_v033 "$c" "$COLD_BUCKET" $((PORT + 15)) $((PORT + 16)) $((PORT + 17)) $((PORT + 18)) || true
  spawn "$c/drive" bash "$0" drive "$c" $((PORT + 15))
  say "cold: restarted on the fenced bucket, driving for $COLD_SECS s"
  # A listing midway, while the process runs, tells a periodic writer from
  # one that writes only at shutdown.
  sleep $((COLD_SECS / 2))
  ls_bucket "$COLD_BUCKET" "$c/mid.list"
  sleep $((COLD_SECS - COLD_SECS / 2))
  stop_pid "$c/drive.pid"
  stop_pid "$c/server.pid"
  stop_pid "$c/maintain.pid"
  ls_bucket "$COLD_BUCKET" "$c/after.list"
  # A cold process has no cached view to trust, so its horizon is the fence.
  {
    judge "$c" "$d/fenced.list" "$c/mid.list" "$w" "$w" cold-mid
    judge "$c" "$d/fenced.list" "$c/after.list" "$w" "$w" cold
  } | tee "$c/verdict"
}

# One read-only query against a server, its JSON normalized (keys sorted,
# timing fields dropped) so two binaries' answers compare byte for byte.
read_query() {
  local http=$1 name=$2 out=$3 kind=$4 arg=$5 code
  if [ "$kind" = sql ]; then
    code=$(curl -sS -o "$out.raw" -w '%{http_code}' -m 60 -X POST -H "x-ravel-tenant: $TENANT" \
      -H 'content-type: application/json' --data "{\"query\":\"$arg\"}" \
      "http://127.0.0.1:$http/api/v1/sql" || echo 000)
  else
    code=$(curl -sS -o "$out.raw" -w '%{http_code}' -m 60 -G -H "x-ravel-tenant: $TENANT" \
      --data-urlencode "query=$arg" --data-urlencode "start=$READ_START" \
      --data-urlencode "end=$READ_END" --data-urlencode step=1 \
      "http://127.0.0.1:$http/api/v1/query_range" || echo 000)
  fi
  echo "code $code" > "$out"
  python3 - "$out.raw" >> "$out" << 'EOF'
import json, sys
TIMING = {"stats", "took_ms", "elapsed_ms", "elapsed", "timings", "warnings", "infos"}
def strip(v):
    if isinstance(v, dict):
        return {k: strip(x) for k, x in v.items() if k not in TIMING}
    if isinstance(v, list):
        return [strip(x) for x in v]
    return v
raw = open(sys.argv[1], "rb").read()
try:
    print(json.dumps(strip(json.loads(raw)), sort_keys=True, indent=1))
except ValueError:
    sys.stdout.write(raw.decode("utf-8", "replace"))
EOF
  echo "$name $code"
}

READ_QUERIES=(
  "promql_otlp|promql|drill_otlp_gauge"
  "promql_rw|promql|drill_rw_gauge"
  "sql_samples|sql|SELECT ts, value FROM samples ORDER BY ts, value"
  "sql_samples_count|sql|SELECT count(*) AS n FROM samples"
  "sql_logs|sql|SELECT ts, severity_text, body FROM logs ORDER BY ts, body"
  "sql_logs_count|sql|SELECT count(*) AS n FROM logs"
  "sql_spans|sql|SELECT trace_id, span_id, name, start_ts, duration_ns FROM spans ORDER BY start_ts, span_id"
  "sql_spans_count|sql|SELECT count(*) AS n FROM spans"
)

run_reads() {
  local http=$1 dir=$2 q name kind arg
  mkdir -p "$dir"
  for q in "${READ_QUERIES[@]}"; do
    IFS='|' read -r name kind arg <<< "$q"
    read_query "$http" "$name" "$dir/$name" "$kind" "$arg"
  done
}

# (e) v0.33.0 writes an unfenced copy; v0.33.0 and main each read it in
# query mode with an empty cache; every answer must match exactly.
phase_read() {
  load_creds
  local d=$RUN/read h q name n fail=0
  rm -rf "$d"
  mkdir -p "$d"
  python3 "$HERE/drill_s3.py" mb "$READ_BUCKET" || die "bucket $READ_BUCKET exists or RustFS is down: name a fresh DRILL_READ_BUCKET"
  [ -x "$MAIN" ] || die "main ravel-server not built at $MAIN"
  h=$(cat "$RUN/tenant.hash")
  qualify "$READ_BUCKET" "$d"
  start_v033 "$d" "$READ_BUCKET" $((PORT + 21)) $((PORT + 22)) $((PORT + 23)) $((PORT + 24)) || die "read start"
  spawn "$d/drive" bash "$0" drive "$d" $((PORT + 21))
  sleep 60
  stop_pid "$d/drive.pid"
  stop_pid "$d/server.pid"
  stop_pid "$d/maintain.pid"
  ls_bucket "$READ_BUCKET" "$d/written.list"
  : > "$d/report"
  positive_control "$d/written.list" "$h" "$d/report" | tee "$d/pc.out" || fail=1
  READ_START=$(($(head -n 1 "$d/drive.log" | cut -f1) - 120))
  READ_END=$(($(tail -n 1 "$d/drive.log" | cut -f1) + 120))
  export READ_START READ_END
  RAVEL_S3_BUCKET="$READ_BUCKET" NO_COLOR=1 spawn "$d/q033" "$V033_SERVER" --mode query --store s3 \
    --tenant-hash-key-file "$RUN/tenant.key" --dev-insecure-tenant-header \
    --listen-http "127.0.0.1:$((PORT + 25))" --listen-grpc "127.0.0.1:$((PORT + 26))" \
    --cache-dir "$d/q033-cache"
  wait_ready "http://127.0.0.1:$((PORT + 25))" || die "v0.33.0 query server not ready (see $d/q033.log)"
  run_reads $((PORT + 25)) "$d/v033"
  stop_pid "$d/q033.pid"
  RAVEL_S3_BUCKET="$READ_BUCKET" NO_COLOR=1 spawn "$d/qmain" "$MAIN" --mode query --store s3 \
    --tenant-hash-key-file "$RUN/tenant.key" --dev-insecure-tenant-header \
    --listen-http "127.0.0.1:$((PORT + 27))" --listen-grpc "127.0.0.1:$((PORT + 28))" \
    --cache-dir "$d/qmain-cache"
  wait_ready "http://127.0.0.1:$((PORT + 27))" || die "main query server not ready (see $d/qmain.log)"
  run_reads $((PORT + 27)) "$d/main"
  stop_pid "$d/qmain.pid"
  ls_bucket "$READ_BUCKET" "$d/after-reads.list"
  drill_written_keys "$d/written.list" "$d/after-reads.list" > "$d/written-by-reads"
  for q in "${READ_QUERIES[@]}"; do
    name=${q%%|*}
    if ! grep -qx 'code 200' "$d/v033/$name"; then
      echo "read $name FAIL v0.33.0 answered $(head -n 1 "$d/v033/$name")"
      fail=1
    elif cmp -s "$d/v033/$name" "$d/main/$name"; then
      n=$(grep -c . "$d/v033/$name")
      echo "read $name same lines=$n"
    else
      echo "read $name DIFF"
      diff "$d/v033/$name" "$d/main/$name" | head -n 20 || true
      fail=1
    fi
  done | tee "$d/verdict"
  grep -q 'FAIL\|DIFF' "$d/verdict" && fail=1
  # Row counts against the driver's accepted requests: one row per request,
  # plus at most one per kind still in flight when the driver was stopped.
  local acc_m acc_l acc_s
  acc_m=$(awk -F'\t' '($2 == "otlp-metrics" && $3 == 200) || ($2 == "remote-write" && $3 == 204)' "$d/drive.log" | grep -c . || true)
  acc_l=$(awk -F'\t' '$2 == "otlp-logs" && $3 == 200' "$d/drive.log" | grep -c . || true)
  acc_s=$(awk -F'\t' '$2 == "otlp-traces" && $3 == 200' "$d/drive.log" | grep -c . || true)
  for name in samples logs spans; do
    echo "figure read_rows_$name $(python3 -c 'import json,sys; t=open(sys.argv[1]).read().split("\n",1)[1]; print(json.loads(t)["data"]["rows"][0][0])' "$d/v033/sql_${name}_count")" >> "$d/report"
  done
  {
    drill_check_figure "$d/report" read_rows_samples "$acc_m" $((acc_m + 2)) || echo "verdict read FAIL samples"
    drill_check_figure "$d/report" read_rows_logs "$acc_l" $((acc_l + 1)) || echo "verdict read FAIL logs"
    drill_check_figure "$d/report" read_rows_spans "$acc_s" $((acc_s + 1)) || echo "verdict read FAIL spans"
  } | tee -a "$d/verdict"
  [ "$acc_l" -ge 1 ] || fail=1
  grep -q 'FAIL' "$d/verdict" && fail=1
  [ "$fail" -eq 0 ] && echo "verdict read pass" | tee -a "$d/verdict"
  [ "$fail" -eq 0 ] || echo "verdict read FAIL" | tee -a "$d/verdict"
}

phase_verdict() {
  local fail=0 f
  for f in "$RUN/warm/pc.out" "$RUN/cold/pc.out" "$RUN/read/pc.out"; do
    if [ ! -s "$f" ] || grep -q band-miss "$f"; then
      echo "verdict positive-control FAIL $f"
      fail=1
    fi
  done
  if [ "$(cat "$RUN/warm/status" 2> /dev/null)" = "NOT RUN" ]; then
    echo "verdict warm NOT RUN ($(grep ingest_utc "$RUN/warm/horizon"))"
  fi
  for f in "$RUN/warm/verdict" "$RUN/cold/restart/verdict" "$RUN/read/verdict"; do
    if [ ! -s "$f" ]; then
      echo "verdict missing $f"
      fail=1
    elif grep -q '^verdict .* FAIL' "$f"; then
      grep '^verdict' "$f"
      fail=1
    else
      grep '^verdict' "$f"
    fi
  done
  [ "$fail" -eq 0 ] && echo "DRILL PASS"
  [ "$fail" -eq 0 ] || { echo "DRILL FAIL"; return 1; }
}

case "${1:-}" in
  preflight) phase_preflight ;;
  read) phase_read ;;
  verdict) phase_verdict ;;
  start) phase_start ;;
  warm-start) phase_warm_start ;;
  warm-fence) phase_warm_fence ;;
  warm-finish) phase_warm_finish ;;
  cold) phase_cold ;;
  drive) drive "$2" "$3" ;;
  stop)
    for f in "$RUN"/*/drive.pid "$RUN"/*/*/drive.pid "$RUN"/*/server.pid "$RUN"/*/*/server.pid \
      "$RUN"/*/maintain.pid "$RUN"/*/*/maintain.pid "$RUN"/rustfs.pid; do
      [ -f "$f" ] && stop_pid "$f"
    done
    ;;
  *)
    sed -n '2,/^set -euo/p' "$0" | sed '$d'
    exit 64
    ;;
esac
