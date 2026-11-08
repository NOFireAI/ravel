#!/bin/bash
# Runs on the box launch.sh started: build the harness at the pinned commit,
# run a `ci` smoke, then the measured profile with run.sh, uploading the box
# log, status lines and outputs to s3://$BUCKET/$PREFIX as it goes.
# REAL_S3_PREFIX empty: the portable s3s-fs lane. Set: the real-S3 lane.
# STAGE1=1: both run.sh invocations run with MB_STAGE1=1.
set -uo pipefail
export HOME=/root PATH=/snap/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
. /mb/env
export MB_STAGE1="${STAGE1:-}"
log=/mb/box.log
exec >>"$log" 2>&1
stage() { echo "=== $(date -u +%FT%TZ) $*"; }
up() { aws s3 cp --only-show-errors --recursive "$1" "s3://${BUCKET}/${PREFIX}/$2/" || echo "upload of $1 failed"; }
put() { aws s3 cp --only-show-errors "$1" "s3://${BUCKET}/${PREFIX}/$(basename "$1")" || echo "upload of $1 failed"; }
status_line() { echo "$1 exit=$2" >>/mb/status.txt; put /mb/status.txt; put "$log"; }
finish() { status_line "DONE" "$1"; shutdown -h "+${2:-2}"; exit "$1"; }

stage prereqs
export DEBIAN_FRONTEND=noninteractive
apt-get update -q && apt-get install -y -q docker.io docker-compose-v2 python3 build-essential pkg-config libssl-dev cmake clang protobuf-compiler git curl || finish 10
systemctl enable --now docker || finish 11
docker compose version
stage source
cd /mb && aws s3 cp --only-show-errors "s3://${BUCKET}/${PREFIX}/src.tar.gz" src.tar.gz || finish 12
echo "${SRC_SHA256}  src.tar.gz" | sha256sum -c - || finish 13
mkdir -p /mb/src && tar -xzf src.tar.gz -C /mb/src || finish 14
cd /mb/src
stage toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none --profile minimal || finish 15
. "$HOME/.cargo/env"
rustup toolchain install || finish 16
stage build
CARGO_INCREMENTAL=0 cargo build --release --locked -p ravel-bench \
  --bin metricsbench_ingest --bin metricsbench_query --bin metricsbench_report \
  --bin metricsbench_holders || finish 17
status_line build 0
stage "pull image"
docker pull "$RAVEL_IMAGE" || finish 18

store_args=()
ci_args=()
if [ -n "$REAL_S3_PREFIX" ]; then
  region=${AWS_DEFAULT_REGION:-}
  # An unset region ends the run through finish, which uploads the log and
  # schedules the shutdown; a bare ${var:?} would exit here and leave the box
  # idle until the launch-time cap.
  [ -n "$region" ] || { echo "/mb/env names no region"; finish 19 2; }
  store_args=(--real-s3-bucket-prefix "$REAL_S3_PREFIX" --real-s3-region "$region")
  ci_args=(--real-s3-bucket-prefix "${REAL_S3_PREFIX}-ci" --real-s3-region "$region")
fi
stage "ci smoke"
code=0
deploy/metricsbench/run.sh --profile ci --ravel-image "$RAVEL_IMAGE" --ravel-source-commit "$RAVEL_SOURCE_COMMIT" \
  --bin-dir target/release --out-dir /mb/out-ci ${ci_args[@]+"${ci_args[@]}"} || code=$?
up /mb/out-ci out-ci
status_line ci "$code"
[ "$code" -eq 0 ] || finish 20 30

# Store-request and memory counters once a minute, container CPU and memory
# every 30 s, and the live reports every 10 minutes.
( while true; do
    ts=$(date -u +%FT%TZ)
    curl -s -m 5 http://127.0.0.1:4318/metrics | grep -E '^(ravel_store_|ravel_memory_gate_|ravel_memory_budget_refusals|ravel_process_allocator_bytes|ravel_cache_(resident_bytes|misses_total|hits_total))' | sed "s/^/${ts} ravel /"
    curl -s -m 5 http://127.0.0.1:9009/metrics | grep -E '^thanos_objstore_bucket_(operations_total|operation_failures_total)|^cortex_ingester_shipper_uploads_total' | sed "s/^/${ts} mimir /"
    sleep 60
  done ) >/mb/store-metrics.log 2>&1 &
met=$!
( while true; do echo "$(date -u +%T) $(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' | grep -v createbuckets | tr '\n' ' ')"; sleep 30; done ) >/mb/cpu.log 2>&1 &
cpu=$!
( while true; do
    sleep 600
    put "$log"; put /mb/cpu.log
    for f in stamps.json ingest.json query.log; do
      [ -e "/mb/out-run/$f" ] && aws s3 cp --only-show-errors "/mb/out-run/$f" "s3://${BUCKET}/${PREFIX}/live/$f"
    done
  done ) &
live=$!

stage "$PROFILE"
status_line "${PROFILE}-start" 0
code=0
deploy/metricsbench/run.sh --profile "$PROFILE" --ravel-image "$RAVEL_IMAGE" --ravel-source-commit "$RAVEL_SOURCE_COMMIT" \
  --bin-dir target/release --out-dir /mb/out-run --anchor-ms "$ANCHOR_MS" \
  --rss-band "$RSS_BAND" --ravel-objects-band "$OBJECTS_BAND" ${store_args[@]+"${store_args[@]}"} || code=$?
kill "$met" "$cpu" "$live"
stage "$PROFILE exit ${code}"
up /mb/out-run out-run
gzip -k /mb/store-metrics.log; put /mb/store-metrics.log.gz; put /mb/cpu.log
if [ -n "$REAL_S3_PREFIX" ]; then
  for s in ravel mimir-blocks mimir-ruler mimir-alertmanager; do
    echo "${PROFILE} ${s} $(aws s3 ls --recursive --summarize "s3://${REAL_S3_PREFIX}-${s}/" | tail -2 | tr -s ' ' | tr '\n' ' ')" >>/mb/buckets.txt
  done
  put /mb/buckets.txt
fi
status_line "$PROFILE" "$code"
finish "$code" 5
