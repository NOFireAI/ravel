#!/bin/bash
# Runs on the box launch.sh started: build mcp_walk_expiry at the pinned
# commit, run its production profile against the store bucket, and upload the
# results to s3://$BUCKET/$PREFIX. Any failure uploads a FAILED marker naming
# the stage, then the box shuts down (and so terminates). Success uploads a
# DONE marker instead. The 10-hour cap armed in user data stands either way.
set -uo pipefail
export HOME=/root PATH=/snap/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
. /we/env
log=/we/box.log
exec >>"$log" 2>&1
stage() { echo "=== $(date -u +%FT%TZ) $*"; }
put() { aws s3 cp --only-show-errors "$1" "s3://${BUCKET}/${PREFIX}/$2" || echo "upload of $1 failed"; }
fail() {
  echo "stage=$1 exit=$2 at=$(date -u +%FT%TZ)" >/we/FAILED
  [ -d /we/out ] && aws s3 cp --only-show-errors --recursive /we/out "s3://${BUCKET}/${PREFIX}/out/"
  put "$log" box.log
  put /we/FAILED FAILED
  shutdown -h +1
  exit "$2"
}

stage prereqs
export DEBIAN_FRONTEND=noninteractive
apt-get update -q && apt-get install -y -q build-essential pkg-config libssl-dev cmake clang protobuf-compiler git curl || fail prereqs 10
stage source
cd /we && aws s3 cp --only-show-errors "s3://${BUCKET}/${PREFIX}/src.tar.gz" src.tar.gz || fail source 11
echo "${SRC_SHA256}  src.tar.gz" | sha256sum -c - || fail source 12
mkdir -p /we/src && tar -xzf src.tar.gz -C /we/src || fail source 13
cd /we/src
stage toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none --profile minimal || fail toolchain 14
. "$HOME/.cargo/env"
rustup toolchain install || fail toolchain 15
stage build
CARGO_INCREMENTAL=0 cargo build --release --locked -p ravel-bench --features walk-expiry --bin mcp_walk_expiry || fail build 16
# The binary's path names the commit it was built from; the run's
# binary-path stamp refuses any other.
bin_dir="/we/bin/${COMMIT}"
mkdir -p "$bin_dir" && cp target/release/mcp_walk_expiry "$bin_dir/" || fail build 17
put "$log" box.log

# The box log every 10 minutes, so a long run can be watched.
( while true; do sleep 600; put "$log" box.log; done ) &
live=$!

stage run
code=0
RAVEL_S3_BUCKET="$STORE_BUCKET" RAVEL_S3_REGION="$AWS_DEFAULT_REGION" RAVEL_S3_AUTH=instance-role \
  RAVEL_S3_FORCE_PATH_STYLE=false \
  "$bin_dir/mcp_walk_expiry" --profile production --store s3 --out-dir /we/out || code=$?
kill "$live" 2>/dev/null
stage "run exit ${code}"
[ "$code" -eq 0 ] || fail run "$code"
aws s3 cp --only-show-errors --recursive /we/out "s3://${BUCKET}/${PREFIX}/out/" || fail upload 18
echo "exit=0 at=$(date -u +%FT%TZ)" >/we/DONE
put "$log" box.log
put /we/DONE DONE
shutdown -h +1
