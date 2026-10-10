#!/usr/bin/env bash
# One measured pass: start the arm's server on a fresh process, sample its
# VmRSS every second, run clickbench_parquet_bench (serial cold/hot tries plus
# the concurrency phase), stop the server and keep the evidence.
#
# usage: run-pass.sh <A|B> <label> [statement-filter-unused]
set -euo pipefail

arm="$1"
label="$2"
root=/var/lib/fleet/work/48495dd1-2713-4de9-b90e-05bcc029e5dc
work="$root/.gate-logs/df55"
here="$root/investigations/2720-df55/parquet"
case "$arm" in
  A) server="$work/bin/ravel-server-A-6f1cfaf" ;;
  B) server="$work/bin/ravel-server-B-e2b2132" ;;
  *) echo "arm must be A or B" >&2; exit 64 ;;
esac
bench="$work/bin/clickbench_parquet_bench-A-6f1cfaf"
conc_s="${CONC_S:-300}"
port=44318
out="$here/passes/$label"
raw="$work/runs/$label"
mkdir -p "$out" "$raw"

if curl -s -o /dev/null "http://127.0.0.1:$port/"; then
  echo "refused: something already listens on $port" >&2
  exit 1
fi

# Wait up to LOAD_WAIT_S (default 600) for the 1-minute load to fall below 2.
waited=0
while awk '{exit !($1 >= 2)}' /proc/loadavg && (( waited < ${LOAD_WAIT_S:-600} )); do
  sleep 10; waited=$((waited + 10))
done
echo "waited ${waited} s for load < 2" >"$out/load-wait.txt"
{ date -u +%FT%TZ; uptime; } >"$out/uptime-before.txt"
cat "$out/uptime-before.txt"
ps -eo pid,pcpu,rss,etime,comm --sort=-pcpu | head -8 >"$out/top-before.txt"

token="$(openssl rand -hex 16)"
printf '%s=clickbench;ddl\n' "$token" >"$raw/tokens"
chmod 600 "$raw/tokens"
audit_key="$(openssl rand -hex 32)"
ak="$(cat "$work/rustfs.ak")"
sk="$(cat "$work/rustfs.sk")"

date +%s.%N >"$out/t-server-start"
RAVEL_AUDIT_TOKEN_KEY="$audit_key" nohup "$server" \
  --store s3 --s3-endpoint http://127.0.0.1:39000 --s3-bucket ravel \
  --s3-region us-east-1 --s3-access-key "$ak" --s3-secret-key "$sk" \
  --tenant-hash-unkeyed --parquet-profiles "$work/profiles.json" \
  --tenant-token-file "$raw/tokens" \
  --listen-http "127.0.0.1:$port" --listen-grpc 127.0.0.1:44317 \
  >"$raw/server.log" 2>&1 &
pid=$!
echo "$pid" >"$raw/server.pid"
nohup python3 "$here/sampler.py" "$pid" "$out/vmrss.tsv" 0ea24c8c-1d77-4a47-9dee-9eff96a07ddd 127.0.0.1:39200 >/dev/null 2>&1 &

ready=0
for _ in $(seq 1 180); do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "server exited during startup" >&2
    tail -20 "$raw/server.log" >&2
    exit 1
  fi
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/readyz" || true)"
  if [[ "$code" == 200 ]]; then ready=1; break; fi
  sleep 1
done
[[ $ready == 1 ]] || { echo "server not ready after 180 s" >&2; kill "$pid"; exit 1; }

sed 's/\x1b\[[0-9;]*m//g' "$raw/server.log" | grep ' performance default resolved ' >"$out/server-settings.txt"
setting() {
  grep -o "setting=\"$1\" value=[0-9]*" "$out/server-settings.txt" | sed 's/.*value=//'
}
qmax="$(setting sql_max_query_bytes)"
tmax="$(setting sql_tenant_max_bytes)"
[[ "$qmax" =~ ^[0-9]+$ && "$tmax" =~ ^[0-9]+$ ]] || { echo "settings unreadable: q=$qmax t=$tmax" >&2; kill "$pid"; exit 1; }
sed "s/^memory_cap_bytes = .*/memory_cap_bytes = $qmax/" "$here/prereg-2720.toml" >"$raw/prereg.toml"

bench_rc=0
RAVEL_BENCH_TOKEN="$token" "$bench" --server "http://127.0.0.1:$port" \
  --token-env RAVEL_BENCH_TOKEN --arm a --location s3://clickbench-parquet/hits/ \
  --reference "$work/ref" --prereg "$raw/prereg.toml" --server-log "$raw/server.log" \
  --out "$out/bench-report.json" --concurrency-seconds "$conc_s" \
  --sql-max-query-bytes "$qmax" --sql-tenant-max-bytes "$tmax" \
  >"$out/bench.log" 2>&1 || bench_rc=$?
date +%s.%N >"$out/t-bench-end"
echo "$bench_rc" >"$out/bench-exit"

kill -TERM "$pid"
for _ in $(seq 1 120); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
if kill -0 "$pid" 2>/dev/null; then echo "server did not exit in 120 s; SIGKILL" | tee -a "$out/notes.txt"; kill -KILL "$pid"; fi

sed 's/\x1b\[[0-9;]*m//g' "$raw/server.log" | grep 'sql query error redacted' \
  | sed -E 's/tenant=[0-9a-f]+/tenant=<hash>/' >"$out/server-errors.txt" || true
sed 's/\x1b\[[0-9;]*m//g' "$raw/server.log" | grep -E 'shutdown complete|listening|allocator' \
  | head -20 >"$out/server-stamps.txt" || true
{ date -u +%FT%TZ; uptime; } >"$out/uptime-after.txt"
echo "pass $label arm $arm bench_rc=$bench_rc"
