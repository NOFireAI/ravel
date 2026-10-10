#!/bin/bash
# One measured pass: start the arm's server, run the 43-statement corpus over
# Flight SQL with three runs per statement, stop the server.
# Usage: pass.sh <A|B> <pass-name> [corpus-file]
set -u
ARM=$1
NAME=$2
R=/var/lib/fleet/work/0ea24c8c-1d77-4a47-9dee-9eff96a07ddd
G=$R/.gate-logs
CORPUS=${3:-$R/benchmarks/clickbench/hits.corpus.json}
OUT=$R/investigations/2720-df55/rlog/$NAME
case $ARM in
  A) SERVER=$G/bin/ravel-server-A-6f1cfafb ;;
  B) SERVER=$G/bin/ravel-server-B-e2b2132c ;;
  *) echo "arm must be A or B" >&2; exit 64 ;;
esac
BENCH=$G/bin/sql_latency_bench-A-6f1cfafb
MEMORY_BUDGET=26000000000
GRPC=127.0.0.1:39217

mkdir -p "$OUT" || exit 1
. $G/store/env.sh
export RAVEL_AUDIT_TOKEN_KEY
RAVEL_AUDIT_TOKEN_KEY=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
TOKEN=$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')

if ss -ltn | grep -q "$GRPC "; then echo "refused: $GRPC already in use" >&2; exit 1; fi

{
  echo "arm=$ARM server=$SERVER"
  echo "server_sha256=$(sha256sum "$SERVER" | cut -d' ' -f1)"
  echo "bench_sha256=$(sha256sum "$BENCH" | cut -d' ' -f1)"
  echo "corpus=$CORPUS"
  echo "start_utc=$(date -u +%FT%TZ)"
  echo "uptime=$(uptime)"
  grep -E 'MemTotal|MemAvailable' /proc/meminfo
  echo "--- top cpu consumers before start"
  ps -eo pid,pcpu,rss,etime,comm --sort=-pcpu | head -8
} > "$OUT/pass-meta.txt"

nohup "$SERVER" --mode query --store s3 --tenant-hash-unkeyed \
  --listen-grpc $GRPC --listen-http 127.0.0.1:39218 --listen-health 127.0.0.1:39219 \
  --tenant-token "$TOKEN=clickbench" --shards 4 \
  --memory-budget-bytes $MEMORY_BUDGET > "$OUT/server.log" 2>&1 &
SPID=$!
echo "$SPID" > "$OUT/server.pid"
up=0
for _ in $(seq 1 120); do
  if ss -ltn | grep -q "$GRPC "; then up=1; break; fi
  if ! kill -0 $SPID 2>/dev/null; then break; fi
  sleep 1
done
if [ $up -ne 1 ]; then echo "refused: server did not listen" >&2; tail -20 "$OUT/server.log" >&2; kill $SPID 2>/dev/null; exit 1; fi

python3 $R/investigations/2720-df55/rlog/sampler.py $SPID "$OUT/samples.tsv" &
SAMPLER=$!

grep 'performance default resolved' "$OUT/server.log" > "$OUT/server-resolved.txt"
FETCH=$(grep 'setting="fetch_concurrency"' "$OUT/server-resolved.txt" | sed -E 's/.*value=([0-9]+).*/\1/')
QBYTES=$(grep 'setting="sql_max_query_bytes"' "$OUT/server-resolved.txt" | sed -E 's/.*value=([0-9]+).*/\1/')
if [ -z "$FETCH" ] || [ -z "$QBYTES" ]; then
  echo "refused: resolved settings missing from server log" >&2; kill $SPID $SAMPLER; exit 1
fi

echo "bench_start_utc=$(date -u +%FT%TZ)" >> "$OUT/pass-meta.txt"
code=0
RAVEL_FLIGHT_TOKEN=$TOKEN "$BENCH" \
  --tenant clickbench --store s3 --flight $GRPC \
  --corpus "$CORPUS" \
  --runs 3 --compaction pre --window-hours 200000 \
  --fetch-concurrency "$FETCH" --sql-max-query-bytes "$QBYTES" \
  --continue-on-error --progress-jsonl "$OUT/progress.jsonl" \
  > "$OUT/bench.stdout" 2> "$OUT/bench.stderr" || code=$?
echo "bench_exit=$code" >> "$OUT/pass-meta.txt"
echo "bench_end_utc=$(date -u +%FT%TZ)" >> "$OUT/pass-meta.txt"

kill -TERM $SPID
for _ in $(seq 1 60); do kill -0 $SPID 2>/dev/null || break; sleep 1; done
if kill -0 $SPID 2>/dev/null; then echo "server did not exit on TERM, killing" >> "$OUT/pass-meta.txt"; kill -KILL $SPID; fi
wait $SAMPLER 2>/dev/null
echo "server_stopped_utc=$(date -u +%FT%TZ)" >> "$OUT/pass-meta.txt"
echo "pass $NAME arm $ARM bench_exit=$code"
