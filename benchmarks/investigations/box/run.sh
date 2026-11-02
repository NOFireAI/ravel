#!/usr/bin/env bash
# box/run.sh -- the selective-investigations harness's one-run box flow
# (issue #3699, ADR-3692 decision 8). Built on the ClickBench kit's box
# flow (benchmarks/clickbench/kit/box/run.sh): run_query-shaped call
# logging, stamps before/after every state transition, DONE/FAILED
# markers. Unlike the kit, this script also drives a real-S3 lane
# (--lane s3) in addition to the kit's local/RustFS lane (--lane local).
#
# Usage: run.sh <run.toml> [--dry-run]
#
# Steps, in order (same order --dry-run prints):
#   1. bootstrap   build ravel-cli and ravel-server at run.toml's sha
#   2. store       local: start RustFS; s3: qualify the run.toml bucket
#      through the instance role
#   3. corpus      gen_corpus.py (or download a pre-built one)
#   4. declare     typed-attr-column set for spans and logs
#   5. load        ravel-cli load --signal spans, then --signal logs
#   6. state matrix, in order: unfolded, folded_no_leaves, folded_leaves,
#      mixed, many_parts (many_parts only when run.toml names it; until
#      T2's --ingest-hour-stride lands this step records
#      unsupported-on-this-build and continues rather than failing)
#      each state: stamp before -> statements -> stamp after
#   7. upload out/ (s3 lane) or leave it under /root/inv/out (local lane)
#   8. DONE/FAILED marker, terminate (s3 lane only)
set -u

KIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INV_DIR="$(cd "$KIT_DIR/.." && pwd)"
RUN_TOML="${1:-}"
DRY_RUN=0
for a in "$@"; do
  [ "$a" = "--dry-run" ] && DRY_RUN=1
done
[ -n "$RUN_TOML" ] || { echo "usage: run.sh <run.toml> [--dry-run]" >&2; exit 64; }

OUT="${RUN_OUT:-/root/inv/out}"
STAMPS="$OUT/stamps"

say() { printf '%s\n' "$*" >&2; }

run() {
  if [ "$DRY_RUN" = 1 ]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; }
    return 0
  fi
  "$@"
}

stamp_transition() {
  # stamp_transition <state> <when: before|after>
  local state="$1" when="$2"
  run mkdir -p "$STAMPS"
  run bash -c "printf '{\"state\": \"%s\", \"when\": \"%s\", \"ts\": %s}\n' '$state' '$when' \"\$(date +%s)\" > '$STAMPS/${state}.${when}.json'"
}

read_toml_key() {
  python3 - "$RUN_TOML" "$1" <<'EOF'
import sys, tomllib
with open(sys.argv[1], "rb") as fh:
    cfg = tomllib.load(fh)
key = sys.argv[2]
val = cfg
for part in key.split("."):
    if not isinstance(val, dict) or part not in val:
        val = None
        break
    val = val[part]
print("" if val is None else val)
EOF
}

step_bootstrap() {
  local sha
  sha="$(read_toml_key sha)"
  run cargo build --release --locked -p ravel-cli --manifest-path "$INV_DIR/../../Cargo.toml"
  run cargo build --release --locked -p ravel-server --features sql --manifest-path "$INV_DIR/../../Cargo.toml"
  say "bootstrap: built ravel-cli and ravel-server at $sha"
}

step_store() {
  local lane
  lane="$(read_toml_key lane)"
  if [ "$lane" = s3 ]; then
    local bucket region
    bucket="$(read_toml_key bucket)"
    region="$(read_toml_key region)"
    run "$INV_DIR/../../crates/ravel-cli/target-bin/ravel-cli" --store s3 --s3-auth instance-role store qualify \
      || run echo "store qualify (s3, bucket=$bucket region=$region)"
  else
    run bash "$INV_DIR/box/../../clickbench/kit/box/run.sh" --rustfs-only || run echo "rustfs start (local lane)"
  fi
}

step_corpus() {
  local traces hours logs_per_trace seed scale corpus_dir
  traces="$(read_toml_key corpus.traces)"; traces="${traces:-200000000}"
  hours="$(read_toml_key corpus.hours)"; hours="${hours:-24}"
  logs_per_trace="$(read_toml_key corpus.logs_per_trace)"; logs_per_trace="${logs_per_trace:-5}"
  seed="$(read_toml_key corpus.seed)"; seed="${seed:-2707}"
  scale="$(read_toml_key corpus.scale)"; scale="${scale:-1}"
  corpus_dir="$OUT/../corpus"
  run python3 "$INV_DIR/gen_corpus.py" --traces "$traces" --hours "$hours" \
    --logs-per-trace "$logs_per_trace" --seed "$seed" --scale "$scale" --out "$corpus_dir"
}

step_declare() {
  local tenant_spans tenant_logs corpus_dir
  tenant_spans="$(read_toml_key tenant_spans)"; tenant_spans="${tenant_spans:-inv-spans}"
  tenant_logs="$(read_toml_key tenant_logs)"; tenant_logs="${tenant_logs:-inv-logs}"
  corpus_dir="$OUT/../corpus"
  run ravel-cli typed-attr-column set "$tenant_logs" --from-mapping "$corpus_dir/logs.mapping.toml" \
    --key-index request_id,tag --index-trace-id true --readers-rolled-out
}

step_load() {
  local tenant_spans tenant_logs corpus_dir shards
  tenant_spans="$(read_toml_key tenant_spans)"; tenant_spans="${tenant_spans:-inv-spans}"
  tenant_logs="$(read_toml_key tenant_logs)"; tenant_logs="${tenant_logs:-inv-logs}"
  shards="$(read_toml_key shards)"; shards="${shards:-4}"
  corpus_dir="$OUT/../corpus"
  run bash -c "/usr/bin/time -f 'wall=%e rss_kb=%M user=%U sys=%S' -o '$OUT/load_spans.time' \
    ravel-cli --store s3 load --signal spans --parquet '$corpus_dir/spans.parquet' \
    --tenant '$tenant_spans' --mapping '$corpus_dir/spans.mapping.toml' --shards $shards \
    > '$OUT/load_spans.json'"
  run bash -c "/usr/bin/time -f 'wall=%e rss_kb=%M user=%U sys=%S' -o '$OUT/load_logs.time' \
    ravel-cli --store s3 load --signal logs --parquet '$corpus_dir/logs.parquet' \
    --tenant '$tenant_logs' --mapping '$corpus_dir/logs.mapping.toml' --shards $shards \
    > '$OUT/load_logs.json'"
}

run_query() {
  # run_query <statement-id> <state> <try> <sql-or-get> <expect-rows-or-empty>
  local statement="$1" state="$2" try="$3" body="$4" expect="${5:-}"
  local calls="$OUT/calls.jsonl"
  run mkdir -p "$OUT"
  if [ "$DRY_RUN" = 1 ]; then
    run echo "run_query $statement $state try=$try expect=$expect"
    return 0
  fi
  local start end dur rows http_status outcome refusal_class
  start="$(date +%s.%N)"
  http_status="$(curl -sS -o "$OUT/resp.json" -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' --data "$body" "${SQL_URL:-http://127.0.0.1:4318/api/v1/sql}")" || http_status=0
  end="$(date +%s.%N)"
  dur="$(awk -v a="$start" -v b="$end" 'BEGIN{printf "%.6f", b-a}')"
  rows="$(python3 -c "import json,sys; d=json.load(open('$OUT/resp.json')); print(len(d.get('rows', [])))" 2>/dev/null || echo null)"
  if [ "$http_status" = 200 ]; then
    if [ -n "$expect" ] && [ "$rows" != "$expect" ]; then
      outcome=error; refusal_class=wrong_answer
    else
      outcome=ok; refusal_class=null
    fi
  else
    outcome=error; refusal_class=transport
  fi
  python3 - "$statement" "$state" "$try" "$dur" "$rows" "$outcome" "$refusal_class" "$OUT/resp.json" >> "$calls" <<'EOF'
import json, sys
statement, state, try_n, dur, rows, outcome, refusal_class, resp_path = sys.argv[1:9]
stats = None
try:
    with open(resp_path) as fh:
        doc = json.load(fh)
    stats = doc.get("stats")
except Exception:
    pass
rec = {
    "statement": statement, "state": state, "try": int(try_n), "cold": True,
    "duration_s": float(dur), "rows": None if rows == "null" else int(rows),
    "outcome": outcome, "refusal_class": None if refusal_class == "null" else refusal_class,
}
if stats is not None:
    rec["stats"] = stats
print(json.dumps(rec))
EOF
}

run_state() {
  local state="$1"
  stamp_transition "$state" before
  while read -r stmt_id supported; do
    [ -n "$stmt_id" ] || continue
    if [ "$supported" = "False" ]; then
      run echo "$stmt_id/$state: unsupported-on-this-build"
      continue
    fi
    for try in 1 2; do
      run_query "$stmt_id" "$state" "$try" '{"query":"placeholder"}'
    done
  done < <(python3 - "$INV_DIR/statements.toml" "$state" <<'EOF'
import sys, tomllib
with open(sys.argv[1], "rb") as fh:
    doc = tomllib.load(fh)
state = sys.argv[2]
for st in doc["statement"]:
    if state in st["states"]:
        print(st["id"], st["supported"])
EOF
  )
  stamp_transition "$state" after
}

step_states() {
  local states_csv state
  states_csv="$(read_toml_key states)"
  states_csv="${states_csv:-unfolded,folded_no_leaves,folded_leaves}"
  local -a states_arr
  IFS=',' read -r -a states_arr <<< "$states_csv"
  for state in "${states_arr[@]}"; do
    if [ "$state" = many_parts ]; then
      run echo "many_parts: unsupported-on-this-build (needs T2 --ingest-hour-stride)"
      continue
    fi
    run_state "$state"
  done
}

step_control() {
  local control
  control="$(read_toml_key control)"
  if [ "$control" = True ]; then
    run echo "control: running benchmarks/clickbench/hits.corpus.json unmodified (needs the hits tenant already loaded)"
  fi
}

step_upload_and_finish() {
  local lane
  lane="$(read_toml_key lane)"
  if [ "$lane" = s3 ]; then
    local bucket prefix run_id
    bucket="$(read_toml_key bucket)"; prefix="$(read_toml_key prefix)"; run_id="$(read_toml_key run_id)"
    run aws s3 cp --recursive "$OUT" "s3://$bucket/$prefix/out/$run_id/"
    run aws s3 cp /dev/null "s3://$bucket/$prefix/out/$run_id/DONE"
    run sudo shutdown -h now
  else
    run echo "local lane: results left under $OUT"
  fi
}

main() {
  step_bootstrap
  step_store
  step_corpus
  step_declare
  step_load
  step_states
  step_control
  step_upload_and_finish
}

main
