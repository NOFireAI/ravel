#!/usr/bin/env bash
# Cases for the ClickBench kit: launch.sh, the user-data it writes, box/run.sh
# and fetch.sh. Nothing here reaches AWS or builds anything. Stubs first on
# PATH stand in for `aws` (S3 is a local directory, $FAKE_S3), `curl` (the
# instance metadata service and the SQL endpoint), `perf`, `shutdown` and
# `nohup`; a
# box is a local directory standing in for /root (KIT_ROOT), with stub
# ravel-cli and ravel-server binaries. Each case names the mutation it
# catches.
#
# KIT_TEST_DIR points the cases at another copy of the kit, to show a case
# failing against a broken one.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
KIT_SRC="${KIT_TEST_DIR:-$HERE}"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; fails=$((fails + 1)); }

STUBS="$TMP/stubs"
BINSTUBS="$TMP/binstubs"
mkdir -p "$STUBS" "$BINSTUBS"
export FAKE_S3="$TMP/s3"
export STUB_DIR="$TMP/stubstate"
export STUB_LOG="$STUB_DIR/calls.log"
mkdir -p "$FAKE_S3" "$STUB_DIR"

cat >"$STUBS/aws" <<'STUB'
#!/usr/bin/env bash
# S3 is $FAKE_S3/<bucket>/<key>. STUB_S3_DOWN=1 fails every s3 call as an
# unreachable endpoint would (exit 255); STUB_S3_HANG=1 never answers.
# STUB_DENY_ACTION names an IAM action the simulated role lacks; the role
# may only get in/* and put out/* under the kit-test-bucket's clickbench-kit/
# runs. STUB_NO_PROFILE=1 makes the instance profile unreadable.
# STUB_S3_DENY_KEY makes a download of any key containing it a 403, and
# STUB_S3_NO_BUCKET=1 makes every download a NoSuchBucket error.
# STUB_FAIL_ARM names the arm whose run-instances fails.
echo "aws $*" >>"$STUB_LOG"
endpoint=""
pos=()
userdata=""
token=""
resource=""
action=""
while [ $# -gt 0 ]; do
  case "$1" in
    --region|--query|--output|--instance-profile-name|--policy-source-arn|--name|--image-ids|--filters|--instance-ids)
      pos+=("$1=$2"); shift 2; continue ;;
    --endpoint-url) endpoint="$2"; shift 2; continue ;;
    --user-data) userdata="$2"; shift 2; continue ;;
    --client-token) token="$2"; shift 2; continue ;;
    --action-names) action="$2"; pos+=("action=$2"); shift 2; continue ;;
    --resource-arns) resource="$2"; shift 2; continue ;;
    --only-show-errors|--recursive|--summarize) shift; continue ;;
  esac
  pos+=("$1")
  shift
done
s3path() { printf '%s\n' "$FAKE_S3/${1#s3://}"; }
case "${pos[0]:-} ${pos[1]:-}" in
  "s3 cp"|"s3 ls")
    if [ -n "$endpoint" ]; then
      # Ravel's own bucket on the box's RustFS: only the object total.
      printf 'Total Objects: %s\n   Total Size: 1\n' "$(cat "$STUB_DIR/objects" 2>/dev/null || echo 10)"
      exit 0
    fi
    [ -z "${STUB_S3_HANG:-}" ] || exec sleep 30
    [ -z "${STUB_S3_DOWN:-}" ] || { echo "Could not connect to the endpoint URL" >&2; exit 255; }
    ;;
esac
case "${pos[0]:-} ${pos[1]:-}" in
  "s3 cp")
    src="${pos[@]: -2:1}"
    dst="${pos[@]: -1}"
    case "$src" in
      s3://*) f="$(s3path "$src")"
        case "$src" in *"${STUB_S3_DENY_KEY:-/none/}"*)
          echo "fatal error: An error occurred (403) when calling the HeadObject operation: Forbidden" >&2; exit 1 ;;
        esac
        [ -z "${STUB_S3_NO_BUCKET:-}" ] || {
          echo "fatal error: An error occurred (NoSuchBucket) when calling the GetObject operation: The specified bucket does not exist" >&2; exit 1; }
        [ -f "$f" ] || { echo "fatal error: An error occurred (404) when calling the HeadObject operation: Key \"$src\" does not exist" >&2; exit 1; }
        mkdir -p "$(dirname "$dst")" && cp "$f" "$dst" ;;
      *) f="$(s3path "$dst")"
        mkdir -p "$(dirname "$f")" && cp "$src" "$f" ;;
    esac
    ;;
  "s3 ls")
    d="$(s3path "${pos[@]: -1}")"
    [ -d "$d" ] && [ -n "$(ls -A "$d")" ] || exit 1
    for f in "$d"/*; do printf '2026-10-11 10:00:00 %10s %s\n' "$(wc -c <"$f" | tr -d ' ')" "$(basename "$f")"; done
    ;;
  "iam get-instance-profile")
    [ -z "${STUB_NO_PROFILE:-}" ] || { echo "An error occurred (NoSuchEntity) when calling the GetInstanceProfile operation" >&2; exit 254; }
    echo "arn:aws:iam::123456789012:role/kit-test-role" ;;
  "iam simulate-principal-policy")
    runs="arn:aws:s3:::kit-test-bucket/clickbench-kit/runs/"
    rest="${resource#"$runs"}"
    if [ "$action" = "${STUB_DENY_ACTION:-none}" ] || [ "$rest" = "$resource" ]; then
      echo implicitDeny
    else
      case "$action $rest" in
        "s3:GetObject "*/in/\*|"s3:PutObject "*/out/\*) echo allowed ;;
        *) echo implicitDeny ;;
      esac
    fi
    ;;
  "ssm get-parameter") echo ami-0123456789abcdef0 ;;
  "ec2 describe-images") echo /dev/xvda ;;
  "ec2 run-instances")
    arm="${token##*-}"
    printf '%s' "$userdata" >"$STUB_DIR/userdata-$arm"
    echo "$token" >>"$STUB_DIR/client-tokens"
    [ "${STUB_FAIL_ARM:-}" != "$arm" ] || { echo "InsufficientInstanceCapacity" >&2; exit 255; }
    case "$arm" in A) echo i-0aaaaaaaaaaaaaaaa ;; *) echo i-0bbbbbbbbbbbbbbbb ;; esac
    ;;
  "ec2 terminate-instances") echo terminating ;;
  "ec2 describe-volumes") printf 'vol-0123456789abcdef0\tgp3\n' ;;
  *) echo "unexpected aws call: ${pos[*]}" >&2; exit 99 ;;
esac
STUB

cat >"$STUBS/curl" <<'STUB'
#!/usr/bin/env bash
# IMDS answers at once, and a rustfs.invalid download is the rustfs-asset file.
# Readiness and SQL answer only while the stub server's
# ready file exists, as a server that is not listening refuses (curl exit 7,
# printing "000 <seconds>" for a -w format as curl does); the stub server
# writes it only after logging its argv and settings, and removes it when
# stopped. A readyz call is logged to readyz-calls. A body named check.json is
# the concurrent phase's SELECT 1 check: it is logged to check-calls, whether
# or not the server is ready, and, while
# the check-refuse file exists, answered 503 (curl exit 22 under
# --fail-with-body); while the check-hang file exists it is never answered, as
# a server that accepts the connection and then hangs: curl exits 28 after its
# --max-time, or never returns without one. STUB_SQL_TIMEOUT_N names a
# statement whose call times out at once (curl exit 28, status 000).
# STUB_SQL_DEAD refuses every measured call as a server that is not
# listening does, while the SELECT 1 check still answers.
# STUB_SQL_TRUNCATE_N names a statement whose answer
# prints status 200 and then fails the transfer (curl exit 18). A SQL call
# logs its request body to
# sql-bodies, writes an answer to -o and prints "<status> <seconds>":
# STUB_SQL_TIME seconds (default 0.5), or the try's field of STUB_SQL_TIMES
# ("cold hot hot"), or for a q29_probe call the try's field of
# STUB_PROBE_TIMES, and a 422 resource_exhausted refusal for statement
# STUB_SQL_REFUSE_N. An answer holds the statement's expected row count from
# the index.tsv beside its body (3 rows where the corpus states none), or
# STUB_SQL_ROWS rows for statement STUB_SQL_ROWS_N (only on try
# STUB_SQL_ROWS_TRY when that is set), with status STUB_SQL_STATUS (default
# 200). The try is read from the -o name,
# <number>-<try>.json. A body under a verify/ directory is the box's row-count
# check: it is logged to verify-bodies and answered with one cell,
# STUB_COUNT_STAR (default the hits row count).
out=""
body=""
url=""
wfmt=""
fwb=""
maxtime=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2; continue ;;
    --max-time) maxtime="$2"; shift 2; continue ;;
    --data-binary) body="${2#@}"; shift 2; continue ;;
    -w) wfmt="$2"; shift 2; continue ;;
    --fail-with-body) fwb=1 ;;
    -H|-X) shift 2; continue ;;
    http*) url="$1" ;;
  esac
  shift
done
case "$url" in
  */latest/api/token) echo stub-imds-token ;;
  */meta-data/instance-id) echo i-0b0x00000000000001 ;;
  */meta-data/instance-type) echo c6a.4xlarge ;;
  https://rustfs.invalid/*) cp "$STUB_DIR/rustfs-asset" "$out" ;;
  */readyz)
    echo readyz >>"$STUB_DIR/readyz-calls"
    [ -f "$STUB_DIR/server-ready" ] || { echo "curl: (7) Failed to connect" >&2; exit 7; }
    echo ready ;;
  */api/v1/sql)
    case "$body" in */check.json) echo check >>"$STUB_DIR/check-calls" ;; esac
    if [ ! -f "$STUB_DIR/server-ready" ]; then
      [ -z "$wfmt" ] || printf '000 0.000412'
      echo "curl: (7) Failed to connect" >&2
      exit 7
    fi
    case "$body" in
      */check.json)
        if [ -f "$STUB_DIR/check-hang" ]; then
          [ -n "$maxtime" ] || exec sleep 3600
          sleep "$maxtime"
          echo "curl: (28) Operation timed out after $maxtime seconds with 0 bytes received" >&2
          exit 28
        fi
        if [ -f "$STUB_DIR/check-refuse" ]; then
          printf '{"status":"error","errorType":"unavailable","error":"memory pressure"}' >"$out"
          [ -z "$fwb" ] || exit 22
          exit 0
        fi
        printf '{"status":"success","data":{"columns":["1"],"rows":[[1]]},"stats":{}}' >"$out"
        exit 0 ;;
      */verify/*)
        { cat "$body"; echo; } >>"$STUB_DIR/verify-bodies"
        printf '{"status":"success","data":{"columns":["count(*)"],"rows":[[%s]]},"stats":{}}' \
          "${STUB_COUNT_STAR:-99997497}" >"$out"
        printf '200 0.1'
        exit 0 ;;
    esac
    if [ -n "${STUB_SQL_DEAD:-}" ]; then
      printf '000 0.000412'
      echo "curl: (7) Failed to connect to 127.0.0.1 port 4318" >&2
      exit 7
    fi
    echo "sql $(basename "$body")" >>"$STUB_DIR/curl-sql"
    n="$(basename "$body" .json)"
    try="${out##*-}"
    try="${try%.json}"
    { cat "$body"; echo; } >>"$STUB_DIR/sql-bodies"
    if [ "$n" = "${STUB_SQL_TIMEOUT_N:-none}" ]; then
      printf '000 0.3'
      echo "curl: (28) Operation timed out" >&2
      exit 28
    elif [ "$n" = "${STUB_SQL_TRUNCATE_N:-none}" ]; then
      printf '{"status":"success","data":{"col' >"$out"
      printf '200 0.2'
      echo "curl: (18) transfer closed with outstanding read data remaining" >&2
      exit 18
    elif [ "$n" = "${STUB_SQL_REFUSE_N:-none}" ]; then
      printf '{"status":"error","errorType":"resource_exhausted","error":"query pool exhausted"}' >"$out"
      printf '422 0.1'
    else
      rows="$(awk -F'\t' -v n="$n" '$1 == n { print $3 }' "$(dirname "$body")/index.tsv")"
      case "$rows" in ''|-) rows=3 ;; esac
      if [ "$n" = "${STUB_SQL_ROWS_N:-none}" ] \
        && { [ -z "${STUB_SQL_ROWS_TRY:-}" ] || [ "$try" = "$STUB_SQL_ROWS_TRY" ]; }; then
        rows="$STUB_SQL_ROWS"
      fi
      json=""
      for ((i = 0; i < rows; i++)); do json="${json:+$json,}[$i]"; done
      json="[$json]"
      printf '{"status":"success","data":{"columns":["c"],"rows":%s},"stats":{}}' "$json" >"$out"
      seconds="${STUB_SQL_TIME:-0.5}"
      [ -z "${STUB_SQL_TIMES:-}" ] || seconds="$(printf '%s\n' $STUB_SQL_TIMES | sed -n "${try}p")"
      case "$out" in
        */q29_probe/*) [ -z "${STUB_PROBE_TIMES:-}" ] || seconds="$(printf '%s\n' $STUB_PROBE_TIMES | sed -n "${try}p")" ;;
      esac
      printf '%s %s' "${STUB_SQL_STATUS:-200}" "$seconds"
    fi
    ;;
  *) echo "unexpected curl: $url" >&2; exit 7 ;;
esac
STUB

cat >"$STUBS/shutdown" <<'STUB'
#!/usr/bin/env bash
echo "shutdown $*" >>"$STUB_LOG"
STUB

cat >"$STUBS/nohup" <<'STUB'
#!/usr/bin/env bash
echo "nohup $*" >>"$STUB_LOG"
STUB

# `perf record` logs its argv, writes its -o file and records until TERM;
# `perf script -i FILE` prints two samples of one stack from that file, or
# fails when STUB_PERF_SCRIPT_FAIL is set.
cat >"$STUBS/perf" <<'STUB'
#!/usr/bin/env bash
echo "perf $*" >>"$STUB_DIR/perf-argv"
case "$1" in
  record)
    out=""
    while [ $# -gt 0 ]; do
      [ "$1" != -o ] || out="$2"
      shift
    done
    echo stub-perf-data >"$out"
    sleep 30 &
    pid=$!
    trap 'kill "$pid" 2>/dev/null; exit 0' TERM
    wait "$pid" ;;
  script)
    [ "$2" = -i ] && [ -f "$3" ] || { echo "perf script: no $3" >&2; exit 1; }
    [ -z "${STUB_PERF_SCRIPT_FAIL:-}" ] || { echo "perf script: stub failure" >&2; exit 1; }
    for t in 1.000000 1.010101; do
      printf 'ravel-server 4242 [000] %s: 10101010 cpu-clock:\n' "$t"
      printf '\t    5555aaaa scan_rows+0x10 (/bin/ravel-server)\n'
      printf '\t    5555bbbb execute (/bin/ravel-server)\n'
      printf '\t    5555cccc main (/bin/ravel-server)\n\n'
    done ;;
esac
STUB

# The box's binaries. The server logs its argv, the five settings stamps and
# the store keys and audit key it was given (the evidence must not carry
# them), removes the check-refuse and check-hang files, then after the seconds in the
# ready-delay file, else STUB_READY_DELAY (default 0.05), and while the
# ready-after-checks file exists until that many more SELECT 1 checks have
# been logged, writes the ready file and sleeps until stopped; the CLI logs
# its argv, and `load` adds 200 keys
# to the store (100 data objects and their 100 commit records) and prints
# ravel-cli's load summary with STUB_LOAD_ROWS rows written (default the hits
# row count) and STUB_LOAD_OBJECTS objects written (default 100; "none"
# leaves the line out). STUB_FOLD=unsealed
# makes the fold seal nothing, STUB_FOLD=twice prints entry_count twice, and
# STUB_FOLD_ENTRIES sets the entry_count it prints (default 100).
cat >"$BINSTUBS/ravel-server" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"$STUB_DIR/server-argv"
for kv in fetch_concurrency=32 store_get_concurrency=32 cache_max_bytes=7689077760 \
  catalog_cache_max_bytes=1537815552 sql_max_query_bytes=16451897344 sql_tenant_max_bytes=32903794688; do
  printf '2026-10-11T10:00:00.000000Z  INFO ravel_server::config: performance default resolved setting="%s" value=%s source="derived"\n' "${kv%%=*}" "${kv#*=}"
done
echo "stub-server: store access_key=${RAVEL_S3_ACCESS_KEY:-} secret_key=${RAVEL_S3_SECRET_KEY:-} audit_key=${RAVEL_AUDIT_TOKEN_KEY:-none}"
rm -f "$STUB_DIR/check-refuse" "$STUB_DIR/check-hang"
sleep "$(cat "$STUB_DIR/ready-delay" 2>/dev/null || echo "${STUB_READY_DELAY:-0.05}")"
if [ -f "$STUB_DIR/ready-after-checks" ]; then
  want=$(($(cat "$STUB_DIR/check-calls" 2>/dev/null | wc -l) + $(cat "$STUB_DIR/ready-after-checks")))
  for ((i = 0; i < 6000; i++)); do
    [ "$(cat "$STUB_DIR/check-calls" 2>/dev/null | wc -l)" -lt "$want" ] || break
    sleep 0.01
  done
fi
sleep 120 &
pid=$!
trap 'rm -f "$STUB_DIR/server-ready"; kill "$pid" 2>/dev/null; exit 0' TERM
touch "$STUB_DIR/server-ready"
wait "$pid"
STUB
cat >"$BINSTUBS/ravel-cli" <<'STUB'
#!/usr/bin/env bash
echo "cli $*" >>"$STUB_DIR/cli-argv"
case " $* " in
  *" load "*)
    echo 210 >"$STUB_DIR/objects"
    printf 'bulk load complete\n  rows_skipped     : 0\n  rows_written     : %s\n  rows/sec         : 1000\n' \
      "${STUB_LOAD_ROWS:-99997497}"
    [ "${STUB_LOAD_OBJECTS:-100}" = none ] || printf '  objects written  : %s\n' "${STUB_LOAD_OBJECTS:-100}" ;;
  *" catalog fold "*)
    if [ "${STUB_FOLD:-}" = unsealed ]; then echo "seal_through_hour: None"; else echo "seal_through_hour: Some(491234)"; fi
    printf 'previous_watermark_hour: Some(491200)\nwatermark_hour: Some(491235)\nentry_count: %s\nbuckets_folded: 5\npart_bytes: 12345\n' "${STUB_FOLD_ENTRIES:-100}"
    [ "${STUB_FOLD:-}" != twice ] || echo "entry_count: 100" ;;
esac
exit 0
STUB
chmod +x "$STUBS"/* "$BINSTUBS"/*
export PATH="$STUBS:$PATH"
# macOS assesses a newly written executable on its first exec, which took
# about 0.25 s here, longer than the 0.05 s a case gives perf record before
# it is sent SIGTERM; a stub killed before its first line records nothing.
# One exec now pays that outside every case.
perf warm >/dev/null 2>&1
rm -f "$STUB_DIR/perf-argv"

# The server's two listeners as the healthz sampler sees them: one HTTP server
# per role (4318 the query listener, 4319 the health listener) on an ephemeral
# port, exported as KIT_HEALTH_BASE_4318 and KIT_HEALTH_BASE_4319. GET /healthz
# answers 200 while the stub server's ready file exists and 503 otherwise;
# while health-hang-<role> exists it answers only after 3 s, past the
# sampler's 2 s timeout, and while health-500-<role> exists it answers 500.
# GET /metrics on the 4318 role carries ravel_health_heartbeat_age_seconds
# (the value in the heartbeat file, default 0.5) unless heartbeat-absent
# exists, and answers only after 3 s while metrics-hang exists; on the 4319
# role it is a 404.
cat >"$TMP/health-stub.py" <<'PY'
import http.server, os, socketserver, sys, threading, time
state = sys.argv[1]

class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    def handle_error(self, request, client_address):
        pass  # a hung answer's client has timed out and gone

def handler(role):
    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass
        def answer(self, code, body):
            data = body.encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def do_GET(self):
            f = lambda name: os.path.join(state, name)
            if self.path == "/healthz":
                if os.path.exists(f("health-hang-" + role)):
                    time.sleep(3)
                if os.path.exists(f("health-500-" + role)):
                    return self.answer(500, "stub failure\n")
                ready = os.path.exists(f("server-ready"))
                return self.answer(200 if ready else 503, "ok\n" if ready else "starting\n")
            if self.path == "/metrics" and role == "4318":
                if os.path.exists(f("metrics-hang")):
                    time.sleep(3)
                body = "# TYPE x gauge\n"
                if not os.path.exists(f("heartbeat-absent")):
                    try:
                        age = open(f("heartbeat")).read().strip()
                    except OSError:
                        age = "0.5"
                    body += 'ravel_health_heartbeat_age_seconds{mode="x"} %s\n' % age
                return self.answer(200, body)
            return self.answer(404, "no such path\n")
    return H

ports = []
for role in ("4318", "4319"):
    s = Server(("127.0.0.1", 0), handler(role))
    ports.append(str(s.server_address[1]))
    threading.Thread(target=s.serve_forever, daemon=True).start()
with open(sys.argv[2] + ".tmp", "w") as out:
    out.write(" ".join(ports) + "\n")
os.replace(sys.argv[2] + ".tmp", sys.argv[2])
while True:
    time.sleep(3600)
PY
python3 "$TMP/health-stub.py" "$STUB_DIR" "$TMP/health-ports" &
HEALTH_STUB_PID=$!
trap 'kill "$HEALTH_STUB_PID" 2>/dev/null; pkill -KILL -f "healthz-sampler $TMP/" 2>/dev/null; rm -rf "$TMP"' EXIT
for _ in $(seq 1 500); do [ -s "$TMP/health-ports" ] && break; sleep 0.01; done
read -r hp_4318 hp_4319 <"$TMP/health-ports" || { echo "FAIL health stub: no ports"; exit 1; }
export KIT_HEALTH_BASE_4318="http://127.0.0.1:$hp_4318" KIT_HEALTH_BASE_4319="http://127.0.0.1:$hp_4319"

# The hardware the box stamps, so no case reads this machine's /proc.
printf 'processor\t: 0\nmodel name\t: Stub CPU 9000 @ 2.00GHz\n' >"$TMP/cpuinfo"
printf 'MemTotal:       32000000 kB\nMemFree:        1000 kB\n' >"$TMP/meminfo"
export KIT_CPUINFO="$TMP/cpuinfo" KIT_MEMINFO="$TMP/meminfo"

SHA_A=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
SHA_B=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb

# A filled copy of the rlog lane's band file: 43 statements at 0.5 s sum to
# 21.5 s hot and cold, under 30; at 2.0 s they miss.
BANDS="$TMP/rlog.toml"
sed -e 's/^hot_sum_s_max = .*/hot_sum_s_max = 30/' \
  -e 's/^cold_sum_s_max = .*/cold_sum_s_max = 30/' \
  -e 's/^error_pooled_max = .*/error_pooled_max = 0.1/' \
  -e 's/^concurrent_qps_min = .*/concurrent_qps_min = 1.0/' \
  -e 's/^concurrent_error_pooled_max = .*/concurrent_error_pooled_max = 0.1/' \
  -e 's/^q29_probe_s_max = .*/q29_probe_s_max = 1.0/' \
  -e 's/^\(q[0-9][0-9]\) = .*/\1 = 1.0/' \
  "$REPO_ROOT/benchmarks/clickbench/bands/rlog.toml" >"$BANDS"
DATASET_SHA=1111111111111111111111111111111111111111111111111111111111111111

# write_run_toml FILE BANDS_PATH [BAND_SECTIONS_ARRAY]
write_run_toml() {
  local sections='["relative", "absolute"]'
  [ -z "${3:-}" ] || sections="$3"
  cat >"$1" <<EOF
sha_a = "$SHA_A"
sha_b = "$SHA_B"
lanes = ["rlog"]
phases = ["benchmark"]
band_sections = $sections
instance_type = "c6a.4xlarge"
region = "us-east-1"
bucket = "kit-test-bucket"
prefix = "clickbench-kit"
instance_profile = "ravel-clickbench-instance-profile"
repo_url = "https://example.invalid/ravel.git"
tenant = "clickbench-v4"
shards = 8
corpus = "benchmarks/clickbench/hits.corpus.json"
dataset_url = "https://example.invalid/hits.parquet"
dataset_sha256 = "$DATASET_SHA"
rows_expected = 99997497
rustfs_url = "https://example.invalid/rustfs.zip"
rustfs_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
memory_budget_bytes = 25769803776
audit_mode = "off"

[bands]
rlog = "$2"
EOF
}
RUN_TOML="$TMP/run.toml"
write_run_toml "$RUN_TOML" "$BANDS"

reset_log() { : >"$STUB_LOG"; rm -f "$STUB_DIR"/userdata-* "$STUB_DIR/client-tokens"; }

# --- launch.sh --------------------------------------------------------------

# Catches: a launch that starts one instance, gives the arms different run ids,
# calls aws under --dry-run, or drops any of shutdown -h +600, the terminate
# behaviour, the instance profile, the 500 GB gp3 volume, the arm name or
# either sha256 check from the user-data.
reset_log
code=0
bash "$KIT_SRC/launch.sh" --dry-run "$RUN_TOML" >"$TMP/dry.out" 2>"$TMP/dry.err" || code=$?
run_id="$(sed -n 's/^run id: *//p' "$TMP/dry.out")"
n_launch="$(grep -c '^DRY-RUN: aws ec2 run-instances' "$TMP/dry.err")"
tokens="$(grep '^DRY-RUN: aws ec2 run-instances' "$TMP/dry.err" | sed -n 's/.*--client-token \([^ ]*\) .*/\1/p' | tr '\n' ' ')"
ud_ok=1
for arm in A B; do
  ud="$(sed -n "/^DRY-RUN user-data $arm:\$/,/^DRY-RUN end user-data $arm\$/p" "$TMP/dry.err")"
  for want in 'shutdown -h +600' 'sha256sum -c kit.sha256' 'sha256sum -c SHA256SUMS' \
    "printf '%s\\n' $arm >\"\$KIT/ARM\"" "printf '%s\\n' $run_id >\"\$KIT/RUN_ID\"" \
    "OUT=s3://kit-test-bucket/clickbench-kit/runs/$run_id/out/$arm" 'nohup bash "$KIT/kit/box/run.sh"'; do
    case "$ud" in *"$want"*) ;; *) ud_ok=0; echo "  user-data $arm lacks: $want" ;; esac
  done
done
flags_ok=1
while IFS= read -r line; do
  # printf %q escapes the braces and commas of the block device mapping.
  line="${line//\\/}"
  for want in '--instance-initiated-shutdown-behavior terminate' '--iam-instance-profile Name=ravel-clickbench-instance-profile' \
    'VolumeSize=500,VolumeType=gp3' '--metadata-options HttpTokens=required'; do
    case "$line" in *"$want"*) ;; *) flags_ok=0; echo "  run-instances lacks: $want" ;; esac
  done
done < <(grep '^DRY-RUN: aws ec2 run-instances' "$TMP/dry.err")
if [ "$code" = 0 ] && [ -n "$run_id" ] && [ "$n_launch" = 2 ] \
  && [ "$tokens" = "$run_id-A $run_id-B " ] && [ "$ud_ok" = 1 ] && [ "$flags_ok" = 1 ] \
  && [ ! -s "$STUB_LOG" ] && grep -q "^instance A: *i-dryruna" "$TMP/dry.out" \
  && grep -q "^instance B: *i-dryrunb" "$TMP/dry.out" \
  && grep -q "^out prefix: *s3://kit-test-bucket/clickbench-kit/runs/$run_id/out/\$" "$TMP/dry.out"; then
  pass "dry-run launches arm A and arm B under one run id with the full user-data"
else
  fail "dry-run" "exit $code, run id '$run_id', $n_launch launches, tokens '$tokens', aws calls $(wc -l <"$STUB_LOG")"
fi
DRY_STAGE="$(sed -n 's/^launch: staged in\/ files kept in //p' "$TMP/dry.err")"

# Catches: a user-data that does not carry the staged kit.tgz's and run.toml's
# own digests, leaving the box to trust a kit.sha256 read from S3.
kit_sha="$(sha256sum "$DRY_STAGE/in/kit.tgz" 2>/dev/null | cut -d' ' -f1)"
toml_sha="$(sha256sum "$DRY_STAGE/in/run.toml" 2>/dev/null | cut -d' ' -f1)"
ud="$(sed -n '/^DRY-RUN user-data B:$/,/^DRY-RUN end user-data B$/p' "$TMP/dry.err")"
if [ -n "$kit_sha" ] && [ -n "$toml_sha" ] \
  && [[ "$ud" == *"printf '%s  %s\\n' $kit_sha kit.tgz $toml_sha run.toml >\"\$KIT/in/kit.sha256\""* ]] \
  && [[ "$ud" == *'for f in kit.tgz run.toml; do'* ]]; then
  pass "the user-data embeds the staged kit.tgz and run.toml digests and downloads no kit.sha256"
else
  fail "embedded digests" "kit.tgz $kit_sha, run.toml $toml_sha not in the user-data"
fi
[ -z "$DRY_STAGE" ] || rm -rf "$DRY_STAGE"

# Catches: a band file still holding a FILL-IN placeholder in a section the
# run selects reaching the launch, or a refusal that does not name the keys.
# The checked-in rlog.toml's absolute section is not filled in yet.
reset_log
write_run_toml "$TMP/run-placeholder.toml" "benchmarks/clickbench/bands/rlog.toml"
code=0
bash "$KIT_SRC/launch.sh" --dry-run "$TMP/run-placeholder.toml" >"$TMP/ph.out" 2>"$TMP/ph.err" || code=$?
if [ "$code" = 64 ] \
  && grep -q 'placeholder value in absolute.hot_sum_s_max, absolute.cold_sum_s_max, absolute.error_pooled_max, absolute.concurrent_qps_min, absolute.concurrent_error_pooled_max, absolute.q29_probe_s_max, absolute.hot_s_max.q01, ' "$TMP/ph.err" \
  && grep -q 'absolute.hot_s_max.q43; fill it in' "$TMP/ph.err" && ! grep -q 'DRY-RUN' "$TMP/ph.err"; then
  pass "a placeholder in a selected band section is refused, naming the keys"
else
  fail "placeholder" "exit $code: $(head -c 300 "$TMP/ph.err")"
fi

# Catches: placeholders refused in a section the run does not select, which
# would block every run until the absolute bands are measured.
write_run_toml "$TMP/run-relonly.toml" "benchmarks/clickbench/bands/rlog.toml" '["relative"]'
code=0
bash "$KIT_SRC/launch.sh" --dry-run "$TMP/run-relonly.toml" >/dev/null 2>"$TMP/relonly.err" || code=$?
if [ "$code" = 0 ] && grep -q '^DRY-RUN: aws ec2 run-instances' "$TMP/relonly.err"; then
  pass "placeholders in an unselected band section are allowed"
else
  fail "unselected placeholder" "exit $code: $(head -c 300 "$TMP/relonly.err")"
fi
DRY_STAGE="$(sed -n 's/^launch: staged in\/ files kept in //p' "$TMP/relonly.err")"
[ -z "$DRY_STAGE" ] || rm -rf "$DRY_STAGE"

# Catches: a placeholder in the relative section slipping through when only
# the absolute section was checked.
sed 's/^qps_min_ratio = .*/qps_min_ratio = "FILL-IN-LATER"/' "$BANDS" >"$TMP/rlog-relph.toml"
write_run_toml "$TMP/run-relph.toml" "$TMP/rlog-relph.toml" '["relative"]'
code=0
bash "$KIT_SRC/launch.sh" --dry-run "$TMP/run-relph.toml" >/dev/null 2>"$TMP/relph.err" || code=$?
if [ "$code" = 64 ] && grep -q 'placeholder value in relative.qps_min_ratio;' "$TMP/relph.err"; then
  pass "a placeholder in the relative section is refused, naming the key"
else
  fail "relative placeholder" "exit $code: $(head -c 300 "$TMP/relph.err")"
fi

# band_case NAME SED WANT: kit.py check-bands on a copy of the filled band file
# edited by SED must exit 2 with WANT in its message.
band_case() {
  local name="$1" edit="$2" want="$3" code=0
  sed "$edit" "$BANDS" >"$TMP/rlog-$name.toml"
  python3 "$KIT_SRC/lib/kit.py" check-bands rlog "$TMP/rlog-$name.toml" relative absolute \
    >/dev/null 2>"$TMP/bands-$name.err" || code=$?
  if [ "$code" = 2 ] && grep -qF -- "$want" "$TMP/bands-$name.err"; then
    pass "check-bands refuses a $name key, naming it"
  else
    fail "check-bands $name" "exit $code: $(head -c 300 "$TMP/bands-$name.err")"
  fi
}
# Catches: a key the kit no longer reads (the old concurrency ceiling) kept in
# a band file as if it still bound something.
band_case removed '/^\[relative\]$/a\
concurrency_error_ratio_ceiling = 0.01' \
  'removed key(s) relative.concurrency_error_ratio_ceiling; delete them'
# Catches: a misspelt key accepted, leaving the band it meant unset.
band_case unknown 's/^qps_min_ratio = /qps_min_ratoi = /' 'unknown key(s) relative.qps_min_ratoi for lane rlog'
# Catches: a band file with a required key absent judged anyway.
band_case missing '/^cold_sum_max_ratio = /d' 'missing required key(s) relative.cold_sum_max_ratio'

# run_case NAME SED WANT: kit.py check-run on a copy of the run.toml edited by
# SED must exit 2 with WANT in its message.
run_case() {
  local name="$1" edit="$2" want="$3" code=0
  sed "$edit" "$RUN_TOML" >"$TMP/run-$name.toml"
  python3 "$KIT_SRC/lib/kit.py" check-run "$TMP/run-$name.toml" >/dev/null 2>"$TMP/run-$name.err" || code=$?
  if [ "$code" = 2 ] && grep -qF -- "$want" "$TMP/run-$name.err"; then
    pass "check-run refuses $name, naming it"
  else
    fail "check-run $name" "exit $code: $(head -c 300 "$TMP/run-$name.err")"
  fi
}
# Catches: a misspelt optional key accepted, leaving the default it meant to
# override in force.
run_case "an unknown top-level key" '/^shards = /i\
shardz = 9' 'unknown key(s) shardz'
run_case "an unknown table" '$a\
[extra]\
x = 1' 'unknown table(s) [extra]'
run_case "an unknown [bands] key" '$a\
parquet = "benchmarks/clickbench/bands/parquet.toml"' 'unknown key(s) bands.parquet'

run_case "a truncated rustfs_sha256" 's/^rustfs_sha256 = .*/rustfs_sha256 = "abc123"/' \
  'rustfs_sha256 must be 64 lowercase hex digits'

# Catches: a perf phase that runs without the run.toml saying perf_record =
# true, whether the key is false or left at its default.
run_case "phase perf with perf_record false" \
  's/^phases = .*/phases = ["benchmark", "perf"]\nperf_record = false/' 'phase perf needs perf_record = true'
run_case "phase perf with perf_record left out" \
  's/^phases = .*/phases = ["benchmark", "perf"]/' 'phase perf needs perf_record = true'
# Catches: a phase key of the wrong type reaching the box, where bash
# arithmetic or the seed would take it as something else.
run_case "a string concurrent_rounds" 's/^shards = /concurrent_rounds = "3"\n&/' \
  "concurrent_rounds must be a whole number of at least 1, got '3'"
run_case "a zero concurrent_connections" 's/^shards = /concurrent_connections = 0\n&/' \
  'concurrent_connections must be a whole number of at least 1, got 0'
run_case "a boolean concurrent_duration_s" 's/^shards = /concurrent_duration_s = true\n&/' \
  'concurrent_duration_s must be a whole number of at least 1, got True'
run_case "a numeric concurrent_seed" 's/^shards = /concurrent_seed = 7\n&/' \
  'concurrent_seed must be a non-empty string, got 7'
run_case "a negative q29_probe_warmup" 's/^shards = /q29_probe_warmup = -1\n&/' \
  'q29_probe_warmup must be a whole number of at least 0, got -1'
run_case "a zero q29_probe_runs" 's/^shards = /q29_probe_runs = 0\n&/' \
  'q29_probe_runs must be a whole number of at least 1, got 0'
run_case "a string perf_record" 's/^shards = /perf_record = "true"\n&/' \
  "perf_record must be true or false, got 'true'"

# Catches: an arm's extra server flags overriding a flag the kit sets, so the
# arms differ by a setting nobody declared, in either spelling of the flag.
run_case "a server_flags_b repeating --shards" 's/^shards = /server_flags_b = ["--fetch-concurrency", "64", "--shards", "4"]\n&/' \
  'server_flags_b repeats --shards, which the kit sets itself'
run_case "a server_flags_a repeating --memory-budget-bytes=" 's/^shards = /server_flags_a = ["--memory-budget-bytes=1"]\n&/' \
  'server_flags_a repeats --memory-budget-bytes, which the kit sets itself'
run_case "a string server_flags_a" 's/^shards = /server_flags_a = "--fetch-concurrency 64"\n&/' \
  "server_flags_a must be an array of non-empty strings, got '--fetch-concurrency 64'"
# Catches: a flag the kit does not set refused, or read back as anything but
# its entries one per line.
code=0
sed 's/^shards = /server_flags_a = []\nserver_flags_b = ["--fetch-concurrency", "64"]\n&/' "$RUN_TOML" >"$TMP/run-newflag.toml"
python3 "$KIT_SRC/lib/kit.py" check-run "$TMP/run-newflag.toml" >/dev/null 2>"$TMP/run-newflag.err" || code=$?
got_b="$(python3 "$KIT_SRC/lib/kit.py" get "$TMP/run-newflag.toml" server_flags_b 2>&1 | tr '\n' ' ')"
got_a="$(python3 "$KIT_SRC/lib/kit.py" get "$TMP/run-newflag.toml" server_flags_a 2>&1)"
if [ "$code" = 0 ] && [ "$got_b" = "--fetch-concurrency 64 " ] && [ -z "$got_a" ]; then
  pass "check-run accepts an extra server flag the kit does not set"
else
  fail "check-run new flag" "exit $code, b '$got_b', a '$got_a': $(head -c 300 "$TMP/run-newflag.err")"
fi

# Catches: an absent optional key and an unreadable run.toml giving the same
# answer from kit.py get, so run.sh would take a failed read for "no extra
# flags": absent is exit 3, a file that cannot be read is exit 2.
absent=0 unreadable=0
python3 "$KIT_SRC/lib/kit.py" get "$RUN_TOML" server_flags_b >/dev/null 2>&1 || absent=$?
python3 "$KIT_SRC/lib/kit.py" get "$TMP/no-such-run.toml" server_flags_b >/dev/null 2>&1 || unreadable=$?
if [ "$absent" = 3 ] && [ "$unreadable" = 2 ]; then
  pass "kit.py get exits 3 for an absent key and 2 for a file it cannot read"
else
  fail "get exit codes" "absent $absent, unreadable $unreadable"
fi

# Catches: healthz_phases naming a phase the sampler does not cover, naming
# one twice, or not being an array.
run_case "a healthz_phases naming q29_probe" 's/^shards = /healthz_phases = ["benchmark", "q29_probe"]\n&/' \
  "healthz_phases must be an array of distinct phases from benchmark, concurrent, got ['benchmark', 'q29_probe']"
run_case "a healthz_phases naming a phase twice" 's/^shards = /healthz_phases = ["concurrent", "concurrent"]\n&/' \
  "healthz_phases must be an array of distinct phases from benchmark, concurrent, got ['concurrent', 'concurrent']"
run_case "a string healthz_phases" 's/^shards = /healthz_phases = "benchmark"\n&/' \
  "healthz_phases must be an array of distinct phases from benchmark, concurrent, got 'benchmark'"

# Catches: a stamped server setting that a start logged zero times or twice
# being stamped as a list, which two arms then compare equal on.
settings_case() {
  local name="$1" drop="$2" dup="$3" want="$4" code=0 log="$TMP/settings-$1.log"
  : >"$log"
  for kv in fetch_concurrency=32 cache_max_bytes=7689077760 catalog_cache_max_bytes=1537815552 \
    sql_max_query_bytes=16451897344 sql_tenant_max_bytes=32903794688; do
    [ "${kv%%=*}" = "$drop" ] && continue
    printf 'INFO ravel_server::config: performance default resolved setting="%s" value=%s source="derived"\n' \
      "${kv%%=*}" "${kv#*=}" >>"$log"
    [ "${kv%%=*}" = "$dup" ] && printf 'INFO ravel_server::config: performance default resolved setting="%s" value=%s source="derived"\n' \
      "${kv%%=*}" "${kv#*=}" >>"$log"
  done
  python3 -c 'import sys; sys.path.insert(0, sys.argv[1]); import kit
try:
    print(kit.server_settings([sys.argv[2]]))
except kit.Refusal as e:
    print(e, file=sys.stderr); sys.exit(2)' "$KIT_SRC/lib" "$log" >"$TMP/settings-$name.out" 2>"$TMP/settings-$name.err" || code=$?
  if [ -z "$want" ]; then
    if [ "$code" = 0 ] && grep -qF "'fetch_concurrency': 32" "$TMP/settings-$name.out"; then
      pass "server settings: $name stamps one number per setting"
    else
      fail "server settings $name" "exit $code: $(head -c 300 "$TMP/settings-$name.err")"
    fi
  elif [ "$code" = 2 ] && grep -qF -- "$want" "$TMP/settings-$name.err"; then
    pass "server settings: $name is refused, naming the setting"
  else
    fail "server settings $name" "exit $code: $(head -c 300 "$TMP/settings-$name.err") $(head -c 200 "$TMP/settings-$name.out")"
  fi
}
settings_case "all five once" "" "" ""
settings_case "a setting absent" fetch_concurrency "" 'carries fetch_concurrency 0 times, not once'
settings_case "a setting logged twice" "" cache_max_bytes 'carries cache_max_bytes 2 times, not once'

# Catches: launching (or uploading) when the instance role could not write
# results, or a refusal that does not name the role.
reset_log
code=0
STUB_DENY_ACTION=s3:PutObject bash "$KIT_SRC/launch.sh" "$RUN_TOML" >/dev/null 2>"$TMP/deny.err" || code=$?
if [ "$code" = 65 ] && grep -q 'role arn:aws:iam::123456789012:role/kit-test-role (instance profile ravel-clickbench-instance-profile) lacks s3:PutObject' "$TMP/deny.err" \
  && ! grep -q 'aws s3 cp' "$STUB_LOG" && ! grep -q 'run-instances' "$STUB_LOG"; then
  pass "a role without s3:PutObject is refused by name before any upload"
else
  fail "role check" "exit $code: $(head -c 300 "$TMP/deny.err")"
fi

# Catches: a role check that asks about some resource other than the run's own
# in/ and out/ prefixes. The stub role may only use clickbench-kit/runs/, so a
# run under another prefix must be refused, naming the resource.
reset_log
sed 's/^prefix = .*/prefix = "elsewhere"/' "$RUN_TOML" >"$TMP/run-prefix.toml"
code=0
bash "$KIT_SRC/launch.sh" "$TMP/run-prefix.toml" >/dev/null 2>"$TMP/prefix.err" || code=$?
if [ "$code" = 65 ] && grep -q 'lacks s3:GetObject on arn:aws:s3:::kit-test-bucket/elsewhere/runs/.*/in/\*' "$TMP/prefix.err" \
  && grep -q 'simulate-principal-policy .*--resource-arns arn:aws:s3:::kit-test-bucket/elsewhere/runs/' "$STUB_LOG" \
  && ! grep -q 'aws s3 cp' "$STUB_LOG" && ! grep -q 'run-instances' "$STUB_LOG"; then
  pass "the role check asks about the run's own prefix and refuses a role that lacks it"
else
  fail "role prefix" "exit $code: $(head -c 300 "$TMP/prefix.err")"
fi

# Catches: an unreadable instance profile treated as anything but a role
# refusal (exit 65), or reaching an upload.
reset_log
code=0
STUB_NO_PROFILE=1 bash "$KIT_SRC/launch.sh" "$RUN_TOML" >/dev/null 2>"$TMP/noprofile.err" || code=$?
if [ "$code" = 65 ] && grep -q 'cannot read instance profile ravel-clickbench-instance-profile' "$TMP/noprofile.err" \
  && ! grep -q 'aws s3 cp' "$STUB_LOG" && ! grep -q 'run-instances' "$STUB_LOG"; then
  pass "a missing instance profile exits 65 before any upload"
else
  fail "missing profile" "exit $code: $(head -c 300 "$TMP/noprofile.err")"
fi

# Catches: a failed second launch leaving the first instance running.
reset_log
code=0
STUB_FAIL_ARM=B bash "$KIT_SRC/launch.sh" "$RUN_TOML" >/dev/null 2>"$TMP/failb.err" || code=$?
if [ "$code" = 1 ] && grep -q 'ec2 terminate-instances .*--instance-ids i-0aaaaaaaaaaaaaaaa' "$STUB_LOG"; then
  pass "a failed arm B launch terminates arm A"
else
  fail "failed launch cleanup" "exit $code: $(tail -n 3 "$TMP/failb.err")"
fi

# The real launch every box case below boots from. Catches: in/ missing a
# file, a kit.tgz shipping anything beyond the box script, kit.py, the corpus,
# the lane band files and SHA256SUMS, or two launches sharing a client token.
reset_log
code=0
bash "$KIT_SRC/launch.sh" "$RUN_TOML" >"$TMP/launch.out" 2>"$TMP/launch.err" || code=$?
RUN_ID="$(sed -n 's/^run id: *//p' "$TMP/launch.out")"
RUN_DIR="$FAKE_S3/kit-test-bucket/clickbench-kit/runs/$RUN_ID"
tar_list="$(tar -tzf "$RUN_DIR/in/kit.tgz" 2>/dev/null | grep -v '/$' | LC_ALL=C sort | tr '\n' ' ')"
if [ "$code" = 0 ] && [ -f "$RUN_DIR/in/kit.tgz" ] && [ -f "$RUN_DIR/in/kit.sha256" ] && [ -f "$RUN_DIR/in/run.toml" ] \
  && [ "$tar_list" = "kit/SHA256SUMS kit/bands/rlog.toml kit/box/run.sh kit/corpus.json kit/lib/kit.py " ] \
  && [ "$(sort -u "$STUB_DIR/client-tokens" | wc -l | tr -d ' ')" = 2 ] \
  && [ -s "$STUB_DIR/userdata-A" ] && [ -s "$STUB_DIR/userdata-B" ]; then
  pass "launch uploads kit.tgz, kit.sha256 and run.toml to in/ and launches both arms"
else
  fail "launch" "exit $code, run id '$RUN_ID', kit.tgz holds '$tar_list'"
fi
cp "$STUB_DIR/userdata-A" "$TMP/userdata-A"
cp "$STUB_DIR/userdata-B" "$TMP/userdata-B"
IN_SAVED="$TMP/in-saved"
cp -R "$RUN_DIR/in" "$IN_SAVED"

# --- user-data --------------------------------------------------------------

# boot_box ARM BOX: run the arm's user-data with /root/kit moved to BOX/kit.
boot_box() {
  local arm="$1" box="$2"
  rm -rf "$box"
  mkdir -p "$box"
  grep -q '^KIT=/root/kit$' "$TMP/userdata-$arm" || { echo "  user-data has no KIT=/root/kit line"; return 1; }
  sed "s#^KIT=/root/kit\$#KIT=$box/kit#" "$TMP/userdata-$arm" >"$box/user-data.sh"
  bash "$box/user-data.sh"
  # nohup's stub ran in the background.
  local i
  for ((i = 0; i < 50; i++)); do
    grep -q '^nohup ' "$STUB_LOG" && break
    grep -q "^aws s3 cp .*out/$arm/FAILED" "$STUB_LOG" && break
    sleep 0.1
  done
}

# Catches: a user-data that starts run.sh without the kit and run.toml in
# place, or writes the wrong arm or run id.
reset_log
BOX_UD="$TMP/box-ud"
code=0
boot_box A "$BOX_UD" || code=$?
if [ "$code" = 0 ] && grep -q "^nohup bash $BOX_UD/kit/kit/box/run.sh\$" "$STUB_LOG" \
  && grep -q '^shutdown -h +600$' "$STUB_LOG" && ! grep -q '^shutdown -h now' "$STUB_LOG" \
  && [ "$(cat "$BOX_UD/kit/ARM")" = A ] && [ "$(cat "$BOX_UD/kit/RUN_ID")" = "$RUN_ID" ] \
  && cmp -s "$BOX_UD/kit/run.toml" "$IN_SAVED/run.toml" && [ -f "$BOX_UD/kit/kit/box/run.sh" ]; then
  pass "user-data arms shutdown -h +600, verifies the kit and starts run.sh under nohup"
else
  fail "user-data" "exit $code; log: $(tr '\n' ';' <"$STUB_LOG" | head -c 400)"
fi

# Catches: launch uploading the run.toml as given, so the box falls back to
# defaults the laptop never recorded, or changing a key the file did set.
added="$(diff "$RUN_TOML" "$IN_SAVED/run.toml" | sed -n 's/^> //p' | grep -v '^#' | grep -v '^$' | tr '\n' ';')"
removed="$(diff "$RUN_TOML" "$IN_SAVED/run.toml" | grep -c '^<')"
if [ "$added" = 'concurrent_rounds = 3;concurrent_connections = 10;concurrent_duration_s = 600;concurrent_seed = "clickbench";q29_probe_warmup = 1;q29_probe_runs = 3;perf_record = false;' ] \
  && [ "$removed" = 0 ] && python3 "$KIT_SRC/lib/kit.py" check-run "$IN_SAVED/run.toml" >/dev/null; then
  pass "launch uploads run.toml with every phase key it left out written at its default"
else
  fail "resolved run.toml" "added '$added', removed $removed line(s)"
fi

# Catches: dropping or weakening the kit.sha256 check: a tampered kit.tgz
# must stop the box before anything runs, upload FAILED and shut down.
reset_log
printf 'x' >>"$RUN_DIR/in/kit.tgz"
BOX_T="$TMP/box-tampered"
boot_box B "$BOX_T"
if [ -f "$RUN_DIR/out/B/FAILED" ] && grep -q 'FAILED step=user-data sha256 check of kit.tgz and run.toml' "$RUN_DIR/out/B/FAILED" \
  && ! grep -q '^nohup' "$STUB_LOG" && grep -q '^shutdown -h now' "$STUB_LOG" && [ ! -e "$BOX_T/kit/kit" ]; then
  pass "a tampered kit.tgz fails the box before any step runs"
else
  fail "tampered kit.tgz" "log: $(tr '\n' ';' <"$STUB_LOG" | head -c 400)"
fi
rm -rf "$RUN_DIR/out/B"
cp "$IN_SAVED/kit.tgz" "$RUN_DIR/in/kit.tgz"

# Catches: a box that trusts a kit.sha256 read from S3: whoever can rewrite
# kit.tgz there can rewrite that file too, so a changed kit.tgz under a
# recomputed kit.sha256 must still fail against the user-data's own digests.
reset_log
mkdir -p "$TMP/retar"
tar -xzf "$IN_SAVED/kit.tgz" -C "$TMP/retar"
echo 'echo tampered' >>"$TMP/retar/kit/box/run.sh"
tar -czf "$RUN_DIR/in/kit.tgz" -C "$TMP/retar" kit
(cd "$RUN_DIR/in" && sha256sum kit.tgz run.toml >kit.sha256)
boot_box B "$BOX_T"
if grep -q 'FAILED step=user-data sha256 check of kit.tgz and run.toml' "$RUN_DIR/out/B/FAILED" 2>/dev/null \
  && ! grep -q '^nohup' "$STUB_LOG" && grep -q '^shutdown -h now' "$STUB_LOG"; then
  pass "a tampered kit.tgz under a recomputed kit.sha256 fails on the user-data's digests"
else
  fail "recomputed kit.sha256" "log: $(tr '\n' ';' <"$STUB_LOG" | head -c 400)"
fi
rm -rf "$RUN_DIR/out/B"

# Catches: dropping the SHA256SUMS check: the same changed kit file, with the
# user-data's own kit.tgz digest updated to match, must fail on SHA256SUMS.
reset_log
cp "$TMP/userdata-B" "$TMP/userdata-B.orig"
old_sha="$(sha256sum "$IN_SAVED/kit.tgz" | cut -d' ' -f1)"
new_sha="$(sha256sum "$RUN_DIR/in/kit.tgz" | cut -d' ' -f1)"
sed "s/$old_sha kit.tgz/$new_sha kit.tgz/" "$TMP/userdata-B.orig" >"$TMP/userdata-B"
boot_box B "$BOX_T"
if grep -q "$new_sha kit.tgz" "$TMP/userdata-B" \
  && grep -q 'FAILED step=user-data sha256 check of the kit files' "$RUN_DIR/out/B/FAILED" 2>/dev/null \
  && ! grep -q '^nohup' "$STUB_LOG"; then
  pass "a kit file changed under matching kit.tgz digests fails on SHA256SUMS"
else
  fail "SHA256SUMS check" "log: $(tr '\n' ';' <"$STUB_LOG" | head -c 400)"
fi
mv "$TMP/userdata-B.orig" "$TMP/userdata-B"
rm -rf "$RUN_DIR/out/B"
cp "$IN_SAVED/kit.tgz" "$IN_SAVED/kit.sha256" "$RUN_DIR/in/"

# --- box/run.sh -------------------------------------------------------------

# The box driver: run.sh's own main, with the bootstrap steps that need the
# network or a compiler replaced, and run_query wrapped by a counter the way
# a later phase would wrap it.
cat >"$TMP/box-driver.sh" <<'DRIVER'
#!/usr/bin/env bash
export KIT_ROOT="$1"
export KIT_RUN_SH_LIB=1
export KIT_IMDS=http://imds.invalid
export KIT_DROP_CACHES_FILE="$KIT_ROOT/drop_caches"
export KIT_READY_TRIES=100 KIT_READY_INTERVAL=0.02
# shellcheck source=/dev/null
. "$KIT_ROOT/kit/kit/box/run.sh"
# DRIVER_NO_REDACT=1: redaction replaces nothing, so the leak check must
# refuse what it is handed, and a key is logged again just before the evidence
# step so it sits inside FAILED's run.log tail.
if [ -n "${DRIVER_NO_REDACT:-}" ]; then
  scrub() { :; }
  eval "real_write_evidence() $(declare -f write_evidence | tail -n +2)"
  write_evidence() {
    echo "rustfs: still running with secret_key=$RUSTFS_SECRET_KEY" >&2
    real_write_evidence
  }
fi
bootstrap_packages() { :; }
bootstrap_toolchain() { :; }
bootstrap_source() {
  mkdir -p "$RAVEL_DIR/benchmarks/clickbench"
  echo '# mapping' >"$MAPPING"
  printf 'rustc 1.90.0 (stub)\ncargo 1.90.0 (stub)\n' >"$STAMPS/toolchain"
}
start_dataset_download() { :; }
bootstrap_build() {
  mkdir -p "$BIN_DIR"
  cp "$BINSTUBS/ravel-cli" "$BIN_DIR/ravel-cli-$SHA"
  cp "$BINSTUBS/ravel-server" "$BIN_DIR/ravel-server-$SHA"
}
wait_dataset() { echo 1111111111111111111111111111111111111111111111111111111111111111 >"$KIT/dataset.sha256"; }
# RustFS logging its own keys, into its log and into run.log, neither of
# which the evidence or FAILED may carry. DRIVER_REAL_RUSTFS=1 keeps run.sh's
# own bootstrap_rustfs, for a case that never gets past its version check.
if [ -z "${DRIVER_REAL_RUSTFS:-}" ]; then
  bootstrap_rustfs() {
    make_secrets && echo 'rustfs 1.0.0-stub' >"$KIT/rustfs.version" && . "$SECRETS_ENV" \
      && echo "rustfs: starting with access_key=$RUSTFS_ACCESS_KEY secret_key=$RUSTFS_SECRET_KEY" >"$KIT/rustfs.log" \
      && echo "rustfs: starting with access_key=$RUSTFS_ACCESS_KEY secret_key=$RUSTFS_SECRET_KEY" >&2
  }
fi
# DRIVER_HEALTHZ_EDIT=dup repeats healthz.txt's first line after the phases
# ran, and =drop deletes its HEARTBEAT line.
if [ -n "${DRIVER_HEALTHZ_EDIT:-}" ]; then
  eval "real_run_phases() $(declare -f run_phases | tail -n +2)"
  run_phases() {
    real_run_phases || return 1
    case "$DRIVER_HEALTHZ_EDIT" in
      dup) head -n 1 "$OUT/healthz.txt" >>"$OUT/healthz.txt" ;;
      drop) grep -v '^HEARTBEAT ' "$OUT/healthz.txt" >"$OUT/healthz.txt.new" \
        && mv "$OUT/healthz.txt.new" "$OUT/healthz.txt" ;;
    esac
  }
fi
drop_caches() {
  echo drop >>"$STUB_DIR/sequence"
  sync
  echo 3 >"$DROP_CACHES_FILE"
}
# GNU time is not on every CI image; run.sh's own do_load and do_compact run
# with only the timing wrapper replaced.
timed() { shift 2; "$@"; }
eval "real_run_query() $(declare -f run_query | tail -n +2)"
run_query() {
  echo "$*" >>"$STUB_DIR/run_query-calls"
  echo "try$4" >>"$STUB_DIR/sequence"
  real_run_query "$@"
}
main
DRIVER
export BINSTUBS

# run_box ARM NAME [ENV=VALUE...]: boot a box for ARM and run main on it.
# BOX_TOML_SED, when set, is a sed script applied to the box's run.toml after
# boot (the user-data has checked its digest by then).
run_box() {
  local arm="$1" name="$2" box="$TMP/$2" code=0
  shift 2
  reset_log
  rm -f "$STUB_DIR/server-argv" "$STUB_DIR/cli-argv" "$STUB_DIR/curl-sql" "$STUB_DIR/run_query-calls" \
    "$STUB_DIR/objects" "$STUB_DIR/sql-bodies" "$STUB_DIR/sequence" "$STUB_DIR/verify-bodies" \
    "$STUB_DIR/server-ready"
  boot_box "$arm" "$box" || return 1
  if [ -n "${BOX_TOML_SED:-}" ]; then
    sed "$BOX_TOML_SED" "$box/kit/run.toml" >"$box/kit/run.toml.new" && mv "$box/kit/run.toml.new" "$box/kit/run.toml"
  fi
  env "$@" bash "$TMP/box-driver.sh" "$box" >"$box/kit/run.log" 2>&1 || code=$?
  return "$code"
}

# The happy path: arm A, every statement answered with its expected rows.
BOX_A="$TMP/box-a"
code=0
run_box A box-a STUB_SQL_TIME=0.5 || code=$?
OUT_A="$RUN_DIR/out/A"

# Catches: a run that does not end in DONE with results.json, evidence.tgz and
# progress.log uploaded, or that leaves its instance running.
if [ "$code" = 0 ] && grep -q '^DONE bands=pass ' "$OUT_A/DONE" 2>/dev/null && [ -f "$OUT_A/results.json" ] \
  && [ -f "$OUT_A/evidence.tgz" ] && [ -f "$OUT_A/progress.log" ] && [ ! -e "$OUT_A/FAILED" ] \
  && grep -q 'ec2 terminate-instances .*--instance-ids i-0b0x00000000000001' "$STUB_LOG"; then
  pass "box run uploads results.json, evidence.tgz, progress.log and DONE, then terminates itself"
else
  fail "box run" "exit $code; $(cat "$OUT_A/FAILED" 2>/dev/null | head -n 3); run.log: $(tail -n 5 "$BOX_A/kit/run.log")"
fi

# Catches: a terminate that succeeds through the API taken as the end of the
# box: `shutdown -h now` must still run, exactly once, after it.
term_line="$(grep -n 'ec2 terminate-instances' "$STUB_LOG" | head -n 1 | cut -d: -f1)"
shut_line="$(grep -n '^shutdown -h now$' "$STUB_LOG" | head -n 1 | cut -d: -f1)"
if [ -n "$term_line" ] && [ -n "$shut_line" ] && [ "$shut_line" -gt "$term_line" ] \
  && [ "$(grep -c '^shutdown -h now$' "$STUB_LOG")" = 1 ] \
  && grep -q 'terminate instance=i-0b0x00000000000001' "$BOX_A/kit/progress.log" 2>/dev/null; then
  pass "a successful terminate is still followed by one shutdown -h now"
else
  fail "shutdown after terminate" "terminate at line '$term_line', shutdown at '$shut_line': $(tr '\n' ';' <"$STUB_LOG" | tail -c 300)"
fi

# Catches: results.json leaving SqlLatencyReport's statement shape (entries of
# EntryReport, failed of FailedEntry), losing a provenance stamp, the fold,
# BurstBalance or query window stamps, the per-statement answer check, or the
# bands block; or a band row carrying a q-prefixed id, which analyse.py would
# count as a second report row.
check="$(python3 - "$OUT_A/results.json" "$SHA_A" "$REPO_ROOT/benchmarks/clickbench/hits.corpus.json" "$DATASET_SHA" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
corpus = {e["id"]: e.get("expected_rows") for e in json.load(open(sys.argv[3]))["entries"]}
p = r["provenance"]
bad = []
def want(cond, what):
    if not cond:
        bad.append(what)
for k in ("provenance", "entries", "skipped", "failed", "totals", "error_pooled", "concurrent",
          "q29_probe", "phases", "bands", "timing_definition"):
    want(k in r, "field " + k)
for k in ("git_sha", "binary", "arm", "tenant", "server", "server_logs", "server_settings",
          "band_sections", "kit_arm", "run_id", "lanes", "toolchain", "hardware", "backend",
          "corpus_digest", "server_flags", "audit_mode", "burst_balance", "fold", "object_counts",
          "query_window", "row_count"):
    want(k in p, "provenance." + k)
rc = p.get("row_count") or {}
want(rc.get("rows_expected") == 99997497 and rc.get("load_rows_written") == 99997497
     and rc.get("count_star") == 99997497 and rc.get("statement") == "q01_count_all", "row_count stamp %r" % rc)
want(isinstance(r.get("timing_definition"), str) and "tries 2 and 3" in r["timing_definition"]
     and "try 1" in r["timing_definition"], "timing_definition")
want(p.get("git_sha") == sys.argv[2], "git_sha")
want(p.get("arm") == "a" and p.get("kit_arm") == "A", "arm")
want(p.get("server_settings", {}).get("sql_max_query_bytes") == 16451897344, "sql_max_query_bytes")
want(len(p.get("server_logs", [])) == 43, "one server log per cold start")
want(p.get("band_sections") == ["relative", "absolute"], "band_sections")
w = p.get("query_window", {})
want(w.get("window_hours") == 200000 and w.get("end", 0) - w.get("start", 0) == 720000000, "query_window")
want(p.get("corpus_digest", {}).get("dataset_sha256") == sys.argv[4], "dataset_sha256")
want(p.get("fold", {}).get("precondition") == "met" and p["fold"].get("seal_through_hour") == "Some(491234)"
     and p["fold"].get("objects_written_by_load") == 100 and p["fold"].get("entry_count") == 100
     and p["fold"].get("store_keys_added_by_load") == 200, "fold stamp %r" % p.get("fold"))
want(p.get("object_counts") == {"before_load": 10, "after_load": 210}, "object_counts")
for side in ("start", "end"):
    b = (p.get("burst_balance") or {}).get(side) or {}
    want(b.get("volume_type") == "gp3" and b.get("value") is None and "gp3" in b.get("reason", ""),
         "burst_balance." + side)
sf = p.get("server_flags", {})
want("--memory-budget-bytes 25769803776" in sf.get("flags", "") and sf.get("extra_flags") == []
     and sf.get("base_flags", [])[-2:] == ["--audit-mode", "off"]
     and " ".join(sf.get("base_flags", [])) == sf.get("flags"), "server_flags %r" % sf)
hz = r.get("healthz") or {}
bench = (hz.get("phases") or {}).get("benchmark") or {}
want(hz.get("interval_ms") == 250.0 and hz.get("timeout_ms") == 2000.0
     and list(hz.get("phases", {})) == ["benchmark"] and bench.get("heartbeat_max_s") == 0.5
     and sorted(bench.get("ports", {})) == ["4318", "4319"]
     and all(v["n"] >= 43 and v["failures"] == 0 and 0 < v["p99_ms"] < 2000.0
             for v in bench["ports"].values()), "healthz %r" % hz)
want(p.get("audit_mode") == "off", "audit_mode")
want(p.get("hardware", {}).get("instance_type") == "c6a.4xlarge"
     and p["hardware"].get("cpu") == "Stub CPU 9000 @ 2.00GHz"
     and p["hardware"].get("mem_total_kb") == 32000000, "hardware %r" % p.get("hardware"))
want(p.get("backend", {}).get("store") == "rustfs", "backend")
want(len(p.get("corpus_digest", {}).get("corpus_sha256", "")) == 64, "corpus_digest")
want(p.get("toolchain", "").startswith("rustc 1.90.0"), "toolchain")
entries = r.get("entries", [])
want([e["id"] for e in entries] == list(corpus), "43 entries in corpus order")
want(r.get("failed") == [] and r.get("skipped") == [], "no failed or skipped statements")
for e in entries:
    want(e["min_ms"] == 500.0 and e["median_ms"] == 500.0 and e["max_ms"] == 500.0
         and e["cold_ms"] == 500.0, e["id"] + " timings")
    want(e["per_run_accounting"] is None and e["error"] is None, e["id"] + " accounting/error")
    exp = corpus[e["id"]]
    want(e["answer"] == {"answered": True, "rows": exp if exp is not None else 3,
                         "expected_rows": exp, "match": True if exp is not None else None},
         e["id"] + " answer " + json.dumps(e["answer"]))
    want(e["rows_returned"] == e["answer"]["rows"], e["id"] + " rows_returned")
want(r["totals"] == {"answered": 43, "failed": [], "hot_sum_s": 21.5, "cold_sum_s": 21.5}, "totals")
want(r["error_pooled"] == {"calls": 129, "errors": 0, "ratio": 0.0, "phases": ["benchmark"]},
     "error_pooled")
want(r["concurrent"] is None and r["q29_probe"] is None, "no concurrent or q29_probe block")
want(r["phases"]["benchmark"]["calls"] == 129 and r["phases"]["benchmark"]["errors"] == 0
     and r["phases"]["benchmark"]["refusal_classes"] == {}, "phases block")
verdicts = {b["band"]: b["verdict"] for b in r["bands"]}
want(len(r["bands"]) == 49 and [b["verdict"] for b in r["bands"]].count("pass") == 46,
     "46 passing absolute bands")
want([b["band"] for b in r["bands"] if b["verdict"] != "pass"]
     == ["concurrent qps >= concurrent_qps_min",
         "concurrent error_ratio_weighted <= concurrent_error_pooled_max",
         "q29_probe min_s <= q29_probe_s_max"]
     and all(b["verdict"] == "inconclusive" for b in r["bands"] if b["verdict"] != "pass"),
     "the three phase bands inconclusive with their phases not run")
want(verdicts.get("hot_sum_s <= hot_sum_s_max") == "pass", "hot band")
want(verdicts.get("q01 hot_s <= hot_s_max.q01") == "pass", "q01 band")
want(all(b.get("lane") == "rlog" and "id" not in b for b in r["bands"]), "band lanes and ids")
print(",".join(bad))
PY
)" || check="python failed"
if [ -z "$check" ] && python3 "$KIT_SRC/lib/kit.py" validate "$OUT_A/results.json"; then
  pass "results.json is SqlLatencyReport-shaped with the answer checks, the provenance stamps and the bands"
else
  fail "results.json" "missing or wrong: $check"
fi

# Catches: the healthz sampler counting samples taken while the server was
# down between statements, its raw samples left out of the evidence, or its
# lines not printed once each. Each statement's server start opens one gate
# generation, and a gated sample is one taken with the server up.
rm -rf "$TMP/evidence-a"
mkdir -p "$TMP/evidence-a"
tar -xzf "$OUT_A/evidence.tgz" -C "$TMP/evidence-a" 2>/dev/null
check="$(python3 - "$TMP/evidence-a/out" <<'PY' 2>&1
import json, os, sys
d = sys.argv[1]
for name in ("4318", "4319", "metrics"):
    recs = [json.loads(l) for l in open(os.path.join(d, "benchmark", "healthz", name + ".jsonl"))]
    gated = [r for r in recs if r["gated"]]
    if sorted(set(r["gen"] for r in gated)) != list(range(1, 44)):
        print(name, "gated generations", sorted(set(r["gen"] for r in gated))[:5])
    if any(not r["ok"] or r["status"] != 200 for r in gated):
        print(name, "a gated sample not a 200", [r for r in gated if not r["ok"]][:1])
    if name != "metrics" and any(not isinstance(r.get("ms"), float) for r in recs):
        print(name, "a sample without ms")
lines = open(os.path.join(d, "healthz.txt")).read().splitlines()
if [l.split(" n=")[0] for l in lines] != ["HEALTHZ phase=benchmark port=4318", "HEALTHZ phase=benchmark port=4319",
                                          "HEARTBEAT phase=benchmark"]:
    print("healthz.txt", lines)
scrapes = [json.loads(l) for l in open(os.path.join(d, "benchmark", "healthz", "metrics.jsonl"))]
n_gated = sum(1 for r in scrapes if r["gated"])
if lines[-1:] != ["HEARTBEAT phase=benchmark n=%d failures=0 bad=0 max_s=0.500" % n_gated] or n_gated < 43:
    print("HEARTBEAT", lines[-1:], "gated scrapes", n_gated)
PY
)"
if [ -z "$check" ] && [ "$(grep -c '^HEALTHZ phase=benchmark port=4318 ' "$BOX_A/kit/run.log")" = 1 ] \
  && [ "$(grep -c '^HEARTBEAT phase=benchmark ' "$BOX_A/kit/run.log")" = 1 ]; then
  pass "the healthz sampler counts only samples with the server up, keeps every raw sample in the evidence and prints each line once"
else
  fail "healthz sampler" "$check"
fi

# Catches: a /healthz timeout or a non-200 dropped from the figures or counted
# at its own latency: 4319 hangs past the 2 s client timeout and 4318 answers
# 500, so every sample on both is a failure counted as 2000 ms. /metrics
# hangs past the timeout too, so every scrape is a stall: HEARTBEAT must count
# each as a failure and report max_s 2.000 rather than refuse the phase, drop
# them or call them bad.
hz="$TMP/healthz-fail"
rm -rf "$hz"
mkdir -p "$hz"
touch "$STUB_DIR/server-ready" "$STUB_DIR/health-hang-4319" "$STUB_DIR/health-500-4318" \
  "$STUB_DIR/metrics-hang"
printf 'up 1\n' >"$hz/gate"
python3 "$KIT_SRC/lib/kit.py" healthz-sampler "$hz" "$hz/gate" "$hz/ack" \
  "$KIT_HEALTH_BASE_4318" "$KIT_HEALTH_BASE_4319" 2>"$hz/sampler.log" &
hz_pid=$!
for _ in $(seq 1 1000); do [ -s "$hz/ack" ] && break; sleep 0.01; done
sleep 2.5
printf 'down 1\n' >"$hz/gate"
kill "$hz_pid" 2>/dev/null
wait "$hz_pid" 2>/dev/null
rm -f "$STUB_DIR/server-ready" "$STUB_DIR/health-hang-4319" "$STUB_DIR/health-500-4318" \
  "$STUB_DIR/metrics-hang"
code=0
python3 "$KIT_SRC/lib/kit.py" healthz-summary benchmark "$hz" >"$hz/summary" 2>&1 || code=$?
n_hb="$(sed -n 's/^HEARTBEAT phase=benchmark n=\([0-9]*\) failures=\([0-9]*\) bad=0 max_s=2.000$/\1 \2/p' "$hz/summary")"
n_4318="$(sed -n 's/^HEALTHZ phase=benchmark port=4318 n=\([0-9]*\) p50_ms=2000.000 p99_ms=2000.000 max_ms=2000.000 failures=\([0-9]*\)$/\1 \2/p' "$hz/summary")"
n_4319="$(sed -n 's/^HEALTHZ phase=benchmark port=4319 n=\([0-9]*\) p50_ms=2000.000 p99_ms=2000.000 max_ms=2000.000 failures=\([0-9]*\)$/\1 \2/p' "$hz/summary")"
raw_4319="$(python3 -c 'import json, sys; r = [json.loads(l) for l in open(sys.argv[1])]; print(min(x["ms"] for x in r) >= 1900, all(x["error"].startswith("timeout") or "timed out" in x["error"] for x in r))' "$hz/4319.jsonl" 2>&1)"
if [ "$code" = 0 ] && [ -s "$hz/ack" ] && [ -n "$n_4318" ] && [ "${n_4318% *}" = "${n_4318#* }" ] && [ "${n_4318% *}" -ge 4 ] \
  && [ -n "$n_4319" ] && [ "${n_4319% *}" = "${n_4319#* }" ] && [ "${n_4319% *}" -ge 1 ] \
  && [ "$raw_4319" = "True True" ] \
  && [ -n "$n_hb" ] && [ "${n_hb% *}" = "${n_hb#* }" ] && [ "${n_hb% *}" -ge 1 ]; then
  pass "a /healthz timeout or non-200 counts as a 2000 ms sample and a failure, and an unanswered /metrics scrape as a 2 s heartbeat age and a failure"
else
  fail "healthz timeout" "exit $code, 4318 '$n_4318', 4319 '$n_4319', raw '$raw_4319', heartbeat '$n_hb': $(cat "$hz/summary") $(head -c 300 "$hz/sampler.log")"
fi

# Catches: a /metrics endpoint that answers fast but unusably read as a 2 s
# stall on every scrape, so a misconfigured endpoint passes every phase with
# max_s 2.000: /metrics answers 200 without the heartbeat age, and the phase
# is refused, naming the reason. The same raw scrapes pin the 1 s cadence:
# consecutive scrapes of one generation sit at least 0.95 s apart on the
# monotonic clock (load only widens the gap), not at /healthz's 250 ms.
hz="$TMP/healthz-unusable"
rm -rf "$hz"
mkdir -p "$hz"
touch "$STUB_DIR/server-ready" "$STUB_DIR/heartbeat-absent"
printf 'up 1\n' >"$hz/gate"
python3 "$KIT_SRC/lib/kit.py" healthz-sampler "$hz" "$hz/gate" "$hz/ack" \
  "$KIT_HEALTH_BASE_4318" "$KIT_HEALTH_BASE_4319" 2>"$hz/sampler.log" &
hz_pid=$!
for _ in $(seq 1 1000); do [ -s "$hz/ack" ] && break; sleep 0.01; done
sleep 2.5
printf 'down 1\n' >"$hz/gate"
kill "$hz_pid" 2>/dev/null
wait "$hz_pid" 2>/dev/null
rm -f "$STUB_DIR/server-ready" "$STUB_DIR/heartbeat-absent"
code=0
python3 "$KIT_SRC/lib/kit.py" healthz-summary benchmark "$hz" >"$hz/summary" 2>"$hz/summary.err" || code=$?
gaps="$(python3 -c 'import json, sys
t = [r["mono"] for r in map(json.loads, open(sys.argv[1])) if r["gen"] == 1]
g = [b - a for a, b in zip(t, t[1:])]
print(len(g) >= 1 and min(g) >= 0.95, [round(x, 3) for x in g])' "$hz/metrics.jsonl" 2>&1)"
if [ "$code" = 2 ] && [ ! -s "$hz/summary" ] \
  && grep -qF "every answered /metrics scrape for phase benchmark in $hz was unusable (no ravel_health_heartbeat_age_seconds in /metrics)" "$hz/summary.err" \
  && [ "${gaps%% *}" = True ]; then
  pass "a phase whose every answered /metrics scrape lacks the heartbeat age is refused, naming why, and /metrics is scraped every 1 s on the monotonic clock"
else
  fail "healthz unusable metrics" "exit $code, metrics gaps '$gaps': $(cat "$hz/summary" "$hz/summary.err") $(head -c 300 "$hz/sampler.log")"
fi

# Catches: an answered but unusable scrape (a 404, a 401, a 200 without the
# metric) counted as a stall or left out of the HEARTBEAT line, or a phase
# whose answered scrapes were all unusable passing beside a stall.
hz_synthetic() {
  local d="$TMP/healthz-syn-$1"
  shift
  rm -rf "$d"
  mkdir -p "$d"
  for port in 4318 4319; do
    echo '{"gated": true, "ok": true, "ms": 1.0}' >"$d/$port.jsonl"
  done
  printf '%s\n' "$@" >"$d/metrics.jsonl"
  code=0
  python3 "$KIT_SRC/lib/kit.py" healthz-summary benchmark "$d" >"$d/summary" 2>"$d/summary.err" || code=$?
}
s_ok='{"gated": true, "ok": true, "answered": true, "status": 200, "error": "", "heartbeat_s": 0.7}'
s_ok2='{"gated": true, "ok": true, "answered": true, "status": 200, "error": "", "heartbeat_s": 0.4}'
s_404='{"gated": true, "ok": false, "answered": true, "status": 404, "error": "HTTP 404"}'
s_401='{"gated": true, "ok": false, "answered": true, "status": 401, "error": "HTTP 401"}'
s_stall='{"gated": true, "ok": false, "answered": false, "status": 0, "error": "TimeoutError: timed out"}'
hz_synthetic mixed "$s_ok" "$s_401" "$s_stall"
mixed="$code $(grep '^HEARTBEAT' "$TMP/healthz-syn-mixed/summary")"
hz_synthetic usable "$s_ok" "$s_ok2" "$s_404"
usable="$code $(grep '^HEARTBEAT' "$TMP/healthz-syn-usable/summary")"
hz_synthetic allbad "$s_404" "$s_401" "$s_stall"
allbad="$code $(cat "$TMP/healthz-syn-allbad/summary.err")"
if [ "$mixed" = "0 HEARTBEAT phase=benchmark n=3 failures=1 bad=1 max_s=2.000" ] \
  && [ "$usable" = "0 HEARTBEAT phase=benchmark n=3 failures=0 bad=1 max_s=0.700" ] \
  && [ "$allbad" = "2 kit: healthz-summary: every answered /metrics scrape for phase benchmark in $TMP/healthz-syn-allbad was unusable (HTTP 401; HTTP 404), so the heartbeat age was never read" ]; then
  pass "an answered but unusable /metrics scrape counts as bad, not as a stall, and a phase with no usable scrape among its answers is refused"
else
  fail "healthz bad scrapes" "mixed '$mixed', usable '$usable', allbad '$allbad'"
fi

# Catches: a request body without an explicit window (the server's default is
# the last hour, which would hide the load), a window other than 200000 h, a
# window that differs between statements or from the provenance stamp, or a
# body sending a statement other than the corpus's. The row-count check's one
# body must send the corpus's COUNT(*) over the same window.
window_ok="$(python3 - "$STUB_DIR/sql-bodies" "$OUT_A/results.json" "$REPO_ROOT/benchmarks/clickbench/hits.corpus.json" \
  "$STUB_DIR/verify-bodies" <<'PY'
import json, sys
bodies = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
w = json.load(open(sys.argv[2]))["provenance"]["query_window"]
sqls = [e["sql"] for e in json.load(open(sys.argv[3]))["entries"]]
ok = len(bodies) == 129
ok = ok and all(sorted(b) == ["end", "query", "start"] and b["start"] == w["start"]
                and b["end"] == w["end"] and b["end"] - b["start"] == 200000 * 3600 for b in bodies)
ok = ok and [b["query"] for b in bodies[::3]] == sqls
try:
    verify = [json.loads(l) for l in open(sys.argv[4]) if l.strip()]
except OSError:
    verify = []
ok = ok and verify == [{"query": "SELECT COUNT(*) FROM logs", "start": w["start"], "end": w["end"]}]
print("yes" if ok else "no")
PY
)"
if [ "$window_ok" = yes ]; then
  pass "every request body, the row-count check's included, carries one start and end 200000 hours apart"
else
  fail "query window" "$(head -c 300 "$STUB_DIR/sql-bodies"); verify: $(head -c 200 "$STUB_DIR/verify-bodies" 2>/dev/null)"
fi

# Catches: the kit's results.json no longer reading in the runbook's analyse.py
# (section 9) unchanged: a duplicate or missing q row, a band row with a q id,
# or hot and cold figures other than the kit's sums. With no executor
# accounting over HTTP, its cost-class bands fail and nothing else may.
sed -n '/^```python$/,/^```$/p' "$REPO_ROOT/docs/internal/clickbench-aws-runbook.md" | sed '1d;$d' >"$TMP/analyse.py"
code=0
python3 "$TMP/analyse.py" "$OUT_A/results.json" "$REPO_ROOT/benchmarks/clickbench/hits.corpus.json" \
  >"$TMP/analyse.out" 2>&1 || code=$?
other="$(grep '^  - ' "$TMP/analyse.out" | grep -vc ': no per_run_accounting; cold cost not emitted$')"
if [ "$code" = 3 ] && grep -q '^measured 43  failed 0 \[\]$' "$TMP/analyse.out" \
  && grep -q '^hot  21.50 s$' "$TMP/analyse.out" && grep -q '^cold 21.50 s$' "$TMP/analyse.out" \
  && grep -q '^  - q01: no per_run_accounting; cold cost not emitted$' "$TMP/analyse.out" && [ "$other" = 0 ]; then
  pass "analyse.py reads results.json unchanged: 43 measured, sums match, only the cost bands fail"
else
  fail "analyse.py" "exit $code, $other other failures: $(head -c 400 "$TMP/analyse.out")"
fi

# Catches: a call record losing one of its fields (rows included), or a
# statement reaching the SQL endpoint without run_query (the curl count would
# exceed the wrapper's).
calls="$BOX_A/kit/out/benchmark/calls.jsonl"
fields_ok="$(python3 - "$calls" <<'PY'
import json, sys
need = ("statement", "number", "arm", "phase", "try", "start", "duration_s", "http_status", "rows",
        "outcome", "refusal_class", "error")
rows = [json.loads(l) for l in open(sys.argv[1])]
ok = len(rows) == 129 and all(all(k in r for k in need) and r["arm"] == "A" and r["phase"] == "benchmark"
                              and r["outcome"] == "ok" and r["http_status"] == 200 for r in rows)
ok = ok and [r["rows"] for r in rows if r["number"] == 1] == [1, 1, 1]
ok = ok and [r["rows"] for r in rows if r["number"] == 8] == [3, 3, 3]
print("yes" if ok else "no")
PY
)"
if [ -d "$BOX_A/kit/out/benchmark" ] && [ "$fields_ok" = yes ] \
  && [ "$(wc -l <"$STUB_DIR/run_query-calls" | tr -d ' ')" = 129 ] \
  && [ "$(wc -l <"$STUB_DIR/curl-sql" | tr -d ' ')" = 129 ]; then
  pass "the benchmark phase writes out/benchmark/ and every one of its 129 calls goes through run_query"
else
  fail "run_query" "fields $fields_ok, run_query $(wc -l <"$STUB_DIR/run_query-calls" 2>/dev/null), curl $(wc -l <"$STUB_DIR/curl-sql" 2>/dev/null)"
fi

# Catches: a server start that does not take its flags from arm.env (the
# row-count check's start and all 43 restarts must run the same argv), or cold
# tries without a cache drop.
if [ "$(wc -l <"$STUB_DIR/server-argv" | tr -d ' ')" = 44 ] && [ "$(sort -u "$STUB_DIR/server-argv" | wc -l | tr -d ' ')" = 1 ] \
  && grep -q -- '--memory-budget-bytes 25769803776 --audit-mode off' "$STUB_DIR/server-argv" \
  && [ "$(cat "$BOX_A/drop_caches")" = 3 ]; then
  pass "the row-count check's start and each of the 43 cold starts run the same server flags"
else
  fail "server starts" "$(sort "$STUB_DIR/server-argv" | uniq -c | head -n 3)"
fi

# Catches: a hot try run after a cache drop, a cold try without one, or a drop
# missing or doubled: the run must be exactly one drop then tries 1, 2 and 3,
# 43 times over.
want_seq="$(for ((i = 0; i < 43; i++)); do printf 'drop try1 try2 try3 '; done)"
got_seq="$(tr '\n' ' ' <"$STUB_DIR/sequence")"
if [ "$got_seq" = "$want_seq" ]; then
  pass "one cache drop before each cold try and none before a hot try"
else
  fail "drop sequence" "$(head -c 200 <<<"$got_seq")"
fi

# Catches: a step that runs without a UTC stamp, or the phases running before
# the fold precondition and the row-count check. The uploaded copy ends at
# evidence: upload is stamped after it is sent.
order="$(sed -n 's/^[0-9]\{4\}-[0-9][0-9]-[0-9][0-9]T[0-9:]*Z step=\([a-z-]*\) state=done$/\1/p' "$OUT_A/progress.log" | tr '\n' ' ')"
unstamped="$(grep -vc '^[0-9]\{4\}-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z ' "$OUT_A/progress.log")"
case "$order" in
  *"objects-before-load load objects-after-load compact fold declare verify phase-benchmark healthz burst-end results evidence "*)
    if [ "$unstamped" = 0 ]; then
      pass "progress.log stamps every step in UTC, fold and the row-count check before the phases"
    else
      fail "progress.log" "$unstamped unstamped lines"
    fi ;;
  *) fail "progress.log" "step order: $order" ;;
esac

# Catches: evidence.tgz missing the server log, the samplers or the object
# counts, or any box secret (RustFS keys, tenant token) leaving the box. The
# stub RustFS logs both of its keys into rustfs.log and run.log, and the stub
# server into its own log, so an evidence step that only leaves secrets.env
# out, or redacts one log, still uploads them. The audit token key, generated
# into secrets.env on every box, must not reach a server running audit_mode
# off (box-a's), which would report it at startup.
mkdir -p "$TMP/evidence-a"
tar -xzf "$OUT_A/evidence.tgz" -C "$TMP/evidence-a"
token="$(sed -n 's/^KIT_TENANT_TOKEN=//p' "$BOX_A/kit/secrets.env")"
secret="$(sed -n 's/^RUSTFS_SECRET_KEY=//p' "$BOX_A/kit/secrets.env")"
access="$(sed -n 's/^RUSTFS_ACCESS_KEY=//p' "$BOX_A/kit/secrets.env")"
audit_off_ok=0
grep -q '^RAVEL_AUDIT_TOKEN_KEY=[0-9a-f]\{64\}$' "$BOX_A/kit/secrets.env" \
  && grep -q 'audit_key=none$' "$BOX_A/kit/server-1.log" \
  && ! grep -q 'audit_key=[0-9a-f]' "$BOX_A/kit/server-"*.log && audit_off_ok=1
if [ "$audit_off_ok" = 1 ] && [ -f "$TMP/evidence-a/server-1.log" ] && [ -f "$TMP/evidence-a/object-counts/before-load.txt" ] \
  && [ -f "$TMP/evidence-a/object-counts/after-load.txt" ] && [ -f "$TMP/evidence-a/out/benchmark/samplers/server-rss.log" ] \
  && [ -f "$TMP/evidence-a/arm.env" ] && [ -n "$token" ] && [ -n "$secret" ] && [ -n "$access" ] \
  && [ -s "$TMP/evidence-a/out/benchmark/healthz/4318.jsonl" ] && [ -s "$TMP/evidence-a/out/benchmark/healthz/4319.jsonl" ] \
  && [ -s "$TMP/evidence-a/out/benchmark/healthz/metrics.jsonl" ] && [ -s "$TMP/evidence-a/out/healthz.txt" ] \
  && grep -qF "$secret" "$BOX_A/kit/rustfs.log" && grep -qF "$secret" "$BOX_A/kit/run.log" \
  && grep -qF "secret_key=$secret" "$BOX_A/kit/server-1.log" \
  && grep -qF 'access_key=[redacted] secret_key=[redacted]' "$TMP/evidence-a/rustfs.log" \
  && grep -qF 'access_key=[redacted] secret_key=[redacted]' "$TMP/evidence-a/run.log" \
  && grep -qF 'access_key=[redacted] secret_key=[redacted]' "$TMP/evidence-a/server-1.log" \
  && ! grep -rqF -e "$token" -e "$secret" -e "$access" "$TMP/evidence-a" "$OUT_A" \
  && [ ! -e "$TMP/evidence-a/secrets.env" ] && [ ! -e "$TMP/evidence-a/auth.header" ]; then
  pass "evidence.tgz holds server logs, samplers, raw healthz samples and object counts, no box secret is uploaded, and an audit_mode off server gets no audit key"
else
  fail "evidence" "audit key kept from an audit_mode off server: $audit_off_ok; $(tar -tzf "$OUT_A/evidence.tgz" | head -n 20 | tr '\n' ' ')"
fi
mv "$OUT_A" "$TMP/fixture-pass"

# Catches: an absolute band miss that the box does not record (kit.py
# absolute must exit 1 and DONE must say bands=miss), or one turned into a
# FAILED run.
code=0
run_box A box-miss STUB_SQL_TIME=2.0 || code=$?
abs=0
python3 "$KIT_SRC/lib/kit.py" absolute A "$OUT_A/results.json" >"$TMP/abs.out" 2>&1 || abs=$?
if [ "$code" = 0 ] && grep -q '^DONE bands=miss ' "$OUT_A/DONE" 2>/dev/null && [ "$abs" = 1 ] \
  && grep -q '^MISS  *A  *hot_sum_s <= hot_sum_s_max  *86.000  *30$' "$TMP/abs.out" \
  && grep -q '^MISS  *A  *q01 hot_s <= hot_s_max.q01  *2.000  *1.000$' "$TMP/abs.out"; then
  pass "an absolute band miss exits 1 on the box and DONE records bands=miss"
else
  fail "absolute miss" "box exit $code, absolute exit $abs, DONE: $(cat "$OUT_A/DONE" 2>/dev/null)"
fi
mv "$OUT_A" "$TMP/fixture-miss"

# Catches: a wrong answer read as a pass. Statement 2 (corpus expected_rows 1)
# answers 5 rows on every try and statement 19 is refused: both must land in
# `failed` with their class, count as errors in the phases and pooled error
# figures, and miss their absolute statement band as not answered.
code=0
run_box A box-errors STUB_SQL_REFUSE_N=19 STUB_SQL_ROWS_N=02 STUB_SQL_ROWS=5 || code=$?
check="$(python3 - "$OUT_A/results.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
bad = []
def want(cond, what):
    if not cond:
        bad.append(what)
failed = {f["id"]: f for f in r["failed"]}
want(sorted(failed) == ["q02_count_adv_engine", "q19_user_minute_phrase"], "failed ids %s" % sorted(failed))
q2 = failed.get("q02_count_adv_engine", {})
want(q2.get("run") == 0 and "wrong_answer" in q2.get("error", "")
     and "answer has 5 rows, expected 1" in q2.get("error", ""), "q02 error %r" % q2.get("error"))
want(q2.get("answer") == {"answered": False, "rows": 5, "expected_rows": 1, "match": False},
     "q02 answer %r" % q2.get("answer"))
q19 = failed.get("q19_user_minute_phrase", {})
want(q19.get("run") == 0 and "resource_exhausted" in q19.get("error", ""), "q19 error")
want(q19.get("answer", {}).get("answered") is False, "q19 answer")
want(len(r["entries"]) == 41 and r["totals"]["answered"] == 41 and r["totals"]["hot_sum_s"] == 20.5, "totals")
want(r["phases"]["benchmark"]["errors"] == 6 and r["phases"]["benchmark"]["refusal_classes"]
     == {"resource_exhausted": 3, "wrong_answer": 3}, "phases %r" % r["phases"]["benchmark"])
want(r["error_pooled"] == {"calls": 129, "errors": 6, "ratio": 6 / 129, "phases": ["benchmark"]},
     "error_pooled")
rows = {b["band"]: b for b in r["bands"]}
for q in ("q02", "q19"):
    b = rows.get("%s hot_s <= hot_s_max.%s" % (q, q), {})
    want(b.get("verdict") == "miss" and b.get("note") == "not answered", q + " band")
want(rows.get("error_pooled <= error_pooled_max", {}).get("verdict") == "pass", "error band")
print(",".join(bad))
PY
)" || check="python failed"
if [ "$code" = 0 ] && grep -q '^DONE bands=miss ' "$OUT_A/DONE" 2>/dev/null && [ -z "$check" ]; then
  pass "a wrong row count and a refusal are errors: failed, counted, and absolute misses"
else
  fail "wrong answer" "box exit $code: $check"
fi
mv "$OUT_A" "$TMP/fixture-errors"

# Catches: an unchecked fold precondition: a fold that sealed nothing must
# fail the run at step fold, before any statement runs.
code=0
run_box A box-fold STUB_FOLD=unsealed || code=$?
if [ "$code" = 1 ] && grep -q '^FAILED step=fold ' "$OUT_A/FAILED" 2>/dev/null && [ ! -e "$OUT_A/DONE" ] \
  && [ ! -e "$TMP/box-fold/kit/out/benchmark/calls.jsonl" ] && [ -f "$OUT_A/progress.log" ] \
  && grep -q 'ec2 terminate-instances' "$STUB_LOG"; then
  pass "a fold that sealed nothing fails the run as FAILED step=fold"
else
  fail "fold precondition" "exit $code, FAILED: $(head -n 1 "$OUT_A/FAILED" 2>/dev/null)"
fi

# Catches: FAILED carrying run.log's tail unredacted. The stub RustFS printed
# its keys into run.log, inside the 20 lines FAILED quotes.
secret="$(sed -n 's/^RUSTFS_SECRET_KEY=//p' "$TMP/box-fold/kit/secrets.env")"
access="$(sed -n 's/^RUSTFS_ACCESS_KEY=//p' "$TMP/box-fold/kit/secrets.env")"
if [ -n "$secret" ] && grep -qF "$secret" "$TMP/box-fold/kit/run.log" \
  && grep -qF 'access_key=[redacted] secret_key=[redacted]' "$OUT_A/FAILED" 2>/dev/null \
  && grep -q 'fold sealed nothing' "$OUT_A/FAILED" \
  && ! grep -rqF -e "$secret" -e "$access" "$OUT_A"; then
  pass "FAILED quotes run.log's tail with every box secret redacted"
else
  fail "FAILED redaction" "$(grep -c "${secret:-none}" "$OUT_A/FAILED" 2>/dev/null) secret line(s) in FAILED: $(head -c 300 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: a key that survives redaction uploaded anyway. With redaction
# replacing nothing, evidence.tgz must not be written and FAILED must withhold
# run.log's tail rather than quote the keys in it.
code=0
run_box A box-noredact DRIVER_NO_REDACT=1 || code=$?
secret="$(sed -n 's/^RUSTFS_SECRET_KEY=//p' "$TMP/box-noredact/kit/secrets.env")"
if [ "$code" = 1 ] && grep -q '^FAILED step=evidence ' "$OUT_A/FAILED" 2>/dev/null \
  && grep -q '^run.log tail withheld: a box secret survived redaction$' "$OUT_A/FAILED" \
  && [ ! -e "$OUT_A/evidence.tgz" ] && [ ! -e "$TMP/box-noredact/kit/evidence.tgz" ] \
  && grep -q 'evidence.tgz not written' "$TMP/box-noredact/kit/run.log" \
  && [ -n "$secret" ] && ! grep -rqF -e "$secret" "$OUT_A"; then
  pass "a key surviving redaction refuses evidence.tgz and withholds FAILED's run.log tail"
else
  fail "redaction refused" "exit $code, evidence $(ls "$OUT_A" 2>/dev/null | tr '\n' ' '), FAILED: $(head -c 300 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: the audit key generated outside secrets.env (so neither redaction
# nor a restart sees it), or not handed to a server running audit_mode
# required with redacted text.
code=0
BOX_TOML_SED='s/^audit_mode = .*/audit_mode = "required"/' run_box A box-audit || code=$?
akey="$(sed -n 's/^RAVEL_AUDIT_TOKEN_KEY=//p' "$TMP/box-audit/kit/secrets.env" 2>/dev/null)"
mkdir -p "$TMP/evidence-audit"
tar -xzf "$OUT_A/evidence.tgz" -C "$TMP/evidence-audit" 2>/dev/null
if [ "$code" = 0 ] && [[ "$akey" =~ ^[0-9a-f]{64}$ ]] \
  && grep -q -- '--audit-mode required --audit-text redacted' "$STUB_DIR/server-argv" \
  && grep -qF "audit_key=$akey" "$TMP/box-audit/kit/server-1.log" \
  && [ "$(grep -c "audit_key=$akey" "$TMP/box-audit/kit/server-44.log")" = 1 ] \
  && grep -qF 'audit_key=[redacted]' "$TMP/evidence-audit/server-1.log" \
  && ! grep -rqF -e "$akey" "$TMP/evidence-audit" "$OUT_A"; then
  pass "audit_mode required runs every server with the audit key from secrets.env, redacted from the evidence"
else
  fail "audit key" "exit $code, key '$akey': $(grep -h audit_key "$TMP/box-audit/kit/server-1.log" 2>/dev/null | head -c 200)"
fi
rm -rf "$OUT_A"

# Catches: a fold whose entry_count is far under the objects the load wrote
# (50 of 100, under the 0.8 ratio) read as met because it sealed an hour.
code=0
run_box A box-foldratio STUB_FOLD_ENTRIES=50 || code=$?
fold_stamp="$(python3 -c 'import json, sys; f = json.load(open(sys.argv[1])); print(f["precondition"], f["entry_count"], f["objects_written_by_load"])' \
  "$TMP/box-foldratio/kit/stamps/fold" 2>/dev/null)"
if [ "$code" = 1 ] && grep -q '^FAILED step=fold ' "$OUT_A/FAILED" 2>/dev/null && [ ! -e "$OUT_A/DONE" ] \
  && grep -q 'fold entry_count 50 is under 0.8 of the 100 objects the load wrote' "$OUT_A/FAILED" \
  && [ "$fold_stamp" = "failed 50 100" ] && [ ! -e "$TMP/box-foldratio/kit/out/benchmark/calls.jsonl" ]; then
  pass "a fold with too few entries for the objects written fails as FAILED step=fold"
else
  fail "fold entry ratio" "exit $code, stamp '$fold_stamp', FAILED: $(head -n 3 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: the fold judged against the bucket's key delta, which also counts
# one commit record per data object: arm A's load added 200 keys for its 100
# objects written, and a fold of 100 entries covers all of them.
fold_stamp="$(python3 -c 'import json, sys; f = json.load(open(sys.argv[1])); print(f["precondition"], f["entry_count"], f["objects_written_by_load"], f["store_keys_added_by_load"])' \
  "$BOX_A/kit/stamps/fold" 2>/dev/null)"
if [ "$fold_stamp" = "met 100 100 200" ]; then
  pass "a fold covering the load's objects written is met though the bucket gained twice as many keys"
else
  fail "fold against objects written" "stamp '$fold_stamp', run.log: $(grep -h 'fold entry_count' "$BOX_A/kit/run.log" | head -n 1)"
fi

# Catches: a load report without its "objects written" figure passing, so
# the fold check has no denominator.
code=0
run_box A box-noobjects STUB_LOAD_OBJECTS=none || code=$?
if [ "$code" = 1 ] && grep -q '^FAILED step=load ' "$OUT_A/FAILED" 2>/dev/null && [ ! -e "$OUT_A/DONE" ] \
  && grep -q 'load.out carries objects written 0 times, not once' "$OUT_A/FAILED" \
  && [ ! -e "$TMP/box-noobjects/kit/stamps/fold" ]; then
  pass "a load report without objects written fails as FAILED step=load, before the fold"
else
  fail "no objects written" "exit $code, FAILED: $(head -n 3 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: a RustFS binary that unpacks and installs but cannot run (the gnu
# build on an older glibc) left to fail later as a store that never answers.
printf '#!/bin/sh\necho "rustfs: /lib64/libc.so.6: version GLIBC_2.38 not found (required by rustfs)" >&2\nexit 1\n' \
  >"$STUB_DIR/rustfs-asset"
asset_sha="$(sha256sum "$STUB_DIR/rustfs-asset" | cut -d' ' -f1)"
code=0
BOX_TOML_SED="s|^rustfs_url = .*|rustfs_url = \"https://rustfs.invalid/rustfs\"|;s|^rustfs_sha256 = .*|rustfs_sha256 = \"$asset_sha\"|" \
  run_box A box-rustfsver DRIVER_REAL_RUSTFS=1 || code=$?
if [ "$code" = 1 ] && grep -q '^FAILED step=rustfs ' "$OUT_A/FAILED" 2>/dev/null && [ ! -e "$OUT_A/DONE" ] \
  && grep -qF 'rustfs --version failed: rustfs: /lib64/libc.so.6: version GLIBC_2.38 not found (required by rustfs)' "$OUT_A/FAILED" \
  && ! grep -q '^nohup .*/rustfs ' "$STUB_LOG"; then
  pass "a rustfs binary whose --version fails fails as FAILED step=rustfs, quoting its output"
else
  fail "rustfs --version" "exit $code, FAILED: $(head -n 3 "$OUT_A/FAILED" 2>/dev/null); $(grep '^nohup' "$STUB_LOG")"
fi
rm -rf "$OUT_A"

# Catches: an arm's server_flags_a not reaching every server start after the
# kit's own flags, or not stamped apart from them.
code=0
BOX_TOML_SED='s/^shards = /server_flags_a = ["--fetch-concurrency", "64"]\n&/' \
  run_box A box-flags || code=$?
starts="$(wc -l <"$STUB_DIR/server-argv" | tr -d ' ')"
tail_ok="$(grep -c -- '--audit-mode off --fetch-concurrency 64$' "$STUB_DIR/server-argv")"
sf="$(python3 -c 'import json, sys; f = json.load(open(sys.argv[1]))["provenance"]["server_flags"]; print(f["extra_flags"], f["base_flags"][-2:], f["flags"].endswith("--audit-mode off --fetch-concurrency 64"))' \
  "$OUT_A/results.json" 2>/dev/null)"
if [ "$code" = 0 ] && [ "$starts" -ge 43 ] && [ "$tail_ok" = "$starts" ] \
  && [ "$sf" = "['--fetch-concurrency', '64'] ['--audit-mode', 'off'] True" ]; then
  pass "server_flags_a follows the kit's flags on every server start and is stamped as extra_flags"
else
  fail "server_flags_a" "exit $code, $tail_ok of $starts starts, stamp '$sf': $(tail -n 1 "$STUB_DIR/server-argv" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: a healthz.txt with a phase's line twice or missing taken as the
# phase's figures: each must fail the box at step healthz, naming the line.
for edit in "dup|HEALTHZ phase=benchmark port=4318 2 times" "drop|HEARTBEAT phase=benchmark 0 times"; do
  code=0
  run_box A "box-healthz-${edit%%|*}" DRIVER_HEALTHZ_EDIT="${edit%%|*}" || code=$?
  if [ "$code" = 1 ] && grep -q '^FAILED step=healthz ' "$OUT_A/FAILED" 2>/dev/null && [ ! -e "$OUT_A/DONE" ] \
    && grep -qF "must hold each line exactly once; it holds ${edit#*|}" "$OUT_A/FAILED"; then
    pass "healthz.txt edited (${edit%%|*}) fails as FAILED step=healthz, naming the line"
  else
    fail "healthz ${edit%%|*}" "exit $code, FAILED: $(head -n 3 "$OUT_A/FAILED" 2>/dev/null)"
  fi
  rm -rf "$OUT_A"
done

# Catches: healthz_phases ignored, so a run that leaves the benchmark phase
# unsampled still runs the sampler beside its statements. Nothing is sampled,
# healthz.txt stays empty and results.json's healthz block is null.
code=0
BOX_TOML_SED='s/^shards = /healthz_phases = ["concurrent"]\n&/' \
  run_box A box-healthz-off || code=$?
hz_off="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["healthz"])' "$OUT_A/results.json" 2>&1)"
if [ "$code" = 0 ] && grep -q '^DONE ' "$OUT_A/DONE" 2>/dev/null && [ "$hz_off" = None ] \
  && [ ! -e "$TMP/box-healthz-off/kit/out/benchmark/healthz" ] && [ ! -s "$TMP/box-healthz-off/kit/out/healthz.txt" ] \
  && ! grep -q '^HEALTHZ ' "$TMP/box-healthz-off/kit/run.log"; then
  pass "healthz_phases without benchmark leaves the benchmark phase unsampled and the healthz block null"
else
  fail "healthz_phases" "exit $code, healthz '$hz_off': $(head -n 3 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: healthz-check passing a line for a phase that was not sampled.
printf '%s\n' \
  'HEALTHZ phase=benchmark port=4318 n=8 p50_ms=1.0 p99_ms=2.0 max_ms=2.0 failures=0' \
  'HEALTHZ phase=benchmark port=4319 n=8 p50_ms=1.0 p99_ms=2.0 max_ms=2.0 failures=0' \
  'HEARTBEAT phase=benchmark n=4 failures=1 bad=2 max_s=2.000' >"$TMP/healthz-extra.txt"
code=0
python3 "$KIT_SRC/lib/kit.py" healthz-check "$TMP/healthz-extra.txt" benchmark \
  >"$TMP/healthz-extra.json" 2>"$TMP/healthz-extra.err" || code=$?
hb="$(python3 -c 'import json, sys; b = json.load(open(sys.argv[1]))["phases"]["benchmark"]; print(b["heartbeat_n"], b["heartbeat_failures"], b["heartbeat_bad"], b["heartbeat_max_s"])' "$TMP/healthz-extra.json" 2>&1)"
[ "$code" = 0 ] && [ "$hb" = "4 1 2 2.0" ] \
  || fail "healthz extra line control" "exit $code, heartbeat '$hb': $(head -c 300 "$TMP/healthz-extra.err")"
echo 'HEARTBEAT phase=concurrent-1 n=4 failures=0 bad=0 max_s=0.500' >>"$TMP/healthz-extra.txt"
code=0
python3 "$KIT_SRC/lib/kit.py" healthz-check "$TMP/healthz-extra.txt" benchmark \
  >/dev/null 2>"$TMP/healthz-extra.err" || code=$?
if [ "$code" = 2 ] \
  && grep -qF "healthz-extra.txt:4 is not an expected line: 'HEARTBEAT phase=concurrent-1 n=4 failures=0 bad=0 max_s=0.500'" "$TMP/healthz-extra.err"; then
  pass "healthz-check refuses a line for a phase it was not asked for, naming file and line"
else
  fail "healthz extra line" "exit $code: $(head -c 300 "$TMP/healthz-extra.err")"
fi

# Catches: a HEARTBEAT line with no scrape behind it, more failed and bad
# scrapes than scrapes, no usable scrape beside bad ones, or one in an older
# form (max only, or without bad=), taken as the phase's heartbeat figure.
for bad in 'HEARTBEAT phase=benchmark n=0 failures=0 bad=0 max_s=0.000|counts 0 samples and 0 failures' \
  'HEARTBEAT phase=benchmark n=2 failures=3 bad=0 max_s=2.000|counts 2 samples and 3 failures' \
  'HEARTBEAT phase=benchmark n=4 failures=2 bad=3 max_s=2.000|counts 4 scrapes, 2 failures and 3 bad' \
  'HEARTBEAT phase=benchmark n=4 failures=1 bad=3 max_s=2.000|counts 3 bad scrapes and no usable one' \
  "HEARTBEAT phase=benchmark n=4 failures=1 max_s=2.000|is not an expected line: 'HEARTBEAT phase=benchmark n=4 failures=1 max_s=2.000'" \
  "HEARTBEAT phase=benchmark max_s=0.500|is not an expected line: 'HEARTBEAT phase=benchmark max_s=0.500'"; do
  head -n 2 "$TMP/healthz-extra.txt" >"$TMP/healthz-hb.txt"
  echo "${bad%%|*}" >>"$TMP/healthz-hb.txt"
  code=0
  python3 "$KIT_SRC/lib/kit.py" healthz-check "$TMP/healthz-hb.txt" benchmark \
    >/dev/null 2>"$TMP/healthz-hb.err" || code=$?
  if [ "$code" = 2 ] && grep -qF -- "${bad#*|}" "$TMP/healthz-hb.err"; then
    pass "healthz-check refuses '${bad%%|*}'"
  else
    fail "healthz heartbeat '${bad%%|*}'" "exit $code: $(head -c 300 "$TMP/healthz-hb.err")"
  fi
done

# Catches: a fold field read from the first of several matches: a fold.out
# carrying entry_count twice is not a figure the kit can stamp.
code=0
run_box A box-foldtwice STUB_FOLD=twice || code=$?
if [ "$code" = 1 ] && grep -q '^FAILED step=fold ' "$OUT_A/FAILED" 2>/dev/null \
  && grep -q 'fold.out carries entry_count 2 times, not once' "$OUT_A/FAILED" \
  && [ ! -e "$TMP/box-foldtwice/kit/out/benchmark/calls.jsonl" ]; then
  pass "a fold.out carrying a field twice fails as FAILED step=fold"
else
  fail "fold field twice" "exit $code, FAILED: $(head -n 3 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: a load whose row count is never checked: a load that wrote one row
# short of run.toml's rows_expected must fail the run at step load.
code=0
run_box A box-loadrows STUB_LOAD_ROWS=99997496 || code=$?
if [ "$code" = 1 ] && grep -q '^FAILED step=load ' "$OUT_A/FAILED" 2>/dev/null \
  && grep -q 'load wrote 99997496 rows, run.toml rows_expected is 99997497' "$OUT_A/FAILED" \
  && [ ! -e "$TMP/box-loadrows/kit/out/benchmark/calls.jsonl" ]; then
  pass "a load that wrote other than rows_expected rows fails as FAILED step=load"
else
  fail "load rows" "exit $code, FAILED: $(head -n 3 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: an answer check that never looks at the data the server reads: the
# corpus COUNT(*), sent before the benchmark, answers one row short of
# rows_expected, so the run must fail at step verify before any statement runs.
code=0
run_box A box-verify STUB_COUNT_STAR=99997496 || code=$?
if [ "$code" = 1 ] && grep -q '^FAILED step=verify ' "$OUT_A/FAILED" 2>/dev/null \
  && grep -q 'SELECT COUNT(\*) FROM logs answered 99997496, run.toml rows_expected is 99997497' "$OUT_A/FAILED" \
  && [ ! -e "$TMP/box-verify/kit/out/benchmark/calls.jsonl" ] && [ ! -s "$STUB_DIR/curl-sql" ]; then
  pass "a COUNT(*) other than rows_expected fails as FAILED step=verify before the benchmark"
else
  fail "verify rows" "exit $code, FAILED: $(head -n 3 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: the statement-count check dropped: a run.toml expecting 42
# statements against the 43-statement corpus must fail before any is sent.
code=0
BOX_TOML_SED='1i\
expected_statements = 42' run_box A box-count || code=$?
if [ "$code" = 1 ] && grep -q '^FAILED step=phase-benchmark ' "$OUT_A/FAILED" 2>/dev/null \
  && grep -q 'corpus holds 43 statements, run.toml expects 42' "$OUT_A/FAILED" \
  && [ ! -s "$STUB_DIR/curl-sql" ]; then
  pass "a corpus holding other than expected_statements fails as FAILED step=phase-benchmark"
else
  fail "statement count" "exit $code, FAILED: $(head -n 3 "$OUT_A/FAILED" 2>/dev/null)"
fi
rm -rf "$OUT_A"

# Catches: a hot try checked against nothing when the corpus states no row
# count (q08 answers 3 rows cold and 4 on try 2, which must be a wrong_answer),
# and a hot figure taken over all three tries: with tries of 0.1, 0.6 and 0.4 s
# hot is 0.4 s (the best of tries 2 and 3) and cold 0.1 s.
code=0
run_box A box-tries STUB_SQL_ROWS_N=08 STUB_SQL_ROWS=4 STUB_SQL_ROWS_TRY=2 'STUB_SQL_TIMES=0.1 0.6 0.4' || code=$?
check="$(python3 - "$OUT_A/results.json" "$REPO_ROOT/benchmarks/clickbench/hits.corpus.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
q08 = json.load(open(sys.argv[2]))["entries"][7]["id"]
bad = []
def want(cond, what):
    if not cond:
        bad.append(what)
failed = {f["id"]: f for f in r["failed"]}
want(sorted(failed) == [q08], "failed ids %s" % sorted(failed))
f = failed.get(q08, {})
want(f.get("run") == 1 and "wrong_answer" in f.get("error", "")
     and "answer has 4 rows, expected 3" in f.get("error", ""), "q08 error %r" % f.get("error"))
want(len(r["entries"]) == 42, "42 entries")
for e in r["entries"]:
    want(abs(e["min_ms"] - 400.0) < 1e-6 and abs(e["cold_ms"] - 100.0) < 1e-6
         and abs(e["median_ms"] - 400.0) < 1e-6 and abs(e["max_ms"] - 600.0) < 1e-6,
         "%s timings %r" % (e["id"], [e["min_ms"], e["median_ms"], e["max_ms"], e["cold_ms"]]))
want(abs(r["totals"]["hot_sum_s"] - 16.8) < 1e-6 and abs(r["totals"]["cold_sum_s"] - 4.2) < 1e-6,
     "totals %r" % r["totals"])
print(",".join(bad[:4]))
PY
)" || check="python failed"
if [ "$code" = 0 ] && grep -q '^DONE bands=miss ' "$OUT_A/DONE" 2>/dev/null && [ -z "$check" ]; then
  pass "a hot try with another row count than the cold try is a wrong_answer, and hot is the best of tries 2 and 3"
else
  fail "tries" "box exit $code: $check"
fi
rm -rf "$OUT_A"

# Catches: a run that dies before load_config set REGION and OUT_URI hanging
# on an unset variable or an upload to nowhere instead of shutting down.
BOX_K="$TMP/box-early"
reset_log
boot_box A "$BOX_K"
: >"$STUB_LOG"
cat >"$TMP/early-driver.sh" <<'DRIVER'
#!/usr/bin/env bash
export KIT_ROOT="$1" KIT_RUN_SH_LIB=1 KIT_IMDS=http://imds.invalid
# shellcheck source=/dev/null
. "$KIT_ROOT/kit/kit/box/run.sh"
load_config() { kill -TERM $$; }
main
DRIVER
code=0
bash "$TMP/early-driver.sh" "$BOX_K" >"$TMP/early.log" 2>&1 || code=$?
if [ "$code" = 1 ] && grep -q '^shutdown -h now$' "$STUB_LOG" \
  && grep -q '^FAILED step=config ' "$BOX_K/kit/FAILED" 2>/dev/null \
  && grep -q 'terminate fallback=shutdown reason=no-region' "$BOX_K/kit/progress.log" \
  && ! grep -q '^aws ' "$STUB_LOG" && ! grep -q 'unbound variable' "$TMP/early.log"; then
  pass "a run killed in load_config uploads nothing and still runs shutdown -h now"
else
  fail "early kill" "exit $code; log: $(tr '\n' ';' <"$STUB_LOG" | head -c 300); $(tail -n 3 "$TMP/early.log")"
fi

# Catches: fail_run or finish dropping every trap before its own shutdown, so
# a signal while it writes evidence or uploads ends run.sh with the box still
# up. fail_run here runs from the EXIT trap (load_config is killed) and is
# killed again inside write_evidence; finish is killed inside the DONE upload.
# Each must still shut down, once.
cat >"$TMP/late-driver.sh" <<'DRIVER'
#!/usr/bin/env bash
export KIT_ROOT="$1" KIT_RUN_SH_LIB=1 KIT_IMDS=http://imds.invalid
# shellcheck source=/dev/null
. "$KIT_ROOT/kit/kit/box/run.sh"
case "$2" in
  fail_run)
    load_config() { kill -TERM $$; }
    write_evidence() { kill -TERM $$; }
    main ;;
  finish)
    REGION=us-east-1 OUT_URI=s3://kit-test-bucket/late
    mkdir -p "$KIT"
    aws() { kill -TERM $$; }
    finish ;;
esac
DRIVER
late_ok=1
late_msg=""
for which in fail_run finish; do
  : >"$STUB_LOG"
  code=0
  bash "$TMP/late-driver.sh" "$BOX_K" "$which" >"$TMP/late-$which.log" 2>&1 || code=$?
  n="$(grep -c '^shutdown -h now$' "$STUB_LOG")"
  if [ "$code" = 0 ] || [ "$n" != 1 ]; then
    late_ok=0
    late_msg="$late_msg $which: exit $code, $n shutdown(s);"
  fi
done
if [ "$late_ok" = 1 ]; then
  pass "a signal inside fail_run's evidence step or finish's DONE upload still shuts the box down once"
else
  fail "late kill" "$late_msg $(tail -n 2 "$TMP/late-fail_run.log")"
fi

# Catches: resolve_arm_env rewriting arm.env on a restart, or start_server
# reading live variables instead of arm.env.
BOX_R="$TMP/box-restart"
reset_log
rm -f "$STUB_DIR/server-argv"
boot_box A "$BOX_R"
(
  export KIT_ROOT="$BOX_R" KIT_RUN_SH_LIB=1 KIT_READY_TRIES=100 KIT_READY_INTERVAL=0.02
  # shellcheck source=/dev/null
  . "$BOX_R/kit/kit/box/run.sh"
  mkdir -p "$STAMPS"
  load_config || exit 1
  bootstrap_build() { mkdir -p "$BIN_DIR"; cp "$BINSTUBS/ravel-server" "$BIN_DIR/ravel-server-$SHA"; }
  bootstrap_build
  resolve_arm_env || exit 1
  start_server || exit 1
  stop_server
  MEMORY_BUDGET=1
  SHARDS=99
  resolve_arm_env || exit 1
  start_server || exit 1
  stop_server
) >"$TMP/restart.log" 2>&1
code=$?
if [ "$code" = 0 ] && [ "$(wc -l <"$STUB_DIR/server-argv" | tr -d ' ')" = 2 ] \
  && [ "$(sort -u "$STUB_DIR/server-argv" | wc -l | tr -d ' ')" = 1 ] \
  && grep -q -- '--shards 8 .*--memory-budget-bytes 25769803776' "$STUB_DIR/server-argv" \
  && grep -q 'arm.env kept' "$BOX_R/kit/progress.log"; then
  pass "arm.env survives a restart of the start step: both starts run the first flags"
else
  fail "arm.env" "exit $code; $(tail -n 3 "$TMP/restart.log"); argv: $(sort -u "$STUB_DIR/server-argv" 2>/dev/null)"
fi

# Catches: a phase loop that ignores the phase list, runs phases out of order,
# or does not give each phase its out/<phase>/ directory.
BOX_P="$TMP/box-phases"
boot_box A "$BOX_P"
(
  export KIT_ROOT="$BOX_P" KIT_RUN_SH_LIB=1
  # shellcheck source=/dev/null
  . "$BOX_P/kit/kit/box/run.sh"
  ARM=A
  QUERY_TIMEOUT_S=5
  echo '{"query": "SELECT 1"}' >"$TMP/q.json"
  phase_alpha() { [ -d "$OUT/alpha" ] && run_query alpha 01 alpha_q 1 "$TMP/q.json"; }
  phase_beta() { [ -d "$OUT/beta" ] && run_query beta 01 beta_q 1 "$TMP/q.json"; }
  PHASES="beta alpha"
  run_phases
) >"$TMP/phases.log" 2>&1
code=$?
order="$(sed -n 's/.* step=\(phase-[a-z]*\) state=done$/\1/p' "$BOX_P/kit/progress.log" | tr '\n' ' ')"
if [ "$code" = 0 ] && [ "$order" = "phase-beta phase-alpha " ] \
  && grep -q '"phase": "alpha"' "$BOX_P/kit/out/alpha/calls.jsonl" \
  && grep -q '"phase": "beta"' "$BOX_P/kit/out/beta/calls.jsonl"; then
  pass "the phase loop runs each named phase in order into its own out/<phase>/"
else
  fail "phase loop" "exit $code, order '$order'; $(tail -n 3 "$TMP/phases.log")"
fi

# --- fetch.sh ---------------------------------------------------------------

# fixture RUN A_DIR B_DIR: a finished run with arm A's and arm B's outputs.
fixture() {
  local d="$FAKE_S3/kit-test-bucket/clickbench-kit/runs/$1"
  rm -rf "$d"
  mkdir -p "$d/out"
  cp -R "$IN_SAVED" "$d/in"
  [ -z "$2" ] || cp -R "$2" "$d/out/A"
  [ -z "$3" ] || cp -R "$3" "$d/out/B"
}

# edit_arm SRC DST [EDIT]: a copy of SRC's outputs with an optional python edit
# applied to its report r.
edit_arm() {
  local src="$1" dst="$2" edit="${3:-}"
  rm -rf "$dst"
  cp -R "$src" "$dst"
  python3 - "$dst/results.json" "$SHA_B" "$edit" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
sha_b = sys.argv[2]
exec(sys.argv[3])
json.dump(r, open(sys.argv[1], "w"))
PY
}

# as_arm_b SRC DST [EDIT]: SRC's outputs relabelled as arm B, then edited.
as_arm_b() {
  edit_arm "$1" "$2" 'r["provenance"].update(arm="b", kit_arm="B", git_sha=sha_b)
'"${3:-}"
}

fetch() {
  local name="$1" run="$2"
  shift 2
  code=0
  bash "$KIT_SRC/fetch.sh" --run-toml "$RUN_TOML" --out "$TMP/fetch-$name" --interval 1 "$@" "$run" \
    >"$TMP/fetch-$name.out" 2>"$TMP/fetch-$name.err" || code=$?
}

# Catches: fetch failing a run whose arms match.
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-pass-b"
fixture run-pass "$TMP/fixture-pass" "$TMP/fixture-pass-b"
fetch pass run-pass --timeout 30
if [ "$code" = 0 ] && grep -q '^PASS  *B vs A  *hot_sum_s <= 1.05 x A (43 both answered)' "$TMP/fetch-pass.out" \
  && grep -q '^PASS  *B vs A  *error_pooled <= A + 0.02' "$TMP/fetch-pass.out" \
  && grep -q '^INCONCLUSIVE  *B vs A  *qps_pooled >= 0.95 x A  *None  *None  *no concurrent phase ran on A or B$' "$TMP/fetch-pass.out" \
  && grep -q '^INCONCLUSIVE  *B vs A  *q29_probe min_s <= 1.15 x A  *None  *None  *no q29_probe phase ran on A or B$' "$TMP/fetch-pass.out" \
  && grep -q '^concurrent B: no concurrent phase ran$' "$TMP/fetch-pass.out" \
  && grep -q '^PASS  *B vs A  *q01 hot_s <= max(1.15 x A, 0.2 s)' "$TMP/fetch-pass.out" \
  && grep -q '^PASS  *B vs A  *both answered >= 43  *43  *43$' "$TMP/fetch-pass.out" \
  && grep -q '^PASS  *B vs A  *rows_returned = A (43 both answered)  *0  *0$' "$TMP/fetch-pass.out" \
  && [ -f "$TMP/fetch-pass/A/evidence.tgz" ] && [ -f "$TMP/fetch-pass/B/evidence.tgz" ]; then
  pass "fetch passes two matching arms and downloads both arms' results and evidence"
else
  fail "fetch pass" "exit $code: $(tail -n 3 "$TMP/fetch-pass.err")"
fi

# Catches: a relative per-statement band that is never judged: q01 hot 0.75 s
# against A's 0.5 s is over max(1.15 x 0.5, 0.2).
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-rel-b" \
  'r["entries"][0]["min_ms"] = 750.0'
fixture run-rel "$TMP/fixture-pass" "$TMP/fixture-rel-b"
fetch rel run-rel --timeout 30
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *q01 hot_s <= max(1.15 x A, 0.2 s)' "$TMP/fetch-rel.out" \
  && ! grep -q '^MISS  *[AB]  ' "$TMP/fetch-rel.out"; then
  pass "a relative band miss exits 1 and names the statement"
else
  fail "relative miss" "exit $code: $(grep MISS "$TMP/fetch-rel.out" | head -n 3)"
fi

# Catches: fetch ignoring the absolute bands the boxes recorded: both arms
# alike (every relative band passes) but over the hot band.
as_arm_b "$TMP/fixture-miss" "$TMP/fixture-miss-b"
fixture run-abs "$TMP/fixture-miss" "$TMP/fixture-miss-b"
fetch abs run-abs --timeout 30
if [ "$code" = 1 ] && grep -q '^MISS  *A  *hot_sum_s <= hot_sum_s_max' "$TMP/fetch-abs.out" \
  && ! grep -q '^MISS  *B vs A' "$TMP/fetch-abs.out"; then
  pass "an absolute band miss on either arm makes fetch exit 1"
else
  fail "absolute miss in fetch" "exit $code: $(grep MISS "$TMP/fetch-abs.out" | head -n 3)"
fi

# Catches: wrong answers not counted as errors in the relative error band, or
# a statement arm A answered and arm B did not dropping out of the comparison
# instead of missing by id. Arm B is the wrong-answer box run above.
as_arm_b "$TMP/fixture-errors" "$TMP/fixture-errors-b"
fixture run-errors "$TMP/fixture-pass" "$TMP/fixture-errors-b"
fetch errors run-errors --timeout 30
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *error_pooled <= A + 0.02  *0.047  *0.020$' "$TMP/fetch-errors.out" \
  && grep -q '^MISS  *B vs A  *q02 answered by B .*answered by A, not by B$' "$TMP/fetch-errors.out" \
  && grep -q '^MISS  *B vs A  *q19 answered by B .*answered by A, not by B$' "$TMP/fetch-errors.out" \
  && grep -q '^PASS  *B vs A  *hot_sum_s <= 1.05 x A (41 both answered)' "$TMP/fetch-errors.out"; then
  pass "wrong answers miss the relative error band and each statement B lost is a miss by id"
else
  fail "relative errors" "exit $code: $(grep -v '^PASS' "$TMP/fetch-errors.out" | head -n 6)"
fi

# Catches: hot and cold sums taken over each arm's own answered set instead of
# the statements both arms answered. Arm A's q05 takes 5 s and arm B failed it;
# arm B is 10% slower on the other 42. Over both-answered statements both sums
# miss (23.1 s against 1.05 x 21.0 s); summing each arm's own set would put A's
# 5 s in and pass.
edit_arm "$TMP/fixture-pass" "$TMP/fixture-sums-a" '
for e in r["entries"]:
    if e["id"].startswith("q05"):
        e["min_ms"] = e["cold_ms"] = 5000.0'
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-sums-b" '
q5 = [e for e in r["entries"] if e["id"].startswith("q05")][0]
r["entries"].remove(q5)
r["failed"].append({"id": q5["id"], "run": 0, "error": "error HTTP 500 internal: stub",
                    "answer": dict(q5["answer"], answered=False)})
for e in r["entries"]:
    e["min_ms"] = e["cold_ms"] = 550.0'
fixture run-sums "$TMP/fixture-sums-a" "$TMP/fixture-sums-b"
fetch sums run-sums --timeout 30
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *hot_sum_s <= 1.05 x A (42 both answered)  *23.100  *22.050$' "$TMP/fetch-sums.out" \
  && grep -q '^MISS  *B vs A  *cold_sum_s <= 1.05 x A (42 both answered)  *23.100  *22.050$' "$TMP/fetch-sums.out" \
  && grep -q '^MISS  *B vs A  *q05 answered by B .*answered by A, not by B$' "$TMP/fetch-sums.out" \
  && ! grep -q '^MISS  *B vs A  *q[0-9]* hot_s' "$TMP/fetch-sums.out"; then
  pass "relative sums run over the statements both arms answered"
else
  fail "relative sums" "exit $code: $(grep -v '^PASS' "$TMP/fetch-sums.out" | head -n 6)"
fi

# Catches: a cold-sum band that is never judged, or judged on hot figures: arm
# B's cold tries are 10% slower (23.65 s against 1.05 x 21.5 s) and its hot
# tries match A's.
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-cold-b" '
for e in r["entries"]:
    e["cold_ms"] = 550.0'
fixture run-cold "$TMP/fixture-pass" "$TMP/fixture-cold-b"
fetch cold run-cold --timeout 30
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *cold_sum_s <= 1.05 x A (43 both answered)  *23.650  *22.575$' "$TMP/fetch-cold.out" \
  && grep -q '^PASS  *B vs A  *hot_sum_s <= 1.05 x A (43 both answered)' "$TMP/fetch-cold.out" \
  && [ "$(grep -c '^MISS' "$TMP/fetch-cold.out")" = 1 ]; then
  pass "a cold sum over the band misses on its own"
else
  fail "cold sum" "exit $code: $(grep -v '^PASS' "$TMP/fetch-cold.out" | head -n 4)"
fi

# CONC_PY: a python edit helper for edit_arm. conc(qps) is a one-round
# concurrent block whose qps_pooled is qps, with no errors.
CONC_PY='
def conc(qps):
    rd = {"round": 1, "duration_s": 600, "wall_s": 600.4, "connections": 10, "calls": 100,
          "ok": 100, "err": 0, "wrong_answer": 0, "qps": qps, "error_ratio": 0.0}
    return {"rounds": [rd], "pooled": {"rounds": 1, "calls": 100, "ok": 100, "err": 0,
            "wrong_answer": 0, "error_ratio_weighted": 0.0, "error_ratio_mean_of_rounds": 0.0,
            "qps_pooled": qps}}
'
# Catches: a QPS band that stays inconclusive when both arms ran the
# concurrent phase, or compares the wrong way: B's 0.9 against 0.95 x A's 1.0
# misses. Also catches fetch not printing a round's overran note (B's round
# ran 700 s of a nominal 600) or printing one for a round that has none.
edit_arm "$TMP/fixture-pass" "$TMP/fixture-qps-a" "$CONC_PY"'
r["concurrent"] = conc(1.0)'
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-qps-b" "$CONC_PY"'
r["concurrent"] = conc(0.9)
r["concurrent"]["rounds"][0].update(wall_s=700.0, overran={"wall_s": 700.0, "duration_s": 600})'
fixture run-qps "$TMP/fixture-qps-a" "$TMP/fixture-qps-b"
fetch qps run-qps --timeout 30
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *qps_pooled >= 0.95 x A  *0.900  *0.950$' "$TMP/fetch-qps.out" \
  && grep -q '^PASS  *B vs A  *concurrent error_ratio_weighted <= A + 0.02  *0.000  *0.020$' "$TMP/fetch-qps.out" \
  && grep -q '^concurrent B round 1: qps 0.900 error_ratio 0.000 ' "$TMP/fetch-qps.out" \
  && grep -q '^concurrent B pooled: qps_pooled 0.900 error_ratio_weighted 0.000 ' "$TMP/fetch-qps.out" \
  && grep -q '^concurrent B round 1 overran: wall 700.0 s against a nominal 600 s, more than 10% over; ' "$TMP/fetch-qps.out" \
  && [ "$(grep -c ' overran: ' "$TMP/fetch-qps.out")" = 1 ] \
  && [ "$(grep -c '^MISS' "$TMP/fetch-qps.out")" = 1 ]; then
  pass "a QPS under the ratio misses when both arms measured it"
else
  fail "qps" "exit $code: $(grep -v '^PASS' "$TMP/fetch-qps.out" | head -n 4)"
fi

# DROP_PY: a python edit helper for edit_arm. drop(r, qs) moves every entry
# whose qNN prefix is in qs to failed and recomputes the totals.
DROP_PY='
def drop(r, qs):
    keep = []
    for e in r["entries"]:
        if e["id"].split("_", 1)[0] in qs:
            r["failed"].append({"id": e["id"], "run": 0, "error": "error HTTP 504 deadline: stub",
                                "answer": dict(e["answer"], answered=False)})
        else:
            keep.append(e)
    r["entries"] = keep
    r["totals"].update(answered=len(keep), failed=[f["id"] for f in r["failed"]],
                       hot_sum_s=sum(e["min_ms"] for e in keep) / 1000.0,
                       cold_sum_s=sum(e["cold_ms"] for e in keep) / 1000.0)
ALL = ["q%02d" % n for n in range(1, 44)]
'
# relative NAME A_DIR B_DIR [BANDS]: kit.py relative over two arms' reports,
# output in $TMP/rel-NAME.out and .err, exit code in $code.
relative() {
  code=0
  python3 "$KIT_SRC/lib/kit.py" relative "${4:-$BANDS}" "$2/results.json" "$3/results.json" \
    >"$TMP/rel-$1.out" 2>"$TMP/rel-$1.err" || code=$?
}

# Catches: a run where neither arm answered anything judged a pass: every sum
# over the empty both-answered set is 0 against a bound of 0, so only the
# answered-count band can miss.
edit_arm "$TMP/fixture-pass" "$TMP/fixture-none-a" "$DROP_PY"'
drop(r, ALL)'
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-none-b" "$DROP_PY"'
drop(r, ALL)'
relative none "$TMP/fixture-none-a" "$TMP/fixture-none-b"
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *both answered >= 43  *0  *43  *not answered by A: q01 q02 .* q43; not answered by B: q01 .* q43; arm A answered no statement; arm B answered no statement$' "$TMP/rel-none.out" \
  && grep -q '^PASS  *B vs A  *hot_sum_s <= 1.05 x A (0 both answered)  *0.000  *0.000$' "$TMP/rel-none.out"; then
  pass "two arms with 0 of 43 statements answered miss, naming the count and the statements"
else
  fail "0/43" "exit $code: $(grep -v '^PASS' "$TMP/rel-none.out" | head -c 400) $(head -c 300 "$TMP/rel-none.err")"
fi

# Catches: an answered-count band read as "anything answered": both arms
# answered q01 only.
edit_arm "$TMP/fixture-pass" "$TMP/fixture-one-a" "$DROP_PY"'
drop(r, ALL[1:])'
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-one-b" "$DROP_PY"'
drop(r, ALL[1:])'
relative one "$TMP/fixture-one-a" "$TMP/fixture-one-b"
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *both answered >= 43  *1  *43  *not answered by A: q02 .* q43; not answered by B: q02 .* q43$' "$TMP/rel-one.out"; then
  pass "two arms with 1 of 43 statements answered miss"
else
  fail "1/43" "exit $code: $(grep -v '^PASS' "$TMP/rel-one.out" | head -c 400) $(head -c 300 "$TMP/rel-one.err")"
fi

# Catches: an arm with zero answered statements passing because the key allows
# it, or the zero-answered rule applied to one arm only.
mkdir -p "$TMP/bands-min1"
sed 's/^min_both_answered = .*/min_both_answered = 1/' "$BANDS" >"$TMP/bands-min1/rlog.toml"
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-zero-b" "$DROP_PY"'
drop(r, ALL)'
relative zero-b "$TMP/fixture-pass" "$TMP/fixture-zero-b" "$TMP/bands-min1/rlog.toml"
zero_b="$code"
relative zero-a "$TMP/fixture-none-a" "$TMP/fixture-pass-b" "$TMP/bands-min1/rlog.toml"
zero_a="$code"
if [ "$zero_b" = 1 ] && grep -q '^MISS  *B vs A  *both answered >= 1  *0  *1  *.*arm B answered no statement' "$TMP/rel-zero-b.out" \
  && [ "$zero_a" = 1 ] && grep -q '^MISS  *B vs A  *both answered >= 1  *0  *1  *.*arm A answered no statement' "$TMP/rel-zero-a.out"; then
  pass "an arm that answered no statement misses whatever min_both_answered allows"
else
  fail "zero answered" "exits $zero_a/$zero_b:$(grep 'both answered >=' "$TMP/rel-zero-a.out" "$TMP/rel-zero-b.out") $(head -c 300 "$TMP/rel-zero-a.err")"
fi

# Catches: a statement one arm lost dropping out of the count: A43/B42 and
# A42/B43 with min_both_answered 43. B's lost statement was already a miss by
# id; A's lost statement was only ever inconclusive.
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-b42" "$DROP_PY"'
drop(r, ["q07"])'
edit_arm "$TMP/fixture-pass" "$TMP/fixture-a42" "$DROP_PY"'
drop(r, ["q07"])'
relative b42 "$TMP/fixture-pass" "$TMP/fixture-b42"
b42="$code"
relative a42 "$TMP/fixture-a42" "$TMP/fixture-pass-b"
if [ "$b42" = 1 ] && grep -q '^MISS  *B vs A  *both answered >= 43  *42  *43  *not answered by B: q07$' "$TMP/rel-b42.out" \
  && [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *both answered >= 43  *42  *43  *not answered by A: q07$' "$TMP/rel-a42.out" \
  && [ "$(grep -c '^MISS' "$TMP/rel-a42.out")" = 1 ]; then
  pass "A43/B42 and A42/B43 both miss min_both_answered 43, naming the lost statement"
else
  fail "A43/B42, A42/B43" "exits $b42/$code: $(grep -v '^PASS' "$TMP/rel-b42.out" "$TMP/rel-a42.out" | head -c 600)"
fi
# The same A42/B43 pair passes when the band file names one known deadline miss.
sed 's/^min_both_answered = .*/min_both_answered = 42/' "$BANDS" >"$TMP/bands-min1/rlog.toml"
relative a42-42 "$TMP/fixture-a42" "$TMP/fixture-pass-b" "$TMP/bands-min1/rlog.toml"
if [ "$code" = 0 ] && grep -q '^PASS  *B vs A  *both answered >= 42  *42  *42  *not answered by A: q07$' "$TMP/rel-a42-42.out"; then
  pass "A42/B43 passes min_both_answered 42"
else
  fail "A42/B43 at 42" "exit $code: $(grep -v '^PASS' "$TMP/rel-a42-42.out" | head -c 400) $(head -c 300 "$TMP/rel-a42-42.err")"
fi

# Catches: answers never compared between the arms: both arms answered q08
# (no expected row count in the corpus), A with 3 rows and B with 4.
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-rows-b" '
for e in r["entries"]:
    if e["id"].startswith("q08"):
        e["rows_returned"] = 4
        e["answer"]["rows"] = 4'
relative rows "$TMP/fixture-pass" "$TMP/fixture-rows-b"
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *rows_returned = A (43 both answered)  *1  *0  *q08$' "$TMP/rel-rows.out" \
  && grep -q '^MISS  *B vs A  *q08 rows_returned = A  *4  *3$' "$TMP/rel-rows.out" \
  && [ "$(grep -c '^MISS' "$TMP/rel-rows.out")" = 2 ]; then
  pass "a statement answering another row count on B than on A is a miss naming it"
else
  fail "rows_returned" "exit $code: $(grep -v '^PASS' "$TMP/rel-rows.out" | head -c 400) $(head -c 300 "$TMP/rel-rows.err")"
fi

# Catches: two arms compared when they ran on different data, a different
# mapping or corpus, a different CPU model, a different RustFS build, a
# different audit mode, a different query window or under different server
# settings. Each must exit 2 naming the field. Each entry is the provenance
# path and the python edit that changes it on arm B.
for edit in 'corpus_digest.dataset_sha256|p["corpus_digest"]["dataset_sha256"] = "2" * 64' \
  'corpus_digest.mapping_sha256|p["corpus_digest"]["mapping_sha256"] = "3" * 64' \
  'corpus_digest.corpus_sha256|p["corpus_digest"]["corpus_sha256"] = "4" * 64' \
  'hardware.cpu|p["hardware"]["cpu"] = "Some Other CPU"' \
  'backend.version|p["backend"]["version"] = "rustfs 9.9.9"' \
  'audit_mode|p["audit_mode"] = "required, audit text redacted"' \
  'query_window.window_hours|p["query_window"]["window_hours"] = 100000' \
  'server_settings|p["server_settings"]["fetch_concurrency"] += 1' \
  'server_flags.base_flags|p["server_flags"]["base_flags"][3] = "4"' \
  'server_flags.extra_flags|p["server_flags"]["extra_flags"] = ["--fetch-concurrency", "64"]'; do
  field="${edit%%|*}"
  as_arm_b "$TMP/fixture-pass" "$TMP/fixture-cmp-b" "p = r[\"provenance\"]
${edit#*|}"
  code=0
  python3 "$KIT_SRC/lib/kit.py" relative "$BANDS" "$TMP/fixture-pass/results.json" "$TMP/fixture-cmp-b/results.json" \
    >"$TMP/cmp.out" 2>"$TMP/cmp.err" || code=$?
  if [ "$code" = 2 ] && grep -qF "arms are not comparable: provenance.$field differs" "$TMP/cmp.err" \
    && ! grep -q 'B vs A' "$TMP/cmp.out"; then
    pass "arms with a different $field are refused with exit 2, naming it"
  else
    fail "comparable $field" "exit $code: $(head -c 300 "$TMP/cmp.err")"
  fi
done

# Catches: arms judged by different band sections compared, and a comparable
# field missing on one arm read as equal to nothing on the other.
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-cmp-b" 'r["provenance"]["band_sections"] = ["relative"]'
relative sections "$TMP/fixture-pass" "$TMP/fixture-cmp-b"
sections_code="$code"
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-cmp-b" 'del r["provenance"]["hardware"]["cpu"]'
relative missing "$TMP/fixture-pass" "$TMP/fixture-cmp-b"
if [ "$sections_code" = 2 ] && grep -qF 'arms are not comparable: provenance.band_sections differs' "$TMP/rel-sections.err" \
  && [ "$code" = 2 ] && grep -qF 'arms are not comparable: provenance.hardware.cpu is missing on B' "$TMP/rel-missing.err" \
  && ! grep -q 'B vs A' "$TMP/rel-sections.out" "$TMP/rel-missing.out"; then
  pass "arms with different band sections, or one missing a comparable field, are refused with exit 2"
else
  fail "band_sections / missing field" "exits $sections_code/$code: $(head -c 200 "$TMP/rel-sections.err") $(head -c 200 "$TMP/rel-missing.err")"
fi
# The same refusal through fetch, which must not read it as a band miss.
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-cmp-b" 'r["provenance"]["hardware"]["cpu"] = "Some Other CPU"'
fixture run-cmp "$TMP/fixture-pass" "$TMP/fixture-cmp-b"
fetch cmp run-cmp --timeout 30
if [ "$code" = 2 ] && grep -q 'provenance.hardware.cpu differs' "$TMP/fetch-cmp.err"; then
  pass "fetch exits 2 on arms that are not comparable"
else
  fail "fetch comparable" "exit $code: $(tail -n 2 "$TMP/fetch-cmp.err")"
fi

# flags_fixture RUN A_DIR B_DIR SED: fixture, with the run's uploaded
# in/run.toml edited by SED (fetch reads the declared extra flags from it).
flags_fixture() {
  local d="$FAKE_S3/kit-test-bucket/clickbench-kit/runs/$1"
  fixture "$1" "$2" "$3"
  sed "$4" "$d/in/run.toml" >"$d/in/run.toml.new" && mv "$d/in/run.toml.new" "$d/in/run.toml"
}
declare_b='s/^shards = /server_flags_b = ["--fetch-concurrency", "64"]\n&/'
# Arm B ran with --fetch-concurrency 64, which moved its resolved setting.
flags_b_edit='r["provenance"]["server_flags"]["extra_flags"] = ["--fetch-concurrency", "64"]
r["provenance"]["server_settings"]["fetch_concurrency"] = 64'

# Catches: arms whose only difference is the extra flags run.toml declared for
# them refused, or the difference not printed, or printed more than once.
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-flags-b" "$flags_b_edit"
flags_fixture run-flags "$TMP/fixture-pass" "$TMP/fixture-flags-b" "$declare_b"
fetch flags run-flags --timeout 30
if [ "$code" = 0 ] \
  && [ "$(grep -c '^comparable: ' "$TMP/fetch-flags.out")" = 2 ] \
  && [ "$(grep -cF "comparable: arms differ by their declared extra server flags only: A [], B ['--fetch-concurrency', '64']" "$TMP/fetch-flags.out")" = 1 ] \
  && [ "$(grep -cF "comparable: server_settings differs with them: fetch_concurrency A 32 B 64" "$TMP/fetch-flags.out")" = 1 ] \
  && grep -q '^PASS  *B vs A  *hot_sum_s <= 1.05 x A (43 both answered)' "$TMP/fetch-flags.out"; then
  pass "fetch accepts arms that differ by exactly the declared server_flags_a/b, printing the difference once"
else
  fail "fetch declared flags" "exit $code: $(grep '^comparable' "$TMP/fetch-flags.out") $(tail -n 2 "$TMP/fetch-flags.err")"
fi

# Catches: the declared-flags exception widened past the declared lists: B's
# extra flags other than run.toml declares, extra flags with nothing
# declared, and a kit-set flag that differs alongside declared extras.
flags_refusal() {
  local name="$1" edit="$2" sed_toml="$3" want="$4" id
  id="$(printf '%s' "$name" | tr ' ' '-')"
  as_arm_b "$TMP/fixture-pass" "$TMP/fixture-flags-$id-b" "$edit"
  flags_fixture "run-flags-$id" "$TMP/fixture-pass" "$TMP/fixture-flags-$id-b" "$sed_toml"
  fetch "flags-$id" "run-flags-$id" --timeout 30
  if [ "$code" = 2 ] && grep -qF -- "$want" "$TMP/fetch-flags-$id.err" \
    && ! grep -q 'B vs A' "$TMP/fetch-flags-$id.out" && ! grep -q '^comparable: ' "$TMP/fetch-flags-$id.out"; then
    pass "fetch refuses $name with exit 2"
  else
    fail "fetch flags $name" "exit $code: $(tail -n 2 "$TMP/fetch-flags-$id.err")"
  fi
}
flags_refusal "extra flags other than declared" \
  'r["provenance"]["server_flags"]["extra_flags"] = ["--fetch-concurrency", "32"]' "$declare_b" \
  "provenance.server_flags.extra_flags is A [], B ['--fetch-concurrency', '32'], and run.toml's server_flags_a and server_flags_b are A [], B ['--fetch-concurrency', '64']"
flags_refusal "undeclared extra flags" "$flags_b_edit" 's/^shards = /&/' \
  "provenance.server_flags.extra_flags is A [], B ['--fetch-concurrency', '64'], and run.toml's server_flags_a and server_flags_b are A [], B []"
flags_refusal "a kit-set flag differing beside declared extras" "$flags_b_edit
r[\"provenance\"][\"server_flags\"][\"base_flags\"][3] = \"4\"" "$declare_b" \
  'arms are not comparable: provenance.server_flags.base_flags differs'
flags_refusal "another field differing beside declared extras" "$flags_b_edit
r[\"provenance\"][\"hardware\"][\"cpu\"] = \"Some Other CPU\"" "$declare_b" \
  'arms are not comparable: provenance.hardware.cpu differs'

# Catches: declared extra flags waving through every resolved server setting
# that differs. A setting may differ only when a flag that sets it is set
# differently by the declared lists: #2980's --sql-scan-decode-inline sets
# none of them, and --fetch-concurrency sets only fetch_concurrency.
declare_inline='s/^shards = /server_flags_b = ["--sql-scan-decode-inline"]\n&/'
inline_edit='r["provenance"]["server_flags"]["extra_flags"] = ["--sql-scan-decode-inline"]'
flags_refusal "an unrelated setting differing beside a declared flag that sets none" "$inline_edit
r[\"provenance\"][\"server_settings\"][\"cache_max_bytes\"] = 1" "$declare_inline" \
  "provenance.server_settings.cache_max_bytes differs (A 7689077760, B 1), and the declared extra flags do not set --cache-max-bytes differently"
flags_refusal "a setting the declared flag does not set differing beside one it does" "$flags_b_edit
r[\"provenance\"][\"server_settings\"][\"sql_tenant_max_bytes\"] = 1" "$declare_b" \
  "provenance.server_settings.sql_tenant_max_bytes differs (A 32903794688, B 1), and the declared extra flags do not set --sql-tenant-max-bytes or --sql-max-query-bytes differently"
flags_refusal "a setting no known flag sets differing beside declared extras" "$flags_b_edit
r[\"provenance\"][\"server_settings\"][\"unmapped_setting\"] = 1" "$declare_b" \
  "provenance.server_settings.unmapped_setting differs (A None, B 1), and the kit knows no flag that sets it"

# Catches: the setting check refusing what a declared flag does move, or
# printing a difference that is not there. B declares --sql-scan-decode-inline
# with every setting equal (one comparable line), then --sql-tenant-max-bytes,
# which moves both SQL pools (the per-query pool is clamped to it), then
# --sql-max-query-bytes above the tenant pool, which raises the tenant pool to
# it.
flags_accepted() {
  local name="$1" edit="$2" sed_toml="$3"
  shift 3
  as_arm_b "$TMP/fixture-pass" "$TMP/fixture-flags-$name-b" "$edit"
  flags_fixture "run-flags-$name" "$TMP/fixture-pass" "$TMP/fixture-flags-$name-b" "$sed_toml"
  fetch "flags-$name" "run-flags-$name" --timeout 30
  local want ok=1
  [ "$code" = 0 ] && [ "$(grep -c '^comparable: ' "$TMP/fetch-flags-$name.out")" = "$#" ] || ok=0
  for want in "$@"; do
    [ "$(grep -cxF "comparable: $want" "$TMP/fetch-flags-$name.out")" = 1 ] || ok=0
  done
  if [ "$ok" = 1 ]; then
    pass "fetch accepts declared flags ($name), printing exactly the accepted differences"
  else
    fail "fetch flags $name" "exit $code: $(grep '^comparable' "$TMP/fetch-flags-$name.out") $(tail -n 2 "$TMP/fetch-flags-$name.err")"
  fi
}
flags_accepted inline "$inline_edit" "$declare_inline" \
  "arms differ by their declared extra server flags only: A [], B ['--sql-scan-decode-inline']"
flags_accepted tenant 'r["provenance"]["server_flags"]["extra_flags"] = ["--sql-tenant-max-bytes", "1000"]
r["provenance"]["server_settings"].update(sql_tenant_max_bytes=1000, sql_max_query_bytes=1000)' \
  's/^shards = /server_flags_b = ["--sql-tenant-max-bytes", "1000"]\n&/' \
  "arms differ by their declared extra server flags only: A [], B ['--sql-tenant-max-bytes', '1000']" \
  "server_settings differs with them: sql_max_query_bytes A 16451897344 B 1000, sql_tenant_max_bytes A 32903794688 B 1000"
flags_accepted raise 'r["provenance"]["server_flags"]["extra_flags"] = ["--sql-max-query-bytes", "40000000000"]
r["provenance"]["server_settings"].update(sql_tenant_max_bytes=40000000000, sql_max_query_bytes=40000000000)' \
  's/^shards = /server_flags_b = ["--sql-max-query-bytes", "40000000000"]\n&/' \
  "arms differ by their declared extra server flags only: A [], B ['--sql-max-query-bytes', '40000000000']" \
  "server_settings differs with them: sql_max_query_bytes A 16451897344 B 40000000000, sql_tenant_max_bytes A 32903794688 B 40000000000"

# Catches: a declared flag on one SQL pool letting either pool, or the key it
# sets directly, resolve to any value. B's --sql-max-query-bytes 50 can raise
# the tenant pool only up to 50 and only from below, so 32903794688 moving to
# 7 is refused; B's --sql-tenant-max-bytes above the per-query pool clamps
# nothing, so that pool moving to 3 is refused; and --fetch-concurrency 64
# sets fetch_concurrency to 64, not 17.
flags_refusal "the tenant pool moved past what --sql-max-query-bytes raises it to" \
  'r["provenance"]["server_flags"]["extra_flags"] = ["--sql-max-query-bytes", "50"]
r["provenance"]["server_settings"].update(sql_max_query_bytes=50, sql_tenant_max_bytes=7)' \
  's/^shards = /server_flags_b = ["--sql-max-query-bytes", "50"]\n&/' \
  "provenance.server_settings.sql_tenant_max_bytes is A 32903794688, B 7, which the declared extra flags do not resolve it to: A sets no --sql-tenant-max-bytes or --sql-max-query-bytes, so it holds the unflagged value; B's --sql-max-query-bytes 50 raises it to 50 only from an unflagged value below 50"
flags_refusal "the query pool moved past what --sql-tenant-max-bytes clamps it to" \
  'r["provenance"]["server_flags"]["extra_flags"] = ["--sql-tenant-max-bytes", "40000000000"]
r["provenance"]["server_settings"].update(sql_max_query_bytes=3, sql_tenant_max_bytes=40000000000)' \
  's/^shards = /server_flags_b = ["--sql-tenant-max-bytes", "40000000000"]\n&/' \
  "provenance.server_settings.sql_max_query_bytes is A 16451897344, B 3, which the declared extra flags do not resolve it to: A sets no --sql-max-query-bytes or --sql-tenant-max-bytes, so it holds the unflagged value; B's --sql-tenant-max-bytes 40000000000 clamps it to 40000000000 only from an unflagged value above 40000000000"
flags_refusal "fetch_concurrency resolved to other than --fetch-concurrency sets" \
  'r["provenance"]["server_flags"]["extra_flags"] = ["--fetch-concurrency", "64"]
r["provenance"]["server_settings"]["fetch_concurrency"] = 17' "$declare_b" \
  "provenance.server_settings.fetch_concurrency is A 32, B 17, which the declared extra flags do not resolve it to: A sets no --fetch-concurrency, so it holds the unflagged value; B's --fetch-concurrency 64 sets it to 64"

# Catches: the setting check accepting a move resolve_performance_defaults in
# services/ravel-server/src/config.rs does not make, or refusing one it does.
# Arm A holds the unflagged pools (query 100, tenant 200) unless a case gives
# it flags of its own.
settings="$(python3 - "$KIT_SRC/lib" <<'PY' 2>&1
import sys
sys.path.insert(0, sys.argv[1])
import kit
Q, T, F = "sql_max_query_bytes", "sql_tenant_max_bytes", "fetch_concurrency"
base = {Q: 100, T: 200, F: 32}
def s(**kw):
    out = dict(base)
    out.update({{"q": Q, "t": T, "f": F}[k]: v for k, v in kw.items()})
    return out
cases = [
    # name, A settings, B settings, A flags, B flags, accepted
    ("raise above the tenant pool", base, s(q=500, t=500), [], ["--sql-max-query-bytes", "500"], True),
    ("query flag below the tenant pool raises nothing", base, s(q=150), [],
     ["--sql-max-query-bytes", "150"], True),
    ("tenant pool raised from above", base, s(q=150, t=150), [],
     ["--sql-max-query-bytes", "150"], False),
    ("tenant pool left below the query flag", base, s(q=500), [],
     ["--sql-max-query-bytes", "500"], False),
    ("clamp below the query pool", base, s(q=60, t=60), [], ["--sql-tenant-max-bytes", "60"], True),
    ("query pool clamped although below the tenant flag", base, s(q=60, t=150), [],
     ["--sql-tenant-max-bytes", "150"], False),
    ("query pool left above the tenant flag", base, s(t=60), [],
     ["--sql-tenant-max-bytes", "60"], False),
    ("both pool flags, the query one clamped", base, s(q=80, t=80), [],
     ["--sql-max-query-bytes", "90", "--sql-tenant-max-bytes", "80"], True),
    ("both pool flags, the query one not clamped", base, s(q=90, t=80), [],
     ["--sql-max-query-bytes", "90", "--sql-tenant-max-bytes", "80"], False),
    ("query flags on both arms, each raise consistent", s(q=50, t=60), s(q=80, t=80),
     ["--sql-max-query-bytes", "50"], ["--sql-max-query-bytes", "80"], True),
    ("query flags on both arms, the raise inconsistent", s(q=50, t=70), s(q=80, t=90),
     ["--sql-max-query-bytes", "50"], ["--sql-max-query-bytes", "80"], False),
    ("fetch flag honoured", base, s(f=64), [], ["--fetch-concurrency", "64"], True),
    ("fetch flags on both arms, equal stamps neither sets", base, base,
     ["--fetch-concurrency", "64"], ["--fetch-concurrency", "17"], False),
    ("a setting flag given twice", base, s(f=64), [],
     ["--fetch-concurrency", "64", "--fetch-concurrency=64"], False),
    ("a setting flag with no number", base, s(f=64), [], ["--fetch-concurrency", "lots"], False),
]
bad = []
for name, va, vb, ea, eb, want in cases:
    try:
        kit._setting_differences(va, vb, ea, eb)
        got = True
    except kit.Refusal:
        got = False
    if got != want:
        bad.append("%s %s" % (name, "refused" if want else "accepted"))
print("; ".join(bad))
PY
)"
if [ -z "$settings" ]; then
  pass "a declared SQL pool or fetch flag accepts exactly the values config.rs resolves the settings to"
else
  fail "setting resolution" "$settings"
fi

# Catches: a stamped setting the comparability check has no flag entry for,
# which would then refuse every declared flag that moves it.
unmapped="$(python3 -c 'import sys; sys.path.insert(0, sys.argv[1]); import kit
print(" ".join(sorted(set(kit.STAMPED_SETTINGS) ^ set(kit.SETTING_FLAGS))))' "$KIT_SRC/lib" 2>&1)"
if [ -z "$unmapped" ]; then
  pass "every stamped server setting has an entry in SETTING_FLAGS, and nothing else does"
else
  fail "setting flags" "$unmapped"
fi

# Catches: a healthz p99 band set to a number and never judged, or a
# placeholder one judged: 4318's ratio is set and B's benchmark p99 is three
# times A's, while 4319's stays a placeholder and yields no row.
mkdir -p "$TMP/bands-healthz"
sed 's/^healthz_p99_max_ratio_4318 = .*/healthz_p99_max_ratio_4318 = 1.5/' "$BANDS" >"$TMP/bands-healthz/rlog.toml"
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-hz-b" 'p = r["healthz"]["phases"]["benchmark"]["ports"]["4318"]
p["p99_ms"] *= 3'
relative healthz "$TMP/fixture-pass" "$TMP/fixture-hz-b" "$TMP/bands-healthz/rlog.toml"
abs_hz="$(python3 -c 'import json, sys; sys.path.insert(0, sys.argv[1]); import kit
r = json.load(open(sys.argv[2]))
rows = kit.healthz_absolute({"healthz_p99_ms_max_4318": 0.001, "healthz_p99_ms_max_4319": "FILL-IN-ON-REFERENCE-MACHINE"}, r)
print(" ".join("%s:%s" % (x["band"], x["verdict"]) for x in rows))' "$KIT_SRC/lib" "$TMP/fixture-pass/results.json" 2>&1)"
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *healthz benchmark port 4318 p99_ms <= 1.5 x A ' "$TMP/rel-healthz.out" \
  && ! grep -q 'port 4319' "$TMP/rel-healthz.out" \
  && [ "$abs_hz" = "healthz benchmark p99_ms <= healthz_p99_ms_max_4318:miss" ]; then
  pass "a numeric healthz p99 band is judged per phase and port, and a placeholder one is skipped"
else
  fail "healthz bands" "exit $code, absolute '$abs_hz': $(grep healthz "$TMP/rel-healthz.out") $(head -c 200 "$TMP/rel-healthz.err")"
fi

# Catches: a report with a null sum judged (or crashing into exit 1, which
# every caller reads as a band miss).
edit_arm "$TMP/fixture-pass" "$TMP/fixture-null" 'r["totals"]["hot_sum_s"] = None'
code=0
python3 "$KIT_SRC/lib/kit.py" relative "$BANDS" "$TMP/fixture-pass/results.json" "$TMP/fixture-null/results.json" \
  >/dev/null 2>"$TMP/null.err" || code=$?
if [ "$code" = 2 ] && grep -q 'totals.hot_sum_s is None, not a number' "$TMP/null.err"; then
  pass "a null totals.hot_sum_s exits 2, naming the field"
else
  fail "null sum" "exit $code: $(head -c 300 "$TMP/null.err")"
fi

# Catches: an unexpected exception exiting 1 (a band miss to every caller) or
# without its traceback. A phases block that is not an object passes validate
# and breaks the error ratio.
edit_arm "$TMP/fixture-pass" "$TMP/fixture-crash" 'r["phases"] = {"benchmark": 5}'
code=0
python3 "$KIT_SRC/lib/kit.py" relative "$BANDS" "$TMP/fixture-pass/results.json" "$TMP/fixture-crash/results.json" \
  >/dev/null 2>"$TMP/crash.err" || code=$?
if [ "$code" = 2 ] && grep -q '^Traceback ' "$TMP/crash.err" \
  && grep -q 'kit: unexpected error in relative; see the traceback above' "$TMP/crash.err"; then
  pass "an unexpected exception exits 2 with its traceback"
else
  fail "unexpected exception" "exit $code: $(tail -c 300 "$TMP/crash.err")"
fi

# Catches: a 403 on a download read as a missing object (exit 2) rather than
# as an S3 failure (exit 4).
STUB_S3_DENY_KEY=evidence.tgz fetch deny run-pass --timeout 30
if [ "$code" = 4 ] && grep -q '(403)' "$TMP/fetch-deny.err" \
  && grep -q 'download of .*/out/A/evidence.tgz failed (aws exit 1)' "$TMP/fetch-deny.err"; then
  pass "a 403 on a download exits 4"
else
  fail "fetch 403" "exit $code: $(tail -n 2 "$TMP/fetch-deny.err")"
fi

# Catches: a missing bucket read as a missing key (exit 2, "not there") because
# its message also says "does not exist", rather than as an S3 failure (exit 4).
STUB_S3_NO_BUCKET=1 fetch nobucket run-pass --timeout 30
if [ "$code" = 4 ] && grep -q 'NoSuchBucket' "$TMP/fetch-nobucket.err" \
  && grep -q 'download of .* failed (aws exit 1)' "$TMP/fetch-nobucket.err"; then
  pass "a NoSuchBucket on a download exits 4"
else
  fail "fetch NoSuchBucket" "exit $code: $(tail -n 2 "$TMP/fetch-nobucket.err")"
fi

# Catches: a FAILED arm read as a pass, or its reason not shown.
mkdir -p "$TMP/fixture-failed"
printf 'FAILED step=build at=2026-10-11T10:00:00Z\n' >"$TMP/fixture-failed/FAILED"
fixture run-failed "$TMP/fixture-pass" "$TMP/fixture-failed"
fetch failed run-failed --timeout 30
if [ "$code" = 2 ] && grep -q 'FAILED step=build' "$TMP/fetch-failed.err"; then
  pass "a FAILED arm exits 2 and prints the failing step"
else
  fail "FAILED" "exit $code: $(tail -n 3 "$TMP/fetch-failed.err")"
fi

# Catches: a results.json missing a stamp judged anyway.
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-bad-b" 'del r["provenance"]["fold"]'
fixture run-bad "$TMP/fixture-pass" "$TMP/fixture-bad-b"
fetch bad run-bad --timeout 30
if [ "$code" = 2 ] && grep -q 'provenance missing fold' "$TMP/fetch-bad.err"; then
  pass "a malformed results.json exits 2"
else
  fail "malformed" "exit $code: $(tail -n 3 "$TMP/fetch-bad.err")"
fi

# Catches: an unreachable S3 read as "still running" until the timeout.
fetch down run-pass --timeout 30 --unreachable-tries 2
down_code="$code"
STUB_S3_DOWN=1 fetch down run-pass --timeout 30 --unreachable-tries 2
if [ "$down_code" = 0 ] && [ "$code" = 4 ] && grep -q 'S3 unreachable' "$TMP/fetch-down.err"; then
  pass "unreachable S3 exits 4"
else
  fail "unreachable" "exit $code: $(tail -n 2 "$TMP/fetch-down.err")"
fi

# Catches: an aws call allowed to hang past the per-call limit.
KIT_FETCH_CALL_LIMIT=1 STUB_S3_HANG=1 fetch hang run-pass --timeout 30 --unreachable-tries 1
if [ "$code" = 4 ] && grep -q 'killed an aws call after 1 s' "$TMP/fetch-hang.err"; then
  pass "an aws call over the per-call limit is killed and counts as unreachable"
else
  fail "call limit" "exit $code: $(tail -n 2 "$TMP/fetch-hang.err")"
fi
KIT_FETCH_CALL_LIMIT=91 fetch limit run-pass --timeout 30
if [ "$code" = 64 ]; then
  pass "a per-call limit over 90 s is refused"
else
  fail "call limit cap" "exit $code"
fi

# Catches: polling forever when no marker ever appears.
fixture run-pending "" ""
fetch pending run-pending --timeout 2
if [ "$code" = 3 ] && grep -q 'no marker from both arms within 2 s' "$TMP/fetch-pending.err"; then
  pass "no marker within --timeout exits 3"
else
  fail "timeout" "exit $code: $(tail -n 2 "$TMP/fetch-pending.err")"
fi

# --- concurrent, q29_probe and perf phases ----------------------------------

# Catches: a statement order that is not upstream bench_concurrent_qps's
# (another hash, byte order or shuffle), which would make the two kits' QPS
# figures incomparable. The indexes are pinned literals, not recomputed.
perm="$(python3 "$KIT_SRC/lib/kit.py" permutation clickbench 1 43)"
if [ "$(echo "$perm" | cut -d' ' -f1-5)" = "14 23 41 9 1" ] \
  && [ "$(echo "$perm" | tr ' ' '\n' | sort -n | tr '\n' ' ')" = "$(seq 0 42 | tr '\n' ' ')" ]; then
  pass "seed clickbench, connection 1, 43 statements: the order starts 14 23 41 9 1"
else
  fail "permutation" "got '$perm'"
fi

# A concurrent phase directory of three rounds of 2 s nominal, each with a
# measured wall time over it and a different call count:
#   round 1: 10 calls, 9 of them 2xx (one a wrong answer), 1 a 500
#   round 2: 30 calls, all 2xx
#   round 3:  4 calls, all 503
CONC_DIR="$TMP/conc-units"
mkdir -p "$CONC_DIR"
python3 - "$CONC_DIR" <<'PY'
import json, os, sys
d = sys.argv[1]
with open(os.path.join(d, "rounds.jsonl"), "w") as f:
    for r, wall in ((1, 2.5), (2, 2.25), (3, 3.0)):
        f.write(json.dumps({"round": r, "duration_s": 2, "wall_s": wall, "connections": 2,
                            "statements": 43, "seed": "clickbench"}) + "\n")
calls = []
def call(r, status, cls=""):
    calls.append({"statement": "q01_count_all", "round": r, "connection": 1 + len(calls) % 2,
                  "http_status": status, "curl_exit": 0, "refusal_class": cls})
for i in range(8):
    call(1, 200)
call(1, 200, "wrong_answer")
call(1, 500, "internal")
for i in range(30):
    call(2, 200)
for i in range(4):
    call(3, 503, "unavailable")
with open(os.path.join(d, "calls.jsonl"), "w") as f:
    for c in calls:
        f.write(json.dumps(c) + "\n")
PY
code=0
python3 "$KIT_SRC/lib/kit.py" concurrent-summary "$CONC_DIR" >"$TMP/conc-units.json" 2>"$TMP/conc-units.err" || code=$?

# conc_check NAME PYTHON: PYTHON run against the summary as s, rounds as rd;
# it prints nothing when the claim holds.
conc_check() {
  python3 - "$TMP/conc-units.json" "$2" <<'PY' 2>&1
import json, sys
s = json.load(open(sys.argv[1]))
rd = dict((r["round"], r) for r in s["rounds"])
p = s["pooled"]
exec(sys.argv[2])
PY
}

# Catches: a round's QPS divided by its measured wall time (2.5 s, 2.25 s)
# instead of the nominal duration_s (2 s): 9 / 2.5 is 3.6, not 4.5.
out="$(conc_check qps '
if (rd[1]["qps"], rd[2]["qps"]) != (4.5, 15.0) or rd[1]["wall_s"] == rd[1]["duration_s"]:
    print("qps", rd[1]["qps"], rd[2]["qps"])')"
if [ "$code" = 0 ] && [ -z "$out" ]; then
  pass "a round's QPS is its 2xx calls over the nominal duration, not the wall time"
else
  fail "round qps" "exit $code: $out $(head -c 300 "$TMP/conc-units.err")"
fi

# Catches: a round that made calls but had no successful one reported as
# null QPS, which the pooled mean would then leave out, or its errors dropped.
out="$(conc_check zero '
if rd[3]["qps"] != 0.0 or rd[3]["error_ratio"] != 1.0 or (rd[3]["ok"], rd[3]["err"]) != (0, 4):
    print(rd[3])')"
if [ "$code" = 0 ] && [ -z "$out" ]; then
  pass "an all-error round has QPS 0 and error ratio 1.0"
else
  fail "all-error round" "$out"
fi

# Catches: qps_pooled leaving the all-error round out ((4.5 + 15) / 2 =
# 9.75), so arm B with one dead round could pass qps_min_ratio against arm
# A's three good ones, rather than the mean with it as 0 ((4.5 + 15 + 0) / 3
# = 6.5), and the mean of the round error ratios ((0.1 + 0 + 1) / 3)
# reported under error_ratio_weighted, whose value is all errors over all
# calls (5 / 44).
out="$(conc_check pooled '
want = {"rounds": 3, "calls": 44, "ok": 39, "err": 5, "wrong_answer": 1, "qps_pooled": 6.5}
for k, v in want.items():
    if p[k] != v:
        print(k, p[k], "want", v)
if abs(p["error_ratio_weighted"] - 5 / 44.0) > 1e-12:
    print("error_ratio_weighted", p["error_ratio_weighted"], "want", 5 / 44.0)
if abs(p["error_ratio_mean_of_rounds"] - 1.1 / 3) > 1e-12:
    print("error_ratio_mean_of_rounds", p["error_ratio_mean_of_rounds"], "want", 1.1 / 3)
if abs(p["error_ratio_weighted"] - p["error_ratio_mean_of_rounds"]) < 0.1:
    print("the two error forms do not differ")')"
if [ "$code" = 0 ] && [ -z "$out" ]; then
  pass "three rounds, one with no success, pool to qps_pooled 6.5 (that round as 0), error_ratio_weighted 5/44 and mean_of_rounds 1.1/3"
else
  fail "pooling" "$out"
fi

# Catches: a round that made no call at all counted as 0 in qps_pooled, when
# it has nothing to measure: it stays null and out of the mean.
out="$(python3 - "$KIT_SRC/lib" <<'PY' 2>&1
import sys
sys.path.insert(0, sys.argv[1])
import kit
meta = lambda r: {"round": r, "duration_s": 2, "wall_s": 2.0, "connections": 1}
ok = {"http_status": 200, "curl_exit": 0}
rounds = [kit.concurrent_round(meta(1), [dict(ok)] * 6), kit.concurrent_round(meta(2), [])]
p = kit.pool_rounds(rounds)
if rounds[1]["qps"] is not None or p["qps_pooled"] != 3.0:
    print(rounds[1], p)
PY
)"
if [ -z "$out" ]; then
  pass "a round with no call has QPS null and stays out of qps_pooled"
else
  fail "no-call round" "$out"
fi

# Catches: a round that ran more than 10% past its nominal duration not
# marked "overran" with both figures, one within 10% marked, or the overrun
# changing the QPS formula (successful calls over the nominal duration).
out="$(python3 - "$KIT_SRC/lib" <<'PY' 2>&1
import sys
sys.path.insert(0, sys.argv[1])
import kit
ok = [{"http_status": 200, "curl_exit": 0}] * 20
for wall, want in ((11.5, {"wall_s": 11.5, "duration_s": 10}), (10.9, None), (10.0, None)):
    rd = kit.concurrent_round({"round": 1, "duration_s": 10, "wall_s": wall, "connections": 1}, ok)
    if rd["overran"] != want or rd["qps"] != 2.0:
        print("wall", wall, rd)
PY
)"
if [ -z "$out" ]; then
  pass "a round more than 10% over its nominal duration is marked overran with both figures, its QPS formula unchanged"
else
  fail "overran round" "$out"
fi

# Catches: a 200 whose transfer then failed counted as a success, where
# upstream's curl --fail-with-body exits non-zero and the call is an error.
out="$(python3 - "$KIT_SRC/lib" <<'PY' 2>&1
import sys
sys.path.insert(0, sys.argv[1])
import kit
meta = {"round": 1, "duration_s": 2, "wall_s": 2.0, "connections": 1}
rd = kit.concurrent_round(meta, [{"http_status": 200, "curl_exit": 0},
                                 {"http_status": 200, "curl_exit": 18}])
if (rd["ok"], rd["err"], rd["qps"]) != (1, 1, 0.5):
    print(rd)
PY
)"
if [ -z "$out" ]; then
  pass "a 200 whose transfer failed (curl exit 18) is a concurrent error"
else
  fail "transfer failure" "$out"
fi

# Catches: error_pooled counting any phase but the benchmark's, so its verdict
# would change with which phases a run names: the same benchmark calls must
# give the same figure with q29_probe and perf calls beside them.
out="$(python3 - "$KIT_SRC/lib" <<'PY' 2>&1
import sys
sys.path.insert(0, sys.argv[1])
import kit
def report(phases):
    return {"totals": {"hot_sum_s": 1.0, "cold_sum_s": 1.0}, "phases": phases}
bench = {"benchmark": {"calls": 129, "errors": 6}}
more = dict(bench, q29_probe={"calls": 4, "errors": 4}, perf={"calls": 86, "errors": 0},
            concurrent={"calls": 5000, "errors": 900})
a, b = kit.run_metrics(report(bench)), kit.run_metrics(report(more))
if a["error_pooled"] != 6 / 129.0 or b["error_pooled"] != a["error_pooled"]:
    print("error_pooled", a["error_pooled"], b["error_pooled"])
if kit.pooled_errors(more) != (129, 6):
    print("pooled_errors", kit.pooled_errors(more))
PY
)"
if [ -z "$out" ]; then
  pass "error_pooled counts the benchmark phase only, whichever other phases ran"
else
  fail "error_pooled phases" "$out"
fi

# Catches: a 2xx answer with the wrong row count counted as an error in the
# concurrent phase, where upstream counts any 2xx as a success.
out="$(conc_check wrong '
if (rd[1]["ok"], rd[1]["err"], rd[1]["wrong_answer"]) != (9, 1, 1):
    print(rd[1])')"
if [ "$code" = 0 ] && [ -z "$out" ]; then
  pass "a 2xx with the wrong row count counts as ok and in wrong_answer"
else
  fail "wrong answer in concurrent" "$out"
fi

# Each new band key judged pass, miss and inconclusive (its phase absent),
# against the filled band file: relative qps_min_ratio 0.95,
# concurrent_error_pooled_max_delta 0.02, q29_probe_max_ratio 1.15; absolute
# concurrent_qps_min 1.0, concurrent_error_pooled_max 0.1, q29_probe_s_max 1.0.
python3 - "$KIT_SRC/lib" "$BANDS" "$TMP/fixture-pass/results.json" >"$TMP/bandkeys.out" 2>&1 <<'PY'
import copy, json, sys
sys.path.insert(0, sys.argv[1])
import kit
bands, base = sys.argv[2], json.load(open(sys.argv[3]))

def conc(qps, weighted, mean=None):
    return {"rounds": [], "pooled": {
        "rounds": 1, "calls": 100, "ok": 90, "err": 10, "wrong_answer": 0,
        "error_ratio_weighted": weighted,
        "error_ratio_mean_of_rounds": weighted if mean is None else mean, "qps_pooled": qps}}

def probe(min_s):
    return {"statement": "q29_referer_length_by_host", "warmup": 1, "runs": 3,
            "runs_s": [min_s] * 3, "errors": 0, "min_s": min_s, "median_s": min_s}

def report(c=None, p=None):
    r = copy.deepcopy(base)
    r["concurrent"], r["q29_probe"] = c, p
    return r

def one(rows, prefix):
    hits = [x["verdict"] for x in rows if x["band"].startswith(prefix)]
    return hits[0] if len(hits) == 1 else "%d rows" % len(hits)

def rel(b, prefix, a=None):
    a = a or report(conc(10.0, 0.01), probe(1.0))
    return one(kit.relative_bands(bands, a, b), prefix)

def ab(r, prefix):
    return one(kit.absolute_bands("rlog", bands, ["absolute"], r), prefix)

A = report(conc(10.0, 0.01), probe(1.0))
cases = {
    "relative.qps_min_ratio": [
        (rel(report(conc(9.6, 0.01)), "qps_pooled >="), "pass"),
        (rel(report(conc(9.4, 0.01)), "qps_pooled >="), "miss"),
        (rel(report(None, probe(1.0)), "qps_pooled >="), "inconclusive"),
        (rel(A, "qps_pooled >=", a=report(None, probe(1.0))), "inconclusive")],
    # The mean-of-rounds figure is set to flip each verdict, so a band that
    # read it would give the opposite answer.
    "relative.concurrent_error_pooled_max_delta": [
        (rel(report(conc(10.0, 0.025, mean=0.5)), "concurrent error_ratio_weighted <= A"), "pass"),
        (rel(report(conc(10.0, 0.035, mean=0.0)), "concurrent error_ratio_weighted <= A"), "miss"),
        (rel(report(None, probe(1.0)), "concurrent error_ratio_weighted <= A"), "inconclusive")],
    "relative.q29_probe_max_ratio": [
        (rel(report(conc(10.0, 0.01), probe(1.1)), "q29_probe min_s <= 1.15"), "pass"),
        (rel(report(conc(10.0, 0.01), probe(1.2)), "q29_probe min_s <= 1.15"), "miss"),
        (rel(report(conc(10.0, 0.01), None), "q29_probe min_s <= 1.15"), "inconclusive")],
    "absolute.concurrent_qps_min": [
        (ab(report(conc(2.0, 0.0)), "concurrent qps >="), "pass"),
        (ab(report(conc(0.5, 0.0)), "concurrent qps >="), "miss"),
        (ab(report(), "concurrent qps >="), "inconclusive")],
    "absolute.concurrent_error_pooled_max": [
        (ab(report(conc(2.0, 0.05, mean=0.5)), "concurrent error_ratio_weighted <= concurrent"), "pass"),
        (ab(report(conc(2.0, 0.2, mean=0.0)), "concurrent error_ratio_weighted <= concurrent"), "miss"),
        (ab(report(), "concurrent error_ratio_weighted <= concurrent"), "inconclusive")],
    "absolute.q29_probe_s_max": [
        (ab(report(p=probe(0.8)), "q29_probe min_s <= q29_probe_s_max"), "pass"),
        (ab(report(p=probe(1.5)), "q29_probe min_s <= q29_probe_s_max"), "miss"),
        (ab(report(), "q29_probe min_s <= q29_probe_s_max"), "inconclusive")],
}
for key in sorted(cases):
    got = [g for g, _ in cases[key]]
    want = [w for _, w in cases[key]]
    print("%s\t%s" % (key, "" if got == want else "got %s, want %s" % (got, want)))
PY
bandkeys=0
while IFS=$'\t' read -r key problem; do
  bandkeys=$((bandkeys + 1))
  # Catches: a new band key never judged, judged on the wrong figure, or
  # passing (rather than inconclusive) when its phase did not run.
  if [ -z "$problem" ]; then
    pass "band $key: pass, miss, and inconclusive with its phase absent"
  else
    fail "band $key" "$problem"
  fi
done <"$TMP/bandkeys.out"
[ "$bandkeys" = 6 ] || fail "band keys" "$(head -c 600 "$TMP/bandkeys.out")"

# The whole box with every phase on stubs: two rounds of two connections for
# 2 s each. Statement 02 is refused (422) and statement 01 answers 5 rows
# where the corpus expects 1, on every call; connection 2's order starts with
# them, so each round has at least one of each. The q29 probe's warmup is the
# fastest call, so a summary that took it in would report 0.1.
BOX_C="$TMP/box-conc"
code=0
rm -f "$STUB_DIR/perf-argv"
BOX_TOML_SED='s/^phases = .*/phases = ["benchmark", "concurrent", "q29_probe", "perf"]/
s/^concurrent_rounds = .*/concurrent_rounds = 2/
s/^concurrent_connections = .*/concurrent_connections = 2/
s/^concurrent_duration_s = .*/concurrent_duration_s = 2/
s/^perf_record = .*/perf_record = true/' \
  run_box A box-conc STUB_SQL_REFUSE_N=02 STUB_SQL_ROWS_N=01 STUB_SQL_ROWS=5 \
  'STUB_PROBE_TIMES=0.1 0.3 0.7 0.4' KIT_PERF_SETTLE=0.05 || code=$?
rm -rf "$TMP/fixture-conc"
mv "$RUN_DIR/out/A" "$TMP/fixture-conc"
if [ "$code" = 0 ] && [ -f "$TMP/fixture-conc/DONE" ] && [ -f "$TMP/fixture-conc/results.json" ]; then
  pass "a box with every phase finishes DONE"
else
  fail "box with every phase" "exit $code; $(head -n 3 "$TMP/fixture-conc/FAILED" 2>/dev/null); run.log: $(tail -n 5 "$BOX_C/kit/run.log")"
fi

# box_check PYTHON: PYTHON run against the box's results.json as r, its
# concurrent calls as calls, its q29 probe calls as probe; it prints nothing
# when the claim holds.
box_check() {
  python3 - "$TMP/fixture-conc/results.json" "$BOX_C/kit/out" "$1" <<'PY' 2>&1
import json, os, sys
r = json.load(open(sys.argv[1]))
def lines(p):
    return [json.loads(l) for l in open(os.path.join(sys.argv[2], p))]
calls = lines("concurrent/calls.jsonl")
probe = lines("q29_probe/calls.jsonl")
exec(sys.argv[3])
PY
}

# Catches: a concurrent call recorded without its round and connection, a
# round whose calls are not all in calls.jsonl, or connections that do not
# follow their seeded order: connection 1 starts q15 q24, connection 2 q02 q01.
out="$(box_check '
c = r["concurrent"]
if [x["round"] for x in c["rounds"]] != [1, 2] or c["connections"] != 2 or c["duration_s"] != 2:
    print("block", c["rounds"], c["connections"], c["duration_s"])
for x in calls:
    if x.get("phase") != "concurrent" or x.get("round") not in (1, 2) or x.get("connection") not in (1, 2):
        print("call", x)
for rd in c["rounds"]:
    n = sum(1 for x in calls if x["round"] == rd["round"])
    if n != rd["calls"] or n < 4:
        print("round", rd["round"], "has", n, "lines, reports", rd["calls"])
    for conn, first in ((1, ["q15", "q24"]), (2, ["q02", "q01"])):
        got = [x["statement"][:3] for x in calls if x["round"] == rd["round"] and x["connection"] == conn][:2]
        if got != first:
            print("round", rd["round"], "connection", conn, "starts", got)
if r["error_pooled"]["phases"] != ["benchmark"]:
    print("error_pooled", r["error_pooled"])')"
if [ -z "$out" ]; then
  pass "a stub round writes one line per call with its round and connection, in seeded order"
else
  fail "concurrent calls" "$out"
fi

# Catches: a box round's QPS taken from anything but its 2xx calls over the
# nominal duration_s, or a 2xx wrong answer counted as an error there.
out="$(box_check '
for rd in r["concurrent"]["rounds"]:
    mine = [x for x in calls if x["round"] == rd["round"]]
    ok = sum(1 for x in mine if 200 <= x["http_status"] <= 299 and x["curl_exit"] == 0)
    wrong = [x for x in mine if x["refusal_class"] == "wrong_answer"]
    if rd["ok"] != ok or rd["qps"] != ok / 2.0 or rd["wall_s"] < 1:
        print("round", rd)
    if not wrong or rd["wrong_answer"] != len(wrong) or any(x["http_status"] != 200 for x in wrong):
        print("wrong answers", rd["wrong_answer"], len(wrong))
    if rd["err"] != sum(1 for x in mine if x["http_status"] == 422) or rd["err"] < 1:
        print("errors", rd["err"])')"
if [ -z "$out" ]; then
  pass "a box round's QPS is its 2xx calls over 2 s, wrong answers among them"
else
  fail "box concurrent figures" "$out"
fi

# Catches: the q29 probe timing its warmup, skipping the stats, or reporting
# min and median over anything but the timed runs (0.3, 0.7, 0.4).
out="$(box_check '
if [x.get("probe") for x in probe] != ["warmup", "run", "run", "run"] or any("stats" not in x for x in probe):
    print("calls", probe)
if any(not x["statement"].startswith("q29_") for x in probe):
    print("statements", [x["statement"] for x in probe])
q = r["q29_probe"]
want = {"warmup": 1, "runs": 3, "runs_s": [0.3, 0.7, 0.4], "errors": 0, "min_s": 0.3, "median_s": 0.4}
for k, v in want.items():
    if q[k] != v:
        print(k, q[k], "want", v)')"
if [ -z "$out" ]; then
  pass "the q29 probe logs warmup and runs, with min and median from the timed runs only"
else
  fail "q29 probe" "$out"
fi

# Catches: perf recording another process or at another rate, perf.data kept
# instead of folded stacks, or the stacks left out of the evidence.
perf_lines="$(grep -c '^perf record -F 99 -g -p [0-9][0-9]* -o .*/out/perf/[0-9][0-9]\.data$' "$STUB_DIR/perf-argv" 2>/dev/null)"
folded="$(cat "$BOX_C"/kit/out/perf/*.folded 2>/dev/null | sort | uniq -c | sed 's/^ *//')"
in_tgz="$(tar -tzf "$TMP/fixture-conc/evidence.tgz" | grep -c '^out/perf/[0-9][0-9]\.folded$')"
profiled="$(grep -c '"profiled": true' "$BOX_C/kit/out/perf/calls.jsonl")"
if [ "$perf_lines" = 43 ] && [ "$folded" = "43 ravel-server;main;execute;scan_rows 2" ] && [ "$in_tgz" = 43 ] \
  && [ "$profiled" = 43 ] && ! ls "$BOX_C"/kit/out/perf/*.data >/dev/null 2>&1; then
  pass "perf records each statement's hot call and keeps folded stacks in the evidence"
else
  fail "perf" "record lines $perf_lines, folded '$folded', in evidence $in_tgz, profiled $profiled"
fi

# Catches: fetch printing no per-round figures, or a share table on a
# concurrent error miss that does not add up to the arm's concurrent errors.
# Arm A is the box above with its concurrent errors edited away and the band
# file's delta set to 0, so arm B, the same box, misses on its 422s.
edit_arm "$TMP/fixture-conc" "$TMP/fixture-share-a" '
r["concurrent"]["pooled"].update(err=0, error_ratio_weighted=0.0)
for b in r["bands"]:
    if b["band"].startswith("concurrent error"):
        b["verdict"] = "pass"'
as_arm_b "$TMP/fixture-conc" "$TMP/fixture-share-b"
fixture run-share "$TMP/fixture-share-a" "$TMP/fixture-share-b"
SHARE_RUN="$FAKE_S3/kit-test-bucket/clickbench-kit/runs/run-share"
rm -rf "$TMP/share-kit"
mkdir -p "$TMP/share-kit"
tar -xzf "$SHARE_RUN/in/kit.tgz" -C "$TMP/share-kit"
sed 's/^concurrent_error_pooled_max_delta = .*/concurrent_error_pooled_max_delta = 0/' \
  "$TMP/share-kit/kit/bands/rlog.toml" >"$TMP/share-kit/rlog.toml"
mv "$TMP/share-kit/rlog.toml" "$TMP/share-kit/kit/bands/rlog.toml"
tar -czf "$SHARE_RUN/in/kit.tgz" -C "$TMP/share-kit" kit
fetch share run-share --timeout 30
out="$(python3 - "$TMP/fetch-share.out" "$TMP/fixture-share-b/results.json" <<'PY' 2>&1
import re, sys
text = open(sys.argv[1]).read()
import json
p = json.load(open(sys.argv[2]))["concurrent"]["pooled"]
if not re.search(r"^MISS +B vs A +concurrent error_ratio_weighted <= A \+ 0 ", text, re.M):
    print("no concurrent error miss")
for arm in "AB":
    for n in (1, 2):
        if not re.search(r"^concurrent %s round %d: qps \S+ error_ratio \S+ \(ok \d+" % (arm, n), text, re.M):
            print("no round %d line for %s" % (n, arm))
    if not re.search(r"^concurrent %s pooled: qps_pooled \S+ error_ratio_weighted \S+ "
                     r"error_ratio_mean_of_rounds \S+" % arm, text, re.M):
        print("no pooled line for", arm)
m = re.search(r"^concurrent B errors by statement \((\d+) errors of (\d+) calls\):\n((?:  .*\n)*)", text, re.M)
if not m:
    print("no share table for B")
else:
    rows = re.findall(r"^  (\S+) +(\d+) / (\d+)$", m.group(3), re.M)
    if int(m.group(1)) != p["err"] or int(m.group(2)) != p["calls"]:
        print("header", m.group(1), m.group(2), "want", p["err"], p["calls"])
    if sum(int(n) for _, n, _ in rows) != p["err"] or any(int(t) != p["err"] for _, _, t in rows):
        print("rows", rows, "do not sum to", p["err"])
    if [s for s, _, _ in rows] != ["q02_count_adv_engine"]:
        print("statements", rows)
if "concurrent A errors by statement" in text:
    print("a share table for A, which did not miss")
PY
)"
if [ "$code" = 1 ] && [ -z "$out" ]; then
  pass "fetch prints per-round figures and, on a concurrent error miss, a share table summing to its errors"
else
  fail "share table" "exit $code: $out $(tail -n 3 "$TMP/fetch-share.err")"
fi

# Catches: fetch comparing the arms without the run's own run.toml, whose
# declared extra flags decide what it accepts.
fixture run-notoml "$TMP/fixture-share-a" "$TMP/fixture-share-b"
rm "$FAKE_S3/kit-test-bucket/clickbench-kit/runs/run-notoml/in/run.toml"
fetch notoml run-notoml --timeout 30
if [ "$code" = 2 ] && grep -q '^fetch: .*/runs/run-notoml/in/run.toml is not there$' "$TMP/fetch-notoml.err" \
  && ! grep -q 'B vs A' "$TMP/fetch-notoml.out"; then
  pass "fetch exits 2 when the run's in/run.toml is missing, before comparing the arms"
else
  fail "fetch without in/run.toml" "exit $code: $(tail -n 2 "$TMP/fetch-notoml.err")"
fi

# trunc_evidence SRC DST EDIT: a copy of arm SRC whose evidence.tgz has
# out/concurrent/calls.jsonl replaced: EDIT "truncate" cuts its third line
# short, "drop" leaves the file out.
trunc_evidence() {
  local src="$1" dst="$2" edit="$3"
  rm -rf "$dst" "$TMP/trunc-x"
  cp -R "$src" "$dst"
  mkdir -p "$TMP/trunc-x"
  tar -xzf "$src/evidence.tgz" -C "$TMP/trunc-x"
  if [ "$edit" = truncate ]; then
    awk 'NR == 3 { print substr($0, 1, 40); next } { print }' "$TMP/trunc-x/out/concurrent/calls.jsonl" \
      >"$TMP/trunc-x/calls.jsonl"
    mv "$TMP/trunc-x/calls.jsonl" "$TMP/trunc-x/out/concurrent/calls.jsonl"
  else
    rm "$TMP/trunc-x/out/concurrent/calls.jsonl"
  fi
  # shellcheck disable=SC2046
  tar -czf "$dst/evidence.tgz" -C "$TMP/trunc-x" $(ls "$TMP/trunc-x")
}

# Catches: a truncated calls.jsonl line read as something other than a
# refusal naming its file and line, and fetch letting a concurrent-detail
# failure (the line above, or calls.jsonl missing from the evidence) change
# its exit code or pass without a warning. The run misses on B's concurrent
# errors as the share table case does, so fetch must still exit 1.
for edit in truncate drop; do
  trunc_evidence "$TMP/fixture-share-b" "$TMP/fixture-$edit-b" "$edit"
  fixture "run-$edit" "$TMP/fixture-share-a" "$TMP/fixture-$edit-b"
  cp "$SHARE_RUN/in/kit.tgz" "$FAKE_S3/kit-test-bucket/clickbench-kit/runs/run-$edit/in/kit.tgz"
  fetch "$edit" "run-$edit" --timeout 30
  case "$edit" in
    truncate) want='B/evidence.tgz:out/concurrent/calls.jsonl:3 is not a JSON line: ' ;;
    drop) want="cannot read out/concurrent/calls.jsonl from $TMP/fetch-drop/B/evidence.tgz: " ;;
  esac
  if [ "$code" = 1 ] && grep -qF "$want" "$TMP/fetch-$edit.err" \
    && [ "$(grep -cxF 'fetch: warning: could not print the concurrent errors by statement (see above); the band verdict stands' "$TMP/fetch-$edit.err")" = 1 ] \
    && grep -Eq '^MISS +B vs A +concurrent error_ratio_weighted <= A \+ 0 ' "$TMP/fetch-$edit.out" \
    && grep -q '^fetch: at least one band MISSED' "$TMP/fetch-$edit.err"; then
    pass "fetch with B's evidence calls.jsonl edited ($edit) warns once, names the cause and keeps exit 1"
  else
    fail "fetch $edit calls.jsonl" "exit $code: $(tail -n 4 "$TMP/fetch-$edit.err")"
  fi
done

# lib_box BOX: boot an arm A box in BOX and write BOX/lib-setup.sh, which a
# case's subshell sources: run.sh in library mode with its config loaded, the
# stub server installed, arm.env written and the query window set.
lib_box() {
  local box="$1"
  reset_log
  rm -f "$STUB_DIR/readyz-calls" "$STUB_DIR/check-calls" "$STUB_DIR/ready-delay" \
    "$STUB_DIR/check-refuse" "$STUB_DIR/check-hang" "$STUB_DIR/server-ready" \
    "$STUB_DIR/ready-after-checks"
  boot_box A "$box" || return 1
  cat >"$box/lib-setup.sh" <<SETUP
export KIT_ROOT="$box" KIT_RUN_SH_LIB=1 KIT_READY_TRIES=100 KIT_READY_INTERVAL=0.02
export KIT_DROP_CACHES_FILE="$box/drop_caches"
. "$box/kit/kit/box/run.sh"
mkdir -p "\$STAMPS"
load_config || exit 1
mkdir -p "\$BIN_DIR"
cp "$BINSTUBS/ravel-server" "\$BIN_DIR/ravel-server-\$SHA"
resolve_arm_env || exit 1
set_query_window
SETUP
}

# conc_outage_check BOX DURATION: prints nothing when BOX's one concurrent
# round parsed, ended at its deadline with exactly one watchdog restart, and
# never polled readyz.
conc_outage_check() {
  python3 - "$1/kit/out/concurrent" "$2" "$STUB_DIR" <<'PY' 2>&1
import json, os, sys
d, duration, stub = sys.argv[1], int(sys.argv[2]), sys.argv[3]
calls = [json.loads(l) for l in open(os.path.join(d, "calls.jsonl"))]
s = json.load(open(os.path.join(d, "summary.json")))
rd = s["rounds"][0]
bad = [c for c in calls if not (200 <= c["http_status"] <= 299 and c["curl_exit"] == 0)]
if (s["pooled"]["calls"], s["pooled"]["err"]) != (len(calls), len(bad)):
    print("summary", s["pooled"], "calls", len(calls), "errors", len(bad))
log = open(os.path.join(d, "round-1", "watchdog.log")).read()
if log.count("restart n=") != 1 or "restart failed" in log:
    print("watchdog log", repr(log))
if not duration - 1 <= rd["wall_s"] < duration + 1.5:
    print("wall_s", rd["wall_s"], "for a", duration, "s round")
if os.path.exists(os.path.join(stub, "readyz-calls")):
    print("readyz polled", open(os.path.join(stub, "readyz-calls")).read().count("readyz"), "times")
if open(os.path.join(stub, "check-calls")).read().count("check") < 2:
    print("SELECT 1 check not used")
PY
}

# Catches: run_query writing curl's "000" as the status of a call to a
# server that is down (invalid JSON, so the concurrent summary refuses the
# file and one outage fails the run), a watchdog that does not restart a dead
# server exactly once, a round that does not end at its whole-second deadline
# with the watchdog collected, and readiness read from readyz. The server dies
# 0.5 s into the round and its restart takes 1 s, so calls in between are
# errors.
BOX_O="$TMP/box-outage"
lib_box "$BOX_O"
(
  # shellcheck source=/dev/null
  . "$BOX_O/lib-setup.sh"
  CONC_ROUNDS=1 CONC_CONNECTIONS=2 CONC_DURATION=5
  WATCHDOG_INTERVAL=0.3
  (
    until grep -q 'concurrent round=1' "$PROGRESS" 2>/dev/null; do sleep 0.05; done
    sleep 0.5
    echo 1.0 >"$STUB_DIR/ready-delay"
    kill -TERM "$(cat "$KIT/server.pid")"
  ) &
  killer=$!
  phase_concurrent || exit 1
  wait "$killer"
  rm -f "$STUB_DIR/ready-delay"
  stop_server
) >"$TMP/outage.log" 2>&1
code=$?
rm -f "$STUB_DIR/ready-delay"
out="$(conc_outage_check "$BOX_O" 5)"
down="$(python3 - "$BOX_O/kit/out/concurrent/calls.jsonl" <<'PY' 2>&1
import json, sys
calls = [json.loads(l) for l in open(sys.argv[1])]
down = [i for i, c in enumerate(calls) if c["http_status"] == 0]
if not down or any((calls[i]["outcome"], calls[i]["curl_exit"]) != ("unreachable", 7) for i in down):
    print("calls during the outage", [calls[i] for i in down][:3])
by_conn = {}
for c in calls:
    by_conn.setdefault(c["connection"], []).append(c)
for conn, mine in by_conn.items():
    last_down = max([i for i, c in enumerate(mine) if c["http_status"] == 0] or [-1])
    if not any(c["outcome"] == "ok" for c in mine[last_down + 1:]):
        print("connection", conn, "has no success after the outage")
PY
)"
if [ "$code" = 0 ] && [ -z "$out" ] && [ -z "$down" ]; then
  pass "a server dying mid-round: outage calls are errors with status 0, one restart, the round ends at its deadline"
else
  fail "concurrent outage" "exit $code: $out $down $(tail -n 3 "$TMP/outage.log")"
fi

# Catches: a watchdog or pre-round poll that reads readyz instead of
# upstream's SELECT 1 check: here SELECT 1 is refused (memory pressure) while
# readyz would answer, and upstream restarts the server.
BOX_M="$TMP/box-check-refuse"
lib_box "$BOX_M"
(
  # shellcheck source=/dev/null
  . "$BOX_M/lib-setup.sh"
  CONC_ROUNDS=1 CONC_CONNECTIONS=1 CONC_DURATION=3
  WATCHDOG_INTERVAL=0.3
  (
    until grep -q 'concurrent round=1' "$PROGRESS" 2>/dev/null; do sleep 0.05; done
    sleep 0.2
    touch "$STUB_DIR/check-refuse"
  ) &
  killer=$!
  phase_concurrent || exit 1
  wait "$killer"
  stop_server
) >"$TMP/check-refuse.log" 2>&1
code=$?
out="$(conc_outage_check "$BOX_M" 3)"
if [ "$code" = 0 ] && [ -z "$out" ] && [ ! -e "$STUB_DIR/check-refuse" ]; then
  pass "a refused SELECT 1 check restarts the server once; readyz is never polled"
else
  fail "SELECT 1 check" "exit $code: $out $(tail -n 3 "$TMP/check-refuse.log")"
fi

# kill_tree PID: SIGKILL PID and every process below it.
kill_tree() {
  local child
  for child in $(pgrep -P "$1"); do kill_tree "$child"; done
  kill -KILL "$1" 2>/dev/null
}

# Catches: a SELECT 1 check with no client time limit. A server that accepts
# the connection and never answers then blocks the watchdog in the check
# forever, so it never restarts the server or reaches its deadline, and the
# round never ends. Here the check starts hanging 0.2 s into a 3 s round; the
# case gives the round 20 s before calling it hung and killing it.
BOX_H="$TMP/box-check-hang"
lib_box "$BOX_H"
(
  # shellcheck source=/dev/null
  . "$BOX_H/lib-setup.sh"
  CONC_ROUNDS=1 CONC_CONNECTIONS=1 CONC_DURATION=3
  WATCHDOG_INTERVAL=0.3
  (
    until grep -q 'concurrent round=1' "$PROGRESS" 2>/dev/null; do sleep 0.05; done
    sleep 0.2
    touch "$STUB_DIR/check-hang"
  ) &
  killer=$!
  phase_concurrent || exit 1
  wait "$killer"
  stop_server
) >"$TMP/check-hang.log" 2>&1 &
hang_pid=$!
waited=0
while kill -0 "$hang_pid" 2>/dev/null && [ "$waited" -lt 200 ]; do
  sleep 0.1
  waited=$((waited + 1))
done
if kill -0 "$hang_pid" 2>/dev/null; then
  kill_tree "$hang_pid"
  wait "$hang_pid" 2>/dev/null
  code="hung for 20 s"
else
  code=0
  wait "$hang_pid" || code=$?
fi
out="$(conc_outage_check "$BOX_H" 3)"
if [ "$code" = 0 ] && [ -z "$out" ] && [ ! -e "$STUB_DIR/check-hang" ]; then
  pass "a SELECT 1 check that never answers times out, restarts the server once, and the round ends at its deadline"
else
  fail "hung SELECT 1 check" "exit $code: $out $(tail -n 3 "$TMP/check-hang.log")"
fi
rm -f "$STUB_DIR/check-hang"

# Catches: a watchdog whose restart waits less than the kit's own startup
# (READY_TRIES): a server that needs more polls than that to come up is given
# up on, stopped by the next failed check while it is still booting, and
# restarted again until the round ends, so the whole round reads as an outage.
# The server dies 0.5 s into the round and its restart answers only after 70
# checks: more than upstream's 60, fewer than lib_box's KIT_READY_TRIES of 100.
BOX_S="$TMP/box-slow-restart"
lib_box "$BOX_S"
(
  # shellcheck source=/dev/null
  . "$BOX_S/lib-setup.sh"
  CONC_ROUNDS=1 CONC_CONNECTIONS=2 CONC_DURATION=10
  WATCHDOG_INTERVAL=0.3
  (
    until grep -q 'concurrent round=1' "$PROGRESS" 2>/dev/null; do sleep 0.05; done
    sleep 0.5
    echo 70 >"$STUB_DIR/ready-after-checks"
    kill -TERM "$(cat "$KIT/server.pid")"
  ) &
  killer=$!
  phase_concurrent || exit 1
  wait "$killer"
  rm -f "$STUB_DIR/ready-after-checks"
  stop_server
) >"$TMP/slow-restart.log" 2>&1
code=$?
rm -f "$STUB_DIR/ready-after-checks"
out="$(conc_outage_check "$BOX_S" 10)"
starts="$(grep -c 'server start' "$BOX_S/kit/progress.log")"
ok_after="$(python3 - "$BOX_S/kit/out/concurrent/calls.jsonl" <<'PY2' 2>&1
import json, sys
calls = [json.loads(l) for l in open(sys.argv[1])]
for conn in sorted(set(c["connection"] for c in calls)):
    mine = [c for c in calls if c["connection"] == conn]
    last_down = max([i for i, c in enumerate(mine) if c["http_status"] == 0] or [-1])
    if last_down < 0 or not any(c["outcome"] == "ok" for c in mine[last_down + 1:]):
        print("connection", conn, "saw no outage, or no success after it")
PY2
)"
# The healthz gate stays open across the watchdog's restart, so the outage
# shows as failed samples on both listeners.
hz_fail="$(sed -En 's/^HEALTHZ phase=concurrent-1 port=43(18|19) .* failures=([0-9]+)$/\2/p' \
  "$BOX_S/kit/out/healthz.txt" 2>/dev/null | tr '\n' ' ')"
if [ "$code" = 0 ] && [ -z "$out" ] && [ "$starts" = 2 ] && [ -z "$ok_after" ] \
  && [[ "$hz_fail" =~ ^[1-9][0-9]*\ [1-9][0-9]*\ $ ]]; then
  pass "a restart slower than 60 polls is waited for, not stopped while it boots, calls succeed once it is up, and healthz counts the outage as failures"
else
  fail "slow watchdog restart" "exit $code, $starts server starts, healthz failures '$hz_fail': $out $ok_after $(grep -c restart "$BOX_S/kit/out/concurrent/round-1/watchdog.log") restarts; $(tail -n 3 "$TMP/slow-restart.log")"
fi

# Catches: a failed concurrent call leaving its curl stderr file behind. A
# connection to a dead endpoint fails fast and loops, so one outage leaves a
# file per call under out/concurrent, and write_evidence copies and scrubs
# them one by one, too slowly for evidence.tgz to be written before the box
# shuts down. Also catches evidence carrying the per-connection response
# directories at all, and a deferred call's error losing the curl stderr
# concurrent-answers quotes after the round. Every measured call here is
# refused at once.
BOX_D="$TMP/box-dead-endpoint"
lib_box "$BOX_D"
(
  # shellcheck source=/dev/null
  . "$BOX_D/lib-setup.sh"
  export STUB_SQL_DEAD=1
  CONC_ROUNDS=1 CONC_CONNECTIONS=2 CONC_DURATION=2
  phase_concurrent || exit 1
  stop_server
  write_evidence || exit 1
) >"$TMP/dead-endpoint.log" 2>&1
code=$?
dcalls="$(wc -l <"$BOX_D/kit/out/concurrent/calls.jsonl" 2>/dev/null | tr -d ' ')"
dfailed="$(grep -c '"curl_exit": 7, .*"error": "curl exit 7: curl: (7) Failed to connect to 127.0.0.1 port 4318"' \
  "$BOX_D/kit/out/concurrent/calls.jsonl" 2>/dev/null)"
left="$(find "$BOX_D/kit/out/concurrent" -type f \( -name '*.curl' -o -path '*/conn-*/*' \) | head -n 3)"
tar -tzf "$BOX_D/kit/evidence.tgz" >"$TMP/dead-endpoint.tar" 2>&1 || echo "no evidence.tgz" >>"$TMP/dead-endpoint.tar"
in_tar="$(grep -E '\.curl$|/conn-[0-9]+/|no evidence' "$TMP/dead-endpoint.tar" | head -n 3)"
if [ "$code" = 0 ] && [ "${dcalls:-0}" -ge 50 ] && [ "$dfailed" = "$dcalls" ] && [ -z "$left" ] \
  && grep -q '^out/concurrent/calls.jsonl$' "$TMP/dead-endpoint.tar" && [ -z "$in_tar" ]; then
  pass "a dead endpoint's failed concurrent calls each quote their curl stderr, leave no per-call file, and evidence.tgz is written without them"
else
  fail "dead endpoint cleanup" "exit $code, $dfailed of $dcalls calls failed; left: $left; in evidence: $in_tar; $(tail -n 3 "$TMP/dead-endpoint.log")"
fi

# Catches: run_query recording an answer whose status line said 200 but whose
# transfer then failed as a success, or without its curl exit code.
BOX_T="$TMP/box-transfer"
lib_box "$BOX_T"
(
  # shellcheck source=/dev/null
  . "$BOX_T/lib-setup.sh"
  export STUB_SQL_TRUNCATE_N=05
  phase_requests "$OUT/concurrent" || exit 1
  start_server || exit 1
  RQ_MAX_TIME="" RQ_EXTRA='"round": 1, "connection": 1' run_query concurrent 05 q05 1 \
    "$OUT/concurrent/requests/05.json" || exit 1
  RQ_MAX_TIME="" RQ_EXTRA='"round": 1, "connection": 1' run_query concurrent 06 q06 2 \
    "$OUT/concurrent/requests/06.json" || exit 1
  stop_server
) >"$TMP/transfer.log" 2>&1
code=$?
out="$(python3 - "$KIT_SRC/lib" "$BOX_T/kit/out/concurrent/calls.jsonl" <<'PY' 2>&1
import json, sys
sys.path.insert(0, sys.argv[1])
import kit
calls = [json.loads(l) for l in open(sys.argv[2])]
t = calls[0]
if (t["http_status"], t["curl_exit"], t["outcome"], t["refusal_class"]) != (200, 18, "error", "transfer"):
    print("truncated call", t)
if (calls[1]["http_status"], calls[1]["curl_exit"], calls[1]["outcome"]) != (200, 0, "ok"):
    print("good call", calls[1])
rd = kit.concurrent_round({"round": 1, "duration_s": 1, "wall_s": 1.0, "connections": 1}, calls)
if (rd["ok"], rd["err"]) != (1, 1):
    print("round", rd)
PY
)"
if [ "$code" = 0 ] && [ -z "$out" ]; then
  pass "run_query records a 200 with a failed transfer as curl_exit 18, an error in the concurrent figures"
else
  fail "run_query transfer" "exit $code: $out $(tail -n 3 "$TMP/transfer.log")"
fi

# Catches: a concurrent connection starting python3 (to count an answer's rows
# or classify a refusal) inside the round's timed window, where it competes
# with the server for cores and deflates the QPS, or the deferred reading
# after the round losing wrong_answer, a refusal's class, or a pending marker.
# A python3 stub first on PATH logs every invocation made while any
# connection's loop is running. Statement 01 answers 5 rows where the corpus
# expects 1 and statement 02 is refused (422). TZ is not UTC, so a call's
# start stamped in local time is caught too.
mkdir -p "$TMP/pystub"
cat >"$TMP/pystub/python3" <<STUB
#!/usr/bin/env bash
for f in "\$STUB_DIR"/window-*; do
  [ ! -e "\$f" ] || { echo "python3 \$*" >>"\$STUB_DIR/py-window"; break; }
done
exec "$(command -v python3)" "\$@"
STUB
chmod +x "$TMP/pystub/python3"
BOX_Y="$TMP/box-py-window"
lib_box "$BOX_Y"
rm -f "$STUB_DIR"/window-* "$STUB_DIR/py-window"
(
  # shellcheck source=/dev/null
  . "$BOX_Y/lib-setup.sh"
  export PATH="$TMP/pystub:$PATH" STUB_SQL_ROWS_N=01 STUB_SQL_ROWS=5 STUB_SQL_REFUSE_N=02 STUB_SQL_TIME=0.05 TZ=Asia/Tokyo
  CONC_ROUNDS=1 CONC_CONNECTIONS=2 CONC_DURATION=2
  eval "orig_$(declare -f concurrent_worker)"
  concurrent_worker() {
    local rc=0
    touch "$STUB_DIR/window-$2"
    orig_concurrent_worker "$@" || rc=$?
    rm -f "$STUB_DIR/window-$2"
    return "$rc"
  }
  phase_concurrent || exit 1
  stop_server
) >"$TMP/py-window.log" 2>&1
code=$?
in_window=0
[ ! -f "$STUB_DIR/py-window" ] || in_window="$(wc -l <"$STUB_DIR/py-window" | tr -d ' ')"
out="$(python3 - "$BOX_Y/kit/out/concurrent" <<'PY' 2>&1
import json, os, sys
d = sys.argv[1]
calls = [json.loads(l) for l in open(os.path.join(d, "calls.jsonl"))]
s = json.load(open(os.path.join(d, "summary.json")))
wrong = [c for c in calls if c["refusal_class"] == "wrong_answer"]
if not wrong or any((c["statement"][:3], c["rows"], c["outcome"], c["error"]) !=
                    ("q01", 5, "error", "answer has 5 rows, expected 1") for c in wrong):
    print("wrong answers", wrong[:2])
if s["rounds"][0]["wrong_answer"] != len(wrong):
    print("summary wrong_answer", s["rounds"][0]["wrong_answer"], "of", len(wrong))
refused = [c for c in calls if c["http_status"] == 422]
if not refused or any((c["refusal_class"], c["error"]) != ("resource_exhausted", "query pool exhausted")
                      for c in refused):
    print("refusals", refused[:2])
import calendar, time
skew = [c["start"] for c in calls
        if abs(calendar.timegm(time.strptime(c["start"], "%Y-%m-%dT%H:%M:%SZ")) - time.time()) > 300]
if skew:
    print("start not UTC under TZ=Asia/Tokyo", skew[:1])
if any("pending" in c for c in calls):
    print("a pending member left in calls.jsonl")
oks = [c for c in calls if c["outcome"] == "ok"]
if not oks or any(not isinstance(c["rows"], int) or c["refusal_class"] != "" for c in oks):
    print("ok calls", oks[:2])
rdir = os.path.join(d, "round-1")
for c in calls:
    f = os.path.join(rdir, "conn-%d" % c["connection"], "%02d-%d.json" % (c["number"], c["try"]))
    if os.path.exists(f) != (c["outcome"] != "ok" or c["try"] == 1):
        print("response file", f, "exists" if os.path.exists(f) else "missing", "for", c["outcome"])
        break
PY
)"
hz_n="$(sed -n 's/^HEALTHZ phase=concurrent-1 port=4319 n=\([0-9]*\) .*/\1/p' "$BOX_Y/kit/out/healthz.txt" 2>/dev/null)"
if [ "$code" = 0 ] && [ "$in_window" = 0 ] && [ -z "$out" ] && [ "${hz_n:-0}" -ge 4 ] \
  && grep -Eq '^HEARTBEAT phase=concurrent-1 n=[1-9][0-9]* failures=0 bad=0 max_s=0.500$' "$BOX_Y/kit/out/healthz.txt"; then
  pass "a concurrent round starts no python3 in its timed window, with the healthz sampler running through it, and still reports wrong answers after it"
else
  fail "concurrent harness cost" "exit $code, python3 started $in_window times in the window, healthz n '$hz_n': $(head -n 2 "$STUB_DIR/py-window" 2>/dev/null) $out $(tail -n 3 "$TMP/py-window.log")"
fi

# Catches: a client timeout's error naming a limit the call never had: the
# concurrent phase sets none, so its error says so, and a call with a limit
# names that limit rather than query_timeout_s.
BOX_X="$TMP/box-timeout-text"
lib_box "$BOX_X"
(
  # shellcheck source=/dev/null
  . "$BOX_X/lib-setup.sh"
  export STUB_SQL_TIMEOUT_N=07
  phase_requests "$OUT/concurrent" || exit 1
  start_server || exit 1
  RQ_MAX_TIME="" run_query concurrent 07 q07 1 "$OUT/concurrent/requests/07.json" || exit 1
  RQ_MAX_TIME=7 run_query concurrent 07 q07 2 "$OUT/concurrent/requests/07.json" || exit 1
  stop_server
) >"$TMP/timeout-text.log" 2>&1
code=$?
out="$(python3 - "$BOX_X/kit/out/concurrent/calls.jsonl" <<'PY' 2>&1
import json, sys
calls = [json.loads(l) for l in open(sys.argv[1])]
want = [("timeout", 28, "transfer timed out (curl exit 28) with no client time limit set"),
        ("timeout", 28, "no answer within 7 s")]
got = [(c["outcome"], c["curl_exit"], c["error"]) for c in calls]
if got != want:
    print("got", got)
PY
)"
if [ "$code" = 0 ] && [ -z "$out" ]; then
  pass "a client timeout names the call's own limit, or says it had none"
else
  fail "timeout error text" "exit $code: $out $(tail -n 3 "$TMP/timeout-text.log")"
fi

# Catches: a deferred call reading a 2xx other than 200 as an error, a
# deferred timeout forking rm inside the round or quoting its curl stderr
# when run_query would not, and concurrent-answers leaving a quoted stderr
# file behind. Statement 06 answers 203, statement 07 times out, both under
# RQ_DEFER=1, then concurrent-answers settles the connection's file.
BOX_V="$TMP/box-defer-2xx"
lib_box "$BOX_V"
(
  # shellcheck source=/dev/null
  . "$BOX_V/lib-setup.sh"
  export STUB_SQL_TIMEOUT_N=07 STUB_SQL_STATUS=203
  phase_requests "$OUT/concurrent" || exit 1
  start_server || exit 1
  d="$OUT/concurrent/round-1"
  for n in 06 07; do
    RQ_DEFER=1 RQ_MAX_TIME="" RQ_CALLS="$d/conn-1.jsonl" RQ_RESP_DIR="$d/conn-1" \
      run_query concurrent "$n" "q$n" 1 "$OUT/concurrent/requests/$n.json" || exit 1
  done
  [ -s "$d/conn-1/07-1.curl" ] || { echo "the deferred timeout's stderr file is gone before the round ends"; exit 1; }
  kit_py concurrent-answers "$d" || exit 1
  stop_server
) >"$TMP/defer-2xx.log" 2>&1
code=$?
out="$(python3 - "$BOX_V/kit/out/concurrent/round-1/conn-1.jsonl" <<'PY' 2>&1
import json, sys
calls = [json.loads(l) for l in open(sys.argv[1])]
got = [(c["http_status"], c["curl_exit"], c["outcome"], c["error"], "pending" in c) for c in calls]
want = [(203, 0, "ok", "", False),
        (0, 28, "timeout", "transfer timed out (curl exit 28) with no client time limit set", False)]
if got != want:
    print("got", got)
PY
)"
left="$(find "$BOX_V/kit/out/concurrent/round-1" -name '*.curl' -size +0 | head -n 2)"
if [ "$code" = 0 ] && [ -z "$out" ] && [ -z "$left" ]; then
  pass "a deferred 203 answer is ok, and a deferred timeout keeps run_query's error and leaves no stderr file"
else
  fail "deferred 2xx and timeout" "exit $code: $out left: $left $(tail -n 3 "$TMP/defer-2xx.log")"
fi

# Catches: a perf phase that fails after perf record and leaves the raw
# perf.data in out/perf/, which evidence.tgz would then carry.
BOX_F="$TMP/box-perf-fail"
lib_box "$BOX_F"
(
  # shellcheck source=/dev/null
  . "$BOX_F/lib-setup.sh"
  PERF_RECORD=true PERF_SETTLE=0.05
  export STUB_PERF_SCRIPT_FAIL=1
  mkdir -p "$OUT/perf"
  phase_perf && exit 1
  stop_server
  exit 0
) >"$TMP/perf-fail.log" 2>&1
code=$?
if [ "$code" = 0 ] && [ -f "$BOX_F/kit/out/perf/01.record.log" ] \
  && ! ls "$BOX_F"/kit/out/perf/*.data >/dev/null 2>&1; then
  pass "a failed perf script leaves no perf.data behind"
else
  fail "perf cleanup" "exit $code; left: $(ls "$BOX_F/kit/out/perf" 2>&1 | tr '\n' ' ')"
fi

# Catches: a healthz sampler outliving its phase. healthz_stop must stop the
# python3 process itself, not a subshell wrapping it, so after healthz_finish,
# and after fail_run with a sampler running, no process whose argv holds that
# phase's HEALTHZ_DIR may remain. One that ignores SIGTERM is SIGKILLed after
# the bounded wait, and run.log says so.
BOX_H="$TMP/box-hz-stop"
lib_box "$BOX_H"
(
  # shellcheck source=/dev/null
  . "$BOX_H/lib-setup.sh"
  touch "$STUB_DIR/server-ready"
  : >"$OUT/healthz.txt"
  healthz_start benchmark "$OUT/hz-finish" || exit 1
  healthz_up || exit 1
  healthz_finish benchmark || exit 1
  pgrep -f "$OUT/hz-finish" >"$BOX_H/left-finish"
  exit 0
) >"$TMP/hz-finish.log" 2>&1
finish_code=$?
(
  # shellcheck source=/dev/null
  . "$BOX_H/lib-setup.sh"
  touch "$STUB_DIR/server-ready"
  healthz_start benchmark "$OUT/hz-failrun" || exit 3
  healthz_up || exit 3
  fail_run healthz-unit
) >"$TMP/hz-failrun.log" 2>&1
failrun_code=$?
rm -f "$STUB_DIR/server-ready"
left_failrun="$(pgrep -f "$BOX_H/kit/out/hz-failrun")"
cat >"$TMP/stubborn-kit.py" <<'PY'
import os, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
open(os.path.join(sys.argv[2], "ignoring"), "w").close()
time.sleep(60)
PY
(
  # shellcheck source=/dev/null
  . "$BOX_H/lib-setup.sh"
  # shellcheck disable=SC2034 # read by healthz_start
  PY_KIT="$TMP/stubborn-kit.py"
  healthz_start benchmark "$OUT/hz-stubborn" || exit 1
  for _ in $(seq 1 500); do [ -e "$OUT/hz-stubborn/ignoring" ] && break; sleep 0.01; done
  [ -e "$OUT/hz-stubborn/ignoring" ] || exit 1
  healthz_stop
  pgrep -f "$OUT/hz-stubborn" >"$BOX_H/left-stubborn"
  exit 0
) >"$TMP/hz-stubborn.log" 2>&1
stubborn_code=$?
if [ "$finish_code" = 0 ] && [ -f "$BOX_H/left-finish" ] && [ ! -s "$BOX_H/left-finish" ] \
  && grep -q '^HEARTBEAT phase=benchmark n=' "$BOX_H/kit/out/healthz.txt" \
  && [ "$failrun_code" = 1 ] && grep -q '^FAILED step=healthz-unit ' "$BOX_H/kit/FAILED" 2>/dev/null \
  && [ -z "$left_failrun" ] \
  && [ "$stubborn_code" = 0 ] && [ -f "$BOX_H/left-stubborn" ] && [ ! -s "$BOX_H/left-stubborn" ] \
  && grep -q 'still running 5 s after SIGTERM; sending SIGKILL' "$TMP/hz-stubborn.log"; then
  pass "no healthz sampler outlives healthz_finish or fail_run, and one ignoring SIGTERM is SIGKILLed and logged"
else
  fail "healthz sampler lifetime" "finish exit $finish_code left '$(tr '\n' ' ' <"$BOX_H/left-finish" 2>/dev/null)', fail_run exit $failrun_code left '$left_failrun', stubborn exit $stubborn_code left '$(tr '\n' ' ' <"$BOX_H/left-stubborn" 2>/dev/null)': $(tail -n 3 "$TMP/hz-finish.log") $(tail -n 3 "$TMP/hz-failrun.log") $(tail -n 2 "$TMP/hz-stubborn.log")"
fi
pkill -f "$BOX_H/kit/out/hz-" 2>/dev/null

# Catches: a failed read of server_flags_a or server_flags_b taken for "no
# extra flags", so the arm runs without the flags run.toml declared for it.
BOX_E="$TMP/box-flags-read"
lib_box "$BOX_E"
printf 'this is = = not toml\n' >"$TMP/broken-run.toml"
(
  # shellcheck source=/dev/null
  . "$BOX_E/lib-setup.sh"
  rm -f "$ARM_ENV"
  RUN_TOML="$TMP/broken-run.toml"
  resolve_arm_env && exit 1
  [ ! -e "$ARM_ENV" ] || exit 1
  exit 0
) >"$TMP/flags-read.log" 2>&1
code=$?
if [ "$code" = 0 ] && grep -q "cannot read server_flags_a from $TMP/broken-run.toml (kit.py get exit 2)" "$TMP/flags-read.log"; then
  pass "a run.toml that cannot be read fails resolve_arm_env instead of reading as no extra flags"
else
  fail "extra flags read" "exit $code: $(tail -n 3 "$TMP/flags-read.log")"
fi

# Catches: now_us failing where bash has no EPOCHREALTIME (macOS's bash 3.2),
# through GNU date or, where date has no %N, python3.
mkdir -p "$TMP/bsd-date"
printf '#!/bin/sh\necho 1792174430N\n' >"$TMP/bsd-date/date"
chmod +x "$TMP/bsd-date/date"
out="$(
  KIT_RUN_SH_LIB=1 KIT_ROOT="$TMP/no-box"
  # shellcheck source=/dev/null
  . "$KIT_SRC/box/run.sh"
  unset EPOCHREALTIME
  ref="$(python3 -c 'import time; print(time.time_ns() // 1000)')"
  a="$(now_us)" || echo "date path failed"
  b="$(PATH="$TMP/bsd-date:$PATH" now_us)" || echo "python path failed"
  for v in "$a" "$b"; do
    [[ "$v" =~ ^[0-9]{16}$ ]] && [ $((v - ref)) -lt 5000000 ] && [ $((ref - v)) -lt 5000000 ] \
      || echo "now_us gave '$v' against $ref"
  done
)"
# An array that can be empty must use the expansion bash 3.2 accepts under
# set -u, which expands to nothing.
unguarded="$(grep -nE '"\$\{(opts|extra)\[@\]\}"' "$KIT_SRC/box/run.sh" | grep -vE '\$\{(opts|extra)\[@\]\+')"
if [ -z "$out" ] && [ -z "$unguarded" ]; then
  pass "now_us works without EPOCHREALTIME and empty arrays expand guarded"
else
  fail "bash 3.2" "$out $unguarded"
fi

# Catches: any case or box run leaving a healthz sampler behind; every one
# started under this suite's TMP must be gone by now.
left="$(pgrep -f "healthz-sampler $TMP/" 2>/dev/null | tr '\n' ' ')"
if [ -z "$left" ]; then
  pass "the suite leaves no healthz sampler running"
else
  fail "leftover samplers" "pids $left"
fi

# --- syntax -----------------------------------------------------------------

# Catches: a kit script that no longer parses.
syntax=0
for f in launch.sh fetch.sh box/run.sh kit.test.sh; do
  bash -n "$KIT_SRC/$f" || syntax=1
done
python3 -c 'import ast, sys; ast.parse(open(sys.argv[1]).read())' "$KIT_SRC/lib/kit.py" || syntax=1
if [ "$syntax" = 0 ]; then
  pass "every kit script parses"
else
  fail "syntax" "see above"
fi

if [ "$fails" -ne 0 ]; then
  echo "$fails case(s) failed"
  exit 1
fi
echo "all kit cases passed"
