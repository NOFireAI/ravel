#!/usr/bin/env bash
# Cases for the ClickBench kit: launch.sh, the user-data it writes, box/run.sh
# and fetch.sh. Nothing here reaches AWS or builds anything. Stubs first on
# PATH stand in for `aws` (S3 is a local directory, $FAKE_S3), `curl` (the
# instance metadata service and the SQL endpoint), `shutdown` and `nohup`; a
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
# IMDS answers at once. Readiness and SQL answer only while the stub server's
# ready file exists, as a server that is not listening refuses (curl exit 7);
# the stub server writes it only after logging its argv and settings, and
# removes it when stopped. A SQL call logs its request body to
# sql-bodies, writes an answer to -o and prints "<status> <seconds>":
# STUB_SQL_TIME seconds (default 0.5), or the try's field of STUB_SQL_TIMES
# ("cold hot hot"), and a 422 resource_exhausted refusal for statement
# STUB_SQL_REFUSE_N. An answer holds the statement's expected row count from
# the index.tsv beside its body (3 rows where the corpus states none), or
# STUB_SQL_ROWS rows for statement STUB_SQL_ROWS_N (only on try
# STUB_SQL_ROWS_TRY when that is set). The try is read from the -o name,
# <number>-<try>.json. A body under a verify/ directory is the box's row-count
# check: it is logged to verify-bodies and answered with one cell,
# STUB_COUNT_STAR (default the hits row count).
out=""
body=""
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2; continue ;;
    --data-binary) body="${2#@}"; shift 2; continue ;;
    -w|--max-time|-H|-X) shift 2; continue ;;
    http*) url="$1" ;;
  esac
  shift
done
case "$url" in
  */latest/api/token) echo stub-imds-token ;;
  */meta-data/instance-id) echo i-0b0x00000000000001 ;;
  */meta-data/instance-type) echo c6a.4xlarge ;;
  */readyz)
    [ -f "$STUB_DIR/server-ready" ] || { echo "curl: (7) Failed to connect" >&2; exit 7; }
    echo ready ;;
  */api/v1/sql)
    [ -f "$STUB_DIR/server-ready" ] || { echo "curl: (7) Failed to connect" >&2; exit 7; }
    case "$body" in
      */verify/*)
        { cat "$body"; echo; } >>"$STUB_DIR/verify-bodies"
        printf '{"status":"success","data":{"columns":["count(*)"],"rows":[[%s]]},"stats":{}}' \
          "${STUB_COUNT_STAR:-99997497}" >"$out"
        printf '200 0.1'
        exit 0 ;;
    esac
    echo "sql $(basename "$body")" >>"$STUB_DIR/curl-sql"
    n="$(basename "$body" .json)"
    try="${out##*-}"
    try="${try%.json}"
    { cat "$body"; echo; } >>"$STUB_DIR/sql-bodies"
    if [ "$n" = "${STUB_SQL_REFUSE_N:-none}" ]; then
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
      printf '200 %s' "$seconds"
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

# The box's binaries. The server logs its argv, the five settings stamps and
# the store keys and audit key it was given (the evidence must not carry
# them), then after STUB_READY_DELAY seconds (default 0.05) writes the ready
# file and sleeps until stopped; the CLI logs its argv, and `load` adds 100
# objects to the store and prints ravel-cli's load summary with
# STUB_LOAD_ROWS rows written (default the hits row count). STUB_FOLD=unsealed
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
sleep "${STUB_READY_DELAY:-0.05}"
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
    echo 110 >"$STUB_DIR/objects"
    printf 'bulk load complete\n  rows_skipped     : 0\n  rows_written     : %s\n  rows/sec         : 1000\n  objects written  : 100\n' \
      "${STUB_LOAD_ROWS:-99997497}" ;;
  *" catalog fold "*)
    if [ "${STUB_FOLD:-}" = unsealed ]; then echo "seal_through_hour: None"; else echo "seal_through_hour: Some(491234)"; fi
    printf 'previous_watermark_hour: Some(491200)\nwatermark_hour: Some(491235)\nentry_count: %s\nbuckets_folded: 5\npart_bytes: 12345\n' "${STUB_FOLD_ENTRIES:-100}"
    [ "${STUB_FOLD:-}" != twice ] || echo "entry_count: 100" ;;
esac
exit 0
STUB
chmod +x "$STUBS"/* "$BINSTUBS"/*
export PATH="$STUBS:$PATH"

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
instance_profile = "ravel-clickbench-box"
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
  for want in '--instance-initiated-shutdown-behavior terminate' '--iam-instance-profile Name=ravel-clickbench-box' \
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
  && grep -q 'placeholder value in absolute.hot_sum_s_max, absolute.cold_sum_s_max, absolute.error_pooled_max, absolute.hot_s_max.q01, ' "$TMP/ph.err" \
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

# Catches: launching (or uploading) when the instance role could not write
# results, or a refusal that does not name the role.
reset_log
code=0
STUB_DENY_ACTION=s3:PutObject bash "$KIT_SRC/launch.sh" "$RUN_TOML" >/dev/null 2>"$TMP/deny.err" || code=$?
if [ "$code" = 65 ] && grep -q 'role arn:aws:iam::123456789012:role/kit-test-role (instance profile ravel-clickbench-box) lacks s3:PutObject' "$TMP/deny.err" \
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
if [ "$code" = 65 ] && grep -q 'cannot read instance profile ravel-clickbench-box' "$TMP/noprofile.err" \
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
  && cmp -s "$BOX_UD/kit/run.toml" "$RUN_TOML" && [ -f "$BOX_UD/kit/kit/box/run.sh" ]; then
  pass "user-data arms shutdown -h +600, verifies the kit and starts run.sh under nohup"
else
  fail "user-data" "exit $code; log: $(tr '\n' ';' <"$STUB_LOG" | head -c 400)"
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
# which the evidence or FAILED may carry.
bootstrap_rustfs() {
  make_secrets && echo 'rustfs 1.0.0-stub' >"$KIT/rustfs.version" && . "$SECRETS_ENV" \
    && echo "rustfs: starting with access_key=$RUSTFS_ACCESS_KEY secret_key=$RUSTFS_SECRET_KEY" >"$KIT/rustfs.log" \
    && echo "rustfs: starting with access_key=$RUSTFS_ACCESS_KEY secret_key=$RUSTFS_SECRET_KEY" >&2
}
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
for k in ("provenance", "entries", "skipped", "failed", "totals", "error_pooled", "concurrency",
          "phases", "bands", "timing_definition"):
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
     and p["fold"].get("objects_written_by_load") == 100 and p["fold"].get("entry_count") == 100, "fold stamp")
want(p.get("object_counts") == {"before_load": 10, "after_load": 110}, "object_counts")
for side in ("start", "end"):
    b = (p.get("burst_balance") or {}).get(side) or {}
    want(b.get("volume_type") == "gp3" and b.get("value") is None and "gp3" in b.get("reason", ""),
         "burst_balance." + side)
want("--memory-budget-bytes 25769803776" in p.get("server_flags", {}).get("flags", ""), "server_flags")
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
want(r["error_pooled"] == {"calls": 129, "errors": 0, "ratio": 0.0}, "error_pooled")
want(r["phases"]["benchmark"]["calls"] == 129 and r["phases"]["benchmark"]["errors"] == 0
     and r["phases"]["benchmark"]["refusal_classes"] == {}, "phases block")
verdicts = {b["band"]: b["verdict"] for b in r["bands"]}
want(len(r["bands"]) == 46 and set(verdicts.values()) == {"pass"}, "46 passing absolute bands")
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
  *"objects-before-load load objects-after-load compact fold declare verify phase-benchmark burst-end results evidence "*)
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
  && grep -qF "$secret" "$BOX_A/kit/rustfs.log" && grep -qF "$secret" "$BOX_A/kit/run.log" \
  && grep -qF "secret_key=$secret" "$BOX_A/kit/server-1.log" \
  && grep -qF 'access_key=[redacted] secret_key=[redacted]' "$TMP/evidence-a/rustfs.log" \
  && grep -qF 'access_key=[redacted] secret_key=[redacted]' "$TMP/evidence-a/run.log" \
  && grep -qF 'access_key=[redacted] secret_key=[redacted]' "$TMP/evidence-a/server-1.log" \
  && ! grep -rqF -e "$token" -e "$secret" -e "$access" "$TMP/evidence-a" "$OUT_A" \
  && [ ! -e "$TMP/evidence-a/secrets.env" ] && [ ! -e "$TMP/evidence-a/auth.header" ]; then
  pass "evidence.tgz holds server logs, samplers and object counts, no box secret is uploaded, and an audit_mode off server gets no audit key"
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
want(r["error_pooled"] == {"calls": 129, "errors": 6, "ratio": 6 / 129}, "error_pooled")
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
  && grep -q '^INCONCLUSIVE  *B vs A  *qps >= 0.95 x A' "$TMP/fetch-pass.out" \
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

# Catches: a QPS band that stays inconclusive when both arms ran a concurrency
# phase, or compares the wrong way: B's 0.9 against 0.95 x A's 1.0 misses.
edit_arm "$TMP/fixture-pass" "$TMP/fixture-qps-a" 'r["concurrency"] = {"qps": 1.0}'
as_arm_b "$TMP/fixture-pass" "$TMP/fixture-qps-b" 'r["concurrency"] = {"qps": 0.9}'
fixture run-qps "$TMP/fixture-qps-a" "$TMP/fixture-qps-b"
fetch qps run-qps --timeout 30
if [ "$code" = 1 ] && grep -q '^MISS  *B vs A  *qps >= 0.95 x A  *0.900  *0.950$' "$TMP/fetch-qps.out" \
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
  'server_settings|p["server_settings"]["fetch_concurrency"] += 1'; do
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
