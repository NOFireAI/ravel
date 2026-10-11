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
# STUB_DENY_ACTION names an IAM action the simulated role lacks;
# STUB_FAIL_ARM names the arm whose run-instances fails.
echo "aws $*" >>"$STUB_LOG"
endpoint=""
pos=()
userdata=""
token=""
while [ $# -gt 0 ]; do
  case "$1" in
    --region|--query|--output|--instance-profile-name|--policy-source-arn|--name|--image-ids|--filters|--instance-ids)
      pos+=("$1=$2"); shift 2; continue ;;
    --endpoint-url) endpoint="$2"; shift 2; continue ;;
    --user-data) userdata="$2"; shift 2; continue ;;
    --client-token) token="$2"; shift 2; continue ;;
    --action-names) pos+=("action=$2"); shift 2; continue ;;
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
  "iam get-instance-profile") echo "arn:aws:iam::123456789012:role/kit-test-role" ;;
  "iam simulate-principal-policy")
    case " ${pos[*]} " in
      *" action=${STUB_DENY_ACTION:-none} "*) echo implicitDeny ;;
      *) echo allowed ;;
    esac
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
# IMDS and readiness answer at once. A SQL call logs its request body to
# sql-bodies, writes an answer to -o and prints "<status> <seconds>":
# STUB_SQL_TIME seconds (default 0.5), and a 422 resource_exhausted refusal
# for statement STUB_SQL_REFUSE_N. An answer holds the statement's expected
# row count from the index.tsv beside its body (3 rows where the corpus states
# none), or STUB_SQL_ROWS rows for statement STUB_SQL_ROWS_N.
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
  */readyz) echo ready ;;
  */api/v1/sql)
    echo "sql $(basename "$body")" >>"$STUB_DIR/curl-sql"
    n="$(basename "$body" .json)"
    { cat "$body"; echo; } >>"$STUB_DIR/sql-bodies"
    if [ "$n" = "${STUB_SQL_REFUSE_N:-none}" ]; then
      printf '{"status":"error","errorType":"resource_exhausted","error":"query pool exhausted"}' >"$out"
      printf '422 0.1'
    else
      rows="$(awk -F'\t' -v n="$n" '$1 == n { print $3 }' "$(dirname "$body")/index.tsv")"
      case "$rows" in ''|-) rows=3 ;; esac
      [ "$n" != "${STUB_SQL_ROWS_N:-none}" ] || rows="$STUB_SQL_ROWS"
      json=""
      for ((i = 0; i < rows; i++)); do json="${json:+$json,}[$i]"; done
      json="[$json]"
      printf '{"status":"success","data":{"columns":["c"],"rows":%s},"stats":{}}' "$json" >"$out"
      printf '200 %s' "${STUB_SQL_TIME:-0.5}"
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

# The box's binaries. The server logs its argv and the five settings stamps,
# then sleeps until stopped; the CLI logs its argv, and `load` adds 100
# objects to the store. STUB_FOLD=unsealed makes the fold seal nothing.
cat >"$BINSTUBS/ravel-server" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"$STUB_DIR/server-argv"
for kv in fetch_concurrency=32 store_get_concurrency=32 cache_max_bytes=7689077760 \
  catalog_cache_max_bytes=1537815552 sql_max_query_bytes=16451897344 sql_tenant_max_bytes=32903794688; do
  printf '2026-10-11T10:00:00.000000Z  INFO ravel_server::config: performance default resolved setting="%s" value=%s source="derived"\n' "${kv%%=*}" "${kv#*=}"
done
exec sleep 120
STUB
cat >"$BINSTUBS/ravel-cli" <<'STUB'
#!/usr/bin/env bash
echo "cli $*" >>"$STUB_DIR/cli-argv"
case " $* " in
  *" load "*) echo 110 >"$STUB_DIR/objects" ;;
  *" catalog fold "*)
    if [ "${STUB_FOLD:-}" = unsealed ]; then echo "seal_through_hour: None"; else echo "seal_through_hour: Some(491234)"; fi
    printf 'watermark_hour: Some(491235)\nentry_count: 100\nbuckets_folded: 5\npart_bytes: 12345\n' ;;
esac
exit 0
STUB
chmod +x "$STUBS"/* "$BINSTUBS"/*
export PATH="$STUBS:$PATH"

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

# Catches: dropping the SHA256SUMS check: a kit file changed inside a kit.tgz
# whose own digest was recomputed must still fail.
reset_log
mkdir -p "$TMP/retar"
tar -xzf "$IN_SAVED/kit.tgz" -C "$TMP/retar"
echo 'echo tampered' >>"$TMP/retar/kit/box/run.sh"
tar -czf "$RUN_DIR/in/kit.tgz" -C "$TMP/retar" kit
(cd "$RUN_DIR/in" && sha256sum kit.tgz run.toml >kit.sha256)
boot_box B "$BOX_T"
if grep -q 'FAILED step=user-data sha256 check of the kit files' "$RUN_DIR/out/B/FAILED" 2>/dev/null \
  && ! grep -q '^nohup' "$STUB_LOG"; then
  pass "a kit file changed under a recomputed kit.sha256 fails on SHA256SUMS"
else
  fail "SHA256SUMS check" "log: $(tr '\n' ';' <"$STUB_LOG" | head -c 400)"
fi
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
export KIT_READY_TRIES=5
# shellcheck source=/dev/null
. "$KIT_ROOT/kit/kit/box/run.sh"
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
bootstrap_rustfs() { make_secrets && echo 'rustfs 1.0.0-stub' >"$KIT/rustfs.version"; }
# GNU time is not on every CI image; the load and compact flags stay as run.sh has them.
do_load() { "$CLI_BIN" --store s3 --tenant-hash-unkeyed load --parquet "$DATASET" --tenant "$TENANT" \
  --mapping "$MAPPING" --shards "$SHARDS" --read-cursors 8 --batch-rows 65536; }
do_compact() { "$CLI_BIN" --store s3 --tenant-hash-unkeyed maintain compact-tenant \
  --tenant "$TENANT" --signal logs --shards "$SHARDS"; }
eval "real_run_query() $(declare -f run_query | tail -n +2)"
run_query() {
  echo "$*" >>"$STUB_DIR/run_query-calls"
  real_run_query "$@"
}
main
DRIVER
export BINSTUBS

# run_box ARM NAME [ENV=VALUE...]: boot a box for ARM and run main on it.
run_box() {
  local arm="$1" name="$2" box="$TMP/$2" code=0
  shift 2
  reset_log
  rm -f "$STUB_DIR/server-argv" "$STUB_DIR/cli-argv" "$STUB_DIR/curl-sql" "$STUB_DIR/run_query-calls" \
    "$STUB_DIR/objects" "$STUB_DIR/sql-bodies"
  boot_box "$arm" "$box" || return 1
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
          "phases", "bands"):
    want(k in r, "field " + k)
for k in ("git_sha", "binary", "arm", "tenant", "server", "server_logs", "server_settings",
          "band_sections", "kit_arm", "run_id", "lanes", "toolchain", "hardware", "backend",
          "corpus_digest", "server_flags", "audit_mode", "burst_balance", "fold", "object_counts",
          "query_window"):
    want(k in p, "provenance." + k)
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
want(p.get("hardware", {}).get("instance_type") == "c6a.4xlarge", "hardware")
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
# body sending a statement other than the corpus's.
window_ok="$(python3 - "$STUB_DIR/sql-bodies" "$OUT_A/results.json" "$REPO_ROOT/benchmarks/clickbench/hits.corpus.json" <<'PY'
import json, sys
bodies = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
w = json.load(open(sys.argv[2]))["provenance"]["query_window"]
sqls = [e["sql"] for e in json.load(open(sys.argv[3]))["entries"]]
ok = len(bodies) == 129
ok = ok and all(sorted(b) == ["end", "query", "start"] and b["start"] == w["start"]
                and b["end"] == w["end"] and b["end"] - b["start"] == 200000 * 3600 for b in bodies)
ok = ok and [b["query"] for b in bodies[::3]] == sqls
print("yes" if ok else "no")
PY
)"
if [ "$window_ok" = yes ]; then
  pass "every request body carries an explicit start and end 200000 hours apart"
else
  fail "query window" "$(head -c 300 "$STUB_DIR/sql-bodies")"
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

# Catches: a server start that does not take its flags from arm.env (all 43
# restarts must run the same argv), or cold tries without a cache drop.
if [ "$(wc -l <"$STUB_DIR/server-argv" | tr -d ' ')" = 43 ] && [ "$(sort -u "$STUB_DIR/server-argv" | wc -l | tr -d ' ')" = 1 ] \
  && grep -q -- '--memory-budget-bytes 25769803776 --audit-mode off' "$STUB_DIR/server-argv" \
  && [ "$(cat "$BOX_A/drop_caches")" = 3 ]; then
  pass "each of the 43 cold starts runs the same server flags after a cache drop"
else
  fail "server starts" "$(sort "$STUB_DIR/server-argv" | uniq -c | head -n 3)"
fi

# Catches: a step that runs without a UTC stamp, or the phases running before
# the fold precondition was checked. The uploaded copy ends at evidence: upload
# is stamped after it is sent.
order="$(sed -n 's/^[0-9]\{4\}-[0-9][0-9]-[0-9][0-9]T[0-9:]*Z step=\([a-z-]*\) state=done$/\1/p' "$OUT_A/progress.log" | tr '\n' ' ')"
unstamped="$(grep -vc '^[0-9]\{4\}-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z ' "$OUT_A/progress.log")"
case "$order" in
  *"objects-before-load load objects-after-load compact fold declare phase-benchmark burst-end results evidence "*)
    if [ "$unstamped" = 0 ]; then
      pass "progress.log stamps every step in UTC, fold before the phases"
    else
      fail "progress.log" "$unstamped unstamped lines"
    fi ;;
  *) fail "progress.log" "step order: $order" ;;
esac

# Catches: evidence.tgz missing the server log, the samplers or the object
# counts, or any box secret (RustFS keys, tenant token) leaving the box.
mkdir -p "$TMP/evidence-a"
tar -xzf "$OUT_A/evidence.tgz" -C "$TMP/evidence-a"
token="$(sed -n 's/^KIT_TENANT_TOKEN=//p' "$BOX_A/kit/secrets.env")"
secret="$(sed -n 's/^RUSTFS_SECRET_KEY=//p' "$BOX_A/kit/secrets.env")"
if [ -f "$TMP/evidence-a/server-1.log" ] && [ -f "$TMP/evidence-a/object-counts/before-load.txt" ] \
  && [ -f "$TMP/evidence-a/object-counts/after-load.txt" ] && [ -f "$TMP/evidence-a/out/benchmark/samplers/server-rss.log" ] \
  && [ -f "$TMP/evidence-a/arm.env" ] && [ -n "$token" ] && [ -n "$secret" ] \
  && ! grep -rqF -e "$token" -e "$secret" "$TMP/evidence-a" "$OUT_A" \
  && [ ! -e "$TMP/evidence-a/secrets.env" ] && [ ! -e "$TMP/evidence-a/auth.header" ]; then
  pass "evidence.tgz holds server logs, samplers and object counts, and no box secret is uploaded"
else
  fail "evidence" "$(tar -tzf "$OUT_A/evidence.tgz" | head -n 20 | tr '\n' ' ')"
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
rm -rf "$OUT_A"

# Catches: resolve_arm_env rewriting arm.env on a restart, or start_server
# reading live variables instead of arm.env.
BOX_R="$TMP/box-restart"
reset_log
rm -f "$STUB_DIR/server-argv"
boot_box A "$BOX_R"
(
  export KIT_ROOT="$BOX_R" KIT_RUN_SH_LIB=1 KIT_READY_TRIES=5
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
