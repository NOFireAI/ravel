#!/usr/bin/env bash
# Cases for scripts/bench/fresh-box.sh. Every case runs with stub `aws`, `ssh`
# and `scp` first on PATH that log their arguments and never reach AWS; the
# real binaries are never invoked.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/fresh-box.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; fails=$((fails + 1)); }

STUBS="$TMP/stubs"
mkdir -p "$STUBS"
cat >"$STUBS/aws" <<'STUB'
#!/usr/bin/env bash
echo "aws $*" >>"$STUB_LOG"
case "$*" in
  *"ec2 describe-images"*) echo /dev/sda1 ;;
  *"ec2 run-instances"*) echo i-0123456789abcdef0 ;;
  *"ec2 terminate-instances"*) echo terminating ;;
  *"State.Name"*) echo "${STUB_STATE:-terminated}" ;;
  *"InstanceType"*) echo c7i.2xlarge ;;
  *"PublicIpAddress"*) printf '203.0.113.5\t10.0.0.5\n' ;;
  *"ec2 wait"*) ;;
  *) echo "unexpected aws call: $*" >&2; exit 99 ;;
esac
STUB
cat >"$STUBS/ssh" <<'STUB'
#!/usr/bin/env bash
echo "ssh $*" >>"$STUB_LOG"
case "$*" in
  *bench-tier-b.sh*)
    # Undo the two levels of quoting the launcher applied and log the label
    # the remote record call would receive.
    eval "words=(${*: -1})"
    eval "label=(${words[2]##*record tier-b-baseline.json })"
    echo "LABEL: ${label[0]}" >>"$STUB_LOG"
    [ -n "${STUB_FAIL_BENCH:-}" ] && exit 1
    ;;
  *" nproc") echo 8 ;;
  *" uname -m") echo x86_64 ;;
esac
exit 0
STUB
cat >"$STUBS/scp" <<'STUB'
#!/usr/bin/env bash
echo "scp $*" >>"$STUB_LOG"
dest="${*: -1}"
echo '{"_meta": {"label": "stub"}, "benchmarks": {}}' >"$dest"
STUB
chmod +x "$STUBS/aws" "$STUBS/ssh" "$STUBS/scp"

COMMIT=0123456789abcdef0123456789abcdef01234567
ARGS=(
  --instance-type c7i.2xlarge --ami ami-0aaaabbbbccccdddd --subnet subnet-0feedface
  --security-group sg-0c0ffee --region eu-central-1 --access ssh --key-name bench-key
  --identity-file "$TMP/key.pem" --ssh-user ubuntu --volume-gb 120
  --repo-url https://example.invalid/ravel.git --commit "$COMMIT" --out "$TMP/out.json"
  --sample-size 10 --warmup 1 --measure 3 --max-series 2000
)
FLAGS=(--instance-type --ami --subnet --security-group --region --access --key-name
  --identity-file --ssh-user --volume-gb --repo-url --commit --out --sample-size
  --warmup --measure --max-series)

# fresh_box <case> [args...]: runs the script with stubs, fast retry knobs and
# no FRESH_BOX_* input variables inherited; sets out/code/log.
fresh_box() {
  local name="$1"
  shift
  log="$TMP/$name.log"
  : >"$log"
  out="$(env -i PATH="$STUBS:/usr/bin:/bin" HOME="$TMP" STUB_LOG="$log" \
    STUB_STATE="${STUB_STATE:-}" STUB_FAIL_BENCH="${STUB_FAIL_BENCH:-}" \
    FRESH_BOX_CONFIRM_ATTEMPTS=2 FRESH_BOX_CONFIRM_SLEEP=0 \
    FRESH_BOX_REACH_ATTEMPTS=2 FRESH_BOX_REACH_SLEEP=0 \
    bash "$SCRIPT" "$@" 2>&1)"
  code=$?
}

# --- dry run prints the launch and terminate calls and runs no stub ---
fresh_box dry --dry-run "${ARGS[@]}"
if [ "$code" -ne 0 ]; then
  fail dry-run "exit $code"; printf '%s\n' "$out" | sed 's/^/    /'
elif [ -s "$log" ]; then
  fail dry-run "a stub ran: $(cat "$log")"
elif ! printf '%s\n' "$out" | grep -q 'DRY-RUN: aws ec2 run-instances .*--image-id ami-0aaaabbbbccccdddd .*--subnet-id subnet-0feedface --security-group-ids sg-0c0ffee'; then
  fail dry-run "no run-instances line with the given ids"; printf '%s\n' "$out" | sed 's/^/    /'
elif ! printf '%s\n' "$out" | grep -q 'DRY-RUN: aws ec2 terminate-instances --region eu-central-1 --instance-ids i-dryrun'; then
  fail dry-run "no terminate-instances line"
elif ! printf '%s\n' "$out" | grep -q 'DRY-RUN: ssh .*bench-tier-b.sh'; then
  fail dry-run "no ssh line running bench-tier-b.sh record"
elif ! printf '%s\n' "$out" | grep -q 'DRY-RUN: scp '; then
  fail dry-run "no scp line"
else
  pass dry-run
fi

# --- each missing input exits 64 naming it, before any stub runs ---
for flag in "${FLAGS[@]}"; do
  pruned=()
  skip=""
  for a in "${ARGS[@]}"; do
    if [ -n "$skip" ]; then skip=""; continue; fi
    if [ "$a" = "$flag" ]; then skip=1; continue; fi
    pruned+=("$a")
  done
  fresh_box "missing$flag" "${pruned[@]}"
  if [ "$code" -ne 64 ]; then
    fail "missing $flag" "exit $code, want 64"
  elif ! printf '%s\n' "$out" | grep -q -- "missing required input $flag "; then
    fail "missing $flag" "message does not name it: $out"
  elif [ -s "$log" ]; then
    fail "missing $flag" "a stub ran"
  else
    pass "missing $flag"
  fi
done

# --- --access ssm needs an instance profile and tunnels ssh through SSM ---
SSM_ARGS=("${ARGS[@]/#ssh/ssm}")
fresh_box ssm-missing-profile "${SSM_ARGS[@]}"
if [ "$code" -ne 64 ] || ! printf '%s\n' "$out" | grep -q -- "missing required input --instance-profile"; then
  fail ssm-missing-profile "exit $code: $out"
else
  pass ssm-missing-profile
fi
fresh_box ssm-dry --dry-run "${SSM_ARGS[@]}" --instance-profile bench-ssm
if [ "$code" -ne 0 ] || [ -s "$log" ] \
  || ! printf '%s\n' "$out" | grep -q 'DRY-RUN: aws ec2 run-instances .*--iam-instance-profile Name=bench-ssm' \
  || ! printf '%s\n' "$out" | grep -q 'DRY-RUN: ssh .*ProxyCommand=aws\\ ssm\\ start-session.* ubuntu@i-dryrun'; then
  fail ssm-dry-run "exit $code"; printf '%s\n' "$out" | sed 's/^/    /'
else
  pass ssm-dry-run
fi
fresh_box ssh-with-profile "${ARGS[@]}" --instance-profile bench-ssm
if [ "$code" -ne 64 ]; then fail ssh-with-profile "exit $code, want 64"; else pass ssh-with-profile; fi

# --- a full stubbed run records, copies back and terminates ---
fresh_box happy "${ARGS[@]}"
if [ "$code" -ne 0 ]; then
  fail happy "exit $code"; printf '%s\n' "$out" | sed 's/^/    /'
elif ! grep -q 'aws ec2 terminate-instances --region eu-central-1 --instance-ids i-0123456789abcdef0' "$log"; then
  fail happy "terminate not called"
elif ! label="$(grep '^LABEL: ' "$log")" \
  || ! printf '%s' "$label" | grep -qF 'FRESH EC2 INSTANCE c7i.2xlarge (8 cores, x86_64)' \
  || ! printf '%s' "$label" | grep -qF "Binary commit $COMMIT" \
  || ! printf '%s' "$label" | grep -qF 'Knobs: BENCH_SAMPLE_SIZE=10 BENCH_WARMUP=1 BENCH_MEASURE=3 RAVEL_BENCH_MAX_SERIES=2000' \
  || ! printf '%s' "$label" | grep -qF 'Corpus: none' \
  || ! printf '%s' "$label" | grep -qF 'No flush cadence: store-independent'; then
  fail happy "the record label lacks a provenance stamp"; sed 's/^/    /' "$log"
elif [ ! -s "$TMP/out.json" ]; then
  fail happy "baseline not copied back"
else
  pass happy
fi

# --- a mid-run failure still reaches the terminate call ---
STUB_FAIL_BENCH=1 fresh_box midfail "${ARGS[@]}"
if [ "$code" -eq 0 ]; then
  fail mid-run-failure "exit 0 after the bench failed"
elif ! grep -q 'aws ec2 terminate-instances --region eu-central-1 --instance-ids i-0123456789abcdef0' "$log"; then
  fail mid-run-failure "terminate not called"; sed 's/^/    /' "$log"
elif grep -q '^scp ' "$log"; then
  fail mid-run-failure "copied a baseline back after the bench failed"
else
  pass mid-run-failure
fi

# --- a terminate that cannot be confirmed exits non-zero with the id ---
STUB_STATE=running fresh_box unconfirmed "${ARGS[@]}"
if [ "$code" -ne 70 ]; then
  fail unconfirmed "exit $code, want 70"
elif ! grep -q 'aws ec2 terminate-instances --region eu-central-1 --instance-ids i-0123456789abcdef0' "$log"; then
  fail unconfirmed "terminate not called"
elif ! printf '%s\n' "$out" | grep -q 'COULD NOT CONFIRM TERMINATION of instance i-0123456789abcdef0'; then
  fail unconfirmed "instance id not printed: $out"
else
  pass unconfirmed
fi

# --- --help names every input ---
fresh_box help --help
missing_help=""
for flag in "${FLAGS[@]}" --instance-profile --dry-run; do
  printf '%s\n' "$out" | grep -q -- "$flag" || missing_help="$missing_help $flag"
done
if [ "$code" -ne 0 ] || [ -n "$missing_help" ]; then
  fail help "exit $code, missing:$missing_help"
else
  pass help
fi

if [ "$fails" -ne 0 ]; then
  echo "$fails case(s) failed"
  exit 1
fi
echo "all fresh-box cases passed"
