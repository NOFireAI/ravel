#!/usr/bin/env bash
# Cases for scripts/bench/fresh-box.sh. Every case runs with stub `aws`, `ssh`
# and `scp` first on PATH that log their arguments and never reach AWS; the
# real binaries are never invoked.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# FRESH_BOX_SCRIPT points the cases at another copy of the script, to show a
# case failing against a broken one; FRESH_BOX_TEST_BASH names the bash every
# case runs it under (e.g. /bin/bash 3.2 on macOS).
SCRIPT="${FRESH_BOX_SCRIPT:-$HERE/fresh-box.sh}"
TEST_BASH="${FRESH_BOX_TEST_BASH:-bash}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; fails=$((fails + 1)); }

STUBS="$TMP/stubs"
mkdir -p "$STUBS"
cat >"$STUBS/aws" <<'STUB'
#!/usr/bin/env bash
# Instances "launched" under the run tag are kept one per line in
# $STUB_LOG.instances; STUB_LAUNCH picks how run-instances behaves:
#   ""       launch i-0123456789abcdef0 and print its id
#   fail     launch it, then exit non-zero with no output
#   garbage  launch it, then print no parseable id
#   twice    launch i-0fedcba9876543210 and i-0123456789abcdef0 and print
#            only the second, as a retried launch without a token would
#   none     launch nothing and exit non-zero
echo "aws $*" >>"$STUB_LOG"
INSTANCES="$STUB_LOG.instances"
case "$*" in
  *"ec2 describe-images"*) echo /dev/sda1 ;;
  *"ec2 run-instances"*)
    case "${STUB_LAUNCH:-}" in
      none) echo "An error occurred (InvalidAMIID.Malformed)" >&2; exit 254 ;;
      twice) printf 'i-0fedcba9876543210\ni-0123456789abcdef0\n' >>"$INSTANCES" ;;
      *) echo i-0123456789abcdef0 >>"$INSTANCES" ;;
    esac
    case "${STUB_LAUNCH:-}" in
      fail) echo "Connection was closed before we received a valid response" >&2; exit 255 ;;
      garbage) echo "None" ;;
      *) echo i-0123456789abcdef0 ;;
    esac
    ;;
  *"ec2 terminate-instances"*) echo terminating ;;
  *"--filters"*)
    [ -n "${STUB_LOOKUP_FAIL:-}" ] && { echo "lookup failed" >&2; exit 255; }
    [ -f "$INSTANCES" ] && tr '\n' '\t' <"$INSTANCES"
    echo
    ;;
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
  --identity-file "$TMP/key.pem" --ssh-user ubuntu --volume-gb 120 --max-minutes 90
  --repo-url https://example.invalid/ravel.git --commit "$COMMIT" --out "$TMP/out.json"
  --sample-size 10 --warmup 1 --measure 3 --max-series 2000
)
FLAGS=(--instance-type --ami --subnet --security-group --region --access --key-name
  --identity-file --ssh-user --volume-gb --max-minutes --repo-url --commit --out --sample-size
  --warmup --measure --max-series)

# fresh_box <case> [args...]: runs the script with stubs, fast retry knobs and
# no FRESH_BOX_* input variables inherited; sets out/code/log.
fresh_box() {
  local name="$1"
  shift
  log="$TMP/$name.log"
  : >"$log"
  rm -f "$log.instances"
  out="$(env -i PATH="$STUBS:/usr/bin:/bin" HOME="$TMP" STUB_LOG="$log" \
    STUB_STATE="${STUB_STATE:-}" STUB_FAIL_BENCH="${STUB_FAIL_BENCH:-}" \
    STUB_LAUNCH="${STUB_LAUNCH:-}" STUB_LOOKUP_FAIL="${STUB_LOOKUP_FAIL:-}" \
    FRESH_BOX_CONFIRM_ATTEMPTS=2 FRESH_BOX_CONFIRM_SLEEP=0 FRESH_BOX_LOOKUP_ATTEMPTS=2 \
    FRESH_BOX_REACH_ATTEMPTS=2 FRESH_BOX_REACH_SLEEP=0 \
    "$TEST_BASH" "$SCRIPT" "$@" 2>&1)"
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
elif ! run_id="$(printf '%s\n' "$out" | sed -n 's/^fresh-box: run id \([^ ]*\) .*/\1/p')" \
  || [ -z "$run_id" ]; then
  fail dry-run "no run id printed"
elif ! plain="$(printf '%s\n' "$out" | tr -d '\\')" \
  || ! printf '%s\n' "$plain" | grep -qF -- "--client-token $run_id "; then
  fail dry-run "run-instances carries no --client-token $run_id"
elif ! printf '%s\n' "$plain" | grep -qF -- "{Key=ravel-fresh-box-run,Value=$run_id}"; then
  fail dry-run "run-instances carries no ravel-fresh-box-run=$run_id tag"
elif ! printf '%s\n' "$plain" | grep -q -- "--user-data .*#!/bin/sh.*shutdown -h +90"; then
  fail dry-run "run-instances carries no shutdown -h +90 user-data"; printf '%s\n' "$out" | sed 's/^/    /'
elif ! printf '%s\n' "$plain" | grep -qF -- "--filters Name=tag:ravel-fresh-box-run,Values=$run_id"; then
  fail dry-run "the trap's lookup by tag is not printed"; printf '%s\n' "$out" | sed 's/^/    /'
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

# --- the trap finds launched instances by the run tag when it has no id ---
# run_id_of: the run id the script printed for the last case.
run_id_of() { printf '%s\n' "$out" | sed -n 's/^fresh-box: run id \([^ ]*\) .*/\1/p'; }
# tag_case NAME WANT_IDS: the lookup ran with this run's tag, one terminate call
# named exactly WANT_IDS, each was confirmed, and the exit was 1.
tag_case() {
  local name="$1" want="$2" rid term id
  rid="$(run_id_of)"
  term="$(grep '^aws ec2 terminate-instances' "$log")"
  if [ "$code" -ne 1 ]; then
    fail "$name" "exit $code, want 1"; printf '%s\n' "$out" | sed 's/^/    /'
  elif [ -z "$rid" ] || ! grep -qF -- "--filters Name=tag:ravel-fresh-box-run,Values=$rid Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped" "$log"; then
    fail "$name" "no describe-instances lookup by this run's tag"; sed 's/^/    /' "$log"
  elif [ "$term" != "aws ec2 terminate-instances --region eu-central-1 --instance-ids $want --output text" ]; then
    fail "$name" "terminate call was '$term', want ids '$want'"
  else
    for id in $want; do
      if ! printf '%s\n' "$out" | grep -qF "fresh-box: $id is terminated"; then
        fail "$name" "$id not confirmed: $out"; return
      fi
    done
    pass "$name"
  fi
}
STUB_LAUNCH=fail fresh_box launch-cli-failed "${ARGS[@]}"
tag_case launch-cli-failed "i-0123456789abcdef0"
STUB_LAUNCH=garbage fresh_box launch-no-id "${ARGS[@]}"
tag_case launch-no-id "i-0123456789abcdef0"
STUB_LAUNCH=twice fresh_box launch-twice "${ARGS[@]}"
if grep -q '^ssh ' "$log"; then
  # The run went ahead on the returned id: the trap still ends both.
  [ "$code" -eq 0 ] || fail launch-twice "exit $code"
  code=1
fi
tag_case launch-twice "i-0123456789abcdef0 i-0fedcba9876543210"

# --- a launch AWS refused leaves nothing to terminate and exits 1 ---
STUB_LAUNCH=none fresh_box launch-refused "${ARGS[@]}"
if [ "$code" -ne 1 ]; then
  fail launch-refused "exit $code, want 1"
elif grep -q '^aws ec2 terminate-instances' "$log"; then
  fail launch-refused "terminate called with nothing launched"
elif ! printf '%s\n' "$out" | grep -qF "no instance found under tag ravel-fresh-box-run=$(run_id_of)"; then
  fail launch-refused "the empty lookup is not reported: $out"
else
  pass launch-refused
fi

# --- no id and a failed lookup cannot be confirmed: exit 70 naming the tag ---
STUB_LAUNCH=fail STUB_LOOKUP_FAIL=1 fresh_box lookup-failed "${ARGS[@]}"
if [ "$code" -ne 70 ]; then
  fail lookup-failed "exit $code, want 70"
elif ! printf '%s\n' "$out" | grep -qF "COULD NOT CONFIRM TERMINATION: no instance id and the lookup by tag ravel-fresh-box-run=$(run_id_of) failed"; then
  fail lookup-failed "the run tag is not printed: $out"
else
  pass lookup-failed
fi

# --- a failing step's own exit code is reported as 1 ---
STUB_FAIL_BENCH=1 fresh_box midfail-code "${ARGS[@]}"
if [ "$code" -ne 1 ]; then fail failure-code "exit $code, want 1"; else pass failure-code; fi

# --- each run gets a fresh run id ---
fresh_box dry-a --dry-run "${ARGS[@]}"
first="$(run_id_of)"
fresh_box dry-b --dry-run "${ARGS[@]}"
if [ -z "$first" ] || [ "$first" = "$(run_id_of)" ]; then
  fail run-id-unique "two runs printed run id '$first'"
else
  pass run-id-unique
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
