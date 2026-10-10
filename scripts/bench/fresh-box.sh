#!/usr/bin/env bash
# Record an ADR-0070 tier B timing baseline on a fresh EC2 instance launched
# for the run and terminated after it.
#
# A tier B number means something only against a baseline taken on the same
# hardware under the same load, so a tier B baseline is recorded on a fresh
# instance rather than a shared or long-lived box. This launches one instance,
# builds the given commit there, runs `scripts/bench-tier-b.sh record` with the
# five provenance stamps (bench/baselines/README.md), copies the baseline back,
# and terminates the instance.
#
# Termination has two layers. Each run generates one run id, prints it before
# launching, and uses it both as the run-instances --client-token and as the
# value of the instance tag ravel-fresh-box-run. The EXIT trap terminates the
# instance id run-instances returned together with every instance found under
# that tag in any state but terminated, so an instance is still found when the
# CLI failed after AWS accepted the launch or printed no parseable id. The trap
# does not run if the launcher is killed with SIGKILL or its machine is lost;
# for that the instance's user-data schedules `shutdown -h +MAX_MINUTES` at
# boot, and the launch sets --instance-initiated-shutdown-behavior terminate,
# so the instance terminates itself (on an AMI whose cloud-init runs user-data
# scripts).
#
# Written for bash 3.2 (macOS /bin/bash): no associative arrays, no mapfile,
# no ${var,,}, no date -d. FRESH_BOX_TEST_BASH runs the cases under it.
#
# Every input is required and has no default; run with --help for the list.
#
# Exit codes:
#   0   baseline copied back and every instance confirmed shutting-down or
#       terminated
#   1   a step failed or the run was interrupted; every instance found was
#       still terminated and confirmed. The EXIT trap reports any non-zero
#       code as 1; the failing step's own message is printed above it
#   64  a required input is missing or malformed; nothing was launched
#   70  an instance could not be confirmed terminated, or the tag lookup
#       failed with no instance id to fall back on; the ids or the run tag are
#       printed on stderr and a human must terminate the instance
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/bench/fresh-box.sh [--dry-run] --instance-type T --ami AMI
         --subnet SUBNET --security-group SG --region REGION
         --access ssh|ssm [--instance-profile NAME] --key-name NAME
         --identity-file PATH --ssh-user USER
         --volume-gb N --max-minutes N --repo-url URL --commit SHA --out PATH
         --sample-size N --warmup S --measure S --max-series N

Launches one EC2 instance, records a tier B baseline on it with
scripts/bench-tier-b.sh record, copies it to --out, and terminates the
instance from an EXIT trap. Every input is required; none has a default.
Each flag can also be given as the environment variable in brackets.

  --instance-type   EC2 instance type                    [FRESH_BOX_INSTANCE_TYPE]
  --ami             AMI id (Linux with apt-get or dnf, bash as the
                    --ssh-user login shell, and cloud-init
                    running user-data scripts)            [FRESH_BOX_AMI]
  --subnet          subnet id                             [FRESH_BOX_SUBNET]
  --security-group  security group id                     [FRESH_BOX_SECURITY_GROUP]
  --region          AWS region                            [FRESH_BOX_REGION]
  --access          ssh: connect to the instance's public or private IP;
                    ssm: tunnel ssh through SSM Session Manager to the
                    instance id (needs the session-manager-plugin locally)
                                                          [FRESH_BOX_ACCESS]
  --instance-profile  IAM instance profile allowing SSM; required with
                    --access ssm, refused with --access ssh
                                                          [FRESH_BOX_INSTANCE_PROFILE]
  --key-name        EC2 key pair name, used by both access modes
                                                          [FRESH_BOX_KEY_NAME]
  --identity-file   local private key for that key pair   [FRESH_BOX_IDENTITY_FILE]
  --ssh-user        login user the AMI provides           [FRESH_BOX_SSH_USER]
  --volume-gb       root volume size in GB (a cold build of the bench set
                    needs tens of GB)                     [FRESH_BOX_VOLUME_GB]
  --max-minutes     minutes after boot at which the instance shuts itself
                    down and terminates, whatever the launcher is doing;
                    must cover the build and the bench run [FRESH_BOX_MAX_MINUTES]
  --repo-url        git URL to clone                      [FRESH_BOX_REPO_URL]
  --commit          full 40-character commit to build     [FRESH_BOX_COMMIT]
  --out             local path the baseline is copied to  [FRESH_BOX_OUT]
  --sample-size     BENCH_SAMPLE_SIZE                     [FRESH_BOX_SAMPLE_SIZE]
  --warmup          BENCH_WARMUP seconds                  [FRESH_BOX_WARMUP]
  --measure         BENCH_MEASURE seconds                 [FRESH_BOX_MEASURE]
  --max-series      RAVEL_BENCH_MAX_SERIES                [FRESH_BOX_MAX_SERIES]
  --dry-run         print every aws, ssh and scp command instead of running it
  --help            this text

The four knobs must match the ones the baseline will be compared at (the
bench-compare workflow pins them): bench-compare.py reports a knob mismatch,
and an enforcing compare refuses the pair.
EOF
}

# name|flag|variable, in the order a missing one is reported. Each value is
# held in the shell variable V_<name>.
INPUTS=(
  "instance_type|--instance-type|FRESH_BOX_INSTANCE_TYPE"
  "ami|--ami|FRESH_BOX_AMI"
  "subnet|--subnet|FRESH_BOX_SUBNET"
  "security_group|--security-group|FRESH_BOX_SECURITY_GROUP"
  "region|--region|FRESH_BOX_REGION"
  "access|--access|FRESH_BOX_ACCESS"
  "key_name|--key-name|FRESH_BOX_KEY_NAME"
  "identity_file|--identity-file|FRESH_BOX_IDENTITY_FILE"
  "ssh_user|--ssh-user|FRESH_BOX_SSH_USER"
  "volume_gb|--volume-gb|FRESH_BOX_VOLUME_GB"
  "max_minutes|--max-minutes|FRESH_BOX_MAX_MINUTES"
  "repo_url|--repo-url|FRESH_BOX_REPO_URL"
  "commit|--commit|FRESH_BOX_COMMIT"
  "out|--out|FRESH_BOX_OUT"
  "sample_size|--sample-size|FRESH_BOX_SAMPLE_SIZE"
  "warmup|--warmup|FRESH_BOX_WARMUP"
  "measure|--measure|FRESH_BOX_MEASURE"
  "max_series|--max-series|FRESH_BOX_MAX_SERIES"
)

die64() {
  echo "fresh-box: $*" >&2
  exit 64
}

# value NAME: print V_NAME.
value() {
  local v="V_$1"
  printf '%s' "${!v}"
}

for spec in "${INPUTS[@]}"; do
  IFS='|' read -r name _flag var <<<"$spec"
  printf -v "V_$name" '%s' "${!var:-}"
done

INSTANCE_PROFILE="${FRESH_BOX_INSTANCE_PROFILE:-}"
DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift; continue ;;
    --instance-profile)
      [ $# -ge 2 ] || die64 "--instance-profile needs a value"
      INSTANCE_PROFILE="$2"; shift 2; continue ;;
  esac
  matched=""
  for spec in "${INPUTS[@]}"; do
    IFS='|' read -r name flag _var <<<"$spec"
    if [ "$1" = "$flag" ]; then
      [ $# -ge 2 ] || die64 "$flag needs a value"
      printf -v "V_$name" '%s' "$2"
      matched=1
      break
    fi
  done
  [ -n "$matched" ] || { usage >&2; die64 "unknown argument: $1"; }
  shift 2
done

for spec in "${INPUTS[@]}"; do
  IFS='|' read -r name flag var <<<"$spec"
  [ -n "$(value "$name")" ] || die64 "missing required input $flag ($var)"
done

case "$V_access" in
  ssh) [ -z "$INSTANCE_PROFILE" ] || die64 "--instance-profile applies only to --access ssm" ;;
  ssm) [ -n "$INSTANCE_PROFILE" ] || die64 "missing required input --instance-profile (FRESH_BOX_INSTANCE_PROFILE) for --access ssm" ;;
  *) die64 "--access must be ssh or ssm, got $V_access" ;;
esac
PROFILE_ARGS=()
if [ -n "$INSTANCE_PROFILE" ]; then
  PROFILE_ARGS=(--iam-instance-profile "Name=$INSTANCE_PROFILE")
fi
[[ "$V_commit" =~ ^[0-9a-f]{40}$ ]] || die64 "--commit must be a full 40-character commit, got $V_commit"
for name in volume_gb max_minutes sample_size warmup measure max_series; do
  [[ "$(value "$name")" =~ ^[0-9]+$ ]] || die64 "--${name//_/-} must be a whole number, got $(value "$name")"
done
[ "$V_max_minutes" -gt 0 ] || die64 "--max-minutes must be at least 1, got $V_max_minutes"
# Both paths are first used after the launch, so a bad one would cost a paid run.
{ [ -f "$V_identity_file" ] && [ -r "$V_identity_file" ]; } \
  || die64 "--identity-file is not a readable file: $V_identity_file"
out_dir="$(dirname -- "$V_out")"
{ [ -d "$out_dir" ] && [ -w "$out_dir" ]; } \
  || die64 "--out directory does not exist or is not writable: $out_dir"

REGION="$V_region"
CONFIRM_ATTEMPTS="${FRESH_BOX_CONFIRM_ATTEMPTS:-20}"
CONFIRM_SLEEP="${FRESH_BOX_CONFIRM_SLEEP:-15}"
LOOKUP_ATTEMPTS="${FRESH_BOX_LOOKUP_ATTEMPTS:-8}"
REACH_ATTEMPTS="${FRESH_BOX_REACH_ATTEMPTS:-40}"
REACH_SLEEP="${FRESH_BOX_REACH_SLEEP:-15}"
REMOTE_DIR="ravel"
REMOTE_OUT="tier-b-baseline.json"

# One id per run: the client token makes a retried run-instances return the
# same instance instead of launching a second one, and the tag finds whatever
# was launched when no id came back. A client token is at most 64 characters.
RUN_ID="fresh-box-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
RUN_TAG_KEY="ravel-fresh-box-run"
USER_DATA="#!/bin/sh
shutdown -h +$V_max_minutes
"
echo "fresh-box: run id $RUN_ID (client token $RUN_ID, tag $RUN_TAG_KEY=$RUN_ID, self-terminates $V_max_minutes minutes after boot)" >&2

# run CMD...: run it, or under --dry-run print it on stderr and succeed.
run() {
  if [ "$DRY_RUN" = 1 ]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; } >&2
    return 0
  fi
  "$@"
}

# capture PLACEHOLDER CMD...: print CMD's stdout, or under --dry-run print the
# command on stderr and PLACEHOLDER on stdout.
capture() {
  local placeholder="$1"
  shift
  if [ "$DRY_RUN" = 1 ]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; } >&2
    printf '%s\n' "$placeholder"
    return 0
  fi
  "$@"
}

INSTANCE_ID=""
LAUNCH_ATTEMPTED=0

# tagged_instances: print the ids of every instance under this run's tag that
# is not terminated, space-separated; fails when the lookup itself fails.
tagged_instances() {
  local found
  found="$(capture "${INSTANCE_ID:-}" aws ec2 describe-instances --region "$REGION" \
    --filters "Name=tag:$RUN_TAG_KEY,Values=$RUN_ID" \
    "Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text)" || return 1
  local id out=""
  for id in $found; do
    [[ "$id" =~ ^i-[0-9a-z]+$ ]] && out="$out $id"
  done
  printf '%s' "${out# }"
}

terminate() {
  local code=$?
  trap - EXIT
  [ "$code" -eq 0 ] || code=1
  [ "$LAUNCH_ATTEMPTED" = 1 ] || exit "$code"

  local ids="" found="" lookup_ok=1 attempt id
  for ((attempt = 1; attempt <= LOOKUP_ATTEMPTS; attempt++)); do
    lookup_ok=1
    found="$(tagged_instances)" || lookup_ok=0
    # A just-launched instance can take a moment to show up in a filtered
    # describe; only keep asking when there is no id to fall back on.
    if [ -n "$found" ] || [ -n "$INSTANCE_ID" ]; then
      break
    fi
    [ "$attempt" -lt "$LOOKUP_ATTEMPTS" ] && sleep "$CONFIRM_SLEEP"
  done
  for id in $INSTANCE_ID $found; do
    case " $ids " in
      *" $id "*) ;;
      *) ids="${ids:+$ids }$id" ;;
    esac
  done
  if [ -z "$ids" ]; then
    if [ "$lookup_ok" = 0 ]; then
      echo "fresh-box: COULD NOT CONFIRM TERMINATION: no instance id and the lookup by tag $RUN_TAG_KEY=$RUN_ID failed in $REGION; find and terminate it by hand" >&2
      exit 70
    fi
    echo "fresh-box: no instance found under tag $RUN_TAG_KEY=$RUN_ID in $REGION; nothing to terminate" >&2
    exit "$code"
  fi
  [ "$lookup_ok" = 1 ] || echo "fresh-box: the lookup by tag $RUN_TAG_KEY=$RUN_ID failed; terminating $ids only" >&2

  echo "fresh-box: terminating $ids" >&2
  # shellcheck disable=SC2086 # ids is a space-separated list of instance ids
  run aws ec2 terminate-instances --region "$REGION" --instance-ids $ids \
    --output text >/dev/null || true
  local pending="$ids" state="" i still
  for ((i = 1; i <= CONFIRM_ATTEMPTS; i++)); do
    still=""
    for id in $pending; do
      state="$(capture terminated aws ec2 describe-instances --region "$REGION" \
        --instance-ids "$id" \
        --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)"
      case "$state" in
        shutting-down|terminated) echo "fresh-box: $id is $state" >&2 ;;
        *) still="${still:+$still }$id" ;;
      esac
    done
    pending="$still"
    [ -n "$pending" ] || exit "$code"
    sleep "$CONFIRM_SLEEP"
  done
  echo "fresh-box: COULD NOT CONFIRM TERMINATION of instance $pending in $REGION (last state: ${state:-unknown}); terminate it by hand" >&2
  exit 70
}
trap terminate EXIT
trap 'exit 1' HUP INT TERM

root_device="$(capture /dev/sda1 aws ec2 describe-images --region "$REGION" \
  --image-ids "$V_ami" --query 'Images[0].RootDeviceName' --output text)"

LAUNCH_ATTEMPTED=1
INSTANCE_ID="$(capture i-dryrun0000000000 aws ec2 run-instances --region "$REGION" \
  --client-token "$RUN_ID" \
  --image-id "$V_ami" --instance-type "$V_instance_type" \
  --subnet-id "$V_subnet" --security-group-ids "$V_security_group" \
  --key-name "$V_key_name" --count 1 ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} \
  --instance-initiated-shutdown-behavior terminate \
  --user-data "$USER_DATA" \
  --block-device-mappings "DeviceName=$root_device,Ebs={VolumeSize=$V_volume_gb,DeleteOnTermination=true}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ravel-tier-b-fresh-box},{Key=ravel-commit,Value=$V_commit},{Key=$RUN_TAG_KEY,Value=$RUN_ID}]" \
  --query 'Instances[0].InstanceId' --output text)"
[[ "$INSTANCE_ID" =~ ^i-[0-9a-z]+$ ]] || {
  echo "fresh-box: run-instances returned no instance id: $INSTANCE_ID" >&2
  INSTANCE_ID=""
  exit 1
}
echo "fresh-box: launched $INSTANCE_ID" >&2

run aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

SSH_OPTS=(-i "$V_identity_file" -o StrictHostKeyChecking=accept-new
  -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -o ConnectTimeout=15)
if [ "$V_access" = ssm ]; then
  host="$INSTANCE_ID"
  SSH_OPTS+=(-o "ProxyCommand=aws ssm start-session --region $REGION --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p")
else
  host="$(capture 192.0.2.10 aws ec2 describe-instances --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' \
    --output text | awk '{ print ($1 != "None" && $1 != "") ? $1 : $2 }')"
fi
TARGET="$V_ssh_user@$host"

remote() {
  run ssh "${SSH_OPTS[@]}" "$TARGET" "$@"
}

reached=""
for ((i = 1; i <= REACH_ATTEMPTS; i++)); do
  if remote true; then
    reached=1
    break
  fi
  sleep "$REACH_SLEEP"
done
[ -n "$reached" ] || { echo "fresh-box: $INSTANCE_ID never became reachable over $V_access" >&2; exit 1; }

# The instance's own view of itself, not the flags: the label must name the
# hardware the numbers came from.
actual_type="$(capture "$V_instance_type" aws ec2 describe-instances --region "$REGION" \
  --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].InstanceType' --output text)"
if [ "$DRY_RUN" = 1 ]; then
  remote nproc
  remote uname -m
  cores="N"
  arch="ARCH"
else
  cores="$(remote nproc)"
  arch="$(remote uname -m)"
  [[ "$cores" =~ ^[0-9]+$ ]] || { echo "fresh-box: remote nproc printed '$cores', not a core count" >&2; exit 1; }
  [[ "$arch" =~ ^[A-Za-z0-9_]+$ ]] || { echo "fresh-box: remote uname -m printed '$arch', not an architecture" >&2; exit 1; }
fi

setup='set -euo pipefail
if command -v apt-get >/dev/null; then
  sudo apt-get update -y && sudo apt-get install -y git build-essential pkg-config python3 curl
elif command -v dnf >/dev/null; then
  sudo dnf install -y git gcc gcc-c++ make pkgconf python3 curl
else
  echo "fresh-box: no apt-get or dnf on this AMI" >&2; exit 1
fi
curl --proto =https --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none --profile minimal'
remote "bash -c $(printf '%q' "$setup")"

clone="set -euo pipefail; rm -rf $REMOTE_DIR; git clone $(printf '%q' "$V_repo_url") $REMOTE_DIR; cd $REMOTE_DIR; git checkout --detach $V_commit; test \"\$(git rev-parse HEAD)\" = $V_commit; . \"\$HOME/.cargo/env\"; rustup toolchain install; rustup show"
remote "bash -c $(printf '%q' "$clone")"

label="FRESH EC2 INSTANCE $actual_type ($cores cores, $arch), launched for this run and terminated after it (instance $INSTANCE_ID, $REGION, AMI $V_ami). Binary commit $V_commit. Corpus: none, every tier B bench generates its own synthetic input in-process. Knobs: BENCH_SAMPLE_SIZE=$V_sample_size BENCH_WARMUP=$V_warmup BENCH_MEASURE=$V_measure RAVEL_BENCH_MAX_SERIES=$V_max_series. No flush cadence: store-independent."
bench="set -euo pipefail; cd $REMOTE_DIR; . \"\$HOME/.cargo/env\"; BENCH_SAMPLE_SIZE=$V_sample_size BENCH_WARMUP=$V_warmup BENCH_MEASURE=$V_measure RAVEL_BENCH_MAX_SERIES=$V_max_series CARGO_BUILD_JOBS=$cores scripts/bench-tier-b.sh record $REMOTE_OUT $(printf '%q' "$label")"
remote "bash -c $(printf '%q' "$bench")"

run scp "${SSH_OPTS[@]}" "$TARGET:$REMOTE_DIR/$REMOTE_OUT" "$V_out"
if [ "$DRY_RUN" = 0 ]; then
  python3 -c 'import json, sys; json.load(open(sys.argv[1]))["benchmarks"]' "$V_out" \
    || { echo "fresh-box: $V_out is not a baseline file" >&2; exit 1; }
fi
echo "fresh-box: baseline at $V_out" >&2
