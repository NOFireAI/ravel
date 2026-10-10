#!/usr/bin/env bash
# Record an ADR-0070 tier B timing baseline on a fresh EC2 instance launched
# for the run and terminated after it.
#
# A tier B number means something only against a baseline taken on the same
# hardware under the same load, so a baseline is never recorded on a shared or
# long-lived box. This launches one instance, builds the given commit there,
# runs `scripts/bench-tier-b.sh record` with the five provenance stamps
# (bench/baselines/README.md), copies the baseline back, and terminates the
# instance on every exit path.
#
# Every input is required and has no default; run with --help for the list.
#
# Exit codes:
#   0   baseline copied back and the instance confirmed shutting-down or
#       terminated
#   1   a step failed (the instance is still terminated)
#   64  a required input is missing or malformed; nothing was launched
#   70  the instance could not be confirmed terminated; its id is printed on
#       stderr and a human must terminate it
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/bench/fresh-box.sh [--dry-run] --instance-type T --ami AMI
         --subnet SUBNET --security-group SG --region REGION
         --access ssh|ssm [--instance-profile NAME] --key-name NAME
         --identity-file PATH --ssh-user USER
         --volume-gb N --repo-url URL --commit SHA --out PATH
         --sample-size N --warmup S --measure S --max-series N

Launches one EC2 instance, records a tier B baseline on it with
scripts/bench-tier-b.sh record, copies it to --out, and terminates the
instance on every exit path. Every input is required; none has a default.
Each flag can also be given as the environment variable in brackets.

  --instance-type   EC2 instance type                    [FRESH_BOX_INSTANCE_TYPE]
  --ami             AMI id (Linux with apt-get or dnf)    [FRESH_BOX_AMI]
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

# name|flag|variable, in the order a missing one is reported.
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

declare -A VAL=()
for spec in "${INPUTS[@]}"; do
  IFS='|' read -r name _flag var <<<"$spec"
  VAL[$name]="${!var:-}"
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
      VAL[$name]="$2"
      matched=1
      break
    fi
  done
  [ -n "$matched" ] || { usage >&2; die64 "unknown argument: $1"; }
  shift 2
done

for spec in "${INPUTS[@]}"; do
  IFS='|' read -r name flag var <<<"$spec"
  [ -n "${VAL[$name]}" ] || die64 "missing required input $flag ($var)"
done

case "${VAL[access]}" in
  ssh) [ -z "$INSTANCE_PROFILE" ] || die64 "--instance-profile applies only to --access ssm" ;;
  ssm) [ -n "$INSTANCE_PROFILE" ] || die64 "missing required input --instance-profile (FRESH_BOX_INSTANCE_PROFILE) for --access ssm" ;;
  *) die64 "--access must be ssh or ssm, got ${VAL[access]}" ;;
esac
PROFILE_ARGS=()
if [ -n "$INSTANCE_PROFILE" ]; then
  PROFILE_ARGS=(--iam-instance-profile "Name=$INSTANCE_PROFILE")
fi
[[ "${VAL[commit]}" =~ ^[0-9a-f]{40}$ ]] || die64 "--commit must be a full 40-character commit, got ${VAL[commit]}"
for name in volume_gb sample_size warmup measure max_series; do
  [[ "${VAL[$name]}" =~ ^[0-9]+$ ]] || die64 "--${name//_/-} must be a whole number, got ${VAL[$name]}"
done

REGION="${VAL[region]}"
CONFIRM_ATTEMPTS="${FRESH_BOX_CONFIRM_ATTEMPTS:-20}"
CONFIRM_SLEEP="${FRESH_BOX_CONFIRM_SLEEP:-15}"
REACH_ATTEMPTS="${FRESH_BOX_REACH_ATTEMPTS:-40}"
REACH_SLEEP="${FRESH_BOX_REACH_SLEEP:-15}"
REMOTE_DIR="ravel"
REMOTE_OUT="tier-b-baseline.json"

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

terminate() {
  local code=$?
  trap - EXIT
  if [ -z "$INSTANCE_ID" ]; then
    exit "$code"
  fi
  echo "fresh-box: terminating $INSTANCE_ID" >&2
  run aws ec2 terminate-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
    --output text >/dev/null || true
  local state="" i
  for ((i = 1; i <= CONFIRM_ATTEMPTS; i++)); do
    state="$(capture terminated aws ec2 describe-instances --region "$REGION" \
      --instance-ids "$INSTANCE_ID" \
      --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || true)"
    case "$state" in
      shutting-down|terminated)
        echo "fresh-box: $INSTANCE_ID is $state" >&2
        exit "$code"
        ;;
    esac
    sleep "$CONFIRM_SLEEP"
  done
  echo "fresh-box: COULD NOT CONFIRM TERMINATION of instance $INSTANCE_ID in $REGION (last state: ${state:-unknown}); terminate it by hand" >&2
  exit 70
}
trap terminate EXIT

root_device="$(capture /dev/sda1 aws ec2 describe-images --region "$REGION" \
  --image-ids "${VAL[ami]}" --query 'Images[0].RootDeviceName' --output text)"

INSTANCE_ID="$(capture i-dryrun0000000000 aws ec2 run-instances --region "$REGION" \
  --image-id "${VAL[ami]}" --instance-type "${VAL[instance_type]}" \
  --subnet-id "${VAL[subnet]}" --security-group-ids "${VAL[security_group]}" \
  --key-name "${VAL[key_name]}" --count 1 ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} \
  --instance-initiated-shutdown-behavior terminate \
  --block-device-mappings "DeviceName=$root_device,Ebs={VolumeSize=${VAL[volume_gb]},DeleteOnTermination=true}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ravel-tier-b-fresh-box},{Key=ravel-commit,Value=${VAL[commit]}}]" \
  --query 'Instances[0].InstanceId' --output text)"
[[ "$INSTANCE_ID" =~ ^i-[0-9a-z]+$ ]] || {
  echo "fresh-box: run-instances returned no instance id: $INSTANCE_ID" >&2
  INSTANCE_ID=""
  exit 1
}
echo "fresh-box: launched $INSTANCE_ID" >&2

run aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

SSH_OPTS=(-i "${VAL[identity_file]}" -o StrictHostKeyChecking=accept-new
  -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -o ConnectTimeout=15)
if [ "${VAL[access]}" = ssm ]; then
  host="$INSTANCE_ID"
  SSH_OPTS+=(-o "ProxyCommand=aws ssm start-session --region $REGION --target %h --document-name AWS-StartSSHSession --parameters portNumber=%p")
else
  host="$(capture 192.0.2.10 aws ec2 describe-instances --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' \
    --output text | awk '{ print ($1 != "None" && $1 != "") ? $1 : $2 }')"
fi
TARGET="${VAL[ssh_user]}@$host"

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
[ -n "$reached" ] || { echo "fresh-box: $INSTANCE_ID never became reachable over ${VAL[access]}" >&2; exit 1; }

# The instance's own view of itself, not the flags: the label must name the
# hardware the numbers came from.
actual_type="$(capture "${VAL[instance_type]}" aws ec2 describe-instances --region "$REGION" \
  --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].InstanceType' --output text)"
if [ "$DRY_RUN" = 1 ]; then
  remote nproc
  remote uname -m
  cores="N"
  arch="ARCH"
else
  cores="$(remote nproc)"
  arch="$(remote uname -m)"
fi

setup=$(cat <<'REMOTE'
set -euo pipefail
if command -v apt-get >/dev/null; then
  sudo apt-get update -y && sudo apt-get install -y git build-essential pkg-config python3 curl
elif command -v dnf >/dev/null; then
  sudo dnf install -y git gcc gcc-c++ make pkgconf python3 curl
else
  echo "fresh-box: no apt-get or dnf on this AMI" >&2; exit 1
fi
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none --profile minimal
REMOTE
)
remote "bash -c $(printf '%q' "$setup")"

clone="set -euo pipefail; rm -rf $REMOTE_DIR; git clone $(printf '%q' "${VAL[repo_url]}") $REMOTE_DIR; cd $REMOTE_DIR; git checkout --detach ${VAL[commit]}; test \"\$(git rev-parse HEAD)\" = ${VAL[commit]}; . \"\$HOME/.cargo/env\"; rustup toolchain install; rustup show"
remote "bash -c $(printf '%q' "$clone")"

label="FRESH EC2 INSTANCE $actual_type ($cores cores, $arch), launched for this run and terminated after it (instance $INSTANCE_ID, $REGION, AMI ${VAL[ami]}). Binary commit ${VAL[commit]}. Corpus: none, every tier B bench generates its own synthetic input in-process. Knobs: BENCH_SAMPLE_SIZE=${VAL[sample_size]} BENCH_WARMUP=${VAL[warmup]} BENCH_MEASURE=${VAL[measure]} RAVEL_BENCH_MAX_SERIES=${VAL[max_series]}. No flush cadence: store-independent."
bench="set -euo pipefail; cd $REMOTE_DIR; . \"\$HOME/.cargo/env\"; BENCH_SAMPLE_SIZE=${VAL[sample_size]} BENCH_WARMUP=${VAL[warmup]} BENCH_MEASURE=${VAL[measure]} RAVEL_BENCH_MAX_SERIES=${VAL[max_series]} CARGO_BUILD_JOBS=$cores scripts/bench-tier-b.sh record $REMOTE_OUT $(printf '%q' "$label")"
remote "bash -c $(printf '%q' "$bench")"

run scp "${SSH_OPTS[@]}" "$TARGET:$REMOTE_DIR/$REMOTE_OUT" "${VAL[out]}"
if [ "$DRY_RUN" = 0 ]; then
  python3 -c 'import json, sys; json.load(open(sys.argv[1]))["benchmarks"]' "${VAL[out]}" \
    || { echo "fresh-box: ${VAL[out]} is not a baseline file" >&2; exit 1; }
fi
echo "fresh-box: baseline at ${VAL[out]}" >&2
