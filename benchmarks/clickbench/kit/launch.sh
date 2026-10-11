#!/usr/bin/env bash
# Launch a ClickBench kit run: one instance per arm, each running
# box/run.sh against its own commit (ADR-2867 change 2).
#
# This is the only credentialed step. It reads run.toml, refuses a band file
# that still holds a placeholder value (naming the key), checks with
# `aws iam simulate-principal-policy` that the instance profile's role may
# s3:GetObject the run's in/ prefix and s3:PutObject its out/ prefix (refusing
# with the role's name otherwise), tars the kit with a SHA256SUMS sidecar,
# uploads kit.tgz, kit.sha256 and run.toml to
# s3://<bucket>/<prefix>/runs/<run-id>/in/, and launches arm A and arm B
# under that run id. Each instance gets the instance profile, a 500 GB gp3
# root volume, --instance-initiated-shutdown-behavior terminate, and a
# user-data that arms `shutdown -h +600`, downloads in/ with the instance
# role, checks every sha256, writes the arm name to /root/kit/ARM and starts
# box/run.sh under nohup, logging to /root/kit/run.log. A failed download or
# check uploads FAILED to the arm's out/ prefix and shuts the instance down
# (which terminates it) before run.sh starts. From then on the instance role
# holds every credential.
#
# It prints the run id, both instance ids and the out/ prefix, and exits
# without waiting; fetch.sh collects the results. If the second launch fails,
# the first instance is terminated.
#
# --dry-run prints every aws command on stderr instead of running it, prints
# each arm's user-data between `DRY-RUN user-data <arm>:` and
# `DRY-RUN end user-data <arm>` lines, and keeps the staged in/ files in the
# directory it names.
#
# Written for bash 3.2 (macOS /bin/bash): no associative arrays, no mapfile,
# no ${var,,}, no date -d.
#
# Exit codes:
#   0   both instances launched (or, under --dry-run, every command printed)
#   1   an aws call failed; any instance already launched was terminated
#   64  bad usage, or run.toml or a band file refused; nothing was uploaded
#   65  the instance profile's role lacks S3 access to the run prefix;
#       nothing was uploaded
set -euo pipefail

usage() {
  cat <<'EOF'
usage: benchmarks/clickbench/kit/launch.sh [--dry-run] RUN_TOML

Launches arm A and arm B of a ClickBench kit run on one EC2 instance each and
exits; benchmarks/clickbench/kit/fetch.sh RUN_ID collects the results.
run.example.toml beside this script lists every key.

  --dry-run   print every aws command instead of running it
  --help      this text
EOF
}

die64() {
  echo "launch: $*" >&2
  exit 64
}

KIT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$KIT_DIR/../../.." && pwd)"
SHUTDOWN_MINUTES=600
VOLUME_GB=500
AL2023_AMI_PARAM=/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64

DRY_RUN=0
RUN_TOML=""
while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1 ;;
    -*) usage >&2; die64 "unknown argument: $1" ;;
    *) [ -z "$RUN_TOML" ] || die64 "one run.toml only"; RUN_TOML="$1" ;;
  esac
  shift
done
[ -n "$RUN_TOML" ] || { usage >&2; die64 "missing RUN_TOML"; }
[ -f "$RUN_TOML" ] || die64 "no such file: $RUN_TOML"
command -v python3 >/dev/null || die64 "python3 is required"

kit_py() {
  python3 "$KIT_DIR/lib/kit.py" "$@"
}

toml() {
  kit_py get "$RUN_TOML" "$1"
}

toml_opt() {
  kit_py get "$RUN_TOML" "$1" 2>/dev/null || true
}

kit_py check-run "$RUN_TOML" || die64 "run.toml refused"

SHA_A="$(toml sha_a)"
SHA_B="$(toml sha_b)"
REGION="$(toml region)"
BUCKET="$(toml bucket)"
PREFIX="$(toml prefix)"
PREFIX="${PREFIX%/}"
INSTANCE_TYPE="$(toml instance_type)"
PROFILE="$(toml instance_profile)"
CORPUS="$(toml corpus)"
LANES="$(toml lanes | tr '\n' ' ')"
AMI="$(toml_opt ami)"
SUBNET="$(toml_opt subnet)"
SECURITY_GROUP="$(toml_opt security_group)"
KEY_NAME="$(toml_opt key_name)"

# These are pasted into the user-data unquoted, so their alphabet is checked.
[[ "$REGION" =~ ^[a-z0-9-]+$ ]] || die64 "region '$REGION' is not a region name"
[[ "$BUCKET" =~ ^[a-z0-9][a-z0-9.-]+$ ]] || die64 "bucket '$BUCKET' is not a bucket name"
[[ "$PREFIX" =~ ^[A-Za-z0-9._/-]+$ ]] || die64 "prefix '$PREFIX' may hold only letters, digits, . _ / -"
[[ "$PROFILE" =~ ^[A-Za-z0-9+=,.@_-]+$ ]] || die64 "instance_profile '$PROFILE' is not a profile name"

resolve() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s\n' "$REPO_ROOT/$1" ;;
  esac
}

CORPUS_PATH="$(resolve "$CORPUS")"
[ -f "$CORPUS_PATH" ] || die64 "corpus not found: $CORPUS_PATH"
for lane in $LANES; do
  band="$(resolve "$(toml "bands.$lane")")"
  [ -f "$band" ] || die64 "lane $lane: band file not found: $band"
  kit_py check-bands "$band" || die64 "lane $lane: band file refused"
done

if command -v sha256sum >/dev/null; then
  sha256() { sha256sum "$@"; }
else
  sha256() { shasum -a 256 "$@"; }
fi

RUN_ID="clickbench-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
RUN_URI="s3://$BUCKET/$PREFIX/runs/$RUN_ID"
echo "launch: run id $RUN_ID" >&2

# run CMD...: run it, or under --dry-run print it on stderr and succeed.
run() {
  if [ "$DRY_RUN" = 1 ]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; } >&2
    return 0
  fi
  "$@"
}

# capture PLACEHOLDER CMD...: CMD's stdout, or under --dry-run the command on
# stderr and PLACEHOLDER on stdout.
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

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/clickbench-kit.XXXXXX")"
LAUNCHED=""
LAUNCH_OK=0

cleanup() {
  local code=$?
  trap - EXIT
  if [ "$LAUNCH_OK" != 1 ] && [ -n "$LAUNCHED" ]; then
    echo "launch: terminating $LAUNCHED after the failed launch" >&2
    # shellcheck disable=SC2086 # LAUNCHED is a space-separated list of ids
    run aws ec2 terminate-instances --region "$REGION" --instance-ids $LAUNCHED --output text >/dev/null || true
    [ "$code" -ne 0 ] || code=1
  fi
  case "$code" in
    0|64|65) ;;
    *) code=1 ;;
  esac
  if [ "$DRY_RUN" = 1 ]; then
    echo "launch: staged in/ files kept in $STAGE" >&2
  else
    rm -rf "$STAGE"
  fi
  exit "$code"
}
trap cleanup EXIT

# The role's access, before anything is uploaded.
ROLE_ARN="$(capture "arn:aws:iam::000000000000:role/DRY-RUN" aws iam get-instance-profile \
  --instance-profile-name "$PROFILE" --query 'InstanceProfile.Roles[0].Arn' --output text)"
[[ "$ROLE_ARN" =~ ^arn:aws[a-z-]*:iam::[0-9]+:role/ ]] \
  || { echo "launch: instance profile $PROFILE has no role (got '$ROLE_ARN')" >&2; exit 65; }
for check in "s3:GetObject arn:aws:s3:::$BUCKET/$PREFIX/runs/$RUN_ID/in/*" \
  "s3:PutObject arn:aws:s3:::$BUCKET/$PREFIX/runs/$RUN_ID/out/*"; do
  action="${check%% *}"
  resource="${check#* }"
  decision="$(capture allowed aws iam simulate-principal-policy --policy-source-arn "$ROLE_ARN" \
    --action-names "$action" --resource-arns "$resource" \
    --query 'EvaluationResults[0].EvalDecision' --output text)"
  if [ "$decision" != allowed ]; then
    echo "launch: role $ROLE_ARN (instance profile $PROFILE) lacks $action on $resource: simulate-principal-policy says '$decision'" >&2
    exit 65
  fi
done

# Stage the kit: the box scripts, the corpus, and each lane's band file.
mkdir -p "$STAGE/kit/box" "$STAGE/kit/lib" "$STAGE/kit/bands" "$STAGE/in"
cp "$KIT_DIR/box/run.sh" "$STAGE/kit/box/run.sh"
cp "$KIT_DIR/lib/kit.py" "$STAGE/kit/lib/kit.py"
cp "$CORPUS_PATH" "$STAGE/kit/corpus.json"
for lane in $LANES; do
  cp "$(resolve "$(toml "bands.$lane")")" "$STAGE/kit/bands/$lane.toml"
done
(cd "$STAGE/kit" && find . -type f | LC_ALL=C sort | while read -r f; do sha256 "$f"; done) >"$STAGE/SHA256SUMS"
mv "$STAGE/SHA256SUMS" "$STAGE/kit/SHA256SUMS"
COPYFILE_DISABLE=1 tar -czf "$STAGE/in/kit.tgz" -C "$STAGE" kit
cp "$RUN_TOML" "$STAGE/in/run.toml"
(cd "$STAGE/in" && sha256 kit.tgz run.toml >kit.sha256)

for f in kit.tgz kit.sha256 run.toml; do
  run aws s3 cp --region "$REGION" --only-show-errors "$STAGE/in/$f" "$RUN_URI/in/$f"
done

if [ -z "$AMI" ]; then
  AMI="$(capture ami-dryrun0000000000 aws ssm get-parameter --region "$REGION" \
    --name "$AL2023_AMI_PARAM" --query Parameter.Value --output text)"
fi
ROOT_DEVICE="$(capture /dev/xvda aws ec2 describe-images --region "$REGION" \
  --image-ids "$AMI" --query 'Images[0].RootDeviceName' --output text)"

NET_ARGS=()
[ -z "$SUBNET" ] || NET_ARGS+=(--subnet-id "$SUBNET")
[ -z "$SECURITY_GROUP" ] || NET_ARGS+=(--security-group-ids "$SECURITY_GROUP")
[ -z "$KEY_NAME" ] || NET_ARGS+=(--key-name "$KEY_NAME")

# user_data ARM: the arm's boot script.
user_data() {
  local arm="$1"
  cat <<EOF
#!/bin/bash
# ClickBench kit run $RUN_ID, arm $arm.
shutdown -h +$SHUTDOWN_MINUTES
KIT=/root/kit
IN=$RUN_URI/in
OUT=$RUN_URI/out/$arm
REGION=$REGION
mkdir -p "\$KIT/in"
exec >>"\$KIT/user-data.log" 2>&1
fail() {
  printf 'FAILED step=user-data %s\n' "\$1" >"\$KIT/FAILED"
  aws s3 cp --region "\$REGION" --only-show-errors "\$KIT/FAILED" "\$OUT/FAILED"
  shutdown -h now
  exit 1
}
for f in kit.tgz kit.sha256 run.toml; do
  aws s3 cp --region "\$REGION" --only-show-errors "\$IN/\$f" "\$KIT/in/\$f" || fail "download \$f"
done
(cd "\$KIT/in" && sha256sum -c kit.sha256) || fail "sha256 check of kit.tgz and run.toml"
tar -xzf "\$KIT/in/kit.tgz" -C "\$KIT" || fail "untar kit.tgz"
(cd "\$KIT/kit" && sha256sum -c SHA256SUMS) || fail "sha256 check of the kit files"
cp "\$KIT/in/run.toml" "\$KIT/run.toml" || fail "copy run.toml"
printf '%s\n' $arm >"\$KIT/ARM" || fail "write ARM"
printf '%s\n' $RUN_ID >"\$KIT/RUN_ID" || fail "write RUN_ID"
nohup bash "\$KIT/kit/box/run.sh" >"\$KIT/run.log" 2>&1 &
EOF
}

INSTANCE_A=""
INSTANCE_B=""
for arm in A B; do
  if [ "$arm" = A ]; then sha="$SHA_A"; else sha="$SHA_B"; fi
  ud="$(user_data "$arm")"
  if [ "$DRY_RUN" = 1 ]; then
    { echo "DRY-RUN user-data $arm:"; printf '%s\n' "$ud"; echo "DRY-RUN end user-data $arm"; } >&2
  fi
  id="$(capture "i-dryrun$(printf '%s' "$arm" | tr AB ab)000000000000" aws ec2 run-instances --region "$REGION" \
    --client-token "$RUN_ID-$arm" \
    --image-id "$AMI" --instance-type "$INSTANCE_TYPE" --count 1 \
    ${NET_ARGS[@]+"${NET_ARGS[@]}"} \
    --iam-instance-profile "Name=$PROFILE" \
    --instance-initiated-shutdown-behavior terminate \
    --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --user-data "$ud" \
    --block-device-mappings "DeviceName=$ROOT_DEVICE,Ebs={VolumeSize=$VOLUME_GB,VolumeType=gp3,DeleteOnTermination=true}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ravel-clickbench-kit},{Key=ravel-clickbench-run,Value=$RUN_ID},{Key=ravel-clickbench-arm,Value=$arm},{Key=ravel-commit,Value=$sha}]" \
    --query 'Instances[0].InstanceId' --output text)"
  [[ "$id" =~ ^i-[0-9a-z]+$ ]] || { echo "launch: run-instances for arm $arm returned no instance id: $id" >&2; exit 1; }
  LAUNCHED="${LAUNCHED:+$LAUNCHED }$id"
  if [ "$arm" = A ]; then INSTANCE_A="$id"; else INSTANCE_B="$id"; fi
done
LAUNCH_OK=1

printf 'run id:      %s\n' "$RUN_ID"
printf 'instance A:  %s (%s)\n' "$INSTANCE_A" "$SHA_A"
printf 'instance B:  %s (%s)\n' "$INSTANCE_B" "$SHA_B"
printf 'out prefix:  %s/out/\n' "$RUN_URI"
printf 'collect:     %s --run-toml %s %s\n' "$KIT_DIR/fetch.sh" "$RUN_TOML" "$RUN_ID"
