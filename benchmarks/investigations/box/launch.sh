#!/usr/bin/env bash
# box/launch.sh: launch the s3 lane of a selective-investigations run (issue
# #3699, ADR-3692 decision 8) on one on-demand EC2 instance running
# box/run.sh --lane s3.
#
# The launch safety shape is benchmarks/clickbench/kit/launch.sh's: this is
# the only credentialed step. It reads run.toml, checks with
# `aws iam simulate-principal-policy` that the instance profile's role may
# read the run's in/ prefix, write its out/ prefix and read, write, delete
# and list box.store_bucket (refusing with the role's name otherwise), tars
# the harness with a SHA256SUMS sidecar, uploads kit.tgz, kit.sha256 and
# run.toml to s3://<bucket>/<prefix>/runs/<run-id>/in/, and launches one
# instance with the instance profile, a gp3 root volume of box.volume_gb,
# IMDSv2 required, --instance-initiated-shutdown-behavior terminate, and no
# spot request. Its user-data arms `shutdown -h +<box.max_minutes>` (default
# and ceiling 600) first, installs
# python3.11, downloads kit.tgz and run.toml with the instance role, checks
# them against the digests written into the user-data itself, checks
# SHA256SUMS inside the kit, writes the run id to /root/inv/RUN_ID (the
# marker box/run.sh --lane s3 refuses to run without) and starts
# box/run.sh. A failed download or check uploads FAILED to out/ and shuts the
# instance down, which terminates it. From then on the instance role holds
# every credential, and box/run.sh terminates the instance when it ends.
#
# It prints the run id, the instance id and the out/ prefix and exits without
# waiting; box/fetch.sh collects the results. If run-instances returned an id
# and a later step fails, that instance is terminated.
#
# --dry-run prints every aws command on stderr instead of running it, prints
# the user-data between `DRY-RUN user-data:` and `DRY-RUN end user-data`
# lines, and keeps the staged in/ files in the directory it names.
#
# Exit codes:
#   0   the instance launched (or, under --dry-run, every command printed)
#   1   an aws call failed; an instance already launched was terminated
#   64  bad usage or a run.toml value refused; nothing was uploaded
#   65  the instance profile cannot be read or its role lacks an S3
#       permission the run needs; nothing was uploaded
set -euo pipefail

usage() {
  cat <<'EOF'
usage: benchmarks/investigations/box/launch.sh [--dry-run] RUN_TOML

Launches the s3 lane of a selective-investigations run on one EC2 instance
and exits; benchmarks/investigations/box/fetch.sh RUN_TOML RUN_ID collects
the results. box/run.example.toml lists every key.

  --dry-run   print every aws command instead of running it
  --help      this text
EOF
}

die64() {
  echo "launch: $*" >&2
  exit 64
}

BOX_DIR="$(cd "$(dirname "$0")" && pwd)"
INV_DIR="$(cd "$BOX_DIR/.." && pwd)"
BENCH_DIR="$(cd "$INV_DIR/.." && pwd)"
AL2023_AMI_PARAM=/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64

DRY_RUN=0
RUN_TOML=""
while [ $# -gt 0 ]; do
  case "$1" in
    --help | -h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1 ;;
    -*) usage >&2; die64 "unknown argument: $1" ;;
    *) [ -z "$RUN_TOML" ] || die64 "one run.toml only"; RUN_TOML="$1" ;;
  esac
  shift
done
[ -n "$RUN_TOML" ] || { usage >&2; die64 "missing RUN_TOML"; }
[ -f "$RUN_TOML" ] || die64 "no such file: $RUN_TOML"
command -v python3 >/dev/null || die64 "python3 is required"

toml() {
  python3 "$BOX_DIR/lib/inv.py" get "$RUN_TOML" "$@"
}

LANE="$(toml run.lane)"
[ "$LANE" = s3 ] || die64 "run.lane is '$LANE': launch.sh starts the s3 lane only (box/run.sh --lane local runs the other)"
SHA="$(toml box.sha)"
REGION="$(toml box.region)"
BUCKET="$(toml box.bucket)"
PREFIX="$(toml box.prefix --default investigations)"
PREFIX="${PREFIX%/}"
STORE_BUCKET="$(toml box.store_bucket)"
PROFILE="$(toml box.instance_profile)"
INSTANCE_TYPE="$(toml box.instance_type --default c6a.4xlarge)"
VOLUME_GB="$(toml box.volume_gb --default 200)"
SHUTDOWN_MINUTES="$(toml box.max_minutes --default 600)"
AMI="$(toml box.ami --default "")"
SUBNET="$(toml box.subnet --default "")"
SECURITY_GROUP="$(toml box.security_group --default "")"
KEY_NAME="$(toml box.key_name --default "")"

# These are pasted into the user-data unquoted, so their alphabet is checked.
[[ "$SHA" =~ ^[0-9a-f]{40}$ ]] || die64 "box.sha '$SHA' is not a full 40-character commit SHA"
[[ "$REGION" =~ ^[a-z0-9-]+$ ]] || die64 "box.region '$REGION' is not a region name"
[[ "$BUCKET" =~ ^[a-z0-9][a-z0-9.-]+$ ]] || die64 "box.bucket '$BUCKET' is not a bucket name"
[[ "$STORE_BUCKET" =~ ^[a-z0-9][a-z0-9.-]+$ ]] || die64 "box.store_bucket '$STORE_BUCKET' is not a bucket name"
[ "$STORE_BUCKET" != "$BUCKET" ] || die64 "box.store_bucket must be its own bucket: the store keys the whole bucket"
[[ "$PREFIX" =~ ^[A-Za-z0-9._/-]+$ ]] || die64 "box.prefix '$PREFIX' may hold only letters, digits, . _ / -"
[[ "$PROFILE" =~ ^[A-Za-z0-9+=,.@_-]+$ ]] || die64 "box.instance_profile '$PROFILE' is not a profile name"
[[ "$INSTANCE_TYPE" =~ ^[a-z0-9]+\.[a-z0-9]+$ ]] || die64 "box.instance_type '$INSTANCE_TYPE' is not an instance type"
[[ "$VOLUME_GB" =~ ^[0-9]+$ ]] || die64 "box.volume_gb '$VOLUME_GB' is not a number"
[[ "$SHUTDOWN_MINUTES" =~ ^[0-9]+$ ]] && [ "$SHUTDOWN_MINUTES" -ge 1 ] && [ "$SHUTDOWN_MINUTES" -le 600 ] \
  || die64 "box.max_minutes '$SHUTDOWN_MINUTES' must be a whole number of minutes from 1 to 600"

if command -v sha256sum >/dev/null; then
  sha256() { sha256sum "$@"; }
else
  sha256() { shasum -a 256 "$@"; }
fi

RUN_ID="investigations-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
RUN_URI="s3://$BUCKET/$PREFIX/runs/$RUN_ID"
echo "launch: run id $RUN_ID" >&2

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

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/investigations-launch.XXXXXX")"
LAUNCHED=""
LAUNCH_OK=0

cleanup() {
  local code=$?
  trap - EXIT
  if [ "$LAUNCH_OK" != 1 ] && [ -n "$LAUNCHED" ]; then
    echo "launch: terminating $LAUNCHED after the failed launch" >&2
    run aws ec2 terminate-instances --region "$REGION" --instance-ids "$LAUNCHED" --output text >/dev/null || true
    [ "$code" -ne 0 ] || code=1
  fi
  case "$code" in
    0 | 64 | 65) ;;
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
  --instance-profile-name "$PROFILE" --query 'InstanceProfile.Roles[0].Arn' --output text)" \
  || { echo "launch: cannot read instance profile $PROFILE (missing, or this principal may not read it)" >&2; exit 65; }
[[ "$ROLE_ARN" =~ ^arn:aws[a-z-]*:iam::[0-9]+:role/ ]] \
  || { echo "launch: instance profile $PROFILE has no role (got '$ROLE_ARN')" >&2; exit 65; }
for check in "s3:GetObject arn:aws:s3:::$BUCKET/$PREFIX/runs/$RUN_ID/in/*" \
  "s3:PutObject arn:aws:s3:::$BUCKET/$PREFIX/runs/$RUN_ID/out/*" \
  "s3:GetObject arn:aws:s3:::$STORE_BUCKET/*" \
  "s3:PutObject arn:aws:s3:::$STORE_BUCKET/*" \
  "s3:DeleteObject arn:aws:s3:::$STORE_BUCKET/*" \
  "s3:ListBucket arn:aws:s3:::$STORE_BUCKET"; do
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

# Stage the harness at its repository paths: box/run.sh finds the hits corpus
# and mapping beside benchmarks/investigations, as in a checkout.
KIT_FILES="investigations/box/run.sh investigations/box/lib/inv.py investigations/gen_corpus.py
investigations/evaluate.py investigations/statements.toml investigations/bands.toml
investigations/hits.truth.json clickbench/hits.corpus.json clickbench/hits.mapping.toml"
for f in $KIT_FILES; do
  mkdir -p "$STAGE/kit/benchmarks/$(dirname "$f")"
  cp "$BENCH_DIR/$f" "$STAGE/kit/benchmarks/$f"
done
mkdir -p "$STAGE/in"
(cd "$STAGE/kit" && find . -type f | LC_ALL=C sort >"$STAGE/kit-files") \
  || { echo "launch: cannot list the staged kit files" >&2; exit 1; }
while read -r f; do
  (cd "$STAGE/kit" && sha256 "$f") || { echo "launch: cannot hash kit file $f" >&2; exit 1; }
done <"$STAGE/kit-files" >"$STAGE/SHA256SUMS"
[ "$(wc -l <"$STAGE/SHA256SUMS")" -eq "$(wc -l <"$STAGE/kit-files")" ] \
  || { echo "launch: SHA256SUMS does not list every kit file" >&2; exit 1; }
mv "$STAGE/SHA256SUMS" "$STAGE/kit/SHA256SUMS"
COPYFILE_DISABLE=1 tar -czf "$STAGE/in/kit.tgz" -C "$STAGE" kit
cp "$RUN_TOML" "$STAGE/in/run.toml"
(cd "$STAGE/in" && sha256 kit.tgz run.toml >kit.sha256)
# The user-data carries these two digests itself and the box checks against
# them, so rewriting kit.tgz and kit.sha256 in S3 together does not pass.
KIT_TGZ_SHA="$(sed -n 's/^\([0-9a-f]\{64\}\)  kit\.tgz$/\1/p' "$STAGE/in/kit.sha256")"
RUN_TOML_SHA="$(sed -n 's/^\([0-9a-f]\{64\}\)  run\.toml$/\1/p' "$STAGE/in/kit.sha256")"
[ -n "$KIT_TGZ_SHA" ] && [ -n "$RUN_TOML_SHA" ] \
  || { echo "launch: cannot read the kit.tgz and run.toml digests" >&2; exit 1; }

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

user_data() {
  cat <<EOF
#!/bin/bash
# Selective-investigations run $RUN_ID, commit $SHA.
shutdown -h +$SHUTDOWN_MINUTES
INV=/root/inv
IN=$RUN_URI/in
OUT=$RUN_URI/out
REGION=$REGION
mkdir -p "\$INV/in"
exec >>"\$INV/user-data.log" 2>&1
fail() {
  printf 'FAILED step=user-data %s\n' "\$1" >"\$INV/FAILED"
  aws s3 cp --region "\$REGION" --only-show-errors "\$INV/FAILED" "\$OUT/FAILED"
  shutdown -h now
  exit 1
}
dnf install -y python3.11 || fail "dnf install python3.11"
for f in kit.tgz run.toml; do
  aws s3 cp --region "\$REGION" --only-show-errors "\$IN/\$f" "\$INV/in/\$f" || fail "download \$f"
done
printf '%s  %s\n' $KIT_TGZ_SHA kit.tgz $RUN_TOML_SHA run.toml >"\$INV/in/kit.sha256" || fail "write kit.sha256"
(cd "\$INV/in" && sha256sum -c kit.sha256) || fail "sha256 check of kit.tgz and run.toml"
tar -xzf "\$INV/in/kit.tgz" -C "\$INV" || fail "untar kit.tgz"
(cd "\$INV/kit" && sha256sum -c SHA256SUMS) || fail "sha256 check of the kit files"
printf '%s\n' $RUN_ID >"\$INV/RUN_ID" || fail "write RUN_ID"
nohup bash "\$INV/kit/benchmarks/investigations/box/run.sh" --lane s3 --run-toml "\$INV/in/run.toml" \\
  --out "\$INV/out" >"\$INV/run.log" 2>&1 &
EOF
}

ud="$(user_data)"
if [ "$DRY_RUN" = 1 ]; then
  { echo "DRY-RUN user-data:"; printf '%s\n' "$ud"; echo "DRY-RUN end user-data"; } >&2
fi
id="$(capture i-dryrun0000000000000 aws ec2 run-instances --region "$REGION" \
  --client-token "$RUN_ID" \
  --image-id "$AMI" --instance-type "$INSTANCE_TYPE" --count 1 \
  ${NET_ARGS[@]+"${NET_ARGS[@]}"} \
  --iam-instance-profile "Name=$PROFILE" \
  --instance-initiated-shutdown-behavior terminate \
  --metadata-options HttpTokens=required,HttpEndpoint=enabled \
  --user-data "$ud" \
  --block-device-mappings "DeviceName=$ROOT_DEVICE,Ebs={VolumeSize=$VOLUME_GB,VolumeType=gp3,DeleteOnTermination=true}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ravel-investigations},{Key=ravel-investigations-run,Value=$RUN_ID},{Key=ravel-commit,Value=$SHA}]" \
  --query 'Instances[0].InstanceId' --output text)"
[[ "$id" =~ ^i-[0-9a-z]+$ ]] || { echo "launch: run-instances returned no instance id: $id" >&2; exit 1; }
LAUNCHED="$id"
LAUNCH_OK=1

printf 'run id:      %s\n' "$RUN_ID"
printf 'instance:    %s (%s)\n' "$id" "$SHA"
printf 'out prefix:  %s/out/\n' "$RUN_URI"
printf 'collect:     %s %s %s\n' "$BOX_DIR/fetch.sh" "$RUN_TOML" "$RUN_ID"
