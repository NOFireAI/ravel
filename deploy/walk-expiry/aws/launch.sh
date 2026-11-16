#!/bin/bash
# Launch one self-terminating walk-expiry box on EC2
# (docs/internal/walk-expiry-aws-runbook.md, issue #3779 item 1).
#
# Usage: launch.sh [--dry-run] <commit> <store-bucket> <label>
#
#   commit        the full 40-character commit the probe is built from
#   store-bucket  an empty S3 bucket the probe's Ravel store writes to
#   label         [a-z0-9-], names the run
#
# Required environment: WE_RESULTS_BUCKET (receives the source, the box
# script, the results and the FAILED marker), WE_INSTANCE_PROFILE (the sandbox
# instance profile, whose role can read and write both buckets), WE_SUBNET_ID,
# WE_SECURITY_GROUP_ID. Optional: WE_VOLUME_GB (100), WE_REPO (this checkout).
# AWS_PROFILE and AWS_REGION select the account and region as for any aws CLI
# call.
#
# --dry-run prints every aws and git command it would run and the rendered user
# data, and runs none of them. It still validates the arguments.
#
# Bash 3.2 compatible (macOS /bin/bash): no associative arrays, no mapfile.
set -euo pipefail

dry=0
if [ "${1:-}" = "--dry-run" ]; then
  dry=1
  shift
fi
usage="usage: $0 [--dry-run] <commit> <store-bucket> <label>"
[ "$#" -eq 3 ] || { echo "$usage" >&2; exit 64; }
commit=$1 store_bucket=$2 label=$3

here=$(cd "$(dirname "$0")" && pwd)
repo=${WE_REPO:-$(cd "$here/../../.." && pwd)}
: "${WE_RESULTS_BUCKET:?set WE_RESULTS_BUCKET to the bucket that receives the source, the box script and the results}"
: "${WE_INSTANCE_PROFILE:?set WE_INSTANCE_PROFILE to the sandbox instance profile}"
: "${WE_SUBNET_ID:?set WE_SUBNET_ID}"
: "${WE_SECURITY_GROUP_ID:?set WE_SECURITY_GROUP_ID}"

case "$commit" in
  *[!0-9a-f]*|'') echo "commit must be a full 40-character lowercase commit id, got '$commit'" >&2; exit 64;;
esac
[ "${#commit}" -eq 40 ] || { echo "commit must be a full 40-character lowercase commit id, got ${#commit} characters" >&2; exit 64; }
case "$store_bucket" in
  *[!a-z0-9.-]*|'') echo "store-bucket must be a bare bucket name ([a-z0-9.-]), not a URL" >&2; exit 64;;
esac
[ "$store_bucket" != "$WE_RESULTS_BUCKET" ] || { echo "the store bucket must not be the results bucket" >&2; exit 64; }
case "$label" in
  *[!a-z0-9-]*|'') echo "label must be [a-z0-9-]" >&2; exit 64;;
esac

# The instance type is fixed: the run's figures are stamped with it.
instance_type=c6a.4xlarge
volume_gb=${WE_VOLUME_GB:-100}
case "$volume_gb" in ''|*[!0-9]*) echo "WE_VOLUME_GB must be an integer" >&2; exit 64;; esac
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-}}
if [ -z "$region" ] && [ "$dry" -eq 0 ]; then
  region=$(aws configure get region 2>/dev/null || true)
fi
[ -n "$region" ] || { echo "no region: set AWS_REGION" >&2; exit 64; }

boxrun="$here/box-run.sh"
template="$here/user-data.tpl"
short=$(printf '%s' "$commit" | cut -c1-9)
run_id="we-${label}-$(date -u +%Y%m%dT%H%M%SZ)-${short}"
prefix="walk-expiry/${run_id}"

show() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

if [ "$dry" -eq 1 ]; then
  show git -C "$repo" cat-file -e "${commit}^{commit}"
  show git -C "$repo" archive --format=tar.gz -o "$work/src.tar.gz" "$commit"
  src_sha256="<sha256 of src.tar.gz>"
  boxrun_sha256=$(shasum -a 256 "$boxrun" | cut -d' ' -f1)
  show aws s3 cp --only-show-errors "$work/src.tar.gz" "s3://${WE_RESULTS_BUCKET}/${prefix}/src.tar.gz"
  show aws s3 cp --only-show-errors "$boxrun" "s3://${WE_RESULTS_BUCKET}/${prefix}/box-run.sh"
else
  git -C "$repo" cat-file -e "${commit}^{commit}" || { echo "commit $commit is not in $repo" >&2; exit 64; }
  git -C "$repo" archive --format=tar.gz -o "$work/src.tar.gz" "$commit"
  src_sha256=$(shasum -a 256 "$work/src.tar.gz" | cut -d' ' -f1)
  boxrun_sha256=$(shasum -a 256 "$boxrun" | cut -d' ' -f1)
  aws s3 cp --only-show-errors "$work/src.tar.gz" "s3://${WE_RESULTS_BUCKET}/${prefix}/src.tar.gz"
  aws s3 cp --only-show-errors "$boxrun" "s3://${WE_RESULTS_BUCKET}/${prefix}/box-run.sh"
fi

# Each value is shell-quoted: /we/env is sourced on the box.
env_block=""
for kv in "BUCKET=${WE_RESULTS_BUCKET}" "PREFIX=${prefix}" "SRC_SHA256=${src_sha256}" \
  "BOXRUN_SHA256=${boxrun_sha256}" "COMMIT=${commit}" "STORE_BUCKET=${store_bucket}" \
  "AWS_DEFAULT_REGION=${region}"; do
  env_block="${env_block}$(printf '%s=%q' "${kv%%=*}" "${kv#*=}")
"
done
tpl=$(cat "$template")
# Bash 5.2 expands & in a substitution's replacement; 3.2 has no such option.
shopt -u patsub_replacement 2>/dev/null || true
rendered=${tpl/__ENV__/${env_block%
}}
printf '%s\n' "$rendered" >"$work/user-data.sh"
bash -n "$work/user-data.sh"

if [ "$dry" -eq 1 ]; then
  show aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text
  ami="<ami from ssm>"
else
  ami=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
fi
launch_args=(ec2 run-instances --client-token "$run_id" --image-id "$ami" --instance-type "$instance_type"
  --subnet-id "$WE_SUBNET_ID" --security-group-ids "$WE_SECURITY_GROUP_ID"
  --iam-instance-profile "Name=${WE_INSTANCE_PROFILE}"
  --instance-initiated-shutdown-behavior terminate
  --metadata-options HttpTokens=required,HttpPutResponseHopLimit=2
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${volume_gb},VolumeType=gp3,DeleteOnTermination=true}"
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=walk-expiry-${label}},{Key=run,Value=${run_id}}]"
  --user-data "file://$work/user-data.sh" --query 'Instances[0].InstanceId' --output text)
if [ "$dry" -eq 1 ]; then
  show aws "${launch_args[@]}"
  echo "--- rendered user data ---"
  cat "$work/user-data.sh"
  echo "--- end user data ---"
  echo "dry run: run=${run_id} prefix=s3://${WE_RESULTS_BUCKET}/${prefix}"
  exit 0
fi
instance_id=$(aws "${launch_args[@]}")
echo "instance=${instance_id} run=${run_id} prefix=s3://${WE_RESULTS_BUCKET}/${prefix}"
