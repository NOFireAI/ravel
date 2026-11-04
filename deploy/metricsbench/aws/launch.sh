#!/bin/bash
# Launch one self-terminating MetricsBench box on EC2 (docs/internal/metricsbench-aws-runbook.md).
#
# Usage: launch.sh <harness-commit> <ravel-image@sha256:...> <ravel-source-commit> <anchor-ms> <label> [real-s3-prefix]
#
# With no real-s3-prefix the box runs the portable s3s-fs lane; with one it runs
# a `ci` smoke on <prefix>-ci-* buckets and then the measured profile on
# <prefix>-* buckets (create both sets first with make-buckets.sh).
#
# Required environment: MB_RESULTS_BUCKET, MB_SUBNET_ID, MB_SECURITY_GROUP_ID,
# MB_INSTANCE_PROFILE. Optional: MB_PROFILE (cardinality), MB_INSTANCE_TYPE
# (m6a.4xlarge), MB_CAP_MIN (2160), MB_VOLUME_GB (150), MB_RSS_BAND,
# MB_OBJECTS_BAND, MB_REPO (this checkout). AWS_PROFILE and AWS_REGION select
# the account and region as for any aws CLI call.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=${MB_REPO:-$(cd "$here/../../.." && pwd)}
[ "$#" -ge 5 ] || { echo "usage: $0 <harness-commit> <ravel-image@sha256:...> <ravel-source-commit> <anchor-ms> <label> [real-s3-prefix]" >&2; exit 64; }
harness_commit=$1 ravel_image=$2 ravel_source_commit=$3 anchor_ms=$4 label=$5 real_s3_prefix=${6:-}
: "${MB_RESULTS_BUCKET:?set MB_RESULTS_BUCKET to the bucket that receives the source, the box script and the results}"
: "${MB_SUBNET_ID:?set MB_SUBNET_ID}"
: "${MB_SECURITY_GROUP_ID:?set MB_SECURITY_GROUP_ID}"
: "${MB_INSTANCE_PROFILE:?set MB_INSTANCE_PROFILE to the instance profile whose role can read and write the buckets}"
case "$ravel_image" in *@sha256:*) ;; *) echo "the ravel image must be digest-pinned (name@sha256:...)" >&2; exit 64;; esac
case "$anchor_ms" in ''|*[!0-9]*) echo "anchor-ms must be an integer" >&2; exit 64;; esac
profile=${MB_PROFILE:-cardinality}
instance_type=${MB_INSTANCE_TYPE:-m6a.4xlarge}
cap_min=${MB_CAP_MIN:-2160}
volume_gb=${MB_VOLUME_GB:-150}
# The default bands are the cardinality profile's pre-registered ones (#2821
# comment 6590940753 for the Prometheus RSS band, comment 6611923577 for the
# data-object band). Another profile needs both bands chosen and
# pre-registered for it: they are refused here rather than defaulted.
if [ "$profile" != cardinality ] && { [ -z "${MB_RSS_BAND:-}" ] || [ -z "${MB_OBJECTS_BAND:-}" ]; }; then
  echo "MB_PROFILE=$profile needs MB_RSS_BAND and MB_OBJECTS_BAND set for it; the defaults are cardinality's" >&2
  exit 64
fi
rss_band=${MB_RSS_BAND:-prometheus:2147483648:6442450944}
objects_band=${MB_OBJECTS_BAND:-14000:16500}
boxrun="$here/box-run.sh"
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || true)}}
[ -n "$region" ] || { echo "no region: set AWS_REGION" >&2; exit 64; }

git -C "$repo" cat-file -e "${harness_commit}^{commit}" || { echo "unknown commit $harness_commit" >&2; exit 64; }
harness_sha=$(git -C "$repo" rev-parse "$harness_commit")
short=$(git -C "$repo" rev-parse --short=9 "$harness_commit")
run_id="mb-${label}-$(date -u +%Y%m%dT%H%M%SZ)-${short}"
prefix="metricsbench/${run_id}"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

git -C "$repo" archive --format=tar.gz -o "$work/src.tar.gz" "$harness_sha"
src_sha256=$(shasum -a 256 "$work/src.tar.gz" | cut -d' ' -f1)
boxrun_sha256=$(shasum -a 256 "$boxrun" | cut -d' ' -f1)
aws s3 cp --only-show-errors "$work/src.tar.gz" "s3://${MB_RESULTS_BUCKET}/${prefix}/src.tar.gz"
aws s3 cp --only-show-errors "$boxrun" "s3://${MB_RESULTS_BUCKET}/${prefix}/box-run.sh"

# Each value is shell-quoted: /mb/env is sourced on the box.
env_block=$(
  for kv in "BUCKET=${MB_RESULTS_BUCKET}" "PREFIX=${prefix}" "SRC_SHA256=${src_sha256}" \
    "BOXRUN_SHA256=${boxrun_sha256}" "HARNESS_COMMIT=${harness_sha}" "RAVEL_IMAGE=${ravel_image}" \
    "RAVEL_SOURCE_COMMIT=${ravel_source_commit}" "ANCHOR_MS=${anchor_ms}" "PROFILE=${profile}" \
    "RSS_BAND=${rss_band}" "OBJECTS_BAND=${objects_band}" "REAL_S3_PREFIX=${real_s3_prefix}" \
    "AWS_DEFAULT_REGION=${region}"; do
    printf '%s=%q\n' "${kv%%=*}" "${kv#*=}"
  done
)
python3 -I - "$here/user-data.tpl" "$work/user-data.sh" "$env_block" "$cap_min" <<'PY'
import sys
tpl, out, env, cap = sys.argv[1:5]
open(out, "w").write(open(tpl).read().replace("__ENV__", env).replace("__CAP_MIN__", cap))
PY
bash -n "$work/user-data.sh"

ami=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id --query Parameter.Value --output text)
instance_id=$(aws ec2 run-instances --client-token "$run_id" --image-id "$ami" --instance-type "$instance_type" \
  --subnet-id "$MB_SUBNET_ID" --security-group-ids "$MB_SECURITY_GROUP_ID" \
  --iam-instance-profile "Name=${MB_INSTANCE_PROFILE}" \
  --instance-initiated-shutdown-behavior terminate \
  --metadata-options HttpTokens=required,HttpPutResponseHopLimit=2 \
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${volume_gb},VolumeType=gp3,DeleteOnTermination=true}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=metricsbench-${label}},{Key=run,Value=${run_id}}]" \
  --user-data "file://$work/user-data.sh" --query 'Instances[0].InstanceId' --output text)
echo "instance=${instance_id} run=${run_id} prefix=s3://${MB_RESULTS_BUCKET}/${prefix}"
