#!/bin/bash
# Launch one self-terminating MetricsBench box on EC2 (docs/internal/metricsbench-aws-runbook.md).
#
# Usage: launch.sh <harness-commit> <ravel-image@sha256:...> <ravel-source-commit> <anchor-ms> <label> [real-s3-prefix]
#
# With no real-s3-prefix the box runs the portable s3s-fs lane; with one it runs
# a `ci` smoke on <prefix>-ci-* buckets and then the measured profile on
# <prefix>-* buckets (create both sets first with make-buckets.sh). With
# MB_RAVEL_ONLY=1 the measured run uses <prefix>-ravel alone
# (make-buckets.sh --ravel-only <prefix>); the ci smoke still needs all four.
#
# Required environment: MB_RESULTS_BUCKET, MB_SUBNET_ID, MB_SECURITY_GROUP_ID,
# MB_INSTANCE_PROFILE. Optional: MB_PROFILE (cardinality), MB_INSTANCE_TYPE
# (m6a.4xlarge), MB_CAP_MIN (2160), MB_VOLUME_GB (150), MB_RSS_BAND,
# MB_OBJECTS_BAND, MB_REPO (this checkout), MB_STAGE1 (1 runs run.sh's ADR-3694
# Stage 1 steps after the measured profile's folded read-back and after the ci
# smoke's, cardinality or ci only), MB_STAGE1_BANDS (the measured run's Stage 1
# band file, a path relative to the repository that must exist at the harness
# commit; required with MB_STAGE1=1 for any profile but ci, which defaults to
# deploy/metricsbench/stage1-bands.ci.json), MB_STAGE1_ARMS, MB_CONTROL_IMAGE
# (digest-pinned) and MB_CONTROL_SOURCE_COMMIT (run.sh's Stage 1 arms, on the
# measured run only; each needs MB_STAGE1=1), MB_L1_MAX_PART_BYTES (a positive
# integer, with l1 in MB_STAGE1_ARMS; the measured run only), MB_RAVEL_ONLY (1
# runs the measured profile as run.sh --ravel-only: history or ci, with
# MB_STAGE1=1 MB_STAGE1_ARMS=l1 and MB_OBJECTS_BAND, without MB_RSS_BAND or
# MB_CONTROL_IMAGE, on either lane; the ci smoke stays a full run).
# MB_STAGE1=1 on history needs MB_RAVEL_ONLY=1. AWS_PROFILE and AWS_REGION
# select the account and region as for any aws CLI call.
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
# run.sh refuses the same, but only after the box has built the harness and
# run the ci smoke. The Ravel-only shape checks no memory band, so it takes no
# MB_RSS_BAND, and only its objects band needs choosing.
ravel_only=${MB_RAVEL_ONLY:-}
case "$ravel_only" in
  ''|0) ravel_only="" ;;
  1) case "$profile" in
       history|ci) ;;
       *) echo "MB_RAVEL_ONLY=1 runs on the history and ci profiles only, not MB_PROFILE=$profile" >&2; exit 64;;
     esac
     if [ "${MB_STAGE1:-}" != 1 ] || [ "${MB_STAGE1_ARMS:-}" != l1 ]; then
       echo "MB_RAVEL_ONLY=1 runs the Stage 1 l1 arm alone and needs MB_STAGE1=1 MB_STAGE1_ARMS=l1" >&2
       exit 64
     fi
     if [ -n "${MB_CONTROL_IMAGE:-}" ] || [ -n "${MB_CONTROL_SOURCE_COMMIT:-}" ]; then
       echo "MB_RAVEL_ONLY=1 with MB_CONTROL_IMAGE: the control arm compares two Ravel builds over passes the Ravel-only shape does not run" >&2
       exit 64
     fi
     [ -z "${MB_RSS_BAND:-}" ] || { echo "MB_RAVEL_ONLY=1 with MB_RSS_BAND: run.sh --ravel-only refuses --rss-band" >&2; exit 64; }
     [ -n "${MB_OBJECTS_BAND:-}" ] || { echo "MB_RAVEL_ONLY=1 with MB_PROFILE=$profile needs MB_OBJECTS_BAND set for it; the default is cardinality's" >&2; exit 64; }
     # The shipped ci Stage 1 band file is pre-registered for a four-engine
     # run; a Ravel-only run stamps its Stage 1 compare figures skipped, so
     # it fails that file. Every Ravel-only launch names its own band file.
     [ -n "${MB_STAGE1_BANDS:-}" ] || { echo "MB_RAVEL_ONLY=1 needs MB_STAGE1_BANDS, a band file pre-registered for the Ravel-only shape; the ci default is for a four-engine run" >&2; exit 64; }
     ;;
  *) echo "MB_RAVEL_ONLY must be 1, 0 or unset, not '$ravel_only'" >&2; exit 64;;
esac
# The default bands are the cardinality profile's pre-registered ones (#2821
# comment 6590940753 for the Prometheus RSS band, comment 6611923577 for the
# data-object band). Another profile needs both bands chosen and
# pre-registered for it: they are refused here rather than defaulted.
if [ -z "$ravel_only" ] && [ "$profile" != cardinality ] && { [ -z "${MB_RSS_BAND:-}" ] || [ -z "${MB_OBJECTS_BAND:-}" ]; }; then
  echo "MB_PROFILE=$profile needs MB_RSS_BAND and MB_OBJECTS_BAND set for it; the defaults are cardinality's" >&2
  exit 64
fi
# run.sh refuses the same values, but only after the box has built the
# harness and run the ci smoke. The history case is narrower here than in
# run.sh: run.sh accepts history with the l1 arm alone, launch.sh also
# requires MB_RAVEL_ONLY=1.
stage1=${MB_STAGE1:-}
case "$stage1" in
  ''|0) stage1="" ;;
  1) case "$profile" in
       cardinality|ci) ;;
       history) [ -n "$ravel_only" ] || { echo "MB_STAGE1=1 with MB_PROFILE=history needs MB_RAVEL_ONLY=1 MB_STAGE1_ARMS=l1" >&2; exit 64; } ;;
       *) echo "MB_STAGE1=1 runs on the cardinality and ci profiles only, not MB_PROFILE=$profile" >&2; exit 64;;
     esac ;;
  *) echo "MB_STAGE1 must be 1, 0 or unset, not '$stage1'" >&2; exit 64;;
esac
# box-run.sh refuses the same, but only after the box has booted.
stage1_bands=${MB_STAGE1_BANDS:-}
if [ -n "$stage1" ] && [ "$profile" != ci ] && [ -z "$stage1_bands" ]; then
  echo "MB_STAGE1=1 with MB_PROFILE=$profile needs MB_STAGE1_BANDS, a band file pre-registered for that profile" >&2
  exit 64
fi
if [ -n "$stage1_bands" ] && [ -z "$stage1" ]; then
  echo "MB_STAGE1_BANDS is set but MB_STAGE1 is not 1" >&2
  exit 64
fi
stage1_arms=${MB_STAGE1_ARMS:-}
control_image=${MB_CONTROL_IMAGE:-}
control_source_commit=${MB_CONTROL_SOURCE_COMMIT:-}
if { [ -n "$stage1_arms" ] || [ -n "$control_image" ] || [ -n "$control_source_commit" ]; } && [ -z "$stage1" ]; then
  echo "MB_STAGE1_ARMS and MB_CONTROL_IMAGE add Stage 1 arms and need MB_STAGE1=1" >&2
  exit 64
fi
if [ -n "$control_image" ] || [ -n "$control_source_commit" ]; then
  case "$control_image" in *@sha256:*) ;; *) echo "MB_CONTROL_IMAGE must be digest-pinned (name@sha256:...)" >&2; exit 64;; esac
  [ -n "$control_source_commit" ] || { echo "MB_CONTROL_IMAGE needs MB_CONTROL_SOURCE_COMMIT" >&2; exit 64; }
fi
l1_max_part_bytes=${MB_L1_MAX_PART_BYTES:-}
if [ -n "$l1_max_part_bytes" ]; then
  case ",$stage1_arms," in
    *,l1,*) ;;
    *) echo "MB_L1_MAX_PART_BYTES sets the l1 arm's compaction and needs MB_STAGE1=1 with l1 in MB_STAGE1_ARMS" >&2; exit 64;;
  esac
  [[ "$l1_max_part_bytes" =~ ^[1-9][0-9]*$ ]] ||
    { echo "MB_L1_MAX_PART_BYTES must be a positive integer, not '$l1_max_part_bytes'" >&2; exit 64; }
fi
rss_band=${MB_RSS_BAND:-prometheus:2147483648:6442450944}
[ -z "$ravel_only" ] || rss_band=""
objects_band=${MB_OBJECTS_BAND:-14000:16500}
boxrun="$here/box-run.sh"
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || true)}}
[ -n "$region" ] || { echo "no region: set AWS_REGION" >&2; exit 64; }

git -C "$repo" cat-file -e "${harness_commit}^{commit}" || { echo "unknown commit $harness_commit" >&2; exit 64; }
harness_sha=$(git -C "$repo" rev-parse "$harness_commit")
short=$(git -C "$repo" rev-parse --short=9 "$harness_commit")
if [ -n "$stage1_bands" ] &&
  [ "$(git -C "$repo" cat-file -t "${harness_sha}:${stage1_bands}" 2>/dev/null)" != blob ]; then
  echo "MB_STAGE1_BANDS=$stage1_bands is not a file at $harness_commit" >&2
  exit 64
fi
# run.sh refuses a --ravel-source-commit that is not a full 40-character id,
# and only after the box has built the harness, so resolve it here.
if git -C "$repo" cat-file -e "${ravel_source_commit}^{commit}" 2>/dev/null; then
  ravel_source_commit=$(git -C "$repo" rev-parse "${ravel_source_commit}^{commit}")
fi
case "$ravel_source_commit" in
  *[!0-9a-f]*) echo "ravel-source-commit must be a commit in $repo or a full 40-character id" >&2; exit 64;;
esac
[ "${#ravel_source_commit}" -eq 40 ] || { echo "ravel-source-commit must be a commit in $repo or a full 40-character id" >&2; exit 64; }
if [ -n "$control_source_commit" ]; then
  if git -C "$repo" cat-file -e "${control_source_commit}^{commit}" 2>/dev/null; then
    control_source_commit=$(git -C "$repo" rev-parse "${control_source_commit}^{commit}")
  fi
  case "$control_source_commit" in
    *[!0-9a-f]*) echo "MB_CONTROL_SOURCE_COMMIT must be a commit in $repo or a full 40-character id" >&2; exit 64;;
  esac
  [ "${#control_source_commit}" -eq 40 ] || { echo "MB_CONTROL_SOURCE_COMMIT must be a commit in $repo or a full 40-character id" >&2; exit 64; }
fi
run_id="mb-${label}-$(date -u +%Y%m%dT%H%M%SZ)-${short}"
prefix="metricsbench/${run_id}"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

git -C "$repo" archive --format=tar.gz -o "$work/src.tar.gz" "$harness_sha"
src_sha256=$(shasum -a 256 "$work/src.tar.gz" | cut -d' ' -f1)
boxrun_sha256=$(shasum -a 256 "$boxrun" | cut -d' ' -f1)
aws s3 cp --only-show-errors "$work/src.tar.gz" "s3://${MB_RESULTS_BUCKET}/${prefix}/src.tar.gz"
aws s3 cp --only-show-errors "$boxrun" "s3://${MB_RESULTS_BUCKET}/${prefix}/box-run.sh"

# Each value is shell-quoted: /mb/env is sourced on the box. RAVEL_ONLY and
# L1_MAX_PART_BYTES are written only when set, so a launch without them renders
# the same user data as before they existed.
env_kvs=("BUCKET=${MB_RESULTS_BUCKET}" "PREFIX=${prefix}" "SRC_SHA256=${src_sha256}" \
  "BOXRUN_SHA256=${boxrun_sha256}" "HARNESS_COMMIT=${harness_sha}" "RAVEL_IMAGE=${ravel_image}" \
  "RAVEL_SOURCE_COMMIT=${ravel_source_commit}" "ANCHOR_MS=${anchor_ms}" "PROFILE=${profile}" \
  "RSS_BAND=${rss_band}" "OBJECTS_BAND=${objects_band}" "REAL_S3_PREFIX=${real_s3_prefix}" \
  "STAGE1=${stage1}" "STAGE1_BANDS=${stage1_bands}" "STAGE1_ARMS=${stage1_arms}" \
  "CONTROL_IMAGE=${control_image}" "CONTROL_SOURCE_COMMIT=${control_source_commit}" \
  "AWS_DEFAULT_REGION=${region}")
[ -z "$ravel_only" ] || env_kvs+=("RAVEL_ONLY=${ravel_only}")
[ -z "$l1_max_part_bytes" ] || env_kvs+=("L1_MAX_PART_BYTES=${l1_max_part_bytes}")
env_block=$(
  for kv in "${env_kvs[@]}"; do
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
