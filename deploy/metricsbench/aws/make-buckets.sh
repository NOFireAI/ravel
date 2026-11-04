#!/bin/bash
# Create the four empty real-S3 buckets run.sh's real-S3 lane expects for each
# prefix, each with a bucket policy granting only the benchmark's instance role,
# so the role's own policy is not widened per run.
# Usage: MB_INSTANCE_ROLE_ARN=arn:aws:iam::<account>:role/<role> make-buckets.sh <prefix> [<prefix> ...]
# A real-S3 run needs <prefix> for the measured profile and <prefix>-ci for the
# smoke; run.sh refuses a bucket that already holds an object, so every run
# needs fresh prefixes.
set -euo pipefail
: "${MB_INSTANCE_ROLE_ARN:?set MB_INSTANCE_ROLE_ARN to the role behind MB_INSTANCE_PROFILE}"
[ "$#" -ge 1 ] || { echo "usage: $0 <prefix> [<prefix> ...]" >&2; exit 64; }
region=${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || true)}}
[ -n "$region" ] || { echo "no region: set AWS_REGION" >&2; exit 64; }
for p in "$@"; do
  for s in ravel mimir-blocks mimir-ruler mimir-alertmanager; do
    b="${p}-${s}"
    if [ "$region" = us-east-1 ]; then
      aws s3api create-bucket --bucket "$b" >/dev/null
    else
      aws s3api create-bucket --bucket "$b" --create-bucket-configuration "LocationConstraint=${region}" >/dev/null
    fi
    aws s3api wait bucket-exists --bucket "$b"
    aws s3api put-bucket-policy --bucket "$b" --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"${MB_INSTANCE_ROLE_ARN}\"},\"Action\":[\"s3:*Object*\",\"s3:ListBucket*\",\"s3:GetBucket*\",\"s3:*Multipart*\"],\"Resource\":[\"arn:aws:s3:::${b}\",\"arn:aws:s3:::${b}/*\"]}]}"
    echo "ok ${b}"
  done
done
