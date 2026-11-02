#!/usr/bin/env bash
# box/launch.sh -- launch one EC2 instance running box/run.sh for a
# run.toml, s3 lane only. Mirrors benchmarks/clickbench/kit/launch.sh's
# run()/capture() wrapper shape so --dry-run prints every aws call
# without touching an account.
#
# Usage: launch.sh <run.toml> [--dry-run]
set -u

RUN_TOML="${1:-}"
DRY_RUN=0
for a in "$@"; do
  [ "$a" = "--dry-run" ] && DRY_RUN=1
done
[ -n "$RUN_TOML" ] || { echo "usage: launch.sh <run.toml> [--dry-run]" >&2; exit 64; }

run() {
  if [ "$DRY_RUN" = 1 ]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; }
    return 0
  fi
  "$@"
}

capture() {
  if [ "$DRY_RUN" = 1 ]; then
    printf 'i-DRYRUN0000000000\n'
    return 0
  fi
  "$@"
}

read_toml_key() {
  python3 - "$RUN_TOML" "$1" <<'EOF'
import sys, tomllib
with open(sys.argv[1], "rb") as fh:
    cfg = tomllib.load(fh)
key = sys.argv[2]
val = cfg
for part in key.split("."):
    if not isinstance(val, dict) or part not in val:
        val = None
        break
    val = val[part]
print("" if val is None else val)
EOF
}

main() {
  local region ami instance_type run_id instance_id
  region="$(read_toml_key region)"; region="${region:-us-east-1}"
  ami="$(read_toml_key ami)"
  instance_type="$(read_toml_key instance_type)"; instance_type="${instance_type:-c6i.4xlarge}"
  run_id="$(read_toml_key run_id)"
  [ -n "$run_id" ] || { echo "run.toml must set run_id" >&2; exit 64; }

  instance_id="$(capture aws ec2 run-instances --region "$region" --image-id "$ami" \
    --instance-type "$instance_type" \
    --instance-market-options 'MarketType=spot' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=inv-$run_id}]" \
    --iam-instance-profile 'Name=inv-box-role' \
    --user-data "file://$(dirname "$0")/cloud-init.sh" \
    --query 'Instances[0].InstanceId' --output text)"
  echo "launched $instance_id (run_id=$run_id, region=$region)"
}

main
