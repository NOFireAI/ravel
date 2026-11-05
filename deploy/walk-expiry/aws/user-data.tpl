#!/bin/bash
# EC2 user data rendered by launch.sh. The 10-hour shutdown is armed first, so
# the box terminates at the cap whatever happens after this line: launch.sh
# sets the instance-initiated shutdown behaviour to terminate.
shutdown -h +600
mkdir -p /we
cat >/we/env <<'ENV'
__ENV__
ENV
. /we/env
exec >>/we/userdata.log 2>&1
snap install aws-cli --classic || { echo "aws-cli install failed"; shutdown -h now; }
export PATH=/snap/bin:$PATH
fail() {
  echo "stage=user-data reason=$1 at=$(date -u +%FT%TZ)" >/we/FAILED
  aws s3 cp --only-show-errors /we/FAILED "s3://${BUCKET}/${PREFIX}/FAILED" || true
  aws s3 cp --only-show-errors /we/userdata.log "s3://${BUCKET}/${PREFIX}/userdata.log" || true
  shutdown -h now
  exit 1
}
aws s3 cp --only-show-errors "s3://${BUCKET}/${PREFIX}/box-run.sh" /we/box-run.sh || fail "box-run fetch"
echo "${BOXRUN_SHA256}  /we/box-run.sh" | sha256sum -c - || fail "box-run checksum"
bash -n /we/box-run.sh || fail "box-run does not parse"
PATH=/snap/bin:$PATH nohup bash /we/box-run.sh >/dev/null 2>&1 &
