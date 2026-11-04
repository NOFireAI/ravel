#!/bin/bash
# EC2 user data rendered by launch.sh. The shutdown is scheduled first, so the
# box terminates at the cap whatever happens after this line.
shutdown -h +__CAP_MIN__
mkdir -p /mb
cat >/mb/env <<'ENV'
__ENV__
ENV
. /mb/env
exec >>/mb/userdata.log 2>&1
snap install aws-cli --classic || { echo "aws-cli install failed"; shutdown -h now; }
export PATH=/snap/bin:$PATH
aws s3 cp --only-show-errors "s3://${BUCKET}/${PREFIX}/box-run.sh" /mb/box-run.sh || { echo "fetch failed"; shutdown -h now; }
echo "${BOXRUN_SHA256}  /mb/box-run.sh" | sha256sum -c - || { echo "box-run checksum mismatch"; shutdown -h now; }
bash -n /mb/box-run.sh || { echo "box-run does not parse"; shutdown -h now; }
PATH=/snap/bin:$PATH nohup bash /mb/box-run.sh >/dev/null 2>&1 &
