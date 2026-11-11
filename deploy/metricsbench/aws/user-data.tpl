#!/bin/bash
# EC2 user data rendered by launch.sh. The shutdown is scheduled first, so the
# box terminates at the cap whatever happens after this line.
shutdown -h +__CAP_MIN__
mkdir -p /mb
cat >/mb/env <<'ENV'
__ENV__
ENV
. /mb/env
# Also to the serial console, so a box that shuts itself down here leaves its
# reason in get-console-output.
exec > >(tee -a /mb/userdata.log >/dev/console) 2>&1
# The snap store is not always reachable from the benchmark subnet (it timed
# out on 2026-11-11 while apt and awscli.amazonaws.com answered), so fall back
# to the official AWS CLI v2 bundle, installed under /usr/local/bin.
install_awscli_bundle() {
  apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -y -q unzip curl &&
    curl -sSfL --retry 3 -o /tmp/awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip &&
    unzip -q -o /tmp/awscliv2.zip -d /tmp && /tmp/aws/install --update
}
timeout 300 snap install aws-cli --classic || { echo "aws-cli snap install failed, trying the AWS CLI bundle"; install_awscli_bundle; } ||
  { echo "aws-cli install failed"; shutdown -h now; }
export PATH=/snap/bin:/usr/local/bin:$PATH
command -v aws || { echo "aws-cli not on PATH"; shutdown -h now; }
aws s3 cp --only-show-errors "s3://${BUCKET}/${PREFIX}/box-run.sh" /mb/box-run.sh || { echo "fetch failed"; shutdown -h now; }
echo "${BOXRUN_SHA256}  /mb/box-run.sh" | sha256sum -c - || { echo "box-run checksum mismatch"; shutdown -h now; }
bash -n /mb/box-run.sh || { echo "box-run does not parse"; shutdown -h now; }
PATH=/snap/bin:/usr/local/bin:$PATH nohup bash /mb/box-run.sh >/dev/null 2>&1 &
