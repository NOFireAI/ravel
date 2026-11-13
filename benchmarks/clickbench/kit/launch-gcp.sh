#!/usr/bin/env bash
# Launch a ClickBench kit run on GCE: one box (arm A) running box/run.sh
# against a fresh GCS bucket through Ravel's native GCS store, measuring the
# run.toml's release_tag binaries as published. The GCP counterpart of
# launch.sh; box/run.sh is the same script, switched by cloud.
#
# Before anything is created it refuses a run.toml that is not cloud = "gcp"
# (kit.py check-run) or whose prereg file does not name every judged figure
# (kit.py check-prereg), and checks that:
#   - release_tag names sha_a in this checkout, and run.toml's mapping is
#     byte-identical to that commit's benchmarks/clickbench/hits.mapping.toml;
#   - the release's SHA256SUMS verifies against its keyless signature
#     (cosign verify-blob, bound to the release workflow's identity for the
#     tag) and lists ravel_server_sha256 and ravel_cli_sha256 for the
#     linux-amd64 binaries;
#   - the machine type is x86_64 (gcloud compute machine-types describe,
#     architecture): only linux-amd64 release artifacts are pinned, so an
#     arm64 type, or an answer it cannot read, is refused;
#   - the service account exists and holds roles/storage.objectAdmin on the
#     results bucket (setup-gcp.sh grants it).
# It then stages the kit (box/run.sh, lib/kit.py, the corpus, the mapping and
# the pre-registration, with a SHA256SUMS sidecar), uploads kit.tgz, kit.sha256 and the resolved
# run.toml to gs://<bucket>/<prefix>/runs/<run-id>/in/, creates the run's
# store bucket (regional Standard, uniform access, no versioning, soft delete
# off) and grants the service account objectAdmin on it, creates the instance
# and grants the service account roles/compute.instanceAdmin.v1 on that one
# instance so the box can delete itself. If any of the last three fails, what
# it created is deleted.
#
# The instance: run.toml's machine type, image family and boot disk size
# (pd-balanced, deleted with the instance), the service account with the
# cloud-platform scope, no restart on host failure, and a max run duration of
# 590 minutes with termination action DELETE. Its startup script arms
# `shutdown -h +600`, refuses to run twice (a reboot fails the run and deletes
# the instance), downloads kit.tgz and run.toml with the metadata server's
# token, checks them against the digests written into the script itself,
# checks SHA256SUMS inside the kit and starts box/run.sh as a transient
# systemd unit. So the box is gone by 600 minutes three ways: it deletes
# itself at the end of the run, success or failure; GCE deletes it at 590
# minutes; and a guest shutdown at 600 stops it if both failed.
#
# It prints the run id, the instance, the store bucket and the out/ prefix,
# and exits without waiting; fetch-gcp.sh collects the result and
# teardown-gcp.sh removes the store bucket.
#
# --dry-run prints every gcloud, curl and cosign command instead of running
# it, prints the startup script between `DRY-RUN user-data A:` and `DRY-RUN
# end user-data A` lines, and keeps the staged in/ files.
#
# Written for bash 3.2 (macOS /bin/bash).
#
# Exit codes:
#   0   the instance was created (or, under --dry-run, every command printed)
#   1   a gcloud call failed; whatever this run created was deleted
#   64  bad usage, or run.toml, the prereg file, the release or the mapping
#       refused; nothing was created
#   65  the service account is missing or lacks access to the results bucket;
#       nothing was created
set -euo pipefail

usage() {
  cat <<'EOF'
usage: benchmarks/clickbench/kit/launch-gcp.sh [--dry-run] RUN_TOML

Launches one GCE box for a cloud = "gcp" ClickBench kit run and exits;
benchmarks/clickbench/kit/fetch-gcp.sh --run-toml RUN_TOML RUN_ID collects the
result. run.gcp.example.toml beside this script lists every key.

  --dry-run   print every gcloud, curl and cosign command instead of running it
  --help      this text
EOF
}

die64() {
  echo "launch-gcp: $*" >&2
  exit 64
}

KIT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$KIT_DIR/../../.." && pwd)"
SHUTDOWN_MINUTES=600
MAX_RUN_SECONDS=35400
RELEASE_WORKFLOW="https://github.com/NOFireAI/ravel/.github/workflows/publish-images.yml"
OIDC_ISSUER="https://token.actions.githubusercontent.com"

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

toml_or() {
  local v
  v="$(kit_py get "$RUN_TOML" "$1" 2>/dev/null)" || v=""
  [ -n "$v" ] || v="$2"
  printf '%s\n' "$v"
}

kit_py check-run "$RUN_TOML" || die64 "run.toml refused"
[ "$(toml_or cloud aws)" = gcp ] || die64 "$RUN_TOML is not a cloud = \"gcp\" run.toml; launch.sh runs the AWS kit"

SHA="$(toml sha_a)"
TAG="$(toml release_tag)"
SERVER_SHA256="$(toml ravel_server_sha256)"
CLI_SHA256="$(toml ravel_cli_sha256)"
RELEASE_URL="$(toml_or release_url https://github.com/NOFireAI/ravel/releases/download)"
PROJECT="$(toml project)"
REGION="$(toml region)"
ZONE="$(toml zone)"
MACHINE="$(toml instance_type)"
IMAGE_FAMILY="$(toml image_family)"
IMAGE_PROJECT="$(toml image_project)"
DISK_GB="$(toml_or boot_disk_gb 100)"
SA="$(toml service_account)"
BUCKET="$(toml bucket)"
PREFIX="$(toml prefix)"
PREFIX="${PREFIX%/}"
STORE_PREFIX="$(toml store_bucket_prefix)"

resolve() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s\n' "$REPO_ROOT/$1" ;;
  esac
}

CORPUS_PATH="$(resolve "$(toml corpus)")"
MAPPING_PATH="$(resolve "$(toml mapping)")"
PREREG_PATH="$(resolve "$(toml prereg)")"
[ -f "$CORPUS_PATH" ] || die64 "corpus not found: $CORPUS_PATH"
[ -f "$MAPPING_PATH" ] || die64 "mapping not found: $MAPPING_PATH"
[ -f "$PREREG_PATH" ] || die64 "prereg not found: $PREREG_PATH"
kit_py check-prereg "$PREREG_PATH" || die64 "prereg file refused"

if command -v sha256sum >/dev/null; then
  sha256() { sha256sum "$@"; }
else
  sha256() { shasum -a 256 "$@"; }
fi
digest_of() { sha256 "$1" | awk '{ print $1 }'; }

# run CMD...: run it, or under --dry-run print it on stderr and succeed.
run() {
  if [ "$DRY_RUN" = 1 ]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; } >&2
    return 0
  fi
  "$@"
}

# The release tag and the mapping, against this checkout.
got="$(git -C "$REPO_ROOT" rev-parse -q --verify "$TAG^{commit}" 2>/dev/null)" \
  || die64 "tag $TAG is not in this checkout (git fetch origin tag $TAG)"
[ "$got" = "$SHA" ] || die64 "tag $TAG is commit $got, run.toml's sha_a is $SHA"
git -C "$REPO_ROOT" show "$SHA:benchmarks/clickbench/hits.mapping.toml" >"${TMPDIR:-/tmp}/launch-gcp-mapping.$$" \
  || die64 "cannot read benchmarks/clickbench/hits.mapping.toml at $TAG"
want="$(digest_of "${TMPDIR:-/tmp}/launch-gcp-mapping.$$")"
rm -f "${TMPDIR:-/tmp}/launch-gcp-mapping.$$"
[ "$want" = "$(digest_of "$MAPPING_PATH")" ] \
  || die64 "$MAPPING_PATH differs from $TAG's benchmarks/clickbench/hits.mapping.toml"

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/clickbench-kit-gcp.XXXXXX")"
INSTANCE_MADE=""
BUCKET_MADE=""
LAUNCH_OK=0

cleanup() {
  local code=$?
  trap - EXIT
  if [ "$LAUNCH_OK" != 1 ]; then
    if [ -n "$INSTANCE_MADE" ]; then
      echo "launch-gcp: deleting instance $INSTANCE_MADE after the failed launch" >&2
      run gcloud compute instances delete "$INSTANCE_MADE" --project "$PROJECT" --zone "$ZONE" --quiet || true
    fi
    if [ -n "$BUCKET_MADE" ]; then
      echo "launch-gcp: deleting bucket gs://$BUCKET_MADE after the failed launch" >&2
      run gcloud storage rm --recursive "gs://$BUCKET_MADE" --project "$PROJECT" --quiet || true
    fi
    [ -z "$INSTANCE_MADE$BUCKET_MADE" ] || [ "$code" -ne 0 ] || code=1
  fi
  case "$code" in
    0|64|65) ;;
    *) code=1 ;;
  esac
  if [ "$DRY_RUN" = 1 ]; then
    echo "launch-gcp: staged in/ files kept in $STAGE" >&2
  else
    rm -rf "$STAGE"
  fi
  exit "$code"
}
trap cleanup EXIT

# The release: SHA256SUMS signed by the release workflow for this tag, and
# listing the two pinned digests.
mkdir -p "$STAGE/release"
for f in SHA256SUMS SHA256SUMS.sig SHA256SUMS.pem; do
  run curl -sSfL --retry 3 --max-time 120 "$RELEASE_URL/$TAG/$f" -o "$STAGE/release/$f" \
    || die64 "cannot download $f of $TAG from $RELEASE_URL"
done
if [ "$DRY_RUN" != 1 ]; then
  command -v cosign >/dev/null || die64 "cosign is required to verify $TAG's SHA256SUMS signature"
  mkdir -p "$STAGE/docker-config"
  # An empty DOCKER_CONFIG: cosign otherwise asks the Docker credential helper,
  # which can hang.
  BOUND=()
  if command -v timeout >/dev/null; then BOUND=(timeout 120); fi
  DOCKER_CONFIG="$STAGE/docker-config" ${BOUND[@]+"${BOUND[@]}"} cosign verify-blob \
    --certificate "$STAGE/release/SHA256SUMS.pem" --signature "$STAGE/release/SHA256SUMS.sig" \
    --certificate-identity "$RELEASE_WORKFLOW@refs/tags/$TAG" \
    --certificate-oidc-issuer "$OIDC_ISSUER" "$STAGE/release/SHA256SUMS" </dev/null \
    || die64 "SHA256SUMS of $TAG does not verify against its signature"
  for pair in "ravel-server-linux-amd64 $SERVER_SHA256" "ravel-cli-linux-amd64 $CLI_SHA256"; do
    name="${pair%% *}"
    listed="$(awk -v f="$name" '$2 == f { print $1 }' "$STAGE/release/SHA256SUMS")"
    [ "$listed" = "${pair#* }" ] || die64 "SHA256SUMS of $TAG lists $name as '$listed', run.toml pins ${pair#* }"
  done
fi

# The machine type's architecture. The box picks its release binaries by
# uname -m but run.toml pins the linux-amd64 digests only.
if [ "$DRY_RUN" = 1 ]; then
  run gcloud compute machine-types describe "$MACHINE" --project "$PROJECT" --zone "$ZONE" --format='value(architecture)'
else
  arch="$(gcloud compute machine-types describe "$MACHINE" --project "$PROJECT" --zone "$ZONE" --format='value(architecture)')" \
    || die64 "cannot look up the architecture of machine type $MACHINE in $ZONE"
  arch="$(printf '%s' "$arch" | tr -d '[:space:]')"
  case "$arch" in
    X86_64) ;;
    ARM64) die64 "machine type $MACHINE is arm64; only linux-amd64 release artifacts are pinned, so arm64 machine types are refused" ;;
    *) die64 "cannot tell the architecture of machine type $MACHINE (gcloud answered '$arch'); only linux-amd64 release artifacts are pinned, so a machine type that is not x86_64 is refused" ;;
  esac
fi

# The service account and its access to the results bucket.
if [ "$DRY_RUN" = 1 ]; then
  run gcloud iam service-accounts describe "$SA" --project "$PROJECT"
  run gcloud storage buckets get-iam-policy "gs://$BUCKET" --project "$PROJECT" --format=json
else
  gcloud iam service-accounts describe "$SA" --project "$PROJECT" --format='value(email)' >/dev/null 2>&1 \
    || { echo "launch-gcp: service account $SA does not exist or cannot be read (run setup-gcp.sh)" >&2; exit 65; }
  gcloud storage buckets get-iam-policy "gs://$BUCKET" --project "$PROJECT" --format=json >"$STAGE/bucket-iam.json" \
    || { echo "launch-gcp: cannot read the IAM policy of gs://$BUCKET (run setup-gcp.sh)" >&2; exit 65; }
  python3 -c 'import json, sys
policy = json.load(open(sys.argv[1]))
member = "serviceAccount:" + sys.argv[2]
sys.exit(0 if any(b.get("role") == "roles/storage.objectAdmin" and member in b.get("members", [])
                  and not b.get("condition") for b in policy.get("bindings", [])) else 1)' \
    "$STAGE/bucket-iam.json" "$SA" \
    || { echo "launch-gcp: $SA lacks roles/storage.objectAdmin on gs://$BUCKET (run setup-gcp.sh)" >&2; exit 65; }
fi

RUN_ID="clickbench-$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
NAMES="$(kit_py gcp-names "$RUN_ID" "$STORE_PREFIX")" || die64 "cannot name the run's resources"
INSTANCE="$(printf '%s\n' "$NAMES" | sed -n 1p)"
STORE_BUCKET="$(printf '%s\n' "$NAMES" | sed -n 2p)"
LABEL="$(printf '%s\n' "$NAMES" | sed -n 3p)"
RUN_URI="gs://$BUCKET/$PREFIX/runs/$RUN_ID"
echo "launch-gcp: run id $RUN_ID" >&2

# Stage the kit: the box scripts, the corpus, the mapping, and the
# pre-registration fetch-gcp.sh judges the run by.
mkdir -p "$STAGE/kit/box" "$STAGE/kit/lib" "$STAGE/in"
cp "$KIT_DIR/box/run.sh" "$STAGE/kit/box/run.sh"
cp "$KIT_DIR/lib/kit.py" "$STAGE/kit/lib/kit.py"
cp "$CORPUS_PATH" "$STAGE/kit/corpus.json"
cp "$MAPPING_PATH" "$STAGE/kit/mapping.toml"
cp "$PREREG_PATH" "$STAGE/kit/prereg.toml"
(cd "$STAGE/kit" && find . -type f | LC_ALL=C sort >"$STAGE/kit-files") \
  || { echo "launch-gcp: cannot list the staged kit files" >&2; exit 1; }
while read -r f; do
  (cd "$STAGE/kit" && sha256 "$f") || { echo "launch-gcp: cannot hash kit file $f" >&2; exit 1; }
done <"$STAGE/kit-files" >"$STAGE/SHA256SUMS"
[ "$(wc -l <"$STAGE/SHA256SUMS")" -eq "$(wc -l <"$STAGE/kit-files")" ] \
  || { echo "launch-gcp: SHA256SUMS does not list every kit file" >&2; exit 1; }
mv "$STAGE/SHA256SUMS" "$STAGE/kit/SHA256SUMS"
COPYFILE_DISABLE=1 tar -czf "$STAGE/in/kit.tgz" -C "$STAGE" kit
kit_py resolved-run "$RUN_TOML" "$STAGE/in/run.toml" \
  || { echo "launch-gcp: cannot write the resolved run.toml" >&2; exit 1; }
(cd "$STAGE/in" && sha256 kit.tgz run.toml >kit.sha256)
KIT_TGZ_SHA="$(sed -n 's/^\([0-9a-f]\{64\}\)  kit\.tgz$/\1/p' "$STAGE/in/kit.sha256")"
RUN_TOML_SHA="$(sed -n 's/^\([0-9a-f]\{64\}\)  run\.toml$/\1/p' "$STAGE/in/kit.sha256")"
[ -n "$KIT_TGZ_SHA" ] && [ -n "$RUN_TOML_SHA" ] \
  || { echo "launch-gcp: cannot read the kit.tgz and run.toml digests" >&2; exit 1; }

for f in kit.tgz kit.sha256 run.toml; do
  run gcloud storage cp --project "$PROJECT" --quiet "$STAGE/in/$f" "$RUN_URI/in/$f"
done

# user_data: the box's startup script. It reaches GCS and Compute with curl
# and the metadata server's token, so it needs no gcloud on the image.
user_data() {
  cat <<EOF
#!/bin/bash
# ClickBench kit run $RUN_ID, arm A, cloud gcp.
KIT=/root/kit
IN=${RUN_URI#gs://}/in
OUT=${RUN_URI#gs://}/out/A
MD=\${KIT_GCE_METADATA:-http://metadata.google.internal/computeMetadata/v1}
GCS=\${KIT_GCS_API:-https://storage.googleapis.com}
GCE=\${KIT_GCE_API:-https://compute.googleapis.com/compute/v1}
mkdir -p "\$KIT/in"
exec >>"\$KIT/user-data.log" 2>&1
md() { curl -sSf --max-time 5 -H 'Metadata-Flavor: Google' "\$MD/\$1"; }
token() { md instance/service-accounts/default/token | sed -n 's/.*"access_token" *: *"\\([^"]*\\)".*/\\1/p'; }
put() {
  local tok key="\${2#*/}"
  tok="\$(token)" && [ -n "\$tok" ] || return 1
  curl -sSf --max-time 120 -o /dev/null -X POST -H "Authorization: Bearer \$tok" \\
    -H 'Content-Type: application/octet-stream' --data-binary @"\$1" \\
    "\$GCS/upload/storage/v1/b/\${2%%/*}/o?uploadType=media&name=\${key//\\//%2F}"
}
get() {
  local tok key="\${1#*/}"
  tok="\$(token)" && [ -n "\$tok" ] || return 1
  curl -sSf --max-time 600 -H "Authorization: Bearer \$tok" -o "\$2" \\
    "\$GCS/storage/v1/b/\${1%%/*}/o/\${key//\\//%2F}?alt=media"
}
delete_self() {
  local tok name zone project
  name="\$(md instance/name)" && zone="\$(md instance/zone)" && project="\$(md project/project-id)" \\
    && tok="\$(token)" && [ -n "\$tok" ] || return 1
  curl -sSf --max-time 30 -o /dev/null -X DELETE -H "Authorization: Bearer \$tok" \\
    "\$GCE/projects/\$project/zones/\${zone##*/}/instances/\$name"
}
fail() {
  printf 'FAILED step=user-data %s\n' "\$1" >"\$KIT/FAILED"
  put "\$KIT/FAILED" "\$OUT/FAILED"
  delete_self
  shutdown -h now
  exit 1
}
[ ! -e "\$KIT/BOOTED" ] || fail "reboot: the startup script ran a second time"
: >"\$KIT/BOOTED"
shutdown -h +$SHUTDOWN_MINUTES
for f in kit.tgz run.toml; do
  get "\$IN/\$f" "\$KIT/in/\$f" || fail "download \$f"
done
printf '%s  %s\n' $KIT_TGZ_SHA kit.tgz $RUN_TOML_SHA run.toml >"\$KIT/in/kit.sha256" || fail "write kit.sha256"
(cd "\$KIT/in" && sha256sum -c kit.sha256) || fail "sha256 check of kit.tgz and run.toml"
tar -xzf "\$KIT/in/kit.tgz" -C "\$KIT" || fail "untar kit.tgz"
(cd "\$KIT/kit" && sha256sum -c SHA256SUMS) || fail "sha256 check of the kit files"
cp "\$KIT/in/run.toml" "\$KIT/run.toml" || fail "copy run.toml"
printf '%s\n' A >"\$KIT/ARM" || fail "write ARM"
printf '%s\n' $RUN_ID >"\$KIT/RUN_ID" || fail "write RUN_ID"
printf '%s\n' gcp >"\$KIT/CLOUD" || fail "write CLOUD"
printf '%s\n' $STORE_BUCKET >"\$KIT/STORE_BUCKET" || fail "write STORE_BUCKET"
systemd-run --unit=ravel-clickbench-kit --collect --property=LimitNOFILE=1048576 /bin/bash -c 'exec /bin/bash /root/kit/kit/box/run.sh >>/root/kit/run.log 2>&1' \\
  || fail "start box/run.sh"
EOF
}

ud="$(user_data)"
printf '%s\n' "$ud" >"$STAGE/startup.sh"
if [ "$DRY_RUN" = 1 ]; then
  { echo "DRY-RUN user-data A:"; printf '%s\n' "$ud"; echo "DRY-RUN end user-data A"; } >&2
fi

BUCKET_MADE="$STORE_BUCKET"
run gcloud storage buckets create "gs://$STORE_BUCKET" --project "$PROJECT" --location "$REGION" \
  --default-storage-class STANDARD --uniform-bucket-level-access --soft-delete-duration 0 \
  --labels "ravel-clickbench-run=$LABEL" \
  || { echo "launch-gcp: cannot create the store bucket gs://$STORE_BUCKET" >&2; exit 1; }
run gcloud storage buckets add-iam-policy-binding "gs://$STORE_BUCKET" --project "$PROJECT" \
  --member "serviceAccount:$SA" --role roles/storage.objectAdmin --format='value(etag)' >/dev/null \
  || { echo "launch-gcp: cannot grant $SA objectAdmin on gs://$STORE_BUCKET" >&2; exit 1; }

INSTANCE_MADE="$INSTANCE"
run gcloud compute instances create "$INSTANCE" --project "$PROJECT" --zone "$ZONE" \
  --machine-type "$MACHINE" --image-family "$IMAGE_FAMILY" --image-project "$IMAGE_PROJECT" \
  --boot-disk-size "${DISK_GB}GB" --boot-disk-type pd-balanced --boot-disk-auto-delete \
  --service-account "$SA" --scopes cloud-platform \
  --maintenance-policy MIGRATE --no-restart-on-failure \
  --max-run-duration "${MAX_RUN_SECONDS}s" --instance-termination-action DELETE \
  --metadata-from-file "startup-script=$STAGE/startup.sh" \
  --labels "ravel-clickbench-run=$LABEL,ravel-clickbench-arm=a,ravel-commit=$SHA" \
  --format='value(name)' >/dev/null \
  || { echo "launch-gcp: cannot create instance $INSTANCE" >&2; exit 1; }
run gcloud compute instances add-iam-policy-binding "$INSTANCE" --project "$PROJECT" --zone "$ZONE" \
  --member "serviceAccount:$SA" --role roles/compute.instanceAdmin.v1 --format='value(etag)' >/dev/null \
  || { echo "launch-gcp: cannot let $SA delete $INSTANCE" >&2; exit 1; }
LAUNCH_OK=1

[ "$DRY_RUN" != 1 ] || echo "launch-gcp: dry run, nothing was created; these are the names a real launch would use"
printf 'run id:        %s\n' "$RUN_ID"
printf 'instance:      %s (%s, %s, %s %s)\n' "$INSTANCE" "$ZONE" "$MACHINE" "$TAG" "$SHA"
printf 'store bucket:  gs://%s\n' "$STORE_BUCKET"
printf 'out prefix:    %s/out/A/\n' "$RUN_URI"
printf 'collect:       %s --run-toml %s %s\n' "$KIT_DIR/fetch-gcp.sh" "$RUN_TOML" "$RUN_ID"
printf 'teardown:      %s %s %s\n' "$KIT_DIR/teardown-gcp.sh" "$RUN_TOML" "$RUN_ID"
