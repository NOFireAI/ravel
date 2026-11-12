#!/usr/bin/env bash
# One-time GCP setup for ClickBench kit runs on GCE (launch-gcp.sh). Safe to
# run again: each step checks what exists before it creates anything.
#
# From the run.toml it reads project, region, service_account and bucket (the
# long-lived bucket that holds every run's in/ and out/ files), and it:
#
#   1. creates the service account when it does not exist; it never creates
#      a key for it (the boxes use it through the metadata server);
#   2. creates the results bucket when it does not exist: regional, Standard,
#      uniform bucket-level access, no versioning, soft delete off;
#   3. grants the service account roles/storage.objectAdmin on that bucket,
#      and nothing at the project level.
#
# launch-gcp.sh grants the same account objectAdmin on each run's own store
# bucket and roles/compute.instanceAdmin.v1 on the run's one instance, so the
# box can delete itself; both bindings go away with the bucket and instance.
#
# --dry-run prints every gcloud command that would change something instead
# of running it; the checks still run.
#
# Exit codes: 0 done (or every change printed), 1 a gcloud call failed,
# 64 bad usage.
set -euo pipefail

usage() {
  echo "usage: benchmarks/clickbench/kit/setup-gcp.sh [--dry-run] RUN_TOML" >&2
}

KIT_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=0
RUN_TOML=""
while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --dry-run) DRY_RUN=1 ;;
    -*) usage; echo "setup-gcp: unknown argument: $1" >&2; exit 64 ;;
    *) [ -z "$RUN_TOML" ] || { usage; exit 64; }; RUN_TOML="$1" ;;
  esac
  shift
done
[ -n "$RUN_TOML" ] && [ -f "$RUN_TOML" ] || { usage; exit 64; }

get() { python3 "$KIT_DIR/lib/kit.py" get "$RUN_TOML" "$1"; }
PROJECT="$(get project)" || exit 64
REGION="$(get region)" || exit 64
SA="$(get service_account)" || exit 64
BUCKET="$(get bucket)" || exit 64
[[ "$SA" =~ ^([a-z][a-z0-9-]{4,28}[a-z0-9])@([a-z0-9-]+)\.iam\.gserviceaccount\.com$ ]] \
  || { echo "setup-gcp: service_account '$SA' is not a service account email" >&2; exit 64; }
SA_ID="${BASH_REMATCH[1]}"
[ "${BASH_REMATCH[2]}" = "$PROJECT" ] \
  || { echo "setup-gcp: service_account '$SA' is not in project $PROJECT" >&2; exit 64; }

change() {
  if [ "$DRY_RUN" = 1 ]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; } >&2
    return 0
  fi
  "$@"
}

if gcloud iam service-accounts describe "$SA" --project "$PROJECT" --format='value(email)' >/dev/null 2>&1; then
  echo "setup-gcp: service account $SA exists"
else
  change gcloud iam service-accounts create "$SA_ID" --project "$PROJECT" \
    --display-name "Ravel ClickBench kit boxes"
fi

if gcloud storage buckets describe "gs://$BUCKET" --project "$PROJECT" --format='value(name)' >/dev/null 2>&1; then
  echo "setup-gcp: bucket gs://$BUCKET exists"
else
  change gcloud storage buckets create "gs://$BUCKET" --project "$PROJECT" --location "$REGION" \
    --default-storage-class STANDARD --uniform-bucket-level-access --soft-delete-duration 0
fi

change gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --project "$PROJECT" \
  --member "serviceAccount:$SA" --role roles/storage.objectAdmin --format='value(etag)' >/dev/null
echo "setup-gcp: $SA has roles/storage.objectAdmin on gs://$BUCKET"
