#!/usr/bin/env bash
# Remove what one GCP ClickBench kit run (launch-gcp.sh) left behind: every
# instance carrying the run's label, in any status (a box stopped by its
# guest shutdown still bills its disk), and the run's store bucket with every
# object in it. It keeps the long-lived results bucket and service account
# (setup-gcp.sh), so fetch-gcp.sh can still read the run's out/ files.
#
# It then lists, and prints, the instances carrying the label and asks for
# the store bucket again, and exits non-zero unless both are gone.
#
# Exit codes: 0 nothing of the run remains, 1 something remains or a gcloud
# call failed, 64 bad usage.
set -uo pipefail

usage() {
  echo "usage: benchmarks/clickbench/kit/teardown-gcp.sh RUN_TOML RUN_ID" >&2
}

KIT_DIR="$(cd "$(dirname "$0")" && pwd)"
[ $# -eq 2 ] || { usage; exit 64; }
RUN_TOML="$1"
RUN_ID="$2"
[ -f "$RUN_TOML" ] || { usage; exit 64; }

kit_py() {
  python3 "$KIT_DIR/lib/kit.py" "$@"
}

PROJECT="$(kit_py get "$RUN_TOML" project)" || exit 64
STORE_PREFIX="$(kit_py get "$RUN_TOML" store_bucket_prefix)" || exit 64
NAMES="$(kit_py gcp-names "$RUN_ID" "$STORE_PREFIX")" || exit 64
STORE_BUCKET="$(printf '%s\n' "$NAMES" | sed -n 2p)"
LABEL="$(printf '%s\n' "$NAMES" | sed -n 3p)"
FILTER="labels.ravel-clickbench-run=$LABEL"

code=0
listing="$(gcloud compute instances list --project "$PROJECT" --filter "$FILTER" \
  --format='value(name,zone.basename())')" || { echo "teardown-gcp: cannot list instances" >&2; exit 1; }
while read -r name zone; do
  [ -n "$name" ] || continue
  echo "teardown-gcp: deleting instance $name ($zone)"
  gcloud compute instances delete "$name" --project "$PROJECT" --zone "$zone" --quiet || code=1
done <<<"$listing"

ERR="$(mktemp "${TMPDIR:-/tmp}/teardown-gcp.XXXXXX")"
trap 'rm -f "$ERR"' EXIT
if gcloud storage buckets describe "gs://$STORE_BUCKET" --project "$PROJECT" --format='value(name)' >/dev/null 2>&1; then
  echo "teardown-gcp: deleting gs://$STORE_BUCKET and every object in it"
  # gcloud names every object it removes on stderr; show that only on failure.
  gcloud storage rm --recursive "gs://$STORE_BUCKET" --project "$PROJECT" --quiet >/dev/null 2>"$ERR" \
    || { tail -n 20 "$ERR" >&2; code=1; }
fi

# gcloud warns on stderr when no instance carries the label at all; only
# stdout is the listing.
echo "teardown-gcp: gcloud compute instances list --project $PROJECT --filter $FILTER"
remaining="$(gcloud compute instances list --project "$PROJECT" --filter "$FILTER" \
  --format='table(name,zone.basename(),status)' 2>"$ERR")" || { cat "$ERR" >&2; exit 1; }
printf '%s\n' "${remaining:-Listed 0 items.}"
[ -z "$remaining" ] || { echo "teardown-gcp: instances of $RUN_ID remain" >&2; code=1; }
if gcloud storage buckets describe "gs://$STORE_BUCKET" --project "$PROJECT" --format='value(name)' >/dev/null 2>&1; then
  echo "teardown-gcp: gs://$STORE_BUCKET still exists" >&2
  code=1
else
  echo "teardown-gcp: gs://$STORE_BUCKET does not exist"
fi
exit "$code"
