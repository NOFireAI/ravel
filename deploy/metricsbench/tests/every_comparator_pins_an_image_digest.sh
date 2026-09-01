#!/bin/sh
# every_comparator_pins_an_image_digest.sh (issue #934, ADR-0927).
#
# Acceptance check for the MetricsBench comparator deployment. Issue #934 names
# this check `metricsbench::deploy::tests::every_comparator_pins_an_image_digest`,
# a Rust test path, but its own scope line says the task touches no crate, and
# the crate that path would live in (crates/ravel-bench) is edited by the
# parallel M1 task. This is that check implemented instead as a dependency-free
# script under deploy/metricsbench/, preserving the name exactly. The deviation
# is recorded in the task report and in README.md.
#
# It enforces, over the deployment files:
#   1. every image reference is pinned by an `@sha256:` digest (a tag alone, or a
#      tag with no digest, fails: a moving tag makes a run unreproducible);
#   2. every comparator ADR-0927 requires is present (deleting a system cannot
#      make the check pass);
#   3. the number of image references equals the exact expected count (a check
#      that scanned zero files and exited zero is the failure this guards
#      against).
# Exit 0 only when all three hold.
#
# POSIX sh, no external dependencies beyond grep/sed. Fails closed.

set -u

# Resolve the deployment directory from this script's own location so the check
# runs correctly from any working directory.
# `CDPATH= cd` clears CDPATH for that one command, so a user's CDPATH cannot
# make `cd` resolve somewhere else and print the directory it chose. The empty
# assignment is the point, not a typo; SC1007 cannot tell the two apart.
# shellcheck disable=SC1007
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1007
DEPLOY_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
COMPOSE_FILE="$DEPLOY_DIR/docker-compose.yml"

# The comparators ADR-0927 requires in the portable cross-engine lane. Each must
# appear as a service in the compose file. Prometheus and VictoriaMetrics are
# named directly; "mimir" is the object-storage-native PromQL system (ADR-0927
# permits Mimir or Thanos Receive) chosen for this deployment.
REQUIRED_COMPARATORS="prometheus victoriametrics mimir"

# Exact number of `image:` references the committed deployment must contain:
# prometheus, victoriametrics, minio, createbuckets, mimir. If a service is added
# or removed, update this number deliberately in the same change.
EXPECTED_IMAGE_COUNT=5

fail=0

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "FAIL: compose file not found at $COMPOSE_FILE"
  exit 1
fi

echo "MetricsBench image-pin check"
echo "  deployment file: $COMPOSE_FILE"

# Collect image references. Match indented `image:` keys only, strip the key and
# surrounding whitespace to leave the bare reference.
IMAGE_REFS=$(grep -E '^[[:space:]]*image:[[:space:]]*' "$COMPOSE_FILE" \
  | sed -E 's/^[[:space:]]*image:[[:space:]]*//; s/[[:space:]]*$//')

if [ -z "$IMAGE_REFS" ]; then
  echo "FAIL: no image references found; the check must never scan zero images"
  exit 1
fi

# Count references. `grep -c` counts matching lines, which is one per image key.
image_count=$(printf '%s\n' "$IMAGE_REFS" | grep -c .)

echo "  image references found: $image_count (expected $EXPECTED_IMAGE_COUNT)"
echo "  references:"
# A pinned reference ends in `@sha256:` followed by exactly 64 hex digits.
# Matching the bare substring `@sha256:` is not enough: `repo:tag@sha256:` with
# an empty or truncated digest would satisfy it while pinning nothing, which is
# the failure this check exists to catch.
DIGEST_RE='@sha256:[0-9a-f]\{64\}$'

# Check every reference carries a complete digest.
printf '%s\n' "$IMAGE_REFS" | while IFS= read -r ref; do
  if printf '%s\n' "$ref" | grep -q "$DIGEST_RE"; then
    echo "    [pinned]   $ref"
  else
    echo "    [UNPINNED] $ref"
  fi
done

# The digest check drives the exit code from the parent shell (the while loop
# above runs in a subshell and cannot set `fail`). Recount unpinned refs here.
unpinned=$(printf '%s\n' "$IMAGE_REFS" | grep -vc "$DIGEST_RE")
if [ "$unpinned" -ne 0 ]; then
  echo "FAIL: $unpinned image reference(s) lack an @sha256: digest"
  fail=1
fi

if [ "$image_count" -ne "$EXPECTED_IMAGE_COUNT" ]; then
  echo "FAIL: found $image_count image references, expected exactly $EXPECTED_IMAGE_COUNT"
  fail=1
fi

# Every ADR-0927-required comparator must be present as a service. A service key
# is a two-space-indented `name:` under `services:`. Matching that indentation
# avoids a false positive from a bucket name or a comment mentioning the word.
for svc in $REQUIRED_COMPARATORS; do
  if grep -Eq "^  ${svc}:[[:space:]]*\$" "$COMPOSE_FILE"; then
    echo "  comparator present: $svc"
  else
    echo "FAIL: required comparator '$svc' is missing from the deployment set"
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "RESULT: FAIL"
  exit 1
fi

echo "RESULT: PASS ($image_count image references, all pinned; comparators: $REQUIRED_COMPARATORS)"
exit 0
