#!/bin/sh
# every_comparator_pins_an_image_digest.sh (issue #934, ADR-0927; generalised
# for issue #1310).
#
# Started as the MetricsBench comparator deployment's acceptance check. Issue
# #934 names this check
# `metricsbench::deploy::tests::every_comparator_pins_an_image_digest`, a Rust
# test path, but its own scope line says the task touches no crate, and the
# crate that path would live in (crates/ravel-bench) was edited by a parallel
# task. This is that check implemented instead as a dependency-free script
# under deploy/metricsbench/, preserving the name exactly. The deviation is
# recorded in README.md.
#
# Issue #1310 generalised it into a repo-wide pin check, on the same rationale:
# a moving tag or a mutable action ref is unreproducible and unauditable
# wherever it appears, not just in this one compose file. It now scans three
# categories, each with its own exact-count assertion so a scan that finds
# nothing in a category fails rather than passing silently:
#
#   1. every `image:` reference in deploy/metricsbench/docker-compose.yml
#      (the original check, unchanged in behaviour);
#   2. every base image in Dockerfile and Dockerfile.prebuilt (`FROM ...` and
#      the `ARG RUNTIME_BASE=...` default) -- excluding `scratch` (a
#      zero-content pseudo-image with no registry manifest to pin) and a bare
#      `${VAR}` FROM (Dockerfile.prebuilt's runtime stages source the build
#      arg, whose own default is what carries the pin, checked separately);
#   3. every `uses:` action reference in .github/workflows/*.yml and
#      .github/actions/*/*.yml -- excluding a local action (`uses: ./...`),
#      which is repo-tracked code, not a fetched external action.
#
# Every category requires an `@sha256:<64 hex>` digest (categories 1 and 2) or
# a full 40-character commit SHA (category 3): a tag alone, a branch, or a
# short SHA is a moving or ambiguous reference and fails the same as a bare
# tag. Exit 0 only when every category is fully pinned and every category's
# reference count equals its expected total.
#
# POSIX sh, no external dependencies beyond grep/sed. Fails closed.

set -u

# Resolve directories from this script's own location so the check runs
# correctly from any working directory.
# `CDPATH= cd` clears CDPATH for that one command, so a user's CDPATH cannot
# make `cd` resolve somewhere else and print the directory it chose. The empty
# assignment is the point, not a typo; SC1007 cannot tell the two apart.
# shellcheck disable=SC1007
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1007
DEPLOY_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
# shellcheck disable=SC1007
REPO_ROOT=$(CDPATH= cd -- "$DEPLOY_DIR/../.." && pwd)
COMPOSE_FILE="$DEPLOY_DIR/docker-compose.yml"

# The comparators ADR-0927 requires in the portable cross-engine lane. Each must
# appear as a service in the compose file. Prometheus and VictoriaMetrics are
# named directly; "mimir" is the object-storage-native PromQL system (ADR-0927
# permits Mimir or Thanos Receive) chosen for this deployment.
REQUIRED_COMPARATORS="prometheus victoriametrics mimir"

# Exact number of `image:` references the committed deployment must contain:
# prometheus, victoriametrics, minio, createbuckets, mimir. If a service is added
# or removed, update this number deliberately in the same change.
COMPOSE_EXPECTED_IMAGE_COUNT=5

# Exact number of base-image references across both Dockerfiles that must
# carry a digest: Dockerfile's four `FROM` stages that pull a real image
# (builder, server, operator, ingest-router -- `debug-symbols`'s `FROM scratch`
# is excluded, see above) plus Dockerfile.prebuilt's one `ARG RUNTIME_BASE=`
# default. Update deliberately if a stage is added, removed, or repointed at a
# build-arg.
DOCKERFILE_EXPECTED_IMAGE_COUNT=5

# Exact number of external `uses:` action references across every workflow and
# composite action file. Update deliberately whenever a workflow gains,
# loses, or repoints a `uses:` step.
WORKFLOW_EXPECTED_ACTION_COUNT=92

# A pinned image reference ends in `@sha256:` followed by exactly 64 hex
# digits. Matching the bare substring `@sha256:` is not enough: `repo:tag@sha256:`
# with an empty or truncated digest would satisfy it while pinning nothing,
# which is the failure this check exists to catch.
IMAGE_DIGEST_RE='@sha256:[0-9a-f]\{64\}$'

# A pinned action reference ends in `@` followed by exactly 40 hex digits (a
# full git commit SHA). A tag (`@v4`), a branch, or an abbreviated SHA all fail
# this, which is the point: only a full commit SHA is an immutable pin.
ACTION_SHA_RE='@[0-9a-f]\{40\}$'

fail=0

DOCKERFILE_REFS_FILE=$(mktemp)
WORKFLOW_REFS_FILE=$(mktemp)
trap 'rm -f "$DOCKERFILE_REFS_FILE" "$WORKFLOW_REFS_FILE"' EXIT

echo "Repo-wide pin check (issue #1310)"

# --- 1. MetricsBench compose file -------------------------------------------

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "FAIL: compose file not found at $COMPOSE_FILE"
  exit 1
fi

echo
echo "== docker-compose image pins =="
echo "  deployment file: $COMPOSE_FILE"

# Collect image references. Match indented `image:` keys only, strip the key and
# surrounding whitespace to leave the bare reference.
IMAGE_REFS=$(grep -E '^[[:space:]]*image:[[:space:]]*' "$COMPOSE_FILE" \
  | sed -E 's/^[[:space:]]*image:[[:space:]]*//; s/[[:space:]]*$//')

if [ -z "$IMAGE_REFS" ]; then
  echo "FAIL: no image references found; the check must never scan zero images"
  fail=1
else
  image_count=$(printf '%s\n' "$IMAGE_REFS" | grep -c .)
  echo "  image references found: $image_count (expected $COMPOSE_EXPECTED_IMAGE_COUNT)"
  echo "  references:"
  printf '%s\n' "$IMAGE_REFS" | while IFS= read -r ref; do
    if printf '%s\n' "$ref" | grep -q "$IMAGE_DIGEST_RE"; then
      echo "    [pinned]   $ref"
    else
      echo "    [UNPINNED] $ref"
    fi
  done

  unpinned=$(printf '%s\n' "$IMAGE_REFS" | grep -vc "$IMAGE_DIGEST_RE")
  if [ "$unpinned" -ne 0 ]; then
    echo "FAIL: $unpinned compose image reference(s) lack an @sha256: digest"
    fail=1
  fi

  if [ "$image_count" -ne "$COMPOSE_EXPECTED_IMAGE_COUNT" ]; then
    echo "FAIL: found $image_count compose image references, expected exactly $COMPOSE_EXPECTED_IMAGE_COUNT"
    fail=1
  fi
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

# --- 2. Dockerfile base images -----------------------------------------------

echo
echo "== Dockerfile base-image pins =="

for df in "$REPO_ROOT/Dockerfile" "$REPO_ROOT/Dockerfile.prebuilt"; do
  if [ ! -f "$df" ]; then
    echo "FAIL: Dockerfile not found at $df"
    fail=1
    continue
  fi
  echo "  scanning: $df"

  # `FROM <image> [AS <stage>]`. Field 2 is the image token. Skip `scratch`
  # (no registry manifest exists to pin) and a bare `${VAR}` reference (its
  # value's own default, not the FROM line, is what carries the pin -- see the
  # ARG scan below).
  grep -n '^FROM[[:space:]]' "$df" | while IFS= read -r line; do
    lineno=$(printf '%s\n' "$line" | cut -d: -f1)
    content=$(printf '%s\n' "$line" | cut -d: -f2-)
    image=$(printf '%s\n' "$content" | awk '{print $2}')
    case "$image" in
      scratch) continue ;;
      '$'*) continue ;;
    esac
    echo "$df:$lineno:$image" >>"$DOCKERFILE_REFS_FILE"
  done

  # `ARG NAME=<value>` whose value looks like an image reference (contains a
  # `/`, e.g. a registry/repo path). Dockerfile.prebuilt's `ARG RUNTIME_BASE=`
  # default is the only line in either file this currently matches.
  grep -n '^ARG[[:space:]]\+[A-Za-z_][A-Za-z0-9_]*=' "$df" | while IFS= read -r line; do
    lineno=$(printf '%s\n' "$line" | cut -d: -f1)
    content=$(printf '%s\n' "$line" | cut -d: -f2-)
    value=$(printf '%s\n' "$content" | sed -E 's/^ARG[[:space:]]+[A-Za-z_][A-Za-z0-9_]*=//')
    case "$value" in
      */*) echo "$df:$lineno:$value" >>"$DOCKERFILE_REFS_FILE" ;;
    esac
  done
done

dockerfile_count=$(wc -l <"$DOCKERFILE_REFS_FILE" | tr -d '[:space:]')
echo "  base-image references found: $dockerfile_count (expected $DOCKERFILE_EXPECTED_IMAGE_COUNT)"

if [ "$dockerfile_count" -eq 0 ]; then
  echo "FAIL: no Dockerfile base-image references found; the check must never scan zero images"
  fail=1
else
  while IFS=: read -r file lineno image; do
    if printf '%s\n' "$image" | grep -q "$IMAGE_DIGEST_RE"; then
      echo "    [pinned]   $file:$lineno: $image"
    else
      echo "    [UNPINNED] $file:$lineno: $image"
    fi
  done <"$DOCKERFILE_REFS_FILE"

  unpinned=$(grep -vc "$IMAGE_DIGEST_RE" "$DOCKERFILE_REFS_FILE")
  if [ "$unpinned" -ne 0 ]; then
    echo "FAIL: $unpinned Dockerfile base-image reference(s) lack an @sha256: digest"
    fail=1
  fi

  if [ "$dockerfile_count" -ne "$DOCKERFILE_EXPECTED_IMAGE_COUNT" ]; then
    echo "FAIL: found $dockerfile_count Dockerfile base-image references, expected exactly $DOCKERFILE_EXPECTED_IMAGE_COUNT"
    fail=1
  fi
fi

# --- 3. Workflow and composite-action `uses:` pins ---------------------------

echo
echo "== GitHub Actions 'uses:' pins =="

# shellcheck disable=SC2044
for wf in $(find "$REPO_ROOT/.github/workflows" -maxdepth 1 -name '*.yml' -type f | sort) \
          $(find "$REPO_ROOT/.github/actions" -mindepth 2 -maxdepth 2 -name '*.yml' -type f 2>/dev/null | sort); do
  # A step's `uses:` key appears either on its own line (following a separate
  # `- name:` line) or inline after the list-item dash (`- uses: ...`, for a
  # step with no `name:`). Both forms must match, or every no-name step (most
  # of the `- uses: ./.github/actions/...` and bare `- uses: actions/...`
  # steps in this repo) is silently skipped.
  grep -nE '^[[:space:]]*-?[[:space:]]*uses:[[:space:]]*' "$wf" | while IFS= read -r line; do
    lineno=$(printf '%s\n' "$line" | cut -d: -f1)
    ref=$(printf '%s\n' "$line" | sed -E 's/^[0-9]+:[[:space:]]*-?[[:space:]]*uses:[[:space:]]*//; s/[[:space:]]*$//')
    case "$ref" in
      ./*) continue ;; # local action: repo-tracked code, not a fetched external action
    esac
    echo "$wf:$lineno:$ref" >>"$WORKFLOW_REFS_FILE"
  done
done

workflow_count=$(wc -l <"$WORKFLOW_REFS_FILE" | tr -d '[:space:]')
echo "  external action references found: $workflow_count (expected $WORKFLOW_EXPECTED_ACTION_COUNT)"

if [ "$workflow_count" -eq 0 ]; then
  echo "FAIL: no workflow action references found; the check must never scan zero actions"
  fail=1
else
  # `read <file` (not a pipe) keeps this loop in the current shell, so
  # `unpinned` accumulates correctly -- unlike the compose section's loop
  # above, which pipes into `while` and therefore runs in a subshell.
  #
  # The stored ref is the whole trailing content of the `uses:` line,
  # including a `# vX.Y.Z` comment where one is present (kept for display,
  # matching how this pin reads elsewhere in the repo). The pin itself is
  # only the first whitespace-delimited token, so the comment must be
  # stripped before the anchored SHA regex is applied, or every commented
  # reference would spuriously fail the `$`-anchored check.
  unpinned=0
  while IFS=: read -r file lineno ref; do
    pin=$(printf '%s\n' "$ref" | awk '{print $1}')
    if printf '%s\n' "$pin" | grep -q "$ACTION_SHA_RE"; then
      echo "    [pinned]   $file:$lineno: $ref"
    else
      echo "    [UNPINNED] $file:$lineno: $ref"
      unpinned=$((unpinned + 1))
    fi
  done <"$WORKFLOW_REFS_FILE"

  if [ "$unpinned" -ne 0 ]; then
    echo "FAIL: $unpinned workflow action reference(s) lack a full 40-character commit SHA"
    fail=1
  fi

  if [ "$workflow_count" -ne "$WORKFLOW_EXPECTED_ACTION_COUNT" ]; then
    echo "FAIL: found $workflow_count workflow action references, expected exactly $WORKFLOW_EXPECTED_ACTION_COUNT"
    fail=1
  fi
fi

# --- Result -------------------------------------------------------------

echo
if [ "$fail" -ne 0 ]; then
  echo "RESULT: FAIL"
  exit 1
fi

echo "RESULT: PASS ($image_count compose images, $dockerfile_count Dockerfile base images, $workflow_count workflow actions, all pinned; comparators: $REQUIRED_COMPARATORS)"
exit 0
