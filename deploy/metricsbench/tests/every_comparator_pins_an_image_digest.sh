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
# wherever it appears, not just in this one compose file. Issue #1720 added a
# fourth category for the same reason, scoped to the quickstart compose file.
# Issue #1338 added a fifth, scoped to the image argument of `docker run`,
# `docker pull`, and `docker create` inside workflow `run:` blocks. Issue
# #1720's residual round added a sixth, scoped to the `image:` references in
# deploy/k8s manifests that category 4's own scope note originally deferred.
# It now scans six categories, each with its own exact-count assertion so a
# scan that finds nothing in a category fails rather than passing silently:
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
#      which is repo-tracked code, not a fetched external action;
#   4. every `image:` reference in the two quickstart compose files,
#      deploy/docker-compose/ravel.yml and deploy/docker-compose/rustfs.yml
#      (ravel.yml documents rustfs.yml as its RustFS-and-bucket mirror, so both
#      must be scanned or the mirror can drift unpinned with nothing to
#      notice) -- excluding the two `${RAVEL_IMAGE:-...}` references by exact
#      match, since their default is Ravel's own released image (pinned by
#      release tag, ADR-0081), not a third-party image this check governs.
#      Scoped to these two files at the time: deploy/k8s's own registry
#      images were pinned separately by category 6 below (issue #1720's
#      residual round).
#   5. every image argument of a `docker run`, `docker pull`, or
#      `docker create` invocation inside a `run:` block, across every
#      workflow under .github/workflows and every composite action under
#      .github/actions -- the same scope category 3 scans. Docker's
#      management-command spellings (`docker image pull`, `docker container
#      run`, `docker container create`) are matched too, as are global flags
#      between `docker` and its subcommand. A backslash line continuation is
#      joined before matching, since the image commonly sits on a line after
#      the `docker run` token (ci.yml's RustFS and floci starts), and a line
#      inside a here-doc body (publish-images.yml's release-notes template,
#      which contains a literal `docker pull ...` example for humans, not an
#      invocation this job runs) is skipped. Every invocation on a logical
#      line is scanned, not only the first. `"$RAVEL_SERVER_IMAGE"`,
#      `"$RAVEL_OPERATOR_IMAGE"` and `"$ref"` are excluded by exact match:
#      each is a shell variable holding an image this same workflow just
#      built or resolved (a locally assembled tag being smoke-tested, or
#      `$image@$digest` from a platform loop that already pins by digest one
#      line above), not a third-party image reference this scan can check
#      statically.
#   6. every `image:` reference across every manifest under deploy/k8s (the
#      file list is built with `find`, the same reason category 5's list is,
#      so a pattern matching nothing cannot silently shrink the scan) --
#      excluding the two kind-loaded local tags by exact match:
#      `ravel-server:latest` (deploy/k8s/examples/ravelcluster-dev.yaml) and
#      `ravel-operator:latest` (deploy/k8s/operator/operator.yaml). Neither
#      is pulled from a registry: both are built locally and loaded into the
#      kind cluster with `kind load docker-image`, so there is no registry
#      manifest to pin against.
#
# Every category requires an `@sha256:<64 hex>` digest (categories 1, 2, 4,
# 5, and 6) or a full 40-character commit SHA (category 3): a tag alone, a
# branch, or a short SHA is a moving or ambiguous reference and fails the
# same as a bare tag. Exit 0 only when every category is fully pinned and
# every category's reference count equals its expected total.
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
RAVEL_COMPOSE_FILE="$REPO_ROOT/deploy/docker-compose/ravel.yml"
RUSTFS_COMPOSE_FILE="$REPO_ROOT/deploy/docker-compose/rustfs.yml"
K8S_DIR="$REPO_ROOT/deploy/k8s"

# The comparators ADR-0927 requires in the portable cross-engine lane. Each must
# appear as a service in the compose file. Prometheus and VictoriaMetrics are
# named directly; "mimir" is the object-storage-native PromQL system (ADR-0927
# permits Mimir or Thanos Receive) chosen for this deployment.
REQUIRED_COMPARATORS="prometheus victoriametrics mimir"

# Exact number of `image:` references the committed deployment must contain:
# prometheus, victoriametrics, rustfs, createbuckets, mimir. If a service is added
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
# loses, or repoints a `uses:` step. Last raised 92->94 when the ci `features`
# job was split into `features` and `features-otap` (issue #1590): the new job
# adds a checkout and a rust-cache `uses:` (its other two steps, free-disk-space
# and `rustup show`, are a local `./...` action and a plain `run:` step; neither
# is an external `uses:` this scan counts, and the job has no setup-sccache step
# at all). Raised 97->98 when the bench-compare workflow was added (issue
# #533): it carries exactly one external `uses:`, its checkout. Raised 98->101
# when the k8s-nightly chaos job was added (issue #534): it adds a checkout, an
# sccache-action, and a rust-cache. That job's free-disk-space step is a local
# `./.github/actions/...` action, which this scan does not count. Raised
# 101->104 when the dr-rehearsal workflow was added (issue #814): it adds a
# checkout, a rust-cache, and an upload-artifact. Its free-disk-space step is
# a local `./.github/actions/...` action, which this scan does not count, and
# its four matrix cases are one job definition, so the three `uses:` are
# counted once each rather than per case. Raised 104->107 when the
# interop-nightly workflow was added (issue #1716): its `interop` job adds a
# checkout, a nextest install (taiki-e/install-action), and a rust-cache; its
# free-disk-space step is the same local `./.github/actions/...` action this
# scan does not count, and its `report` job uses no action at all.
WORKFLOW_EXPECTED_ACTION_COUNT=107

# Exact number of `image:` lines across the two quickstart compose files:
# ravel.yml's six (rustfs, createbucket (aws-cli), qualify, ravel-server,
# otel-collector, grafana) plus rustfs.yml's two (rustfs, createbucket
# (aws-cli), the same mirror pair under different service wiring). Update
# deliberately
# if a service is added or removed from either file.
QUICKSTART_EXPECTED_IMAGE_COUNT=8

# Of those eight, the number that must carry a digest pin: every image except
# the two `${RAVEL_IMAGE:-...}` references excluded below (both in
# ravel.yml; rustfs.yml carries none). Update deliberately alongside
# QUICKSTART_EXPECTED_IMAGE_COUNT.
QUICKSTART_EXPECTED_PINNED_COUNT=6

# The exact text of a stripped `image:` reference (key and surrounding
# whitespace removed, same as IMAGE_REFS above) that is Ravel's own image
# rather than a third-party one: excluded from category 4 by exact string
# match, not by pattern, so a typo'd variable reference does not silently
# slip through as "exempt".
RAVEL_IMAGE_VAR_REF='${RAVEL_IMAGE:-ghcr.io/nofireai/ravel-server:0.16.1}'

# Category 5 scans every workflow under .github/workflows, the same glob
# category 3 uses for action refs. A fixed file list would leave a workflow
# added tomorrow scanned by nothing, with no count assertion to notice.

# Exact number of `docker run`/`docker pull`/`docker create` image arguments
# across every scanned workflow. Update deliberately if a `docker run`,
# `docker pull`, or `docker create` invocation is added, removed, or
# repointed at a different image inside one of their `run:` blocks. Raised
# 17->18 when the dr-rehearsal workflow was added (issue #814): it starts one
# object-store container in a `run:` block. Its bucket-management invocations
# live in scripts/dr/lib.sh, outside this scan's scope, and are pinned by
# DR_AWS_CLI_IMAGE's own default there.
RUN_IMAGE_EXPECTED_COUNT=18

# Of those, the number that must carry a digest pin: every reference except
# the three shell-variable exemptions below. Update deliberately alongside
# RUN_IMAGE_EXPECTED_COUNT. Raised 11->12 with the dr-rehearsal object-store run,
# which is digest pinned.
RUN_IMAGE_EXPECTED_PINNED_COUNT=12

# The exact text of an extracted image argument (same stripping as the
# extraction below: the whitespace-delimited token itself, quotes included
# where the shell command quoted it) that is a shell variable rather than a
# literal third-party image reference, excluded from category 5 by exact
# string match, not by pattern, so a typo'd variable reference does not
# silently slip through as "exempt" -- same rationale as RAVEL_IMAGE_VAR_REF
# above.
#
#   "$RAVEL_SERVER_IMAGE" (ci.yml:1597, k8s-nightly.yml:85) and
#   "$RAVEL_OPERATOR_IMAGE" (ci.yml:1598, k8s-nightly.yml:86): both jobs
#   `docker build` these images from source earlier in the same job and
#   `docker run --help` them as a smoke test before the cluster is ever
#   involved; the variable holds the just-built local tag, not a pulled
#   third-party image.
#
#   "$ref" (publish-images.yml:744-745): set one line above each use to
#   `$image@$digest` from a loop over the platform digests a prior step in
#   the same job already resolved from the published manifest list -- it is
#   already pinned by digest, just not as a literal `@sha256:` token this
#   scan's static extraction can see.
RUN_IMAGE_VAR_REF_SERVER='"$RAVEL_SERVER_IMAGE"'
RUN_IMAGE_VAR_REF_OPERATOR='"$RAVEL_OPERATOR_IMAGE"'
RUN_IMAGE_VAR_REF_RESOLVED='"$ref"'

# Category 6 scans every manifest under deploy/k8s. A fixed file list would
# leave a manifest added tomorrow scanned by nothing, with no count
# assertion to notice -- same rationale as category 5's `find` call.

# Exact number of `image:` lines across every deploy/k8s manifest: rustfs.yaml
# (rustfs, aws-cli), floci.yaml (floci, curlimages/curl), and the two kind-loaded
# example/operator manifests below. Update deliberately if a manifest gains,
# loses, or repoints an `image:` line.
K8S_EXPECTED_IMAGE_COUNT=6

# Of those six, the number that must carry a digest pin: every reference
# except the two kind-loaded local-tag exemptions below. Update deliberately
# alongside K8S_EXPECTED_IMAGE_COUNT.
K8S_EXPECTED_PINNED_COUNT=4

# The exact text of a stripped `image:` reference (same stripping as
# IMAGE_REFS above) that is a locally built image loaded into the kind
# cluster with `kind load docker-image` rather than pulled from a registry:
# excluded from category 6 by exact string match, not by pattern, so a
# typo'd tag does not silently slip through as "exempt" -- same rationale as
# RAVEL_IMAGE_VAR_REF above.
K8S_LOCAL_TAG_SERVER='ravel-server:latest'
K8S_LOCAL_TAG_OPERATOR='ravel-operator:latest'

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
QUICKSTART_REFS_FILE=$(mktemp)
QUICKSTART_REQUIRED_FILE=$(mktemp)
RUN_IMAGE_REFS_FILE=$(mktemp)
RUN_IMAGE_REQUIRED_FILE=$(mktemp)
K8S_REFS_FILE=$(mktemp)
K8S_REQUIRED_FILE=$(mktemp)
trap 'rm -f "$DOCKERFILE_REFS_FILE" "$WORKFLOW_REFS_FILE" "$QUICKSTART_REFS_FILE" "$QUICKSTART_REQUIRED_FILE" "$RUN_IMAGE_REFS_FILE" "$RUN_IMAGE_REQUIRED_FILE" "$K8S_REFS_FILE" "$K8S_REQUIRED_FILE"' EXIT

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

# --- 4. Quickstart compose files (ravel.yml and rustfs.yml) ----------------

echo
echo "== quickstart compose image pins (deploy/docker-compose/{ravel,rustfs}.yml, issue #1720) =="

for f in "$RAVEL_COMPOSE_FILE" "$RUSTFS_COMPOSE_FILE"; do
  if [ ! -f "$f" ]; then
    echo "FAIL: quickstart compose file not found at $f"
    fail=1
    continue
  fi
  echo "  deployment file: $f"
  grep -nE '^[[:space:]]*image:[[:space:]]*' "$f" | while IFS= read -r line; do
    lineno=$(printf '%s\n' "$line" | cut -d: -f1)
    content=$(printf '%s\n' "$line" | cut -d: -f2-)
    ref=$(printf '%s\n' "$content" | sed -E 's/^[[:space:]]*image:[[:space:]]*//; s/[[:space:]]*$//')
    echo "$f:$lineno:$ref" >>"$QUICKSTART_REFS_FILE"
  done
done

quickstart_count=$(wc -l <"$QUICKSTART_REFS_FILE" | tr -d '[:space:]')
echo "  image references found: $quickstart_count (expected $QUICKSTART_EXPECTED_IMAGE_COUNT)"

if [ "$quickstart_count" -eq 0 ]; then
  echo "FAIL: no quickstart compose image references found; the check must never scan zero images"
  fail=1
  quickstart_required_count=0
else
  echo "  references:"
  while IFS=: read -r file lineno ref; do
    if [ "$ref" = "$RAVEL_IMAGE_VAR_REF" ]; then
      echo "    [ravel image, exempt] $file:$lineno: $ref"
    elif printf '%s\n' "$ref" | grep -q "$IMAGE_DIGEST_RE"; then
      echo "    [pinned]   $file:$lineno: $ref"
    else
      echo "    [UNPINNED] $file:$lineno: $ref"
    fi
  done <"$QUICKSTART_REFS_FILE"

  # Exclude the two Ravel-own-image references by exact match on the ref
  # field before counting and pin-checking what remains: they are checked
  # against QUICKSTART_EXPECTED_PINNED_COUNT, not against the digest regex.
  while IFS=: read -r file lineno ref; do
    if [ "$ref" != "$RAVEL_IMAGE_VAR_REF" ]; then
      echo "$file:$lineno:$ref" >>"$QUICKSTART_REQUIRED_FILE"
    fi
  done <"$QUICKSTART_REFS_FILE"

  quickstart_required_count=$(wc -l <"$QUICKSTART_REQUIRED_FILE" | tr -d '[:space:]')

  if [ "$quickstart_required_count" -eq 0 ]; then
    echo "FAIL: no pin-required quickstart compose image references found; the check must never scan zero images"
    fail=1
  else
    quickstart_unpinned=$(grep -vc "$IMAGE_DIGEST_RE" "$QUICKSTART_REQUIRED_FILE")

    if [ "$quickstart_unpinned" -ne 0 ]; then
      echo "FAIL: $quickstart_unpinned quickstart compose image reference(s) lack an @sha256: digest"
      fail=1
    fi

    if [ "$quickstart_required_count" -ne "$QUICKSTART_EXPECTED_PINNED_COUNT" ]; then
      echo "FAIL: found $quickstart_required_count pin-required quickstart compose image references, expected exactly $QUICKSTART_EXPECTED_PINNED_COUNT"
      fail=1
    fi
  fi

  if [ "$quickstart_count" -ne "$QUICKSTART_EXPECTED_IMAGE_COUNT" ]; then
    echo "FAIL: found $quickstart_count quickstart compose image references, expected exactly $QUICKSTART_EXPECTED_IMAGE_COUNT"
    fail=1
  fi
fi

# --- 5. docker run/pull/create image pins in workflow run: blocks -----------

echo
echo "== docker run/pull/create image pins (every workflow under .github/workflows) =="

workflow_scan_count=$(find "$REPO_ROOT/.github/workflows" "$REPO_ROOT/.github/actions" \
  -type f \( -name '*.yml' -o -name '*.yaml' \) | wc -l | tr -d '[:space:]')
if [ "$workflow_scan_count" -eq 0 ]; then
  echo "FAIL: no workflow or composite-action files found under .github"
  fail=1
fi

if [ "$workflow_scan_count" -gt 0 ]; then
  # A run: block is shell, not YAML, so a `docker run ...` line is scanned by
  # joining a trailing backslash continuation onto the next physical line
  # before matching (the image commonly lands on a later line than the
  # `docker run` token itself: ci.yml's RustFS and floci starts). A line
  # inside a here-doc body is skipped outright: publish-images.yml writes a
  # `docker pull ...` example into release notes for a human to read, which
  # is data this job emits, not a command this job runs. A `<<<` here-string
  # (three angle brackets, used elsewhere in publish-images.yml to feed a
  # variable to `read`) must not be mistaken for a here-doc start (two angle
  # brackets): the heredoc-start pattern requires a non-`<` character
  # immediately before the `<<`, which a third leading `<` fails.
  #
  # Once past `docker run`/`docker pull`/`docker create`, flags that consume
  # a following argument (`-p`, `-e`, `--name`, `--network`, `--entrypoint`,
  # and a handful of others no invocation here uses yet) are skipped along
  # with their value; the first remaining token that is not itself a flag is
  # the image argument.
  awk '
    BEGIN {
      n = split("-p -e --name --network --entrypoint -v --volume -u --user -w --workdir -h --hostname --env --add-host --link --label -l --platform --pull --mount --device --gpus -m --memory --cpus --restart", vf, " ")
      for (i = 1; i <= n; i++) VALUE_FLAGS[vf[i]] = 1
    }
    FNR == 1 {
      in_heredoc = 0
      heredoc_delim = ""
      buf = ""
      bufstart = 0
    }
    {
      line = $0

      if (in_heredoc) {
        trimmed = line
        sub(/^[ \t]+/, "", trimmed)
        if (trimmed == heredoc_delim) in_heredoc = 0
        next
      }

      if (match(line, /[^<]<<-?[ \t]*['"'"'"]?[A-Za-z_][A-Za-z0-9_]*['"'"'"]?[ \t]*$/)) {
        seg = substr(line, RSTART, RLENGTH)
        sub(/^.*<<-?[ \t]*/, "", seg)
        sub(/[ \t]*$/, "", seg)
        delim = seg
        sub(/^[^A-Za-z0-9_]*/, "", delim)
        sub(/[^A-Za-z0-9_]*$/, "", delim)
        if (delim != "") { heredoc_delim = delim; in_heredoc = 1 }
      }

      cont = 0
      work = line
      if (match(work, /\\[ \t]*$/)) {
        cont = 1
        sub(/\\[ \t]*$/, "", work)
      }

      if (buf == "") {
        bufstart = FNR
        t = line
        sub(/^[ \t]+/, "", t)
        buf_is_comment = (substr(t, 1, 1) == "#")
        if (buf_is_comment) cont = 0
      }

      buf = (buf == "" ? work : buf " " work)

      if (cont) next

      logical = buf
      buf = ""
      was_comment = buf_is_comment

      if (was_comment) next
      # Every invocation on the logical line, not just the first: `docker pull
      # a && docker run b` is ordinary shell, and stopping at the first one
      # would let b through unscanned with the reference count unchanged.
      # Global flags may sit between `docker` and the subcommand
      # (`docker --context ci run ...`), so allow a run of them.
      tail = logical
      while (match(tail, /docker([ \t]+-[^ \t]+([ \t]+[^- \t][^ \t]*)?)*([ \t]+(image|container))?[ \t]+(run|pull|create)([ \t]|$)/)) {
      rest = substr(tail, RSTART + RLENGTH)
      tail = rest
      sub(/^[ \t]+/, "", rest)
      image = ""
      while (rest != "") {
        if (!match(rest, /^[^ \t]+/)) break
        tok = substr(rest, RSTART, RLENGTH)
        rest = substr(rest, RSTART + RLENGTH)
        sub(/^[ \t]+/, "", rest)
        if (substr(tok, 1, 1) == "-") {
          if (index(tok, "=") > 0) continue
          if (tok in VALUE_FLAGS) {
            if (match(rest, /^[^ \t]+/)) {
              rest = substr(rest, RSTART + RLENGTH)
              sub(/^[ \t]+/, "", rest)
            }
          }
          continue
        } else {
          image = tok
          # A quoted literal is the same reference as an unquoted one, so
          # strip the quotes before the digest check. A token holding a shell
          # variable keeps them: the exemption list matches its exact text.
          if (index(image, "$") == 0) {
            gsub(/^["'"'"']|["'"'"']$/, "", image)
          }
          break
        }
      }
      if (image != "") {
        print FILENAME ":" bufstart ":" image
      }
      }
    }
  ' $(find "$REPO_ROOT/.github/workflows" "$REPO_ROOT/.github/actions" \
        -type f \( -name '*.yml' -o -name '*.yaml' \) | sort) \
    >"$RUN_IMAGE_REFS_FILE"
fi

run_image_count=$(wc -l <"$RUN_IMAGE_REFS_FILE" | tr -d '[:space:]')
echo "  docker run/pull/create image references found: $run_image_count (expected $RUN_IMAGE_EXPECTED_COUNT)"

if [ "$run_image_count" -eq 0 ]; then
  echo "FAIL: no docker run/pull/create image references found; the check must never scan zero images"
  fail=1
else
  echo "  references:"
  while IFS=: read -r file lineno image; do
    if [ "$image" = "$RUN_IMAGE_VAR_REF_SERVER" ] || [ "$image" = "$RUN_IMAGE_VAR_REF_OPERATOR" ] \
      || [ "$image" = "$RUN_IMAGE_VAR_REF_RESOLVED" ]; then
      echo "    [variable ref, exempt] $file:$lineno: $image"
    elif printf '%s\n' "$image" | grep -q "$IMAGE_DIGEST_RE"; then
      echo "    [pinned]   $file:$lineno: $image"
    else
      echo "    [UNPINNED] $file:$lineno: $image"
    fi
  done <"$RUN_IMAGE_REFS_FILE"

  # Exclude the three shell-variable exemptions by exact match before
  # counting and pin-checking what remains, same shape as the quickstart
  # category's RAVEL_IMAGE_VAR_REF exclusion above.
  while IFS=: read -r file lineno image; do
    if [ "$image" != "$RUN_IMAGE_VAR_REF_SERVER" ] && [ "$image" != "$RUN_IMAGE_VAR_REF_OPERATOR" ] \
      && [ "$image" != "$RUN_IMAGE_VAR_REF_RESOLVED" ]; then
      echo "$file:$lineno:$image" >>"$RUN_IMAGE_REQUIRED_FILE"
    fi
  done <"$RUN_IMAGE_REFS_FILE"

  run_image_required_count=$(wc -l <"$RUN_IMAGE_REQUIRED_FILE" | tr -d '[:space:]')

  if [ "$run_image_required_count" -eq 0 ]; then
    echo "FAIL: no pin-required docker run/pull/create image references found; the check must never scan zero images"
    fail=1
  else
    run_image_unpinned=$(grep -vc "$IMAGE_DIGEST_RE" "$RUN_IMAGE_REQUIRED_FILE")

    if [ "$run_image_unpinned" -ne 0 ]; then
      echo "FAIL: $run_image_unpinned docker run/pull/create image reference(s) lack an @sha256: digest"
      fail=1
    fi

    if [ "$run_image_required_count" -ne "$RUN_IMAGE_EXPECTED_PINNED_COUNT" ]; then
      echo "FAIL: found $run_image_required_count pin-required docker run/pull/create image references, expected exactly $RUN_IMAGE_EXPECTED_PINNED_COUNT"
      fail=1
    fi
  fi

  if [ "$run_image_count" -ne "$RUN_IMAGE_EXPECTED_COUNT" ]; then
    echo "FAIL: found $run_image_count docker run/pull/create image references, expected exactly $RUN_IMAGE_EXPECTED_COUNT"
    fail=1
  fi
fi

# --- 6. Kubernetes manifest image pins (deploy/k8s, issue #1720 residual) ---

echo
echo "== k8s manifest image pins (deploy/k8s, issue #1720) =="

k8s_scan_count=$(find "$K8S_DIR" -type f \( -name '*.yml' -o -name '*.yaml' \) | wc -l | tr -d '[:space:]')
if [ "$k8s_scan_count" -eq 0 ]; then
  echo "FAIL: no k8s manifest files found under $K8S_DIR"
  fail=1
fi

if [ "$k8s_scan_count" -gt 0 ]; then
  # shellcheck disable=SC2044
  for f in $(find "$K8S_DIR" -type f \( -name '*.yml' -o -name '*.yaml' \) | sort); do
    grep -nE '^[[:space:]]*image:[[:space:]]*' "$f" | while IFS= read -r line; do
      lineno=$(printf '%s\n' "$line" | cut -d: -f1)
      content=$(printf '%s\n' "$line" | cut -d: -f2-)
      ref=$(printf '%s\n' "$content" | sed -E 's/^[[:space:]]*image:[[:space:]]*//; s/[[:space:]]*$//')
      echo "$f:$lineno:$ref" >>"$K8S_REFS_FILE"
    done
  done
fi

k8s_count=$(wc -l <"$K8S_REFS_FILE" | tr -d '[:space:]')
echo "  image references found: $k8s_count (expected $K8S_EXPECTED_IMAGE_COUNT)"

if [ "$k8s_count" -eq 0 ]; then
  echo "FAIL: no k8s manifest image references found; the check must never scan zero images"
  fail=1
else
  echo "  references:"
  while IFS=: read -r file lineno ref; do
    if [ "$ref" = "$K8S_LOCAL_TAG_SERVER" ] || [ "$ref" = "$K8S_LOCAL_TAG_OPERATOR" ]; then
      echo "    [kind-local, exempt] $file:$lineno: $ref"
    elif printf '%s\n' "$ref" | grep -q "$IMAGE_DIGEST_RE"; then
      echo "    [pinned]   $file:$lineno: $ref"
    else
      echo "    [UNPINNED] $file:$lineno: $ref"
    fi
  done <"$K8S_REFS_FILE"

  # Exclude the two kind-loaded local-tag exemptions by exact match before
  # counting and pin-checking what remains, same shape as the quickstart
  # category's RAVEL_IMAGE_VAR_REF exclusion above.
  while IFS=: read -r file lineno ref; do
    if [ "$ref" != "$K8S_LOCAL_TAG_SERVER" ] && [ "$ref" != "$K8S_LOCAL_TAG_OPERATOR" ]; then
      echo "$file:$lineno:$ref" >>"$K8S_REQUIRED_FILE"
    fi
  done <"$K8S_REFS_FILE"

  k8s_required_count=$(wc -l <"$K8S_REQUIRED_FILE" | tr -d '[:space:]')

  if [ "$k8s_required_count" -eq 0 ]; then
    echo "FAIL: no pin-required k8s manifest image references found; the check must never scan zero images"
    fail=1
  else
    k8s_unpinned=$(grep -vc "$IMAGE_DIGEST_RE" "$K8S_REQUIRED_FILE")

    if [ "$k8s_unpinned" -ne 0 ]; then
      echo "FAIL: $k8s_unpinned k8s manifest image reference(s) lack an @sha256: digest"
      fail=1
    fi

    if [ "$k8s_required_count" -ne "$K8S_EXPECTED_PINNED_COUNT" ]; then
      echo "FAIL: found $k8s_required_count pin-required k8s manifest image references, expected exactly $K8S_EXPECTED_PINNED_COUNT"
      fail=1
    fi
  fi

  if [ "$k8s_count" -ne "$K8S_EXPECTED_IMAGE_COUNT" ]; then
    echo "FAIL: found $k8s_count k8s manifest image references, expected exactly $K8S_EXPECTED_IMAGE_COUNT"
    fail=1
  fi
fi

# --- Result -------------------------------------------------------------

echo
if [ "$fail" -ne 0 ]; then
  echo "RESULT: FAIL"
  exit 1
fi

echo "RESULT: PASS ($image_count compose images, $dockerfile_count Dockerfile base images, $workflow_count workflow actions, $quickstart_count quickstart compose images, $run_image_count docker run/pull/create images, $k8s_count k8s manifest images, all pinned; comparators: $REQUIRED_COMPARATORS)"
exit 0
