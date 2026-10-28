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
# under deploy/metricsbench/, preserving the name exactly.
#
# Issue #1310 generalised it into a repo-wide pin check, on the same rationale:
# a moving tag or a mutable action ref is unreproducible and unauditable
# wherever it appears, not just in this one compose file. Issue #1720 added a
# fourth category for the same reason, scoped to the quickstart compose file.
# Issue #1338 added a fifth, scoped to the image argument of `docker run`,
# `docker pull`, and `docker create` inside workflow `run:` blocks. Issue
# #1720's residual round added a sixth, scoped to the `image:` references in
# deploy/k8s manifests that category 4's own scope note originally deferred.
# ADR-0927 amendment 2026-10-29 added a seventh, for the s3s-fs binary the
# MetricsBench stack runs from a release asset rather than an image.
# It now scans seven categories, each with its own exact-count assertion so a
# scan that finds nothing in a category fails rather than passing silently:
#
#   1. every `image:` reference in deploy/metricsbench/docker-compose.yml
#      and its Ravel overlay deploy/metricsbench/ravel.yml -- excluding
#      ravel.yml's two `${RAVEL_IMAGE:?...}` references by exact match, since
#      the image under test is a required run parameter with no default in
#      the file (the overlay was added for ADR-0927 amendment 2026-10-26 A;
#      the docker-compose.yml check is unchanged in behaviour);
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
#   7. the s3s-fs binary deploy/metricsbench/run.sh fetches for the s3s
#      service: run.sh must carry both arch sha256 constants (exact values
#      below), map each arch to its own, and in fetch_s3s_fs compare the
#      sha256 of the cached binary and of the download against the pinned
#      one, each mismatch exiting 65, with the download moved into the cache
#      and S3S_FS_BIN exported only after its comparison; S3S_FS_BIN is
#      exported nowhere else but the never-started placeholder, there is one
#      download, and docker-compose.yml takes the binary only from
#      `${S3S_FS_BIN:?...}`, with no default.
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
RUN_SCRIPT="$DEPLOY_DIR/run.sh"
METRICSBENCH_RAVEL_FILE="$DEPLOY_DIR/ravel.yml"
RAVEL_COMPOSE_FILE="$REPO_ROOT/deploy/docker-compose/ravel.yml"
RUSTFS_COMPOSE_FILE="$REPO_ROOT/deploy/docker-compose/rustfs.yml"
K8S_DIR="$REPO_ROOT/deploy/k8s"

# The comparators ADR-0927 requires in the portable cross-engine lane. Each must
# appear as a service in the compose file. Prometheus and VictoriaMetrics are
# named directly; "mimir" is the object-storage-native PromQL system (ADR-0927
# permits Mimir or Thanos Receive) chosen for this deployment.
REQUIRED_COMPARATORS="prometheus victoriametrics mimir"

# Exact number of `image:` references the committed deployment must contain,
# across docker-compose.yml and its ravel.yml overlay: prometheus,
# victoriametrics, s3s, createbuckets, mimir, ravel-qualify, ravel-server.
# Raised 5->7 when ravel.yml put Ravel in the portable lane (ADR-0927
# amendment 2026-10-26 A): its two services both run the image under test. If
# a service is added or removed, update this number deliberately in the same
# change.
COMPOSE_EXPECTED_IMAGE_COUNT=7

# Of those seven, the number that must carry a digest pin: every image except
# the two ravel.yml references matched by METRICSBENCH_RAVEL_IMAGE_REF below.
# Unchanged at 5 by ravel.yml. Update deliberately alongside
# COMPOSE_EXPECTED_IMAGE_COUNT.
COMPOSE_EXPECTED_PINNED_COUNT=5

# The exact stripped `image:` text of ravel.yml's two services. Ravel's image
# is a required run parameter with no default (`${RAVEL_IMAGE:?...}`), so the
# file holds no reference to pin; the run pins it by passing a digest and
# capture-effective-config.sh records what ran. Excluded by exact string
# match, so an unpinned literal or a `${RAVEL_IMAGE:-tag}` default in its
# place is not exempt.
METRICSBENCH_RAVEL_IMAGE_REF='${RAVEL_IMAGE:?set RAVEL_IMAGE to the ravel-server image under test, digest-pinned}'

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
# scan does not count, and its `report` job uses no action at all. Raised
# 107->108 when ci.yml's `tla` job gained a second download-artifact step
# that retries the marker download. Raised 108->110 when ci.yml gained the
# `all-features` job (issue #1925): a checkout and a rust-cache. Raised 110->111
# when ci.yml's `features` job became the `features-lane` matrix (issue #1793):
# the matrix job adds a nextest install (taiki-e/install-action), and the
# `features` job that reports on it uses no action at all. Raised 111->112
# when k8s-nightly.yml's `chaos` job gained an upload-artifact step for the
# scenarios' process logs on failure (issue #2666). Lowered 112->110 when
# ci.yml's per-PR `quickstart` job was removed (ADR-0081 A6): a checkout and a
# rust-cache. Lowered 110->109 when k8s-nightly.yml's `k8s-build` job moved its
# cargo build into the builder image and dropped its sccache-action step,
# which no longer reached the build (#3146). Raised 109->115 when ci.yml
# gained the features-agents and features-mcp jobs (a checkout and a
# rust-cache each) and the sdk job (a checkout and a setup-node), epic
# #3331 task T2. Raised 115->118 when sim-nightly.yml gained the agents-sim
# job (a checkout, a rust-cache and an upload-artifact), epic #3331 task T22.
WORKFLOW_EXPECTED_ACTION_COUNT=118

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
RAVEL_IMAGE_VAR_REF='${RAVEL_IMAGE:-ghcr.io/nofireai/ravel-server:0.27.0}'

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
# DR_AWS_CLI_IMAGE's own default there. Lowered 18->15 when the three
# AWS CLI container runs that created a RustFS bucket (ci.yml's
# object-store-contract and bench-smoke, metricsbench-nightly.yml) moved to
# the runner's own aws CLI (issue #2036). Lowered 15->13 when ci.yml's
# `quickstart` job and its two telemetrygen runs were removed (ADR-0081 A6).
# Lowered 13->12 when metricsbench-nightly.yml stopped starting its own
# RustFS container and ran deploy/metricsbench/run.sh against the compose
# stack instead (issue #940).
RUN_IMAGE_EXPECTED_COUNT=12

# Of those, the number that must carry a digest pin: every reference except
# the three shell-variable exemptions below. Update deliberately alongside
# RUN_IMAGE_EXPECTED_COUNT. Raised 11->12 with the dr-rehearsal object-store run,
# which is digest pinned. Lowered 12->9 with the three AWS CLI runs above,
# all of which were digest pinned. Lowered 9->7 with the two telemetrygen
# runs above, both digest pinned. Lowered 7->6 with metricsbench-nightly.yml's
# RustFS run above, which was digest pinned.
RUN_IMAGE_EXPECTED_PINNED_COUNT=6

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

for f in "$COMPOSE_FILE" "$METRICSBENCH_RAVEL_FILE"; do
  if [ ! -f "$f" ]; then
    echo "FAIL: compose file not found at $f"
    exit 1
  fi
done

echo
echo "== docker-compose image pins =="
echo "  deployment file: $COMPOSE_FILE"
echo "  deployment file: $METRICSBENCH_RAVEL_FILE"

# Collect image references. Match indented `image:` keys only, strip the key and
# surrounding whitespace to leave the bare reference.
IMAGE_REFS=$(grep -hE '^[[:space:]]*image:[[:space:]]*' "$COMPOSE_FILE" "$METRICSBENCH_RAVEL_FILE" \
  | sed -E 's/^[[:space:]]*image:[[:space:]]*//; s/[[:space:]]*$//')

if [ -z "$IMAGE_REFS" ]; then
  echo "FAIL: no image references found; the check must never scan zero images"
  fail=1
else
  image_count=$(printf '%s\n' "$IMAGE_REFS" | grep -c .)
  echo "  image references found: $image_count (expected $COMPOSE_EXPECTED_IMAGE_COUNT)"
  echo "  references:"
  printf '%s\n' "$IMAGE_REFS" | while IFS= read -r ref; do
    if [ "$ref" = "$METRICSBENCH_RAVEL_IMAGE_REF" ]; then
      echo "    [ravel image, exempt] $ref"
    elif printf '%s\n' "$ref" | grep -q "$IMAGE_DIGEST_RE"; then
      echo "    [pinned]   $ref"
    else
      echo "    [UNPINNED] $ref"
    fi
  done

  # The Ravel-image references are excluded by exact match before the pin
  # check; everything else must carry a digest.
  REQUIRED_REFS=$(printf '%s\n' "$IMAGE_REFS" | while IFS= read -r ref; do
    if [ "$ref" != "$METRICSBENCH_RAVEL_IMAGE_REF" ]; then
      printf '%s\n' "$ref"
    fi
  done)
  required_count=$(printf '%s\n' "$REQUIRED_REFS" | grep -c .)
  unpinned=$(printf '%s\n' "$REQUIRED_REFS" | grep . | grep -vc "$IMAGE_DIGEST_RE")
  if [ "$unpinned" -ne 0 ]; then
    echo "FAIL: $unpinned compose image reference(s) lack an @sha256: digest"
    fail=1
  fi

  if [ "$required_count" -ne "$COMPOSE_EXPECTED_PINNED_COUNT" ]; then
    echo "FAIL: found $required_count pin-required compose image references, expected exactly $COMPOSE_EXPECTED_PINNED_COUNT"
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
if grep -Eq "^  ravel-server:[[:space:]]*\$" "$METRICSBENCH_RAVEL_FILE"; then
  echo "  comparator present: ravel-server"
else
  echo "FAIL: required comparator 'ravel-server' is missing from $METRICSBENCH_RAVEL_FILE"
  fail=1
fi

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

# --- 7. s3s-fs release binary pinned by sha256 in run.sh ---------------------
# Line-exact on purpose: a rewording of any of these lines must update this
# category deliberately, the same way a count change does.

echo
echo "== s3s-fs binary sha256 pin (deploy/metricsbench/run.sh) =="

S3S_PIN_X86_64='S3S_FS_SHA256_X86_64="0ac5480b51389c1913f9645d0e1ff2ff1dfdd3a44bb396a7cb74c5e91176c01f"'
S3S_PIN_AARCH64='S3S_FS_SHA256_AARCH64="6d5d47a9823e62a36a7ce4b702886ba0e474557084dae877998015ffdc6717ad"'
S3S_EXPECTED_PIN_COUNT=2

# count_exact <file> <line>: lines of <file> equal to <line> after leading
# whitespace is stripped.
count_exact() {
  sed -E 's/^[[:space:]]+//' "$1" | grep -cxF -- "$2"
}

# line_in_body <line>: the 1-based line number of <line> (leading whitespace
# stripped) within fetch_s3s_fs's body, or empty. Only called after the line
# is known to occur exactly once there.
line_in_body() {
  printf '%s\n' "$S3S_BODY" | sed -E 's/^[[:space:]]+//' | grep -nxF -- "$1" | cut -d: -f1
}

s3s_pin_count=0
if [ ! -f "$RUN_SCRIPT" ]; then
  echo "FAIL: run.sh not found at $RUN_SCRIPT"
  fail=1
else
  for pin in "$S3S_PIN_X86_64" "$S3S_PIN_AARCH64"; do
    n=$(count_exact "$RUN_SCRIPT" "$pin")
    if [ "$n" -eq 1 ]; then
      echo "  [pinned]   $pin"
      s3s_pin_count=$((s3s_pin_count + 1))
    else
      echo "FAIL: run.sh carries '$pin' $n times, expected exactly once"
      fail=1
    fi
  done
  echo "  sha256 constants found: $s3s_pin_count (expected $S3S_EXPECTED_PIN_COUNT)"

  for arm in 'x86_64) s3s_fs_sha256="$S3S_FS_SHA256_X86_64" ;;' \
    'aarch64) s3s_fs_sha256="$S3S_FS_SHA256_AARCH64" ;;'; do
    if [ "$(count_exact "$RUN_SCRIPT" "$arm")" -eq 1 ]; then
      echo "  arch mapped: $arm"
    else
      echo "FAIL: run.sh does not map the arch to its pinned sha256 exactly once: '$arm'"
      fail=1
    fi
  done
  if [ "$(grep -c 's3s_fs_sha256=' "$RUN_SCRIPT")" -ne 2 ]; then
    echo "FAIL: run.sh assigns s3s_fs_sha256 other than from the two pinned constants"
    fail=1
  fi

  # Exactly two exports of S3S_FS_BIN: the never-started placeholder, and
  # the verified path inside fetch_s3s_fs.
  if [ "$(grep -c 'S3S_FS_BIN=' "$RUN_SCRIPT")" -ne 2 ] ||
    [ "$(count_exact "$RUN_SCRIPT" 'export S3S_FS_BIN="/nonexistent/s3s-fs-not-fetched"')" -ne 1 ]; then
    echo "FAIL: run.sh sets S3S_FS_BIN somewhere other than the placeholder and fetch_s3s_fs's verified path"
    fail=1
  fi
  if [ "$(grep -c 'S3S_FS_URL_BASE' "$RUN_SCRIPT")" -ne 2 ]; then
    echo "FAIL: run.sh uses the s3s-fs release URL outside fetch_s3s_fs"
    fail=1
  fi

  S3S_BODY=$(sed -n '/^fetch_s3s_fs() {$/,/^}$/p' "$RUN_SCRIPT")
  if [ -z "$S3S_BODY" ]; then
    echo "FAIL: run.sh has no fetch_s3s_fs function"
    fail=1
  else
    s3s_bad=0
    for want in 'got="$(sha256_of "$bin")" || fail_step fetch-s3s-fs "cannot hash the cached $bin"' \
      'got="$(sha256_of "$tmp")" || { rm -f "$tmp"; fail_step fetch-s3s-fs "cannot hash the download of $url"; }' \
      'chmod 0755 "$tmp" && mv -f "$tmp" "$bin" ||' \
      'export S3S_FS_BIN="$bin"'; do
      if [ "$(printf '%s\n' "$S3S_BODY" | sed -E 's/^[[:space:]]+//' | grep -cxF -- "$want")" -ne 1 ]; then
        echo "FAIL: fetch_s3s_fs lacks exactly one '$want'"
        s3s_bad=1
      fi
    done
    cmp_line='if [ "$got" != "$s3s_fs_sha256" ]; then'
    if [ "$(printf '%s\n' "$S3S_BODY" | sed -E 's/^[[:space:]]+//' | grep -cxF -- "$cmp_line")" -ne 2 ] ||
      [ "$(printf '%s\n' "$S3S_BODY" | grep -cF 'exit "$EXIT_REFUSED"')" -ne 2 ]; then
      echo "FAIL: fetch_s3s_fs lacks its two sha256 comparisons, each exiting 65"
      s3s_bad=1
    fi
    if [ "$(printf '%s\n' "$S3S_BODY" | grep -cE '^[[:space:]]*curl ')" -ne 1 ]; then
      echo "FAIL: fetch_s3s_fs downloads other than once"
      s3s_bad=1
    fi
    if [ "$s3s_bad" -eq 0 ]; then
      hash_bin=$(line_in_body 'got="$(sha256_of "$bin")" || fail_step fetch-s3s-fs "cannot hash the cached $bin"')
      hash_tmp=$(line_in_body 'got="$(sha256_of "$tmp")" || { rm -f "$tmp"; fail_step fetch-s3s-fs "cannot hash the download of $url"; }')
      cmp1=$(line_in_body "$cmp_line" | sed -n 1p)
      cmp2=$(line_in_body "$cmp_line" | sed -n 2p)
      curl_at=$(printf '%s\n' "$S3S_BODY" | grep -nE '^[[:space:]]*curl ' | cut -d: -f1)
      mv_at=$(line_in_body 'chmod 0755 "$tmp" && mv -f "$tmp" "$bin" ||')
      export_at=$(line_in_body 'export S3S_FS_BIN="$bin"')
      # cached: hash, then compare, before any download; downloaded: fetch,
      # hash, compare, then cache, then export.
      if [ "$hash_bin" -lt "$cmp1" ] && [ "$cmp1" -lt "$curl_at" ] &&
        [ "$curl_at" -lt "$hash_tmp" ] && [ "$hash_tmp" -lt "$cmp2" ] &&
        [ "$cmp2" -lt "$mv_at" ] && [ "$mv_at" -lt "$export_at" ]; then
        echo "  fetch_s3s_fs compares the cached binary and the download before export"
      else
        echo "FAIL: fetch_s3s_fs does not hash and compare before it caches or exports (lines: cached hash $hash_bin, compare $cmp1, curl $curl_at, download hash $hash_tmp, compare $cmp2, mv $mv_at, export $export_at)"
        s3s_bad=1
      fi
    fi
    [ "$s3s_bad" -eq 0 ] || fail=1
  fi
fi

if [ -f "$COMPOSE_FILE" ]; then
  s3s_mounts=$(grep -c 'S3S_FS_BIN' "$COMPOSE_FILE")
  s3s_required=$(grep -cE '^[[:space:]]*- \$\{S3S_FS_BIN:\?[^}]+\}:/usr/local/bin/s3s-fs:ro$' "$COMPOSE_FILE")
  if [ "$s3s_required" -eq 1 ] && [ "$(grep -cE '\$\{S3S_FS_BIN[^}]*\}' "$COMPOSE_FILE")" -eq 1 ]; then
    echo "  docker-compose.yml mounts only \${S3S_FS_BIN:?...} ($s3s_mounts mentions)"
  else
    echo "FAIL: docker-compose.yml must take the s3s-fs binary from exactly one \${S3S_FS_BIN:?...} mount, with no default"
    fail=1
  fi
fi

if [ "$s3s_pin_count" -ne "$S3S_EXPECTED_PIN_COUNT" ]; then
  echo "FAIL: found $s3s_pin_count s3s-fs sha256 constants, expected exactly $S3S_EXPECTED_PIN_COUNT"
  fail=1
fi

# --- Result -------------------------------------------------------------

echo
if [ "$fail" -ne 0 ]; then
  echo "RESULT: FAIL"
  exit 1
fi

echo "RESULT: PASS ($image_count compose images, $dockerfile_count Dockerfile base images, $workflow_count workflow actions, $quickstart_count quickstart compose images, $run_image_count docker run/pull/create images, $k8s_count k8s manifest images, $s3s_pin_count s3s-fs binary sha256s, all pinned; comparators: $REQUIRED_COMPARATORS ravel-server)"
exit 0
