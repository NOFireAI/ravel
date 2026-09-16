#!/usr/bin/env bash
# Cases for every_comparator_pins_an_image_digest.sh, focused on the fourth
# category issue #1720 added: deploy/docker-compose/ravel.yml AND
# deploy/docker-compose/minio.yml pin enforcement (minio.yml is ravel.yml's
# standalone MinIO-and-bucket mirror and must stay pinned in lockstep).
#
# The script resolves every path it reads (Dockerfile, Dockerfile.prebuilt,
# .github/workflows, .github/actions, deploy/metricsbench/docker-compose.yml,
# and the two quickstart compose files) from its own location, two
# directories up. To exercise the quickstart category with a mutated file
# while the other three categories still see real, passing content, each
# case runs against a scratch copy of that whole subtree, with only the
# quickstart compose files mutated per case.
#
# No `sed -i`: GNU sed requires a bare `-i` (in-place, no backup) while BSD
# sed (macOS) requires `-i ''` (a mandatory backup-suffix argument), and a
# script written for one silently misbehaves or errors on the other. Every
# mutation here instead runs sed without `-i`, writing to a fresh temp file,
# then renames that file over the original -- POSIX `sed` and `mv` behave
# identically on both userlands.
#
# Run: bash deploy/metricsbench/tests/every_comparator_pins_an_image_digest.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(cd "${HERE}/.." && pwd)"
REPO_ROOT="$(cd "${DEPLOY_DIR}/../.." && pwd)"
SCRIPT="${HERE}/every_comparator_pins_an_image_digest.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/pin-check-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# mutate <file> <sed-script>: portable substitute-in-place. Writes sed's
# output to a fresh temp file and renames it over the original, rather than
# relying on sed -i, whose flag syntax differs between GNU and BSD sed.
mutate() {
  local file="$1" script="$2" tmp
  tmp="$(mktemp)"
  sed "${script}" "${file}" >"${tmp}" && mv "${tmp}" "${file}"
}

# new_tree <name>: a scratch copy of the whole subtree the script reads, with
# the script itself installed at the same relative path. Prints its root.
new_tree() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/deploy/metricsbench/tests" "${dir}/deploy/docker-compose" \
    "${dir}/.github/workflows" "${dir}/.github/actions"
  cp "${REPO_ROOT}/Dockerfile" "${dir}/Dockerfile"
  cp "${REPO_ROOT}/Dockerfile.prebuilt" "${dir}/Dockerfile.prebuilt"
  cp -r "${REPO_ROOT}/.github/workflows/." "${dir}/.github/workflows/"
  cp -r "${REPO_ROOT}/.github/actions/." "${dir}/.github/actions/"
  cp "${REPO_ROOT}/deploy/metricsbench/docker-compose.yml" \
    "${dir}/deploy/metricsbench/docker-compose.yml"
  cp "${REPO_ROOT}/deploy/docker-compose/ravel.yml" \
    "${dir}/deploy/docker-compose/ravel.yml"
  cp "${REPO_ROOT}/deploy/docker-compose/minio.yml" \
    "${dir}/deploy/docker-compose/minio.yml"
  cp "${SCRIPT}" \
    "${dir}/deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh"
  chmod +x "${dir}/deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh"
  printf '%s\n' "${dir}"
}

# check <name> <tree> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(sh "${dir}/deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh" 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing "%s"\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- the committed files pass ------------------------------------------------

d="$(new_tree committed)"
check "the committed tree passes with all four categories pinned" "${d}" 0 \
  "RESULT: PASS"

# --- a bare tag in ravel.yml fails naming the line --------------------------

d="$(new_tree bare-tag-grafana)"
mutate "${d}/deploy/docker-compose/ravel.yml" \
  's#image: grafana/grafana:13\.2\.2@sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0#image: grafana/grafana:latest#'
check "a bare tag on grafana in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "ravel.yml:159: grafana/grafana:latest"

d="$(new_tree bare-tag-minio-ravel)"
mutate "${d}/deploy/docker-compose/ravel.yml" \
  's#image: quay\.io/minio/minio:RELEASE\.2025-04-08T15-41-24Z@sha256:8834ae47a2de3509b83e0e70da9369c24bbbc22de42f2a2eddc530eee88acd1b#image: quay.io/minio/minio:latest#'
check "a bare tag on minio in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "ravel.yml:21: quay.io/minio/minio:latest"

# --- a bare tag in minio.yml fails naming the line (issue #1720 fix round: --
# --- minio.yml previously diverged from ravel.yml's pin with nothing to    --
# --- notice, because the guard never read it) -------------------------------

d="$(new_tree bare-tag-minio-mirror)"
mutate "${d}/deploy/docker-compose/minio.yml" \
  's#image: quay\.io/minio/minio:RELEASE\.2025-04-08T15-41-24Z@sha256:8834ae47a2de3509b83e0e70da9369c24bbbc22de42f2a2eddc530eee88acd1b#image: quay.io/minio/minio:latest#'
check "a bare tag on minio in minio.yml fails naming the unpinned reference" \
  "${d}" 1 "minio.yml:7: quay.io/minio/minio:latest"

d="$(new_tree bare-tag-mc-mirror)"
mutate "${d}/deploy/docker-compose/minio.yml" \
  's#image: quay\.io/minio/mc:RELEASE\.2025-04-08T15-39-49Z@sha256:7e3efb09c22c0882fbf341b9d99f61f94ae6c4c20a06f2f1a2b20ea8993d8952#image: quay.io/minio/mc:latest#'
check "a bare tag on mc in minio.yml fails naming the unpinned reference" \
  "${d}" 1 "minio.yml:18: quay.io/minio/mc:latest"

# A truncated digest must fail too: the regex requires exactly 64 hex chars,
# not just the @sha256: substring.
d="$(new_tree truncated-digest)"
mutate "${d}/deploy/docker-compose/ravel.yml" \
  's#image: grafana/grafana:13\.2\.2@sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0#image: grafana/grafana:13.2.2@sha256:ac461fb3#'
check "a truncated digest on grafana in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "ravel.yml:159: grafana/grafana:13.2.2@sha256:ac461fb3"

# --- a wrong count fails, across both files ---------------------------------

# Deleting the grafana image line from ravel.yml drops the combined total
# from 8 to 7, and the pin-required count from 6 to 5: both must be caught.
d="$(new_tree wrong-total-count)"
mutate "${d}/deploy/docker-compose/ravel.yml" \
  '/^    image: grafana\/grafana:13\.2\.2@sha256:/d'
check "removing an image line fails the total-count assertion" "${d}" 1 \
  "found 7 quickstart compose image references, expected exactly 8"
check "removing an image line also fails the pin-required-count assertion" \
  "${d}" 1 \
  "found 5 pin-required quickstart compose image references, expected exactly 6"

# Duplicating the grafana image line raises the combined total to 9.
d="$(new_tree extra-image-line)"
mutate "${d}/deploy/docker-compose/ravel.yml" \
  '/^    image: grafana\/grafana:13\.2\.2@sha256:/{p;}'
check "an extra image line fails the total-count assertion" "${d}" 1 \
  "found 9 quickstart compose image references, expected exactly 8"

# Removing an image line from minio.yml must be caught the same way: the
# guard has to count across both quickstart files, not just ravel.yml.
d="$(new_tree wrong-count-minio)"
mutate "${d}/deploy/docker-compose/minio.yml" \
  '/^    image: quay\.io\/minio\/mc:RELEASE/d'
check "removing an image line from minio.yml fails the total-count assertion" \
  "${d}" 1 "found 7 quickstart compose image references, expected exactly 8"

# A missing minio.yml must fail outright (missing file) and also drop the
# combined count, not silently scan ravel.yml alone.
d="$(new_tree missing-minio-file)"
rm "${d}/deploy/docker-compose/minio.yml"
check "a missing minio.yml fails naming the missing path" "${d}" 1 \
  "quickstart compose file not found at"
check "a missing minio.yml also fails the total-count assertion" "${d}" 1 \
  "found 6 quickstart compose image references, expected exactly 8"

# --- fifth category: docker run/pull/create image pins in workflow run: ----
# --- blocks (issue #1338) ---------------------------------------------------

# A bare-tag image on a single-line docker run fails naming the unpinned
# reference. This is the acceptance test for issue #1338.
d="$(new_tree docker-run-image-with-tag-only-fails)"
mutate "${d}/.github/workflows/ci.yml" \
  's#quay\.io/minio/mc:RELEASE\.2025-08-13T08-35-41Z@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727#quay.io/minio/mc:latest#'
check "docker_run_image_with_tag_only_fails" "${d}" 1 \
  "ci.yml:1217: quay.io/minio/mc:latest"

# The three shell-variable image references this static scan cannot resolve
# ("$RAVEL_SERVER_IMAGE"/"$RAVEL_OPERATOR_IMAGE" in ci.yml and k8s-nightly.yml,
# "$ref" in publish-images.yml) are exempt by exact string, not flagged
# unpinned, and the committed tree still passes despite carrying no digest on
# any of them.
d="$(new_tree docker-run-variable-ref-is-exempt)"
check "docker_run_variable_ref_is_exempt: RAVEL_SERVER_IMAGE ref in ci.yml is not unpinned" \
  "${d}" 0 'ci.yml:1597: "$RAVEL_SERVER_IMAGE"'
check "docker_run_variable_ref_is_exempt: exempt marker is used, not [UNPINNED]" \
  "${d}" 0 '[variable ref, exempt]'

# A docker run whose image argument sits on a backslash-continued line, not
# the same physical line as "docker run", is still found: the scanner joins
# continuation lines before matching. Mutating the digest on the
# continuation line (metricsbench-nightly.yml's minio start spans lines
# 67-71, with the image on line 71) must be caught and reported at the
# invocation's start line, proving the join actually ran rather than the
# image happening to be on the same line as "docker run".
d="$(new_tree docker-run-with-line-continuation-is-scanned)"
mutate "${d}/.github/workflows/metricsbench-nightly.yml" \
  's#quay\.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e#quay.io/minio/minio:latest#'
check "docker_run_with_line_continuation_is_scanned" "${d}" 1 \
  "metricsbench-nightly.yml:67: quay.io/minio/minio:latest"

# Removing one docker-run invocation must fail the exact-count assertion,
# not silently scan fewer references.
d="$(new_tree docker-run-wrong-count)"
mutate "${d}/.github/workflows/metricsbench-nightly.yml" \
  '/^          docker run --rm --network host --entrypoint sh \\$/,/^            quay\.io\/minio\/mc@sha256:/d'
check "removing a docker run line fails the docker-run-image count assertion" \
  "${d}" 1 "found 16 docker run/pull/create image references, expected exactly 17"

# A second invocation on the same logical line is scanned too. Chaining with
# && is ordinary shell, and a scanner that stops at the first `docker` on the
# line lets the second image through with the reference count unchanged, so
# the count assertion cannot catch it either.
d="$(new_tree docker-run-second-invocation-on-one-line)"
mutate "${d}/.github/workflows/k8s-nightly.yml" \
  's#^      - name: Install sccache#      - name: Chained docker invocations\n        run: docker pull alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000 \&\& docker run busybox:latest echo hi\n      - name: Install sccache#'
check "docker_run_second_invocation_on_one_line_is_scanned" "${d}" 1 \
  "busybox:latest"

# A global flag between `docker` and its subcommand must not hide the
# invocation.
d="$(new_tree docker-run-global-flag-before-subcommand)"
mutate "${d}/.github/workflows/k8s-nightly.yml" \
  's#^      - name: Install sccache#      - name: Global flag before the subcommand\n        run: docker --context ci run nginx:latest\n      - name: Install sccache#'
check "docker_run_global_flag_before_subcommand_is_scanned" "${d}" 1 \
  "nginx:latest"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
