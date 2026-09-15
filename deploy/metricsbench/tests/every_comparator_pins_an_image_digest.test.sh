#!/usr/bin/env bash
# Cases for every_comparator_pins_an_image_digest.sh, focused on the fourth
# category issue #1720 added: deploy/docker-compose/ravel.yml pin enforcement.
#
# The script resolves every path it reads (Dockerfile, Dockerfile.prebuilt,
# .github/workflows, .github/actions, deploy/metricsbench/docker-compose.yml,
# and deploy/docker-compose/ravel.yml) from its own location, two directories
# up. To exercise the ravel.yml category with a mutated file while the other
# three categories still see real, passing content, each case runs against a
# scratch copy of that whole subtree, with only deploy/docker-compose/ravel.yml
# mutated per case.
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

# --- the committed file passes ----------------------------------------------

d="$(new_tree committed)"
check "the committed tree passes with all four categories pinned" "${d}" 0 \
  "RESULT: PASS"

# --- a bare tag in ravel.yml fails naming the line --------------------------

d="$(new_tree bare-tag-grafana)"
sed -i \
  's#image: grafana/grafana:13\.2\.2@sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0#image: grafana/grafana:latest#' \
  "${d}/deploy/docker-compose/ravel.yml"
check "a bare tag on grafana in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "[UNPINNED] grafana/grafana:latest"

d="$(new_tree bare-tag-minio)"
sed -i \
  's#image: quay\.io/minio/minio:RELEASE\.2025-04-08T15-41-24Z@sha256:8834ae47a2de3509b83e0e70da9369c24bbbc22de42f2a2eddc530eee88acd1b#image: quay.io/minio/minio:latest#' \
  "${d}/deploy/docker-compose/ravel.yml"
check "a bare tag on minio in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "[UNPINNED] quay.io/minio/minio:latest"

# A truncated digest must fail too: the regex requires exactly 64 hex chars,
# not just the @sha256: substring.
d="$(new_tree truncated-digest)"
sed -i \
  's#image: grafana/grafana:13\.2\.2@sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0#image: grafana/grafana:13.2.2@sha256:ac461fb3#' \
  "${d}/deploy/docker-compose/ravel.yml"
check "a truncated digest on grafana in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "[UNPINNED] grafana/grafana:13.2.2@sha256:ac461fb3"

# --- a wrong count fails -----------------------------------------------------

# Deleting the grafana image line drops the total from 6 to 5, and drops the
# pin-required count from 4 to 3: both must be caught.
d="$(new_tree wrong-total-count)"
sed -i '/^    image: grafana\/grafana:13\.2\.2@sha256:/d' \
  "${d}/deploy/docker-compose/ravel.yml"
check "removing an image line fails the total-count assertion" "${d}" 1 \
  "found 5 quickstart compose image references, expected exactly 6"
check "removing an image line also fails the pin-required-count assertion" \
  "${d}" 1 \
  "found 3 pin-required quickstart compose image references, expected exactly 4"

# Duplicating the grafana image line raises the total to 7 and the
# pin-required count to 5.
d="$(new_tree extra-image-line)"
sed -i '/^    image: grafana\/grafana:13\.2\.2@sha256:/{p}' \
  "${d}/deploy/docker-compose/ravel.yml"
check "an extra image line fails the total-count assertion" "${d}" 1 \
  "found 7 quickstart compose image references, expected exactly 6"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
