#!/usr/bin/env bash
# Cases for every_comparator_pins_an_image_digest.sh, focused on the fourth
# category issue #1720 added: deploy/docker-compose/ravel.yml AND
# deploy/docker-compose/rustfs.yml pin enforcement (rustfs.yml is ravel.yml's
# standalone RustFS-and-bucket mirror and must stay pinned in lockstep).
#
# The script resolves every path it reads (Dockerfile, Dockerfile.prebuilt,
# .github/workflows, .github/actions, deploy/metricsbench/docker-compose.yml,
# the two quickstart compose files, and deploy/k8s) from its own location, two
# directories up. To exercise one category with a mutated file while the
# other categories still see real, passing content, each case runs against a
# scratch copy of that whole subtree, with only the file(s) under test
# mutated per case.
#
# All four of deploy/k8s's registry images are pinned: three mirror the exact
# digest ci.yml already pins for the same image, and curlimages/curl was
# resolved from the registry. The committed tree passes every category, which
# is what the cases below assert; each seeds its own bad input instead.
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
    "${dir}/deploy/k8s" "${dir}/.github/workflows" "${dir}/.github/actions"
  cp "${REPO_ROOT}/Dockerfile" "${dir}/Dockerfile"
  cp "${REPO_ROOT}/Dockerfile.prebuilt" "${dir}/Dockerfile.prebuilt"
  cp -r "${REPO_ROOT}/.github/workflows/." "${dir}/.github/workflows/"
  cp -r "${REPO_ROOT}/.github/actions/." "${dir}/.github/actions/"
  cp "${REPO_ROOT}/deploy/metricsbench/docker-compose.yml" \
    "${dir}/deploy/metricsbench/docker-compose.yml"
  cp "${REPO_ROOT}/deploy/docker-compose/ravel.yml" \
    "${dir}/deploy/docker-compose/ravel.yml"
  cp "${REPO_ROOT}/deploy/docker-compose/rustfs.yml" \
    "${dir}/deploy/docker-compose/rustfs.yml"
  cp -r "${REPO_ROOT}/deploy/k8s/." "${dir}/deploy/k8s/"
  cp "${SCRIPT}" \
    "${dir}/deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh"
  chmod +x "${dir}/deploy/metricsbench/tests/every_comparator_pins_an_image_digest.sh"
  printf '%s\n' "${dir}"
}

# line_of <tree> <relative-compose-path> <fixed-substring>
#
# The line number the guard reports, resolved from the fixture rather than
# hardcoded. These expectations used to carry literal line numbers, and any
# edit ABOVE a pinned image in deploy/docker-compose/ravel.yml broke this
# suite even though the guard was working: adding five lines for the S3
# plaintext flag moved grafana from 159 to 164 and failed two cases here.
# The line is still asserted, because "names the line" is the behaviour under
# test; it is just no longer written down in two places that drift apart.
line_of() {
  local dir="$1" rel="$2" needle="$3"
  grep -n -F -- "${needle}" "${dir}/${rel}" | head -1 | cut -d: -f1
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

# --- the committed files pass every category ---------------------------------

# Every category passes on the committed tree. A suite whose baseline expects
# the repository to be red cannot tell a fixed reference from a broken scan,
# so each case below seeds its own bad input instead.
d="$(new_tree committed)"
check "the committed tree passes with all six categories pinned" \
  "${d}" 0 "RESULT: PASS"

# --- a bare tag in ravel.yml fails naming the line --------------------------

d="$(new_tree bare-tag-grafana)"
mutate "${d}/deploy/docker-compose/ravel.yml" \
  's#image: grafana/grafana:13\.2\.2@sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0#image: grafana/grafana:latest#'
check "a bare tag on grafana in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "ravel.yml:$(line_of "${d}" deploy/docker-compose/ravel.yml 'image: grafana/grafana:latest'): grafana/grafana:latest"

d="$(new_tree bare-tag-rustfs-ravel)"
mutate "${d}/deploy/docker-compose/ravel.yml" \
  's#image: ghcr\.io/rustfs/rustfs:1\.0\.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff#image: ghcr.io/rustfs/rustfs:latest#'
check "a bare tag on rustfs in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "ravel.yml:$(line_of "${d}" deploy/docker-compose/ravel.yml 'image: ghcr.io/rustfs/rustfs:latest'): ghcr.io/rustfs/rustfs:latest"

# --- a bare tag in rustfs.yml fails naming the line (issue #1720 fix round: -
# --- the mirror previously diverged from ravel.yml's pin with nothing to   --
# --- notice, because the guard never read it) -------------------------------

d="$(new_tree bare-tag-rustfs-mirror)"
mutate "${d}/deploy/docker-compose/rustfs.yml" \
  's#image: ghcr\.io/rustfs/rustfs:1\.0\.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff#image: ghcr.io/rustfs/rustfs:latest#'
check "a bare tag on rustfs in rustfs.yml fails naming the unpinned reference" \
  "${d}" 1 "rustfs.yml:$(line_of "${d}" deploy/docker-compose/rustfs.yml 'image: ghcr.io/rustfs/rustfs:latest'): ghcr.io/rustfs/rustfs:latest"

d="$(new_tree bare-tag-aws-cli-mirror)"
mutate "${d}/deploy/docker-compose/rustfs.yml" \
  's#image: public\.ecr\.aws/aws-cli/aws-cli:2\.37\.2@sha256:e38214027df83cb6631adcf980a092a98d1d29788789bff2a0f424e87e3da8ed#image: public.ecr.aws/aws-cli/aws-cli:latest#'
check "a bare tag on the AWS CLI in rustfs.yml fails naming the unpinned reference" \
  "${d}" 1 "rustfs.yml:$(line_of "${d}" deploy/docker-compose/rustfs.yml 'image: public.ecr.aws/aws-cli/aws-cli:latest'): public.ecr.aws/aws-cli/aws-cli:latest"

# A truncated digest must fail too: the regex requires exactly 64 hex chars,
# not just the @sha256: substring.
d="$(new_tree truncated-digest)"
mutate "${d}/deploy/docker-compose/ravel.yml" \
  's#image: grafana/grafana:13\.2\.2@sha256:ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0#image: grafana/grafana:13.2.2@sha256:ac461fb3#'
check "a truncated digest on grafana in ravel.yml fails naming the unpinned reference" \
  "${d}" 1 "ravel.yml:$(line_of "${d}" deploy/docker-compose/ravel.yml 'image: grafana/grafana:13.2.2@sha256:ac461fb3'): grafana/grafana:13.2.2@sha256:ac461fb3"

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

# Removing an image line from rustfs.yml must be caught the same way: the
# guard has to count across both quickstart files, not just ravel.yml.
d="$(new_tree wrong-count-rustfs)"
mutate "${d}/deploy/docker-compose/rustfs.yml" \
  '/^    image: public\.ecr\.aws\/aws-cli\/aws-cli:/d'
check "removing an image line from rustfs.yml fails the total-count assertion" \
  "${d}" 1 "found 7 quickstart compose image references, expected exactly 8"

# A missing rustfs.yml must fail outright (missing file) and also drop the
# combined count, not silently scan ravel.yml alone.
d="$(new_tree missing-rustfs-file)"
rm "${d}/deploy/docker-compose/rustfs.yml"
check "a missing rustfs.yml fails naming the missing path" "${d}" 1 \
  "quickstart compose file not found at"
check "a missing rustfs.yml also fails the total-count assertion" "${d}" 1 \
  "found 6 quickstart compose image references, expected exactly 8"

# --- fifth category: docker run/pull/create image pins in workflow run: ----
# --- blocks (issue #1338) ---------------------------------------------------

# A bare-tag image on a single-line docker run fails naming the unpinned
# reference. This is the acceptance test for issue #1338.
d="$(new_tree docker-run-image-with-tag-only-fails)"
mutate "${d}/.github/workflows/ci.yml" \
  's#public\.ecr\.aws/aws-cli/aws-cli:2\.37\.2@sha256:e38214027df83cb6631adcf980a092a98d1d29788789bff2a0f424e87e3da8ed#public.ecr.aws/aws-cli/aws-cli:latest#'
check "docker_run_image_with_tag_only_fails" "${d}" 1 \
  "public.ecr.aws/aws-cli/aws-cli:latest"

# The three shell-variable image references this static scan cannot resolve
# ("$RAVEL_SERVER_IMAGE"/"$RAVEL_OPERATOR_IMAGE" in ci.yml and k8s-nightly.yml,
# "$ref" in publish-images.yml) are exempt by exact string, not flagged
# unpinned, and carry no digest on any of them without affecting the result:
# The committed tree passes, so these assert exit 0 and check that the refs
# appear under the exempt marker rather than as findings.
d="$(new_tree docker-run-variable-ref-is-exempt)"
check "docker_run_variable_ref_is_exempt: RAVEL_SERVER_IMAGE ref in ci.yml is not unpinned" \
  "${d}" 0 '"$RAVEL_SERVER_IMAGE"'
check "docker_run_variable_ref_is_exempt: exempt marker is used, not [UNPINNED]" \
  "${d}" 0 '[variable ref, exempt]'

# A docker run whose image argument sits on a backslash-continued line, not
# the same physical line as "docker run", is still found: the scanner joins
# continuation lines before matching. Mutating the digest on the
# continuation line (metricsbench-nightly.yml's RustFS start spans lines
# 67-71, with the image on line 71) must be caught and reported at the
# invocation's start line, proving the join actually ran rather than the
# image happening to be on the same line as "docker run".
d="$(new_tree docker-run-with-line-continuation-is-scanned)"
mutate "${d}/.github/workflows/metricsbench-nightly.yml" \
  's#ghcr\.io/rustfs/rustfs:1\.0\.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff#ghcr.io/rustfs/rustfs:latest#'
check "docker_run_with_line_continuation_is_scanned" "${d}" 1 \
  "metricsbench-nightly.yml:67: ghcr.io/rustfs/rustfs:latest"

# Removing one docker-run invocation must fail the exact-count assertion,
# not silently scan fewer references.
d="$(new_tree docker-run-wrong-count)"
mutate "${d}/.github/workflows/metricsbench-nightly.yml" \
  '/^            docker run --rm --network host \\$/,/^              public\.ecr\.aws\/aws-cli\/aws-cli:2\.37\.2@sha256:/d'
check "removing a docker run line fails the docker-run-image count assertion" \
  "${d}" 1 "found 17 docker run/pull/create image references, expected exactly 18"

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

# Docker's management-command spellings are matched: `docker image pull X`
# and `docker container run X` are the same invocations with a subcommand
# group in front, and a scanner that misses them extracts nothing, so the
# count assertion cannot notice either.
d="$(new_tree docker-management-command-spelling)"
mutate "${d}/.github/workflows/k8s-nightly.yml" \
  's#^      - name: Install sccache#      - name: Management command spellings\n        run: docker image pull nginx:latest\n      - name: Install sccache#'
check "docker_management_command_spelling_is_scanned" "${d}" 1 \
  "nginx:latest"

# A composite action under .github/actions is scanned too. Category 3 already
# covers that directory for `uses:` refs, and the actions here do run docker.
d="$(new_tree docker-run-in-composite-action)"
mutate "${d}/.github/actions/free-disk-space/action.yml" \
  's#^      run: |#      run: |\n        docker run redis:latest true#'
check "docker_run_in_a_composite_action_is_scanned" "${d}" 1 \
  "redis:latest"

# --- sixth category: deploy/k8s manifest image pins (issue #1720 residual) --

# A bare-tag image on a k8s manifest fails naming the unpinned reference.
# This is the acceptance test for the sixth category: against the scanner as
# it stood before this round (no k8s category at all), this mutation would
# have gone entirely unnoticed.
d="$(new_tree k8s_manifest_image_without_digest_fails)"
mutate "${d}/deploy/k8s/rustfs.yaml" \
  's#image: ghcr\.io/rustfs/rustfs:1\.0\.0@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff#image: ghcr.io/rustfs/rustfs:latest#'
check "k8s_manifest_image_without_digest_fails" "${d}" 1 \
  "rustfs.yaml:51: ghcr.io/rustfs/rustfs:latest"

# The two kind-loaded local tags (built and `kind load docker-image`d rather
# than pulled from a registry) are exempt by exact string, not flagged
# unpinned, on the unmutated committed tree. Against the scanner as it stood
# before this round, none of these substrings appeared anywhere in the
# output, since deploy/k8s was not scanned at all.
d="$(new_tree kind_local_tag_is_exempt)"
check "kind_local_tag_is_exempt: ravel-server:latest in ravelcluster-dev.yaml is not unpinned" \
  "${d}" 0 "examples/ravelcluster-dev.yaml:15: ravel-server:latest"
check "kind_local_tag_is_exempt: ravel-operator:latest in operator.yaml is not unpinned" \
  "${d}" 0 "operator/operator.yaml:41: ravel-operator:latest"
check "kind_local_tag_is_exempt: exempt marker is used, not [UNPINNED]" \
  "${d}" 0 "[kind-local, exempt]"

# Removing a k8s manifest image line must fail both the total-count and the
# pin-required-count assertions, not silently scan fewer references. Deleting
# rustfs.yaml's (pin-required) RustFS image line drops the total from 6 to 5
# and the pin-required count from 4 to 3.
d="$(new_tree k8s-wrong-count)"
mutate "${d}/deploy/k8s/rustfs.yaml" \
  '/^          image: ghcr\.io\/rustfs\/rustfs:/d'
check "removing a k8s manifest image line fails the k8s total-count assertion" \
  "${d}" 1 "found 5 k8s manifest image references, expected exactly 6"
check "removing a k8s manifest image line also fails the k8s pin-required-count assertion" \
  "${d}" 1 "found 3 pin-required k8s manifest image references, expected exactly 4"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
