#!/usr/bin/env bash
# Cases for check-release-profile.sh. Add a case here before changing the rule.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into
# its scripts/guards/, so the guard's own `cd repo_root` lands on the fixture
# and nothing here touches the real checkout.
#
# Run: bash scripts/guards/check-release-profile.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-release-profile.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-release-profile-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: a scratch repo with the guard installed and a clean
# manifest, Dockerfile and publish workflow. Cases edit one of the three.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/.github/workflows"
  cp "${GUARD}" "${dir}/scripts/guards/check-release-profile.sh"
  cat >"${dir}/Cargo.toml" <<'TOML'
[workspace]
members = ["crates/*"]

[profile.dev]
debug = "line-tables-only"

[profile.ci]
inherits = "dev"
debug = false

# Thin LTO for the shipped binaries.
[profile.release]
lto = "thin"
codegen-units = 1
debug = 1

[profile.bench]
inherits = "release"
TOML
  cat >"${dir}/Dockerfile" <<'DOCKER'
FROM rust:1 AS builder
# cargo build without flags is only mentioned here, in a comment.
ENV CARGO_BUILD_JOBS=2
RUN cargo build --release --locked -p ravel-server --features sql \
    && cargo build --release --locked -p ravel-cli
DOCKER
  cat >"${dir}/.github/workflows/publish-images.yml" <<'YAML'
jobs:
  build:
    steps:
      - uses: docker/build-push-action@0000000000000000000000000000000000000000
        with:
          context: .
          target: server
YAML
  printf '%s\n' "${dir}"
}

# edit <file> <sed-expression>: in-place edit that works with BSD and GNU sed.
edit() {
  sed "$2" "$1" >"$1.new" && mv "$1.new" "$1"
}

# check <name> <repo> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-release-profile.sh 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing %s\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- the clean fixture passes, and says what it checked ----------------------

d="$(new_repo clean)"
check "clean_tree_passes" "${d}" 0 "lto=thin, codegen-units=1, debug=1, 2 cargo build(s)"

d="$(new_repo fat-lto)"
edit "${d}/Cargo.toml" 's/^lto = "thin"/lto = "fat"/'
check "fat_lto_passes" "${d}" 0 "lto=fat"

# `lto = true` is cargo's spelling of fat LTO.
d="$(new_repo true-lto)"
edit "${d}/Cargo.toml" 's/^lto = "thin"/lto = true/'
check "lto_true_passes" "${d}" 0 "lto=true"

d="$(new_repo explicit-opt3)"
awk '/^codegen-units = 1$/ { print; print "opt-level = 3"; next } { print }' \
  "${d}/Cargo.toml" >"${d}/Cargo.toml.new" && mv "${d}/Cargo.toml.new" "${d}/Cargo.toml"
check "explicit_opt_level_3_passes" "${d}" 0 ""

# --- the release profile ------------------------------------------------------

d="$(new_repo no-lto)"
edit "${d}/Cargo.toml" '/^lto = "thin"$/d'
check "lto_removed_fails" "${d}" 1 "sets no lto"

d="$(new_repo lto-off)"
edit "${d}/Cargo.toml" 's/^lto = "thin"/lto = "off"/'
check "lto_off_fails" "${d}" 1 "lto = off"

d="$(new_repo units-16)"
edit "${d}/Cargo.toml" 's/^codegen-units = 1$/codegen-units = 16/'
check "codegen_units_16_fails" "${d}" 1 "codegen-units = 16"

d="$(new_repo units-unset)"
edit "${d}/Cargo.toml" '/^codegen-units = 1$/d'
check "codegen_units_unset_fails" "${d}" 1 "codegen-units = <unset>"

d="$(new_repo opt-1)"
awk '/^codegen-units = 1$/ { print; print "opt-level = 1"; next } { print }' \
  "${d}/Cargo.toml" >"${d}/Cargo.toml.new" && mv "${d}/Cargo.toml.new" "${d}/Cargo.toml"
check "opt_level_1_fails" "${d}" 1 "[profile.release] opt-level = 1"

d="$(new_repo package-opt)"
printf '\n[profile.release.package.ravel-sql]\nopt-level = 1\n' >>"${d}/Cargo.toml"
check "package_override_opt_level_fails" "${d}" 1 "[profile.release.package.ravel-sql] opt-level = 1"

# The same override as an inline table under a bare [profile.release.package].
d="$(new_repo package-opt-inline)"
printf '\n[profile.release.package]\nravel-sql = { opt-level = 0 }\n' >>"${d}/Cargo.toml"
check "inline_package_override_opt_level_fails" "${d}" 1 "[profile.release.package] lowers opt-level"

d="$(new_repo package-opt-inline-3)"
printf '\n[profile.release.package]\nravel-sql = { opt-level = 3 }\n' >>"${d}/Cargo.toml"
check "inline_package_override_at_3_passes" "${d}" 0 ""

d="$(new_repo debug-0)"
edit "${d}/Cargo.toml" 's/^debug = 1$/debug = 0/'
check "release_debug_0_fails" "${d}" 1 "[profile.release] debug = 0"

d="$(new_repo debug-unset)"
edit "${d}/Cargo.toml" '/^debug = 1$/d'
check "release_debug_unset_fails" "${d}" 1 "debug = <unset>"

# Another profile's settings are not the release profile's: the ci profile may
# set what it likes, and a key that only appears there does not satisfy the
# release rule either.
d="$(new_repo ci-profile-tuned)"
awk '/^debug = false$/ { print; print "opt-level = 1"; print "codegen-units = 256"; next } { print }' \
  "${d}/Cargo.toml" >"${d}/Cargo.toml.new" && mv "${d}/Cargo.toml.new" "${d}/Cargo.toml"
check "other_profile_settings_are_ignored" "${d}" 0 ""

d="$(new_repo lto-only-in-ci)"
edit "${d}/Cargo.toml" '/^lto = "thin"$/d'
awk '/^debug = false$/ { print; print "lto = \"thin\""; next } { print }' \
  "${d}/Cargo.toml" >"${d}/Cargo.toml.new" && mv "${d}/Cargo.toml.new" "${d}/Cargo.toml"
check "lto_in_another_profile_does_not_count" "${d}" 1 "sets no lto"

d="$(new_repo no-release-table)"
awk '/^\[profile\.release\]$/ { skip = 1; next } /^\[/ { skip = 0 } !skip { print }' \
  "${d}/Cargo.toml" >"${d}/Cargo.toml.new" && mv "${d}/Cargo.toml.new" "${d}/Cargo.toml"
check "missing_release_table_is_cannot_check" "${d}" 2 "no [profile.release] table"

# --- the Dockerfile -----------------------------------------------------------

d="$(new_repo docker-no-release)"
edit "${d}/Dockerfile" 's/cargo build --release --locked -p ravel-cli/cargo build --locked -p ravel-cli/'
check "second_build_without_release_fails" "${d}" 1 "cargo build without --release"

# Two builds chained on ONE physical line: the second has to be checked on its
# own, or everything after the first build on a line goes unread.
d="$(new_repo docker-chained-one-line)"
cat >"${d}/Dockerfile" <<'DOCKER'
FROM rust:1 AS builder
RUN cargo build --release --locked -p ravel-server && cargo build --locked -p ravel-cli
DOCKER
check "chained_build_on_one_line_without_release_fails" "${d}" 1 \
  "cargo build without --release: cargo build --locked -p ravel-cli"

d="$(new_repo docker-chained-one-line-clean)"
cat >"${d}/Dockerfile" <<'DOCKER'
FROM rust:1 AS builder
RUN cargo build --release --locked -p ravel-server && cargo build --release --locked -p ravel-cli
DOCKER
check "chained_builds_on_one_line_are_counted_each" "${d}" 0 "2 cargo build(s)"

# A flag on a continuation line belongs to the build that starts the line.
d="$(new_repo docker-profile-on-continuation)"
cat >"${d}/Dockerfile" <<'DOCKER'
FROM rust:1 AS builder
RUN cargo build --release --locked \
    --profile ci \
    -p ravel-server
DOCKER
check "profile_on_a_continuation_line_fails" "${d}" 1 "cargo build with --profile"

d="$(new_repo docker-release-on-continuation)"
cat >"${d}/Dockerfile" <<'DOCKER'
FROM rust:1 AS builder
RUN cargo build \
    --release --locked \
    -p ravel-server
DOCKER
check "release_on_a_continuation_line_passes" "${d}" 0 "1 cargo build(s)"

d="$(new_repo docker-profile)"
edit "${d}/Dockerfile" 's/cargo build --release --locked -p ravel-server/cargo build --release --profile ci --locked -p ravel-server/'
check "build_with_profile_fails" "${d}" 1 "cargo build with --profile"

d="$(new_repo docker-no-build)"
# shellcheck disable=SC2016 # a sed address, not a shell expansion
edit "${d}/Dockerfile" '/^RUN cargo build/,$d'
check "no_build_line_is_cannot_check" "${d}" 2 "has no cargo build line"

d="$(new_repo docker-env-override)"
printf 'ENV CARGO_PROFILE_RELEASE_OPT_LEVEL=1\n' >>"${d}/Dockerfile"
check "dockerfile_env_override_fails" "${d}" 1 "CARGO_PROFILE_RELEASE_"

d="$(new_repo docker-missing)"
rm "${d}/Dockerfile"
check "missing_dockerfile_is_cannot_check" "${d}" 2 "Dockerfile does not exist"

# --- the publish workflow -----------------------------------------------------

d="$(new_repo publish-other-file)"
printf '          file: Dockerfile.prebuilt\n' >>"${d}/.github/workflows/publish-images.yml"
check "publish_from_another_dockerfile_fails" "${d}" 1 "other than the root one"

d="$(new_repo publish-root-file)"
printf '          file: ./Dockerfile\n' >>"${d}/.github/workflows/publish-images.yml"
check "publish_naming_the_root_dockerfile_passes" "${d}" 0 ""

d="$(new_repo publish-env-override)"
printf '    env:\n      CARGO_PROFILE_RELEASE_LTO: "off"\n' >>"${d}/.github/workflows/publish-images.yml"
check "publish_env_override_fails" "${d}" 1 "CARGO_PROFILE_RELEASE_"

d="$(new_repo publish-no-build)"
edit "${d}/.github/workflows/publish-images.yml" '/build-push-action/d'
check "publish_without_build_step_is_cannot_check" "${d}" 2 "no build-push-action step"

printf '\n%s passed, %s failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
