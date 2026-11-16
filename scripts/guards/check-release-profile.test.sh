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
  local c
  for c in ravel-segment ravel-logseg ravel-rspan; do
    mkdir -p "${dir}/crates/${c}"
    cat >"${dir}/crates/${c}/Cargo.toml" <<TOML
[package]
name = "${c}"

[features]
# Test-only; format-drill named in a comment is not a finding.
format-drill = []

[dependencies]
thiserror = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
TOML
  done
  # A dependent with the format crates as plain dependencies.
  mkdir -p "${dir}/services/ravel-server"
  cat >"${dir}/services/ravel-server/Cargo.toml" <<'TOML'
[package]
name = "ravel-server"

[features]
sql = []

[dependencies]
ravel-segment = { workspace = true } # format-drill only under dev-dependencies

[dev-dependencies]
ravel-segment = { workspace = true }
TOML
  # The compatibility tests' crate, the only one allowed to turn the feature
  # on for its tests (it is excluded from the workspace, ADR-4233 decision 9).
  mkdir -p "${dir}/crates/ravel-compat-tests"
  cat >"${dir}/crates/ravel-compat-tests/Cargo.toml" <<'TOML'
[package]
name = "ravel-compat-tests"

[dev-dependencies]
ravel-segment = { path = "../ravel-segment", features = ["format-drill"] }
TOML
  # The crate that defines the Antithesis features (ADR-4326), and the server's
  # same-name forward of them.
  mkdir -p "${dir}/crates/ravel-invariant"
  cat >"${dir}/crates/ravel-invariant/Cargo.toml" <<'TOML'
[package]
name = "ravel-invariant"

[dependencies]
antithesis_sdk = { version = "0.4", optional = true }

[features]
# antithesis in a comment is not a finding.
antithesis = ["dep:antithesis_sdk"]
capture = []
antithesis-mutants = []
TOML
  printf '%s\n' "${dir}"
}

# edit <file> <sed-expression>: in-place edit that works with BSD and GNU sed.
edit() {
  sed "$2" "$1" >"$1.new" && mv "$1.new" "$1"
}

# insert_after <file> <exact-line> <text>: adds <text> (awk escapes such as \n
# allowed) after the first line equal to <exact-line>.
insert_after() {
  awk -v want="$2" -v add="$3" '{ print } $0 == want && !done { print add; done = 1 }' \
    "$1" >"$1.new" && mv "$1.new" "$1"
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

# --- format-drill stays out of release builds (ADR-4233 decision 9) ----------

d="$(new_repo drill-clean-report)"
check "format_drill_scan_says_what_it_read" "${d}" 0 \
  "format-drill test-only: 3 defining crate(s), 7 manifest(s) scanned"

d="$(new_repo docker-format-drill)"
edit "${d}/Dockerfile" 's/--features sql /--features sql,ravel-segment\/format-drill /'
check "docker_build_enabling_format_drill_fails" "${d}" 1 \
  "Dockerfile:4: release build enables format-drill or --all-features"

d="$(new_repo docker-all-features)"
edit "${d}/Dockerfile" 's/-p ravel-cli$/-p ravel-cli --all-features/'
check "docker_build_with_all_features_fails" "${d}" 1 \
  "Dockerfile:5: release build enables format-drill or --all-features"

d="$(new_repo docker-drill-comment)"
insert_after "${d}/Dockerfile" "FROM rust:1 AS builder" "# never --features format-drill or --all-features here"
check "docker_comment_naming_format_drill_passes" "${d}" 0 ""

d="$(new_repo publish-format-drill)"
printf '          build-args: |\n            CARGO_FEATURES=format-drill\n' \
  >>"${d}/.github/workflows/publish-images.yml"
check "publish_workflow_enabling_format_drill_fails" "${d}" 1 \
  "publish-images.yml:9: release build enables format-drill or --all-features"

d="$(new_repo publish-all-features)"
printf '      - run: cargo build --release --all-features\n' \
  >>"${d}/.github/workflows/publish-images.yml"
check "publish_workflow_with_all_features_fails" "${d}" 1 \
  "publish-images.yml:8: release build enables format-drill or --all-features"

d="$(new_repo default-drill)"
insert_after "${d}/crates/ravel-segment/Cargo.toml" "format-drill = []" 'default = ["format-drill"]'
check "default_feature_with_format_drill_fails" "${d}" 1 \
  "crates/ravel-segment/Cargo.toml:7: enables format-drill outside dev-dependencies"

# A multi-line array: the continuation line belongs to `default`, not to the
# format-drill definition above it.
d="$(new_repo default-drill-multiline)"
insert_after "${d}/crates/ravel-rspan/Cargo.toml" "format-drill = []" 'default = [\n    "format-drill",\n]'
check "multiline_default_with_format_drill_fails" "${d}" 1 \
  "crates/ravel-rspan/Cargo.toml:8: enables format-drill outside dev-dependencies"

# A forward under another name would let `--features drill` in the Dockerfile
# enable it without naming it.
d="$(new_repo forward-renamed)"
insert_after "${d}/services/ravel-server/Cargo.toml" "sql = []" 'drill = ["ravel-logseg/format-drill"]'
check "forwarding_feature_under_another_name_fails" "${d}" 1 \
  "services/ravel-server/Cargo.toml:6: enables format-drill outside dev-dependencies"

d="$(new_repo forward-same-name)"
insert_after "${d}/services/ravel-server/Cargo.toml" "sql = []" 'format-drill = [\n    "ravel-segment/format-drill",\n]'
check "forwarding_feature_named_format_drill_passes" "${d}" 0 ""

d="$(new_repo dependency-drill)"
edit "${d}/services/ravel-server/Cargo.toml" \
  's/^ravel-segment = { workspace = true } #.*$/ravel-segment = { workspace = true, features = ["format-drill"] }/'
check "non_dev_dependency_enabling_format_drill_fails" "${d}" 1 \
  "services/ravel-server/Cargo.toml:8: enables format-drill outside dev-dependencies"

d="$(new_repo dependency-table-drill)"
printf '\n[dependencies.ravel-rspan]\nworkspace = true\nfeatures = ["format-drill"]\n' \
  >>"${d}/services/ravel-server/Cargo.toml"
check "dependency_table_enabling_format_drill_fails" "${d}" 1 \
  "services/ravel-server/Cargo.toml:15: enables format-drill outside dev-dependencies"

d="$(new_repo workspace-dependency-drill)"
printf '\n[workspace.dependencies]\nravel-segment = { path = "crates/ravel-segment", features = ["format-drill"] }\n' \
  >>"${d}/Cargo.toml"
check "workspace_dependency_enabling_format_drill_fails" "${d}" 1 \
  "Cargo.toml:21: enables format-drill outside dev-dependencies"

d="$(new_repo target-dependency-drill)"
printf "\n[target.'cfg(unix)'.dependencies]\nravel-logseg = { workspace = true, features = [\"format-drill\"] }\n" \
  >>"${d}/services/ravel-server/Cargo.toml"
check "target_dependency_enabling_format_drill_fails" "${d}" 1 \
  "services/ravel-server/Cargo.toml:14: enables format-drill outside dev-dependencies"

d="$(new_repo target-dev-dependency-drill)"
printf "\n[target.'cfg(unix)'.dev-dependencies]\nravel-logseg = { workspace = true, features = [\"format-drill\"] }\n" \
  >>"${d}/services/ravel-server/Cargo.toml"
check "target_dev_dependency_outside_compat_tests_fails" "${d}" 1 \
  "services/ravel-server/Cargo.toml:14: enables format-drill outside dev-dependencies"

# A workspace member's dev-dependency would be unified into every crate a
# --workspace test or clippy run builds.
d="$(new_repo member-dev-dependency-drill)"
edit "${d}/services/ravel-server/Cargo.toml" \
  's/^ravel-segment = { workspace = true }$/ravel-segment = { workspace = true, features = ["format-drill"] }/'
check "workspace_member_dev_dependency_enabling_format_drill_fails" "${d}" 1 \
  "services/ravel-server/Cargo.toml:11: enables format-drill outside dev-dependencies"

d="$(new_repo compat-tests-dev-dependency-drill)"
check "compat_tests_dev_dependency_enabling_format_drill_passes" "${d}" 0 ""

d="$(new_repo drill-crate-missing)"
rm "${d}/crates/ravel-logseg/Cargo.toml"
check "missing_drill_crate_manifest_is_cannot_check" "${d}" 2 \
  "crates/ravel-logseg/Cargo.toml does not exist"

d="$(new_repo drill-definition-missing)"
edit "${d}/crates/ravel-rspan/Cargo.toml" '/^format-drill = \[\]$/d'
check "missing_drill_feature_definition_is_cannot_check" "${d}" 2 \
  "crates/ravel-rspan/Cargo.toml declares no format-drill feature"

# --- antithesis and antithesis-mutants stay out of release builds (ADR-4326) --

d="$(new_repo antithesis-clean-report)"
check "antithesis_scan_says_what_it_read" "${d}" 0 \
  "antithesis test-only: defined in crates/ravel-invariant, 7 manifest(s) scanned"

d="$(new_repo docker-antithesis-mutants)"
edit "${d}/Dockerfile" 's/--features sql /--features sql,antithesis-mutants /'
check "antithesis-mutants in the published build is a finding" "${d}" 1 \
  "Dockerfile:4: release build enables antithesis or antithesis-mutants"

d="$(new_repo docker-antithesis)"
edit "${d}/Dockerfile" 's/-p ravel-cli$/-p ravel-cli --features ravel-invariant\/antithesis/'
check "antithesis_in_the_published_build_is_a_finding" "${d}" 1 \
  "Dockerfile:5: release build enables antithesis or antithesis-mutants"

# The instrumentation shim's crate name is not the feature.
d="$(new_repo docker-antithesis-other-word)"
insert_after "${d}/Dockerfile" "FROM rust:1 AS builder" "LABEL tool=antithesis-instrumentation-free"
check "antithesis_inside_a_longer_word_passes" "${d}" 0 ""

d="$(new_repo docker-antithesis-comment)"
insert_after "${d}/Dockerfile" "FROM rust:1 AS builder" "# never --features antithesis here"
check "antithesis_in_a_dockerfile_comment_passes" "${d}" 0 ""

d="$(new_repo publish-antithesis)"
printf '          build-args: |\n            CARGO_FEATURES=antithesis\n' \
  >>"${d}/.github/workflows/publish-images.yml"
check "antithesis_in_the_publish_workflow_is_a_finding" "${d}" 1 \
  "publish-images.yml:9: release build enables antithesis or antithesis-mutants"

d="$(new_repo antithesis-default)"
insert_after "${d}/crates/ravel-invariant/Cargo.toml" "capture = []" 'default = ["antithesis-mutants"]'
check "antithesis_in_a_default_feature_list_is_a_finding" "${d}" 1 \
  "crates/ravel-invariant/Cargo.toml:11: enables antithesis outside its own same-name feature"

d="$(new_repo antithesis-forward-other-name)"
insert_after "${d}/services/ravel-server/Cargo.toml" "sql = []" 'chaos = ["ravel-invariant/antithesis"]'
check "antithesis_forwarded_under_another_name_is_a_finding" "${d}" 1 \
  "services/ravel-server/Cargo.toml:6: enables antithesis outside its own same-name feature"

d="$(new_repo antithesis-forward-same-name)"
insert_after "${d}/services/ravel-server/Cargo.toml" "sql = []" \
  'antithesis = [\n    "dep:antithesis-instrumentation",\n    "ravel-invariant/antithesis",\n]\nantithesis-mutants = ["ravel-invariant/antithesis-mutants"]'
check "antithesis_forwarded_under_the_same_name_passes" "${d}" 0 ""

d="$(new_repo antithesis-dependency-edge)"
edit "${d}/services/ravel-server/Cargo.toml" \
  's/^ravel-segment = { workspace = true } #.*$/ravel-invariant = { workspace = true, features = ["antithesis"] }/'
check "antithesis_on_a_dependency_edge_is_a_finding" "${d}" 1 \
  "services/ravel-server/Cargo.toml:8: enables antithesis outside its own same-name feature"

d="$(new_repo antithesis-workspace-edge)"
printf '\n[workspace.dependencies]\nravel-invariant = { path = "crates/ravel-invariant", features = ["antithesis-mutants"] }\n' \
  >>"${d}/Cargo.toml"
check "antithesis_on_a_workspace_dependency_edge_is_a_finding" "${d}" 1 \
  "Cargo.toml:21: enables antithesis outside its own same-name feature"

# Unlike format-drill, no crate may turn it on through a dev-dependency: the
# workspace's test builds would unify it into every crate they compile.
d="$(new_repo antithesis-dev-dependency-edge)"
printf '\n[dev-dependencies.ravel-invariant]\npath = "../ravel-invariant"\nfeatures = [\n    "antithesis",\n]\n' \
  >>"${d}/crates/ravel-compat-tests/Cargo.toml"
check "antithesis_on_a_dev_dependency_edge_is_a_finding" "${d}" 1 \
  "crates/ravel-compat-tests/Cargo.toml:10: enables antithesis outside its own same-name feature"

d="$(new_repo antithesis-definition-missing)"
edit "${d}/crates/ravel-invariant/Cargo.toml" '/^antithesis-mutants = \[\]$/d'
check "missing_antithesis_feature_definition_is_cannot_check" "${d}" 2 \
  "crates/ravel-invariant/Cargo.toml declares no antithesis-mutants feature"

d="$(new_repo antithesis-crate-missing)"
rm "${d}/crates/ravel-invariant/Cargo.toml"
check "missing_invariant_crate_manifest_is_cannot_check" "${d}" 2 \
  "crates/ravel-invariant/Cargo.toml does not exist"

printf '\n%s passed, %s failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
