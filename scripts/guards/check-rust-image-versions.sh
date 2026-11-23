#!/usr/bin/env bash
# Every Rust builder image names the toolchain rust-toolchain.toml pins
# (issue #4630).
#
# Builder images name a Rust version by hand, one line per stage, while
# rust-toolchain.toml pins the channel that every local and CI build uses. A
# toolchain bump has to move all of them; a Dockerfile that lands while the
# bump is in flight keeps the old tag, and an image built from it compiles
# with a different compiler than the one the gates ran.
#
# Scope: tracked files (git ls-files) whose name is `Dockerfile`,
# `Dockerfile.<suffix>` (but not `Dockerfile.dockerignore`, Docker's
# per-Dockerfile ignore file) or `<prefix>.Dockerfile`. Docs and other prose
# are not scanned, so a Markdown example naming an old image is not a finding.
#
# In those files a line is checked when:
#   - its first word is FROM, in any case, and its image, after any `--flag=`
#     options such as `--platform=$BUILDPLATFORM`, is `rust`, `rust:<tag>` or
#     `docker.io/library/rust:<tag>`, optionally followed by an `@sha256:`
#     digest and `AS <name>`; or
#   - its first word is ARG, in any case, and the default it sets starts with
#     `rust:` or `docker.io/library/rust:` (an image a later FROM names by
#     variable).
# Images that only start with "rust" (rustlang/rust, rust-musl-builder) are
# other images and are not checked.
#
# The version is the tag up to the first "-": 1.100.0 in 1.100.0-trixie. It
# must equal the channel exactly. A tag with no x.y.z version (rust:latest,
# rust:1, rust:slim, a bare `rust`, a `${VAR}`) does not pin the toolchain and
# is a finding too.
#
# Suppression: a `#` comment on the line directly above the checked line,
#
#   # rust-image-allow: <reason>
#
# with a non-empty reason. Only the line above counts: Dockerfile comments
# take a whole line, and text after FROM or ARG on the same line is part of
# the instruction. A marker with an empty reason does not suppress.
#
# Usage:
#   scripts/guards/check-rust-image-versions.sh [repo-root]
#     default repo-root: the checkout holding this script
#
# Exit 0 clean, 1 on a finding, 2 when the check could not run (no
# rust-toolchain.toml, no `channel` line, a channel that is not a plain x.y.z
# version, a repo-root that git cannot list, or zero checked lines: this
# repository has such lines, so finding none means the scan missed them),
# 64 on bad usage. Findings print as
# `file:line: <image> has version <found>, want <wanted>`.
#
# Cases: scripts/guards/check-rust-image-versions.test.sh
set -uo pipefail

me="check-rust-image-versions"

usage() {
  printf 'usage: %s [repo-root]\n' "$0" >&2
  exit 64
}

cannot_check() {
  printf '%s: cannot check: %s\n' "${me}" "$1" >&2
  exit 2
}

if [[ $# -gt 1 ]]; then
  usage
fi
if [[ $# -eq 1 ]]; then
  case "$1" in
    -h | --help) usage ;;
    -*) usage ;;
  esac
  [[ -d "$1" ]] || usage
  repo_root="$1"
else
  repo_root="$(cd "$(dirname "$0")/../.." && pwd)" || cannot_check "cannot resolve the repo root"
fi
cd "${repo_root}" || cannot_check "cannot enter ${repo_root}"

toolchain="rust-toolchain.toml"
[[ -f "${toolchain}" ]] || cannot_check "${toolchain} does not exist"

channel_line="$(grep -E '^[[:space:]]*channel[[:space:]]*=' "${toolchain}" || true)"
[[ -n "${channel_line}" ]] || cannot_check "${toolchain} has no channel line"
want="${channel_line#*=}"
want="${want%%#*}"
want="${want//[[:space:]]/}"
want="${want//\"/}"
want="${want//\'/}"
[[ "${want}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
  || cannot_check "${toolchain} channel '${want}' is not a plain x.y.z version"

listing="$(git -c core.quotePath=false ls-files 2>&1)" \
  || cannot_check "git ls-files failed in ${repo_root}: ${listing}"
files=()
while IFS= read -r f; do
  base="${f##*/}"
  case "${base}" in
    Dockerfile.dockerignore) continue ;;
    Dockerfile | Dockerfile.* | *.Dockerfile) files+=("${f}") ;;
  esac
done <<<"${listing}"

checked=0
exempt=0
findings=0

# image_of <word>: prints the tag spec after `rust` (":<tag>[@digest]",
# "@digest" or "") and returns 0 when the word names the official rust image.
image_of() {
  local w="$1"
  w="${w#docker.io/library/}"
  case "${w}" in
    rust) printf '' ;;
    rust:* | rust@*) printf '%s' "${w#rust}" ;;
    *) return 1 ;;
  esac
}

# check_line <file:line> <image> <line above>
check_line() {
  local at="$1" image="$2" prev="$3" spec tag version reason
  spec="$(image_of "${image}")" || return 0
  checked=$((checked + 1))
  tag="${spec%%@*}"
  tag="${tag#:}"
  version="${tag%%-*}"
  if [[ "${version}" == "${want}" ]]; then
    return 0
  fi
  if [[ "${prev}" =~ ^[[:space:]]*#.*rust-image-allow:(.*)$ ]]; then
    reason="${BASH_REMATCH[1]//[[:space:]]/}"
    if [[ -n "${reason}" ]]; then
      exempt=$((exempt + 1))
      return 0
    fi
    printf '%s: rust-image-allow marker above has no reason; it does not suppress\n' "${at}"
  fi
  if [[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s: %s has version %s, want %s (%s)\n' \
      "${at}" "${image}" "${version}" "${want}" "${toolchain}"
  else
    printf '%s: %s has no x.y.z version, want %s (%s)\n' \
      "${at}" "${image}" "${want}" "${toolchain}"
  fi
  findings=$((findings + 1))
}

for file in "${files[@]}"; do
  [[ -f "${file}" ]] || continue
  n=0
  prev=""
  while IFS= read -r line || [[ -n "${line}" ]]; do
    n=$((n + 1))
    line="${line%$'\r'}"
    read -r -a words <<<"${line}"
    if [[ ${#words[@]} -ge 2 ]]; then
      keyword="${words[0],,}"
      if [[ "${keyword}" == "from" ]]; then
        i=1
        while [[ ${i} -lt ${#words[@]} && "${words[i]}" == --* ]]; do
          i=$((i + 1))
        done
        if [[ ${i} -lt ${#words[@]} ]]; then
          check_line "${file}:${n}" "${words[i]}" "${prev}"
        fi
      elif [[ "${keyword}" == "arg" ]]; then
        # One ARG line can declare several variables.
        for ((i = 1; i < ${#words[@]}; i++)); do
          [[ "${words[i]}" == *=* ]] || continue
          value="${words[i]#*=}"
          value="${value//\"/}"
          value="${value//\'/}"
          # check_line ignores a value that is not the official rust image,
          # and reports an untagged one (rust, rust@<digest>) as a FROM does.
          check_line "${file}:${n}" "${value}" "${prev}"
        done
      fi
    fi
    prev="${line}"
  done <"${file}"
done

if [[ ${checked} -eq 0 ]]; then
  cannot_check "no FROM or ARG line names a rust image in ${#files[@]} Dockerfile(s); expected at least one"
fi

printf '%s: %d rust image line(s) checked in %d Dockerfile(s), %d exempt, %d finding(s); want %s\n' \
  "${me}" "${checked}" "${#files[@]}" "${exempt}" "${findings}" "${want}"
[[ ${findings} -eq 0 ]] || exit 1
exit 0
