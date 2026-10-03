#!/usr/bin/env bash
# Fold changelog fragments into CHANGELOG.md (issue #2323).
#
# Usage:
#   scripts/changelog-assemble.sh           fold every fragment, then delete it
#   scripts/changelog-assemble.sh --check   validate every fragment, change nothing
#
# A fragment is one file under changelog.d/ named NUMBER.SECTION.md, or
# NUMBER-N.SECTION.md for a further change under the same issue or pull
# request, holding one CHANGELOG.md bullet exactly as it will appear (its
# first line starts with "- "). SECTION is one of added, changed, removed,
# fixed, security: the Keep a Changelog headings CHANGELOG.md uses.
# changelog.d/README.md is the only other file allowed there.
#
# Folding appends each fragment under its heading in the [Unreleased] section,
# after any bullets already there, in ascending NUMBER order (then N). A
# heading [Unreleased] lacks is created, placed in the order the headings first
# appear in CHANGELOG.md.
#
# Release order matters. The release workflow reads the ## [X.Y.Z] section of
# CHANGELOG.md from the tagged tree, never [Unreleased], so release prep runs
# this FIRST, then turns the filled [Unreleased] into ## [X.Y.Z] - DATE (with
# a fresh empty [Unreleased] above it), bumps the version, commits, and tags.
# Folding after the version heading is cut puts every fragment in the new,
# empty [Unreleased], and the release notes silently miss them.
#
# Exit 0: done (or, with --check, every fragment is valid).
# Exit 1: a bad fragment: a name that does not match the rule, an unknown
#         section, an empty file, a file whose first line does not start
#         with "- ", a heading line, or CRLF line endings. Nothing is changed.
# Exit 2: could not run: no CHANGELOG.md, no [Unreleased] section, a bad
#         argument, or a write that failed.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${repo_root}" || exit 2

me="changelog-assemble.sh"
check_only=0
case "${1:-}" in
  "") ;;
  --check) check_only=1 ;;
  -h | --help)
    awk 'NR > 1 && !/^#/ { exit } NR > 1' "$0"
    exit 0
    ;;
  *)
    echo "${me}: unknown argument: $1 (want --check or nothing)" >&2
    exit 2
    ;;
esac
if [[ $# -gt 1 ]]; then
  echo "${me}: too many arguments" >&2
  exit 2
fi

changelog="CHANGELOG.md"
frag_dir="changelog.d"

# Supported sections, lowercase, in Keep a Changelog order. Deprecated is
# left out because CHANGELOG.md has never used it.
supported=(added changed removed fixed security)

title_of() {
  printf '%s%s' "$(printf '%s' "${1:0:1}" | tr '[:lower:]' '[:upper:]')" "${1:1}"
}

if [[ ! -f "${changelog}" ]]; then
  echo "${me}: no ${changelog} at ${repo_root}" >&2
  exit 2
fi
# A while-read loop rather than mapfile: macOS ships bash 3.2, which has no
# mapfile, and release prep is run by hand, often on a Mac.
if [[ ! -r "${changelog}" ]]; then
  echo "${me}: cannot read ${changelog}" >&2
  exit 2
fi
lines=()
while IFS= read -r line || [[ -n "${line}" ]]; do
  lines+=("${line}")
done <"${changelog}"

u_start=-1
for i in "${!lines[@]}"; do
  if [[ "${lines[i]}" =~ ^##\ \[Unreleased\][[:space:]]*$ ]]; then
    u_start="${i}"
    break
  fi
done
if [[ "${u_start}" -lt 0 ]]; then
  echo "${me}: ${changelog} has no '## [Unreleased]' section" >&2
  exit 2
fi
u_end="${#lines[@]}"
for ((i = u_start + 1; i < ${#lines[@]}; i++)); do
  if [[ "${lines[i]}" == "## "* ]]; then
    u_end="${i}"
    break
  fi
done

# --- validate every fragment ------------------------------------------------

bad=0
entries=()
if [[ -d "${frag_dir}" ]]; then
  # The suffix starts at 2: the unsuffixed name is the first change.
  name_re='^([1-9][0-9]*)(-([2-9]|[1-9][0-9]+))?\.([a-z]+)\.md$'
  while IFS= read -r -d '' path; do
    name="${path#"${frag_dir}"/}"
    [[ "${name}" == "README.md" ]] && continue
    if [[ -d "${path}" || ! "${name}" =~ ${name_re} ]]; then
      echo "${me}: ${path}: name does not match NUMBER.SECTION.md or NUMBER-N.SECTION.md" >&2
      bad=1
      continue
    fi
    number="${BASH_REMATCH[1]}"
    suffix="${BASH_REMATCH[3]:-1}"
    section="${BASH_REMATCH[4]}"
    known=0
    for s in "${supported[@]}"; do
      [[ "${s}" == "${section}" ]] && known=1
    done
    if [[ "${known}" -eq 0 ]]; then
      echo "${me}: ${path}: unknown section '${section}' (want one of: ${supported[*]})" >&2
      bad=1
      continue
    fi
    if [[ ! -s "${path}" ]] || ! grep -q '[^[:space:]]' "${path}"; then
      echo "${me}: ${path}: empty fragment" >&2
      bad=1
      continue
    fi
    first="$(head -n 1 "${path}")"
    if [[ "${first}" != "- "* ]]; then
      echo "${me}: ${path}: first line must start with '- ' (one CHANGELOG.md bullet)" >&2
      bad=1
      continue
    fi
    # A heading line would split [Unreleased] or open a stray subsection, and
    # a carriage return would be copied into CHANGELOG.md.
    if grep -qE '^#{1,6} ' "${path}"; then
      echo "${me}: ${path}: a fragment holds one bullet; a heading line is not allowed" >&2
      bad=1
      continue
    fi
    if grep -q $'\r' "${path}"; then
      echo "${me}: ${path}: CRLF line endings; save the fragment with LF" >&2
      bad=1
      continue
    fi
    entries+=("${number} ${suffix} ${section} ${path}")
  done < <(find "${frag_dir}" -mindepth 1 -maxdepth 1 -print0)
fi

if [[ "${bad}" -ne 0 ]]; then
  echo "${me}: refused; nothing was changed" >&2
  exit 1
fi

if [[ "${check_only}" -eq 1 ]]; then
  echo "${me}: ${#entries[@]} fragment(s) valid"
  exit 0
fi
if [[ "${#entries[@]}" -eq 0 ]]; then
  echo "${me}: no fragments to fold"
  exit 0
fi

sorted=()
while IFS= read -r entry; do
  sorted+=("${entry}")
done < <(printf '%s\n' "${entries[@]}" | sort -k1,1n -k2,2n -k4,4)

# --- heading order: first appearance in CHANGELOG.md, then the rest ---------

order=()
for line in "${lines[@]}"; do
  if [[ "${line}" =~ ^###\ ([A-Za-z]+)[[:space:]]*$ ]]; then
    h="$(printf '%s' "${BASH_REMATCH[1]}" | tr '[:upper:]' '[:lower:]')"
    for s in "${supported[@]}"; do
      if [[ "${s}" == "${h}" && " ${order[*]:-} " != *" ${h} "* ]]; then
        order+=("${h}")
      fi
    done
  fi
done
for s in "${supported[@]}"; do
  [[ " ${order[*]:-} " != *" ${s} "* ]] && order+=("${s}")
done
rank_of() {
  local i
  for i in "${!order[@]}"; do
    if [[ "${order[i]}" == "$1" ]]; then
      printf '%s' "${i}"
      return
    fi
  done
  printf '%s' 999
}

# --- split [Unreleased] into a preamble and its ### blocks ------------------

preamble=()
block_names=()
block_text=()
cur=-1
for ((i = u_start + 1; i < u_end; i++)); do
  line="${lines[i]}"
  if [[ "${line}" == "### "* ]]; then
    h="$(printf '%s' "${line#"### "}" | sed 's/[[:space:]]*$//' | tr '[:upper:]' '[:lower:]')"
    block_names+=("${h}")
    block_text+=("${line}")
    cur=$((${#block_names[@]} - 1))
    continue
  fi
  if [[ "${cur}" -lt 0 ]]; then
    preamble+=("${line}")
  else
    block_text[cur]+=$'\n'"${line}"
  fi
done

# fragment_text <section>: every fragment for it, in order, trailing blank
# lines removed from each.
fragment_text() {
  local e num suf sec path body out=""
  for e in "${sorted[@]}"; do
    read -r num suf sec path <<<"${e}"
    [[ "${sec}" == "$1" ]] || continue
    body="$(cat "${path}")"
    while [[ "${body}" == *$'\n' ]]; do body="${body%$'\n'}"; done
    if [[ -z "${out}" ]]; then out="${body}"; else out+=$'\n'"${body}"; fi
  done
  printf '%s' "${out}"
}

for s in "${order[@]}"; do
  add="$(fragment_text "${s}")"
  [[ -z "${add}" ]] && continue
  found=-1
  for i in "${!block_names[@]}"; do
    [[ "${block_names[i]}" == "${s}" ]] && found="${i}" && break
  done
  if [[ "${found}" -ge 0 ]]; then
    text="${block_text[found]}"
    trail=""
    while [[ "${text}" == *$'\n' ]]; do
      text="${text%$'\n'}"
      trail+=$'\n'
    done
    # The heading line alone gets a blank line before the first bullet.
    [[ "${text}" != *$'\n'* ]] && text+=$'\n'
    block_text[found]="${text}"$'\n'"${add}${trail}"
    continue
  fi
  new="### $(title_of "${s}")"$'\n\n'"${add}"$'\n'
  r="$(rank_of "${s}")"
  at="${#block_names[@]}"
  for i in "${!block_names[@]}"; do
    if [[ "$(rank_of "${block_names[i]}")" -gt "${r}" ]]; then
      at="${i}"
      break
    fi
  done
  block_names=("${block_names[@]:0:at}" "${s}" "${block_names[@]:at}")
  block_text=("${block_text[@]:0:at}" "${new}" "${block_text[@]:at}")
done

# --- write the result, then delete what was folded ---------------------------

tmp="$(mktemp "${changelog}.assemble.XXXXXX")" || {
  echo "${me}: cannot create a temporary file beside ${changelog}" >&2
  exit 2
}
# mktemp creates the file 0600; copying the original over it first carries
# CHANGELOG.md's own mode across the mv below.
cp -p "${changelog}" "${tmp}" || {
  rm -f "${tmp}"
  echo "${me}: cannot copy ${changelog} to ${tmp}" >&2
  exit 2
}
{
  for ((i = 0; i <= u_start; i++)); do printf '%s\n' "${lines[i]}"; done
  last="${lines[u_start]}"
  # The +-guard keeps an empty array legal under set -u on bash 3.2 (macOS).
  for line in ${preamble[@]+"${preamble[@]}"}; do
    printf '%s\n' "${line}"
    last="${line}"
  done
  for i in "${!block_text[@]}"; do
    [[ -n "${last}" ]] && printf '\n'
    printf '%s\n' "${block_text[i]}"
    last="${block_text[i]##*$'\n'}"
  done
  if [[ "${u_end}" -lt "${#lines[@]}" && -n "${last}" ]]; then
    printf '\n'
  fi
  for ((i = u_end; i < ${#lines[@]}; i++)); do printf '%s\n' "${lines[i]}"; done
} >"${tmp}" || {
  rm -f "${tmp}"
  echo "${me}: cannot write ${tmp}" >&2
  exit 2
}
if ! mv "${tmp}" "${changelog}"; then
  rm -f "${tmp}"
  echo "${me}: cannot replace ${changelog}" >&2
  exit 2
fi

for e in "${sorted[@]}"; do
  read -r _ _ _ path <<<"${e}"
  rm -f "${path}" || {
    echo "${me}: folded but could not delete ${path}; delete it by hand" >&2
    exit 2
  }
done
echo "${me}: folded ${#sorted[@]} fragment(s) into ${changelog}"
exit 0
