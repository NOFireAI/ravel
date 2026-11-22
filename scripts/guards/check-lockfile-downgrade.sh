#!/usr/bin/env bash
# Lockfile downgrade guard (issue #4595): refuses a Cargo.lock downgrade in a
# commit range unless a commit in the range names it in a trailer.
#
# Usage:
#   scripts/guards/check-lockfile-downgrade.sh <base-ref> [<head-ref>]
#
# <head-ref> defaults to HEAD. Every Cargo.lock tracked at head is compared
# with the same path at the MERGE BASE of the two refs, so a lockfile change
# that landed on the base branch after the fork point is not read as this
# range's. A lockfile that is new at head, or removed at head, is skipped.
#
# Each lockfile's [[package]] entries are read as (name, source, version).
# Packages with no source line are workspace path members and are skipped.
# For one (name, source), a downgrade is a version W at head that the base
# did not have, below some version V the base had and head no longer has.
# A lower version added beside every base version (two majors side by side)
# is not a downgrade, and neither is a crate removed entirely. Versions are
# ordered by SemVer 2.0.0 precedence: numeric major.minor.patch, then a
# version with a pre-release below the same version without one, pre-release
# identifiers compared field by field (numeric below alphanumeric), build
# metadata ignored.
#
# A commit in the range (merge base excluded, head included) whose message
# carries the trailer line
#   Lockfile-downgrade: <name> <W>
# accepts the downgrade of that crate to that exact version, in every
# lockfile. Any other downgrade fails.
#
# Exit 0: no downgrade, or every downgrade is accepted by a trailer.
# Exit 1: a downgrade no trailer accepts. Each prints the trailer that would.
# Exit 2: usage error.
# Exit 70: could not check (a ref that does not resolve, a git command
#          failing, a lockfile that does not parse). Never a pass.
set -euo pipefail
export LC_ALL=C

me="check-lockfile-downgrade.sh"
repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 70

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  awk 'NR > 1 && !/^#/ { exit } NR > 1' "$0"
  exit 0
fi

if [[ $# -lt 1 || $# -gt 2 || -z "${1:-}" ]]; then
  echo "${me}: usage: ${me} <base-ref> [<head-ref>]" >&2
  exit 2
fi
base_ref="$1"
head_ref="${2:-HEAD}"

base_sha="$(git rev-parse --verify --quiet "${base_ref}^{commit}")" || {
  echo "${me}: cannot resolve base ref: ${base_ref}" >&2
  exit 70
}
head_sha="$(git rev-parse --verify --quiet "${head_ref}^{commit}")" || {
  echo "${me}: cannot resolve head ref: ${head_ref}" >&2
  exit 70
}
merge_base="$(git merge-base "${base_sha}" "${head_sha}")" || {
  echo "${me}: no merge base for ${base_sha} and ${head_sha}" >&2
  exit 70
}
if [[ -z "${merge_base}" ]]; then
  echo "${me}: empty merge base for ${base_sha} and ${head_sha}" >&2
  exit 70
fi

# cmp_num A B: prints -1, 0 or 1 for two digit strings, compared by length
# then lexically so no value overflows shell arithmetic.
cmp_num() {
  local a="$1" b="$2"
  while [[ ${#a} -gt 1 && "${a}" == 0* ]]; do a="${a#0}"; done
  while [[ ${#b} -gt 1 && "${b}" == 0* ]]; do b="${b#0}"; done
  if [[ ${#a} -ne ${#b} ]]; then
    if [[ ${#a} -lt ${#b} ]]; then echo -1; else echo 1; fi
  elif [[ "${a}" == "${b}" ]]; then
    echo 0
  elif [[ "${a}" < "${b}" ]]; then
    echo -1
  else
    echo 1
  fi
}

# cmp_semver A B: prints -1, 0 or 1 by SemVer 2.0.0 precedence.
cmp_semver() {
  local a="${1%%+*}" b="${2%%+*}"
  local a_core="${a%%-*}" b_core="${b%%-*}" a_pre="" b_pre=""
  [[ "${a}" == *-* ]] && a_pre="${a#*-}"
  [[ "${b}" == *-* ]] && b_pre="${b#*-}"
  local -a ac bc ap bp
  IFS=. read -r -a ac <<<"${a_core}"
  IFS=. read -r -a bc <<<"${b_core}"
  local i c
  for i in 0 1 2; do
    c="$(cmp_num "${ac[i]}" "${bc[i]}")"
    if [[ "${c}" != 0 ]]; then echo "${c}"; return; fi
  done
  if [[ -z "${a_pre}" && -z "${b_pre}" ]]; then echo 0; return; fi
  if [[ -z "${a_pre}" ]]; then echo 1; return; fi
  if [[ -z "${b_pre}" ]]; then echo -1; return; fi
  IFS=. read -r -a ap <<<"${a_pre}"
  IFS=. read -r -a bp <<<"${b_pre}"
  local n=${#ap[@]} x y
  [[ ${#bp[@]} -lt ${n} ]] && n=${#bp[@]}
  for ((i = 0; i < n; i++)); do
    x="${ap[i]}"
    y="${bp[i]}"
    if [[ "${x}" =~ ^[0-9]+$ && "${y}" =~ ^[0-9]+$ ]]; then
      c="$(cmp_num "${x}" "${y}")"
    elif [[ "${x}" =~ ^[0-9]+$ ]]; then
      c=-1
    elif [[ "${y}" =~ ^[0-9]+$ ]]; then
      c=1
    elif [[ "${x}" == "${y}" ]]; then
      c=0
    elif [[ "${x}" < "${y}" ]]; then
      c=-1
    else
      c=1
    fi
    if [[ "${c}" != 0 ]]; then echo "${c}"; return; fi
  done
  if [[ ${#ap[@]} -lt ${#bp[@]} ]]; then
    echo -1
  elif [[ ${#ap[@]} -gt ${#bp[@]} ]]; then
    echo 1
  else
    echo 0
  fi
}

# parse_lock: reads a Cargo.lock on stdin and prints one
# "<name>\t<source>\t<version>" line per [[package]], "path" standing in for
# a missing source. Exits non-zero, with the reason on stderr, on anything it
# does not recognise.
parse_lock() {
  awk '
    function fail(msg) { bad = "line " NR ": " msg; exit 1 }
    function flush() {
      if (in_pkg) {
        if (name == "" || ver == "") fail("[[package]] from line " start " has no name or version")
        print name "\t" (src == "" ? "path" : src) "\t" ver
        n++
      }
      in_pkg = 0; in_list = 0; name = ""; ver = ""; src = ""
    }
    function unquote(v) {
      if (v !~ /^"[^"]+"$/) fail("expected a quoted string, got " v)
      return substr(v, 2, length(v) - 2)
    }
    /^\[\[package\]\][ \t]*$/ { flush(); in_pkg = 1; start = NR; next }
    /^\[/ { flush(); in_other = 1; next }
    {
      if (!in_pkg) {
        if (in_other || $0 ~ /^[ \t]*$/ || $0 ~ /^#/ || $0 ~ /^version = [0-9]+$/) next
        fail("unexpected top-level line: " $0)
      }
      if (in_list) {
        if ($0 ~ /^\][ \t]*$/) in_list = 0
        else if ($0 !~ /^ "[^"]*",?$/) fail("unexpected list entry: " $0)
        next
      }
      if ($0 ~ /^[ \t]*$/ || $0 ~ /^#/) next
      if ($0 !~ /^[a-z_-]+ = /) fail("unexpected line in [[package]]: " $0)
      key = $0; sub(/ = .*$/, "", key)
      val = $0; sub(/^[a-z_-]+ = /, "", val)
      if (key == "name") {
        if (name != "") fail("second name in one [[package]]")
        name = unquote(val)
      } else if (key == "version") {
        if (ver != "") fail("second version in one [[package]]")
        ver = unquote(val)
        if (ver !~ /^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$/)
          fail("not a semver version: " ver)
      } else if (key == "source") {
        src = unquote(val)
      } else if (val == "[") {
        in_list = 1
      }
    }
    END {
      if (bad == "" && in_pkg && (name == "" || ver == ""))
        bad = "[[package]] from line " start " has no name or version"
      else if (bad == "")
        flush()
      if (bad == "" && n == 0) bad = "no [[package]] entries"
      if (bad != "") { print bad > "/dev/stderr"; exit 1 }
    }
  '
}

tree_list="$(git ls-tree -r --name-only "${head_sha}")" || {
  echo "${me}: git ls-tree failed at ${head_sha}" >&2
  exit 70
}
locks=()
while IFS= read -r p; do
  [[ "${p}" == Cargo.lock || "${p}" == */Cargo.lock ]] && locks+=("${p}")
done <<<"${tree_list}"

trailers="$(git log --format='%(trailers:only,unfold)' "${merge_base}..${head_sha}")" || {
  echo "${me}: git log failed for ${merge_base}..${head_sha}" >&2
  exit 70
}

# accepted NAME VERSION: whether a trailer line names exactly this downgrade.
accepted() {
  local want="Lockfile-downgrade: $1 $2" line
  while IFS= read -r line; do
    line="${line%"${line##*[![:space:]]}"}"
    [[ "${line}" == "${want}" ]] && return 0
  done <<<"${trailers}"
  return 1
}

found=0
refused=0
checked=0
for lock in "${locks[@]}"; do
  in_base="$(git ls-tree --name-only "${merge_base}" -- "${lock}")" || {
    echo "${me}: git ls-tree failed for ${lock} at ${merge_base}" >&2
    exit 70
  }
  [[ -z "${in_base}" ]] && continue
  base_text="$(git show "${merge_base}:${lock}")" || {
    echo "${me}: git show failed for ${merge_base}:${lock}" >&2
    exit 70
  }
  head_text="$(git show "${head_sha}:${lock}")" || {
    echo "${me}: git show failed for ${head_sha}:${lock}" >&2
    exit 70
  }
  base_pkgs="$(parse_lock <<<"${base_text}" 2>&1)" || {
    echo "${me}: cannot parse ${lock} at ${merge_base}: ${base_pkgs}" >&2
    exit 70
  }
  head_pkgs="$(parse_lock <<<"${head_text}" 2>&1)" || {
    echo "${me}: cannot parse ${lock} at ${head_sha}: ${head_pkgs}" >&2
    exit 70
  }
  checked=$((checked + 1))

  declare -A base_v=() head_v=()
  while IFS=$'\t' read -r name src ver; do
    [[ "${src}" == path ]] && continue
    base_v["${name}"$'\t'"${src}"]+=" ${ver}"
  done <<<"${base_pkgs}"
  while IFS=$'\t' read -r name src ver; do
    [[ "${src}" == path ]] && continue
    head_v["${name}"$'\t'"${src}"]+=" ${ver}"
  done <<<"${head_pkgs}"

  for key in "${!head_v[@]}"; do
    [[ -n "${base_v[${key}]+set}" ]] || continue
    name="${key%%$'\t'*}"
    src="${key#*$'\t'}"
    read -r -a bvs <<<"${base_v[${key}]}"
    read -r -a hvs <<<"${head_v[${key}]}"
    removed=()
    for v in "${bvs[@]}"; do
      [[ " ${head_v[${key}]} " == *" ${v} "* ]] || removed+=("${v}")
    done
    [[ ${#removed[@]} -eq 0 ]] && continue
    for w in "${hvs[@]}"; do
      [[ " ${base_v[${key}]} " == *" ${w} "* ]] && continue
      from=""
      for v in "${removed[@]}"; do
        if [[ "$(cmp_semver "${w}" "${v}")" == -1 ]]; then
          if [[ -z "${from}" || "$(cmp_semver "${v}" "${from}")" == 1 ]]; then
            from="${v}"
          fi
        fi
      done
      [[ -z "${from}" ]] && continue
      found=$((found + 1))
      echo "${lock}: ${name} ${from} -> ${w} (${src})"
      if accepted "${name}" "${w}"; then
        echo "  accepted by trailer 'Lockfile-downgrade: ${name} ${w}'"
      else
        refused=$((refused + 1))
        echo "  NOT accepted; a commit in the range needs the trailer line:"
        echo "    Lockfile-downgrade: ${name} ${w}"
      fi
    done
  done
  unset base_v head_v
done

if [[ "${refused}" -gt 0 ]]; then
  echo "${me}: ${refused} of ${found} Cargo.lock downgrade(s) in ${merge_base:0:12}..${head_sha:0:12} carry no Lockfile-downgrade trailer" >&2
  exit 1
fi
if [[ "${found}" -gt 0 ]]; then
  echo "${me}: clean (${found} downgrade(s), each accepted by a trailer; ${checked} lockfile(s) compared)"
else
  echo "${me}: clean (no downgrade; ${checked} lockfile(s) compared)"
fi
exit 0
