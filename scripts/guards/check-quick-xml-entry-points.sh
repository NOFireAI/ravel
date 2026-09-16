#!/usr/bin/env bash
# quick-xml entry-point guard (issue #1718).
#
# RUSTSEC-2026-0194 and RUSTSEC-2026-0195 affect quick-xml 0.26.0 and 0.39.4
# (quick-xml 0.41.0 fixes both). deny.toml's ignore comment above the
# "RUSTSEC-2026-0194" line records the real entry points: the direct
# Cargo.lock parents of every affected quick-xml version. This guard makes
# that comment mechanical instead of trusted: it parses Cargo.lock for those
# direct parents and fails unless every one of them is named in the comment
# block, so a dependency bump that adds a new entry point (or moves an
# existing one to a different parent) cannot sit undocumented next to a
# stale comment.
#
# Only DIRECT parents count. A crate that depends on object_store, which in
# turn depends on quick-xml, is not itself scanned: the comment names the
# entry points the ignore rests on, not the whole ancestry chain.
#
# Version disambiguation follows Cargo.lock's own rule: a dependency entry
# reads "quick-xml" (bare) when the lockfile carries exactly one quick-xml
# version, and "quick-xml <version>" when it carries more than one. This
# guard counts the quick-xml package entries in the lock itself rather than
# assuming the multi-version form, so it still matches correctly if every
# affected version were ever the only one left.
#
# Usage:
#   scripts/guards/check-quick-xml-entry-points.sh [cargo-lock] [deny-toml]
#     cargo-lock defaults to Cargo.lock, deny-toml to deny.toml, both
#     resolved relative to the repo root. Tests pass throwaway fixture paths
#     for both.
#
# Exit 0 clean, 1 when a direct parent of an affected quick-xml version is
# not named in the comment block above "RUSTSEC-2026-0194" (the missing
# name(s) print to stdout), 2 when the scan finds zero direct parents at all,
# or when the "RUSTSEC-2026-0194" anchor is not found in deny.toml -- either
# means the scan cannot vouch for anything, which must never read as clean,
# 64 on bad usage, 70 if the underlying scan itself fails.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"

case "${1:-}" in
  -h | --help)
    sed -n '2,34p' "$0"
    exit 0
    ;;
esac

if [[ $# -gt 2 ]]; then
  echo "check-quick-xml-entry-points.sh: unexpected extra argument: $3" >&2
  exit 64
fi

lock="${1:-${repo_root}/Cargo.lock}"
deny="${2:-${repo_root}/deny.toml}"

if [[ ! -f "${lock}" ]]; then
  echo "check-quick-xml-entry-points.sh: no such file: ${lock}" >&2
  exit 64
fi
if [[ ! -f "${deny}" ]]; then
  echo "check-quick-xml-entry-points.sh: no such file: ${deny}" >&2
  exit 64
fi

# The two affected versions. quick-xml 0.41.0 (or later) fixes both
# advisories; update this list only alongside the deny.toml comment it is
# checked against.
AFFECTED_VERSIONS="0.26.0 0.39.4"

parents_file="$(mktemp "${TMPDIR:-/tmp}/quick-xml-parents.XXXXXX")"
trap 'rm -f "${parents_file}"' EXIT

# --- pass 1: find the direct parents of every affected quick-xml version ---
lock_status=0
awk -v affected="${AFFECTED_VERSIONS}" '
{ raw[NR] = $0 }
function pkg_field(line, key,    t) {
  t = line
  sub("^" key " = \"", "", t)
  sub("\".*", "", t)
  return t
}
END {
  nl = NR
  nav = split(affected, av, " ")

  # How many quick-xml package entries does the lock carry? Determines
  # whether dependency entries below are bare ("quick-xml") or qualified
  # ("quick-xml <version>").
  nqx = 0
  sole_ver = ""
  i = 1
  while (i <= nl) {
    if (raw[i] ~ /^\[\[package\]\]/) {
      name = ""; ver = ""
      j = i + 1
      while (j <= nl && raw[j] !~ /^\[\[package\]\]/) {
        if (name == "" && raw[j] ~ /^name = "/) name = pkg_field(raw[j], "name")
        if (ver == "" && raw[j] ~ /^version = "/) ver = pkg_field(raw[j], "version")
        j++
      }
      if (name == "quick-xml") { nqx++; sole_ver = ver }
      i = j
    } else {
      i++
    }
  }
  multiple = (nqx > 1)

  # Pass 2: scan every package block dependencies for a quick-xml
  # reference at an affected version, recording that block name as a
  # direct parent.
  nparents = 0
  i = 1
  while (i <= nl) {
    if (raw[i] ~ /^\[\[package\]\]/) {
      name = ""; indeps = 0
      j = i + 1
      while (j <= nl && raw[j] !~ /^\[\[package\]\]/) {
        if (name == "" && raw[j] ~ /^name = "/) name = pkg_field(raw[j], "name")
        if (raw[j] ~ /^dependencies = \[/) { indeps = 1; j++; continue }
        if (indeps && raw[j] ~ /^\]/) { indeps = 0; j++; continue }
        if (indeps) {
          t = raw[j]
          sub(/^[ \t]*"/, "", t)
          sub(/",?[ \t]*$/, "", t)
          dname = t; dver = ""
          if (t ~ / /) {
            dver = t; sub(/^[^ ]+ /, "", dver)
            dname = t; sub(/ .*/, "", dname)
          }
          if (dname == "quick-xml") {
            match_ver = multiple ? dver : (nqx == 1 ? sole_ver : "")
            for (k = 1; k <= nav; k++) {
              if (match_ver == av[k] && !(name in seen)) {
                seen[name] = 1
                nparents++
                parents[nparents] = name
              }
            }
          }
        }
        j++
      }
      i = j
    } else {
      i++
    }
  }

  if (nparents == 0) {
    print "ZERO_PARENTS" > "/dev/stderr"
    exit 2
  }
  for (k = 1; k <= nparents; k++) print parents[k]
}
' "${lock}" >"${parents_file}" || lock_status=$?

if [[ ${lock_status} -eq 2 ]]; then
  echo "check-quick-xml-entry-points.sh: found zero direct parents of any" >&2
  echo "  affected quick-xml version (${AFFECTED_VERSIONS}) in ${lock}." >&2
  echo "  Refusing to report a result: a Cargo.lock format change, or every" >&2
  echo "  affected version dropping out of the graph, must not read as clean." >&2
  exit 2
elif [[ ${lock_status} -ne 0 ]]; then
  echo "check-quick-xml-entry-points.sh: the Cargo.lock scan failed (awk exit ${lock_status})" >&2
  exit 70
fi

# --- pass 2: the comment block above "RUSTSEC-2026-0194" in deny.toml ------
block_file="$(mktemp "${TMPDIR:-/tmp}/quick-xml-block.XXXXXX")"
trap 'rm -f "${parents_file}" "${block_file}"' EXIT

deny_status=0
awk '
{ raw[NR] = $0 }
END {
  nl = NR
  target = -1
  for (i = 1; i <= nl; i++) {
    if (index(raw[i], "\"RUSTSEC-2026-0194\"") > 0) { target = i; break }
  }
  if (target == -1) {
    print "NO_ANCHOR" > "/dev/stderr"
    exit 1
  }
  i = target - 1
  while (i >= 1) {
    t = raw[i]
    sub(/^[ \t]*/, "", t)
    if (t !~ /^#/) break
    print raw[i]
    i--
  }
}
' "${deny}" >"${block_file}" || deny_status=$?

if [[ ${deny_status} -ne 0 ]]; then
  echo "check-quick-xml-entry-points.sh: no \"RUSTSEC-2026-0194\" line found in ${deny}." >&2
  echo "  Refusing to report a result: the comment the parents are checked" >&2
  echo "  against has moved or been renamed, which must not read as clean." >&2
  exit 2
fi

# --- compare: every direct parent must be named in the comment block ------
missing=()
while IFS= read -r parent; do
  [[ -z "${parent}" ]] && continue
  if ! grep -qF -- "${parent}" "${block_file}"; then
    missing+=("${parent}")
  fi
done <"${parents_file}"

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "check-quick-xml-entry-points.sh: direct parent(s) of an affected" >&2
  echo "  quick-xml version are not named in the comment above" >&2
  echo "  \"RUSTSEC-2026-0194\" in ${deny}:" >&2
  for parent in "${missing[@]}"; do
    echo "${parent}"
  done
  exit 1
fi

parent_list="$(paste -sd, "${parents_file}" 2>/dev/null || tr '\n' ',' <"${parents_file}" | sed 's/,$//')"
echo "check-quick-xml-entry-points.sh: clean (direct parent(s): ${parent_list})"
exit 0
