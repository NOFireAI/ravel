#!/usr/bin/env bash
# quick-xml entry-point guard (issue #1718).
#
# RUSTSEC-2026-0194 and RUSTSEC-2026-0195 affect every quick-xml version older
# than 0.41.0 (the release that fixes both). deny.toml's ignore comment above
# the "RUSTSEC-2026-0194" line records the real entry points: the direct
# Cargo.lock parents of every affected quick-xml version, each named together
# with its OWN version as it appears in Cargo.lock (for example
# "object_store 0.13.2"). This guard makes that comment mechanical instead of
# trusted: it parses Cargo.lock for the affected quick-xml versions and their
# direct parents, then fails unless every (parent name, parent version) pair
# is named in the comment block, so a dependency bump that adds a new entry
# point, moves an existing one to a different parent, or bumps a named
# parent to a version that newly resolves an affected quick-xml (round 2 of
# issue #1718: object_store 0.14.1 depending on an affected quick-xml would
# otherwise pass unnoticed against a comment still naming 0.13.2) cannot sit
# undocumented next to a stale comment.
#
# The affected set is derived from Cargo.lock itself, not hardcoded: every
# distinct quick-xml version present that is older than 0.41.0. A bump to a
# still-affected 0.40.x is caught the same way 0.26.0/0.39.4 are today.
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
# The comment match is anchored on both sides (a parent name boundary and a
# version boundary), not a bare substring test: a crate literally named
# "store" must not be considered documented by a comment that only mentions
# "object_store 0.13.2" -- "store" is a substring of that text but is not the
# same crate, and the guard must not confuse the two.
#
# Usage:
#   scripts/guards/check-quick-xml-entry-points.sh [cargo-lock] [deny-toml]
#     cargo-lock defaults to Cargo.lock, deny-toml to deny.toml, both
#     resolved relative to the repo root. Tests pass throwaway fixture paths
#     for both.
#
# Exit 0 clean, 1 when a direct (parent, parent-version) pair of an affected
# quick-xml version is not named in the comment block above
# "RUSTSEC-2026-0194" (the missing pair(s) print to stdout), 2 when the scan
# finds zero affected quick-xml versions or zero direct parents of one at
# all, or when the "RUSTSEC-2026-0194" anchor is not found in deny.toml --
# any of these means the scan cannot vouch for anything, which must never
# read as clean, 64 on bad usage, 70 if the underlying scan itself fails.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"

case "${1:-}" in
  -h | --help)
    sed -n '2,50p' "$0"
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

# quick-xml 0.41.0 fixes both advisories; every earlier version is affected.
# This is a fact about the advisories, not about this repo's lock, so it
# stays a literal here even though the SET of affected versions present is
# derived from Cargo.lock below.
FIXED_VERSION="0.41.0"

parents_file="$(mktemp "${TMPDIR:-/tmp}/quick-xml-parents.XXXXXX")"
trap 'rm -f "${parents_file}"' EXIT

# --- pass 1: find the direct (parent, parent-version) pairs of every -------
# --- affected quick-xml version -------------------------------------------
lock_status=0
awk -v fixed="${FIXED_VERSION}" '
{ raw[NR] = $0 }
function pkg_field(line, key,    t) {
  t = line
  sub("^" key " = \"", "", t)
  sub("\".*", "", t)
  return t
}
function ver_cmp(a, b,    an, bn, av, bv, i, ai, bi) {
  an = split(a, av, ".")
  bn = split(b, bv, ".")
  for (i = 1; i <= 3; i++) {
    ai = (i <= an) ? av[i] + 0 : 0
    bi = (i <= bn) ? bv[i] + 0 : 0
    if (ai < bi) return -1
    if (ai > bi) return 1
  }
  return 0
}
END {
  nl = NR

  # Parse every [[package]] block once: name, own version, and its raw
  # dependency-list entries.
  nblocks = 0
  i = 1
  while (i <= nl) {
    if (raw[i] ~ /^\[\[package\]\]/) {
      nblocks++
      bname[nblocks] = ""; bver[nblocks] = ""; ndeps[nblocks] = 0
      indeps = 0
      j = i + 1
      while (j <= nl && raw[j] !~ /^\[\[package\]\]/) {
        if (bname[nblocks] == "" && raw[j] ~ /^name = "/) bname[nblocks] = pkg_field(raw[j], "name")
        if (bver[nblocks] == "" && raw[j] ~ /^version = "/) bver[nblocks] = pkg_field(raw[j], "version")
        if (raw[j] ~ /^dependencies = \[/) { indeps = 1; j++; continue }
        if (indeps && raw[j] ~ /^\]/) { indeps = 0; j++; continue }
        if (indeps) {
          t = raw[j]
          sub(/^[ \t]*"/, "", t)
          sub(/",?[ \t]*$/, "", t)
          ndeps[nblocks]++
          deps[nblocks, ndeps[nblocks]] = t
        }
        j++
      }
      i = j
    } else {
      i++
    }
  }

  # Distinct quick-xml versions present in the lock.
  nqx = 0
  for (b = 1; b <= nblocks; b++) {
    if (bname[b] == "quick-xml" && !(bver[b] in qxseen)) {
      qxseen[bver[b]] = 1
      nqx++
      qxvers[nqx] = bver[b]
      if (nqx == 1) sole_ver = bver[b]
    }
  }
  multiple = (nqx > 1)

  # Affected = present quick-xml versions older than the fixed release.
  naff = 0
  for (k = 1; k <= nqx; k++) {
    if (ver_cmp(qxvers[k], fixed) < 0) { naff++; aff[naff] = qxvers[k] }
  }

  if (naff == 0) {
    print "ZERO_AFFECTED" > "/dev/stderr"
    exit 2
  }

  # Direct parents: blocks whose dependency list contains a quick-xml entry
  # resolving to one of the affected versions.
  nparents = 0
  for (b = 1; b <= nblocks; b++) {
    for (d = 1; d <= ndeps[b]; d++) {
      t = deps[b, d]
      dname = t; dver = ""
      if (t ~ / /) {
        dver = t; sub(/^[^ ]+ /, "", dver)
        dname = t; sub(/ .*/, "", dname)
      }
      if (dname != "quick-xml") continue
      match_ver = multiple ? dver : (nqx == 1 ? sole_ver : "")
      is_aff = 0
      for (k = 1; k <= naff; k++) if (match_ver == aff[k]) is_aff = 1
      if (is_aff && !(b in seenblock)) {
        seenblock[b] = 1
        nparents++
        pname[nparents] = bname[b]
        pver[nparents] = bver[b]
      }
    }
  }

  if (nparents == 0) {
    print "ZERO_PARENTS" > "/dev/stderr"
    exit 2
  }
  for (k = 1; k <= nparents; k++) print pname[k] "\t" pver[k]
}
' "${lock}" >"${parents_file}" || lock_status=$?

if [[ ${lock_status} -eq 2 ]]; then
  echo "check-quick-xml-entry-points.sh: found zero direct parents of any" >&2
  echo "  affected (older than ${FIXED_VERSION}) quick-xml version in ${lock}." >&2
  echo "  Refusing to report a result: a Cargo.lock format change, every" >&2
  echo "  affected version dropping out of the graph, or every remaining" >&2
  echo "  quick-xml entry being unreferenced must not read as clean." >&2
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

# --- compare: every direct (parent, parent-version) pair must be named ----
# anchored on both sides, so a parent name that is only a substring of a
# longer word in the comment (e.g. "store" inside "object_store 0.13.2")
# does not count as documented, and so a parent's OWN version must appear
# next to its name, not just anywhere in the block.
missing=()
while IFS=$'\t' read -r pname pver; do
  [[ -z "${pname}" ]] && continue
  esc_ver="${pver//./\\.}"
  pattern="(^|[^A-Za-z0-9_-])${pname}[[:space:]]+${esc_ver}([^0-9]|\$)"
  if ! grep -qE -- "${pattern}" "${block_file}"; then
    missing+=("${pname} ${pver}")
  fi
done <"${parents_file}"

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "check-quick-xml-entry-points.sh: direct parent(s) of an affected" >&2
  echo "  quick-xml version are not named, together with their own version," >&2
  echo "  in the comment above \"RUSTSEC-2026-0194\" in ${deny}:" >&2
  for parent in "${missing[@]}"; do
    echo "${parent}"
  done
  exit 1
fi

parent_list="$(sed -e 's/\t/ /' "${parents_file}" | paste -sd, - 2>/dev/null || sed -e 's/\t/ /' "${parents_file}" | tr '\n' ',' | sed 's/,$//')"
echo "check-quick-xml-entry-points.sh: clean (direct parent(s): ${parent_list})"
exit 0
