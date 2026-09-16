#!/usr/bin/env bash
# Superseded-figures guard (issue #1736).
#
# A user guide must not keep a measured figure that a later record superseded.
# When a second pass replaces an earlier one, the earlier pair belongs in its
# own record (the record is correct about its own run), but a guide that still
# quotes it presents a retired number as current and an operator picks a flag
# value from it. This guard refuses any retired figure on a page under the
# guides tree, so a deletion cannot silently come back.
#
# The retired figures live in the table below, each with its own reason. Today
# it holds the four figures of the ADR-0996 store-get-concurrency-256 pass that
# ADR-0996 itself superseded: whole-object 486.0 s and 463.79 GB against ranged
# 285.8 s and 150.28 GB. Each is matched on a numeric word boundary, so a longer
# number that merely contains one of them (1486.0, 486.02) is not a hit.
#
# Usage:
#   scripts/guards/check-superseded-figures.sh [path ...]
#     default: docs/guides
#
# Exit 0 clean, 1 on findings, 2 when the anchor the guard rests on is gone (no
# page found, or docs/guides/cost-model.md not among the scanned pages, either
# of which would leave this scan passing everything), 64 on bad usage. Findings
# print as `file:line: superseded-figure: explanation`.
#
# Escape hatch, per finding, on the flagged line itself or anywhere in the
# contiguous block of marker lines immediately above it:
#
#   superseded-figure-allow: <reason>
#
# The reason is required: the marker without one does not suppress. Because the
# marker lands on a page the documentation gate also scans, its reason must
# carry no `ADR-NNNN` token and no `#<issue>` number, or scripts/check_docs.py
# fails that page (TRACKER rule). Cite the record by prose instead.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

# Retired figures, one per line as `figure|explanation`. The explanation is
# printed after the `superseded-figure:` label, so it names the figure and why
# it is retired.
retired_table='463.79|463.79 GB is ADR-0996 superseded whole-object transfer bytes; issue #1736 deleted the pair from the cost-model guide
486.0|486.0 s is ADR-0996 superseded whole-object wall-clock; issue #1736 deleted the pair from the cost-model guide
150.28|150.28 GB is ADR-0996 superseded ranged transfer bytes; issue #1736 deleted the pair from the cost-model guide
285.8|285.8 s is ADR-0996 superseded ranged wall-clock; issue #1736 deleted the pair from the cost-model guide'

# The page the guard anchors on: if the scan cannot see it, the scan is looking
# at the wrong tree and must fail rather than pass everything.
anchor_page="cost-model.md"

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,31p' "$0"
      exit 0
      ;;
    -*)
      echo "check-superseded-figures.sh: unknown option: $1" >&2
      exit 64
      ;;
    *)
      roots+=("$1")
      shift
      ;;
  esac
done
if [[ ${#roots[@]} -eq 0 ]]; then
  roots=(docs/guides)
fi

for root in "${roots[@]}"; do
  if [[ ! -d "${root}" ]]; then
    echo "check-superseded-figures.sh: no such directory: ${root}" >&2
    exit 64
  fi
done

pages=()
while IFS= read -r -d '' file; do
  pages+=("${file}")
done < <(find "${roots[@]}" -type f -name '*.md' -not -path '*/target/*' -print0 | sort -z)

# No page at all is the anchor-gone case, not a clean pass. Handle it here
# because awk with no file arguments would read stdin and hang.
if [[ ${#pages[@]} -eq 0 ]]; then
  echo "check-superseded-figures.sh: no pages found under ${roots[*]}; the" \
       "anchor is gone, refusing to pass everything (issue #1736)" >&2
  exit 2
fi

awk_prog='
function has_marker(s,   i, rest) {
  i = index(s, MARKER)
  if (i == 0) return 0
  rest = substr(s, i + length(MARKER))
  gsub(/[ \t]/, "", rest)
  return length(rest) > 0
}
BEGIN {
  MARKER = "superseded-figure-allow:"
  nrows = split(TABLE, rows, "\n")
  nfig = 0
  for (r = 1; r <= nrows; r++) {
    if (rows[r] == "") continue
    p = index(rows[r], "|")
    nfig++
    fig[nfig] = substr(rows[r], 1, p - 1)
    why[nfig] = substr(rows[r], p + 1)
    esc = fig[nfig]
    gsub(/\./, "\\.", esc)
    # Numeric word boundary: not preceded by a digit or dot, not followed by a
    # digit, so a longer number containing this one is not a hit.
    rex[nfig] = "(^|[^0-9.])" esc "([^0-9]|$)"
  }
  findings = 0
  npages = 0
  saw_anchor = 0
}
FNR == 1 {
  npages++
  if (FILENAME ~ ("(^|/)" ANCHOR "$")) saw_anchor = 1
  block_marker = 0
}
{
  line = $0
  line_marker = has_marker(line)
  hit = 0
  for (k = 1; k <= nfig; k++) {
    if (line ~ rex[k]) { hit = k; break }
  }
  if (hit && !line_marker && !block_marker) {
    printf "%s:%d: superseded-figure: %s\n", FILENAME, FNR, why[hit]
    findings++
  }
  # A marker line extends the block above; any other line ends it.
  if (line_marker) block_marker = 1
  else block_marker = 0
}
END {
  if (npages == 0) {
    printf "check-superseded-figures.sh: no pages scanned; anchor gone" \
           " (issue #1736)\n" > "/dev/stderr"
    exit 2
  }
  if (!saw_anchor) {
    printf "check-superseded-figures.sh: %s not among the %d scanned pages;" \
           " the anchor is gone, refusing to pass everything (issue #1736)\n", \
           ANCHOR, npages > "/dev/stderr"
    exit 2
  }
  if (findings > 0) exit 1
  exit 0
}
'

awk -v TABLE="${retired_table}" -v ANCHOR="${anchor_page}" "${awk_prog}" "${pages[@]}"
rc=$?
if [[ ${rc} -eq 0 ]]; then
  echo "check-superseded-figures.sh: no retired figure appears under ${roots[*]}"
fi
exit ${rc}
