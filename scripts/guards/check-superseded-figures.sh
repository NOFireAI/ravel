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
# ADR-1196 superseded: whole-object 486.0 s and 463.79 GB against ranged
# 285.8 s and 150.28 GB. Each is matched on a numeric word boundary, so a
# longer number that merely contains one of them (1486.0, 486.02, 486.0.5) is
# not a hit, while a figure that ends a sentence (486.0.) still is. A line that
# quotes two retired figures names both of them.
#
# Usage:
#   scripts/guards/check-superseded-figures.sh [path ...]
#     default: docs/guides
#
# Exit 0 clean, 1 on findings, 2 when the anchor the guard rests on is gone (no
# page found, or docs/guides/cost-model.md not among the scanned pages, either
# of which would leave this scan passing everything), 64 on bad usage, 70 when
# awk itself failed. awk gets its own code because a crashed awk exits 2 on
# this platform: a scan that never ran must not read as a missing anchor, and
# must never read as a clean tree. Findings print as
# `file:line: superseded-figure: explanation`.
#
# Escape hatch, per finding. The marker is
#
#   superseded-figure-allow: <reason>
#
# and it suppresses when it sits on the flagged line itself or anywhere in the
# contiguous block of lines immediately above it. Three forms carry it:
#
#   a bare marker line
#   <!-- superseded-figure-allow: <reason> -->
#   a marker line inside a multi-line <!-- ... --> comment, where the comment
#   closes on the line directly above the flagged line
#
# The reason is required: a marker without one does not suppress. Because the
# marker lands on a page the documentation gate also scans, the reason must
# carry no `ADR-NNNN` token and no `#<issue>` number, or scripts/check_docs.py
# fails that page (TRACKER rule). A reason carrying either does not suppress,
# and the finding says why. Cite the record in prose instead.
#
# SUPERSEDED_FIGURES_AWK names the awk to run, so the cases can drive the
# awk-failure path.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

# Retired figures, one per line as `figure|explanation`. The explanation is
# printed after the `superseded-figure:` label, so it names the figure and why
# it is retired. These reasons are the text an author is most likely to paste
# into a marker, so they carry no tracker token either.
retired_table='463.79|463.79 GB is a retired whole-object transfer-bytes figure, superseded by a later measurement; the live ratio is in the cost-model guide, under Background
486.0|486.0 s is a retired whole-object wall-clock figure, superseded by a later measurement; the live ratio is in the cost-model guide, under Background
150.28|150.28 GB is a retired ranged transfer-bytes figure, superseded by a later measurement; the live ratio is in the cost-model guide, under Background
285.8|285.8 s is a retired ranged wall-clock figure, superseded by a later measurement; the live ratio is in the cost-model guide, under Background'

# The page the guard anchors on: if the scan cannot see it, the scan is looking
# at the wrong tree and must fail rather than pass everything.
anchor_page="cost-model.md"

# awk's own code for the anchor-gone case. It is not 2, because awk exits 2 on
# a syntax error and on an unreadable input file; the shell maps this to 2 and
# everything else it does not know to 70.
awk_anchor_rc=3

awk_bin="${SUPERSEDED_FIGURES_AWK:-awk}"

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,50p' "$0"
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
# 0 no marker, 1 a marker with a usable reason, 2 a marker with no reason,
# 3 a marker whose reason carries a tracker token the documentation gate
# rejects. Only 1 suppresses.
function marker_code(s,   i, rest, p) {
  i = index(s, MARKER)
  if (i == 0) return 0
  rest = substr(s, i + length(MARKER))
  p = index(rest, "-->")
  if (p > 0) rest = substr(rest, 1, p - 1)
  gsub(/^[ \t]+/, "", rest)
  gsub(/[ \t]+$/, "", rest)
  if (rest == "") return 2
  if (rest ~ /ADR-[0-9]+/ || rest ~ /#[0-9]+/) return 3
  return 1
}
# 1 when the last <!-- on the line has no --> after it, so the comment runs on
# into the next line.
function opens_comment(s,   o, c, rest) {
  rest = s
  while ((o = index(rest, "<!--")) > 0) {
    rest = substr(rest, o + 4)
    c = index(rest, "-->")
    if (c == 0) return 1
    rest = substr(rest, c + 3)
  }
  return 0
}
BEGIN {
  MARKER = "superseded-figure-allow:"
  ANCHOR = ENVIRON["SUPERSEDED_FIGURES_ANCHOR"]
  ANCHOR_RC = ENVIRON["SUPERSEDED_FIGURES_ANCHOR_RC"] + 0
  NOTE = " (the superseded-figure-allow reason carries an ADR-NNNN or" \
         " #NNNN token, which the documentation gate rejects, so that marker" \
         " does not suppress)"
  nrows = split(ENVIRON["SUPERSEDED_FIGURES_TABLE"], rows, "\n")
  nfig = 0
  for (r = 1; r <= nrows; r++) {
    if (rows[r] == "") continue
    p = index(rows[r], "|")
    if (p == 0) continue
    nfig++
    fig[nfig] = substr(rows[r], 1, p - 1)
    why[nfig] = substr(rows[r], p + 1)
    esc = fig[nfig]
    # A bare dot is a wildcard, so 486.0 would match 486x0.
    gsub(/\./, "[.]", esc)
    # Numeric word boundary: not preceded by a digit or a dot, and followed by
    # neither a digit nor a dot that a digit follows, so a longer number
    # containing this one (486.02, 486.0.5) is not a hit while a figure that
    # ends a sentence (486.0.) still is. It is matched against the line padded
    # with a space at each end, so the boundary needs no ^ or $ inside an
    # alternation: not every awk reads an anchor there as an anchor.
    rex[nfig] = "[^0-9.]" esc "([^0-9.]|[.][^0-9])"
  }
  anchor_rex = ANCHOR
  gsub(/\./, "[.]", anchor_rex)
  # Matched against the filename with a leading slash, for the same reason.
  anchor_rex = "/" anchor_rex "$"
  findings = 0
  npages = 0
  saw_anchor = 0
}
FNR == 1 {
  npages++
  if (("/" FILENAME) ~ anchor_rex) saw_anchor = 1
  ctx_ok = 0
  ctx_tok = 0
  cmt = 0
  cmt_ok = 0
  cmt_tok = 0
}
{
  line = $0
  padded = " " line " "
  mc = marker_code(line)
  line_ok = (mc == 1)
  line_tok = (mc == 3)

  if (!line_ok && !ctx_ok) {
    note = (line_tok || ctx_tok) ? NOTE : ""
    # Every retired figure on the line is named: stopping at the first one
    # hides the second from the author fixing the page.
    for (k = 1; k <= nfig; k++) {
      if (padded ~ rex[k]) {
        printf "%s:%d: superseded-figure: %s%s\n", FILENAME, FNR, why[k], note
        findings++
      }
    }
  }

  # Comment state. A marker inside a multi-line comment has to survive the
  # lines that close the comment, or the marker form the header documents
  # would never reach the line below it.
  is_cpart = 0
  if (cmt) {
    is_cpart = 1
    if (line_ok) cmt_ok = 1
    if (line_tok) cmt_tok = 1
    c = index(line, "-->")
    if (c > 0) {
      next_cmt = opens_comment(substr(line, c + 3))
      next_cmt_ok = next_cmt ? line_ok : 0
      next_cmt_tok = next_cmt ? line_tok : 0
    } else {
      next_cmt = 1
      next_cmt_ok = cmt_ok
      next_cmt_tok = cmt_tok
    }
  } else if (opens_comment(line)) {
    is_cpart = 1
    cmt_ok = line_ok
    cmt_tok = line_tok
    next_cmt = 1
    next_cmt_ok = cmt_ok
    next_cmt_tok = cmt_tok
  } else {
    next_cmt = 0
    next_cmt_ok = 0
    next_cmt_tok = 0
  }

  # The block above the next line: a marker line extends it, a line of an
  # open comment carries whatever that comment holds, anything else ends it.
  if (line_ok) {
    ctx_ok = 1
    ctx_tok = 0
  } else if (is_cpart) {
    ctx_ok = (ctx_ok || cmt_ok) ? 1 : 0
    ctx_tok = (ctx_tok || cmt_tok) ? 1 : 0
  } else {
    ctx_ok = 0
    ctx_tok = line_tok ? 1 : 0
  }

  cmt = next_cmt
  cmt_ok = next_cmt_ok
  cmt_tok = next_cmt_tok
}
END {
  if (npages == 0) {
    printf "check-superseded-figures.sh: no pages scanned; the anchor is" \
           " gone (issue #1736)\n" > "/dev/stderr"
    exit ANCHOR_RC
  }
  if (!saw_anchor) {
    printf "check-superseded-figures.sh: %s not among the %d scanned pages;" \
           " the anchor is gone, refusing to pass everything (issue #1736)\n", \
           ANCHOR, npages > "/dev/stderr"
    exit ANCHOR_RC
  }
  if (findings > 0) exit 1
  exit 0
}
'

export SUPERSEDED_FIGURES_TABLE="${retired_table}"
export SUPERSEDED_FIGURES_ANCHOR="${anchor_page}"
export SUPERSEDED_FIGURES_ANCHOR_RC="${awk_anchor_rc}"

"${awk_bin}" "${awk_prog}" "${pages[@]}"
rc=$?

if [[ ${rc} -eq 0 ]]; then
  echo "check-superseded-figures.sh: no retired figure appears under ${roots[*]}"
  exit 0
fi
if [[ ${rc} -eq 1 ]]; then
  exit 1
fi
if [[ ${rc} -eq ${awk_anchor_rc} ]]; then
  exit 2
fi
echo "check-superseded-figures.sh: awk (${awk_bin}) exited ${rc}; the scan did" \
     "not run, so this is neither a clean tree nor a missing anchor" \
     "(issue #1736)" >&2
exit 70
