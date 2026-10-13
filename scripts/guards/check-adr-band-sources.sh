#!/usr/bin/env bash
# ADR band-source guard (issue #2879, ADR-2867 change 6).
#
# Every number in an ADR band table names where it came from, so a spec that
# copies a band out of an ADR copies its provenance too, and a reader can tell
# a measured figure from a predicted one. This checks that each band table
# row carries a source token in its basis cell.
#
# WHAT COUNTS AS A BAND TABLE
#
# A markdown table (a `|` row followed by a `|---|` separator row, outside
# fenced code) whose header row contains the whole word band, bar, target or
# miss, in any case. "Bands", "bars", "targets" and "misses" do not count.
# Its data rows are the `|` lines that follow the separator, up to the first
# line that does not start with `|`.
#
# THE BASIS CELL
#
# The cell under the first header column whose whole text is basis, source,
# measured or evidence (case-insensitive, ignoring surrounding `*`, `_` and
# backticks) when the header has one; otherwise the last cell of the row. A
# row with fewer cells than the basis column has an empty basis cell. Cells
# split on every `|` not escaped as `\|`.
#
# SOURCE TOKENS
#
# The basis cell must contain at least one of:
#   - an issue comment link, github.com/<o>/<r>/issues/<n>#issuecomment-<id>
#     (pull/<n> is accepted too);
#   - a bare comment id with the word comment: `comment 6071729407`, or
#     `comments 5854876916 and 5855941650` (the id has at least six digits);
#   - an s3:// path, or a report path starting bench/;
#   - a file:line citation, path.ext:N (the extension starts with a letter,
#     so a ratio such as 12.5:1 is not one);
#   - a stamp `measured YYYY-MM-DD`;
#   - the word predicted, for a figure with no measurement behind it.
#
# EXEMPTION
#
#   <!-- band-sources-allow: <reason> -->
# on the nearest non-blank line above a band table's header row exempts that
# table and no other. The reason is required: an empty reason is a finding,
# and so is a marker line that does not parse or that no band table follows.
#
# Usage:
#   scripts/guards/check-adr-band-sources.sh [dir]
#     default: docs/adrs (README.md there is an index, not an ADR); a
#     relative dir is read from the repository root.
#
# Exit 0 clean, 1 finding(s), 64 bad usage (more than one argument), 70
# could not check: no such directory, no ADR files, no band table anywhere
# in the tree, or a band table header with no data rows. A scan that found
# nothing to check is not a pass.
set -uo pipefail

if [[ $# -gt 1 ]]; then
  echo "check-adr-band-sources.sh: takes at most one argument (got: $*)" >&2
  exit 64
fi

root_arg="${1:-docs/adrs}"

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"

exec python3 - "${repo_root}" "${root_arg}" <<'PY'
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
docs_dir = root / sys.argv[2]

BAND_RE = re.compile(r"\b(?:band|bar|target|miss)\b", re.IGNORECASE)
SEPARATOR_RE = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)*\|?\s*$")
FENCE_RE = re.compile(r"^\s*(```+|~~~+)")
CELL_SPLIT_RE = re.compile(r"(?<!\\)\|")
BASIS_NAMES = {"basis", "source", "measured", "evidence"}
ALLOW_RE = re.compile(r"^\s*<!--\s*band-sources-allow:(.*?)-->\s*$")
ALLOW_MENTION_RE = re.compile(r"band-sources-allow")
SOURCE_RES = [
    re.compile(r"github\.com/[^\s/]+/[^\s/]+/(?:issues|pull)/\d+#issuecomment-\d+"),
    re.compile(r"\bcomments?\s+\d{6,}\b", re.IGNORECASE),
    re.compile(r"s3://\S+"),
    re.compile(r"(?<![\w./-])bench/[\w./-]+"),
    re.compile(r"[\w./-]+\.[A-Za-z][A-Za-z0-9]*:\d+"),
    re.compile(r"\bmeasured\s+\d{4}-\d{2}-\d{2}\b", re.IGNORECASE),
    re.compile(r"\bpredicted\b", re.IGNORECASE),
]

problems: list[str] = []
findings: list[str] = []


def cells(line: str) -> list[str]:
    body = line.strip()
    if body.startswith("|"):
        body = body[1:]
    if body.endswith("|") and not body.endswith("\\|"):
        body = body[:-1]
    return [c.strip() for c in CELL_SPLIT_RE.split(body)]


def header_name(cell: str) -> str:
    return cell.strip().strip("*_`").strip().lower()


def has_source(cell: str) -> bool:
    return any(r.search(cell) for r in SOURCE_RES)


if not docs_dir.is_dir():
    problems.append(f"check-adr-band-sources.sh: no such directory: {docs_dir}")
    paths = []
else:
    paths = sorted(p for p in docs_dir.glob("*.md") if p.name != "README.md")
    if not paths:
        problems.append(f"check-adr-band-sources.sh: no ADR files under {docs_dir}")

tables = 0
rows_checked = 0
exempt = 0

for path in paths:
    try:
        rel = str(path.relative_to(root))
    except ValueError:
        rel = str(path)
    lines = path.read_text(encoding="utf-8").split("\n")

    in_fence = False
    prose: list[bool] = []
    for line in lines:
        if FENCE_RE.match(line):
            in_fence = not in_fence
            prose.append(False)
            continue
        prose.append(not in_fence)

    # Marker lines claimed by a band table below them; every other marker
    # line is reported after the scan.
    claimed: set[int] = set()

    i = 0
    while i < len(lines):
        line = lines[i]
        if not (
            prose[i]
            and line.lstrip().startswith("|")
            and i + 1 < len(lines)
            and prose[i + 1]
            and SEPARATOR_RE.match(lines[i + 1])
        ):
            i += 1
            continue
        header_line = i
        header = cells(line)
        j = i + 2
        data: list[int] = []
        while j < len(lines) and prose[j] and lines[j].lstrip().startswith("|"):
            data.append(j)
            j += 1
        if not BAND_RE.search(line):
            i = j
            continue

        tables += 1
        where = f"{rel}:{header_line + 1}"
        if not data:
            problems.append(
                f"check-adr-band-sources.sh: {where}: band table header has "
                f"no data rows: {line.strip()}"
            )
            i = j
            continue

        above = header_line - 1
        while above >= 0 and not lines[above].strip():
            above -= 1
        if above >= 0 and ALLOW_MENTION_RE.search(lines[above]):
            claimed.add(above)
            m = ALLOW_RE.match(lines[above])
            if m is None:
                findings.append(
                    f"{rel}:{above + 1}: band-sources-allow marker above the "
                    f"table at line {header_line + 1} does not parse (one "
                    "line, <!-- band-sources-allow: <reason> -->)"
                )
            elif not m.group(1).strip():
                findings.append(
                    f"{rel}:{above + 1}: band-sources-allow marker above the "
                    f"table at line {header_line + 1} ({line.strip()}) has no "
                    "reason"
                )
            else:
                exempt += 1
            i = j
            continue

        basis = len(header) - 1
        for k, name in enumerate(header):
            if header_name(name) in BASIS_NAMES:
                basis = k
                break
        basis_name = header[basis] if basis < len(header) else ""

        for r in data:
            rows_checked += 1
            row = cells(lines[r])
            cell = row[basis] if basis < len(row) else ""
            if has_source(cell):
                continue
            first = row[0] if row else ""
            findings.append(
                f"{rel}:{r + 1}: band table at line {header_line + 1} "
                f"({line.strip()}): row {first!r}: basis cell "
                f"{basis_name!r} carries no source token: {cell!r}"
            )
        i = j

    for k, line in enumerate(lines):
        if prose[k] and ALLOW_MENTION_RE.search(line) and k not in claimed:
            findings.append(
                f"{rel}:{k + 1}: band-sources-allow marker with no band "
                "table directly below it exempts nothing"
            )

if paths and tables == 0:
    problems.append(f"check-adr-band-sources.sh: no band table found under {docs_dir}")

HINT = (
    "  A source token is a comment link or `comment <id>`, an s3:// or "
    "bench/ path, a path.ext:N citation, `measured YYYY-MM-DD`, or "
    "`predicted`: see the header of scripts/guards/check-adr-band-sources.sh."
)

for f in findings:
    print(f)

if problems:
    for p in problems:
        print(p, file=sys.stderr)
    print(
        "  Refusing to report a result: a clean scan and a scan that could "
        "not run are different answers.",
        file=sys.stderr,
    )
    raise SystemExit(70)

if findings:
    print(f"check-adr-band-sources.sh: {len(findings)} finding(s)", file=sys.stderr)
    print(HINT, file=sys.stderr)
    raise SystemExit(1)

print(
    f"check-adr-band-sources.sh: clean ({tables} band table(s), "
    f"{rows_checked} row(s) checked, {exempt} exempt, over {len(paths)} file(s))"
)
PY
