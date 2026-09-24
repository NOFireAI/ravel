#!/usr/bin/env bash
# Amendment-integrity guard (issue #1985).
#
# An ADR amendment section sometimes makes a claim about its own effect on
# the rest of the document: "the role table, section 2 and section 3 now
# carry an inline pointer to this amendment", "qualified everywhere it
# appears". Review has repeatedly found the edit only reached one of the
# named places; docs/adrs/0055-storage-credential-scoping.md alone produced
# nine such findings before this guard existed. This checks the claim
# against the document instead of trusting the prose.
#
# Every `## Amendment` heading's own block (the heading line up to the next
# heading of level <= 2, or end of file) must carry at least one marker, an
# HTML comment naming what the amendment did:
#
#   <!-- amendment-applies: none -->
#     the amendment makes no claim that another section now points back to
#     it. No further check runs for this marker.
#
#   <!-- amendment-applies: sections="Heading A|Heading B" pointer="TEXT" -->
#     each named heading (exact text, found once elsewhere in the same
#     file) must contain TEXT somewhere in its own section span (that
#     heading up to the next heading at the same or a shallower level, or
#     end of file).
#
#   <!-- amendment-supersedes: phrase="old wording" pointer="TEXT" -->
#     `phrase` must not appear anywhere else in the file unless the line it
#     appears on, or the line directly above it, contains TEXT or a
#     `amendment-supersedes-allow: <reason>` marker with a non-empty
#     reason.
#
# A block may carry more than one marker (an amendment can both add
# pointers to named sections and retire a phrase). `pointer` is deliberately
# free text rather than a generated anchor, so it can be the same
# parenthetical an author already writes in the pointing prose (this repo's
# ADRs already write "(2026-09-23 amendment below)" at the far end of a
# pointer; `pointer="2026-09-23 amendment"` matches that as a substring).
#
# Usage:
#   scripts/guards/check-amendment-integrity.sh [dir]
#     default: docs/adrs
#
# Exit 0 clean, 1 finding(s), 64 bad usage, 70 could not check: an amendment
# heading with no marker, a marker missing a required attribute, a named
# section heading that does not exist (or exists more than once), or zero
# ADRs/amendments scanned. 70 rather than a silent 0, because a scan that
# did not run and a clean tree are different answers, and this guard exists
# precisely to stop a false claim from reading as clean.
set -uo pipefail

if [[ $# -gt 1 ]]; then
  echo "check-amendment-integrity.sh: takes at most one argument (got: $*)" >&2
  exit 64
fi

root_arg="${1:-docs/adrs}"

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 70

exec python3 - "${repo_root}" "${root_arg}" <<'PY'
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
docs_dir = root / sys.argv[2]

HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$")
AMENDMENT_RE = re.compile(r"^Amendment\b")
APPLIES_RE = re.compile(r"<!--\s*amendment-applies:\s*(.*?)\s*-->")
SUPERSEDES_RE = re.compile(r"<!--\s*amendment-supersedes:\s*(.*?)\s*-->")
ALLOW_RE = re.compile(r"amendment-supersedes-allow:\s*(.*?)\s*(?:-->)?\s*$")
ATTR_RE = re.compile(r'(\w+)="([^"]*)"')


def die(msg: str) -> None:
    print(f"check-amendment-integrity.sh: {msg}", file=sys.stderr)
    print(
        "  Refusing to report a result: a clean scan and a scan that could "
        "not run are different answers.",
        file=sys.stderr,
    )
    raise SystemExit(70)


def parse_attrs(body: str) -> dict[str, str]:
    return {k: v for k, v in ATTR_RE.findall(body)}


class Doc:
    def __init__(self, rel: str, text: str):
        self.rel = rel
        self.lines = text.splitlines()
        self.headings: list[tuple[int, int, str]] = []  # (line_idx, level, text)
        for idx, line in enumerate(self.lines):
            m = HEADING_RE.match(line)
            if m:
                self.headings.append((idx, len(m.group(1)), m.group(2)))

    def span(self, heading_idx: int) -> tuple[int, int]:
        """Line range [start, end) for the heading at self.headings[heading_idx]."""
        start, level, _ = self.headings[heading_idx]
        end = len(self.lines)
        for j in range(heading_idx + 1, len(self.headings)):
            hline, hlevel, _ = self.headings[j]
            if hlevel <= level:
                end = hline
                break
        return start, end

    def find_section(self, name: str) -> tuple[int, int] | None:
        matches = [i for i, (_, _, text) in enumerate(self.headings) if text == name]
        if len(matches) == 0:
            return None
        if len(matches) > 1:
            die(
                f"{self.rel}: section heading {name!r} named by an "
                f"amendment-applies marker occurs {len(matches)} times; "
                "cannot tell which one the pointer must reach"
            )
        return self.span(matches[0])


if not docs_dir.is_dir():
    die(f"no such directory: {docs_dir}")

paths = sorted(docs_dir.glob("*.md"))
if not paths:
    die(f"no ADR files found under {docs_dir}")

findings: list[str] = []
amendments_scanned = 0
docs_scanned = 0

for path in paths:
    rel = str(path.relative_to(root))
    doc = Doc(rel, path.read_text())
    docs_scanned += 1

    amendment_idxs = [
        i
        for i, (_, level, text) in enumerate(doc.headings)
        if level == 2 and AMENDMENT_RE.match(text)
    ]

    for hi in amendment_idxs:
        amendments_scanned += 1
        start, end = doc.span(hi)
        heading_line, _, heading_text = doc.headings[hi]
        block = doc.lines[start:end]
        block_text = "\n".join(block)

        applies_markers = APPLIES_RE.findall(block_text)
        supersedes_markers = SUPERSEDES_RE.findall(block_text)

        if not applies_markers and not supersedes_markers:
            die(
                f"{rel}:{heading_line + 1}: amendment {heading_text!r} carries "
                "no amendment-applies or amendment-supersedes marker"
            )

        for raw in applies_markers:
            if raw.strip() == "none":
                continue
            attrs = parse_attrs(raw)
            sections = attrs.get("sections")
            pointer = attrs.get("pointer")
            if sections is None or pointer is None or not pointer:
                die(
                    f"{rel}:{heading_line + 1}: amendment-applies marker on "
                    f"{heading_text!r} is missing sections= or pointer="
                )
            for name in sections.split("|"):
                name = name.strip()
                if not name:
                    continue
                sec_span = doc.find_section(name)
                if sec_span is None:
                    die(
                        f"{rel}:{heading_line + 1}: amendment {heading_text!r} "
                        f"names section {name!r}, which has no matching heading"
                    )
                sec_start, sec_end = sec_span
                sec_text = "\n".join(doc.lines[sec_start:sec_end])
                if pointer not in sec_text:
                    findings.append(
                        f"{rel}:{sec_start + 1}: section {name!r} does not "
                        f"carry the pointer {pointer!r} claimed by amendment "
                        f"{heading_text!r} ({rel}:{heading_line + 1})"
                    )

        for raw in supersedes_markers:
            attrs = parse_attrs(raw)
            phrase = attrs.get("phrase")
            pointer = attrs.get("pointer")
            if not phrase or not pointer:
                die(
                    f"{rel}:{heading_line + 1}: amendment-supersedes marker on "
                    f"{heading_text!r} is missing phrase= or pointer="
                )
            for k, line in enumerate(doc.lines):
                if start <= k < end:
                    continue
                if phrase not in line:
                    continue
                above = doc.lines[k - 1] if k > 0 else ""
                qualified = pointer in line or pointer in above
                allow = ALLOW_RE.search(line) or ALLOW_RE.search(above)
                allowed = bool(allow and allow.group(1).strip())
                if not qualified and not allowed:
                    findings.append(
                        f"{rel}:{k + 1}: retired phrase {phrase!r} (superseded "
                        f"by amendment {heading_text!r}, {rel}:{heading_line + 1}) "
                        "appears without its pointer or an "
                        "amendment-supersedes-allow marker"
                    )

if amendments_scanned == 0:
    die(f"no '## Amendment' headings found under {docs_dir}")

if findings:
    for f in sorted(findings):
        print(f)
    print(f"check-amendment-integrity.sh: {len(findings)} finding(s)", file=sys.stderr)
    raise SystemExit(1)

print(
    f"check-amendment-integrity.sh: clean ({amendments_scanned} amendment(s) "
    f"over {docs_scanned} file(s))"
)
PY
