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
# docs/adrs/README.md, "Amending an ADR", is the author-facing copy of the
# marker syntax below, with one example per kind.
#
# WHAT COUNTS AS AN AMENDMENT HEADING
#
# Any heading at level 2 or deeper (`##` through `######`, so every heading
# below the document title) whose text contains a word starting "amend" in
# any case: "Amendment", "Amendments", "Amended 2026-09-02", "Proposed
# amendment", "#### Amendment, 2026-08-26". Nothing is excluded by
# spelling or by level, because every narrower rule tried here skipped a
# real amendment section silently.
#
# One structural exclusion, and it is not silent: a recognised heading that
# sits inside another recognised amendment's block belongs to that block
# (a `### Consequences (amendment)` under `## Amendment: ...` is part of
# that amendment, not a second one), so the enclosing amendment's markers
# speak for it.
#
# MARKERS
#
# Every amendment block (its heading line up to the next heading at the
# same or a shallower level, or end of file) must carry at least one
# marker, an HTML comment naming what the amendment did:
#
#   <!-- amendment-applies: none reason="TEXT" -->
#     the amendment retires no earlier wording in this document: it adds a
#     decision, records an outcome, or documents something the document did
#     not cover. The reason says why nothing is retired, and an empty or
#     missing reason is a finding. `none` is the one marker that turns the
#     checks off, so it is the one that has to justify itself.
#
#   <!-- amendment-applies: sections="Heading A|Heading B" pointer="TEXT" -->
#     each named heading (exact text, found once elsewhere in the same
#     file) must contain TEXT somewhere in its own section span (that
#     heading up to the next heading at the same or a shallower level, or
#     end of file), with any amendment block nested inside that span cut
#     out first: an ADR that writes its amendments as `####` under the
#     decision they amend would otherwise have every pointer satisfied by
#     the amendment's own prose, which is the claim being checked rather
#     than evidence for it. A heading nested inside another amendment's own
#     block (a `### Decision` recounting the original one, say) does not
#     count as a match either: only the document's own structure, or
#     another amendment's top-level heading, is something a pointer can be
#     sent to. `|` separates the names, so a heading carrying one in its
#     own text is named with `\|` (ADR-0996's "2. The fetch policy:
#     `request-minimal \| byte-minimal \| cost-based`").
#
#   <!-- amendment-supersedes: phrase="old wording" pointer="TEXT" -->
#     `phrase` must not appear anywhere outside this amendment's own block
#     unqualified. A match is qualified when TEXT appears in its
#     QUALIFYING WINDOW: the lines the match itself spans, plus the line
#     immediately above the first and the line immediately below the last.
#     That window is what a pointer written into the same sentence reaches,
#     including when the sentence wraps. An
#     `amendment-supersedes-allow: <reason>` marker with a non-empty reason
#     anywhere in the same window qualifies a match too, for prose that
#     cites the retired wording on purpose.
#
# Both `pointer` and `phrase` are matched on whitespace-collapsed,
# case-insensitive text, so a phrase or a pointer split across a line wrap
# still matches, and "Sized independently" still matches "sized
# independently". Marker comments themselves are not prose: they are
# excluded from both the search text and the qualifying window, so one
# amendment's marker cannot qualify another amendment's retired phrase.
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
#     default: docs/adrs   (README.md there is an index, not an ADR)
#
# Exit 0 clean, 1 finding(s), 64 bad usage, 70 could not check. A finding is
# something the document says that is not true of the document: a named
# section without its pointer, a retired phrase still standing unqualified,
# a `none` marker with no reason. 70 is a claim that cannot be checked at
# all: an amendment heading with no marker, a marker missing a required
# attribute or carrying an empty `sections=`, a named section heading that
# does not exist or exists more than once, no ADR files, or zero amendments
# scanned. 70 rather than a silent 0, because a scan that did not run and a
# clean tree are different answers, and this guard exists precisely to stop
# a false claim from reading as clean.
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
AMENDMENT_RE = re.compile(r"\bamend", re.IGNORECASE)
APPLIES_RE = re.compile(r"<!--\s*amendment-applies:\s*(.*?)\s*-->")
SUPERSEDES_RE = re.compile(r"<!--\s*amendment-supersedes:\s*(.*?)\s*-->")
ALLOW_RE = re.compile(r"amendment-supersedes-allow:\s*(.*?)\s*(?:-->)?\s*$")
ATTR_RE = re.compile(r'(\w+)="([^"]*)"')
NONE_RE = re.compile(r"^none\b")
MARKER_LINE_RE = re.compile(r"<!--\s*amendment-")
SECTION_SPLIT_RE = re.compile(r"(?<!\\)\|")
WS_RE = re.compile(r"\s+")

SYNTAX_HINT = (
    "  Marker syntax: docs/adrs/README.md, \"Amending an ADR\"."
)

problems: list[str] = []
findings: list[str] = []


def problem(msg: str) -> None:
    problems.append(f"check-amendment-integrity.sh: {msg}")


def norm(text: str) -> str:
    return WS_RE.sub(" ", text).strip().lower()


def parse_attrs(body: str) -> dict[str, str]:
    return {k: v for k, v in ATTR_RE.findall(body)}


def is_amendment(level: int, text: str) -> bool:
    return level >= 2 and bool(AMENDMENT_RE.search(text))


class Flat:
    """Whitespace-collapsed, lowercased view of a document's prose, with a
    map from each character back to the line it came from. Marker comments
    are dropped: they are metadata about the document, not text the document
    asserts."""

    def __init__(self, lines: list[str]):
        parts: list[str] = []
        owner: list[int] = []
        for k, line in enumerate(lines):
            piece = "" if MARKER_LINE_RE.search(line) else norm(line)
            if not piece:
                continue
            if parts:
                parts.append(" ")
                owner.append(k)
            parts.append(piece)
            owner.extend([k] * len(piece))
        self.text = "".join(parts)
        self.owner = owner

    def occurrences(self, needle: str):
        """Yield (first_line, last_line) for each occurrence of `needle`."""
        if not needle:
            return
        at = self.text.find(needle)
        while at != -1:
            yield self.owner[at], self.owner[at + len(needle) - 1]
            at = self.text.find(needle, at + 1)


class Doc:
    def __init__(self, rel: str, text: str):
        self.rel = rel
        self.lines = text.splitlines()
        self.flat = Flat(self.lines)
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

    def amendment_idxs(self) -> list[int]:
        """Amendment headings that are not nested inside another one."""
        recognised = [
            i for i, (_, level, text) in enumerate(self.headings) if is_amendment(level, text)
        ]
        spans = {i: self.span(i) for i in recognised}
        top: list[int] = []
        for i in recognised:
            line = self.headings[i][0]
            if any(spans[o][0] < line < spans[o][1] for o in recognised if o != i):
                continue
            top.append(i)
        return top

    def amendment_spans(self) -> list[tuple[int, int]]:
        return [self.span(i) for i in self.amendment_idxs()]

    def find_section(self, name: str, whose: str) -> tuple[int, int] | None:
        """A heading matching `name`, excluding one nested inside another
        amendment's own block (an amendment's internal subheadings, such as a
        `### Decision` recounting the original one, are not a document
        section a pointer can be sent to; the amendment's own top-level
        heading remains a valid target). Returns None when there is no such
        heading, and records a problem when there is more than one."""
        spans = self.amendment_spans()
        matches = []
        for i, (line, level, text) in enumerate(self.headings):
            if text != name:
                continue
            nested = any(s < line < e for s, e in spans) and not is_amendment(level, text)
            if nested:
                continue
            matches.append(i)
        if len(matches) == 0:
            return None
        if len(matches) > 1:
            problem(
                f"{self.rel}: section heading {name!r} named by the "
                f"amendment-applies marker on {whose!r} occurs "
                f"{len(matches)} times; cannot tell which one the pointer "
                "must reach"
            )
            return None
        return self.span(matches[0])

    def section_prose(self, sec_start: int, sec_end: int) -> str:
        """The section's own text, with any amendment block nested inside it
        cut out. An amendment that sits under the decision it amends is part
        of that heading's span, and letting it answer for the pointer would
        make the marker prove itself."""
        cut = [
            (s, e)
            for s, e in self.amendment_spans()
            if s > sec_start and e <= sec_end
        ]
        kept = [
            line
            for k, line in enumerate(self.lines[sec_start:sec_end], start=sec_start)
            if not any(s <= k < e for s, e in cut)
        ]
        return Flat(kept).text

    def qualifying_window(self, first: int, last: int) -> tuple[str, bool]:
        """Normalised prose of the match's own lines plus the line above and
        the line below, and whether an amendment-supersedes-allow marker with
        a reason sits in that window."""
        lo = max(0, first - 1)
        hi = min(len(self.lines), last + 2)
        prose = []
        allowed = False
        for line in self.lines[lo:hi]:
            if MARKER_LINE_RE.search(line):
                allow = ALLOW_RE.search(line)
                if allow and allow.group(1).strip():
                    allowed = True
                continue
            prose.append(line)
        return norm(" ".join(prose)), allowed


if not docs_dir.is_dir():
    problem(f"no such directory: {docs_dir}")
    paths: list[Path] = []
else:
    paths = sorted(p for p in docs_dir.glob("*.md") if p.name != "README.md")
    if not paths:
        problem(f"no ADR files found under {docs_dir}")

amendments_scanned = 0
docs_scanned = 0

for path in paths:
    rel = str(path.relative_to(root))
    doc = Doc(rel, path.read_text())
    docs_scanned += 1

    for hi in doc.amendment_idxs():
        amendments_scanned += 1
        start, end = doc.span(hi)
        heading_line, _, heading_text = doc.headings[hi]
        where = f"{rel}:{heading_line + 1}"
        block_text = "\n".join(doc.lines[start:end])

        applies_markers = APPLIES_RE.findall(block_text)
        supersedes_markers = SUPERSEDES_RE.findall(block_text)

        if not applies_markers and not supersedes_markers:
            problem(
                f"{where}: amendment {heading_text!r} carries no "
                "amendment-applies or amendment-supersedes marker"
            )
            continue

        for raw in applies_markers:
            attrs = parse_attrs(raw)
            if NONE_RE.match(raw.strip()):
                if not attrs.get("reason", "").strip():
                    findings.append(
                        f"{where}: amendment {heading_text!r} is marked "
                        "amendment-applies: none with no reason=; say why it "
                        "retires no earlier wording in this document"
                    )
                continue
            sections = attrs.get("sections")
            pointer = attrs.get("pointer")
            if sections is None or not pointer:
                problem(
                    f"{where}: amendment-applies marker on {heading_text!r} "
                    "is missing sections= or pointer="
                )
                continue
            names = [
                n.replace("\\|", "|").strip()
                for n in SECTION_SPLIT_RE.split(sections)
            ]
            names = [n for n in names if n]
            if not names:
                problem(
                    f"{where}: amendment-applies marker on {heading_text!r} "
                    "names no section: sections= is empty"
                )
                continue
            for name in names:
                sec_span = doc.find_section(name, heading_text)
                if sec_span is None:
                    problem(
                        f"{where}: amendment {heading_text!r} names section "
                        f"{name!r}, which has no matching heading"
                    )
                    continue
                sec_start, sec_end = sec_span
                sec_flat = doc.section_prose(sec_start, sec_end)
                if norm(pointer) not in sec_flat:
                    findings.append(
                        f"{rel}:{sec_start + 1}: section {name!r} does not "
                        f"carry the pointer {pointer!r} claimed by amendment "
                        f"{heading_text!r} ({where})"
                    )

        for raw in supersedes_markers:
            attrs = parse_attrs(raw)
            phrase = attrs.get("phrase")
            pointer = attrs.get("pointer")
            if not phrase or not pointer:
                problem(
                    f"{where}: amendment-supersedes marker on {heading_text!r} "
                    "is missing phrase= or pointer="
                )
                continue
            for first, last in doc.flat.occurrences(norm(phrase)):
                if start <= first < end:
                    continue
                window, allowed = doc.qualifying_window(first, last)
                if norm(pointer) in window or allowed:
                    continue
                findings.append(
                    f"{rel}:{first + 1}: retired phrase {phrase!r} (superseded "
                    f"by amendment {heading_text!r}, {where}) appears without "
                    "its pointer or an amendment-supersedes-allow marker"
                )

if paths and amendments_scanned == 0:
    problem(f"no amendment headings found under {docs_dir}")

for f in sorted(findings):
    print(f)

if problems:
    for p in sorted(problems):
        print(p, file=sys.stderr)
    print(
        "  Refusing to report a result: a clean scan and a scan that could "
        "not run are different answers.",
        file=sys.stderr,
    )
    print(SYNTAX_HINT, file=sys.stderr)
    raise SystemExit(70)

if findings:
    print(f"check-amendment-integrity.sh: {len(findings)} finding(s)", file=sys.stderr)
    print(SYNTAX_HINT, file=sys.stderr)
    raise SystemExit(1)

print(
    f"check-amendment-integrity.sh: clean ({amendments_scanned} amendment(s) "
    f"over {docs_scanned} file(s))"
)
PY
