#!/usr/bin/env python3
"""Documentation gate for the Ravel repository (ADR-1040 decision 3).

Walks the repository, classifies every markdown file into a reader scope, and
applies a set of rules that hold the documentation to the architecture ADR-1040
decides. Python 3 standard library only, matching scripts/check_readme_commands.py:
CI's ``doc-scripts`` job installs no toolchain beyond the interpreter.

The tree does not pass on the day this lands, so the checker ships with a
baseline (scripts/docs_lint_baseline.txt) listing every finding that exists at
that moment. A finding present in the baseline is a note; a finding absent from
it fails. Each documentation task deletes the baseline entries it fixes; the
final task deletes the file. See ADR-1040 D3.

Usage:
    python3 scripts/check_docs.py                 # gate: exit 1 on a new finding
    python3 scripts/check_docs.py --update-baseline
    python3 scripts/check_docs.py --strict-baseline  # unused baseline entry fails

Exit codes: 0 clean, 1 findings not in the baseline (or unused entries under
--strict-baseline), 2 the checker could not run (an unreadable file, a missing
source constant).
"""

import argparse
import collections
import os
import re
import sys
import xml.etree.ElementTree as ET

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BASELINE_PATH = os.path.join(REPO_ROOT, "scripts", "docs_lint_baseline.txt")

# Directories that hold no repository documentation and would only slow the walk
# (or, for a vendored dependency, produce findings for code this repo does not own).
_PRUNE_DIRS = {".git", "node_modules", "target", ".venv", "__pycache__"}


class CheckError(Exception):
    """The checker cannot run: an unreadable file or a missing source constant."""


# --------------------------------------------------------------------------
# Scope classification
# --------------------------------------------------------------------------


# The pages a test renders from a clap command definition. Every other page
# under docs/reference/ is written by hand and held to the user-page rules.
GENERATED_PAGES = frozenset({
    "docs/reference/ravel-server-flags.md",
    "docs/reference/ravel-cli-flags.md",
})


def classify(rel):
    """Return the scope of a repo-relative (posix) markdown path, or None.

    A file that matches no scope is not checked by the prose rules; the global
    rules (ORPHAN, SVG, ADRINDEX) reach it through their own file sets.
    """
    if rel == "CHANGELOG.md":
        # A changelog entry naming the decision or the issue behind a shipped
        # change is doing its job, so the tracker rule does not apply. Every
        # other rule does: a source path or a commit hash is still noise here.
        return "changelog"
    if rel in ("README.md", "docs/README.md"):
        return "user"
    if rel.startswith("docs/guides/") and rel.endswith(".md"):
        return "user"
    if rel in GENERATED_PAGES:
        # Rendered from the clap definitions, so its prose is the code's
        # prose. The citation rules would demand editing doc comments to
        # satisfy a documentation gate, which is backwards: the page would
        # drift from the definition it is generated from at the next
        # regeneration. Everything else still applies, including provenance:
        # a source path or a commit hash has no business here either.
        return "generated"
    if rel.startswith("docs/reference/") and rel.endswith(".md"):
        # A hand-written reference page is read the way a guide is read.
        return "user"
    if rel.startswith("docs/adrs/") and rel.endswith(".md"):
        return "history"
    if rel.startswith("docs/internal/") and rel.endswith(".md"):
        return "internal"
    # docs/*.md (the specs) and any other documentation markdown under docs/ that
    # is not a user guide, a decision record, or internal material. docs/README.md
    # is carved out above as user. This catch-all is what puts docs/diagrams/README.md
    # (which carries the "Last verified" stamps) in a scope the provenance rule reaches.
    if rel.startswith("docs/") and rel.endswith(".md"):
        return "spec"
    return None


# --------------------------------------------------------------------------
# Markdown structure: fenced code blocks and headings
# --------------------------------------------------------------------------


def _fence_info(line):
    """Return (char, length, info) if `line` opens/closes a fence, else None."""
    stripped = line.lstrip()
    for char in ("`", "~"):
        if stripped.startswith(char * 3):
            run = len(stripped) - len(stripped.lstrip(char))
            info = stripped[run:].strip()
            return (char, run, info)
    return None


_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$")


def iter_prose_lines(lines):
    """Yield (lineno, text, current_heading) for every line outside a code fence.

    `current_heading` is the raw text of the most recent ATX heading, so a rule
    with a section-scoped exception (TRACKER's Background carve-out) can see it.
    Lines inside a fenced code block are not yielded: a rule that bans a phrase in
    prose must not fire on a code sample that legitimately contains it.
    """
    in_fence = False
    fence_char = None
    fence_len = 0
    current_heading = None
    for i, line in enumerate(lines, start=1):
        info = _fence_info(line)
        if in_fence:
            if info is not None:
                c, n, tail = info
                if c == fence_char and n >= fence_len and tail == "":
                    in_fence = False
            continue
        if info is not None:
            in_fence = True
            fence_char, fence_len, _ = info
            continue
        m = _HEADING_RE.match(line)
        if m:
            # Strip a trailing run of # (closed ATX heading) and surrounding space.
            current_heading = m.group(2).rstrip("#").strip()
        yield (i, line, current_heading)


def iter_code_blocks(lines):
    """Yield (info_string, [body lines]) for every fenced code block."""
    in_fence = False
    fence_char = None
    fence_len = 0
    info_string = ""
    body = []
    for line in lines:
        info = _fence_info(line)
        if in_fence:
            if info is not None:
                c, n, tail = info
                if c == fence_char and n >= fence_len and tail == "":
                    in_fence = False
                    yield (info_string, body)
                    body = []
                    continue
            body.append(line)
        elif info is not None:
            in_fence = True
            fence_char, fence_len, info_string = info
            body = []


# --------------------------------------------------------------------------
# GitHub heading-slug anchors
# --------------------------------------------------------------------------

_ANCHOR_TAG_RE = re.compile(r"<a\s+[^>]*?(?:id|name)\s*=\s*[\"']([^\"']+)[\"']", re.I)


_MD_LINK_IN_HEADING_RE = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")


def slugify(text):
    """GitHub's heading slug: lowercase, drop non-word chars, spaces to hyphens.

    GitHub slugs the rendered heading text, so a link inside a heading
    contributes its text and not its URL.
    """
    text = _MD_LINK_IN_HEADING_RE.sub(r"\1", text)
    s = text.strip().lower()
    kept = []
    for ch in s:
        if ch.isalnum() or ch in " -_":
            kept.append(ch)
        # everything else (backticks, punctuation, link brackets) is dropped
    s = "".join(kept)
    s = s.replace(" ", "-")
    return s


def anchors_for(lines):
    """Return the set of GitHub anchors a rendered markdown file exposes.

    Heading slugs (with the -1/-2 disambiguation for a repeated slug in document
    order) plus explicit <a id=...>/<a name=...> anchors. Headings inside a fenced
    code block do not count.
    """
    anchors = set()
    seen = collections.Counter()
    in_fence = False
    fence_char = None
    fence_len = 0
    for line in lines:
        info = _fence_info(line)
        if in_fence:
            if info is not None:
                c, n, tail = info
                if c == fence_char and n >= fence_len and tail == "":
                    in_fence = False
            continue
        if info is not None:
            in_fence = True
            fence_char, fence_len, _ = info
            continue
        for m in _ANCHOR_TAG_RE.finditer(line):
            anchors.add(m.group(1))
        hm = _HEADING_RE.match(line)
        if hm:
            slug = slugify(hm.group(2).rstrip("#"))
            if slug == "":
                continue
            count = seen[slug]
            seen[slug] += 1
            anchors.add(slug if count == 0 else f"{slug}-{count}")
    return anchors


# --------------------------------------------------------------------------
# Case-sensitive path existence
# --------------------------------------------------------------------------


def exists_cs(absp):
    """True if `absp` exists AND every path component matches on-disk case.

    os.path.exists is case-insensitive on macOS, so a link that differs from the
    real entry only in case passes locally and 404s on GitHub. Listing the parent
    and matching the entry exactly is what catches that.
    """
    absp = os.path.normpath(absp)
    try:
        rel = os.path.relpath(absp, REPO_ROOT)
    except ValueError:
        return os.path.exists(absp)
    if rel == ".":
        return True
    if rel.startswith(".."):
        return os.path.exists(absp)
    cur = REPO_ROOT
    for part in rel.split(os.sep):
        try:
            entries = os.listdir(cur)
        except OSError:
            return False
        if part not in entries:
            return False
        cur = os.path.join(cur, part)
    return True


# --------------------------------------------------------------------------
# Prose-rule patterns
# --------------------------------------------------------------------------

_LINK_RE = re.compile(r"(!?)\[[^\]]*\]\(\s*([^)\s]+)(?:\s+[\"'][^\"']*[\"'])?\s*\)")

# PROVENANCE (user, spec)
_SRC_LINE_REF_RE = re.compile(
    r"\b(?:crates|services)/[A-Za-z0-9_./-]+\.[a-z]+:\d+\b"
)
_COMMIT_HASH_RE = re.compile(r"\b[0-9a-fA-F]{7,40}\b")
_LAST_VERIFIED = "Last verified against the code"
# Agent or AI language. "Generated with" on its own is ordinary English (a
# fixture "generated with current timestamps"), so it counts only when a tool
# or an AI is named after it.
_AI_PHRASE_RE = re.compile(
    r"Generated with (?:Claude|Copilot|ChatGPT|Cursor|Codex|Gemini|an? AI\b)"
    r"|Co-Authored-By:"
    r"|\bAs an AI\b",
    re.I,
)

# TRACKER (user)
_ISSUE_HASH_RE = re.compile(r"(?<![\w#])#\d+\b")
_TRACKER_URL_RE = re.compile(r"github\.com/[^\s)]+/(?:issues|pull)/\d+")
_ADR_CITE_RE = re.compile(r"\bADR-\d{3,4}\b")
# A link to the decision-record INDEX (the directory or its README) is
# navigation, which the documentation index must do. A link to one record is
# a citation, which is what TRACKER rejects.
_ADR_INDEX_TARGET_RE = re.compile(r"(?:^|/)adrs(?:/(?:README\.md)?)?(?:#.*)?$", re.I)

# SRCPATH (user)
_SRCPATH_RE = re.compile(
    r"\b(?:crates|services)/[A-Za-z0-9_./-]+\.[a-z]{1,5}\b(?::\d+)?"
)

# SUPERLATIVE (user, spec)
_EM_DASH = "—"
_SUPERLATIVE_WORDS = [
    "seamless", "powerful", "blazing", "enterprise-grade", "comprehensive",
    "world-class", "cutting-edge", "game-changing", "best-in-class",
    "effortless", "painless", "rock-solid", "industry-leading", "simply",
]
_SUPERLATIVE_RE = re.compile(
    r"(?<![\w-])(" + "|".join(re.escape(w) for w in _SUPERLATIVE_WORDS) + r")(?![\w-])",
    re.I,
)

# TERM (user, spec): phrase-level (pattern, message) pairs, extended in one place.
TERM_RULES = [
    (re.compile(r"(?:query|gateway|maintain|ingest)\s+tier", re.I),
     "a process runs in a mode, not a tier; tier is the cache"),
    (re.compile(r"L1\s+part", re.I),
     "an L1 object is a segment; part is a catalog snapshot part"),
    (re.compile(r"declared\s+column", re.I),
     "use typed attribute column"),
    (re.compile(r"\bingester\b", re.I),
     "Ravel has no ingester; say gateway mode or shard actor"),
    (re.compile(r"exactly[- ]once", re.I),
     "Ravel does not offer exactly-once; describe the actual guarantee"),
    (re.compile(r"strongly\s+consistent", re.I),
     "name the scope of the consistency claim instead"),
    (re.compile(r"zero[- ]copy", re.I),
     "state the scope or drop the claim"),
    (re.compile(r"lock[- ]free", re.I),
     "state the scope or drop the claim"),
]
_ROBUST_RE = re.compile(r"\brobust\b", re.I)
_ROBUST_CONTEXT_RE = re.compile(r"median|MAD|estimator", re.I)
_SENTENCE_SPLIT_RE = re.compile(r"[.!?]")


def _robust_findings(text):
    """Yield the matched text for each 'robust' that reads as marketing.

    Silent when the same sentence already carries median/MAD/estimator before it,
    or when a parenthesized gloss follows the word.
    """
    for m in _ROBUST_RE.finditer(text):
        start, end = m.start(), m.end()
        # Sentence bounds within the line.
        prev = 0
        for sm in _SENTENCE_SPLIT_RE.finditer(text, 0, start):
            prev = sm.end()
        nxt = len(text)
        nm = _SENTENCE_SPLIT_RE.search(text, end)
        if nm:
            nxt = nm.start()
        before = text[prev:start]
        after = text[end:nxt]
        if _ROBUST_CONTEXT_RE.search(before):
            continue
        if after.lstrip().startswith("("):
            continue
        yield m.group(0)


# --------------------------------------------------------------------------
# Finding
# --------------------------------------------------------------------------

Finding = collections.namedtuple("Finding", ["path", "rule", "key"])


# --------------------------------------------------------------------------
# Per-file markdown analysis
# --------------------------------------------------------------------------


class Repo:
    """Holds the file inventory and the caches shared across rules."""

    def __init__(self):
        self.md_files = []      # repo-relative posix paths
        self.svg_files = []     # repo-relative posix paths, under docs/
        self._text_cache = {}
        self._anchor_cache = {}
        self.svg_referenced = set()   # svg rel paths referenced by some markdown
        self.link_graph = collections.defaultdict(set)  # md rel -> set(md rel reachable)

    def read(self, rel):
        if rel not in self._text_cache:
            with open(os.path.join(REPO_ROOT, rel), "r", encoding="utf-8") as fh:
                self._text_cache[rel] = fh.read()
        return self._text_cache[rel]

    def anchors(self, rel):
        if rel not in self._anchor_cache:
            self._anchor_cache[rel] = anchors_for(self.read(rel).split("\n"))
        return self._anchor_cache[rel]


def rel_of(absp):
    return os.path.relpath(absp, REPO_ROOT).replace(os.sep, "/")


def gather(repo):
    for dirpath, dirnames, filenames in os.walk(REPO_ROOT):
        dirnames[:] = [d for d in dirnames if d not in _PRUNE_DIRS]
        for name in filenames:
            absp = os.path.join(dirpath, name)
            rel = rel_of(absp)
            if name.endswith(".md"):
                repo.md_files.append(rel)
            elif name.endswith(".svg") and rel.startswith("docs/"):
                repo.svg_files.append(rel)
    repo.md_files.sort()
    repo.svg_files.sort()


def analyze_markdown(rel, repo):
    """Run the per-file rules for one markdown file. Returns a list of Findings."""
    findings = []
    scope = classify(rel)
    text = repo.read(rel)
    lines = text.split("\n")
    src_dir = os.path.dirname(os.path.join(REPO_ROOT, rel))

    for lineno, line, heading in iter_prose_lines(lines):
        # ---- LINK / ANCHOR (all scopes) plus SVG reference + alt-text tracking.
        if scope is not None:
            for is_img, target in _LINK_RE.findall(line):
                _link_and_anchor(rel, src_dir, is_img, target, scope, repo, findings)

        # Scope-gated prose rules.
        if scope in ("user", "spec", "generated", "changelog"):
            _provenance(rel, line, findings)
            _superlative(rel, line, findings)
            _term(rel, line, findings)
        if scope == "user":
            _tracker(rel, line, heading, findings)
        if scope in ("user", "changelog"):
            _srcpath(rel, line, findings)

    if scope in ("user", "generated", "changelog"):
        findings.extend(_sqltable(rel, lines, repo))

    return findings


def _register_link(rel, absp, repo):
    """Record a markdown->markdown edge and an svg reference for the graph rules."""
    r = rel_of(absp)
    if r.endswith(".md"):
        repo.link_graph[rel].add(r)
    elif r.endswith(".svg") and r.startswith("docs/"):
        repo.svg_referenced.add(r)


def _resolve(src_dir, target):
    if target.startswith("/"):
        return os.path.normpath(os.path.join(REPO_ROOT, target.lstrip("/")))
    return os.path.normpath(os.path.join(src_dir, target))


def _link_and_anchor(rel, src_dir, is_img, target, scope, repo, findings):
    lower = target.lower()
    if lower.startswith(("http://", "https://", "ftp://", "mailto:", "tel:")) or target.startswith("//"):
        return
    if target.startswith("#"):
        # Same-file anchor.
        frag = target[1:]
        if frag and frag not in repo.anchors(rel):
            findings.append(Finding(rel, "ANCHOR", target))
        return
    # Relative link, possibly with a fragment.
    path_part, _, frag = target.partition("#")
    if path_part == "":
        return
    absp = _resolve(src_dir, path_part)
    if not exists_cs(absp):
        findings.append(Finding(rel, "LINK", path_part))
        return
    # The target exists: record graph edges and check the fragment / alt text.
    _register_link(rel, absp, repo)
    if is_img == "!" and path_part.lower().endswith(".svg"):
        # Alt text is the [...] portion; re-derive it for this specific link.
        _check_svg_alt(rel, path_part, findings)
    if frag:
        trel = rel_of(absp)
        if trel.endswith(".md") and exists_cs(absp):
            if frag not in repo.anchors(trel):
                findings.append(Finding(rel, "ANCHOR", target))


# Alt-text needs the [alt] text, which _LINK_RE.findall discards; re-scan for images.
_IMG_RE = re.compile(r"!\[([^\]]*)\]\(\s*([^)\s]+)")


def _check_svg_alt(rel, path_part, findings):
    # Handled in a dedicated pass so we can read the alt text; see _svg_alt_pass.
    pass


def _svg_alt_pass(rel, repo, findings):
    """Every markdown image whose target is an .svg must carry non-empty alt text."""
    for line in repo.read(rel).split("\n"):
        for alt, target in _IMG_RE.findall(line):
            tpath = target.split("#", 1)[0]
            if tpath.lower().endswith(".svg") and alt.strip() == "":
                findings.append(Finding(rel, "SVG", target + " (empty alt text)"))


def _provenance(rel, line, findings):
    for m in _SRC_LINE_REF_RE.finditer(line):
        findings.append(Finding(rel, "PROVENANCE", m.group(0)))
    for m in _COMMIT_HASH_RE.finditer(line):
        tok = m.group(0)
        low = tok.lower()
        if any(c.isalpha() for c in low) and any(c.isdigit() for c in low):
            findings.append(Finding(rel, "PROVENANCE", low))
    idx = 0
    while True:
        j = line.find(_LAST_VERIFIED, idx)
        if j < 0:
            break
        findings.append(Finding(rel, "PROVENANCE", _LAST_VERIFIED))
        idx = j + len(_LAST_VERIFIED)
    for m in _AI_PHRASE_RE.finditer(line):
        findings.append(Finding(rel, "PROVENANCE", m.group(0)))


def _tracker(rel, line, heading, findings):
    if heading is not None and heading == "Background":
        return
    for m in _ISSUE_HASH_RE.finditer(line):
        findings.append(Finding(rel, "TRACKER", m.group(0)))
    for m in _TRACKER_URL_RE.finditer(line):
        findings.append(Finding(rel, "TRACKER", m.group(0)))
    for m in _ADR_CITE_RE.finditer(line):
        findings.append(Finding(rel, "TRACKER", m.group(0)))
    for m in _LINK_RE.findall(line):
        target = m[1]
        low = target.lower()
        if low.startswith(("http://", "https://")):
            continue
        if "docs/adrs/" in low or re.search(r"(^|/)adrs/", low):
            if _ADR_INDEX_TARGET_RE.search(low):
                continue
            findings.append(Finding(rel, "TRACKER", target))


def _srcpath(rel, line, findings):
    for m in _SRCPATH_RE.finditer(line):
        tok = m.group(0)
        findings.append(Finding(rel, "SRCPATH", tok))


def _superlative(rel, line, findings):
    if _EM_DASH in line:
        for _ in range(line.count(_EM_DASH)):
            findings.append(Finding(rel, "SUPERLATIVE", "em-dash"))
    for m in _SUPERLATIVE_RE.finditer(line):
        findings.append(Finding(rel, "SUPERLATIVE", m.group(0).lower()))


def _term(rel, line, findings):
    for pattern, _msg in TERM_RULES:
        for m in pattern.finditer(line):
            findings.append(Finding(rel, "TERM", m.group(0).lower()))
    for matched in _robust_findings(line):
        findings.append(Finding(rel, "TERM", matched.lower()))


# --------------------------------------------------------------------------
# SQLTABLE
# --------------------------------------------------------------------------

_TABLE_CONST_RE = re.compile(r'pub\s+const\s+\w*TABLE\w*\s*:\s*&str\s*=\s*"([^"]+)"')
_FROM_RE = re.compile(r"\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)", re.I)
_SQL_INFO_RE = re.compile(r"^(sql|postgresql|postgres)\b", re.I)
# FROM inside these functions names a field or a position, not a table:
# EXTRACT(minute FROM ts), SUBSTRING(s FROM 2), TRIM(BOTH x FROM s),
# OVERLAY(s PLACING t FROM 3). Blanked out before the table scan.
_FUNC_FROM_RE = re.compile(r"\b(?:EXTRACT|SUBSTRING|TRIM|OVERLAY)\s*\([^()]*\)", re.I)
# A common table expression is a legitimate FROM target inside its own
# statement: WITH recent AS (...) SELECT ... FROM recent.
_CTE_RE = re.compile(
    r"(?:\bWITH\b(?:\s+RECURSIVE\b)?|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(", re.I
)
_SELECT_FROM_RE = re.compile(r"\bSELECT\b.*\bFROM\b", re.I)


def registered_tables():
    session = os.path.join(REPO_ROOT, "crates", "ravel-sql", "src", "session.rs")
    if not os.path.isfile(session):
        raise CheckError(f"cannot read SQL table source constants: {session} missing")
    with open(session, "r", encoding="utf-8") as fh:
        src = fh.read()
    names = set(_TABLE_CONST_RE.findall(src))
    if not names:
        raise CheckError(f"no *_TABLE string constants found in {session}")
    return names


def _sqltable(rel, lines, repo):
    findings = []
    tables = repo.sql_tables
    for info, body in iter_code_blocks(lines):
        is_sql = bool(_SQL_INFO_RE.match(info.strip()))
        is_shell = info.strip().lower() in ("sh", "bash", "shell", "console", "")
        if not (is_sql or is_shell):
            continue
        allowed = tables | set(_CTE_RE.findall("\n".join(body)))
        for bl in body:
            # In a shell example, require SELECT on the same line as FROM so an
            # English "from" or a Dockerfile FROM is never read as a table.
            if not is_sql and not _SELECT_FROM_RE.search(bl):
                continue
            scan = _FUNC_FROM_RE.sub(" ", bl)
            for name in _FROM_RE.findall(scan):
                if name not in allowed:
                    findings.append(Finding(rel, "SQLTABLE", name))
    return findings


# --------------------------------------------------------------------------
# FEATURE (README.md support matrix)
# --------------------------------------------------------------------------

_MATRIX_BEGIN = "<!-- BEGIN SUPPORT MATRIX -->"
_MATRIX_END = "<!-- END SUPPORT MATRIX -->"
_FEATURE_LINE_RE = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*=")


def features_available():
    """Every feature name declared in a [features] section under crates/ or services/."""
    names = set()
    for top in ("crates", "services"):
        base = os.path.join(REPO_ROOT, top)
        if not os.path.isdir(base):
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in _PRUNE_DIRS]
            if "Cargo.toml" not in filenames:
                continue
            with open(os.path.join(dirpath, "Cargo.toml"), "r", encoding="utf-8") as fh:
                in_features = False
                for line in fh:
                    stripped = line.strip()
                    if stripped.startswith("["):
                        in_features = stripped == "[features]"
                        continue
                    if in_features:
                        m = _FEATURE_LINE_RE.match(line)
                        if m:
                            names.add(m.group(1))
    return names


def _split_row(row):
    cells = row.strip().strip("|").split("|")
    return [c.strip() for c in cells]


def check_feature(repo):
    findings = []
    rel = "README.md"
    if rel not in repo.md_files:
        return findings
    text = repo.read(rel)
    if _MATRIX_BEGIN not in text or _MATRIX_END not in text:
        return findings  # absent markers are not an error; a later wave adds them
    block = text.split(_MATRIX_BEGIN, 1)[1].split(_MATRIX_END, 1)[0]
    rows = [ln for ln in block.split("\n") if ln.strip().startswith("|")]
    if not rows:
        return findings
    header = _split_row(rows[0])
    if "In published image" not in header:
        findings.append(Finding(rel, "FEATURE", "table missing 'In published image' column"))
    if "Feature gate" not in header:
        return findings
    gate_idx = header.index("Feature gate")
    available = features_available()
    for row in rows[1:]:
        cells = _split_row(row)
        if all(set(c) <= set("-: ") for c in cells):
            continue  # the |---|---| separator row
        if gate_idx >= len(cells):
            continue
        value = cells[gate_idx].strip().strip("`")
        if value == "" or value.lower() == "feature gate":
            continue
        if value != "none" and value not in available:
            findings.append(Finding(rel, "FEATURE", value))
    return findings


# --------------------------------------------------------------------------
# SVG validity
# --------------------------------------------------------------------------


def _localname(tag):
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def check_svg(rel):
    findings = []
    absp = os.path.join(REPO_ROOT, rel)
    with open(absp, "r", encoding="utf-8") as fh:
        raw = fh.read()
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        findings.append(Finding(rel, "SVG", f"{rel} (parse error: {exc})"))
        return findings

    root_attrs = {_localname(k): v for k, v in root.attrib.items()}
    if "viewBox" not in root_attrs:
        findings.append(Finding(rel, "SVG", f"{rel} (no viewBox)"))
    role = root_attrs.get("role", "")
    label = root_attrs.get("aria-label", "")
    if role != "img" or label.strip() == "":
        findings.append(Finding(rel, "SVG", f"{rel} (missing role=img/aria-label)"))

    for el in root.iter():
        name = _localname(el.tag)
        if name == "image":
            findings.append(Finding(rel, "SVG", f"{rel} (embeds raster <image>)"))
        for k, v in el.attrib.items():
            lk = _localname(k)
            if lk in ("href", "src") and not v.startswith("#"):
                findings.append(Finding(rel, "SVG", f"{rel} (external {lk}: {v})"))

    if "@import" in raw:
        findings.append(Finding(rel, "SVG", f"{rel} (@import)"))
    if re.search(r"url\(\s*['\"]?https?:", raw):
        findings.append(Finding(rel, "SVG", f"{rel} (url(http...))"))
    return findings


# --------------------------------------------------------------------------
# ORPHAN
# --------------------------------------------------------------------------


def check_orphan(repo):
    findings = []
    roots = [r for r in ("README.md", "docs/README.md") if r in repo.md_files]
    reachable = set(roots)
    queue = list(roots)
    while queue:
        cur = queue.pop()
        for nxt in repo.link_graph.get(cur, ()):
            if nxt not in reachable:
                reachable.add(nxt)
                queue.append(nxt)
    for rel in repo.md_files:
        if rel.startswith("docs/") and rel not in reachable:
            findings.append(Finding(rel, "ORPHAN", rel))
    return findings


# --------------------------------------------------------------------------
# ADRINDEX
# --------------------------------------------------------------------------

_ADR_FILE_RE = re.compile(r"^(\d{3,4})-.*\.md$")
_ADR_INDEX_LINK_RE = re.compile(r"\]\(\s*(\d{3,4}-[^)#\s]+\.md)\s*\)")


def check_adrindex(repo):
    findings = []
    index_rel = "docs/adrs/README.md"
    if index_rel not in repo.md_files:
        raise CheckError(f"decision-record index {index_rel} is missing")
    index_text = repo.read(index_rel)
    referenced = collections.Counter()
    referenced_files = []
    for m in _ADR_INDEX_LINK_RE.finditer(index_text):
        fname = m.group(1)
        num = fname.split("-", 1)[0]
        referenced[num] += 1
        referenced_files.append(fname)

    present = {}
    for rel in repo.md_files:
        if not rel.startswith("docs/adrs/"):
            continue
        base = rel.rsplit("/", 1)[-1]
        fm = _ADR_FILE_RE.match(base)
        if fm:
            present[fm.group(1)] = base

    for num in sorted(present):
        count = referenced[num]
        if count == 0:
            findings.append(Finding(index_rel, "ADRINDEX", num))
        elif count > 1:
            findings.append(Finding(index_rel, "ADRINDEX", f"{num} (listed {count} times)"))

    for fname in referenced_files:
        if not exists_cs(os.path.join(REPO_ROOT, "docs", "adrs", fname)):
            findings.append(Finding(index_rel, "ADRINDEX", f"{fname} (missing file)"))
    return findings


# --------------------------------------------------------------------------
# SVG reference check
# --------------------------------------------------------------------------


def check_svg_references(repo):
    findings = []
    for rel in repo.svg_files:
        if rel not in repo.svg_referenced:
            findings.append(Finding(rel, "SVG", f"{rel} (referenced by no markdown)"))
    return findings


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


def collect_findings(repo):
    findings = []
    repo.sql_tables = registered_tables()
    for rel in repo.md_files:
        findings.extend(analyze_markdown(rel, repo))
    # Alt-text pass needs the [alt] text the link regex discards.
    for rel in repo.md_files:
        if classify(rel) is not None:
            _svg_alt_pass(rel, repo, findings)
    for rel in repo.svg_files:
        findings.extend(check_svg(rel))
    findings.extend(check_svg_references(repo))
    findings.extend(check_orphan(repo))
    findings.extend(check_adrindex(repo))
    findings.extend(check_feature(repo))
    return findings


BASELINE_HEADER = [
    "# scripts/docs_lint_baseline.txt -- findings the docs gate tolerates today.",
    "#",
    "# Regenerate with: python3 scripts/check_docs.py --update-baseline",
    "#",
    "# Each line is one finding: <path>\\t<rule>\\t<key>. The key carries no line",
    "# number, so a finding keeps its identity when its text moves within a file.",
    "# Delete the entry as a finding is fixed; NEVER regenerate the file wholesale.",
    "# The line count is ADR-1040's epic progress measure and reaches zero before",
    "# the epic closes.",
]


def format_line(f):
    return f"{f.path}\t{f.rule}\t{f.key}"


def write_baseline(findings):
    lines = list(BASELINE_HEADER)
    for line in sorted(format_line(f) for f in findings):
        lines.append(line)
    os.makedirs(os.path.dirname(BASELINE_PATH), exist_ok=True)
    with open(BASELINE_PATH, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def load_baseline():
    if not os.path.isfile(BASELINE_PATH):
        return collections.Counter()
    counter = collections.Counter()
    with open(BASELINE_PATH, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith("#") or line.strip() == "":
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                # A malformed entry baselines nothing, so its finding surfaces as
                # NEW below. Say why, rather than failing on a line that looks
                # baselined to the reader.
                print(f"check_docs: ignoring malformed baseline line: {line!r}", file=sys.stderr)
                continue
            counter[tuple(parts)] += 1
    return counter


def run(update_baseline=False, strict=False, out=sys.stdout):
    repo = Repo()
    gather(repo)
    findings = collect_findings(repo)

    if update_baseline:
        write_baseline(findings)
        by_rule = collections.Counter(f.rule for f in findings)
        out.write(f"wrote {BASELINE_PATH} with {len(findings)} findings\n")
        for rule in sorted(by_rule):
            out.write(f"  {rule}: {by_rule[rule]}\n")
        return 0

    baseline = load_baseline()
    current = collections.Counter(
        (f.path, f.rule, f.key) for f in findings
    )

    # Occurrence counts of an ALREADY-BASELINED (path, rule, key) are not
    # enforced: docs/guides/operations.md holds 147 decision citations, and
    # while this epic is in flight other pull requests keep adding to it. A
    # 148th `ADR-0055` arriving from elsewhere would otherwise fail this gate on
    # a branch that never touched the file.
    #
    # A key the file has not carried before is still NEW, so a genuinely new
    # defect fails even in a file full of tolerated debt: a second, different
    # vocabulary error is caught while another `ADR-0055` is not. Deleting a
    # key's entries as a task fixes them restores exact enforcement for that
    # key, which is what keeps the baseline a debt counter rather than a
    # blanket exemption.
    new = []
    for key, count in current.items():
        if key in baseline:
            continue
        for _ in range(count):
            new.append(key)
    unused = []
    for key, count in baseline.items():
        surplus = count - current.get(key, 0)
        for _ in range(max(0, surplus)):
            unused.append(key)

    for key in sorted(new):
        out.write(f"NEW  {key[0]}\t{key[1]}\t{key[2]}\n")
    if unused:
        for key in sorted(unused):
            level = "FAIL" if strict else "note"
            out.write(f"{level} unused baseline entry: {key[0]}\t{key[1]}\t{key[2]}\n")

    if new:
        out.write(f"\n{len(new)} finding(s) not in the baseline.\n")
        return 1
    if strict and unused:
        out.write(f"\n{len(unused)} unused baseline entr(y/ies) under --strict-baseline.\n")
        return 1
    out.write("docs gate: clean against baseline.\n")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--update-baseline", action="store_true",
                        help="rewrite the baseline from the current tree")
    parser.add_argument("--strict-baseline", action="store_true",
                        help="fail on a baseline entry that matches no finding")
    args = parser.parse_args(argv)
    try:
        return run(update_baseline=args.update_baseline, strict=args.strict_baseline)
    except CheckError as exc:
        print(f"check_docs: cannot run: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
