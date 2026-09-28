#!/usr/bin/env python3
"""Documentation claims gate for the Ravel repository (ADR-1658 decision 3).

Holds the normative docs to the registry at docs/review/claims.yaml, one entry
per decided claim. Four rules:

  QUOTE     every entry's `quote` occurs in its `doc` exactly once. Matching is
            verbatim except that a run of whitespace in the quote also matches
            a line break (and any blockquote `>` markers after it), so a quote
            is one sentence as a reader sees it, not as the editor wrapped it.
  BIND      every `binds` reference (`path.rs::Sym[::Sym]`, the TLA
            traceability lane's syntax) resolves: the file exists, ends in .rs,
            and each symbol names a definition (`fn`, `struct`, `enum`,
            `trait`, `type`, `const`, `static`, `mod`; a `#[test]` function is
            an `fn`) at a word boundary, outside comments.
  NEGATIVE  every prose line of a normative doc that matches MARKERS is
            covered by an entry whose quote occurrence lies on that line.
  STATUS    a `contradicted` entry carries `issue`; a `not-implemented` entry
            whose binds all resolve fails, since the capability now exists.

A fifth identifier, REGISTRY, reports an entry the rules cannot be applied to:
a missing or unknown field, a duplicate id, or a `doc` outside the normative
set (ADR-1658 decision 2: decision records and docs/internal/ are never
registered).

The registry is a strict YAML subset parsed here by hand, so the gate stays
stdlib only: a flat list of mappings whose values are scalar strings (plain,
double-quoted with only \\" and \\\\ escapes, or single-quoted) or lists of
such strings (block `- item` form, or `[]`). A plain scalar may continue on
more-indented lines, folded with single spaces. Anything else (flow mappings,
block scalars, anchors, inline comments) is a parse error, not a guess.

Exit codes: 0 clean, 1 findings, 2 the gate could not run (missing or
unparseable registry, an unreadable or missing normative doc, or zero marker
matches across all normative docs, which would mean the scan is broken).
"""

import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from check_docs import GENERATED_PAGES, iter_prose_lines  # noqa: E402

REPO_ROOT = os.path.dirname(_HERE)

REGISTRY = "docs/review/claims.yaml"

# The docs/*.md specs the CLAUDE.md doc map names. ADRs it also names are
# decision records, which ADR-1658 decision 2 leaves out.
SPEC_DOCS = (
    "docs/analytics.md",
    "docs/architecture.md",
    "docs/catalog-and-mvcc.md",
    "docs/consistency-model.md",
    "docs/ingest.md",
    "docs/log-segment-format.md",
    "docs/object-store-contract.md",
    "docs/otap-ingest.md",
    "docs/query-engine.md",
    "docs/segment-format.md",
)

# Fixed by ADR-1658 decision 3. Widening it is an ADR amendment.
MARKERS = (
    "does not exist",
    "do not exist",
    "not implemented",
    "not yet implemented",
    "will land",
    "is not supported",
    "are not supported",
    "not available",
)

_MARKER_RE = re.compile("|".join(re.escape(m) for m in MARKERS), re.IGNORECASE)

KINDS = ("positive", "negative", "not-a-claim")
STATUSES = ("verified", "contradicted", "not-implemented")
FIELDS = ("id", "doc", "quote", "kind", "binds", "status", "issue", "note")
REQUIRED = ("id", "doc", "quote", "kind", "binds", "status", "note")
LIST_FIELDS = ("binds",)

_DEF_KEYWORDS = ("fn", "struct", "enum", "trait", "type", "const", "static", "mod")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class CheckError(Exception):
    """The gate cannot run."""


# --------------------------------------------------------------------------
# Registry parser (the YAML subset described in the module docstring)
# --------------------------------------------------------------------------


_KEY_RE = re.compile(r"^([a-z_]+):(?: (.*))?$")


def _scalar(raw, lineno):
    raw = raw.strip()
    if raw.startswith('"'):
        out = []
        i = 1
        while i < len(raw):
            c = raw[i]
            if c == "\\":
                nxt = raw[i + 1:i + 2]
                if nxt not in ('"', "\\"):
                    raise CheckError(f"line {lineno}: unsupported escape \\{nxt}")
                out.append(nxt)
                i += 2
                continue
            if c == '"':
                if raw[i + 1:].strip():
                    raise CheckError(f"line {lineno}: text after closing quote")
                return "".join(out)
            out.append(c)
            i += 1
        raise CheckError(f"line {lineno}: unterminated double-quoted string")
    if raw.startswith("'"):
        body = raw[1:]
        out = []
        i = 0
        while i < len(body):
            if body[i] == "'":
                if body[i + 1:i + 2] == "'":
                    out.append("'")
                    i += 2
                    continue
                if body[i + 1:].strip():
                    raise CheckError(f"line {lineno}: text after closing quote")
                return "".join(out)
            out.append(body[i])
            i += 1
        raise CheckError(f"line {lineno}: unterminated single-quoted string")
    if not raw:
        raise CheckError(f"line {lineno}: empty value")
    if raw[0] in "[]{}|>&*!%@`#,?":
        raise CheckError(f"line {lineno}: unsupported YAML syntax {raw[0]!r}; quote the value")
    if " #" in raw or ": " in raw or raw.endswith(":"):
        raise CheckError(f"line {lineno}: plain value needs quoting (contains ': ' or ' #')")
    return raw


def parse_registry(text):
    """Parse the registry subset into a list of {field: str | [str]} dicts.

    Each dict also carries `_line`, the line its entry starts on.
    """
    entries = []
    current = None
    pending_list = None      # key awaiting block list items
    folding = None           # (key, indent) of a plain scalar that may continue
    for lineno, line in enumerate(text.splitlines(), start=1):
        if "\t" in line:
            raise CheckError(f"line {lineno}: tab character")
        stripped = line.strip()
        if not stripped or line.lstrip().startswith("#"):
            folding = None
            continue
        indent = len(line) - len(line.lstrip(" "))

        if line.startswith("- "):
            current = {"_line": lineno}
            entries.append(current)
            body = line[2:]
            key_indent = 2
        elif current is not None and pending_list is not None and indent == 4 \
                and stripped.startswith("- "):
            current[pending_list].append(_scalar(stripped[2:], lineno))
            continue
        elif current is not None and folding is not None and indent > folding[1]:
            key = folding[0]
            current[key] = current[key] + " " + _scalar(stripped, lineno)
            continue
        elif current is not None and indent == 2:
            body = line[2:]
            key_indent = 2
        else:
            raise CheckError(f"line {lineno}: expected '- key: value' or '  key: value'")

        pending_list = None
        folding = None
        m = _KEY_RE.match(body)
        if not m:
            raise CheckError(f"line {lineno}: expected 'key: value'")
        key, value = m.group(1), m.group(2)
        if key in current:
            raise CheckError(f"line {lineno}: duplicate key {key!r}")
        if value is None or value.strip() == "":
            current[key] = []
            pending_list = key
        elif value.strip() == "[]":
            current[key] = []
        else:
            current[key] = _scalar(value, lineno)
            if not value.strip().startswith(("'", '"')):
                folding = (key, key_indent)
    return entries


# --------------------------------------------------------------------------
# Normative docs and quote location
# --------------------------------------------------------------------------


def _walk_md(root, top):
    base = os.path.join(root, top)
    found = []
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames.sort()
        for name in sorted(filenames):
            if name.endswith(".md"):
                rel = os.path.relpath(os.path.join(dirpath, name), root)
                found.append(rel.replace(os.sep, "/"))
    return found


def normative_docs(root):
    """ADR-1658 decision 2: the doc map's specs, the user pages, PROGRESS.md."""
    docs = list(SPEC_DOCS) + ["README.md", "PROGRESS.md"]
    docs += _walk_md(root, "docs/guides")
    docs += [p for p in _walk_md(root, "docs/reference") if p not in GENERATED_PAGES]
    seen = []
    for d in docs:
        if d not in seen:
            seen.append(d)
    return seen


def read_doc(root, rel, required):
    absp = os.path.join(root, rel)
    if not os.path.exists(absp):
        if required:
            raise CheckError(f"normative doc {rel} is missing")
        return None
    try:
        with open(absp, encoding="utf-8") as fh:
            return fh.read()
    except (OSError, UnicodeDecodeError) as exc:
        raise CheckError(f"cannot read {rel}: {exc}") from exc


def quote_regex(quote):
    words = quote.split()
    sep = r"(?:[ \t]*\n[ \t]*(?:>[ \t]*)*|[ \t]+)"
    return re.compile(sep.join(re.escape(w) for w in words))


def quote_spans(text, quote):
    """Return [(first_line, last_line)] for each occurrence of `quote`."""
    if not quote.split():
        return []
    spans = []
    for m in quote_regex(quote).finditer(text):
        first = text.count("\n", 0, m.start()) + 1
        last = text.count("\n", 0, m.end()) + 1
        spans.append((first, last))
    return spans


# --------------------------------------------------------------------------
# Symbol resolution
# --------------------------------------------------------------------------


_RAW_STRING_START_RE = re.compile(r'b?r(#*)"')
_CHAR_LITERAL_RE = re.compile(r"'(?:\\(?:u\{[0-9a-fA-F_]{1,6}\}|x[0-9a-fA-F]{2}|.)|[^'\\\n])'")


def _strip_comments(source):
    """Blank out comments and string/char literals, keeping newlines.

    A definition keyword inside a comment or a string is not a definition, and
    a `/*` inside a string must not open a comment. Lifetimes (`'a`,
    `'static`) are code and are kept.
    """
    out = []
    i, n = 0, len(source)

    def blank(segment):
        out.append("".join("\n" if ch == "\n" else " " for ch in segment))

    while i < n:
        ch = source[i]
        if source.startswith("//", i):
            end = source.find("\n", i)
            end = n if end == -1 else end
            blank(source[i:end])
            i = end
        elif source.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if source.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif source.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(source[i:j])
            i = j
        elif ch in "rb" and (i == 0 or not (source[i - 1].isalnum() or source[i - 1] == "_")) and _RAW_STRING_START_RE.match(source, i):
            m = _RAW_STRING_START_RE.match(source, i)
            close = '"' + m.group(1)
            end = source.find(close, m.end())
            end = n if end == -1 else end + len(close)
            blank(source[i:end])
            i = end
        elif ch == '"' or (ch == "b" and source.startswith('b"', i) and (i == 0 or not (source[i - 1].isalnum() or source[i - 1] == "_"))):
            j = i + (2 if ch == "b" else 1)
            while j < n and source[j] != '"':
                j += 2 if source[j] == "\\" else 1
            end = min(j + 1, n)
            blank(source[i:end])
            i = end
        elif ch == "'":
            m = _CHAR_LITERAL_RE.match(source, i)
            if m:
                blank(m.group(0))
                i = m.end()
            else:
                out.append(ch)
                i += 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def resolve_ref(root, ref, cache):
    """Return None if `ref` resolves, else a reason string."""
    parts = ref.split("::")
    path, symbols = parts[0], parts[1:]
    if not path.endswith(".rs"):
        return f"'{path}' is not a Rust (.rs) path"
    if not symbols or any(not _IDENT_RE.match(s) for s in symbols):
        return "expected path.rs::Symbol[::Symbol]"
    if path not in cache:
        absp = os.path.join(root, path)
        if not os.path.isfile(absp):
            cache[path] = None
        else:
            try:
                with open(absp, encoding="utf-8") as fh:
                    cache[path] = _strip_comments(fh.read())
            except (OSError, UnicodeDecodeError) as exc:
                raise CheckError(f"cannot read {path}: {exc}") from exc
    source = cache[path]
    if source is None:
        return f"missing source '{path}'"
    kw = "|".join(_DEF_KEYWORDS)
    for sym in symbols:
        # `&'static Mode` and `*const Foo` are type positions, not definitions.
        if not re.search(rf"(?<!['*])\b(?:{kw})\s+{re.escape(sym)}\b", source):
            return f"no definition of '{sym}' in '{path}'"
    return None


# --------------------------------------------------------------------------
# Rules
# --------------------------------------------------------------------------


def collect_findings(root):
    reg_path = os.path.join(root, REGISTRY)
    if not os.path.isfile(reg_path):
        raise CheckError(f"registry {REGISTRY} is missing")
    try:
        with open(reg_path, encoding="utf-8") as fh:
            entries = parse_registry(fh.read())
    except (OSError, UnicodeDecodeError) as exc:
        raise CheckError(f"cannot read {REGISTRY}: {exc}") from exc
    except CheckError as exc:
        raise CheckError(f"{REGISTRY}: {exc}") from exc

    docs = normative_docs(root)
    required = set(SPEC_DOCS)
    texts = {}
    for rel in docs:
        text = read_doc(root, rel, rel in required)
        if text is not None:
            texts[rel] = text

    findings = []
    ref_cache = {}
    covered = {}          # doc -> set of line numbers some entry's quote spans
    seen_ids = set()

    for e in entries:
        eid = e.get("id") if isinstance(e.get("id"), str) else f"<entry at line {e['_line']}>"
        bad = False
        for key in e:
            if key != "_line" and key not in FIELDS:
                findings.append(("REGISTRY", eid, f"unknown field '{key}'"))
                bad = True
        for key in REQUIRED:
            if key not in e:
                findings.append(("REGISTRY", eid, f"missing field '{key}'"))
                bad = True
        for key in FIELDS:
            if key not in e:
                continue
            want_list = key in LIST_FIELDS
            if want_list != isinstance(e[key], list):
                findings.append(("REGISTRY", eid,
                                 f"field '{key}' must be a {'list' if want_list else 'string'}"))
                bad = True
        if bad:
            continue
        if eid in seen_ids:
            findings.append(("REGISTRY", eid, "duplicate id"))
        seen_ids.add(eid)
        if e["kind"] not in KINDS:
            findings.append(("REGISTRY", eid, f"kind '{e['kind']}' is not one of {', '.join(KINDS)}"))
        if e["status"] not in STATUSES:
            findings.append(("REGISTRY", eid,
                             f"status '{e['status']}' is not one of {', '.join(STATUSES)}"))
        doc = e["doc"]
        if doc not in texts:
            findings.append(("REGISTRY", eid, f"doc '{doc}' is not a normative doc in this tree"))
        else:
            spans = quote_spans(texts[doc], e["quote"])
            if len(spans) != 1:
                findings.append(("QUOTE", eid,
                                 f"quote occurs {len(spans)} times in {doc}, want exactly 1: "
                                 f"{e['quote']!r}"))
            if spans:
                lines = covered.setdefault(doc, set())
                for first, last in spans:
                    lines.update(range(first, last + 1))

        failures = [(ref, resolve_ref(root, ref, ref_cache)) for ref in e["binds"]]
        if e["status"] == "not-implemented":
            # Only "not defined yet" is expected here; a missing file or a
            # malformed reference would silently disarm the STATUS trip.
            for ref, reason in failures:
                # Only the final symbol may be absent; a missing file, a
                # malformed reference or a missing leading segment is rot.
                last = ref.split("::")[-1]
                if reason is not None and not reason.startswith(f"no definition of '{last}' in "):
                    findings.append(("BIND", eid, f"{ref}: {reason}"))
            if failures and all(reason is None for _, reason in failures):
                findings.append(("STATUS", eid,
                                 "not-implemented but every bind resolves: the capability "
                                 "exists, rewrite the sentence and the entry"))
        else:
            for ref, reason in failures:
                if reason is not None:
                    findings.append(("BIND", eid, f"{ref}: {reason}"))
        if e["status"] == "contradicted" and not str(e.get("issue") or "").strip().isdigit():
            findings.append(("STATUS", eid, "contradicted entry carries no issue"))

    matches = 0
    for rel in docs:
        if rel not in texts:
            continue
        lines = texts[rel].splitlines()
        for lineno, text, _heading in iter_prose_lines(lines):
            if not _MARKER_RE.search(text):
                continue
            matches += 1
            if lineno not in covered.get(rel, ()):
                findings.append(("NEGATIVE", f"{rel}:{lineno}",
                                 f"unregistered marker line: {text.strip()}"))
    if matches == 0:
        raise CheckError("zero marker matches across all normative docs; the scan is broken")
    return findings, len(entries), matches


def run(root=REPO_ROOT, out=sys.stdout, err=sys.stderr):
    try:
        findings, n_entries, n_matches = collect_findings(root)
    except CheckError as exc:
        err.write(f"check-doc-claims: cannot run: {exc}\n")
        return 2
    for rule, where, msg in findings:
        out.write(f"{rule} {where}: {msg}\n")
    if findings:
        out.write(f"\n{len(findings)} finding(s). Registry: {REGISTRY} (ADR-1658).\n")
        return 1
    out.write(f"doc claims: clean ({n_entries} entries, {n_matches} marker lines).\n")
    return 0


def main():
    return run()


if __name__ == "__main__":
    sys.exit(main())
