#!/usr/bin/env bash
# Single-source guard for one claim: what a `0` on
# `ravel_store_probe_last_run_timestamp_seconds` means.
#
# That explanation had been written out in ten places (the probe source, its
# tests, the shipped Prometheus rule, the observability guide, the changelog),
# and every copy stated it wrongly in the same way: one cause instead of three.
# Four successive sweeps tried to keep the copies consistent and two of them
# ADDED copies while removing others. A sweep cannot fix that; one home plus a
# check can, which is what issue #1982 asked for and what this is.
#
# The shape is the sibling `check-doc-figures.sh`: a cargo-free python scan
# with per-site expectations that are asserted present, so a site that moves
# refuses rather than quietly reducing what is checked.
#
# Four rules, each of which a new copy has to get past:
#
#   canonical    Exactly one canonical block exists, delimited by the marker
#                  comments below, in docs/guides/observability.md, under the
#                  heading whose anchor every pointer names, and it states each
#                  of the three causes exactly once. A second block anywhere is
#                  a finding; a block that moved to another file, lost a
#                  marker, or drifted off its anchor refuses.
#   pointers     Every registered site still carries the number of pointers to
#                  that section the registry records. A pointer rewritten back
#                  into prose stops matching and fails here.
#   tells        Around every mention of the gauge (or of the atomic and
#                  accessors behind it), in every file in the tree, a phrase
#                  that only appears when someone is explaining the claim is a
#                  finding. This is the rule that makes a TENTH copy fail:
#                  restating the causes means naming them, and naming them
#                  means a tell. Restricted to a window around the gauge so
#                  unrelated prose elsewhere cannot trip it.
#   help         The gauge's HELP line is the one deliberate exception (it
#                  ships in /metrics output, where the reader has no link to
#                  follow), so it carries a one-line summary instead of a
#                  pointer. It is checked to still name all three causes rather
#                  than being left to drift as the tenth copy by another route.
#
# An exemption is spelled `claim-allow: store-probe-zero -- <reason>` on its own
# comment line; it suppresses that line, the rest of its comment block, and the
# first line below the block. The reason is required. Every exemption must be
# registered below, so adding one is a guard change and not a quiet one.
#
# Usage:
#   scripts/guards/check-claim-single-source.sh          # scan the tree
#
# Exit: 0 clean, 1 finding(s), 64 bad usage, 70 the scan itself failed (a
# registered file missing, the canonical block or the HELP line not found) --
# never a silent pass, since an empty finding list after a failed scan is
# indistinguishable from a clean tree.
set -uo pipefail

if [[ $# -gt 0 ]]; then
  echo "check-claim-single-source.sh: takes no arguments (got: $*)" >&2
  exit 64
fi

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 70

exec python3 - "${repo_root}" <<'PY'
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])

CANONICAL_FILE = "docs/guides/observability.md"
CANONICAL_ANCHOR = "what-0-means"
BEGIN = "<!-- claim:store-probe-zero:canonical-begin -->"
END = "<!-- claim:store-probe-zero:canonical-end -->"

# The three causes, as the canonical block must name them. Each exactly once:
# a cause stated twice in the one home is the drift this guard exists to stop,
# one section lower down.
CAUSE_PHRASES = [
    "no probe task in this process",
    "the startup window",
    "a pre-1970 host clock",
]

# The HELP line's own one-line summary must name the same three. Shorter forms,
# because it is a single exposition line and not prose.
HELP_PHRASES = [
    "no probe task in this process",
    "startup window",
    "pre-1970 host clock",
]

GAUGE = "ravel_store_probe_last_run_timestamp_seconds"

# A mention of any of these opens a window the tells are scanned in. The atomic
# and its accessors are here as well as the exported name, because the Rust
# copies discussed the claim beside the storage rather than beside the gauge.
ANCHOR_TERMS = [
    GAUGE,
    "PROBE_LAST_RUN_UNIX_NS",
    "probe_last_run_unix_ns",
    "stamp_last_run",
]

# Lines either side of an anchor mention that count as discussing it. A doc
# comment block plus its item, or one Prometheus rule's comment, fits inside
# this; unrelated prose in the same large file does not.
WINDOW = 12

# Phrasings that only turn up when the claim is being explained rather than
# pointed at. Matched case-insensitively against the whitespace-normalized
# window, with comment markers stripped, so a wrapped sentence still matches.
TELLS = [
    ("never-spawned", r"never (?:been |ever )?spawn"),
    ("no-probe-task", r"no probe (?:task|was ever)"),
    ("was-never-called", r"was never called"),
    ("startup-window", r"startup window"),
    ("pre-1970-clock", r"pre-?1970|before (?:the )?(?:unix )?epoch|earlier than the unix epoch"),
    ("unwrap-or-zero", r"unwrap_or\(0\)"),
    # Both directions of "this reading has one meaning", tied to a zero in the
    # same clause: an unqualified "means only" is ordinary prose (the
    # deployment guide says liveness "means only that the process is alive").
    (
        "single-meaning",
        r"(?:`0`|\b0\b|zero)[^.]{0,80}?(?:exactly one thing|means only|only one (?:thing|meaning|cause))"
        r"|(?:exactly one thing|only one (?:thing|meaning|cause))[^.]{0,80}?(?:`0`|\b0\b|zero)",
    ),
    ("ambiguous-value", r"ambiguous (?:sentinel|value)|no ambiguous|carries no ambiguous"),
]

# A pointer to the canonical section. Two spellings: the Markdown link used
# inside the guide, and the prose reference used from source, YAML and the
# changelog, where a relative Markdown link would not resolve.
POINTER_RE = re.compile(
    r"\(#what-0-means\)|what\s+.?0.?\s+means.{0,40}?docs/guides/observability\.md",
    re.I,
)

# (path, how many pointers it carries). A path absent from this map is not
# required to point anywhere; a path present with a count is asserted, so a
# pointer rewritten back into prose, or a new one added without registering it,
# fails here. The file must exist: a registered site that moved refuses.
POINTER_SITES = {
    "services/ravel-server/src/store_probe.rs": 3,
    # Two: the test comment beside the zero-valued render, and the HELP line's
    # own trailing reference, which the help rule below also checks separately.
    "services/ravel-server/src/metrics.rs": 2,
    "services/ravel-server/tests/readyz_e2e.rs": 1,
    "services/ravel-server/tests/shipped_rules_name_emitted_metrics.rs": 1,
    "deploy/prometheus/ravel.rules.yaml": 1,
    "CHANGELOG.md": 1,
    # The table row, the alert-derivation paragraph, and the comment inside the
    # reprinted rule block. The canonical block itself points at nothing.
    CANONICAL_FILE: 3,
}

# The reason is optional to MATCH and required to be non-empty: a marker
# spelled with an empty reason has to be counted (so its site's registered
# count still holds) and reported, not skipped into invisibility.
ALLOW_RE = re.compile(r"claim-allow:\s*store-probe-zero\b[ \t]*(?:--(.*))?$")

# Every exemption in the tree, by path and count, asserted the same way the
# pointers are. The one that exists is the HELP line.
ALLOW_SITES = {
    "services/ravel-server/src/metrics.rs": 1,
}

SCAN_SUFFIXES = {".rs", ".md", ".yaml", ".yml", ".toml", ".json", ".sh", ".py"}
SKIP_DIRS = {".git", "target", "node_modules", ".gate-logs", ".dd-tools", "proto"}

# This guard and its cases quote the markers and the tells, so scanning them
# would report the guard on itself.
SELF = {
    "scripts/guards/check-claim-single-source.sh",
    "scripts/guards/check-claim-single-source.test.sh",
}


def die(msg: str) -> None:
    print(f"check-claim-single-source.sh: {msg}", file=sys.stderr)
    print("  Refusing to report a result: a clean scan and a scan that could", file=sys.stderr)
    print("  not run are different answers.", file=sys.stderr)
    raise SystemExit(70)


def tree_files() -> list[tuple[str, str]]:
    """(relative path, text) for every scannable file, sorted."""
    out = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix not in SCAN_SUFFIXES:
            continue
        rel = path.relative_to(root).as_posix()
        if any(part in SKIP_DIRS for part in path.relative_to(root).parts[:-1]):
            continue
        if rel in SELF:
            continue
        try:
            out.append((rel, path.read_text()))
        except (UnicodeDecodeError, OSError):
            continue
    return out


def strip_comment_prefix(line: str) -> str:
    """Drop a leading comment marker so a wrapped sentence normalizes flat."""
    s = line.strip()
    for marker in ("///", "//!", "//", "#!", "#", "<!--", "*"):
        if s.startswith(marker):
            s = s[len(marker) :].strip()
            break
    return s.removesuffix("-->").strip()


def normalize(text: str) -> str:
    return " ".join(
        " ".join(strip_comment_prefix(line) for line in text.splitlines()).split()
    )


def suppressed_lines(lines: list[str]) -> tuple[set[int], int, list[str]]:
    """Line indices an exemption marker covers, the marker count, and gripes.

    A marker on its own comment line covers that line, the rest of its comment
    block, and the first line under the block (the item the block documents,
    which for the HELP line is the string literal itself). A marker with no
    reason covers nothing, so an empty reason cannot be used to silence a
    finding.
    """
    covered: set[int] = set()
    markers = 0
    gripes: list[str] = []
    i = 0
    while i < len(lines):
        m = ALLOW_RE.search(lines[i])
        if not m:
            i += 1
            continue
        markers += 1
        if not (m.group(1) or "").strip():
            gripes.append(f"line {i + 1}: exemption marker carries no reason")
            i += 1
            continue
        stripped = lines[i].strip()
        is_comment = any(
            stripped.startswith(p) for p in ("///", "//!", "//", "#", "<!--", "*")
        )
        covered.add(i)
        if not is_comment:
            i += 1
            continue
        j = i + 1
        while j < len(lines):
            s = lines[j].strip()
            covered.add(j)
            if not any(
                s.startswith(p) for p in ("///", "//!", "//", "#", "<!--", "*")
            ):
                break
            j += 1
        i = j + 1
    return covered, markers, gripes


def anchor_of(heading: str) -> str:
    text = heading.lstrip("#").strip()
    text = text.replace("`", "")
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s+", "-", text.strip()).lower()


findings: list[str] = []
files = tree_files()
if not files:
    die(f"no scannable files under {root}")

# --- canonical block ---------------------------------------------------------
begins = [(rel, text.count(BEGIN)) for rel, text in files if BEGIN in text]
ends = [(rel, text.count(END)) for rel, text in files if END in text]
total_begin = sum(n for _, n in begins)
total_end = sum(n for _, n in ends)
if total_begin == 0 or total_end == 0:
    die(
        "the canonical block markers were not found anywhere in the tree "
        f"({BEGIN} / {END}); the one home for this claim has moved or been "
        "deleted, so there is nothing for the pointers to point at"
    )
if [rel for rel, _ in begins] != [CANONICAL_FILE] or [rel for rel, _ in ends] != [
    CANONICAL_FILE
]:
    die(
        f"the canonical block must live in {CANONICAL_FILE}; found begin markers "
        f"in {[rel for rel, _ in begins]} and end markers in {[rel for rel, _ in ends]}"
    )
if total_begin != 1 or total_end != 1:
    findings.append(
        f"{CANONICAL_FILE}: {total_begin} canonical begin marker(s) and "
        f"{total_end} end marker(s), expected 1 of each. A second canonical "
        "block is a second copy of the claim, which is what this guard exists "
        "to refuse."
    )

canonical_path = root / CANONICAL_FILE
if not canonical_path.is_file():
    die(f"{CANONICAL_FILE} is not readable")
canonical_lines = canonical_path.read_text().splitlines()
begin_i = next(i for i, line in enumerate(canonical_lines) if BEGIN in line)
end_i = next(i for i, line in enumerate(canonical_lines) if END in line)
if end_i <= begin_i:
    die(f"{CANONICAL_FILE}: the canonical end marker precedes its begin marker")

canonical_block = normalize("\n".join(canonical_lines[begin_i + 1 : end_i]))
for phrase in CAUSE_PHRASES:
    got = canonical_block.count(phrase)
    if got != 1:
        findings.append(
            f"{CANONICAL_FILE}: the canonical block states {phrase!r} {got} "
            "time(s), expected 1. All three causes are stated there, once "
            "each; a missing one sends an operator hunting for a dead probe "
            "task that is running normally."
        )

heading = next(
    (
        canonical_lines[i]
        for i in range(begin_i - 1, -1, -1)
        if canonical_lines[i].startswith("#")
    ),
    None,
)
if heading is None:
    die(f"{CANONICAL_FILE}: the canonical block sits under no heading")
if anchor_of(heading) != CANONICAL_ANCHOR:
    findings.append(
        f"{CANONICAL_FILE}: the canonical block sits under {heading.strip()!r} "
        f"(anchor {anchor_of(heading)!r}), but every pointer names "
        f"#{CANONICAL_ANCHOR}. Moving the block under another heading breaks "
        "every pointer while leaving them textually intact."
    )

# --- pointers ----------------------------------------------------------------
for rel, want in sorted(POINTER_SITES.items()):
    path = root / rel
    if not path.is_file():
        die(f"{rel} is a registered pointer site and is not readable")
    got = len(POINTER_RE.findall(normalize(path.read_text())))
    if got != want:
        findings.append(
            f"{rel}: carries {got} pointer(s) to the canonical section, "
            f"expected {want}. Either a pointer was rewritten back into prose, "
            "or a new mention was added without registering it in "
            "scripts/guards/check-claim-single-source.sh."
        )

# --- exemptions --------------------------------------------------------------
allow_counts: dict[str, int] = {}
allow_covered: dict[str, set[int]] = {}
for rel, text in files:
    covered, markers, gripes = suppressed_lines(text.splitlines())
    for gripe in gripes:
        findings.append(
            f"{rel}: {gripe}. An exemption without a reason suppresses nothing."
        )
    if markers:
        allow_counts[rel] = markers
    allow_covered[rel] = covered

for rel, want in sorted(ALLOW_SITES.items()):
    if not (root / rel).is_file():
        die(f"{rel} is a registered exemption site and is not readable")
    got = allow_counts.get(rel, 0)
    if got != want:
        findings.append(
            f"{rel}: carries {got} exemption marker(s), expected {want}. "
            "Adding one is a guard change: register it, with the reason it is "
            "not a pointer."
        )
for rel, got in sorted(allow_counts.items()):
    if rel not in ALLOW_SITES:
        findings.append(
            f"{rel}: carries {got} unregistered exemption marker(s). An "
            "exemption nobody registered is a copy nobody reviewed."
        )

# --- tells -------------------------------------------------------------------
anchor_hits = 0
for rel, text in files:
    lines = text.splitlines()
    covered = allow_covered.get(rel, set())
    in_scope: set[int] = set()
    for i, line in enumerate(lines):
        if any(term in line for term in ANCHOR_TERMS):
            anchor_hits += 1
            in_scope.update(range(max(0, i - WINDOW), min(len(lines), i + WINDOW + 1)))
    if rel == CANONICAL_FILE:
        in_scope -= set(range(begin_i, end_i + 1))
    in_scope -= covered
    if not in_scope:
        continue
    # Maximal runs of in-scope lines, each normalized and scanned as one
    # chunk, so a claim wrapped across lines still matches.
    ordered = sorted(in_scope)
    chunks: list[tuple[int, list[str]]] = []
    for i in ordered:
        if chunks and i == chunks[-1][0] + len(chunks[-1][1]):
            chunks[-1][1].append(lines[i])
        else:
            chunks.append((i, [lines[i]]))
    for start, chunk in chunks:
        haystack = normalize("\n".join(chunk))
        for name, pattern in TELLS:
            if re.search(pattern, haystack, re.I):
                findings.append(
                    f"{rel}:{start + 1}: restates the store-probe `0` claim "
                    f"({name} tell, /{pattern}/) within {WINDOW} lines of a "
                    f"mention of the gauge. That claim has one home, the "
                    f"'What `0` means' section of {CANONICAL_FILE}; point at "
                    "it instead. If this really cannot be a pointer, add a "
                    "`claim-allow: store-probe-zero -- <reason>` comment and "
                    "register the site in this guard."
                )

if anchor_hits == 0:
    die(
        "no mention of "
        + ", ".join(ANCHOR_TERMS)
        + " was found anywhere in the tree, so the tell scan covered nothing; "
        "the gauge was renamed, or this scan is pointed at the wrong tree"
    )

# --- the HELP exception ------------------------------------------------------
metrics_rel = "services/ravel-server/src/metrics.rs"
metrics_path = root / metrics_rel
if not metrics_path.is_file():
    die(f"{metrics_rel} is not readable")
help_match = re.search(
    rf'"{re.escape(GAUGE)}"\s*,(?:\s*//[^\n]*)*\s*\n\s*"((?:[^"\\]|\\.)*)"',
    metrics_path.read_text(),
)
if not help_match:
    die(
        f"the {GAUGE} HELP string was not found in {metrics_rel}; it is the one "
        "site that carries a summary rather than a pointer, so a scan that "
        "cannot find it is checking the exception against nothing"
    )
help_text = help_match.group(1)
for phrase in HELP_PHRASES:
    got = help_text.lower().count(phrase)
    if got != 1:
        findings.append(
            f"{metrics_rel}: the {GAUGE} HELP line names {phrase!r} {got} "
            "time(s), expected 1. It ships in /metrics output, where the "
            "reader has no link to follow, so it summarizes all three causes "
            "rather than pointing."
        )

if findings:
    for f in sorted(findings):
        print(f)
    print(f"check-claim-single-source.sh: {len(findings)} finding(s)", file=sys.stderr)
    raise SystemExit(1)

print(
    f"check-claim-single-source.sh: clean (1 canonical block, "
    f"{len(POINTER_SITES)} pointer sites, {len(ALLOW_SITES)} registered "
    f"exemption, {len(TELLS)} tells over {anchor_hits} gauge mentions in "
    f"{len(files)} files)"
)
PY
