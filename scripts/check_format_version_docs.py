#!/usr/bin/env python3
"""Tie each bulk-format doc's declared supported-version set to the reader's.

The format-lifecycle documents drifted from the shipped reader once already
(issue #531: the docs described an N/N-1 window and pre-release posture while the
reader admitted exactly one version and a bump deleted the old reader outright).
This gate makes that class of drift fail at authoring time: for each bulk data
format it parses the reader's supported-version set out of the source of truth
(`SUPPORTED_VERSIONS` in the format crate) and compares it to a machine-checkable
marker the matching format doc carries:

    <!-- reader-supported-versions: ravel_segment = 7 -->

Python 3 standard library only, matching scripts/check_docs.py: CI's doc-scripts
job installs no toolchain beyond the interpreter, so the reader's set is parsed
from source text rather than by building and running the crate.

Fail-closed by construction:
  - The checker's own comparison logic is exercised against synthetic inputs
    (including a deliberate mismatch that MUST be flagged) before the repo is
    read; if that self-check does not behave, the gate fails rather than going
    quiet. This is the "a scan that finds nothing to compare is itself a failure"
    rule, made mechanical.
  - A source whose SUPPORTED_VERSIONS cannot be parsed, a version constant that
    cannot be resolved, an empty parsed set, a doc missing its marker, or a doc
    carrying more than one marker for the same crate each FAIL the gate. Only an
    exact set match passes.

Usage:
    python3 scripts/check_format_version_docs.py            # gate
    python3 scripts/check_format_version_docs.py --print    # show parsed sets
    python3 scripts/check_format_version_docs.py --selftest # run only the self-check

Exit codes: 0 clean, 1 a doc/source mismatch, 2 the checker could not run
(unparseable source, missing marker, broken self-check).
"""

import argparse
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# (doc path, crate label used in the marker, source file that defines
# SUPPORTED_VERSIONS). One row per bulk data-object format (ADR-0066 Class A).
MAPPINGS = [
    ("docs/segment-format.md", "ravel_segment", "crates/ravel-segment/src/format.rs"),
    ("docs/log-segment-format.md", "ravel_logseg", "crates/ravel-logseg/src/footer.rs"),
    ("docs/span-segment-format.md", "ravel_rspan", "crates/ravel-rspan/src/footer.rs"),
]


class CheckError(Exception):
    """The checker cannot run: unparseable source, unresolved constant, or a
    doc missing its marker. Distinct from a clean mismatch, which is a finding."""


# --------------------------------------------------------------------------
# Source parsing: the reader's supported-version set
# --------------------------------------------------------------------------

_SUPPORTED_RE = re.compile(
    r"pub\s+const\s+SUPPORTED_VERSIONS\s*:\s*SupportedVersions\s*=\s*([^;]+);",
    re.S,
)
_SINGLE_RE = re.compile(r"SupportedVersions::single\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)")
_N_AND_PREV_RE = re.compile(
    r"SupportedVersions::n_and_prev\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)"
)
_OVER_WINDOW_RE = re.compile(
    r"SupportedVersions::over\(\s*([A-Za-z_][A-Za-z0-9_]*)::WINDOW\s*\)"
)


def _resolve_const(src, ident):
    """The numeric value of `pub const <ident>: u16 = <n>;` in `src`."""
    m = re.search(
        r"pub\s+const\s+" + re.escape(ident) + r"\s*:\s*u16\s*=\s*(\d+)\s*;", src
    )
    if not m:
        raise CheckError(f"cannot resolve version constant {ident}")
    return int(m.group(1))


def _window_versions(src, enum_name):
    """The version numbers named by `pub const WINDOW: ... = &[ ... ];`.

    Each element `<enum_name>::V<k>` maps to the constant `VERSION_V<k>`, the
    convention the format crate's `number()` match uses.
    """
    m = re.search(
        r"pub\s+const\s+WINDOW\s*:\s*&'static\s*\[\s*"
        + re.escape(enum_name)
        + r"\s*\]\s*=\s*&\[([^\]]*)\]\s*;",
        src,
    )
    if not m:
        raise CheckError(f"cannot find {enum_name}::WINDOW slice")
    variants = re.findall(re.escape(enum_name) + r"::V(\d+)", m.group(1))
    if not variants:
        raise CheckError(f"{enum_name}::WINDOW names no version variant")
    return {_resolve_const(src, f"VERSION_V{k}") for k in variants}


def supported_versions_from_source(src, label):
    """The set of version numbers `SUPPORTED_VERSIONS` admits, from source text."""
    m = _SUPPORTED_RE.search(src)
    if not m:
        raise CheckError(f"{label}: no SUPPORTED_VERSIONS definition found")
    rhs = m.group(1)

    over = _OVER_WINDOW_RE.search(rhs)
    if over:
        versions = _window_versions(src, over.group(1))
    elif _N_AND_PREV_RE.search(rhs):
        newest = _resolve_const(src, _N_AND_PREV_RE.search(rhs).group(1))
        versions = {newest, newest - 1}
    elif _SINGLE_RE.search(rhs):
        versions = {_resolve_const(src, _SINGLE_RE.search(rhs).group(1))}
    else:
        raise CheckError(
            f"{label}: SUPPORTED_VERSIONS uses an unrecognized constructor: {rhs.strip()!r}"
        )

    if not versions:
        raise CheckError(f"{label}: parsed an empty supported-version set")
    return versions


# --------------------------------------------------------------------------
# Doc parsing: the declared marker
# --------------------------------------------------------------------------


def _marker_re(label):
    return re.compile(
        r"<!--\s*reader-supported-versions:\s*"
        + re.escape(label)
        + r"\s*=\s*([0-9, ]+?)\s*-->"
    )


def supported_versions_from_doc(text, label):
    """The set the doc declares for `label`, from its marker. Missing or
    duplicated marker is a CheckError (fail closed), not a silent pass."""
    hits = _marker_re(label).findall(text)
    if not hits:
        raise CheckError(
            f"{label}: no reader-supported-versions marker found in the doc "
            f"(expected a comment like `<!-- reader-supported-versions: {label} = N -->`)"
        )
    if len(hits) > 1:
        raise CheckError(f"{label}: {len(hits)} markers found, expected exactly one")
    nums = [int(p) for p in hits[0].replace(" ", "").split(",") if p != ""]
    if not nums:
        raise CheckError(f"{label}: marker declares no version")
    return set(nums)


# --------------------------------------------------------------------------
# Comparison (exercised by the self-check and by the repo run)
# --------------------------------------------------------------------------


def compare(label, source_set, doc_set):
    """None when the sets match, else a one-line mismatch description."""
    if source_set != doc_set:
        return (
            f"{label}: doc declares {sorted(doc_set, reverse=True)} but "
            f"the reader's SUPPORTED_VERSIONS is {sorted(source_set, reverse=True)}"
        )
    return None


def _selftest():
    """Prove the comparison and the parsers behave, INCLUDING the mismatch path,
    before any repo file is read. If any case misbehaves the checker is broken
    and the gate must fail rather than report a false clean."""
    ok = True

    # A match passes; a mismatch is flagged; the mismatch path is what keeps the
    # real run from silently passing when the doc is wrong.
    if compare("x", {7}, {7}) is not None:
        print("selftest: equal sets were flagged as a mismatch", file=sys.stderr)
        ok = False
    if compare("x", {7}, {6}) is None:
        print("selftest: a real mismatch was NOT flagged", file=sys.stderr)
        ok = False
    if compare("x", {7, 6}, {7}) is None:
        print("selftest: a set-size mismatch was NOT flagged", file=sys.stderr)
        ok = False

    # Each source constructor form parses to the expected set.
    over_src = (
        "pub const VERSION_V6: u16 = 6;\n"
        "pub const VERSION_V7: u16 = 7;\n"
        "pub const WINDOW: &'static [SegmentVersion] = &[SegmentVersion::V7];\n"
        "pub const SUPPORTED_VERSIONS: SupportedVersions = "
        "SupportedVersions::over(SegmentVersion::WINDOW);\n"
    )
    two_wide_src = over_src.replace(
        "&[SegmentVersion::V7]", "&[SegmentVersion::V7, SegmentVersion::V6]"
    )
    single_src = (
        "pub const VERSION: u16 = 4;\n"
        "pub const SUPPORTED_VERSIONS: SupportedVersions = "
        "SupportedVersions::single(VERSION);\n"
    )
    n_prev_src = (
        "pub const VERSION: u16 = 4;\n"
        "pub const SUPPORTED_VERSIONS: SupportedVersions = "
        "SupportedVersions::n_and_prev(VERSION);\n"
    )
    cases = [
        (over_src, {7}),
        (two_wide_src, {6, 7}),
        (single_src, {4}),
        (n_prev_src, {3, 4}),
    ]
    for src, want in cases:
        got = supported_versions_from_source(src, "selftest")
        if got != want:
            print(f"selftest: parsed {got}, wanted {want}", file=sys.stderr)
            ok = False

    # An unparseable source and a missing marker both fail closed.
    for bad in ("pub const SUPPORTED_VERSIONS: SupportedVersions = whatever();", ""):
        try:
            supported_versions_from_source(bad, "selftest")
        except CheckError:
            pass
        else:
            print("selftest: an unparseable source did not fail closed", file=sys.stderr)
            ok = False
    try:
        supported_versions_from_doc("no marker here", "ravel_segment")
    except CheckError:
        pass
    else:
        print("selftest: a missing marker did not fail closed", file=sys.stderr)
        ok = False

    return ok


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


def run(print_only=False, out=sys.stdout):
    mismatches = []
    compared = 0
    for doc_rel, label, src_rel in MAPPINGS:
        src_path = os.path.join(REPO_ROOT, src_rel)
        doc_path = os.path.join(REPO_ROOT, doc_rel)
        try:
            with open(src_path, "r", encoding="utf-8") as fh:
                src = fh.read()
            with open(doc_path, "r", encoding="utf-8") as fh:
                text = fh.read()
        except OSError as exc:
            raise CheckError(f"{label}: cannot read a file: {exc}") from exc

        source_set = supported_versions_from_source(src, label)
        if print_only:
            out.write(f"{label}: source={sorted(source_set, reverse=True)} ({src_rel})\n")
            continue
        doc_set = supported_versions_from_doc(text, label)
        compared += 1
        m = compare(label, source_set, doc_set)
        if m:
            mismatches.append((doc_rel, m))

    if print_only:
        return 0

    # A run that compared nothing is itself a failure (the mapping list is
    # empty, or every doc was skipped): there is no such thing as a vacuous pass.
    if compared == 0:
        raise CheckError("no format doc was compared; nothing to enforce")

    for doc_rel, m in mismatches:
        out.write(f"MISMATCH  {doc_rel}\t{m}\n")
    if mismatches:
        out.write(f"\n{len(mismatches)} format doc(s) disagree with the reader.\n")
        return 1
    out.write(f"format-version docs: clean ({compared} formats checked).\n")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--print", action="store_true", dest="print_only",
                        help="print the parsed source sets and exit")
    parser.add_argument("--selftest", action="store_true",
                        help="run only the internal self-check")
    args = parser.parse_args(argv)

    # The self-check runs first on every invocation (except --print), so a
    # checker broken into always-passing fails here rather than reporting a
    # false clean over the repo.
    if not args.print_only:
        if not _selftest():
            print("check_format_version_docs: self-check failed", file=sys.stderr)
            return 2
    if args.selftest:
        print("check_format_version_docs: self-check passed")
        return 0

    try:
        return run(print_only=args.print_only)
    except CheckError as exc:
        print(f"check_format_version_docs: cannot run: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
