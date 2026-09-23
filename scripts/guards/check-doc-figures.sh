#!/usr/bin/env bash
# Doc-figure guard: every operator-facing restatement of a derived
# record-cache figure matches what the ravel-catalog constants produce, and
# every occurrence is accounted for rather than only the first.
#
# This is the LOCAL, cargo-free half of
# `crates/ravel-catalog/tests/operator_docs_record_cache_figures.rs` and
# `disable_cache_help_states_the_record_cache_figures_the_constants_derive`
# in `services/ravel-server/src/config.rs`. Those run under cargo, which this
# repository's sessions do not run locally, so their findings arrive from CI
# twenty minutes after a push. Issue #1927's review spent four rounds on
# defects this scan finds in under a second:
#
#   drifted        a doc states a figure that the constants do not produce.
#                    #1904 shipped prose advertising 30,000 entries at a
#                    750-byte rate while the code shipped 25,000 at 900, a
#                    20 percent under-provisioning for an operator sizing a
#                    host from the docs.
#   unaccounted    a figure occurs more times in a doc than this scan has
#                    expectations for. That is the drift-across-copies
#                    failure: three restatements of the disabled-cache cost
#                    were unpinned while the fourth was pinned, and each
#                    could state a wrong number with the suite green.
#
# The figures are DERIVED here from the constants, never typed, so a constant
# change fails this scan rather than silently agreeing with stale prose. The
# comparison collapses whitespace on both sides, because the guides wrap
# mid-phrase and where a line wraps is a formatting choice rather than a
# claim.
#
# Usage:
#   scripts/guards/check-doc-figures.sh          # scan the shipped docs
#
# Exit: 0 clean, 1 finding(s), 64 bad usage, 70 the scan itself failed (a
# constant not found, a doc missing) -- never a silent pass, since an empty
# finding list after a failed scan is indistinguishable from a clean tree.
set -uo pipefail

if [[ $# -gt 0 ]]; then
  echo "check-doc-figures.sh: takes no arguments (got: $*)" >&2
  exit 64
fi

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 70

exec python3 - "${repo_root}" <<'PY'
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])

CONFIG = root / "crates/ravel-catalog/src/config.rs"
SERVER_CONFIG = root / "services/ravel-server/src/config.rs"
GUIDES = {
    "docs/guides/caching.md": root / "docs/guides/caching.md",
    "docs/guides/operations.md": root / "docs/guides/operations.md",
    "docs/catalog-and-mvcc.md": root / "docs/catalog-and-mvcc.md",
}

# A sentence that must NOT track the constants: it describes the cache this
# one replaced. Counting it would demand that history change whenever a
# constant does. Asserted present, so an exclusion that stops matching fails
# loudly instead of silently widening a count.
EXCLUSIONS = {
    "docs/guides/caching.md": ["the old cache held a flat 10,000 records"],
    # `head_cache_capacity` is an unrelated constant that happens to share the
    # record-cache floor's value. A bare-number marker cannot tell them apart,
    # and tying this scan to a figure it does not own would make a change to
    # either one look like drift in the other.
    "docs/catalog-and-mvcc.md": ["`head_cache_capacity`, default 10,000"],
}


def die(msg: str) -> None:
    print(f"check-doc-figures.sh: {msg}", file=sys.stderr)
    print("  Refusing to report a result: a clean scan and a scan that could", file=sys.stderr)
    print("  not run are different answers.", file=sys.stderr)
    raise SystemExit(70)


def constant(name: str) -> int:
    """Read `pub const <name>: <ty> = <int>;` out of ravel-catalog's config."""
    if not CONFIG.is_file():
        die(f"{CONFIG} is not readable")
    text = CONFIG.read_text()
    m = re.search(
        rf"(?m)^\s*pub const {re.escape(name)}\s*:\s*[A-Za-z0-9_]+\s*=\s*([0-9_]+)\s*;",
        text,
    )
    if not m:
        die(f"constant {name} not found in crates/ravel-catalog/src/config.rs")
    return int(m.group(1).replace("_", ""))


def normalize(text: str) -> str:
    return " ".join(text.split())


def commas(n: int) -> str:
    return f"{n:,}"


def scaled(byte_count: int, scale: int) -> str:
    """One decimal place, trailing .0 trimmed -- what an author would write."""
    value = byte_count / scale
    rounded = round(value * 10) / 10
    return f"{rounded:.0f}" if abs(rounded - int(rounded)) < 1e-9 else f"{rounded:.1f}"


def count_figure(haystack: str, marker: str) -> int:
    """Occurrences of `marker` that do not continue a longer number.

    `.` and `,` are rejected alongside a digit because both are digit context
    in these docs: a thousands separator and a decimal point.
    """
    total = 0
    i = 0
    while True:
        j = haystack.find(marker, i)
        if j < 0:
            return total
        prev = haystack[j - 1] if j > 0 else ""
        if not (prev.isdigit() or prev in ".,"):
            total += 1
        i = j + max(len(marker), 1)


entry_bytes = constant("RECORD_CACHE_ENTRY_BYTES")
caches = constant("RECORD_CACHES_PER_TENANT")
budget = constant("MAX_RECORD_CACHE_BYTES_PER_TENANT")
floor = constant("DEFAULT_CACHE_CAPACITY_PER_TENANT")

MB, GB = 1_000_000, 1_000_000_000
derived_cap = budget // (entry_bytes * caches)
floor_share = floor * entry_bytes

# marker -> how many times each doc is expected to state it. A doc absent
# from a marker's map is not scanned for that marker.
EXPECTED = {
    f"{scaled(budget, MB)} MB": {"docs/guides/caching.md": 2, "docs/guides/operations.md": 1},
    f"{scaled(budget // caches, MB)} MB": {"docs/guides/caching.md": 1, "docs/guides/operations.md": 1},
    f"{scaled(floor_share, MB)} MB": {"docs/guides/caching.md": 1},
    f"{scaled(floor_share * caches, MB)} MB": {"docs/guides/caching.md": 3},
    f"{scaled(budget * 100, GB)} GB": {"docs/guides/caching.md": 1},
    # The uncapped per-tenant figure. Its entry count derives from `--shards
    # 64` rather than from these constants, so the count stays a literal here
    # exactly as it does in the Rust test; the BYTE figure does not.
    f"{scaled(2_073_600 * entry_bytes * caches, GB)} GB": {"docs/guides/caching.md": 1},
    commas(floor): {"docs/guides/caching.md": 3, "docs/catalog-and-mvcc.md": 1},
    commas(derived_cap): {"docs/guides/caching.md": 1, "docs/catalog-and-mvcc.md": 2},
}

findings: list[str] = []
scanned = 0

for rel, path in GUIDES.items():
    if not path.is_file():
        die(f"{rel} is not readable")
    text = normalize(path.read_text())
    for phrase in EXCLUSIONS.get(rel, []):
        if phrase not in text:
            die(
                f"{rel}: the exclusion {phrase!r} is not present, so it excludes "
                "nothing and every count below silently changed meaning"
            )
        text = text.replace(phrase, "")
    for marker, per_doc in EXPECTED.items():
        if rel not in per_doc:
            continue
        scanned += 1
        want = per_doc[rel]
        got = count_figure(text, marker)
        if got != want:
            findings.append(
                f"{rel}: states {marker!r} {got} time(s), expected {want}. "
                f"Derived from RECORD_CACHE_ENTRY_BYTES={entry_bytes}, "
                f"RECORD_CACHES_PER_TENANT={caches}, "
                f"MAX_RECORD_CACHE_BYTES_PER_TENANT={budget}, "
                f"DEFAULT_CACHE_CAPACITY_PER_TENANT={floor}. Either the prose "
                f"drifted off the constants, or a restatement was added or "
                f"removed without updating this guard and "
                f"crates/ravel-catalog/tests/operator_docs_record_cache_figures.rs."
            )

# The clap long help for --disable-cache is a fourth operator-facing copy,
# and `ravel-server --help` is the surface with the least indirection between
# these figures and someone sizing a container.
if not SERVER_CONFIG.is_file():
    die(f"{SERVER_CONFIG} is not readable")
server_text = SERVER_CONFIG.read_text()
block = re.search(r"((?:^\s*///.*\n)+)\s*#\[arg\(long\)\]\s*\n\s*pub disable_cache: bool,", server_text, re.M)
if not block:
    die("the --disable-cache doc comment was not found in services/ravel-server/src/config.rs")
help_text = normalize(
    "\n".join(line.strip().removeprefix("///").strip() for line in block.group(1).splitlines())
)
for needle in (
    f"{commas(floor)} entries",
    f"{scaled(floor_share, MB)} MB byte budget in each of the two caches",
    f"about {scaled(floor_share * caches, MB)} MB per actively-queried tenant",
):
    scanned += 1
    if needle not in help_text:
        findings.append(
            f"services/ravel-server/src/config.rs: the --disable-cache long help "
            f"does not state {needle!r}, which the ravel-catalog constants derive. "
            f"`ravel-server --help` states these figures to an operator sizing a "
            f"memory-constrained container."
        )

if findings:
    for f in sorted(findings):
        print(f)
    print(f"check-doc-figures.sh: {len(findings)} finding(s)", file=sys.stderr)
    raise SystemExit(1)

print(f"check-doc-figures.sh: clean ({scanned} figure expectations over {len(GUIDES) + 1} files)")
PY
