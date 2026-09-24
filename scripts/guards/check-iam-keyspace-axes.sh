#!/usr/bin/env bash
# IAM key-space axis guard: every CONTROL-PLANE key space the code names is
# declared here, and every axis a declared owner role exercises over it is
# granted by that role's shipped template.
#
# The defect this exists for has shipped six times, always the same way: a
# grant is derived from the one function a ticket names instead of from every
# call site that touches the prefix, so one axis is left with no Allow at all.
# IAM is default-deny, so the missing axis is not a narrowing, it is a refusal.
# Issue #1975 is the latest: `MaintainDelete` named no `sys/` resource, so
# `WorkerSet::reap_keys` could never delete a dead worker's heartbeat and the
# per-tick LIST over `sys/maintain/workers/` grew without bound.
#
# The per-lifecycle reachability tests in
# `crates/ravel-commit/tests/iam_templates.rs` catch this precisely, with
# witness keys built by the real constructors -- but only for a lifecycle
# somebody remembered to write a test for. This scan needs nobody to remember:
# a new control-plane key space fails here until it is declared, and declaring
# it forces a per-axis answer.
#
# WHAT IT CHECKS
#
#   undeclared     a `const NAME: &str = "<root>..."` or a
#                    `format!("<root>...")` in any workspace source names a
#                    key space no MANIFEST entry covers. Discovery is the
#                    forcing function: the declaration cannot be skipped. A
#                    declaration covers everything beneath it, which is what
#                    lets `quarantine/` be declared once for the whole root
#                    the orphan sweep composes its keys under; declare a
#                    narrower key space when its axes differ from its
#                    parent's.
#   missing-grant  an owner role exercises an axis over a key space and its
#                    template grants that axis nowhere under it. This is the
#                    #1975 shape exactly.
#   stale-unused   an axis declared unused IS granted to an owner. Either the
#                    code gained a call and the note is stale, or the grant is
#                    unnecessary.
#   stale-gap      a KNOWN_GAPS entry whose grant now exists. The gap closed;
#                    delete the entry so the next one is visible.
#   stale-keyspace a MANIFEST key space no discovered literal names any more.
#                    A rename must move the declaration, not orphan it.
#
# WHAT IT DOES NOT CHECK, and why the reachability tests still matter
#
#   - Tenant-rooted key spaces (`t/...`). They are not built from constants
#     but composed by the constructors in `crates/ravel-commit/src/keys.rs`,
#     through `format!("{prefix}{...}")` chains whose components are const
#     interpolations and match-arm literals. `maint_cursor_key` renders as
#     `t/{}/{}/{}/{}/{}`, which no text scanner can distinguish from
#     `t/*/*/*/*/*`; deriving a glob from it soundly needs constant folding,
#     which needs the type system. So this scan is bounded to the roots whose
#     key spaces ARE single constants, where discovery was measured exact.
#   - Whether a grant covers the WHOLE key space. A pattern rooted anywhere
#     inside the key space counts as reaching it, so "granted but too narrow"
#     passes here. That distinction needs a real constructor-built witness,
#     which is what `maintain_template_covers_every_*_call` is for.
#   - Over-granting. `every_role_grants_exactly_the_expected_pattern_set`
#     pins every pattern of every role by exact equality already.
#
# Usage:
#   scripts/guards/check-iam-keyspace-axes.sh            # scan this repo
#   scripts/guards/check-iam-keyspace-axes.sh <root>     # scan a tree (tests)
#
# Exit: 0 clean, 1 finding(s), 64 bad usage, 70 the scan itself could not run
# (a template missing or unparseable, or a discovery floor not met) -- never a
# silent pass, since a scan that reaches nothing reports the same empty finding
# list as a clean tree.
set -uo pipefail

if [[ $# -gt 1 ]]; then
  echo "check-iam-keyspace-axes.sh: takes at most one argument (got: $*)" >&2
  exit 64
fi

if [[ $# -eq 1 ]]; then
  if [[ -z "$1" ]]; then
    echo "check-iam-keyspace-axes.sh: empty root argument" >&2
    echo "  A caller whose variable did not survive is not a caller asking" >&2
    echo "  for the default." >&2
    exit 64
  fi
  scan_root="$1"
else
  scan_root="$(cd "$(dirname "$0")/../.." && pwd)"
fi

exec python3 - "${scan_root}" <<'PY'
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])

# The object-key roots whose key spaces are single string constants. Every
# root here was measured to yield an exact key-space list with no false
# positives; `t/` was measured and deliberately excluded (see the header).
CONTROL_PLANE_ROOTS = ("sys/", "quarantine/", "admission/")

ROLES = ("gateway", "query", "maintain", "admin")
AXES = ("list", "get", "put", "delete")

# The IAM actions each axis is spelled with, and where a template states the
# object keys for it. `list` is the odd one: `s3:ListBucket` names the bucket
# as its Resource and the keys as `s3:prefix` condition values.
AXIS_ACTIONS = {
    "list": ("s3:ListBucket",),
    "get": ("s3:GetObject",),
    "put": ("s3:PutObject",),
    "delete": ("s3:DeleteObject", "s3:DeleteObjectVersion"),
}

# Anti-vacuity floors. A rename, a moved directory, or a tightened regex that
# empties the scan must fail rather than report every key space clean. Each is
# set well below what the tree holds today and above zero.
MIN_SOURCES = 200
MIN_LITERALS = 10
MIN_KEYSPACES_LIVE = 8

# Directory names never walked: build output, vendored trees, and the test
# fixtures this guard's own suite writes, which name key spaces on purpose.
SKIP_DIRS = {"target", "node_modules", ".git", ".gate-logs", ".dd-tools"}


def die(msg: str) -> None:
    print(f"check-iam-keyspace-axes.sh: {msg}", file=sys.stderr)
    print("  Refusing to report a result: a clean scan and a scan that could", file=sys.stderr)
    print("  not run are different answers.", file=sys.stderr)
    raise SystemExit(70)


# ---------------------------------------------------------------------------
# The manifest.
#
# One entry per control-plane key space. `owners` are the roles whose PROCESS
# runs the calls, not the roles that happen to hold a grant. Every axis needs
# an answer: a call site for a used axis, a reason for an unused one. Both are
# required to be non-empty, because an axis nobody wrote a word about is the
# axis that ships refused.
# ---------------------------------------------------------------------------
MANIFEST = [
    {
        "keyspace": "admission/query/",
        "adr": "ADR-0071 query admission snapshots",
        "owners": ["query"],
        "list": ("used", "query_admission.rs:327 list_all(store, QUERY_ADMISSION_PREFIX)"),
        "get": ("used", "query_admission.rs:336 store.get(&meta.key, GetRange::Full)"),
        "put": ("used", "query_admission.rs:400 publishes this process's snapshot"),
        "delete": (
            "unused",
            "a snapshot is overwritten in place under the writer's own key and "
            "ages out by its stamped time; query_admission.rs contains no delete",
        ),
    },
    {
        "keyspace": "quarantine/",
        "adr": "ADR-0058 decision 6 orphan quarantine",
        "owners": ["maintain"],
        "list": ("used", "sweep.rs:901 list_all over the quarantine l0 data prefix"),
        "get": (
            "unused",
            "nothing reads a quarantined object: sweep_orphans copies from the "
            "LIVE key, and no restore path exists in the code (issue #1978 "
            "tracks the operator-facing restore, which would add one)",
        ),
        "put": ("used", "sweep.rs:767 store.put(dest, got.data, ..) in the copy step"),
        "delete": ("used", "sweep.rs:921 store.delete past the second horizon"),
    },
    {
        "keyspace": "sys/auth",
        "adr": "auth token map",
        "owners": ["gateway", "query"],
        "list": ("unused", "a single object at a fixed key; nothing enumerates it"),
        "get": ("used", "auth_token_map.rs:422 store.get(AUTH_KEY), on every refresh tick"),
        "put": ("used", "auth_token_map.rs:496 put(AUTH_KEY, .., create_if_absent())"),
        "delete": ("unused", "the map is replaced by a write, never removed"),
    },
    {
        "keyspace": "sys/gc",
        "adr": "GC configuration record",
        "owners": ["gateway", "query", "maintain"],
        "list": ("unused", "a single object at a fixed key; nothing enumerates it"),
        "get": (
            "used",
            "gc_config.rs:322 store.get(GC_CONFIG_KEY); main.rs:351 bootstraps it "
            "in every process mode before validating maintain and query settings",
        ),
        "put": (
            "used",
            "gc_config.rs:369 put(GC_CONFIG_KEY, .., create_if_absent()) from the "
            "same unconditional main.rs:351 bootstrap, so whichever mode starts "
            "against a fresh bucket issues it",
        ),
        "delete": ("unused", "protected by DenyDeleteProtected in all four templates"),
    },
    {
        "keyspace": "sys/maintain/claims/compaction/",
        "adr": "ADR-1029 advisory compaction claims (Proposed)",
        "owners": ["maintain"],
        "list": (
            "unused",
            "a claim is addressed by its input-set identity, never enumerated; "
            "claim.rs does no listing",
        ),
        "get": ("used", "claim.rs:568 store.get(key, GetRange::Full)"),
        "put": ("used", "claim.rs:531 the CAS claim write"),
        "delete": (
            "unused",
            "ADR-1029 forbids it: claim.rs states there is no unconditional "
            "delete anywhere in the module, since deleting a claim is the write "
            "that would break the advisory guarantee",
        ),
    },
    {
        "keyspace": "sys/maintain/memo/",
        "adr": "maintain warm-start memo snapshots",
        "owners": ["maintain"],
        "list": ("used", "memo_snapshot.rs:81 list_all(store, MEMO_PREFIX)"),
        "get": ("used", "memo_snapshot.rs:85 store.get(&meta.key, GetRange::Full)"),
        "put": ("used", "memo_snapshot.rs:57 the handoff write"),
        "delete": ("unused", "a memo is overwritten under its own key, never removed"),
    },
    {
        "keyspace": "sys/maintain/workers/",
        "adr": "ADR-0065 decision 1 leased distributed maintenance",
        "owners": ["maintain"],
        "list": ("used", "worker_set.rs:403 list_all(store, WORKERS_PREFIX)"),
        "get": ("used", "worker_set.rs:424 store.get(&meta.key, GetRange::Full)"),
        "put": ("used", "worker_set.rs:358 write_heartbeat"),
        "delete": (
            "used",
            "worker_set.rs:463 reap_keys, driven from maintain.rs:1111. This is "
            "the axis issue #1975 found ungranted",
        ),
    },
    {
        "keyspace": "sys/qualification",
        "adr": "ADR-0050 section 6 backend qualification record",
        "owners": ["admin"],
        "list": ("unused", "a single object at a fixed key; nothing enumerates it"),
        "get": ("used", "ravel-cli qualify.rs:163 reads an existing record before re-recording"),
        "put": ("used", "ravel-cli qualify.rs:110 records a passing qualification"),
        "delete": ("unused", "protected by DenyDeleteProtected in all four templates"),
    },
    {
        "keyspace": "sys/qualify/",
        "adr": "ADR-0050 qualification probe scratch space",
        "owners": ["admin"],
        "list": (
            "unused",
            "each probe addresses the key it just wrote; conformance.rs lists "
            "only its own per-probe scratch prefix through the store handle it "
            "was given, and the operator template reaches that through sys/*",
        ),
        "get": (
            "used",
            "conformance.rs readback probes, e.g. :641 and :1053, under the "
            "scratch prefix qualify.rs:44 builds",
        ),
        "put": ("used", "conformance.rs:614 and the write probes under it"),
        "delete": ("used", "conformance.rs:1536 the delete-visibility probe cleans up"),
    },
    {
        "keyspace": "sys/query/workers/",
        "adr": "ADR-0071 query worker set",
        "owners": ["query"],
        "list": ("used", "query_workers.rs:358 list_all(store, QUERY_WORKERS_PREFIX)"),
        "get": ("used", "query_workers.rs:383 store.get(&meta.key, GetRange::Full)"),
        "put": ("used", "query_workers.rs:287 the heartbeat write"),
        "delete": (
            "used",
            "query_workers.rs:424 reap_keys, and query_workers.rs:308 the "
            "graceful-drain self-delete driven from distrib.rs:1818",
        ),
    },
    {
        "keyspace": "sys/t/",
        "adr": "ADR-0050 section 3 per-tenant recovery manifest",
        "owners": ["gateway"],
        "list": (
            "unused",
            "the manifest key is derived from the tenant hash, so recovery "
            "addresses it directly; the operator-facing enumeration runs under "
            "admin's blanket sys/* list",
        ),
        "get": (
            "unused",
            "no production reader: recovery is an operator action, and admin's "
            "blanket sys/* read is what serves it",
        ),
        "put": (
            "used",
            "tenancy.rs:512 RecoveryManifestWriter::ensure, on a tenant's first "
            "write through the ingest path",
        ),
        "delete": ("unused", "a recovery manifest is never removed while its tenant exists"),
    },
    {
        "keyspace": "sys/tenancy",
        "adr": "deployment tenancy marker",
        "owners": ["gateway", "query", "maintain", "admin"],
        "list": ("unused", "a single object at a fixed key; nothing enumerates it"),
        "get": ("used", "tenancy.rs:187 store.get(TENANCY_MARKER_KEY) at startup"),
        "put": ("used", "tenancy.rs:205 stamps the marker on a fresh bucket"),
        "delete": ("unused", "protected by DenyDeleteProtected in all four templates"),
    },
]

# Axes a role genuinely exercises today and its shipped template does NOT
# grant. Each is a real refusal on a shipped deployment, recorded rather than
# suppressed: the entry must carry a reason, and the guard fails again the
# moment the grant appears, so closing a gap cannot leave the table stale.
#
# Nothing may be added here to make a NEW change pass. A grant this guard
# demands is one a running process needs; the table is for gaps that predate
# the guard and need their own change.
KNOWN_GAPS = {
    ("sys/auth", "get", "gateway"): (
        "no template names sys/auth on any axis; only admin's blanket sys/* "
        "reaches it. Found while deriving #1975, out of that ticket's scope"
    ),
    ("sys/auth", "get", "query"): (
        "same gap as the gateway entry above: the auth token map is read on "
        "every refresh tick by both serving modes and granted to neither"
    ),
    ("sys/auth", "put", "gateway"): (
        "the create_if_absent bootstrap write of the auth token map is granted "
        "to no serving role"
    ),
    ("sys/auth", "put", "query"): (
        "same gap as the gateway entry above"
    ),
    ("sys/gc", "put", "gateway"): (
        "main.rs:351 bootstraps the GC config in every mode, but only "
        "MaintainWrite names sys/gc. A deployment whose gateway starts first "
        "against a fresh bucket is refused; one where maintain started first "
        "never issues the put, which is why this has not been noticed"
    ),
    ("sys/gc", "put", "query"): (
        "same ordering-dependent gap as the gateway entry above"
    ),
    ("sys/maintain/memo/", "list", "maintain"): (
        "MaintainList carries no sys/maintain/memo/* prefix, so the warm-start "
        "handoff read at memo_snapshot.rs:81 is refused. It fails open, so the "
        "only symptom is a cold start every time"
    ),
    ("sys/query/workers/", "delete", "query"): (
        "query.json has no Allow delete statement at all, so both the reap at "
        "query_workers.rs:424 and the graceful-drain self-delete at "
        "query_workers.rs:308 are refused. This is #1975's defect in the query "
        "role, found while deriving it and out of that ticket's scope"
    ),
    ("sys/t/", "put", "gateway"): (
        "no template names sys/t/*; the templates carry only the literal "
        "sys/tenancy, so every recovery manifest write is refused"
    ),
}


# ---------------------------------------------------------------------------
# Manifest self-checks. A malformed declaration must stop the scan, not narrow
# it: an entry with a typo in a role or axis name would otherwise be skipped
# and read as clean.
# ---------------------------------------------------------------------------
_seen_keyspaces: set[str] = set()
for entry in MANIFEST:
    ks = entry["keyspace"]
    if ks in _seen_keyspaces:
        die(f"MANIFEST declares {ks!r} twice; one declaration would be dead")
    _seen_keyspaces.add(ks)
    if not ks.startswith(CONTROL_PLANE_ROOTS):
        die(f"MANIFEST entry {ks!r} is not under a scanned root {CONTROL_PLANE_ROOTS}")
    if not entry["owners"]:
        die(f"MANIFEST entry {ks!r} names no owner role, so no axis is checked")
    for role in entry["owners"]:
        if role not in ROLES:
            die(f"MANIFEST entry {ks!r} names unknown role {role!r}")
    for axis in AXES:
        if axis not in entry:
            die(f"MANIFEST entry {ks!r} has no answer for the {axis} axis")
        state, note = entry[axis]
        if state not in ("used", "unused"):
            die(f"MANIFEST entry {ks!r} axis {axis} has unknown state {state!r}")
        if not note.strip():
            die(
                f"MANIFEST entry {ks!r} axis {axis} carries an empty "
                f"{'call site' if state == 'used' else 'reason'}"
            )

for (ks, axis, role), reason in KNOWN_GAPS.items():
    if ks not in _seen_keyspaces:
        die(f"KNOWN_GAPS names key space {ks!r}, which MANIFEST does not declare")
    if axis not in AXES:
        die(f"KNOWN_GAPS entry for {ks!r} names unknown axis {axis!r}")
    if role not in ROLES:
        die(f"KNOWN_GAPS entry for {ks!r} names unknown role {role!r}")
    if not reason.strip():
        die(f"KNOWN_GAPS entry ({ks!r}, {axis}, {role}) carries an empty reason")


# ---------------------------------------------------------------------------
# Discovery: key-space literals in workspace sources.
# ---------------------------------------------------------------------------
CONST_RE = re.compile(
    r'(?m)^[ \t]*(?:pub(?:\([^)]*\))?[ \t]+)?const[ \t]+([A-Z0-9_]+)[ \t]*:[ \t]*'
    r"&(?:'static[ \t]+)?str[ \t]*=[ \t]*\"([^\"]*)\"[ \t]*;"
)
FORMAT_RE = re.compile(r'format!\(\s*"([^"]*)"')


def walk_sources(base: Path):
    if not base.is_dir():
        die(f"{base} is not a directory")
    out = []
    stack = [base]
    while stack:
        current = stack.pop()
        try:
            children = list(current.iterdir())
        except OSError as err:
            die(f"cannot read {current}: {err}")
        for child in children:
            if child.is_symlink():
                continue
            if child.is_dir():
                if child.name in SKIP_DIRS or child.name.startswith("."):
                    continue
                stack.append(child)
            elif child.suffix == ".rs":
                out.append(child)
    return sorted(out)


sources = walk_sources(root)
if len(sources) < MIN_SOURCES:
    die(
        f"found {len(sources)} Rust sources under {root}, below the floor of "
        f"{MIN_SOURCES}. A moved or renamed source tree must fail here rather "
        f"than report every key space clean"
    )

# literal prefix -> sorted list of "<relative path>:<name>" sites.
literals: dict[str, set[str]] = {}
for path in sources:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as err:
        die(f"cannot read {path}: {err}")
    rel = path.relative_to(root).as_posix()
    for match in CONST_RE.finditer(text):
        name, value = match.group(1), match.group(2)
        if value.startswith(CONTROL_PLANE_ROOTS):
            literals.setdefault(value, set()).add(f"{rel}:{name}")
    for match in FORMAT_RE.finditer(text):
        template = match.group(1)
        if not template.startswith(CONTROL_PLANE_ROOTS):
            continue
        # Everything up to the first interpolation is the fixed key space; the
        # rest is runtime. `format!("sys/t/{}")` names `sys/t/`.
        prefix = template.split("{", 1)[0]
        if prefix.startswith(CONTROL_PLANE_ROOTS):
            literals.setdefault(prefix, set()).add(f"{rel}:format!")

if len(literals) < MIN_LITERALS:
    die(
        f"discovered {len(literals)} control-plane key-space literal(s) under "
        f"{root}, below the floor of {MIN_LITERALS}. A tightened pattern or a "
        f"renamed constant form must fail here, not empty the scan"
    )


# ---------------------------------------------------------------------------
# The shipped templates.
# ---------------------------------------------------------------------------
BUCKET_ARN_PREFIX = "arn:aws:s3:::"


def as_list(value):
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return value
    return None


def load_role_patterns(role: str) -> dict[str, list[str]]:
    """Allow-granted object-key patterns for `role`, per axis."""
    path = root / "deploy/iam" / f"{role}.json"
    if not path.is_file():
        die(f"{path} is not readable")
    try:
        policy = json.loads(path.read_text())
    except (OSError, ValueError) as err:
        die(f"{path} does not parse as JSON: {err}")
    statements = as_list(policy.get("Statement"))
    if not statements:
        die(f"{path} has no Statement list")

    granted: dict[str, list[str]] = {axis: [] for axis in AXES}
    for index, statement in enumerate(statements):
        if not isinstance(statement, dict):
            die(f"{path} statement {index} is not an object")
        if statement.get("Effect") != "Allow":
            continue
        actions = as_list(statement.get("Action"))
        if actions is None:
            die(f"{path} statement {index} has no usable Action")
        for axis, axis_actions in AXIS_ACTIONS.items():
            if not any(a in actions or a == "*" for a in axis_actions):
                continue
            if axis == "list":
                condition = statement.get("Condition", {})
                if not isinstance(condition, dict):
                    die(f"{path} statement {index} has a non-object Condition")
                like = condition.get("StringLike", {})
                if not isinstance(like, dict):
                    die(f"{path} statement {index} has a non-object StringLike")
                prefixes = as_list(like.get("s3:prefix"))
                if prefixes is None:
                    # A ListBucket Allow with no s3:prefix condition admits the
                    # whole bucket. Model it as such rather than as nothing.
                    granted[axis].append("*")
                else:
                    granted[axis].extend(prefixes)
                continue
            resources = as_list(statement.get("Resource"))
            if resources is None:
                die(f"{path} statement {index} has no usable Resource")
            for resource in resources:
                if not isinstance(resource, str):
                    die(f"{path} statement {index} has a non-string Resource")
                if not resource.startswith(BUCKET_ARN_PREFIX):
                    die(f"{path} statement {index} Resource {resource!r} is not an S3 ARN")
                body = resource[len(BUCKET_ARN_PREFIX) :]
                key = body.split("/", 1)[1] if "/" in body else ""
                if key:
                    granted[axis].append(key)
    return granted


ROLE_PATTERNS = {role: load_role_patterns(role) for role in ROLES}

if not any(ROLE_PATTERNS[role][axis] for role in ROLES for axis in AXES):
    die("no Allow pattern was read from any template; the scan reaches nothing")


def glob_matches(pattern: str, key: str) -> bool:
    """IAM StringLike: `*` is any run of characters, `?` is exactly one."""
    out = []
    for char in pattern:
        if char == "*":
            out.append(".*")
        elif char == "?":
            out.append(".")
        else:
            out.append(re.escape(char))
    return re.fullmatch("".join(out), key) is not None


# A segment no real key holds, so a match on it comes from a wildcard in the
# pattern rather than from the witness agreeing with a literal.
WITNESS_SEGMENT = "KEYSPACE-AXIS-WITNESS"


def reaches(pattern: str, keyspace: str) -> bool:
    """Does `pattern` grant anything inside `keyspace`?

    Three ways, all of which are a real grant under StringLike:
      - the pattern is rooted inside the key space (`quarantine/t/*/*/l0/*`
        for `quarantine/`),
      - a wildcard above the key space reaches into it (`sys/*`),
      - the pattern names the key space exactly, for a key space that is one
        object (`sys/gc`).
    """
    if pattern.startswith(keyspace):
        return True
    if glob_matches(pattern, keyspace):
        return True
    return glob_matches(pattern, keyspace + WITNESS_SEGMENT)


def is_blanket(pattern: str, keyspace: str) -> bool:
    """Does `pattern` reach `keyspace` only by being wider than it?

    Admin's `sys/*` reaches every key space under `sys/` and distinguishes
    none of them: it is the operator role's deliberate posture over the whole
    control plane, pinned by exact equality in
    `every_role_grants_exactly_the_expected_pattern_set`. Such a pattern
    SATISFIES a need (so the missing-grant rule counts it) but is not
    EVIDENCE of one (so the stale-unused rule must not read it as somebody
    adding a grant for this axis over this key space).

    The test is on the pattern's fixed prefix: anything before its first
    wildcard. A prefix strictly shorter than the key space cannot tell this
    key space from its siblings.
    """
    fixed = re.split(r"[*?]", pattern, maxsplit=1)[0]
    return len(fixed) < len(keyspace) and keyspace.startswith(fixed)


# ---------------------------------------------------------------------------
# The rules.
# ---------------------------------------------------------------------------
findings: list[str] = []
checked = 0

# Rule 1: every discovered literal falls inside a declared key space.
for literal in sorted(literals):
    if any(literal.startswith(entry["keyspace"]) for entry in MANIFEST):
        continue
    sites = ", ".join(sorted(literals[literal]))
    findings.append(
        f"undeclared: the key space {literal!r} ({sites}) is named by no "
        f"MANIFEST entry in scripts/guards/check-iam-keyspace-axes.sh. Add one "
        f"stating which roles run its calls and, for each of "
        f"{'/'.join(AXES)}, the call site that uses it or the reason it does "
        f"not. An undeclared control-plane key space is how six IAM grants "
        f"shipped with an axis refused (#1975)."
    )

# Rule 4: every declared key space is still named by something.
for entry in MANIFEST:
    ks = entry["keyspace"]
    if any(literal.startswith(ks) for literal in literals):
        continue
    findings.append(
        f"stale-keyspace: MANIFEST declares {ks!r} but no constant or format! "
        f"template in the workspace names it any more. A rename moves the "
        f"declaration; an orphan one silently stops checking anything."
    )

# Rules 2 and 3: per owner role, per axis.
for entry in MANIFEST:
    ks = entry["keyspace"]
    for role in entry["owners"]:
        for axis in AXES:
            state, note = entry[axis]
            patterns = ROLE_PATTERNS[role][axis]
            granted = [p for p in patterns if reaches(p, ks)]
            checked += 1
            gap = KNOWN_GAPS.get((ks, axis, role))
            if state == "used":
                if granted:
                    if gap is not None:
                        findings.append(
                            f"stale-gap: KNOWN_GAPS still records {role} as "
                            f"lacking the {axis} axis over {ks!r}, but "
                            f"{granted!r} grants it. The gap closed; remove the "
                            f"entry so the next one is visible."
                        )
                    continue
                if gap is not None:
                    continue
                findings.append(
                    f"missing-grant: {role} exercises the {axis} axis over "
                    f"{ks!r} ({entry['adr']}) and deploy/iam/{role}.json grants "
                    f"it nowhere under that key space. IAM is default-deny, so "
                    f"every such call is refused on a shipped deployment. Call "
                    f"site: {note}. {role} {axis} patterns: {patterns!r}"
                )
            else:
                specific = [p for p in granted if not is_blanket(p, ks)]
                if not specific:
                    continue
                findings.append(
                    f"stale-unused: {ks!r} declares the {axis} axis unused "
                    f"({note}), but deploy/iam/{role}.json grants it through "
                    f"{specific!r}. Either the code gained a call and the "
                    f"declaration is stale, or the grant is unnecessary."
                )

live_keyspaces = sum(
    1 for entry in MANIFEST if any(lit.startswith(entry["keyspace"]) for lit in literals)
)
if live_keyspaces < MIN_KEYSPACES_LIVE:
    die(
        f"only {live_keyspaces} declared key space(s) are still named by the "
        f"sources, below the floor of {MIN_KEYSPACES_LIVE}. The scan is no "
        f"longer reaching the code it checks"
    )

if findings:
    for finding in sorted(findings):
        print(finding)
    print(f"check-iam-keyspace-axes.sh: {len(findings)} finding(s)", file=sys.stderr)
    raise SystemExit(1)

print(
    f"check-iam-keyspace-axes.sh: clean ({checked} role/axis checks over "
    f"{live_keyspaces} key space(s) from {len(literals)} literal(s) in "
    f"{len(sources)} source(s), {len(KNOWN_GAPS)} known gap(s))"
)
PY
