#!/usr/bin/env bash
# IAM key-space axis guard: every CONTROL-PLANE key space the code names is
# declared here, every axis the code exercises over it is declared used, and
# every declared-used axis is granted by the template of each role whose
# process runs the call.
#
# The defect this exists for has shipped repeatedly (#1849, #1934, #1847/#1955,
# #1957, #1975), always the same way: a grant is derived from the one function
# a ticket names instead of from every call site that touches the prefix, so
# one axis is left with no Allow at all. IAM is default-deny, so the missing
# axis is not a narrowing, it is a refusal. Issue #1975 is the latest:
# `MaintainDelete` named no `sys/` resource, so `WorkerSet::reap_keys` could
# never delete a dead worker's heartbeat and the per-tick LIST over
# `sys/maintain/workers/` grew without bound.
#
# The per-lifecycle reachability tests in
# `crates/ravel-commit/tests/iam_templates.rs` catch this precisely, with
# witness keys built by the real constructors -- but only for a lifecycle
# somebody remembered to write a test for. This scan needs nobody to remember:
# a new control-plane key space fails here until it is declared, and the axes
# are read out of the code rather than typed in, so declaring a key space
# cannot settle for a wrong answer on one of its axes.
#
# HOW THE AXES ARE DERIVED
#
# Typed-in axes are what let #1975's exact shape through: a new call on an
# existing key space needs no declaration change, so a stale "unused" note
# stays green. So the axis set is computed instead. Every control-plane key
# space value the sources name (a `const`/`static &str`, a `format!` template,
# or a bare string literal passed as a store call's key) is tracked through the
# production sources by a value-flow pass: `let` and `for` bindings, struct
# fields, function parameters and returns, block tails and match arms, until it
# reaches an object-store call, whose method name gives the axis. Each derived
# axis must be declared used. Anything under `#[cfg(test)]`, and any source
# under a `tests/`, `benches/`, `examples/` or `fuzz/` directory, is excluded:
# a test writing a key is not a role exercising an axis, which is precisely how
# `sys/auth` came to be declared as written by the two serving roles when only
# the admin CLI writes it.
#
# A key-bearing value that reaches a call this pass does not recognise is NOT
# skipped: it is reported and the scan exits 70, unless the (file, callee) pair
# is in UNCLASSIFIED_ALLOWLIST with a reason. Silently dropping an unreadable
# call site is how a derivation becomes a decoration.
#
# WHAT IT CHECKS
#
#   undeclared       a key-space value in any workspace source names a key
#                      space no MANIFEST entry covers. Discovery is the forcing
#                      function: the declaration cannot be skipped. A
#                      declaration covers everything beneath it (on path
#                      segment boundaries), which is what lets `quarantine/` be
#                      declared once for the whole root the orphan sweep
#                      composes its keys under; declare a narrower key space
#                      when its axes differ from its parent's.
#   undeclared-axis  the code exercises an axis the MANIFEST declares unused.
#                      This is the #1975 shape on an already-declared key
#                      space, and it is a finding whether or not any template
#                      grants the axis.
#   stale-used       the MANIFEST declares an axis used over a key space whose
#                      other axes were derived, and the derivation finds no
#                      call on it. The call moved or went away; the note is
#                      stale.
#   missing-grant    an owner role of a declared-used axis has a template that
#                      grants that axis nowhere under the key space. This is
#                      the #1975 shape exactly.
#   stale-unused     an axis declared unused IS granted specifically (not by a
#                      blanket wildcard) to some role. The grant is unnecessary,
#                      or the code gained a call the derivation cannot see.
#   stale-gap        a KNOWN_GAPS entry whose grant now exists, or whose axis is
#                      no longer declared used, or whose role no longer owns it.
#                      The gap closed; delete the entry so the next is visible.
#   stale-keyspace   a MANIFEST key space no discovered value names any more.
#                      A rename must move the declaration, not orphan it.
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
#   - WHICH role runs a derived call. The axis comes from the code; the owner
#     roles beside it are declared by hand, because the mapping from a call
#     site to a process role runs through `--mode` gating and CLI subcommands
#     that no key-flow pass reads.
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
# (a template missing or unparseable, a discovery floor not met, or a key-
# bearing value reaching an unrecognised call) -- never a silent pass, since a
# scan that reaches nothing reports the same empty finding list as a clean tree.
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
import bisect
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
MIN_DERIVED_AXES = 20

# Directory names never walked: build output, vendored trees, and the
# executor's own scratch. The guard's test fixtures are not here; they are
# built under TMPDIR and handed in as the scan root, so they are scanned in
# full exactly like a real tree.
SKIP_DIRS = {"target", "node_modules", ".git", ".gate-logs", ".dd-tools"}

# Directory names whose sources name key spaces without a role running them:
# a test writing `sys/auth` is not the gateway writing `sys/auth`. Discovery
# still reads them (a key space named only by a test is still a key space that
# must be declared); axis derivation does not.
NON_PRODUCTION_DIRS = ("tests", "benches", "examples", "fuzz")


def die(msg: str) -> None:
    print(f"check-iam-keyspace-axes.sh: {msg}", file=sys.stderr)
    print("  Refusing to report a result: a clean scan and a scan that could", file=sys.stderr)
    print("  not run are different answers.", file=sys.stderr)
    raise SystemExit(70)


# ---------------------------------------------------------------------------
# The manifest.
#
# One entry per control-plane key space, with one answer per axis:
#
#   ("used", (roles...), "<call site>")   the axis IS exercised. The roles are
#                                         the ones whose PROCESS runs the call,
#                                         not the ones that happen to hold a
#                                         grant; each must grant the axis.
#   ("unused", (), "<reason>")            the axis is NOT exercised.
#
# The derivation checks the used/unused half against the code, so a wrong
# answer there fails rather than sits. The role list is the hand-written half:
# it is read off the call site's callers (which `--mode`, which CLI
# subcommand), and it is what the missing-grant rule checks. An optional
# `out_of_scope` note records a production writer that runs under no template
# in deploy/iam/.
# ---------------------------------------------------------------------------
MANIFEST = [
    {
        "keyspace": "admission/query/",
        "adr": "ADR-0071 query admission snapshots",
        "list": (
            "used",
            ("query",),
            "crates/ravel-query/src/query_admission.rs:327 "
            "list_all(store, QUERY_ADMISSION_PREFIX)",
        ),
        "get": (
            "used",
            ("query",),
            "crates/ravel-query/src/query_admission.rs:336 "
            "store.get(&meta.key, GetRange::Full)",
        ),
        "put": (
            "used",
            ("query",),
            "crates/ravel-query/src/query_admission.rs:401 publishes this "
            "process's snapshot",
        ),
        "delete": (
            "unused",
            (),
            "a snapshot is overwritten in place under the writer's own key and "
            "ages out by its stamped time; query_admission.rs contains no delete",
        ),
    },
    {
        "keyspace": "quarantine/",
        "adr": "ADR-0058 decision 6 orphan quarantine",
        "list": (
            "used",
            ("maintain",),
            "crates/ravel-maintain/src/sweep.rs:901 list_all over the "
            "quarantine l0 data prefix",
        ),
        "get": (
            "unused",
            (),
            "nothing reads a quarantined object: sweep_orphans copies from the "
            "LIVE key, and no restore path exists in the code (issue #1978 "
            "tracks the operator-facing restore, which would add one)",
        ),
        "put": (
            "used",
            ("maintain",),
            "crates/ravel-maintain/src/sweep.rs:767 store.put(dest, got.data, ..) "
            "in the copy step",
        ),
        "delete": (
            "used",
            ("maintain",),
            "crates/ravel-maintain/src/sweep.rs:921 store.delete past the second "
            "horizon",
        ),
    },
    {
        "keyspace": "sys/auth",
        "adr": "auth token map",
        "list": ("unused", (), "a single object at a fixed key; nothing enumerates it"),
        "get": (
            "used", ("gateway", "query", "admin"),
            "crates/ravel-catalog/src/auth_token_map.rs:422 store.get(AUTH_KEY), "
            "reached from services/ravel-server/src/lifecycle_refresh.rs:210 on "
            "every refresh tick (the durable auth state exists in Mode::All, "
            "Gateway and Query, lib.rs:1926) and from "
            "services/ravel-cli/src/tenant_token.rs:97 for `tenant-token list`",
        ),
        "put": (
            "used",
            ("admin",),
            "crates/ravel-catalog/src/auth_token_map.rs:482 and :496, reached "
            "only from services/ravel-cli/src/tenant_token.rs:50 "
            "(upsert_token_owned) and :78 (remove_tokens_by_tenant). Every "
            "ravel-server call of a writer is under #[cfg(test)]",
        ),
        "delete": ("unused", (), "the map is replaced by a write, never removed"),
        "out_of_scope": (
            "services/ravel-operator/src/controller.rs:775 and :818 also write "
            "this map (replace_tenant_tokens, remove_tokens_by_tenant_owned_by). "
            "The operator runs under no template in deploy/iam/, so its grants "
            "are outside what this guard can check"
        ),
    },
    {
        "keyspace": "sys/gc",
        "adr": "GC configuration record",
        "list": ("unused", (), "a single object at a fixed key; nothing enumerates it"),
        "get": (
            "used",
            ("gateway", "query", "maintain", "admin"),
            "crates/ravel-maintain/src/gc_config.rs:322 store.get(GC_CONFIG_KEY), "
            "reached from services/ravel-server/src/main.rs:352 in every process "
            "mode and from services/ravel-cli/src/gc_config.rs:35 for "
            "`gc-config show`",
        ),
        "put": (
            "used",
            ("gateway", "query", "maintain", "admin"),
            "crates/ravel-maintain/src/gc_config.rs:369 the create_if_absent "
            "bootstrap, from the same unconditional main.rs:352 call, so "
            "whichever mode starts against a fresh bucket issues it; :434 and "
            ":449 are set_gc_config, reached from "
            "services/ravel-cli/src/gc_config.rs:91",
        ),
        "delete": ("unused", (), "protected by DenyDeleteProtected in all four templates"),
    },
    {
        "keyspace": "sys/maintain/claims/compaction/",
        "adr": "ADR-1029 advisory compaction claims (Proposed)",
        "list": (
            "unused",
            (),
            "a claim is addressed by its input-set identity, never enumerated; "
            "claim.rs does no listing",
        ),
        "get": (
            "used",
            ("maintain",),
            "crates/ravel-fleet/src/claim.rs:568 and :573 store.get(key, "
            "GetRange::Full), from the maintain compactor",
        ),
        "put": (
            "used",
            ("maintain",),
            "crates/ravel-fleet/src/claim.rs:532 the CAS claim write in acquire, "
            "and :724 the CAS steal write",
        ),
        "delete": (
            "unused",
            (),
            "ADR-1029 forbids it: claim.rs states there is no unconditional "
            "delete anywhere in the module, since deleting a claim is the write "
            "that would break the advisory guarantee",
        ),
    },
    {
        "keyspace": "sys/maintain/memo/",
        "adr": "maintain warm-start memo snapshots",
        "list": (
            "used",
            ("maintain",),
            "crates/ravel-maintain/src/memo_snapshot.rs:81 list_all(store, MEMO_PREFIX)",
        ),
        "get": (
            "used",
            ("maintain",),
            "crates/ravel-maintain/src/memo_snapshot.rs:85 "
            "store.get(&meta.key, GetRange::Full)",
        ),
        "put": (
            "used",
            ("maintain",),
            "crates/ravel-maintain/src/memo_snapshot.rs:58 the handoff write",
        ),
        "delete": (
            "unused",
            (),
            "a memo is overwritten under its own key, never removed",
        ),
    },
    {
        "keyspace": "sys/maintain/workers/",
        "adr": "ADR-0065 decision 1 leased distributed maintenance",
        "list": (
            "used",
            ("maintain",),
            "crates/ravel-fleet/src/worker_set.rs:403 list_all(store, WORKERS_PREFIX)",
        ),
        "get": (
            "used",
            ("maintain",),
            "crates/ravel-fleet/src/worker_set.rs:424 store.get(&meta.key, GetRange::Full)",
        ),
        "put": (
            "used",
            ("maintain",),
            "crates/ravel-fleet/src/worker_set.rs:359 write_heartbeat",
        ),
        "delete": (
            "used",
            ("maintain",),
            "crates/ravel-fleet/src/worker_set.rs:463 reap_keys, driven from "
            "services/ravel-server/src/maintain.rs:1111. This is the axis issue "
            "#1975 found ungranted",
        ),
    },
    {
        "keyspace": "sys/qualification",
        "adr": "ADR-0050 section 6 backend qualification record",
        "list": ("unused", (), "a single object at a fixed key; nothing enumerates it"),
        "get": (
            "used",
            ("gateway", "query", "maintain", "admin"),
            "services/ravel-server/src/qualification.rs:128, reached from "
            "main.rs:109 in every process mode, and "
            "services/ravel-cli/src/qualify.rs:163, which reads an existing "
            "record before re-recording",
        ),
        "put": (
            "used",
            ("admin",),
            "services/ravel-cli/src/qualify.rs:110 and :183 record a passing "
            "qualification; ravel-server only reads the record",
        ),
        "delete": ("unused", (), "protected by DenyDeleteProtected in all four templates"),
    },
    {
        "keyspace": "sys/qualify/",
        "adr": "ADR-0050 qualification probe scratch space",
        "list": (
            "used",
            ("admin",),
            "crates/ravel-object-store/src/conformance.rs:1163 and :1164, the "
            "cross-page listing probes, under the scratch prefix "
            "services/ravel-cli/src/qualify.rs:44 builds",
        ),
        "get": (
            "used",
            ("admin",),
            "crates/ravel-object-store/src/conformance.rs readback probes, "
            "e.g. :641 and :1053",
        ),
        "put": (
            "used",
            ("admin",),
            "crates/ravel-object-store/src/conformance.rs:615 and the write "
            "probes under it",
        ),
        "delete": (
            "used",
            ("admin",),
            "crates/ravel-object-store/src/conformance.rs:1536 the "
            "delete-visibility probe, and :1594 its cleanup",
        ),
    },
    {
        "keyspace": "sys/query/workers/",
        "adr": "ADR-0071 query worker set",
        "list": (
            "used",
            ("query",),
            "crates/ravel-fleet/src/query_workers.rs:358 list_all(store, QUERY_WORKERS_PREFIX)",
        ),
        "get": (
            "used",
            ("query",),
            "crates/ravel-fleet/src/query_workers.rs:383 store.get(&meta.key, GetRange::Full)",
        ),
        "put": (
            "used",
            ("query",),
            "crates/ravel-fleet/src/query_workers.rs:288 the heartbeat write",
        ),
        "delete": (
            "used",
            ("query",),
            "crates/ravel-fleet/src/query_workers.rs:424 reap_keys, and :308 the "
            "graceful-drain self-delete driven from distrib.rs:1818",
        ),
    },
    {
        "keyspace": "sys/t/",
        "adr": "ADR-0050 section 3 per-tenant recovery manifest",
        "list": (
            "unused",
            (),
            "the manifest key is derived from the tenant hash, so recovery "
            "addresses it directly; the operator-facing enumeration runs under "
            "admin's blanket sys/* list",
        ),
        "get": (
            "unused",
            (),
            "no production reader: recovery is an operator action, and admin's "
            "blanket sys/* read is what serves it",
        ),
        "put": (
            "used", ("gateway",),
            "services/ravel-server/src/tenancy.rs:512 "
            "RecoveryManifestWriter::ensure, on a tenant's first write through "
            "the ingest path (ingest.rs:144, logs_ingest.rs:168, "
            "traces_ingest.rs:153, otap_grpc.rs:326)",
        ),
        "delete": ("unused", (), "a recovery manifest is never removed while its tenant exists"),
    },
    {
        "keyspace": "sys/tenancy",
        "adr": "deployment tenancy marker",
        "list": ("unused", (), "a single object at a fixed key; nothing enumerates it"),
        "get": (
            "used",
            ("gateway", "query", "maintain", "admin"),
            "services/ravel-server/src/tenancy.rs:187 read_marker, from "
            "resolve_and_pin at main.rs:141 in every process mode; "
            "services/ravel-server/src/store_probe.rs:180 the liveness probe; "
            "services/ravel-cli/src/tenancy.rs:109 and :173, which pass the key "
            "as a bare string literal",
        ),
        "put": (
            "used",
            ("gateway", "query", "maintain"),
            "services/ravel-server/src/tenancy.rs:206 write_marker stamps the "
            "marker on a fresh bucket, from the same unconditional "
            "resolve_and_pin; no CLI path writes it",
        ),
        "delete": ("unused", (), "protected by DenyDeleteProtected in all four templates"),
    },
]

# Axes a role genuinely exercises today and its shipped template does NOT
# grant. Each is a real refusal on a shipped deployment, recorded rather than
# suppressed: the entry must carry a reason, and the guard fails again the
# moment the grant appears, so closing a gap cannot leave the table stale.
#
# Nothing may be added here to make a NEW change pass. A grant this guard
# demands is one a running process needs; the table is for gaps that predate
# the guard and need their own change. Issue #1995 is that change.
KNOWN_GAPS = {
    ("sys/auth", "get", "gateway"): (
        "no template names sys/auth on any axis; only admin's blanket sys/* "
        "reaches it. Found while deriving #1975, tracked by #1995"
    ),
    ("sys/auth", "get", "query"): (
        "same gap as the gateway entry above: the auth token map is read on "
        "every refresh tick by both serving modes and granted to neither (#1995)"
    ),
    ("sys/auth", "put", "admin"): (
        "AdminWrite names sys/tenancy, sys/qualification, sys/qualify/* and "
        "sys/gc, but not sys/auth, so every `ravel-cli tenant-token "
        "set|revoke` write is refused. Admin's blanket sys/* covers read and "
        "list only. Tracked by #1995"
    ),
    ("sys/gc", "put", "gateway"): (
        "main.rs:352 bootstraps the GC config in every mode, but only "
        "MaintainWrite and AdminWrite name sys/gc. A deployment whose gateway "
        "starts first against a fresh bucket is refused; one where maintain "
        "started first never issues the put, which is why this has not been "
        "noticed (#1995)"
    ),
    ("sys/gc", "put", "query"): (
        "same ordering-dependent gap as the gateway entry above (#1995)"
    ),
    ("sys/maintain/memo/", "list", "maintain"): (
        "MaintainList carries no sys/maintain/memo/* prefix, so the warm-start "
        "handoff read at memo_snapshot.rs:81 is refused. It fails open, so the "
        "only symptom is a cold start every time (#1995)"
    ),
    ("sys/query/workers/", "delete", "query"): (
        "query.json has no Allow delete statement at all, so both the reap at "
        "query_workers.rs:424 and the graceful-drain self-delete at "
        "query_workers.rs:308 are refused. This is #1975's defect in the query "
        "role, found while deriving it and tracked by #1995"
    ),
    ("sys/t/", "put", "gateway"): (
        "no template names sys/t/*; the templates carry only the literal "
        "sys/tenancy, so every recovery manifest write is refused (#1995)"
    ),
}

# Call sites the derivation reaches with a key-bearing value and cannot
# classify, each with the reason it is not a store call. A site not listed here
# exits 70 rather than being dropped: an unreadable call site is the one place
# a derived axis set can silently lose an axis.
#
# Keyed by (source path relative to the scan root, callee as the pass names it:
# a bare function name, `name!` for a macro, or `.name` for a method that ends
# the chain). Empty today, and measured empty: the production tree has no such
# site. The mechanism is exercised by the guard's own cases, not by an entry
# here.
UNCLASSIFIED_ALLOWLIST: dict[tuple[str, str], str] = {}


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
    if "out_of_scope" in entry and not entry["out_of_scope"].strip():
        die(f"MANIFEST entry {ks!r} carries an empty out_of_scope note")
    used_axes = 0
    for axis in AXES:
        if axis not in entry:
            die(f"MANIFEST entry {ks!r} has no answer for the {axis} axis")
        state, roles, note = entry[axis]
        if state not in ("used", "unused"):
            die(f"MANIFEST entry {ks!r} axis {axis} has unknown state {state!r}")
        if not note.strip():
            die(
                f"MANIFEST entry {ks!r} axis {axis} carries an empty "
                f"{'call site' if state == 'used' else 'reason'}"
            )
        for role in roles:
            if role not in ROLES:
                die(f"MANIFEST entry {ks!r} axis {axis} names unknown role {role!r}")
        if len(set(roles)) != len(roles):
            die(f"MANIFEST entry {ks!r} axis {axis} names a role twice")
        if state == "used":
            used_axes += 1
            if not roles:
                die(
                    f"MANIFEST entry {ks!r} axis {axis} is used but names no "
                    f"owner role, so no template is checked for it"
                )
        elif roles:
            die(
                f"MANIFEST entry {ks!r} axis {axis} is unused but names owner "
                f"role(s) {roles!r}; an unused axis has no caller"
            )
    if not used_axes:
        die(f"MANIFEST entry {ks!r} declares every axis unused, so it checks nothing")

for (ks, axis, role), reason in KNOWN_GAPS.items():
    if ks not in _seen_keyspaces:
        die(f"KNOWN_GAPS names key space {ks!r}, which MANIFEST does not declare")
    if axis not in AXES:
        die(f"KNOWN_GAPS entry for {ks!r} names unknown axis {axis!r}")
    if role not in ROLES:
        die(f"KNOWN_GAPS entry for {ks!r} names unknown role {role!r}")
    if not reason.strip():
        die(f"KNOWN_GAPS entry ({ks!r}, {axis}, {role}) carries an empty reason")

for (rel, callee), reason in UNCLASSIFIED_ALLOWLIST.items():
    if not reason.strip():
        die(f"UNCLASSIFIED_ALLOWLIST entry ({rel!r}, {callee!r}) carries an empty reason")


def covers(keyspace: str, value: str) -> bool:
    """Is `value` the key space `keyspace` or something inside it?

    On path segment boundaries, so `sys/authx` is not covered by `sys/auth`.
    A declaration ending in `/` covers its whole subtree.
    """
    if value == keyspace:
        return True
    if not value.startswith(keyspace):
        return False
    return keyspace.endswith("/") or value[len(keyspace)] == "/"


# ---------------------------------------------------------------------------
# Source text handling, shared by discovery and derivation.
# ---------------------------------------------------------------------------
IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
TOKEN_RE = re.compile(r"//|/\*|(?<![A-Za-z0-9_])b?r#*\"|(?<![A-Za-z0-9_])b\"|\"|'")


def mask(text: str):
    """Blank comments, string bodies and char literals; keep offsets and lines.

    Returns (masked, [(start, end, body)]) with one entry per string literal.
    Masking is what lets the passes below use plain regexes and bracket
    matching without a string or a comment standing in for code.
    """
    out = list(text)
    strings = []
    n = len(text)

    def blank(a, b):
        out[a:b] = [" " if c != "\n" else "\n" for c in text[a:b]]

    i = 0
    while i < n:
        m = TOKEN_RE.search(text, i)
        if not m:
            break
        i = m.start()
        tok = m.group(0)
        if tok == "//":
            j = text.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
            continue
        if tok == "/*":
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth += 1
                    j += 2
                elif text.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            blank(i, j)
            i = j
            continue
        if tok.endswith('"'):
            if "r" in tok[:-1]:
                close = '"' + "#" * tok.count("#")
                body_start = i + len(tok)
                j = text.find(close, body_start)
                j = n if j < 0 else j + len(close)
                body = text[body_start : j - len(close)]
            else:
                body_start = i + len(tok)
                j = body_start
                while j < n:
                    if text[j] == "\\":
                        j += 2
                        continue
                    if text[j] == '"':
                        break
                    j += 1
                body = text[body_start:j]
                j = min(j + 1, n)
            strings.append((i, j, body))
            blank(i, j)
            i = j
            continue
        # A `'`: a char literal, or a lifetime (which is left alone).
        if text.startswith("'\\", i):
            j = text.find("'", i + 2)
            j = n if j < 0 else j + 1
        elif i + 2 < n and text[i + 2] == "'":
            j = i + 3
        else:
            i += 1
            continue
        blank(i, j)
        i = j
    return "".join(out), strings


def match_brace(masked: str, idx: int) -> int:
    depth = 0
    for i in range(idx, len(masked)):
        if masked[i] == "{":
            depth += 1
        elif masked[i] == "}":
            depth -= 1
            if not depth:
                return i + 1
    return len(masked)


def match_paren(masked: str, idx: int) -> int:
    depth = 0
    for i in range(idx, len(masked)):
        if masked[i] == "(":
            depth += 1
        elif masked[i] == ")":
            depth -= 1
            if not depth:
                return i + 1
    return len(masked)


def split_top(text: str):
    parts, depth, cur = [], 0, []
    for ch in text:
        if ch in "([{<":
            depth += 1
        elif ch in ")]}>":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


def excluded_spans(masked: str, raw: str):
    """Byte spans the derivation must not read: `#[cfg(test)]` items and
    `use` declarations (a `use` names a writer without calling it)."""
    spans = []
    for m in re.finditer(r"#\[cfg\([^\]]*\btest\b[^\]]*\)\]", raw):
        if "not(test" in m.group(0):
            continue
        brace = masked.find("{", m.end())
        if brace >= 0:
            spans.append((m.start(), match_brace(masked, brace)))
    for m in re.finditer(r"(?m)^[ \t]*(?:pub[ \t]+)?use[ \t][^;]*;", masked):
        spans.append((m.start(), m.end()))
    return spans


def in_spans(spans, pos: int) -> bool:
    return any(a <= pos < b for a, b in spans)


def functions(masked: str):
    """[(name, body_start, body_end, [param names], (sig_start, body_start))]"""
    out = []
    for m in re.finditer(r"\bfn[ \t]+(" + IDENT + r")", masked):
        p = masked.find("(", m.end())
        if p < 0:
            continue
        pend = match_paren(masked, p)
        brace = masked.find("{", pend)
        if brace < 0:
            continue
        # A `;` between the parameter list and the next `{` means this is a
        # declaration without a body (a trait method), and the brace belongs
        # to something else.
        if ";" in masked[pend:brace]:
            continue
        params = []
        for part in split_top(masked[p + 1 : pend - 1]):
            part = part.strip()
            if not part or part.endswith("self"):
                continue
            name = part.split(":", 1)[0].strip().lstrip("&").replace("mut ", "").strip()
            if re.fullmatch(IDENT, name):
                params.append(name)
        out.append((m.group(1), brace, match_brace(masked, brace), params, (m.start(), brace)))
    return out


def callee_before(masked: str, open_idx: int):
    """The identifier immediately before a `(`, as (name, is_macro, start)."""
    j = open_idx - 1
    while j >= 0 and masked[j].isspace():
        j -= 1
    macro = False
    if j >= 0 and masked[j] == "!":
        macro = True
        j -= 1
    end = j + 1
    while j >= 0 and (masked[j].isalnum() or masked[j] == "_"):
        j -= 1
    name = masked[j + 1 : end]
    if not name:
        return None, False, j + 1
    return name, macro, j + 1


def enclosing_call(masked: str, pos: int):
    """The call whose argument list encloses `pos`, with the argument index."""
    depth, commas = 0, 0
    i = pos - 1
    while i >= 0:
        ch = masked[i]
        if ch in ")]}":
            depth += 1
        elif ch == "(":
            if depth == 0:
                name, macro, start = callee_before(masked, i)
                if name is None:
                    return None
                return (name + ("!" if macro else ""), commas, i, start)
            depth -= 1
        elif ch in "[{":
            if depth == 0:
                return None
            depth -= 1
        elif ch == "," and depth == 0:
            commas += 1
        elif ch == ";" and depth == 0:
            return None
        i -= 1
    return None


def enclosing_brace(masked: str, pos: int):
    """The innermost unclosed `{` before pos, or None."""
    depth = 0
    i = pos - 1
    while i >= 0:
        ch = masked[i]
        if ch in ")]}":
            depth += 1
        elif ch in "([{":
            if depth == 0:
                return i if ch == "{" else None
            depth -= 1
        elif ch == ";" and depth == 0:
            return None
        i -= 1
    return None


def statement_head_start(masked: str, pos: int) -> int:
    depth = 0
    i = pos - 1
    while i >= 0:
        ch = masked[i]
        if ch in ")]}":
            depth += 1
        elif ch in "([{":
            if depth == 0:
                return i + 1
            depth -= 1
        elif ch == ";" and depth == 0:
            return i + 1
        i -= 1
    return 0


def statement_head(masked: str, pos: int) -> str:
    return masked[statement_head_start(masked, pos) : pos]


# ---------------------------------------------------------------------------
# The derivation: which axes the code exercises over each key space.
#
# The tables below are the object-store surface of
# `crates/ravel-object-store/src/lib.rs` plus the wrappers around it, and the
# value-preserving operations a key passes through on the way to one.
# ---------------------------------------------------------------------------

# callee -> (axis, index of the key argument). `self` is not counted, so a
# method's first real argument is index 0.
STORE_CALLS = {
    "get": ("get", 0),
    "head": ("get", 0),
    "put": ("put", 0),
    "put_multipart": ("put", 0),
    "delete": ("delete", 0),
    "list": ("list", 0),
    "list_after": ("list", 0),
    "list_delimited": ("list", 0),
    "list_all": ("list", 1),
    "drain_list": ("list", 1),
}
# The subset whose RESULT carries keys under the same key space, so the object
# metadata a listing returns keeps the taint and a get or delete on one of
# those keys is attributed to the listed key space.
LIST_CALLS = {"list", "list_after", "list_delimited", "list_all", "drain_list"}

# Calls whose result is the same key value: the taint passes outward.
IDENTITY = {
    "clone", "to_string", "to_owned", "as_str", "as_ref", "to_vec", "into",
    "borrow", "unwrap", "expect", "Some", "Ok", "Err", "from", "String::from",
    "map", "filter_map", "and_then", "unwrap_or", "unwrap_or_else",
    "unwrap_or_default", "collect", "iter", "cloned", "copied", "into_iter",
    "map_err",
}
# The same, as a postfix method chain on a key expression.
POSTFIX_IDENTITY = {
    "clone", "to_string", "to_owned", "as_str", "as_ref", "to_vec", "into",
    "borrow", "unwrap", "expect", "collect", "iter", "cloned", "copied",
    "into_iter", "map_err",
}
# Methods that mutate the receiver in place without producing a key.
RECEIVER_INERT = {"sort", "sort_unstable", "dedup", "reverse", "truncate", "retain"}
# Methods that move the key INTO their receiver, which the receiver then holds.
CONTAINER_WRITE = {"push", "insert", "extend", "push_str"}
# Calls that consume a key and produce something that is not one.
INERT = {
    "strip_prefix", "strip_suffix", "starts_with", "ends_with", "contains",
    "split_once", "rsplit_once", "splitn", "rsplitn", "split", "rsplit",
    "trim_start_matches", "trim_end_matches", "len", "is_empty", "eq", "ne",
    "cmp", "find", "matches", "chars", "bytes", "with_key_contains",
    "is_protected", "parse", "hash", "update", "write_all",
}
# Macros that render or assert on a key without storing it. `format!` is
# handled before this: it can BUILD a key, and its template is read.
MACRO_INERT = {
    "format!", "write!", "writeln!", "print!", "println!", "eprint!", "eprintln!",
    "panic!", "assert!", "assert_eq!", "assert_ne!", "anyhow!", "bail!", "vec!",
    "info!", "warn!", "error!", "debug!", "trace!", "matches!", "unreachable!",
    "todo!", "expect!",
}

FORMAT_RE = re.compile(r"\bformat!\(")
KEYLIKE_RE = re.compile(r"[A-Za-z0-9_./{}:*-]+")
DERIVE_CONST_RE = re.compile(
    r"(?:pub(?:\([^)]*\))?[ \t]+)?(?:const|static)[ \t]+(" + IDENT + r")[ \t]*:[ \t]*"
    r"&(?:'static[ \t]+)?str[ \t]*="
)


def skip_postfix(masked: str, end: int):
    """Consume `?`, `.await`, identity calls and `.key` after a key expression.

    Returns (new_end, alive, terminal). `alive` is False when the chain ends in
    something that is not a key any more; `terminal` names it so the caller can
    decide whether that is a known dead end or an unreadable one.
    """
    i = end
    n = len(masked)
    while i < n:
        while i < n and masked[i] in " \t\n":
            i += 1
        if masked.startswith("?", i):
            i += 1
            continue
        if masked.startswith(".", i):
            j = i + 1
            while j < n and masked[j] in " \t\n":
                j += 1
            m = re.match(IDENT, masked[j:])
            if not m:
                return i, True, None
            name = m.group(0)
            k = j + len(name)
            while k < n and masked[k] in " \t\n":
                k += 1
            if k < n and masked[k] == "(":
                kend = match_paren(masked, k)
                if name in POSTFIX_IDENTITY or name == "await":
                    i = kend
                    continue
                return kend, False, name
            if name == "await":
                i = k
                continue
            # Only `.key` carries the taint through a field read: object
            # metadata holds a key beside a size and a timestamp, and letting
            # every field carry it taints unrelated values.
            if name == "key":
                i = k
                continue
            return k, False, "field:" + name
        break
    return i, True, None


def cp_prefix(template: str, consts: dict) -> str | None:
    """The fixed control-plane prefix a `format!` template renders, or None."""
    s = template
    m = re.match(r"\{(" + IDENT + r")\}", s)
    if m and m.group(1) in consts:
        s = consts[m.group(1)] + s[m.end() :]
    if not s.startswith(CONTROL_PLANE_ROOTS):
        return None
    pre = s.split("{", 1)[0]
    return pre if pre.startswith(CONTROL_PLANE_ROOTS) else None


def format_keyspaces(body: str, consts: dict, local_map: dict):
    """Key spaces a `format!` renders a key of, from its leading atom."""
    m = re.match(r"\{(" + IDENT + r")[^}]*\}", body)
    if m and m.group(1) in local_map:
        return set(local_map[m.group(1)])
    ks = cp_prefix(body, consts)
    return {ks} if ks else set()


class Source:
    """One production source, masked and indexed once."""

    def __init__(self, rel: str, raw: str):
        self.rel = rel
        self.masked, self.strings = mask(raw)
        self.excluded = excluded_spans(self.masked, raw)
        self.fns = functions(self.masked)
        size = len(raw)
        skip = bytearray(size)
        for a, b in self.excluded:
            b = min(b, size)
            if b > a:
                skip[a:b] = b"\x01" * (b - a)
        sig = bytearray(size)
        for fn in self.fns:
            a, b = fn[4][0], min(fn[4][1], size)
            if b > a:
                sig[a:b] = b"\x01" * (b - a)
        self.str_starts = [s[0] for s in self.strings]
        self.str_span = {(s[0], s[1]): s[2] for s in self.strings}
        self.ident_pos: dict[str, list[tuple[int, int]]] = {}
        for m in re.finditer(IDENT, self.masked):
            s = m.start()
            if skip[s] or sig[s]:
                continue
            self.ident_pos.setdefault(m.group(0), []).append((s, m.end()))
        self.fn_starts = [fn[1] for fn in self.fns]
        self.line_starts = [0] + [m.end() for m in re.finditer("\n", raw)]

    def line(self, pos: int) -> int:
        return bisect.bisect_right(self.line_starts, pos)

    def fn_at(self, pos: int):
        i = bisect.bisect_right(self.fn_starts, pos) - 1
        while i >= 0:
            if self.fns[i][2] > pos:
                return i
            i -= 1
        return None

    def string_at(self, pos: int):
        """The body of the string literal at `pos`, whitespace allowed before."""
        i = bisect.bisect_left(self.str_starts, pos)
        if i >= len(self.strings):
            return None
        s0, _s1, body = self.strings[i]
        if self.masked[pos:s0].strip():
            return None
        return body


class Derivation:
    """The fixpoint: key-space values, where they flow, and where they land."""

    MAX_ROUNDS = 12
    MAX_DEPTH = 8

    def __init__(self, sources: list[Source]):
        self.sources = sources
        self.consts: dict[str, str] = {}
        self.locals: dict[tuple[str, int, str], set[str]] = {}
        self.fields: dict[tuple[str, str], set[str]] = {}
        self.keyfns: dict[str, set[str]] = {}
        self.derived: dict[str, dict[str, set[str]]] = {}
        self.literal_keys: dict[str, set[str]] = {}
        self.unclassified: dict[tuple[str, str], tuple[str, int]] = {}
        self.fn_by_name: dict[str, list[tuple[Source, int]]] = {}
        self.rounds = 0
        self._changed = False
        for f in sources:
            for i, fn in enumerate(f.fns):
                self.fn_by_name.setdefault(fn[0], []).append((f, i))
        for f in sources:
            for m in DERIVE_CONST_RE.finditer(f.masked):
                body = f.string_at(m.end())
                if body is not None and body.startswith(CONTROL_PLANE_ROOTS):
                    self.consts[m.group(1)] = body

    def add(self, table: dict, key, value) -> None:
        bucket = table.setdefault(key, set())
        if value not in bucket:
            bucket.add(value)
            self._changed = True

    def run(self) -> None:
        changed = True
        while changed and self.rounds < self.MAX_ROUNDS:
            self.rounds += 1
            self._changed = False
            self.unclassified.clear()
            for f in self.sources:
                for start, end, keyspaces in self.atoms(f):
                    for ks in keyspaces:
                        self.flow(f, start, end, ks, 0)
            changed = self._changed

    def atoms(self, f: Source):
        """Every position in `f` holding a value known to be a key space."""
        out = []
        text = f.masked
        interesting = set(self.consts) | set(self.keyfns)
        interesting |= {nm for (rel, _i, nm) in self.locals if rel == f.rel}
        interesting |= {nm for (rel, nm) in self.fields if rel == f.rel}
        for name in interesting & f.ident_pos.keys():
            for istart, iend in f.ident_pos[name]:
                # The binder of a `let`/`for` is not an occurrence of the value.
                if re.search(r"\b(?:let|for)\s+(?:mut\s+)?$", statement_head(text, istart)):
                    continue
                keyspaces = set()
                if name in self.consts:
                    if re.search(r"\b(?:const|static)\s+$", statement_head(text, istart)):
                        continue
                    keyspaces.add(self.consts[name])
                if name in self.keyfns and text[iend : iend + 40].lstrip().startswith("("):
                    call_open = text.index("(", iend)
                    out.append(
                        (istart, match_paren(text, call_open), set(self.keyfns[name]) | keyspaces)
                    )
                    continue
                fi = f.fn_at(istart)
                if fi is not None and (f.rel, fi, name) in self.locals:
                    keyspaces |= self.locals[(f.rel, fi, name)]
                if istart > 0 and text[istart - 1] == "." and (f.rel, name) in self.fields:
                    keyspaces |= self.fields[(f.rel, name)]
                if keyspaces:
                    out.append((istart, iend, keyspaces))
        for m in FORMAT_RE.finditer(text):
            if in_spans(f.excluded, m.start()):
                continue
            body = f.string_at(m.end())
            if body is None:
                continue
            fi = f.fn_at(m.start())
            local_map = {}
            if fi is not None:
                for (rel, idx, nm), v in self.locals.items():
                    if rel == f.rel and idx == fi:
                        local_map[nm] = v
            keyspaces = format_keyspaces(body, self.consts, local_map)
            if keyspaces:
                out.append(
                    (m.start(), match_paren(text, text.index("(", m.start())), keyspaces)
                )
        for s0, s1, body in f.strings:
            if in_spans(f.excluded, s0):
                continue
            # A bare key-shaped literal, e.g. `store.get("sys/tenancy", ..)`.
            # The const-declaration form is already an atom through its name.
            if (
                body.startswith(CONTROL_PLANE_ROOTS)
                and "{" not in body
                and KEYLIKE_RE.fullmatch(body)
                and not re.search(
                    r"\b(?:const|static)[ \t]+" + IDENT + r"[^;=]*=[ \t\n]*$",
                    statement_head(text, s0),
                )
            ):
                out.append((s0, s1, {body}))
        return out

    def flow(self, f: Source, start: int, end: int, ks: str, depth: int) -> None:
        """Follow one key-bearing value outward from [start, end)."""
        if depth > self.MAX_DEPTH:
            return
        text = f.masked
        new_end, alive, terminal = skip_postfix(text, end)
        if not alive:
            if not (
                terminal in INERT
                or terminal in CONTAINER_WRITE
                or terminal in RECEIVER_INERT
                or str(terminal).startswith("field:")
            ):
                self.unclassified[(f.rel, "." + str(terminal))] = (ks, f.line(start))
            return
        enc = enclosing_call(text, new_end)
        if enc is None:
            self.flow_binding(f, start, end, new_end, ks, depth)
            return
        callee, argidx, open_idx, callee_start = enc
        close = match_paren(text, open_idx)
        if callee in STORE_CALLS:
            axis, keyidx = STORE_CALLS[callee]
            if argidx != keyidx:
                # A key in a non-key position (a marker, a delimiter) is a call
                # shape this pass does not model; it must not be read as an
                # axis and must not be dropped either.
                self.unclassified[(f.rel, f"{callee}#{argidx}")] = (ks, f.line(start))
                return
            self.add(self.derived.setdefault(ks, {}), axis, f"{f.rel}:{f.line(start)}")
            literal = f.str_span.get((start, end))
            if literal is not None:
                self.add(self.literal_keys, literal, f"{f.rel}:{f.line(start)}")
            if callee in LIST_CALLS:
                self.flow(f, open_idx, close, ks, depth + 1)
            return
        if callee[:1].isupper() and callee not in IDENTITY:
            # A type or enum-variant constructor. It holds the key, it does not
            # store it; the value is followed through its binding instead.
            return
        if callee in IDENTITY:
            self.flow(f, callee_start, close, ks, depth + 1)
            return
        if callee in CONTAINER_WRITE:
            self.taint_receiver(f, callee_start, start, ks)
            return
        if callee.endswith("!"):
            if callee not in MACRO_INERT:
                self.unclassified[(f.rel, callee)] = (ks, f.line(start))
            return
        if callee in INERT:
            return
        # A call of a function defined in this workspace: bind its parameter.
        # Same-file first, then a unique definition, so three unrelated
        # `reap_keys` do not contaminate each other.
        targets = [(g, i) for (g, i) in self.fn_by_name.get(callee, []) if g is f]
        if not targets:
            targets = self.fn_by_name.get(callee, [])
            if len(targets) != 1:
                targets = []
        if targets:
            g, i = targets[0]
            params = g.fns[i][3]
            if argidx < len(params):
                self.add(self.locals, (g.rel, i, params[argidx]), ks)
            return
        self.unclassified[(f.rel, callee)] = (ks, f.line(start))

    def flow_binding(
        self, f: Source, start: int, end: int, new_end: int, ks: str, depth: int
    ) -> None:
        """No enclosing call: record what the value was bound to instead."""
        text = f.masked
        head = statement_head(text, start)
        fi = f.fn_at(start)
        mlet = re.search(r"\blet[ \t]+(?:mut[ \t]+)?(" + IDENT + r")[^=;]*=[^=;]*$", head)
        mfor = re.search(r"\bfor[ \t]+(" + IDENT + r")[ \t]+in[ \t\n]*&?[ \t\n]*$", head)
        if mlet and fi is not None:
            self.add(self.locals, (f.rel, fi, mlet.group(1)), ks)
            return
        if mfor and fi is not None:
            self.add(self.locals, (f.rel, fi, mfor.group(1)), ks)
            return
        if re.search(r"\breturn[ \t\n]*$", head) and fi is not None:
            self.add(self.keyfns, f.fns[fi][0], ks)
            return
        brace = enclosing_brace(text, start)
        if brace is not None:
            name, _macro, _s = callee_before(text, brace)
            if name and name[:1].isupper():
                # A struct literal: the field now holds a key.
                field = None
                mf = re.search(r"(" + IDENT + r")[ \t]*:[^,{}:]*$", head)
                if mf:
                    field = mf.group(1)
                elif re.fullmatch(r"[ \t\n]*(?:[^,]*,)?[ \t\n]*", head) or head.strip().endswith(","):
                    # `Struct { field }` shorthand: the value's own name is the
                    # field's.
                    field = text[start:end]
                if field and re.fullmatch(IDENT, field):
                    self.add(self.fields, (f.rel, field), ks)
                    return
        j = new_end
        while j < len(text) and text[j] in " \t\n":
            j += 1
        if fi is not None and j < len(text) and text[j] == "}" and j == f.fns[fi][2] - 1:
            # The tail expression of a function: the function returns keys.
            self.add(self.keyfns, f.fns[fi][0], ks)
        elif brace is not None and j < len(text) and text[j] == "}" and j == match_brace(text, brace) - 1:
            self.flow(f, brace, j + 1, ks, depth + 1)
        elif brace is not None and "=>" in head:
            mm = re.search(r"\bmatch\b", statement_head(text, brace))
            if mm:
                mstart = statement_head_start(text, brace) + mm.start()
                self.flow(f, mstart, match_brace(text, brace), ks, depth + 1)

    def taint_receiver(self, f: Source, callee_start: int, start: int, ks: str) -> None:
        text = f.masked
        j = callee_start - 1
        while j >= 0 and text[j] in " \t\n":
            j -= 1
        if j < 0 or text[j] != ".":
            return
        k = j
        while k > 0 and (text[k - 1].isalnum() or text[k - 1] in "_."):
            k -= 1
        chain = text[k:j]
        base = chain.split(".")[-1]
        if "." in chain:
            self.add(self.fields, (f.rel, base), ks)
            return
        fi = f.fn_at(start)
        if fi is not None and re.fullmatch(IDENT, base):
            self.add(self.locals, (f.rel, fi, base), ks)


# ---------------------------------------------------------------------------
# Discovery: every key-space value the sources name.
# ---------------------------------------------------------------------------
CONST_RE = re.compile(
    r'(?m)^[ \t]*(?:pub(?:\([^)]*\))?[ \t]+)?const[ \t]+([A-Z0-9_]+)[ \t]*:[ \t]*'
    r"&(?:'static[ \t]+)?str[ \t]*=[ \t]*\"([^\"]*)\"[ \t]*;"
)
DISCOVERY_FORMAT_RE = re.compile(r'format!\(\s*"([^"]*)"')


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


def is_production(rel: str) -> bool:
    return not any(part in NON_PRODUCTION_DIRS for part in rel.split("/")[:-1])


source_paths = walk_sources(root)
if len(source_paths) < MIN_SOURCES:
    die(
        f"found {len(source_paths)} Rust sources under {root}, below the floor of "
        f"{MIN_SOURCES}. A moved or renamed source tree must fail here rather "
        f"than report every key space clean"
    )

# key-space value -> the sites that name it.
literals: dict[str, set[str]] = {}
production: list[Source] = []
for path in source_paths:
    rel = path.relative_to(root).as_posix()
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as err:
        die(f"cannot read {path}: {err}")
    for match in CONST_RE.finditer(text):
        name, value = match.group(1), match.group(2)
        if value.startswith(CONTROL_PLANE_ROOTS):
            literals.setdefault(value, set()).add(f"{rel}:{name}")
    for match in DISCOVERY_FORMAT_RE.finditer(text):
        template = match.group(1)
        if not template.startswith(CONTROL_PLANE_ROOTS):
            continue
        # Everything up to the first interpolation is the fixed key space; the
        # rest is runtime. `format!("sys/t/{}")` names `sys/t/`.
        prefix = template.split("{", 1)[0]
        if prefix.startswith(CONTROL_PLANE_ROOTS):
            literals.setdefault(prefix, set()).add(f"{rel}:format!")
    if is_production(rel):
        production.append(Source(rel, text))

derivation = Derivation(production)
derivation.run()

# A bare string literal handed straight to a store call is a key space too,
# and the two spellings above do not see it.
for value, sites in derivation.literal_keys.items():
    literals.setdefault(value, set()).update(sites)

if len(literals) < MIN_LITERALS:
    die(
        f"discovered {len(literals)} control-plane key-space literal(s) under "
        f"{root}, below the floor of {MIN_LITERALS}. A tightened pattern or a "
        f"renamed constant form must fail here, not empty the scan"
    )

unreadable = {
    site: fact
    for site, fact in derivation.unclassified.items()
    if site not in UNCLASSIFIED_ALLOWLIST
}
if unreadable:
    for (rel, callee), (ks, line) in sorted(unreadable.items()):
        print(
            f"check-iam-keyspace-axes.sh: {rel}:{line} passes a key under "
            f"{ks!r} to {callee!r}, which the axis derivation does not "
            f"recognise",
            file=sys.stderr,
        )
    die(
        f"{len(unreadable)} key-bearing call site(s) could not be classified. "
        f"Teach the derivation the call, or add each (source, callee) to "
        f"UNCLASSIFIED_ALLOWLIST with the reason it exercises no axis"
    )

derived_axis_count = sum(len(axes) for axes in derivation.derived.values())
if derived_axis_count < MIN_DERIVED_AXES:
    die(
        f"derived {derived_axis_count} key-space/axis pair(s) from "
        f"{len(production)} production source(s), below the floor of "
        f"{MIN_DERIVED_AXES}. A derivation that stops reaching the call sites "
        f"reports every declared axis correct"
    )


# ---------------------------------------------------------------------------
# The shipped templates.
# ---------------------------------------------------------------------------
BUCKET_ARN_PREFIX = "arn:aws:s3:::"

# Condition operators whose `s3:prefix` values are key patterns this guard can
# read as a grant. `StringLike` globs and `StringEquals` does not, but an exact
# value is a glob with no wildcards, so both are matched the same way. An
# operator outside this set (a negation, a case-insensitive form, an ARN or
# numeric comparison) changes what the condition means, so it refuses rather
# than being read as one of these.
PREFIX_CONDITION_OPERATORS = {
    "StringLike",
    "StringLikeIfExists",
    "StringEquals",
    "StringEqualsIfExists",
    "ForAnyValue:StringLike",
    "ForAnyValue:StringEquals",
    "ForAllValues:StringLike",
    "ForAllValues:StringEquals",
}


def as_list(value):
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return value
    return None


def list_prefixes(path: Path, index: int, statement: dict):
    """The `s3:prefix` patterns a ListBucket Allow is conditioned on.

    None means unconditioned: the Allow admits the whole bucket.
    """
    condition = statement.get("Condition", {})
    if not isinstance(condition, dict):
        die(f"{path} statement {index} has a non-object Condition")
    found = None
    for operator, tests in condition.items():
        if not isinstance(tests, dict):
            die(f"{path} statement {index} condition {operator!r} is not an object")
        if "s3:prefix" not in tests:
            continue
        if operator not in PREFIX_CONDITION_OPERATORS:
            die(
                f"{path} statement {index} conditions s3:prefix under "
                f"{operator!r}, an operator this guard does not model. Reading "
                f"it as one it does model would report a grant it may not be"
            )
        prefixes = as_list(tests["s3:prefix"])
        if prefixes is None:
            die(f"{path} statement {index} has a non-string s3:prefix condition value")
        found = (found or []) + prefixes
    return found


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
                prefixes = list_prefixes(path, index, statement)
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


def walk_glob(pattern: str, states: set, text: str) -> set:
    """Advance a set of pattern offsets over every character of `text`.

    IAM StringLike: `*` is any run of characters (`/` included), `?` is
    exactly one. The result is the offsets the pattern can be at once `text`
    has been consumed; empty means it cannot consume `text` at all.
    """
    end = len(pattern)

    def closure(seed: set) -> set:
        out = set(seed)
        stack = list(seed)
        while stack:
            i = stack.pop()
            if i < end and pattern[i] == "*" and (i + 1) not in out:
                out.add(i + 1)
                stack.append(i + 1)
        return out

    current = closure(states)
    for char in text:
        following = set()
        for i in current:
            if i >= end:
                continue
            token = pattern[i]
            if token == "*":
                following.add(i)
            elif token == "?" or token == char:
                following.add(i + 1)
        if not following:
            return set()
        current = closure(following)
    return current


def reaches(pattern: str, keyspace: str) -> bool:
    """Does `pattern` match at least one key inside `keyspace`?

    That is the question a grant answers, and it is not a prefix test in
    either direction: `quarantine/t/*/*/l0/*` reaches `quarantine/`, `sys/*`
    reaches every key space under `sys/`, `sys/maintain/*/current` reaches
    `sys/maintain/workers/` through a wildcard it spans, and `sys/gc` reaches
    the single-object key space it names exactly.

    A key is inside the key space when it IS the key space or sits under it on
    a path segment boundary, so `sys/authx` is not a key of `sys/auth`.
    """
    states = walk_glob(pattern, {0}, keyspace)
    if not states:
        return False
    # Whatever is left of the pattern after the key space always matches some
    # key, so the only question left is whether that key is inside it.
    if any(set(pattern[i:]) <= {"*"} for i in states):
        return True  # the pattern names the key space itself
    if keyspace.endswith("/"):
        return True  # every longer key the pattern still matches is under it
    return bool(walk_glob(pattern, states, "/"))


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

# The declared key space each derived one belongs to: the longest declaration
# that covers it, so a narrower declaration takes its own axes.
declared = sorted((e["keyspace"] for e in MANIFEST), key=len, reverse=True)


def declaring(value: str):
    for ks in declared:
        if covers(ks, value):
            return ks
    return None


derived_by_declaration: dict[str, dict[str, set[str]]] = {}
for value, axes in derivation.derived.items():
    owner_ks = declaring(value)
    if owner_ks is None:
        continue
    bucket = derived_by_declaration.setdefault(owner_ks, {})
    for axis, sites in axes.items():
        bucket.setdefault(axis, set()).update(sites)

# Rule 1: every discovered key-space value falls inside a declared key space.
for literal in sorted(literals):
    if declaring(literal) is not None:
        continue
    sites = ", ".join(sorted(literals[literal]))
    findings.append(
        f"undeclared: the key space {literal!r} ({sites}) is named by no "
        f"MANIFEST entry in scripts/guards/check-iam-keyspace-axes.sh. Add one "
        f"stating, for each of {'/'.join(AXES)}, either the roles whose "
        f"process runs the call or the reason the axis is unused. An "
        f"undeclared control-plane key space is how IAM grants have shipped "
        f"with an axis refused (#1975)."
    )

# Rule 2: every declared key space is still named by something.
for entry in MANIFEST:
    ks = entry["keyspace"]
    if any(covers(ks, literal) for literal in literals):
        continue
    findings.append(
        f"stale-keyspace: MANIFEST declares {ks!r} but no constant, format! "
        f"template or store-call literal in the workspace names it any more. A "
        f"rename moves the declaration; an orphan one silently stops checking "
        f"anything."
    )

# Rule 3: the declared axis set equals the derived one, both directions.
for entry in MANIFEST:
    ks = entry["keyspace"]
    found = derived_by_declaration.get(ks, {})
    for axis in AXES:
        state, _roles, note = entry[axis]
        sites = sorted(found.get(axis, ()))
        if sites and state == "unused":
            findings.append(
                f"undeclared-axis: {ks!r} declares the {axis} axis unused "
                f"({note}), but the code exercises it at {', '.join(sites)}. "
                f"An axis that is called and declared unused is #1975's shape "
                f"on an already-declared key space: declare it used, naming "
                f"the roles whose process runs the call."
            )
        elif not sites and state == "used" and found:
            findings.append(
                f"stale-used: {ks!r} declares the {axis} axis used ({note}), "
                f"but the derivation finds no call on it while it does find "
                f"{'/'.join(sorted(found))} under the same key space. The call "
                f"moved or went away; move the declaration with it."
            )

# Rule 4: every declared-used axis is granted to each of its owner roles, and
# every recorded gap is still a gap.
for entry in MANIFEST:
    ks = entry["keyspace"]
    for axis in AXES:
        state, roles, note = entry[axis]
        if state == "used":
            for role in roles:
                patterns = ROLE_PATTERNS[role][axis]
                granted = [p for p in patterns if reaches(p, ks)]
                checked += 1
                gap = KNOWN_GAPS.get((ks, axis, role))
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
            # An unused axis has no owner, so every role is asked: a grant
            # nobody needs is as much a finding as a need nobody granted.
            for role in ROLES:
                granted = [
                    p
                    for p in ROLE_PATTERNS[role][axis]
                    if reaches(p, ks) and not is_blanket(p, ks)
                ]
                if not granted:
                    continue
                findings.append(
                    f"stale-unused: {ks!r} declares the {axis} axis unused "
                    f"({note}), but deploy/iam/{role}.json grants it through "
                    f"{granted!r}. The grant is unnecessary, or the code gained "
                    f"a call this scan cannot see."
                )

# Rule 5: a recorded gap whose axis or owner the MANIFEST no longer declares.
for (ks, axis, role) in sorted(KNOWN_GAPS):
    entry = next(e for e in MANIFEST if e["keyspace"] == ks)
    state, roles, _note = entry[axis]
    if state != "used":
        findings.append(
            f"stale-gap: KNOWN_GAPS records {role} as lacking the {axis} axis "
            f"over {ks!r}, but MANIFEST now declares that axis unused. Remove "
            f"the entry so the next gap is visible."
        )
    elif role not in roles:
        findings.append(
            f"stale-gap: KNOWN_GAPS records {role} as lacking the {axis} axis "
            f"over {ks!r}, but MANIFEST no longer names {role} as a role that "
            f"exercises it. Remove the entry so the next gap is visible."
        )

live_keyspaces = sum(
    1 for entry in MANIFEST if any(covers(entry["keyspace"], lit) for lit in literals)
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
    f"check-iam-keyspace-axes.sh: clean ({checked} role/axis grant checks over "
    f"{live_keyspaces} key space(s) from {len(literals)} literal(s) in "
    f"{len(source_paths)} source(s); {derived_axis_count} axis/key-space pair(s) "
    f"derived from {len(production)} production source(s) in "
    f"{derivation.rounds} round(s), {len(KNOWN_GAPS)} known gap(s))"
)
PY
