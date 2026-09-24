#!/usr/bin/env bash
# Cases for check-iam-keyspace-axes.sh, in the pattern of
# check-workflow-permissions.test.sh: add a case here before changing a rule.
#
# Each case is a throwaway tree under $TMPDIR carrying the four REAL shipped
# templates plus generated sources that name every declared key space AND
# exercise it on exactly the axes the MANIFEST declares used, so a case mutates
# exactly one thing and the rest of the scan stays true. The guard takes the
# tree as its argument, so nothing here reads the real workspace sources.
#
# The guard's own declarations (its MANIFEST, its KNOWN_GAPS and its
# unclassified allowlist) cannot be driven from a fixture tree, so the cases
# for those refusals run a COPY of the guard with one literal string replaced.
# `mutate_guard` is that copy; it refuses a replacement that does not match
# exactly once, so a reworded declaration fails here rather than silently
# testing nothing.
#
# Every rule and every refusal below was mutation-proven: it was removed from a
# copy of the guard, the case named beside it was watched failing, and it was
# restored.
#
# Run: bash scripts/guards/check-iam-keyspace-axes.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"
GUARD="${HERE}/check-iam-keyspace-axes.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-iam-keyspace-axes-test.XXXXXX")"
trap 'chmod -R u+rwX "${TMP}" 2>/dev/null; rm -rf "${TMP}"' EXIT

fails=0
passes=0

# The base tree, built once and copied per case. `keyspaces.rs` names every key
# space the MANIFEST declares, in the three spellings discovery reads (a
# `const NAME: &str`, a `format!` template, and a bare literal handed to a
# store call). `calls.rs` exercises each on exactly the axes the MANIFEST
# declares used, so the derivation has the same 32 key-space/axis pairs to find
# here as it does in the workspace.
BASE="${TMP}/base"
mkdir -p "${BASE}/deploy/iam" "${BASE}/crates/fixture/src"
cp "${REPO}/deploy/iam/gateway.json" "${BASE}/deploy/iam/gateway.json"
cp "${REPO}/deploy/iam/query.json" "${BASE}/deploy/iam/query.json"
cp "${REPO}/deploy/iam/maintain.json" "${BASE}/deploy/iam/maintain.json"
cp "${REPO}/deploy/iam/admin.json" "${BASE}/deploy/iam/admin.json"

cat >"${BASE}/crates/fixture/src/keyspaces.rs" <<'RS'
// Every key space check-iam-keyspace-axes.sh declares, in the spellings its
// discovery reads. Not a copy of any real module: only the key-space values
// matter here.
const QUERY_ADMISSION_PREFIX: &str = "admission/query/";
const QUARANTINE_PREFIX: &str = "quarantine/";
const AUTH_KEY: &str = "sys/auth";
const GC_CONFIG_KEY: &str = "sys/gc";
const COMPACTION_CLAIMS_PREFIX: &str = "sys/maintain/claims/compaction/";
const MEMO_PREFIX: &str = "sys/maintain/memo/";
const WORKERS_PREFIX: &str = "sys/maintain/workers/";
const QUALIFICATION_KEY: &str = "sys/qualification";
const QUERY_WORKERS_PREFIX: &str = "sys/query/workers/";
const TENANCY_MARKER_KEY: &str = "sys/tenancy";

fn scratch_prefix(run_id: &str) -> String {
    format!("sys/qualify/{run_id}/")
}

fn recovery_manifest_key(hash: &str) -> String {
    format!("sys/t/{hash}")
}
RS

cat >"${BASE}/crates/fixture/src/calls.rs" <<'RS'
// One function per key space, exercising exactly the axes the MANIFEST
// declares used over it. The store surface is the real one: the method names
// and the two listing helpers are what the guard's derivation classifies.

async fn query_admission(store: &dyn ObjectStoreBackend, body: Bytes) {
    let snapshots = list_all(store, QUERY_ADMISSION_PREFIX).await;
    for meta in snapshots {
        let _got = store.get(&meta.key).await;
    }
    store.put(QUERY_ADMISSION_PREFIX, body).await;
}

async fn sweep_orphans(store: &dyn ObjectStoreBackend, body: Bytes) {
    let held = list_all(store, QUARANTINE_PREFIX).await;
    for meta in held {
        store.delete(&meta.key).await;
    }
    store.put(QUARANTINE_PREFIX, body).await;
}

async fn auth_token_map(store: &dyn ObjectStoreBackend, body: Bytes) {
    let _got = store.get(AUTH_KEY).await;
    store.put(AUTH_KEY, body).await;
}

async fn gc_config(store: &dyn ObjectStoreBackend, body: Bytes) {
    let _got = store.get(GC_CONFIG_KEY).await;
    store.put(GC_CONFIG_KEY, body).await;
}

async fn compaction_claim(store: &dyn ObjectStoreBackend, body: Bytes) {
    let _got = store.get(COMPACTION_CLAIMS_PREFIX).await;
    store.put(COMPACTION_CLAIMS_PREFIX, body).await;
}

async fn memo_snapshot(store: &dyn ObjectStoreBackend, body: Bytes) {
    let memos = list_all(store, MEMO_PREFIX).await;
    for meta in memos {
        let _got = store.get(&meta.key).await;
    }
    store.put(MEMO_PREFIX, body).await;
}

async fn worker_set(store: &dyn ObjectStoreBackend, body: Bytes) {
    let workers = list_all(store, WORKERS_PREFIX).await;
    for meta in workers {
        let _got = store.get(&meta.key).await;
        store.delete(&meta.key).await;
    }
    store.put(WORKERS_PREFIX, body).await;
}

async fn qualification(store: &dyn ObjectStoreBackend, body: Bytes) {
    let _got = store.get(QUALIFICATION_KEY).await;
    store.put(QUALIFICATION_KEY, body).await;
}

async fn qualify_probe(store: &dyn ObjectStoreBackend, body: Bytes) {
    let prefix = scratch_prefix("run");
    store.put(&prefix, body).await;
    let _got = store.get(&prefix).await;
    let probes = list_all(store, &prefix).await;
    for meta in probes {
        store.delete(&meta.key).await;
    }
}

async fn query_workers(store: &dyn ObjectStoreBackend, body: Bytes) {
    let workers = list_all(store, QUERY_WORKERS_PREFIX).await;
    for meta in workers {
        let _got = store.get(&meta.key).await;
        store.delete(&meta.key).await;
    }
    store.put(QUERY_WORKERS_PREFIX, body).await;
}

async fn recovery_manifest(store: &dyn ObjectStoreBackend, body: Bytes) {
    let key = recovery_manifest_key("hash");
    store.put(&key, body).await;
}

async fn tenancy(store: &dyn ObjectStoreBackend, body: Bytes) {
    let _marker = store.get("sys/tenancy").await;
    let _again = store.get(TENANCY_MARKER_KEY).await;
    store.put(TENANCY_MARKER_KEY, body).await;
}
RS

# Filler sources, so the tree clears MIN_SOURCES the way the workspace does.
# A tree below that floor is its own case further down.
for i in $(seq 1 210); do
  printf 'pub fn filler_%s() -> u32 { %s }\n' "${i}" "${i}" \
    >"${BASE}/crates/fixture/src/filler_${i}.rs"
done

# new_tree <name>: a copy of the base tree. Prints its path.
new_tree() {
  local dir="${TMP}/$1"
  cp -r "${BASE}" "${dir}"
  printf '%s\n' "${dir}"
}

# check_guard <guard> <name> <want-exit> <want-substring-or-empty> <arg...>
check_guard() {
  local guard="$1" name="$2" want_rc="$3" want_sub="$4"
  shift 4
  local out rc=0
  out="$(bash "${guard}" "$@" 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing %s\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# check <name> <want-exit> <want-substring-or-empty> <arg...>
check() {
  local name="$1"
  shift
  check_guard "${GUARD}" "${name}" "$@"
}

# mutate_guard <name> <old> <new> [<old> <new> ...]: a copy of the guard with
# each literal substring replaced. Refuses a substring that is not present
# exactly once, so a reworded declaration fails loudly here. Prints its path.
mutate_guard() {
  local name="$1"
  shift
  python3 - "${GUARD}" "${TMP}/guard-${name}.sh" "$@" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
pairs = sys.argv[3:]
text = open(src).read()
for old, new in zip(pairs[0::2], pairs[1::2]):
    seen = text.count(old)
    if seen != 1:
        sys.exit(f"mutation {old!r} matched {seen} times, wanted exactly 1")
    text = text.replace(old, new)
open(dst, "w").write(text)
PY
  printf '%s\n' "${TMP}/guard-${name}.sh"
}

# drop_pattern <file> <sid> <substring>: remove every Resource (or s3:prefix)
# value containing <substring> from the named statement.
drop_pattern() {
  python3 - "$1" "$2" "$3" <<'PY'
import json, sys
path, sid, needle = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as fh:
    doc = json.load(fh)
for st in doc["Statement"]:
    if st.get("Sid") != sid:
        continue
    if "Resource" in st and isinstance(st["Resource"], list):
        st["Resource"] = [r for r in st["Resource"] if needle not in r]
    cond = st.get("Condition", {}).get("StringLike", {})
    if "s3:prefix" in cond:
        cond["s3:prefix"] = [p for p in cond["s3:prefix"] if needle not in p]
with open(path, "w") as fh:
    json.dump(doc, fh, indent=2)
PY
}

# add_pattern <file> <sid> <key>: append one object-key pattern to a statement.
add_pattern() {
  python3 - "$1" "$2" "$3" <<'PY'
import json, sys
path, sid, key = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as fh:
    doc = json.load(fh)
for st in doc["Statement"]:
    if st.get("Sid") == sid:
        st["Resource"].append(f"arn:aws:s3:::my-ravel-bucket/{key}")
with open(path, "w") as fh:
    json.dump(doc, fh, indent=2)
PY
}

# add_statement <file> <json>: append a whole statement.
add_statement() {
  python3 - "$1" "$2" <<'PY'
import json, sys
path, raw = sys.argv[1], sys.argv[2]
with open(path) as fh:
    doc = json.load(fh)
doc["Statement"].append(json.loads(raw))
with open(path, "w") as fh:
    json.dump(doc, fh, indent=2)
PY
}

# edit_statement <file> <sid> <json-fragment>: merge keys into a statement,
# or delete a key when its value is null.
edit_statement() {
  python3 - "$1" "$2" "$3" <<'PY'
import json, sys
path, sid, raw = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as fh:
    doc = json.load(fh)
for st in doc["Statement"]:
    if st.get("Sid") != sid:
        continue
    for key, value in json.loads(raw).items():
        if value is None:
            st.pop(key, None)
        else:
            st[key] = value
with open(path, "w") as fh:
    json.dump(doc, fh, indent=2)
PY
}

# --- clean -----------------------------------------------------------------

d="$(new_tree clean)"
check "the shipped templates pass against every declared key space" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# --- undeclared-axis: the #1975 shape on a key space already declared -------
# The defect this guard exists for, in the form typed-in axes cannot catch: no
# declaration changes when a new call lands on an existing key space, so a
# stale "unused" note stays green while the axis is exercised and granted
# nowhere. Killed by: deleting the undeclared-axis branch of rule 3, and by
# emptying STORE_CALLS of "delete".

d="$(new_tree new_call_on_declared_keyspace)"
python3 - "${d}/crates/fixture/src/calls.rs" <<'PY'
import sys
path = sys.argv[1]
text = open(path).read()
old = """    let memos = list_all(store, MEMO_PREFIX).await;
    for meta in memos {
        let _got = store.get(&meta.key).await;
    }"""
new = """    let memos = list_all(store, MEMO_PREFIX).await;
    for meta in memos {
        let _got = store.get(&meta.key).await;
        store.delete(&meta.key).await;
    }"""
assert text.count(old) == 1
open(path, "w").write(text.replace(old, new))
PY
check "flags a new delete call on a key space whose delete is declared unused" 1 \
  "undeclared-axis: 'sys/maintain/memo/' declares the delete axis unused" "${d}"

# --- stale-used: the same check in the other direction ----------------------
# Killed by: deleting the stale-used branch of rule 3.

d="$(new_tree call_removed)"
python3 - "${d}/crates/fixture/src/calls.rs" <<'PY'
import sys
path = sys.argv[1]
text = open(path).read()
old = """        let _got = store.get(&meta.key).await;
        store.delete(&meta.key).await;
    }
    store.put(WORKERS_PREFIX, body).await;"""
new = """        let _got = store.get(&meta.key).await;
    }
    store.put(WORKERS_PREFIX, body).await;"""
assert text.count(old) == 1
open(path, "w").write(text.replace(old, new))
PY
check "flags an axis declared used that no call exercises any more" 1 \
  "stale-used: 'sys/maintain/workers/' declares the delete axis used" "${d}"

# --- unclassified call sites ------------------------------------------------
# A key-bearing value reaching a call the derivation cannot read is the one
# place a derived axis set can silently lose an axis. Killed by: dropping the
# unreadable-site refusal (the tree then reports clean).

d="$(new_tree unclassified)"
printf 'fn sink_key() { external_sink(WORKERS_PREFIX); }\n' \
  >"${d}/crates/fixture/src/unclassified.rs"
check "refuses a key-bearing value reaching a call it cannot classify" 70 \
  "passes a key under 'sys/maintain/workers/' to 'external_sink'" "${d}"

# The complement: the allowlist is what makes that refusal answerable, so it
# has to work. Killed by: making the allowlist lookup always miss.
g="$(mutate_guard allowlisted \
  'UNCLASSIFIED_ALLOWLIST: dict[tuple[str, str], str] = {}' \
  'UNCLASSIFIED_ALLOWLIST: dict[tuple[str, str], str] = {("crates/fixture/src/unclassified.rs", "external_sink"): "the fixture case for this mechanism"}')"
check_guard "${g}" "an allowlisted call site with a reason is not a refusal" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# --- missing-grant: the #1975 defect ---------------------------------------
# Killed by: deleting the missing-grant rule, and by making `reaches` return
# True unconditionally.

d="$(new_tree gap_1975)"
drop_pattern "${d}/deploy/iam/maintain.json" MaintainDelete "sys/maintain/workers"
check "flags the #1975 shape: maintain reaps heartbeats with no delete Allow" 1 \
  "missing-grant: maintain exercises the delete axis over 'sys/maintain/workers/'" "${d}"

# The #1957 shape, three axes at once, on a key space outside `t/`.
d="$(new_tree gap_1957)"
drop_pattern "${d}/deploy/iam/maintain.json" MaintainDelete "quarantine/"
drop_pattern "${d}/deploy/iam/maintain.json" MaintainWrite "quarantine/"
drop_pattern "${d}/deploy/iam/maintain.json" MaintainList "quarantine/"
check "flags the #1957 shape: maintain sweeps quarantine with no grants" 1 \
  "missing-grant: maintain exercises the put axis over 'quarantine/'" "${d}"

# --- undeclared ------------------------------------------------------------
# Killed by: deleting the rule-1 loop.

d="$(new_tree undeclared_const)"
printf 'const NEW_PREFIX: &str = "sys/newthing/";\n' \
  >"${d}/crates/fixture/src/newthing.rs"
check "flags a new control-plane key space with no MANIFEST entry" 1 \
  "undeclared: the key space 'sys/newthing/'" "${d}"

d="$(new_tree undeclared_format)"
printf 'fn k(x: &str) -> String { format!("sys/relay/{x}") }\n' \
  >"${d}/crates/fixture/src/relay.rs"
check "flags an undeclared key space named only by a format! template" 1 \
  "undeclared: the key space 'sys/relay/'" "${d}"

# The third spelling: a key space that appears only as a bare literal in a
# store call, the form `store.get("sys/tenancy", ..)` uses. Killed by: dropping
# the bare-literal atom from the derivation's `atoms`, and by not folding
# `literal_keys` into `literals`.
d="$(new_tree undeclared_bare_literal)"
printf 'async fn probe(store: &dyn ObjectStoreBackend) { let _g = store.get("sys/bare/marker").await; }\n' \
  >"${d}/crates/fixture/src/bare.rs"
check "flags a key space named only by a store call's bare string literal" 1 \
  "undeclared: the key space 'sys/bare/marker'" "${d}"

# A literal INSIDE a declared key space is not a new key space: the
# declaration covers everything beneath it, which is what lets `quarantine/`
# be declared once for the whole root the sweep composes keys under.
d="$(new_tree nested_literal)"
printf 'fn k(p: &str) -> String { format!("sys/maintain/workers/{p}") }\n' \
  >"${d}/crates/fixture/src/nested.rs"
printf 'fn q(o: &str) -> String { format!("quarantine/{o}/held") }\n' \
  >"${d}/crates/fixture/src/held.rs"
# A literal STRICTLY below a declared one, so this case also fails if rule 1
# matches declarations by equality instead of by containment.
printf 'const LEASE_PREFIX: &str = "sys/maintain/workers/lease/";\n' \
  >"${d}/crates/fixture/src/lease.rs"
check "a literal inside a declared key space is not a finding" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# Containment is on path segment boundaries. `sys/authx` is a different key
# space from `sys/auth`, so it must be reported rather than swallowed.
# Killed by: making `covers` a plain str.startswith test.
d="$(new_tree sibling_literal)"
printf 'const AUTHX_KEY: &str = "sys/authx";\n' \
  >"${d}/crates/fixture/src/authx.rs"
check "a key space that merely shares a prefix is not covered by its sibling" 1 \
  "undeclared: the key space 'sys/authx'" "${d}"

# A declaration nested inside another takes its own axes: the narrower entry
# says nothing is deleted under it, and the wider entry's used delete must not
# answer for it. Killed by: matching a value against the SHORTEST covering
# declaration instead of the longest, which hands the call to the parent and
# reports clean.
d="$(new_tree nested_declaration)"
cat >"${d}/crates/fixture/src/lease.rs" <<'RS'
const LEASE_PREFIX: &str = "sys/maintain/workers/lease/";

async fn lease(store: &dyn ObjectStoreBackend, body: Bytes) {
    store.put(LEASE_PREFIX, body).await;
    store.delete(LEASE_PREFIX).await;
}
RS
g="$(mutate_guard nested_declaration \
  '    {
        "keyspace": "sys/maintain/workers/",' \
  '    {
        "keyspace": "sys/maintain/workers/lease/",
        "adr": "fixture: a declaration nested inside another",
        "list": ("unused", (), "fixture"),
        "get": ("unused", (), "fixture"),
        "put": ("used", ("maintain",), "fixture"),
        "delete": ("unused", (), "fixture"),
    },
    {
        "keyspace": "sys/maintain/workers/",')"
check_guard "${g}" "a nested declaration answers for its own axes, not its parent" \
  1 "undeclared-axis: 'sys/maintain/workers/lease/' declares the delete axis unused" \
  "${d}"

# --- stale-keyspace --------------------------------------------------------
# Killed by: deleting the rule-2 loop.

d="$(new_tree renamed)"
sed -i.bak 's|"sys/query/workers/"|"sys/queryworkers/v2/"|' \
  "${d}/crates/fixture/src/keyspaces.rs"
rm -f "${d}/crates/fixture/src/keyspaces.rs.bak"
check "flags a MANIFEST key space no source names any more" 1 \
  "stale-keyspace: MANIFEST declares 'sys/query/workers/'" "${d}"

# --- stale-unused ----------------------------------------------------------
# Killed by: dropping the stale-unused branch, and by making is_blanket return
# True unconditionally (which suppresses this case while the blanket case
# below still passes, so both are needed).

d="$(new_tree stale_unused)"
add_pattern "${d}/deploy/iam/maintain.json" MaintainDelete "sys/maintain/memo/*"
check "flags a grant appearing on an axis declared unused" 1 \
  "stale-unused: 'sys/maintain/memo/' declares the delete axis unused" "${d}"

# An unused axis has no owner role, so every role is asked. Killed by: asking
# only the roles that own some other axis of the same key space.
d="$(new_tree stale_unused_other_role)"
add_statement "${d}/deploy/iam/gateway.json" \
  '{"Sid":"GatewayMemoDelete","Effect":"Allow","Action":["s3:DeleteObject"],"Resource":["arn:aws:s3:::my-ravel-bucket/sys/maintain/memo/*"]}'
check "flags an unnecessary grant held by a role that owns no axis there" 1 \
  "deploy/iam/gateway.json grants it through" "${d}"

# A blanket root wildcard reaching a key space is the operator role's posture,
# not evidence that this axis is used. It must satisfy a need without
# creating one.
d="$(new_tree blanket)"
add_statement "${d}/deploy/iam/admin.json" \
  '{"Sid":"AdminBlanketDelete","Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":["arn:aws:s3:::my-ravel-bucket/sys/*"]}'
check "a blanket sys/* does not read as evidence an unused axis is used" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# The complement, so the blanket case above cannot pass vacuously: the SAME
# axis and role, granted specifically instead of blanket, is a finding.
d="$(new_tree specific)"
add_statement "${d}/deploy/iam/admin.json" \
  '{"Sid":"AdminSpecificDelete","Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":["arn:aws:s3:::my-ravel-bucket/sys/qualification"]}'
check "a specific grant on the same unused axis IS a finding" 1 \
  "stale-unused: 'sys/qualification' declares the delete axis unused" "${d}"

# --- stale-gap -------------------------------------------------------------
# Killed by: dropping the stale-gap branch of rule 4.

d="$(new_tree closed_gap)"
add_statement "${d}/deploy/iam/query.json" \
  '{"Sid":"QueryDelete","Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":["arn:aws:s3:::my-ravel-bucket/sys/query/workers/*"]}'
check "flags a KNOWN_GAPS entry whose grant now exists" 1 \
  "stale-gap: KNOWN_GAPS still records query as lacking the delete axis" "${d}"

# The other two ways a gap goes stale live in rule 5, which reads the MANIFEST
# rather than a template, so they are driven by a mutated guard. Redeclaring an
# axis unused also contradicts the fixture's call on it, so the exit code alone
# would not tell rule 5 from rule 3: the message is what pins rule 5 here.
g="$(mutate_guard gap_axis_unused \
  '"list": (
            "used",
            ("maintain",),
            "crates/ravel-maintain/src/memo_snapshot.rs:81 list_all(store, MEMO_PREFIX)",' \
  '"list": (
            "unused",
            (),
            "crates/ravel-maintain/src/memo_snapshot.rs:81 list_all(store, MEMO_PREFIX)",')"
check_guard "${g}" "flags a KNOWN_GAPS entry whose axis is now declared unused" \
  1 "but MANIFEST now declares that axis unused" "${BASE}"

g="$(mutate_guard gap_role_dropped \
  '"used", ("gateway",),' '"used", ("query",),')"
check_guard "${g}" "flags a KNOWN_GAPS entry whose role no longer owns the axis" \
  1 "but MANIFEST no longer names gateway as a role that exercises it" "${BASE}"

# --- how a pattern reaches a key space -------------------------------------

# A wildcard the pattern spans, with a literal tail after it. A prefix test in
# either direction misses this. Killed by: replacing `reaches` with a pair of
# str.startswith tests.
d="$(new_tree wildcard_with_tail)"
drop_pattern "${d}/deploy/iam/maintain.json" MaintainDelete "sys/maintain/workers"
add_pattern "${d}/deploy/iam/maintain.json" MaintainDelete "sys/maintain/*/heartbeat"
check "a grant reaches through a wildcard with a literal tail after it" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# The single-key form: a pattern that names the key space exactly, here with a
# `?` standing for its last character. Killed by: dropping the
# names-the-key-space-itself branch of `reaches`, and by dropping `?` from
# `walk_glob`.
d="$(new_tree question_mark_glob)"
drop_pattern "${d}/deploy/iam/maintain.json" MaintainWrite "sys/gc"
add_pattern "${d}/deploy/iam/maintain.json" MaintainWrite "sys/g?"
check "a pattern naming the key space exactly, through a ? glob, grants it" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# Reach is on segment boundaries too: `sys/authx` grants nothing under
# `sys/auth`, so the recorded gateway gap must stay a gap. Killed by: making
# `reaches` accept any pattern that starts with the key space.
d="$(new_tree reach_boundary)"
add_pattern "${d}/deploy/iam/gateway.json" GatewayRead "sys/authx"
check "a grant one character past a key space's boundary does not reach it" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# The blanket wildcard above a key space, on a USED axis, where it does count.
# Killed by: dropping the wildcard walk from `walk_glob`.
d="$(new_tree blanket_read_removed)"
drop_pattern "${d}/deploy/iam/admin.json" AdminRead "sys/*"
check "admin's blanket sys/* is what grants it the reads it exercises" 1 \
  "missing-grant: admin exercises the get axis over 'sys/auth'" "${d}"

# A ListBucket Allow carrying no s3:prefix condition admits the whole bucket.
# Killed by: reading a condition-less ListBucket as granting nothing, which
# leaves the recorded memo gap in place and the tree clean.
d="$(new_tree list_without_condition)"
edit_statement "${d}/deploy/iam/maintain.json" MaintainList '{"Condition": null}'
check "a ListBucket Allow with no s3:prefix condition admits the whole bucket" 1 \
  "stale-gap: KNOWN_GAPS still records maintain as lacking the list axis" "${d}"

# An s3:prefix under an operator other than StringLike is read the same way.
# Killed by: reading only Condition.StringLike, which drops every prefix and
# leaves maintain granted nothing on the list axis.
d="$(new_tree prefix_under_string_equals)"
python3 - "${d}/deploy/iam/maintain.json" <<'PY'
import json, sys
path = sys.argv[1]
with open(path) as fh:
    doc = json.load(fh)
for st in doc["Statement"]:
    if st.get("Sid") == "MaintainList":
        prefixes = st["Condition"].pop("StringLike")
        st["Condition"]["ForAnyValue:StringEquals"] = prefixes
with open(path, "w") as fh:
    json.dump(doc, fh, indent=2)
PY
check "an s3:prefix under another modelled operator is read as a grant" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# --- what the scan does not walk -------------------------------------------
# Killed by: emptying SKIP_DIRS, which makes build output a source of key
# spaces.

d="$(new_tree skip_dirs)"
mkdir -p "${d}/target/debug/build" "${d}/node_modules/pkg"
printf 'const STALE: &str = "sys/oldbuild/";\n' \
  >"${d}/target/debug/build/generated.rs"
printf 'const VENDORED: &str = "sys/vendored/";\n' \
  >"${d}/node_modules/pkg/lib.rs"
check "build output and vendored trees are not scanned for key spaces" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# Every dot directory, not only the three SKIP_DIRS names. Killed by: dropping
# the leading-dot test from walk_sources.
d="$(new_tree dot_dirs)"
mkdir -p "${d}/.worktrees/other/src"
printf 'const OTHER: &str = "sys/otherbranch/";\n' \
  >"${d}/.worktrees/other/src/lib.rs"
check "a dot directory is not scanned for key spaces" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# A symlink is not walked, in either spelling: a link out of the tree would
# make the scan depend on what sits beside the tree, and a link back into it
# would count the same source twice. Killed by: dropping the is_symlink test.
printf 'const OUTSIDE: &str = "sys/outside/";\n' >"${TMP}/outside.rs"
d="$(new_tree symlinked_source)"
ln -s "${TMP}/outside.rs" "${d}/crates/fixture/src/linked.rs"
ln -s "${TMP}/base/crates" "${d}/crates/fixture/src/linked_tree"
check "a symlinked source or directory is not scanned" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# Test sources are scanned for key spaces but not for axes: a test writing a
# key is not a role exercising an axis. Killed by: dropping the
# NON_PRODUCTION_DIRS filter, which derives a delete axis from the test and
# reports undeclared-axis.
d="$(new_tree test_sources)"
mkdir -p "${d}/crates/fixture/tests"
printf 'async fn t(store: &dyn ObjectStoreBackend) { store.delete(AUTH_KEY).await; }\n' \
  >"${d}/crates/fixture/tests/auth.rs"
check "a store call under tests/ does not make an axis used" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# --- anti-vacuity floors ---------------------------------------------------
# Killed by: lowering each of MIN_SOURCES, MIN_LITERALS, MIN_DERIVED_AXES and
# MIN_KEYSPACES_LIVE to 0, which turns the matching case green while the clean
# case stays green -- the exact silent no-op these refuse.

d="${TMP}/few_sources"
mkdir -p "${d}/deploy/iam" "${d}/src"
cp "${REPO}"/deploy/iam/*.json "${d}/deploy/iam/"
cp "${BASE}/crates/fixture/src/keyspaces.rs" "${d}/src/keyspaces.rs"
cp "${BASE}/crates/fixture/src/calls.rs" "${d}/src/calls.rs"
check "refuses a tree whose source count is below the floor" 70 \
  "below the floor of 200" "${d}"

d="$(new_tree few_literals)"
printf 'const WORKERS_PREFIX: &str = "sys/maintain/workers/";\n' \
  >"${d}/crates/fixture/src/keyspaces.rs"
check "refuses a tree where discovery finds almost no key spaces" 70 \
  "below the floor of 10" "${d}"

# The literal floor does not cover a derivation that stopped reaching the call
# sites: every key space is still named, and every declared axis reports clean
# because nothing contradicts it.
d="$(new_tree few_derived)"
rm "${d}/crates/fixture/src/calls.rs"
check "refuses a tree where the axis derivation reaches almost nothing" 70 \
  "below the floor of 20" "${d}"

# Nor does it cover a sweeping rename: a tree can name plenty of key spaces
# while most of them no longer match a declaration.
d="$(new_tree few_live)"
sed -i.bak \
  -e 's|"sys/gc"|"sys/v2gc"|' \
  -e 's|"sys/maintain/claims/compaction/"|"sys/v2claims/compaction/"|' \
  -e 's|"sys/maintain/memo/"|"sys/v2memo/"|' \
  -e 's|"sys/qualification"|"sys/v2qualification"|' \
  -e 's|"sys/query/workers/"|"sys/v2queryworkers/"|' \
  -e 's|"sys/tenancy"|"sys/v2tenancy"|' \
  "${d}/crates/fixture/src/keyspaces.rs"
rm -f "${d}/crates/fixture/src/keyspaces.rs.bak"
check "refuses a tree where most declared key spaces went missing" 70 \
  "below the floor of 8" "${d}"

# --- the scan could not run: the tree ---------------------------------------

check "refuses a root that is not a directory" 70 "is not a directory" \
  "${TMP}/no-such-tree"

d="$(new_tree unreadable_dir)"
mkdir -p "${d}/crates/fixture/src/locked"
printf 'const L: &str = "sys/locked/";\n' >"${d}/crates/fixture/src/locked/x.rs"
chmod 000 "${d}/crates/fixture/src/locked"
check "refuses when a directory in the tree cannot be listed" 70 \
  "cannot read " "${d}"
chmod 755 "${d}/crates/fixture/src/locked"

d="$(new_tree unreadable_source)"
chmod 000 "${d}/crates/fixture/src/keyspaces.rs"
check "refuses when a source in the tree cannot be read" 70 "cannot read " "${d}"
chmod 644 "${d}/crates/fixture/src/keyspaces.rs"

# --- the scan could not run: the templates ----------------------------------
# Each of these is a template this guard would otherwise read as granting
# nothing, which reports as a finding list rather than as a scan that failed.

d="$(new_tree no_template)"
rm "${d}/deploy/iam/query.json"
check "refuses when a role template is missing" 70 "is not readable" "${d}"

d="$(new_tree bad_json)"
printf '{ "Version": "2012-10-17", "Statement": [\n' >"${d}/deploy/iam/admin.json"
check "refuses when a role template does not parse" 70 "does not parse as JSON" "${d}"

d="$(new_tree no_statements)"
printf '{ "Version": "2012-10-17" }\n' >"${d}/deploy/iam/admin.json"
check "refuses a template with no Statement list" 70 "has no Statement list" "${d}"

d="$(new_tree statement_not_object)"
add_statement "${d}/deploy/iam/admin.json" '"AdminWrite"'
check "refuses a Statement entry that is not an object" 70 \
  "is not an object" "${d}"

d="$(new_tree no_action)"
edit_statement "${d}/deploy/iam/admin.json" AdminWrite '{"Action": null}'
check "refuses an Allow statement with no usable Action" 70 \
  "has no usable Action" "${d}"

d="$(new_tree no_resource)"
edit_statement "${d}/deploy/iam/admin.json" AdminWrite '{"Resource": null}'
check "refuses an Allow statement with no usable Resource" 70 \
  "has no usable Resource" "${d}"

d="$(new_tree resource_not_string)"
edit_statement "${d}/deploy/iam/admin.json" AdminWrite '{"Resource": [17]}'
check "refuses a non-string Resource" 70 "has a non-string Resource" "${d}"

d="$(new_tree resource_not_arn)"
edit_statement "${d}/deploy/iam/admin.json" AdminWrite '{"Resource": ["my-ravel-bucket/sys/gc"]}'
check "refuses a Resource that is not an S3 ARN" 70 "is not an S3 ARN" "${d}"

d="$(new_tree condition_not_object)"
edit_statement "${d}/deploy/iam/maintain.json" MaintainList '{"Condition": "sys/*"}'
check "refuses a non-object Condition" 70 "has a non-object Condition" "${d}"

d="$(new_tree condition_test_not_object)"
edit_statement "${d}/deploy/iam/maintain.json" MaintainList \
  '{"Condition": {"StringLike": "sys/*"}}'
check "refuses a condition operator whose tests are not an object" 70 \
  "is not an object" "${d}"

d="$(new_tree unknown_prefix_operator)"
edit_statement "${d}/deploy/iam/maintain.json" MaintainList \
  '{"Condition": {"StringNotLike": {"s3:prefix": ["sys/maintain/*"]}}}'
check "refuses an s3:prefix under an operator it does not model" 70 \
  "an operator this guard does not model" "${d}"

d="$(new_tree prefix_not_string)"
edit_statement "${d}/deploy/iam/maintain.json" MaintainList \
  '{"Condition": {"StringLike": {"s3:prefix": 17}}}'
check "refuses a non-string s3:prefix condition value" 70 \
  "non-string s3:prefix condition value" "${d}"

d="$(new_tree no_allow_patterns)"
for role in gateway query maintain admin; do
  printf '%s\n' \
    '{"Version":"2012-10-17","Statement":[{"Sid":"DenyAll","Effect":"Deny","Action":"s3:*","Resource":"arn:aws:s3:::my-ravel-bucket/*"}]}' \
    >"${d}/deploy/iam/${role}.json"
done
check "refuses when no Allow pattern was read from any template" 70 \
  "no Allow pattern was read" "${d}"

# --- the guard's own declarations -------------------------------------------
# A malformed declaration must stop the scan rather than narrow it: an entry
# with a typo in a role or an axis name would otherwise be skipped and read as
# clean. Each case replaces one string in a copy of the guard.

g="$(mutate_guard dup_keyspace '"keyspace": "sys/gc",' '"keyspace": "sys/auth",')"
check_guard "${g}" "refuses a MANIFEST that declares one key space twice" 70 \
  "declares 'sys/auth' twice" "${BASE}"

g="$(mutate_guard root_keyspace '"keyspace": "sys/gc",' '"keyspace": "t/gc",')"
check_guard "${g}" "refuses a MANIFEST entry outside the scanned roots" 70 \
  "is not under a scanned root" "${BASE}"

g="$(mutate_guard empty_out_of_scope '"out_of_scope": (' '"out_of_scope": "  ", "_moved": (')"
check_guard "${g}" "refuses an empty out-of-scope note" 70 \
  "carries an empty out_of_scope note" "${BASE}"

g="$(mutate_guard missing_axis \
  '"delete": ("unused", (), "the map is replaced by a write, never removed"),' \
  '"deleted": ("unused", (), "the map is replaced by a write, never removed"),')"
check_guard "${g}" "refuses a MANIFEST entry with no answer for an axis" 70 \
  "has no answer for the delete axis" "${BASE}"

g="$(mutate_guard unknown_state \
  '("unused", (), "the map is replaced by a write, never removed")' \
  '("maybe", (), "the map is replaced by a write, never removed")')"
check_guard "${g}" "refuses an axis state that is neither used nor unused" 70 \
  "has unknown state 'maybe'" "${BASE}"

g="$(mutate_guard empty_note \
  '"the map is replaced by a write, never removed"' '"   "')"
check_guard "${g}" "refuses an axis answer with no reason or call site" 70 \
  "carries an empty reason" "${BASE}"

g="$(mutate_guard unknown_role \
  '"used", ("gateway", "query", "admin"),' '"used", ("gateway", "query", "root"),')"
check_guard "${g}" "refuses an owner role that is not a template" 70 \
  "names unknown role 'root'" "${BASE}"

g="$(mutate_guard repeated_role \
  '"used", ("gateway", "query", "admin"),' '"used", ("gateway", "query", "gateway"),')"
check_guard "${g}" "refuses an owner role named twice" 70 \
  "names a role twice" "${BASE}"

g="$(mutate_guard used_without_owner '"used", ("gateway",),' '"used", (),')"
check_guard "${g}" "refuses a used axis with no owner role to check" 70 \
  "is used but names no owner role" "${BASE}"

g="$(mutate_guard unused_with_owner \
  '"unused", (), "the map is replaced by a write, never removed"' \
  '"unused", ("admin",), "the map is replaced by a write, never removed"')"
check_guard "${g}" "refuses an unused axis that names an owner role" 70 \
  "is unused but names owner role" "${BASE}"

g="$(mutate_guard all_axes_unused '"used", ("gateway",),' '"unused", (),')"
check_guard "${g}" "refuses a MANIFEST entry that checks nothing" 70 \
  "declares every axis unused" "${BASE}"

g="$(mutate_guard gap_unknown_keyspace \
  '("sys/t/", "put", "gateway"): (' '("sys/tx/", "put", "gateway"): (')"
check_guard "${g}" "refuses a KNOWN_GAPS entry on an undeclared key space" 70 \
  "which MANIFEST does not declare" "${BASE}"

g="$(mutate_guard gap_unknown_axis \
  '("sys/t/", "put", "gateway"): (' '("sys/t/", "write", "gateway"): (')"
check_guard "${g}" "refuses a KNOWN_GAPS entry naming an unknown axis" 70 \
  "names unknown axis 'write'" "${BASE}"

g="$(mutate_guard gap_unknown_role \
  '("sys/t/", "put", "gateway"): (' '("sys/t/", "put", "ingest"): (')"
check_guard "${g}" "refuses a KNOWN_GAPS entry naming an unknown role" 70 \
  "names unknown role 'ingest'" "${BASE}"

g="$(mutate_guard gap_empty_reason \
  '("sys/t/", "put", "gateway"): (' \
  '("sys/t/", "put", "gateway"): "  ", ("sys/t/", "list", "gateway"): (')"
check_guard "${g}" "refuses a KNOWN_GAPS entry with no reason" 70 \
  "carries an empty reason" "${BASE}"

g="$(mutate_guard allowlist_empty_reason \
  'UNCLASSIFIED_ALLOWLIST: dict[tuple[str, str], str] = {}' \
  'UNCLASSIFIED_ALLOWLIST: dict[tuple[str, str], str] = {("a.rs", "sink"): "  "}')"
check_guard "${g}" "refuses an allowlisted call site with no reason" 70 \
  "carries an empty reason" "${BASE}"

# --- bad usage -------------------------------------------------------------

check "rejects more than one argument" 64 "takes at most one argument" a b
check "rejects an empty root argument" 64 "empty root argument" ""

# ---------------------------------------------------------------------------

printf '\n%s passed, %s failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]] || exit 1
