#!/usr/bin/env bash
# Cases for check-iam-keyspace-axes.sh, in the pattern of
# check-workflow-permissions.test.sh: add a case here before changing a rule.
#
# Each case is a throwaway tree under $TMPDIR carrying the four REAL shipped
# templates plus generated sources that name every declared key space, so a
# case mutates exactly one thing and the rest of the scan stays true. The
# guard takes the tree as its argument, so nothing here reads the real
# workspace sources.
#
# Every rule below was mutation-proven: the rule was broken in the guard, the
# case listed beside it was watched failing, and the rule was restored.
#
# Run: bash scripts/guards/check-iam-keyspace-axes.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "${HERE}/../.." && pwd)"
GUARD="${HERE}/check-iam-keyspace-axes.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-iam-keyspace-axes-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# The base tree, built once and copied per case. Its sources name every key
# space the guard's MANIFEST declares, in both discovered spellings (a
# `const NAME: &str` and a `format!` template), so the discovery floors are
# met the same way the real workspace meets them.
BASE="${TMP}/base"
mkdir -p "${BASE}/deploy/iam" "${BASE}/crates/fixture/src"
cp "${REPO}/deploy/iam/gateway.json" "${BASE}/deploy/iam/gateway.json"
cp "${REPO}/deploy/iam/query.json" "${BASE}/deploy/iam/query.json"
cp "${REPO}/deploy/iam/maintain.json" "${BASE}/deploy/iam/maintain.json"
cp "${REPO}/deploy/iam/admin.json" "${BASE}/deploy/iam/admin.json"

cat >"${BASE}/crates/fixture/src/keyspaces.rs" <<'RS'
// Every key space check-iam-keyspace-axes.sh declares, in the two spellings
// its discovery reads. Not a copy of any real module: only the key-space
// literals matter here.
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

# check <name> <want-exit> <want-substring-or-empty> <arg...>
check() {
  local name="$1" want_rc="$2" want_sub="$3"
  shift 3
  local out rc=0
  out="$(bash "${GUARD}" "$@" 2>&1)" || rc=$?
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

# --- clean -----------------------------------------------------------------

d="$(new_tree clean)"
check "the shipped templates pass against every declared key space" 0 \
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

# A literal INSIDE a declared key space is not a new key space: the
# declaration covers everything beneath it, which is what lets `quarantine/`
# be declared once for the whole root the sweep composes keys under.
d="$(new_tree nested_literal)"
printf 'fn k(p: &str) -> String { format!("sys/maintain/workers/{p}") }\n' \
  >"${d}/crates/fixture/src/nested.rs"
printf 'fn q(o: &str) -> String { format!("quarantine/{o}/held") }\n' \
  >"${d}/crates/fixture/src/held.rs"
# A literal STRICTLY below a declared one, so this case also fails if rule 1
# matches declarations by equality instead of by prefix.
printf 'const LEASE_PREFIX: &str = "sys/maintain/workers/lease/";\n' \
  >"${d}/crates/fixture/src/lease.rs"
check "a literal inside a declared key space is not a finding" 0 \
  "check-iam-keyspace-axes.sh: clean" "${d}"

# --- stale-keyspace --------------------------------------------------------
# Killed by: deleting the rule-4 loop.

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
# Killed by: dropping the stale-gap branch.

d="$(new_tree closed_gap)"
add_statement "${d}/deploy/iam/query.json" \
  '{"Sid":"QueryDelete","Effect":"Allow","Action":["s3:DeleteObject","s3:DeleteObjectVersion"],"Resource":["arn:aws:s3:::my-ravel-bucket/sys/query/workers/*"]}'
check "flags a KNOWN_GAPS entry whose grant now exists" 1 \
  "stale-gap: KNOWN_GAPS still records query as lacking the delete axis" "${d}"

# --- anti-vacuity floors ---------------------------------------------------
# Killed by: lowering each of MIN_SOURCES, MIN_LITERALS and MIN_KEYSPACES_LIVE
# to 0, which turns the matching case green while the clean case stays green --
# the exact silent no-op these refuse.

d="${TMP}/few_sources"
mkdir -p "${d}/deploy/iam" "${d}/src"
cp "${REPO}"/deploy/iam/*.json "${d}/deploy/iam/"
cp "${BASE}/crates/fixture/src/keyspaces.rs" "${d}/src/keyspaces.rs"
check "refuses a tree whose source count is below the floor" 70 \
  "below the floor of 200" "${d}"

d="$(new_tree few_literals)"
printf 'const WORKERS_PREFIX: &str = "sys/maintain/workers/";\n' \
  >"${d}/crates/fixture/src/keyspaces.rs"
check "refuses a tree where discovery finds almost no key spaces" 70 \
  "below the floor of 10" "${d}"

# The literal floor alone does not cover this: a tree can name plenty of key
# spaces while most of them no longer match a declaration, which is what a
# sweeping rename looks like. Twelve literals, seven of them declared.
d="$(new_tree few_live)"
sed -i.bak \
  -e 's|"sys/maintain/claims/compaction/"|"sys/v2claims/compaction/"|' \
  -e 's|"sys/maintain/memo/"|"sys/v2memo/"|' \
  -e 's|"sys/qualification"|"sys/v2qualification"|' \
  -e 's|"sys/query/workers/"|"sys/v2queryworkers/"|' \
  -e 's|"sys/tenancy"|"sys/v2tenancy"|' \
  "${d}/crates/fixture/src/keyspaces.rs"
rm -f "${d}/crates/fixture/src/keyspaces.rs.bak"
check "refuses a tree where most declared key spaces went missing" 70 \
  "below the floor of 8" "${d}"

# --- the scan could not run ------------------------------------------------

d="$(new_tree no_template)"
rm "${d}/deploy/iam/query.json"
check "refuses when a role template is missing" 70 "is not readable" "${d}"

d="$(new_tree bad_json)"
printf '{ "Version": "2012-10-17", "Statement": [\n' >"${d}/deploy/iam/admin.json"
check "refuses when a role template does not parse" 70 "does not parse as JSON" "${d}"

check "refuses a root that is not a directory" 70 "is not a directory" \
  "${TMP}/no-such-tree"

# --- bad usage -------------------------------------------------------------

check "rejects more than one argument" 64 "takes at most one argument" a b
check "rejects an empty root argument" 64 "empty root argument" ""

# ---------------------------------------------------------------------------

printf '\n%s passed, %s failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]] || exit 1
