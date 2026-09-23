#!/usr/bin/env bash
# Cases for check-doc-figures.sh, in the pattern of
# check-workflow-permissions.test.sh: add a case here before changing a rule.
#
# Each case builds a throwaway tree under $TMPDIR carrying only the four files
# the guard reads, so nothing here depends on the real docs staying still. The
# fixtures use the shipped constants (900 / 2 / 45000000 / 10000) because the
# guard derives every figure from them, and a case that hardcoded a figure
# would stop testing the derivation.
#
# Run: bash scripts/guards/check-doc-figures.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-doc-figures.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-doc-figures-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_tree <name>: a scratch tree whose docs are all clean. Prints its path.
new_tree() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/crates/ravel-catalog/src" \
    "${dir}/services/ravel-server/src" "${dir}/docs/guides" "${dir}/docs"
  cp "${GUARD}" "${dir}/scripts/guards/check-doc-figures.sh"

  cat >"${dir}/crates/ravel-catalog/src/config.rs" <<'RS'
pub const RECORD_CACHE_ENTRY_BYTES: u64 = 900;
pub const RECORD_CACHES_PER_TENANT: u64 = 2;
pub const MAX_RECORD_CACHE_BYTES_PER_TENANT: u64 = 45_000_000;
pub const DEFAULT_CACHE_CAPACITY_PER_TENANT: usize = 10_000;
RS

  # caching.md: 45 MB x2, 22.5 MB x1, 9 MB x1, 18 MB x3, 4.5 GB x1,
  # 3.7 GB x1, 10,000 x3 (plus one excluded), 25,000 x1.
  cat >"${dir}/docs/guides/caching.md" <<'MD'
Capacity is floored at 10,000 entries and capped at 25,000.
A byte budget of `capacity x 900 bytes`, 22.5 MB per tenant at the cap.
So the worst case is 45 MB per actively-queried tenant.
Budget it as 45 MB times the number of tenants queried concurrently: 100 of
them is 4.5 GB worst case.
(`--shards 64` derives 2,073,600 entries and 3.7 GB per tenant uncapped).
Held at their 10,000-entry floor rather than the derived capacity, about
18 MB per actively-queried tenant.
They cost about 18 MB per actively-queried tenant under it.
At the 10,000-entry floor rather than the derived value, the flag costs about
18 MB per actively-queried tenant: a 9 MB byte budget for each of the two.
Historically the old cache held a flat 10,000 records whatever they cost.
MD

  # operations.md: 45 MB x1, 22.5 MB x1.
  cat >"${dir}/docs/guides/operations.md" <<'MD'
These cost up to 45 MB per actively-queried tenant on top of the carved
read-cache shares. Both halves are enforced in bytes, 22.5 MB each.
MD

  # catalog-and-mvcc.md: 10,000 x1 (plus one excluded collision), 25,000 x2.
  cat >"${dir}/docs/catalog-and-mvcc.md" <<'MD'
It returns the 25,000-entry cap, so the budget is 22.5 MB per
actively-queried tenant; at the 10,000-entry floor it is 9 MB.
(22.5 MB each at the 25,000-entry cap, 45 MB together.)
A process-wide capacity bound (`head_cache_capacity`, default 10,000 (tenant,
signal) entries) is unrelated to the per-tenant record caches.
MD

  cat >"${dir}/services/ravel-server/src/config.rs" <<'RS'
    /// Turns both caches off. The catalog's two per-tenant record caches stay
    /// on, held at their 10,000 entries and a 9 MB byte budget in each of the
    /// two caches, about 18 MB per actively-queried tenant.
    #[arg(long)]
    pub disable_cache: bool,
RS
  printf '%s\n' "${dir}"
}

# check <name> <tree> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-doc-figures.sh 2>&1)" || rc=$?
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

t="$(new_tree clean)"
check "a tree whose prose matches the constants is clean" "${t}" 0 "clean"

# The defect #1904 shipped: prose drifts while the constant stays.
t="$(new_tree drifted)"
sed -i.bak 's/cost about 18 MB per actively-queried tenant under it/cost about 30 MB per actively-queried tenant under it/' "${t}/docs/guides/caching.md"
check "a drifted restatement is a finding" "${t}" 1 "expected 3"

# The constant moves and every doc is left stale. This is the direction that
# matters: the guard derives, so it fails on the docs rather than agreeing.
t="$(new_tree constant-moved)"
sed -i.bak 's/RECORD_CACHE_ENTRY_BYTES: u64 = 900;/RECORD_CACHE_ENTRY_BYTES: u64 = 950;/' "${t}/crates/ravel-catalog/src/config.rs"
check "moving a constant fails against stale prose" "${t}" 1 "9.5 MB"

# A new copy nobody added an expectation for. The count catches what a
# `contains` check cannot.
t="$(new_tree unaccounted)"
sed -i.bak 's/Both halves are enforced in bytes, 22.5 MB each./Both halves are enforced in bytes, 22.5 MB each, 45 MB total./' "${t}/docs/guides/operations.md"
check "an unaccounted extra copy is a finding" "${t}" 1 "docs/guides/operations.md"

# The clap help is a fourth operator-facing copy and reaches operators with
# the least indirection of all of them.
t="$(new_tree help-drift)"
sed -i.bak 's/about 18 MB per actively-queried tenant./about 20 MB per actively-queried tenant./' "${t}/services/ravel-server/src/config.rs"
check "a drifted --disable-cache help is a finding" "${t}" 1 "disable-cache long help"

# Wrapping is a formatting choice, not a claim: the same prose rewrapped
# mid-phrase must stay clean.
t="$(new_tree rewrapped)"
printf '%s\n' \
  'Capacity is floored at' '10,000 entries and capped at' '25,000.' \
  'A byte budget of `capacity x 900 bytes`, 22.5' 'MB per tenant at the cap.' \
  'So the worst case is 45 MB per actively-queried tenant.' \
  'Budget it as 45 MB times the number of tenants queried concurrently: 100 of' \
  'them is 4.5 GB worst case.' \
  '(`--shards 64` derives 2,073,600 entries and 3.7 GB per tenant uncapped).' \
  'Held at their 10,000-entry floor rather than the derived capacity, about' \
  '18 MB per actively-queried tenant.' \
  'They cost about 18' 'MB per actively-queried tenant under it.' \
  'At the 10,000-entry floor rather than the derived value, the flag costs about' \
  '18 MB per actively-queried tenant: a 9 MB byte budget for each of the two.' \
  'Historically the old cache held a flat 10,000 records whatever they cost.' \
  >"${t}/docs/guides/caching.md"
check "rewrapped prose stays clean" "${t}" 0 "clean"

# An exclusion that stops matching would silently widen a count, so it is
# asserted rather than applied best-effort.
t="$(new_tree exclusion-gone)"
sed -i.bak 's/Historically the old cache held a flat 10,000 records whatever they cost./Historically the old cache held a fixed number of records./' "${t}/docs/guides/caching.md"
check "a stale exclusion refuses rather than passing" "${t}" 70 "excludes"

# A moved or renamed constant must refuse, not report clean over nothing.
t="$(new_tree constant-renamed)"
sed -i.bak 's/pub const RECORD_CACHE_ENTRY_BYTES/pub const RECORD_CACHE_ENTRY_SIZE/' "${t}/crates/ravel-catalog/src/config.rs"
check "a renamed constant refuses" "${t}" 70 "not found"

# The --disable-cache arm going away must refuse too: a scan that finds no
# help text would otherwise report clean having checked nothing.
t="$(new_tree help-gone)"
sed -i.bak 's/pub disable_cache: bool,/pub disable_cache_renamed: bool,/' "${t}/services/ravel-server/src/config.rs"
check "a missing --disable-cache arm refuses" "${t}" 70 "doc comment was not found"

# docs/catalog-and-mvcc.md restates the same byte figures as the guides and
# was scanned only for bare counts until #1966: a drift there went unpinned.
t="$(new_tree catalog-mvcc-drift)"
sed -i.bak 's/10,000-entry floor it is 9 MB./10,000-entry floor it is 10 MB./' "${t}/docs/catalog-and-mvcc.md"
check "a drifted byte figure in catalog-and-mvcc.md is a finding" "${t}" 1 "docs/catalog-and-mvcc.md"

# The explicit 0: that doc states the both-caches figure nowhere today, and
# an unannounced new copy of it must fail rather than go unpinned.
t="$(new_tree catalog-mvcc-new-copy)"
sed -i.bak 's/10,000-entry floor it is 9 MB./10,000-entry floor it is 9 MB, or 18 MB across both./' "${t}/docs/catalog-and-mvcc.md"
check "a new unpinned copy in catalog-and-mvcc.md is a finding" "${t}" 1 "18 MB"

# A doc that moved must refuse: a scan over three of four files that reports
# clean is the failure this guard exists to prevent one level up.
t="$(new_tree doc-gone)"
rm "${t}/docs/catalog-and-mvcc.md"
check "a missing guide refuses" "${t}" 70 "not readable"

# Two derivations that render identically would overwrite each other and drop
# one doc's expectations silently. Halving the budget makes the per-cache
# share equal the floor's share, both "9 MB".
t="$(new_tree colliding-markers)"
sed -i.bak 's/MAX_RECORD_CACHE_BYTES_PER_TENANT: u64 = 45_000_000;/MAX_RECORD_CACHE_BYTES_PER_TENANT: u64 = 18_000_000;/' "${t}/crates/ravel-catalog/src/config.rs"
check "two derived figures rendering alike refuse" "${t}" 70 "both render as"

# The help block is counted too: a duplicated figure there is as unpinned as
# a duplicated one in a guide, and `contains` reports it clean.
t="$(new_tree help-duplicate)"
sed -i.bak 's|/// two caches, about 18 MB per actively-queried tenant.|/// two caches, about 18 MB per actively-queried tenant, so about 18 MB per actively-queried tenant in all.|' "${t}/services/ravel-server/src/config.rs"
check "a duplicated figure in the help is a finding" "${t}" 1 "2 time(s)"

# A marker at offset 0 of the normalized text. `"" in ".,"` is True, so the
# string spelling of the preceding-character test never counted one, and a
# figure that opened a doc went unscanned however wrong it was.
t="$(new_tree marker-at-offset-zero)"
printf '%s\n' '18 MB across both caches, which this doc states nowhere else.' \
  >"${t}/docs/catalog-and-mvcc.md.head"
cat "${t}/docs/catalog-and-mvcc.md.head" "${t}/docs/catalog-and-mvcc.md" \
  >"${t}/docs/catalog-and-mvcc.md.new"
mv "${t}/docs/catalog-and-mvcc.md.new" "${t}/docs/catalog-and-mvcc.md"
rm "${t}/docs/catalog-and-mvcc.md.head"
check "a marker at offset 0 is counted" "${t}" 1 "expected 0"

# A longer number that merely opens with a marker is not that marker. Both
# separators continue a number only when a digit follows them, which is what
# separates "10,000,000" from "capped at 25,000. Neither cache".
t="$(new_tree longer-numbers)"
printf '%s\n' 'A retention limit of 10,000,000 samples and 25,000.5 average rows.' \
  >>"${t}/docs/guides/caching.md"
check "a longer number containing a marker is not counted" "${t}" 0 "clean"

# The trailing-digit half of that rule, which no prose in the scanned docs
# can produce today. Synthetic on purpose: the rule is cheap and the
# convention is a case before a rule, not a case only where prose reaches.
t="$(new_tree trailing-digit)"
sed -i.bak 's/Capacity is floored at 10,000 entries/Capacity is floored at 10,0001 entries/' "${t}/docs/guides/caching.md"
check "a marker continued by a trailing digit is not counted" "${t}" 1 "expected 3"

t="$(new_tree bad-usage)"
out="$(cd "${t}" && bash scripts/guards/check-doc-figures.sh extra-arg 2>&1)"; rc=$?
if [[ "${rc}" == 64 ]]; then
  printf 'ok    %s\n' "an argument is bad usage"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s, wanted 64\n' "an argument is bad usage" "${rc}"
  fails=$((fails + 1))
fi

printf '\ncheck-doc-figures.test.sh: %d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
