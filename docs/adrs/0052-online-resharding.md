# ADR-0052: Online resharding via generation-versioned shard_count

Status: Accepted (2026-08-04)

Epic EK (issue #461), program #450. Depends on epic EC task EC5
(ADR-0050 section 5): the durable per-(tenant, signal) provisioning
record. EC5 is specified but not yet implemented at the time of writing;
this ADR extends that record's design rather than inventing a parallel
mechanism, and the two must land in a coordinated order (section
"Sequencing with EC5").

## Context

`shard_count` is immutable per (tenant, signal). ADR-0010 section 9
froze it; crates/ravel-catalog/src/config.rs:59 documents changing it as
a forbidden data-loss operation ("segments already routed to a shard
index become unreachable if the shard count changes"); ADR-0050 section
5 makes the current value durable and startup-checked but explicitly
defers changeability. Four independent reviewers flagged the same
exposure (findings S3-06/S5-14; the same family as ADR-0050's
S1-06/S2-10): every tenant is pinned to a day-one guess (default 4,
crates/ravel-ingest/src/config.rs:115, `--shards` in
services/ravel-server/src/main.rs:188), and a tenant that outgrows it
has no path forward except a bespoke full-tenant rewrite invented under
duress.

What the code actually does with `shard_count` today, from reading every
call site in the workspace:

**Write-side routing.** Three hash-mod-count functions, all persistent
contracts: `shard_for` (series id leading 8 bytes LE mod count,
crates/ravel-types/src/lib.rs:225), `shard_for_log`
(crates/ravel-types/src/lib.rs:241), `shard_for_span` (trace id,
crates/ravel-ingest/src/span_router.rs:58). The ingest routers group
records by these functions and send each group to one of `shard_count`
shard actors spawned at router construction
(crates/ravel-ingest/src/log_router.rs:69, docs/ingest.md). Each actor
pins an `ingest_hour_bucket` at flush open (ADR-0010 section 1) and
writes data objects and commit records under keys that embed the shard
index.

**Persistent artifacts that carry a shard index.** The object key layout
(docs/catalog-and-mvcc.md): `l0/<shard>/`, `c/<shard>/<ingest_hour>/`,
`l1/<shard>/<ingest_hour>/`, retention tombstones, and the advisory
maint cursor, with `shard` a zero-padded 4-digit decimal. Commit records
carry a `shard` field validated against the key on load. Catalog
snapshot parts record a `shard` per entry and a `shard_count` in both
`SnapshotPartHeader` and `SnapshotHead` (proto/ravel/catalog.proto:12,
:41). The commit token v2 (`crates/ravel-types/src/lib.rs:255`,
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`) carries the
shard index and is a client-visible frozen contract.

**Read-side fan-out.** The catalog's Phase 1 resolve loops `for shard in
0..self.config.shard_count` crossed with the query's hour buckets, one
LIST per (shard, hour) pair (crates/ravel-catalog/src/catalog.rs:508).
Phase 2 validates `SnapshotHead.shard_count` against the catalog's
configured value and treats a mismatch as a loud `FieldMismatch` error
excluded from listing fallback (crates/ravel-catalog/src/catalog.rs,
snapshot_resolve.rs:569). Incremental folds enumerate `(shard, hour)`
pairs from `shard_count` (crates/ravel-catalog/src/fold.rs:357).

**The load-bearing finding: no read path routes by hash.** `shard_for`,
`shard_for_log`, and `shard_for_span` have zero callers in
ravel-catalog, ravel-query, ravel-promql, ravel-sql, or ravel-maintain
(the only non-ingest reference is a cost-estimate multiplier in
crates/ravel-sql/src/executor.rs:381 and test helpers). Queries never
compute "which shard does series X live in"; they scan every shard and
merge by series identity. Compaction operates per `(tenant, signal,
shard, hour)` bucket discovered by listing
(crates/ravel-maintain/src/scan.rs, `list_shard_hours`), never by
re-deriving a series's shard. Shard placement is therefore a write-time
load-balancing decision, not a read-time addressing scheme. This is
what makes resharding cheap: changing the count going forward cannot
strand data, as long as the read fan-out knows the set of shard indices
that were ever written for the hours it scans.

**Where a count change breaks things today.** (a) The Phase 1 loop and
the maintain loops (`for shard in 0..shard_count`,
services/ravel-server/src/maintain.rs:435,
services/ravel-cli/src/maintain.rs:283) derive the scan set from a
single static number, so a decrease silently omits shards (the S1-E6
hazard ADR-0050 closes by refusing mismatches) and an increase makes
old snapshot HEADs fail the `shard_count` equality check. (b) The
ingest routers fix their actor set at construction. (c) EC5's
provisioning record, as specified, stores one immutable scalar.
Nothing else breaks: commit-token resolution reconstructs the exact
commit key from the token's own fields
(crates/ravel-commit/src/keys.rs:242) and never consults `shard_count`;
idempotency markers are per-signal, not per-shard
(docs/catalog-and-mvcc.md:15); there is no cross-shard ordering
guarantee to preserve (docs/consistency-model.md:35).

The epic's acceptance test: a tenant's `shard_count` is changed with
ingest running, and queries spanning the change return complete results
across both generations.

## Decision

Adopt **generation-versioned shard_count**. Existing data is never
moved, rewritten, or re-keyed. A reshard appends a new *shard
generation* — `(generation, shard_count, activation_hour)` — to the
tenant's provisioning record. Writers route new data with the count of
the generation active for the wall-clock hour they are writing; readers
derive the per-hour scan set from the generation history. Old
generations remain readable under their original shard indices forever
(until retention ages their hours out, which needs no changes: hours
age out per (shard, hour) bucket regardless of generation).

### 1. The generation record: an extension of EC5's provisioning record

EC5 (ADR-0050 section 5) introduces
`t/<tenant_hash>/<sig>/prov`, a protobuf record `{tenant_hash, signal,
shard_count, format floor, created_unix_ns}` written with
`CreateIfAbsent`. This ADR extends that message additively:

```proto
message ShardGeneration {
  uint32 generation      = 1;  // dense, 0-based
  uint32 shard_count     = 2;  // 1..=10000 (4-digit key field)
  uint32 activation_hour = 3;  // unix hours; ingest hours >= this
                               // route with this generation's count
  int64  appended_unix_ns = 4; // audit only
}

// added to the EC5 provisioning message:
repeated ShardGeneration generations = <next free field>;
```

Compatibility rules: the existing scalar `shard_count` field stays and
MUST equal `generations[0].shard_count`; an empty `generations` list is
read as the single implicit generation 0 with `activation_hour = 0`.
Generations are dense, `activation_hour` strictly increasing, counts of
adjacent generations differing. A record violating these is a decode
error (typed, fail closed), same discipline as every other corrupt-
input path.

The record's mutability model changes from immutable to **append-only
under `CasVersion`**: the only legal mutation appends one generation
whose `activation_hour` is in the future (section 3); every existing
byte of history is immutable. EC5's `CreateIfAbsent` first-write,
adoption path, and startup/first-touch validation are unchanged;
validation now compares the full generation history, not one scalar.
`ravel-cli provision reshard --tenant <t> --signal <s> --shard-count
<n> [--lead-hours <L>]` performs the CAS append and writes a
control-plane audit record (ADR-0040/ADR-0042), since a reshard changes
where future data lands and must be attributable.

### 2. Routing rule (write side)

For a record routed at wall-clock time `t`, the router uses
`count(g)` of the latest generation `g` with `activation_hour(g) <=
hour(t)`. The hash functions themselves (`shard_for`, `shard_for_log`,
`shard_for_span`) are unchanged — same bytes, same mod — only the
divisor becomes generation-dependent. No new hash scheme: since no read
path routes by hash, minimizing key movement (consistent hashing's
selling point) buys nothing here.

The ingest routers must support a live switch: on observing an
activation, the router constructs the new generation's shard-actor set,
routes subsequent records to it, and lets the old actors drain and
flush normally (their commit records and keys remain valid under the
old indices; ADR-0047's per-shard exemplar admission caps re-arm on the
new actor set). Routers refresh their view of the provisioning record
on a bounded interval `C`; a router whose cached record is older than
`C` MUST fail the flush closed (typed error, counter) rather than route
on a stale view — the same fail-closed posture as ADR-0050, and the
property the safety argument in section 3 needs.

### 3. Activation protocol and the safety window

Activation is denominated in ingest-hour buckets, the unit the key
layout and catalog fan-out already use. The CLI computes
`activation_hour = now_hour + L` with lead `L` (hours) satisfying
`L >= ceil(C) + 1`, so every live writer either observes the new
generation before it activates or has already fail-stopped on record
staleness. The append is rejected if `L` would place activation in the
past.

Data written into an hour bucket `h` can have been routed under a
generation other than the one nominally active at `h`, within a bounded
slack: a flush open pins `h` at wall-clock time `t`, but its records
were routed up to `max_flush_delay` earlier and the flush itself lives
at most `max_flush_lifetime` (both bounded and enforced,
crates/ravel-ingest/src/config.rs, issue #182), plus inter-writer clock
skew. Define slack `S` = ceil of (`max_flush_delay` +
`max_flush_lifetime` + max tolerated clock skew) in hours; with today's
defaults (500 ms + 3600 s) `S = 2` is safe.

- **Increase (the common case): no slack needed.** A straggler routed
  under the old, smaller count lands in a shard index that is a subset
  of the new range (`0..old ⊂ 0..new`), so the new generation's scan
  set already covers it.
- **Decrease: the retiring generation's count must remain in the scan
  set for `S` hours past the successor's activation** (section 4), so
  a straggler routed under the old, larger count into an
  early-new-generation hour is still scanned.

### 4. Scan rule (read side)

The catalog's per-hour fan-out becomes a function of the generation
history instead of a constant. Normatively, for hour bucket `h`:

```
scan_count(h) = max over generations g of count(g)
                where activation_hour(g) <= h
                  and h < activation_hour(g+1) + S
                (activation_hour of a nonexistent successor = infinity)
```

This replaces `0..self.config.shard_count` in Phase 1 resolve
(catalog.rs:508), the fold's `incremental_buckets` (fold.rs:357), and
the maintain shard loops (services/ravel-server/src/maintain.rs:435,
services/ravel-cli/src/maintain.rs:283, :439). Listing a shard prefix
that was never written is one empty LIST, and maintain's
`list_shard_hours` already tolerates empty shards, so the conservative
window costs little. `CatalogConfig.shard_count` and the server/CLI
`--shards` flag stop being the source of truth per EC5's own direction;
the generation history from the provisioning record is.

Query correctness across the boundary needs nothing beyond the scan
rule: the query engine already merges a series's samples from any set
of segments by series identity. A series that hashed to shard 1 under
count 4 and shard 5 under count 8 simply has segments under both
indices in different (or, within the slack window, the same) hours;
that is structurally identical to a series spanning multiple segments
today. Compaction conservation (ADR-0048) is per (shard, hour) bucket
and unaffected.

### 5. Snapshot format and validation

`SnapshotHead.shard_count` / `SnapshotPartHeader.shard_count` (field 4
in both, frozen numbers) are reinterpreted, additively: the value is
the *fan-out ceiling at fold time*, i.e. `max(scan_count(h))` over
hours up to the head's watermark. One additive field is introduced:

```proto
// SnapshotHead, additive:
uint32 shard_generation_count = 10; // generations known at fold time;
                                    // absent/0 = pre-reshard head
```

Reader validation (replacing the equality check that would otherwise
reject every pre-reshard head after an increase): a head is valid if
its `shard_count` equals the ceiling the reader computes from the
provisioning record for the head's watermark hour, **or** the head's
`shard_generation_count` is lower than the reader's (an older head from
before a reshard; Phase 1 listing covers the newer hours it lacks). A
head claiming *more* generations than the reader's record forces one
record re-read; if still behind, fail closed — the reader's record
view is stale, and serving from it could under-scan. `FieldMismatch`
stays loud and stays excluded from listing fallback.

### 6. Commit tokens: no format change

The v2 token is untouched. Its `shard` field is a raw index into the
key layout, and resolution is an exact commit-key GET reconstructed
from the token's own fields (keys.rs:242) with no `shard_count` input;
a token minted under any generation resolves forever. One new MUST:
no code may validate `token.shard` against a current or configured
`shard_count` (none does today; this ADR makes it a stated contract so
a future "sanity check" doesn't break read-your-write across a
decrease). Ack semantics are unchanged: one token per shard flushed,
whatever generation those shards belong to. This satisfies the
format-change principles vacuously — additive by being a no-op, no
dual-reader ambiguity because the token never needed the count.

### 7. Key layout: no byte change, one semantic amendment

No new key shapes, no changed encodings; `shard` stays a 4-digit
decimal, which caps `shard_count` at 10000 (enforced at append). The
sentence in docs/catalog-and-mvcc.md ("`shard_count` is immutable per
(tenant, signal)", line 84) and its echoes in ADR-0010 section 9 and
crates/ravel-catalog/src/config.rs are amended by this ADR: the count
is immutable *per generation*; the generation history is append-only;
the shard-index domain of hour `h` is `0..scan_count(h)`. Implementing
tasks update docs/catalog-and-mvcc.md, docs/consistency-model.md, and
docs/ingest.md in the same commits as the behavior.

## Sequencing with EC5

EK consumes the provisioning record EC5 builds. Coordination is one of:
(a) EC5 lands first with `generations` reserved and EK adds the field
additively (field numbers are frozen, additions are legal), or (b) if
EK's implementation is planned before EC5 ships, the `ShardGeneration`
message ships inside EC5's initial proto with only generation 0 ever
written until EK activates the append path. Either way the append-only
CAS mutation model in section 1 supersedes EC5's "immutable" wording
for this record, and EC5's adoption path writes generation 0. This ADR
does not change EC5's validation choke points; it changes what they
compare.

## Rejected alternatives

**Full-tenant rewrite (drain, re-ingest, cut over) — the naive
baseline.** Rejected. It doubles storage and PUT spend for the tenant's
entire history, takes unbounded time during which the tenant runs
degraded or frozen, and regenerates writer identities: every
outstanding commit token dangles (read-your-write breaks, violating
docs/consistency-model.md), and every rewritten object breaks
compliance custody continuity (ADR-0042 legal holds and verify-custody
bind to the original objects). It is exactly the "bespoke rewrite
invented under duress" the epic exists to eliminate.

**True online live migration (copy old objects to new shard indices,
tombstone the originals, while ingest runs).** Rejected. Data objects,
commit records, and manifests are immutable, so "migration" is
copy-plus-delete: the commit record embedding the old shard in both its
key and its validated fields must be superseded by a new one, breaking
the reconstruct-and-verify discipline (docs/catalog-and-mvcc.md:216)
for every migrated object and dangling every token that names the old
record outside the compaction-supersession paths built for it. It
requires a long-running checkpointed migration state machine on object
storage driven by disposable compute, with a dual-read window in which
every query must reconcile both layouts — the failure surface of a
distributed rebalancer, added to a system whose read path (the
load-bearing finding above) gets no benefit from data being "in the
right shard": queries scan all shards either way. All cost, no read-side
payoff.

**Consistent/rendezvous hashing to minimize key movement.** Rejected as
a category error: it optimizes how much data moves on a count change,
but under generation versioning nothing moves at all, and no read path
routes by hash, so placement stability has no consumer. It would also
change a frozen routing contract (`shard_for` v1) for zero benefit.

**A per-series shard directory (explicit placement map).** Rejected: a
mutable, unboundedly growing index consulted on the ingest hot path,
with its own MVCC and durability story, replacing a pure function.
Violates the spirit of exactness and immutability for a problem the
generation map solves with O(generations) state.

**Hybrid considered: generations now, optional background
"re-generation compaction" later** (rewrite an old generation's L1
parts into the newest generation's layout through the existing
compaction supersession machinery, which already handles
token-to-superseded-object resolution). Not adopted here — it inherits
scoped versions of the live-migration problems and needs its own ADR —
but the generation model deliberately leaves the door open: nothing in
this design assumes an old generation's hours are never re-homed by a
future, deliberate, compaction-shaped process.

## Consequences

- **The acceptance test becomes concrete.** With ingest running:
  `provision reshard` appends generation 1 (count 8, activation now+L);
  routers observe and switch at activation; a query spanning hours on
  both sides resolves per-hour scan sets from the generation history
  and returns complete results from both layouts. Downstream tasks must
  prove exactly this end to end, plus: a decrease with straggler writes
  inside the slack window; a query via a pre-reshard snapshot HEAD
  (older `shard_generation_count`); read-your-write with a token minted
  under the old generation after a decrease; a stale-record writer
  fail-stopping instead of routing; and a `FaultStore`-injected CAS
  race on the append (one winner, loser re-reads).
- **The provisioning record is no longer immutable**, weakening EC5's
  simplest invariant to "append-only under CAS". All the validation
  choke points stay, but they now compare histories, and a new failure
  mode exists: a reader with a stale record view. Increases are safe
  under staleness (subset scan is impossible — the ceiling only
  grows... a stale *reader* scans fewer shards for post-activation
  hours, which is why section 5 forces the record re-read and fails
  closed); decreases are safe because old indices stay in scan sets.
  The fail-closed staleness bound `C` on writers is load-bearing and
  must be enforced, not advisory.
- **More LISTs near boundaries and after decreases.** Phase 1 scans the
  ceiling count for hours in slack windows, and forever-empty high
  shards after a decrease still cost one empty LIST per hour until
  those hours age out or are folded into snapshots. Bounded and cheap,
  but nonzero.
- **Ingest routers get a second lifecycle path** (generation switch:
  spawn, drain, retire actor sets) with its own failure modes —
  shard-actor death during drain, backpressure during the overlap.
  Per-shard admission caps and metrics (span_metrics.rs,
  log_metrics.rs) reset at the switch.
- **The SQL cost estimate** (crates/ravel-sql/src/executor.rs:381) and
  the resolve accounting estimate (catalog.rs:588) become hour-
  dependent sums instead of a constant multiplier.
- **What stays untouched:** commit-token format and resolution, all key
  byte layouts, RSEG/RLOG/RSPAN segment formats (a segment never
  records its shard count), idempotency markers, retention semantics,
  compaction conservation, and every existing object — no migration,
  no rewrite, no delete.

## Open questions for review

1. Whether decreases should ship at all in the first implementation.
   Increases are the motivating case and are strictly safer (no slack
   reasoning on the read side). Gating decreases behind a follow-up
   halves the initial test matrix; the record format supports both
   either way.
2. The concrete refresh interval `C` and lead floor `L` (this ADR fixes
   the inequality, not the numbers), and whether the router learns of
   activations by polling the record or by a cheaper signal.
3. Whether `SnapshotHead.shard_count`'s reinterpretation (fan-out
   ceiling) should instead be a new additive field with the old field
   frozen at generation-0 count; section 5 chose reinterpretation
   because every existing head already satisfies it (one generation:
   ceiling = the count), so no dual-reader ambiguity exists — but this
   deserves a reviewer's eye as a frozen-contract judgment call.

## Amendment 2026-08-12: bounded degraded-grace routing (NF-2) and clock-skew-covering read slack (NF-3)

Adversarial review (v2) found two of section 3's assumptions load-bearing in a way that turns ordinary operational conditions into an outage (NF-2) or a silent-invisibility correctness gap (NF-3). This amendment revises the normative posture accordingly. Both changes ship together and MUST NOT be reverted independently: NF-2's safety depends on NF-3's widened read slack.

NF-2 -- bounded degraded-grace routing. Section 2's rule that a router whose cached provisioning view has aged past the refresh interval C MUST fail every flush closed is relaxed to a bounded grace window. A router that fails to re-read past C MAY continue routing on its last-known-good view while hour_of(now) < hour_of(refreshed_at) + min_lead_hours(C), where min_lead_hours(C) = ceil(C) + 1; beyond that horizon it MUST fail closed as before. This converts sustained store slowness from a total ingest outage into bounded degraded throughput while a provably-still-current shard_count is in effect, and emits the grace_extended_stale_flushes counter so operators observe degraded mode. It is safe because an activation the router has not yet seen has activation_hour >= hour_of(refreshed_at) + L >= the grace horizon under synced clocks, so grace never routes past an unseen generation change.

New assumption (normative). Under clock skew between the reshard-append process and a router, an unseen generation can activate up to TOLERATED_CLOCK_SKEW_HOURS before the grace horizon, so grace can route the old shard_count into an hour the successor generation nominally owns. This does not split-brain the tenant's data only because NF-3's widened read-side slack S covers that overshoot. Therefore this amendment adds the assumption that the reshard-append process's clock is within TOLERATED_CLOCK_SKEW_HOURS of every router's clock (satisfied by any NTP-disciplined fleet with large margin: real skew is sub-second against a one-hour budget). The base fail-closed design did not depend on the append process's clock for read-side coverage; the grace window introduces this dependency, and it is discharged by NF-3.

NF-3 -- clock-skew-covering read slack. Section 3 always named "max tolerated clock skew" as a term of the read-side slack S, but the shipped value had silently dropped it (S = 2 hours, flush-bound only). S is corrected to S = FLUSH_BOUND_SLACK_HOURS + TOLERATED_CLOCK_SKEW_HOURS = 2 + 1 = 3 hours, recorded here normatively. This widens the read-side scan set so that a straggler routed under either the retiring or the activating generation near a boundary -- worst on a shard-count decrease, where the retiring generation's wider range is exactly what covers the straggler -- is always inside the scan set. The invariant restored: no acknowledged write is ever routed to a shard index the read-side scan set for its hour does not cover.

Coupling. NF-2's grace overshoot past an activation is bounded by the append-vs-router skew; NF-3's S = 3 budgets flush timing (~1 hour real) plus that skew (1 hour) with a full hour of margin. Reverting NF-3 while keeping NF-2 reopens the split-brain; reverting NF-2 while keeping NF-3 only over-scans harmlessly. Section 2's fail-closed MUST and section 3's safety argument are to be read as amended by this section.
