# ADR-0815: Clustered compaction and object-level pruning

Status: Proposed.

The number is issue #815's, the ticket that produced this ADR, per the
README's issue-number rule.

## Context

On the ClickBench reference tenant `clickbench-v4` (8,424 objects, 17,731
blocks, 11.116 GB, 8 shards, RLOG v4), the selective statement class q37-q43
already prunes well at block granularity and terribly at object granularity:

| figure | value |
|---|---|
| blocks kept after pruning | 144 of 17,731 (0.8%) |
| bytes read | 2.1 GB of 11.1 GB |
| objects opened | 8,424 of 8,424 (100%) |
| data GETs | ~15,800 (~1.88 per object) |
| wall | 40-46 s |
| competitor wall, same shapes | 0.06-0.09 s |

Block pruning works; object pruning does not exist. If the surviving 0.8% of
blocks were gathered into objects they would occupy roughly 0.8% of them (~67);
touching 100% of objects to keep 0.8% of blocks means those blocks are spread
over every object in the corpus.

**What prunes on this corpus is CounterID, not time.** The attribution matters
because it decides which corpus may measure this ADR, so state it exactly.
q37-q42 filter `EventDate BETWEEN 15887 AND 15917`
(benchmarks/clickbench/hits.corpus.json; the epoch-day mapping is
docs/guides/clickbench.md). That is 2013-07-01 to 2013-07-31, and the `hits`
corpus is entirely July 2013, so the date predicate selects the whole corpus
and prunes nothing at all. The predicate that selects is `CounterID = 62`, at
roughly 0.7-0.8% of rows. The measured 144 of 17,731 surviving blocks is 0.81%,
which is CounterID's row share, not the share any time window would keep. q43
is the one statement in the class carrying a real window (`EventDate BETWEEN
15900 AND 15901`, two days), and it carries the same `CounterID = 62` arm.

The mechanism behind the 144 blocks is entity locality in the load, not
clustering of any kind. `hits.parquet` arrives sorted by `(CounterID,
EventDate, ...)` (#560) and the load preserves those runs, so a block is
near-single-CounterID and the block-level `CounterID` min/max arm excludes
almost all of them. EventTime does not correlate with CounterID, so every
flushed batch spans nearly the whole EventTime range, and every object's
catalog-level `[min_event_ts, max_event_ts]` is therefore nearly the whole
range. The resolve-time event-overlap filter (docs/catalog-and-mvcc.md,
"Snapshot resolution" step 4, and the per-part event bounds on an L1
`SegmentRef`) already excludes an object whose event range misses the query
window without a data GET, but with every object's bounds spanning the whole
range it excludes nothing. Those two facts are consistent only under this
attribution: wide event bounds are exactly why time excludes no object, while
CounterID locality excludes 99.2% of blocks regardless.

**So this corpus is where time-leading clustering backfires, and it must not be
the corpus that justifies or measures it.** A global EventTime sort scatters
each counter's rows evenly across the whole order. `CounterID = 62` holds
roughly 800,000 of the corpus's 99,997,497 rows; spread evenly over the
measured 17,731 blocks that is about **45** matching rows per block, so nearly
every block would hold some, and the `CounterID` min/max
arm would stop excluding anything, and q37-q42 would go from 144 surviving
blocks to nearly all of them. q43 would improve modestly, because its two-day
window is real. Decision 2 states what does justify the time-leading default,
acceptance 1 states which corpus may carry the object-count claim, and
acceptance 12 is the pre-registered A/B that must run before the key is
enabled on any measured tenant.

Two facts bound the design:

- **The per-object exclusion plumbing for EventTime already exists.** The
  commit record carries `min/max event ts`; an L1 part ref carries per-part
  `event bounds`; RLOG v4's SKIP_IDX level-2 summary carries `min_ts_ns/
  max_ts_ns` inside the object. Resolve filters on the catalog-level bounds
  before issuing any data GET. What is missing is not a place to read
  per-object time bounds, it is bounds narrow enough to exclude anything.
- **Clustering is not an output invariant today.** The RLOG L1 merge re-sorts
  its inputs by `(stream_ref, ts)` (crates/ravel-maintain/src/rlog.rs) --
  stream-major, so a multi-stream object's per-part EventTime bounds each span
  nearly the whole range -- and L1 compaction is within-hour by construction
  (ADR-0018 scopes cross-hour L2 out). A bulk-imported corpus lands all
  EventTime ranges in a handful of ingest hours, so no tier ever produces
  objects whose EventTime bounds are narrow.

This ADR makes clustering an explicit compaction output invariant and states
which existing exclusion plumbing it reuses.

## Decision

### 1. Clustering happens in compaction, never in ingest

The ingest path must stay append-ordered, cheap, and bounded in memory. A
flush pins its identity at open (docs/catalog-and-mvcc.md, "Pinned flush
identity"): `seq`, `ingest_hour_bucket`, the serialized bytes, and the content
hash are fixed for the flush lifetime, and a retry must not re-serialize or
accrete newly arrived samples. Clustering by a key means buffering and sorting
across a window of arrivals before the bytes are sealed. Doing that at ingest
would either hold rows back from the ack (raising ack latency by the width of
the clustering window) or grow the in-memory buffer without the
ADR-0069 global bound being able to reason about it, and it would break the
retry-reuses-bytes contract that makes at-least-once idempotent. Ingest stays
as it is: arrival-ordered L0 objects, each spanning whatever EventTime range
its batch happened to carry.

Compaction already rewrites objects after the fact, off the ack path, over a
sealed and therefore complete input set (the seal lemma). It already loads
every input, orders the merged record set, and re-blocks from scratch through
`rewrite_and_publish` under a `MergeMemoryTracker` bound. Clustering is a
change to *which order* that merge sorts in and *where* it cuts parts -- for
the default time-leading key, not a new pass and not new I/O; the override
key's different cost shape is stated in decision 4. It is stateless and
idempotent exactly as compaction is today: content-addressed part keys and a
`CreateIfAbsent` record mean a re-run converges.

Decision: clustering is a compaction output choice. Ingest is untouched.

### 2. Clustering key: time-leading per signal, per-tenant override

The clustering key is a fixed per-signal default, overridable per tenant, never
silently derived:

- **Logs, alerts, audit (RLOG):** lead with EventTime. Decision 4 states
  precisely what changes in `crates/ravel-maintain/src/rlog.rs::merge_catalogs`
  to get there; stream identity moves from an outer partition to a low-order
  tiebreak, it does not merely change position within one comparator.
- **Spans (RSPAN): out of scope.** Not part of a combined "RLOG/RSPAN" family
  for this decision. Decision 4 states why: the RSPAN merge's trace-transition
  part split cannot carry a time-leading order without a different algorithm,
  not a sort-key change, so this ADR does not cluster spans by time. A
  narrow-time-window statement over spans gets no object-level exclusion from
  this ADR.
- **Metrics (RSEG):** unchanged. RSEG is already series-major and a metric
  query is series-scoped, not a narrow-EventTime-window scan over an
  unclustered table; the measurement class this ADR targets does not exist for
  metrics. Metrics are explicitly out of scope.

The per-tenant override lives in the existing durable config record
`t/<tenant_hash>/config` (docs/catalog-and-mvcc.md; ADR-0066 §6), which already
carries the indexed-field set and the declared typed-attribute-column set. A
new optional `clustering_key` field (a declared column name, or the sentinel
for EventTime) is an additive field on `TenantConfigRecord`, resolved
override-over-default exactly as retention and admission limits already are. A
tenant whose queries filter on a different low-cardinality dimension (say a
`Region` column) sets the key to that column and gets object exclusion on it
instead of on time. Which declared types are eligible as an override key,
and the canonical order each clusters under, are defined in decision 3's
eligibility subsection.

**What justifies EventTime as the default, and what does not.** Not q37-q43:
Context shows those statements are selected by `CounterID`, and a global
EventTime sort would regress six of the seven. The default rests on organic
telemetry instead, where two properties hold that the ClickBench corpus does
not have. Queries are dominated by recent-time windows, so a narrow per-object
EventTime bound is the bound most statements can use. Ingest arrival order
already correlates with event time, so a time-leading merge concentrates rather
than scatters: it tightens per-object bounds without spreading any entity's
rows across more objects than the arrival order already did. The scattering
hazard Context describes needs a bulk-loaded, entity-sorted corpus whose
arrival order is uncorrelated with event time, which is exactly what ClickBench
is. For the `clickbench-v4` tenant specifically the selective clustering key is
`CounterID`, so that tenant belongs to the override-key path above rather than
to the time-leading default. ADR-0849 §6 records the same split.

A tenant whose queries filter on a **high-cardinality point** dimension
(`WHERE UserID = c`) is not served by any single clustering key; that is point
5, and the answer there is an index, not this sort. Derived-per-query keys are
rejected: the per-object bound the planner reads must be a stable contract
against a known key, not a guess re-made per statement.

### 3. Object exclusion: narrow catalog-level bounds, read without a GET

Clustering changes nothing unless an object can be excluded WITHOUT a data GET.
Both halves are required and must be stated separately. Every bound in this
section is **per output part, never per compaction record**: the catalog
mints exactly one `SegmentRef` per `CompactionRecord.parts` entry
(docs/catalog-and-mvcc.md "Snapshot resolution"; `crates/ravel-catalog/src/
snapshot.rs::SegmentRef`), and object exclusion filters on that `SegmentRef`.
A record-level bound cannot represent two parts whose clustering-key ranges
are disjoint: a record spanning both would report a min/max wide enough to
cover the gap between them, excluding nothing a query landing in that gap
should have excluded. Every clustering *bound* this decision adds
therefore lives on `CompactionPart` and is copied to its `SegmentRef` exactly
as today's per-part `min_event_ts_ns`/`max_event_ts_ns` already are --
`SegmentRef` carries that pair flat on the struct, not nested under the L1
variant, and `CompactionPart` already carries the same pair per part
(proto/ravel/commit.proto). No bound this ADR adds is ever read or written at
record granularity. The per-part rule is about bounds -- the min/max pairs
exclusion filters on -- not about every field this ADR touches: the record
does gain non-bound descriptor fields that describe the whole record and are
meaningful only at that granularity (`clustering_key`,
`clustering_key_type`, and `clustering_order_version` in the identity
subsection below, and decision 7's
`covered_hour_min`/`covered_hour_max`).

- **The clustering half** (this ADR's new work): compaction emits parts whose
  per-part span of the clustering key is narrow, because the merge is sorted by
  that key and parts are cut on its boundaries (decision 4).
- **The exclusion half** (where the per-part min/max lives, and how the
  planner reads it):
  - **For the EventTime key, the exclusion half already exists and is
    already per-part.** `CompactionPart.min_event_ts_ns`/`max_event_ts_ns`
    (proto/ravel/commit.proto), reconstructed onto each part's `SegmentRef`
    and never trusted from a stored string (ADR-0010 §7), are the per-part
    min/max of the clustering key. Resolve step 4 filters on them before any
    data GET. Today's block stats also live *inside* the object (SKIP_IDX),
    which is exactly why discovering a block has nothing still costs a
    request; the part-record bounds are the copy that lives in the catalog
    and costs no data GET. So for EventTime the two halves reduce to one new
    thing: clustering. No new catalog field.
  - **For a non-EventTime clustering key** (the per-tenant override on a
    declared column), the catalog carries no per-part min/max of that column
    today; it lives only in the object's per-block column stats, so
    discovering a part has nothing costs a GET. That key needs the exclusion
    half built: an additive per-part min/max pair on `CompactionPart` --
    wire shape, canonical order, and eligible key types are defined in the
    next subsection -- populated by the compactor from that part's own
    merged rows and copied onto the part's `SegmentRef` the same way the
    EventTime pair already is. The planner reads it in resolve step 4
    beside the event-bound filter. These are additive proto fields on
    `CompactionPart`, not a segment-format change (decision 6).

Say it plainly: for the default (time) key this ADR ships clustering alone and
reuses existing per-part bounds; for an override key it ships clustering plus
additive per-part bound fields and per-record descriptor fields. Neither path
reads a data object to exclude one, and no bound is ever read or written at
record granularity.

#### Override-key eligibility, canonical order, and wire encoding

Two implementations that disagree about a bound's byte meaning or its
comparison order disagree about which objects a query may skip, which is a
correctness bug, not a formatting choice. So this section is normative, and it
deliberately reuses the one per-type ordered vocabulary the codebase already
freezes rather than inventing a second one: the RLOG SKIP_IDX `NumStat`
bit-pattern encoding and its type-aware order
(`crates/ravel-logseg/src/skip_index.rs::min_bits`/`max_bits`, shared with the
query planner's `NumRangeArm` so "nothing re-implements per-type comparison").

**Eligible key types.** An override key must name a column in the tenant's
declared typed-attribute-column set (ADR-0090); an undeclared attribute has no
authoritative type to order by. The declarable types are exactly the eligible
ones: `str`, `i64`, `bool`, `bytes` today, plus `f64` once ADR-0101's writer
release makes it declarable. ADR-0101's exclusion of date and timestamp types
does not restrict this ADR: those types are not declarable at all, so they
cannot be named as an override key, and a secondary time column declared as
`i64` (the pattern ADR-0101 itself endorses) clusters through the `i64` rule
below with full fidelity. `List`/`Map` attribute values have no declarable
type and are likewise out by construction. Nothing needs to be scoped out.

**Canonical order**, per declared type. This is both the merge sort order for
the clustering key's leading position and the order the resolver's overlap
test runs under; they are one definition so they can never disagree:

- `i64`: signed two's-complement order.
- `f64`: IEEE-754 total order over the bit pattern (`f64::total_cmp`), so
  `-0.0 < +0.0` and NaN payloads order deterministically, per the repo's
  bit-pattern invariant.
- `bool`: `false < true`.
- `str`: lexicographic unsigned byte order over the UTF-8 bytes (Rust `&str`
  `Ord`).
- `bytes`: lexicographic unsigned byte order.

The full merge comparator for an override key is `(key order as above, ts_ns,
then decision 4's existing tiebreak)`. A row whose clustering value is null --
absent, or stored under a variant other than the declared type, which
ADR-0101 decision 2's read rule already defines as NULL -- sorts after every
non-null value. Nulls-last is not cosmetic: it concentrates null rows in the
trailing part(s), so the absent-bounds rule below costs bounds on those parts
only, not on every part a sparse column touches.

**Wire encoding** on `CompactionPart`, selected by the part's
`clustering_key_type` descriptor:

- `i64`, `f64`, `bool`: `clustering_min_bits`/`clustering_max_bits`, optional
  `uint64`, holding exactly the `NumStat` bit patterns (an `i64` as its
  two's-complement `u64`, an `f64` as `to_bits`, a `bool` as `0`/`1`).
- `str`, `bytes`: `clustering_min_bytes`/`clustering_max_bytes`, optional
  `bytes`, the raw value bytes, capped at 128 bytes per bound. A `min` over
  the cap is truncated to its 128-byte prefix (a prefix orders at-or-below
  the full value, so a truncated min only widens). A `max` over the cap is
  the shortest strict prefix successor within the cap (truncate, then
  increment the last byte that is not `0xFF`, dropping everything after it);
  if every byte of the truncated prefix is `0xFF`, the max is left absent.
  A truncated max must widen, never narrow: a bare prefix of the true max
  orders *below* it and would exclude parts that hold matching rows.
- Populating the wrong pair for the declared type, or both pairs, is
  malformed and hits the fail-open rule: the resolver treats the bound as
  unknown and reads the part.

**NaN.** Under an `f64` key, NaN rows sort by the total order above (so they
land in the extreme parts), but a part containing any NaN in the clustering
column leaves its bounds absent, exactly as the null rule does. This is
deliberately stricter than SKIP_IDX's `has_nan` treatment (where min/max
bound the non-NaN values and a range predicate can still prune, ADR-0095):
the catalog bound is consumed at resolve time, before per-predicate NaN
semantics are established, and this ADR's posture is absent-on-doubt. The
cost is pruning on NaN-polluted parts only, which total-order sorting has
already pushed to the tail.

#### Identity: pinning the clustering configuration that produced a record

The clustering key is read from `t/<tenant_hash>/config`
(`TenantConfigRecord.clustering_key`, decision 2), a mutable, whole-record
CAS-replaced object. `input_set_hash` (docs/catalog-and-mvcc.md) is defined
today as the blake3 digest over the compaction record's sorted `inputs` list
alone. If the clustering key changes between a compaction attempt and its
retry, the same input set now merges into different rows-per-part on each
attempt: the first attempt's `CreateIfAbsent` publishes one set of parts, the
retry recomputes different parts (parts are independently content-addressed
by their own bytes, so they never collide with each other) but tries to
publish the SAME record key with a DIFFERENT `parts` list body. That is
exactly the shape the commit-record protocol calls split-brain and crashes
loudly on (docs/catalog-and-mvcc.md "Commit sequence" step 3); here it would
crash-loop rather than converge, because the cause is a durable config value
that stays changed, not a transient retry.

The key name alone is not the identity, because the key name alone does not
determine the output. The same inputs under the same column name produce
different parts if the column's declared type changes (a different canonical
order), if the canonical order or wire-encoding rules themselves change (a
future revision of the subsection above), if decision 4's stream-batch
partition changes (a different resolved `max_merge_cursors`), or if the part
byte cap changes (different cut points). Pinning only `clustering_key` would
close one hole and leave four.

Decision: for a clustering-eligible (tenant, signal), `input_set_hash` is
computed over the sorted `inputs` list domain-separated by the full resolved
**clustering execution tuple**: the canonical-order/encoding version tag
(`clustering-order-v1`, bumped by any change to the subsection above), the
resolved `clustering_key` name, its resolved declared type, the resolved
`max_merge_cursors` value (decision 4's batch partition is a deterministic
function of the input set and this value), the resolved part byte cap
(`max_l1_part_bytes` or the tier's equivalent), and -- for an override-key
merge -- the resolved partition parameters of decision 4's
range-partitioned rewrite (`clustering_sample_target`,
`clustering_sample_key_cap`, `clustering_resplit_depth`,
`max_clustering_partitions`, and the resolved sort-budget bytes; the
sampler byte reservation and its fixed per-entry accounting charge are
derived from the first two, so they are pinned transitively, and a change
to either derivation is a tuple change like any other), because
partition boundaries, and therefore part cut points, are a deterministic
function of the input set and these values exactly as the batch partition
is of `max_merge_cursors`. The tuple is serialized with
the frozen canonical length-prefixed encoding `ravel-commit` already uses for
`compute_rewrite_input_set_hash`, and the domain separation is the same idiom
that rewrite hash already uses to stay distinct from the compaction hash
(docs/catalog-and-mvcc.md: "a distinct domain from the compaction
`input_set_hash` so the two can never collide"). Any output-affecting value a
later change adds to the clustering configuration joins the tuple in the same
change, or it reopens this hole.

One pre-existing exposure this deliberately does not fix: for *unclustered*
signals, `max_l1_part_bytes` already affects the parts body today and is not
part of today's `input_set_hash`; a mid-retry cap change on an unclustered
signal can already produce the split-brain body collision described above.
That hazard predates this ADR and is untouched by it (unclustered signals
keep today's derivation byte for byte); it is reported here rather than
silently widened or silently fixed.

A change to any tuple element between attempts therefore produces a genuinely
different record key, never a body collision on the same key: two attempts
under two configs coexist on distinct keys and converge through
supersession, not through a crash. Coexistence is NOT claimed harmless:
ADR-0018's overlap-harmlessness argument rests on query-time dedup
collapsing the duplicated candidates, which metrics have and the RLOG
signals this ADR clusters do not (docs/query-engine.md, "The log/span
coordinator merge: order-independent, no dedup" -- a retry after a lost ack
produces byte-identical rows that must both survive, so the log path never
dedups), so two live records over the same input set would return every row
twice on a logs tenant. The compactor-rollout subsection under decision 4
states the resolver rule that includes exactly one such record
deterministically, the durable supersession that converges the anomaly, and
the acceptance test that pins the row count. Unclustered signals (metrics)
keep the existing hash definition byte-for-byte -- untouched, no migration
-- because they never resolve a `clustering_key` in the first place.

That still leaves: given a clustering bound on some `SegmentRef`, how does a
reader know which key, which type's order, and which REVISION of the
canonical-order and wire-encoding rules produced it? Answer:
`CompactionRecord` gains additive `clustering_key`, `clustering_key_type`,
and `clustering_order_version` fields (the key is the sentinel for "none" on
an unclustered signal, the sentinel for EventTime, or the declared override
column name; the type reuses the `TypedAttrColumnType` vocabulary and is
meaningful only for an override key; the order version is a `uint32`, `1`
under `clustering-order-v1`, single-sourced with the identity tuple's tag so
the stored stamp and the hashed tag can never drift) recording the resolved
values used for that record's merge, and `CompactionPart` mirrors all three
onto each part alongside its bounds, and the catalog copies all three onto
each part's `SegmentRef`, so a `SegmentRef` is self-describing: the resolver
never has to re-derive the key, its type, or the encoding rules from the
CURRENT tenant config or the CURRENT codebase (either may have moved on
since this record was written) to know what column its own bounds mean or
which canonical order they were computed under. The tuple's tag alone
cannot serve this purpose: the tag is hashed into `input_set_hash`, never
stored, and ADR-0066's Class D records exactly what a hashed-never-stored
version does -- "a bump is a silent identity split, not a decode error."
The stamp must be a stored field in all three places a bound travels
through, or a reader has no way to know which rules to compare under. A
resolver filtering a narrow-window predicate on column `X` matches it only
against a `SegmentRef` whose own `clustering_key` names `X` AND whose
`clustering_key_type` equals the type the planner resolved the predicate
under AND whose `clustering_order_version` is in the resolver's supported
set (today exactly `{1}`) -- bounds encoded under a since-changed declared
type or a different rules revision compare in a different order and prove
nothing. A `SegmentRef` produced under a different or since-changed key,
type, or order version is read as EventTime-only-bounded (or unbounded, see
the fail-open rule below) for that predicate, never assumed to also bound
`X`.

The stamp alone is only half the protection, and this ADR requires both
halves. A reader that predates the `clustering_order_version` field ignores
it as an unknown protobuf field and would prune a v2-encoded bound under v1
comparison rules -- the exact additive-field hazard decision 7's retention
gate also closes -- which is why (a) the version field ships in the SAME
additive proto change as the bound fields themselves, so no reader exists
that decodes the bounds but predates the stamp, and (b) any future bump of
`clustering-order-v1` is a readers-before-writers rollout under ADR-0066
decision 1: no v2-stamped bound may be written until every pruning reader
in the fleet treats an out-of-set `clustering_order_version` as NO BOUND
and reads the part. Migration class: these three fields are Class C
additive (ADR-0066 decision 4; decision 6 below lists them), and a bound
written before the fields existed decodes with `clustering_order_version`
absent (proto3 zero), which is outside every supported set, so it is
treated as no bound -- the part is read, never pruned. The EventTime pair
(`min_event_ts_ns`/`max_event_ts_ns`) deliberately carries no order
version: its comparison order is the signed-nanosecond order frozen by the
commit-record contract since before this ADR, and it has no encoding
revision to disambiguate.

#### Fail-open: missing, malformed, or unvalidated bounds

A clustering bound (the EventTime pair or an override-column
pair) is a pruning hint, never a correctness gate. If a `SegmentRef`'s
clustering bounds for the predicate's key are absent, fail to decode, carry
a `clustering_order_version` that is absent or outside the resolver's
supported set (the identity subsection above), or were
never validated as having been computed from that part's own final merged
rows, the resolver MUST treat the bound as unknown and include the part:
read the object. The failure mode is **fail-open (read), never fail-closed
(skip)** -- skipping on an unknown bound is silent data loss (a query returns
fewer rows than exist, with no error), not a slow query, and this ADR's
entire value proposition depends on exclusion being provably safe, so an
unsafe exclusion is strictly worse than no exclusion at all.

- **Null handling.** If any row the compactor merges into a part has a null
  in the clustering column -- absent, or stored under a variant other than
  the declared type, the eligibility subsection's null class -- the compactor
  does not populate that part's clustering bounds at all (leaves the fields
  absent), rather than encoding a sentinel or silently excluding the null
  rows from the computed range. The same rule covers a NaN under an `f64`
  key (the eligibility subsection states why). An absent field is unknown by
  the rule above, so the part is always read whenever it contains a row the
  compactor could not bound; nulls-last sorting confines the cost to the
  trailing parts. This applies only to the override-column bounds; the
  EventTime pair has no null case (EventTime is a required field on every
  admitted row, ADR-0010 §8's admission bound).
- **Validated means computed from this part's own output rows.** A
  clustering bound is populated only at the moment the
  compactor writes the part it describes, directly from the rows that landed
  in it. Nothing ever carries a bound forward from an input or from a
  different part, and there is no code path that populates the field without
  computing it fresh; "validated" and "present" are therefore the same
  condition by construction, and there is no partially-trusted intermediate
  state to define semantics for beyond "absent, therefore unknown, therefore
  read."

**Prune, read, or refuse: one matrix, split by blast radius.** Fail-open
(read) is the rule for the QUERY path only; two other consumers of the same
descriptors fail the other way, and this ADR states the split explicitly so
the three rules cannot be mistaken for a contradiction. For any given part
and reader version:

- **Query resolver** (decides prune vs read): prunes only when it fully
  understands the descriptor -- key name matches the predicate's column,
  type matches the planned type, `clustering_order_version` in its supported
  set, bound pair well-formed for the type -- AND the bound misses the
  window. Every other state reads the part. The resolver never refuses: its
  worst mistake is a slow query.
- **Compactor and erasure rewriter** (decide how to consume a record's
  parts as merge inputs): descriptor ABSENT means a legacy stream-major
  input, driven as such; descriptor present and fully understood means a
  clustered input, driven as one sequential run; descriptor present but NOT
  understood (unknown key type, out-of-set order version, malformed pair)
  means REFUSE the merge with a typed error -- never a guess. Their worst
  mistake is durable: mis-ordered replacement objects and a forked
  `input_set_hash` that later work builds on (the compactor-rollout
  subsection, decision 4).
- **Retention sweep** (decides tombstone vs defer): tombstones only on
  fully-understood, fully-expired coverage; every doubtful state defers
  (decision 7's enumerated gate). Its worst mistake is irreversible
  deletion.

The split is not a preference, it is the blast radius: a wrong read costs
latency, a wrong merge writes bad durable state, a wrong tombstone destroys
data. Each consumer fails toward the outcome it can afford.

### 4. Interaction with the compaction tiers

- **L1 compaction -- RLOG signals (logs, alerts, audit) (ADR-0018,
  `crates/ravel-maintain/src/rlog.rs`, `max_l1_part_bytes` default 256 MiB):**
  modified, not replaced -- but the modification is a loop restructuring, not
  a comparator tweak. `merge_catalogs` today is a two-level loop: sorted
  `stream_id` in the OUTER loop, then a ts-ascending k-way merge of just that
  stream's records across inputs in the inner loop ("Merge stream by stream
  in sorted stream_id order", rlog.rs) -- every record of one stream precedes
  every record of the next regardless of timestamp, which is what makes
  today's output stream-major. Reordering the comparator inside the existing
  loop nesting cannot produce ts-major output; the loop nesting itself has to
  invert to a single k-way merge across every stream and every input at once,
  selecting the globally-minimum `(ts, stream_ref, <existing tiebreak>)` head
  at each step, with `stream_ref` demoted from an outer partition to a
  tiebreak. `PartSink`'s cut predicate does NOT change: it already flushes
  purely on `max_l1_part_bytes`, mid-stream if that is where the cap lands
  (issue #711); cutting mid-(what is no longer a stream boundary at all)
  needs no new logic, because the predicate was never "cut on stream
  boundaries" to begin with -- it is, and stays, a byte-cap cut only. The
  exact-conservation contract (`conserve_exact`, rows in = rows out) is
  unchanged by either version of the loop. Within one ingest hour this
  narrows per-part bounds for any hour that holds more than one part.

  One resource-shape consequence the restructuring introduces: today's merge
  holds one decode unit open per INPUT (one stream active at a time, ranged
  readers advance stream by stream). A global k-way merge across every stream
  needs a live cursor per (input, stream) pair, since any stream's next
  record can be the global minimum at any step. That growth is bounded by an
  explicit rule, not by hoping `MergeMemoryTracker` absorbs it; the "Merge
  cursor bound" subsection below states the cap, the batching strategy, and
  the failure mode.

- **L1 compaction -- spans (RSPAN):** unmodified. Decision 2 excludes spans
  from time-leading clustering: `crates/ravel-maintain/src/
  rspan_codec.rs::merge` selects the minimum `(trace_id, start_ts_ns,
  input_index)` head, and the writer "splits only on a trace boundary, never
  mid-trace" (rspan_codec.rs). Moving the leading key to `ts` would scatter
  one trace's records to wherever each record's own timestamp falls in the
  global order, so there is no longer a single trace-boundary point left to
  cut a part on, and preserving the never-split invariant would require
  buffering an unbounded number of concurrently open traces -- a different
  merge algorithm, not a sort-key change. RSPAN keeps its current merge,
  current part-cut rule, and current (wide) per-part EventTime bounds; this
  ADR does not attempt object-level time exclusion for spans.
- **#593 (L2 cross-hour exact compaction):** composes, and is where clustering
  pays off most. A bulk-imported corpus puts the whole EventTime range into a
  few ingest hours, so within-hour L1 clustering alone cannot separate hours
  that do not exist as a partition; L2 merges across hours and is the tier that
  produces objects whose EventTime bounds each cover a narrow, corpus-wide time
  slice. L2 applies the identical sorted-merge-and-cut; it is the same rewrite
  primitive over a wider input set, and it inherits this ADR's per-part bound
  and identity-pinning rules verbatim (decision 3). L2's cross-hour span also
  makes decision 7's retention rule load-bearing: an L2 record is the first
  object shape in Ravel whose retention lifetime cannot be read off a single
  ingest-hour key component, and decision 7 states the rule that covers it.
  This ADR specifies the output invariant; it does not subsume #593, it
  requires #593 to reach full effect on a time-uncorrelated ingest order and
  composes with it. That is a statement about which TIER produces narrow
  bounds, not about which corpus justifies the time-leading key: a
  bulk-loaded, entity-sorted corpus is the case decision 2 excludes from the
  default and acceptance 12 gates.
- **#118 (rollup/downsampling tier):** orthogonal. Rollup is a lossy
  pre-aggregation that reduces row count; clustering is an exact reordering that
  preserves rows. A rollup tier's outputs can themselves be clustered by the
  same rule. No subsumption, no conflict.

#### The RLOG reader contract for a clustered object

The global merge removes the guarantee that one `stream_ref`'s blocks form a
narrow contiguous run, so this subsection states what every RLOG section
means on a clustered object and what each reader does about it. The format
is not changed; what changes is which access patterns stay cheap.

- **STREAM_DIR.** Each entry's `first_blk`/`last_blk` pair stays what the
  format defines it as -- a *containment* claim ("block range containing this
  stream's records", docs/log-segment-format.md), computed by the writer from
  actual row placement -- and stays truthful on a clustered object; it merely
  goes wide, typically near `[0, block_count)`. No reader consumes the pair
  today (the writer computes it, decode validates it, nothing reads it back:
  per-stream block discovery goes through SKIP_IDX), so nothing breaks; the
  contract going forward is that the pair is containment-only and a reader
  MUST NOT treat it as an exclusive-occupancy or contiguity claim.
- **SKIP_IDX.** Per-block `min/max stream_ref` stats stay truthful and go
  wide: on a clustered object nearly every block's stream_ref range spans
  the directory, so `candidate_blocks` with a stream filter degrades to
  approximately every block -- conservative and correct, no longer
  selective. The ts stats move the opposite way, which is this ADR's point:
  ts-filtered `candidate_blocks` gets strictly more selective.
- **Per-stream ordering: holds for the time key, not for an override key.**
  `(ts, stream_ref)` global order keeps any single stream's rows
  ts-ascending across ascending blocks, which is exactly the property
  `RlogRangeReader::stream_blocks` documents ("the ascending block order is
  ts-ascending for the stream"); its current justification ("records sorted
  by `(stream_ref, ts)`") is one sufficient condition, and the
  implementation updates that comment to the property itself, which
  stream-major and time-clustered objects both satisfy. An
  override-key-clustered object does NOT satisfy it -- a stream's rows are
  `(key, ts)`-ordered across blocks -- which is one more reason the run
  model below selects a reader shape from the input's own clustering
  descriptor instead of assuming one. Per-block decode already filters rows
  by `stream_ref` (the boundary-block path); on a clustered object every
  block is a boundary block, same code path, no new logic.
- **Ranged-read cost.** `stream_block_span` fuses a stream's candidate
  blocks into one byte range; on a clustered object that range is
  approximately the whole BLOCKS section, and block-at-a-time
  `stream_blocks` touches approximately every row group. So per-stream
  access to a clustered object costs O(object) bytes, not O(stream). That is
  acceptable because no query-path reader does per-stream RLOG access: the
  logs scan prunes blocks by ts and numeric arms with no stream filter
  (`crates/ravel-query/src/log_fetcher.rs`, `candidate_blocks(ts_min,
  ts_max, None, ..)`), and POSTINGS/GRAM pruning is block-granular, not
  stream-granular. The only per-stream readers in the system are the
  compaction merge and the ADR-0064 erasure rewrite, both driven by
  `merge_catalogs`, and they switch shape per the run model below.
- **Merge inputs become run-shaped, chosen by the input's own record.** An
  input whose compaction record carries no `clustering_key` (every L0, every
  pre-clustering L1) is stream-major: it contributes one ts-ascending run
  per stream via `stream_blocks`, today's `StreamCursor` unchanged. An input
  whose record's `clustering_key` matches the merge's resolved key is
  already globally sorted: it contributes exactly ONE run -- a sequential
  cursor over its level-0 blocks in stored order, fetching whole row groups
  front to back and decoding every stream's rows, which arrive already in
  the record's own clustering order. This is not just an optimization:
  driving a clustered input through per-stream cursors would fetch nearly
  every row group once per stream (quadratic read amplification), and the
  sequential run is what makes L2-over-clustered-L1 cost one pass. Shape
  selection is fail-open for ABSENCE only: an input whose record carries no
  clustering descriptor (every L0, every pre-clustering L1) is driven as
  stream-major, which is correct for it by construction -- its per-stream
  runs are ts-ascending and the merge's sorted-run precondition holds, so
  the cost is read amplification only. A descriptor that is PRESENT but not
  understood -- an unknown `clustering_key_type`, a
  `clustering_order_version` outside the compactor's supported set, or a
  malformed bound pair -- REFUSES the merge with a typed error naming the
  record and the unsupported value; it never falls through to
  `stream_blocks`. The reason falling through is forbidden: per-stream rows
  of an override-key-clustered input are `(key, ts)`-ordered, not ts-runs,
  so a merge that consumed them as ts-runs would emit imperfectly ordered
  REPLACEMENT objects that supersede correctly ordered ones, and later
  compactions and erasure rewrites then build on that output. The damage a
  violation could do is bounded -- `conserve_exact` still holds (no row
  added or dropped), every emitted part's bounds are still computed from
  its own actual rows and stay truthful (merely wide), and exclusion stays
  safe -- but bounded is not licensed: durable mis-ordered output plus the
  forked `input_set_hash` described in the compactor-rollout subsection
  below is exactly the state the refusal exists to prevent. The erasure
  rewrite of a clustered object applies the same rule -- refuse a
  descriptor it does not understand -- and otherwise reads the object
  through the same sequential run, drops the matched rows, and re-emits in
  the same order; its rewrite record copies the input record's clustering
  descriptors (all three, including `clustering_order_version`), so a
  rewritten clustered part stays self-describing.

#### Compactor rollout: readers before writers, applied to compactors

This is ADR-0066 decision 1's rule -- readers before writers -- with the
compactor as the reader, cited rather than reinvented. A clustered record
becomes writable the moment the first tenant's config carries a
`clustering_key` (decision 2's config write is the writer-enable event).
The processes that must already understand it by then are not only query
resolvers (which fail open harmlessly, decision 3's rule) but every process
that consumes compaction records as MERGE INPUTS -- compactors and ADR-0064
erasure rewriters -- because their consumption publishes durable
replacements. Decision: no clustered override may be PUBLISHED until every
compactor and rewriter in the fleet is descriptor-aware, meaning it both
selects input drive shape from the descriptor and refuses a descriptor it
does not understand rather than falling through to `stream_blocks` (the
run-model bullet above). Setting any tenant's `clustering_key` before that
point is a rollout violation, and the release notes for the
descriptor-aware release must say so.

The gate is deployment ordering -- the release that can write clustered
records ships strictly after the release that reads them is fleet-wide,
exactly the ADR-0066 decision 1 shape -- because it CANNOT be a
record-level check: a compactor built before the descriptor fields existed
decodes a clustered record cleanly and silently ignores the unknown fields
(proto3 drops them), so it cannot distinguish a clustered record from an
unclustered one, cannot refuse, and cannot even see that there was
something to refuse. That blindness is the same protobuf-additive hazard
decision 7's retention gate closes with a reader-floor bump. Here the bump
is deliberately NOT taken -- clustered records stay `format_version = 1` --
because query-side fail-open (an old resolver reading everything, pruning
nothing) is this ADR's designed degradation, and a floor bump would trade
it for fleet-wide query failure on every clustered bucket. The cost of
keeping clustered records decodable by pre-descriptor builds is that the
compactor gate is procedural (release ordering) rather than mechanical,
and the next paragraph is its backstop.

**The `input_set_hash` half, separately.** While clustering is enabled for
a tenant, a pre-clustering compactor derives today's plain `input_set_hash`
over the same sealed input set for which a clustering compactor derives the
domain-separated tuple hash (decision 3). Nothing in `CreateIfAbsent` stops
both records from publishing: different hashes mean different record keys,
and each PUT succeeds. The answer to "what stops it" is layered, and "this
cannot happen" is not one of the layers:

1. **Prevention** is the rollout gate above. Once no pre-clustering
   compactor runs, the tuple hash is the only derivation in the fleet, and
   racing clustering compactors still converge on one key because the tuple
   is a deterministic function of the sealed input set and the resolved
   config.
2. **The resolver is the backstop** for a violated gate. Today's rule for
   two live compaction records in one bucket with different
   `input_set_hash` -- include both parts sets and alarm
   (docs/catalog-and-mvcc.md, snapshot resolution step 3) -- is correct
   only under query-time dedup, which metrics have and RLOG signals do not
   (docs/query-engine.md: the log path never dedups; identical rows must
   both survive). For an RLOG signal, including both part sets over the
   same inputs returns every row twice: silent, user-visible duplicate
   rows on every query touching the bucket. So for signals without
   query-time dedup the resolution rule is amended: two live records whose
   sorted `inputs` lists are IDENTICAL but whose hashes differ contribute
   exactly one parts set -- the record with the lexically greater
   `input_set_hash`, a deterministic pick every resolver makes identically
   with no coordination -- the loser's parts are treated as superseded for
   that snapshot, and the existing alarm still fires. The next maintenance
   pass supersedes the loser durably, so the anomaly converges instead of
   alarming forever; an erasure rewrite targeting the bucket must resolve
   this anomaly (supersede the loser) BEFORE it rewrites, or superseding
   only the winner would promote the loser's un-erased parts back to live.
   If the two records' input sets are not identical: when one is a strict
   subset of the other, only the superset record's parts are included (its
   parts contain every row of the subset's inputs, so the subset record is
   redundant); when they overlap incomparably, the resolve FAILS with a
   typed error rather than return silently duplicated or silently missing
   rows -- that state needs a second independent defect to reach, and
   exact-semantics-by-default forbids guessing through it. This amends
   snapshot resolution step 3 in docs/catalog-and-mvcc.md for no-dedup
   signals; the doc amendment ships with the implementing change, and this
   ADR is its decision record.

Consequence named plainly: the failure this closes is duplicate rows
returned from one query on a logs tenant after a mixed-version compactor
race. Acceptance test 8 pins it with an exact row count, its companion pins
the refusal path, and both name which build writes and which reads.

#### Merge cursor bound: a cap, stream batching, and the failure mode

Each live cursor's floor is real memory the tracker cannot shrink: one
decoded block plus up to two locs' raw bytes (the group being decoded and
the one-ahead prefetch -- `StreamCursor` in rlog.rs). A k-way merge needs
every run's head resident to take the next global minimum, so the merge's
minimum working set is `runs x (one decoded block + two row groups)`.
`MergeMemoryTracker` bounds *accumulated* heap and can only observe this
floor, not reduce it: a bucket whose stream cardinality is high enough
pushes the floor past the budget and the merge cannot make progress at all.
Because stream cardinality is durable in the sealed inputs, that is a
deterministic crash-loop, not a transient -- exactly the failure class this
ADR must not ship.

Decision: `CompactorConfig` gains `max_merge_cursors`, defaulting to the
merge memory budget divided by the per-cursor floor above (a derived
default, so the two knobs cannot drift apart). A merge whose total run count
fits the cap runs as one global merge. When it does not, the merge
partitions the bucket's stream-id space -- from the union of the inputs'
STREAM_DIRs, so the partition is a deterministic function of the input set
and the cap, which is why the cap sits in the identity tuple (decision 3) --
into G consecutive stream-id ranges, each contributing at most the cap's
worth of runs, and clusters each batch independently through the same merge,
all batches' parts under the one compaction record in batch-then-part
order. Parts from different batches overlap in time, which costs nothing
this ADR relies on: the resolver unions per-part `SegmentRef`s and never
assumed cross-part disjointness, and per-part bounds stay as narrow as the
unbatched merge's to first order -- a window keeping fraction `f` of the
data keeps about `f` of each batch's parts, so about `f` of all parts
either way; the price is up to G boundary parts per window instead of one.
A clustered input contributes one run regardless of its stream count, so an
L2 merge over clustered L1 parts needs O(inputs) cursors and the cap
effectively never binds there; the cap binds on the first clustering pass
over stream-major inputs, which is exactly where the cardinality lives.
Floor of the rule: every batch needs at least one run per input that
carries any of its streams, so a merge whose INPUT COUNT alone exceeds the
cap fails loudly with both numbers in the error -- the scheduler must
dispatch fewer inputs per merge -- rather than thrash or exceed the cap
silently.

#### The override-key merge has no pre-sorted runs

The run model above merges runs that exist because every input stores rows
in `(stream_ref, ts)` order. That pre-order serves the default EventTime key
and serves nothing else: no input carries any `Region`-ordered run to merge,
so an override-key compaction cannot be a k-way merge of existing runs. It
is a range-partitioned rewrite instead. For an `i64`/`f64`/`bool` key the
compactor unions the inputs' SKIP_IDX NumStats for the key column into a
coarse histogram and cuts key-range partitions from it; for a `str`/`bytes`
key SKIP_IDX carries no per-block stats, and the splitters come from the
sampling pass defined below. Either way, partitions are processed in
canonical key order: each partition's rows are selected by streaming the
inputs' candidate blocks
(`candidate_blocks` numeric arms for an `i64`/`f64`/`bool` key; every block
for a `str`/`bytes` key, which carries no NumStat), sorted in memory under
the `MergeMemoryTracker` bound by the full override comparator, and appended
to the same `PartSink`. A partition whose rows exceed the memory budget
splits recursively by key range (the enforcement, determinism, and refusal
rules below apply to numeric keys exactly as to lexical ones); a partition
that cannot split because it is
one single key value needs no sort at all -- rows sharing a key value order
by `(ts_ns, stream_ref)` per the comparator, per-stream runs are already
ts-sorted, so the run machinery above finishes that value under its existing
cursor bound. Partition boundaries are a deterministic function of the input
set and the resolved configuration, so the identity tuple of decision 3
already pins them (the sampler constants and the sort budget are tuple
members for exactly this reason). The cost shape is stated honestly: an
override-key merge
re-reads any input block whose key range straddles partitions once per
partition that keeps rows from it -- and for a `str`/`bytes` key, with no
block stats to select on, it re-reads every block once per partition, plus
the one sampling pass below. That
read amplification is the price of clustering by a key that ingest order
does not correlate with; it is paid once per compaction, off the query and
ack paths, and an L2 re-merge of already-clustered inputs does not pay it
again (a clustered input is key-sorted, contributes one run, and the run
model applies).

**Initial lexical partitions for a `str`/`bytes` key.** Before any
partition is processed, the compactor streams every input block once in
canonical order (inputs in the record's sorted input order, blocks in
stored order, rows in stored order), decodes each row far enough to read
the key column, and retains every `ceil(N / S)`-th non-null key value,
where N is the exact input row count summed from the input records'
`sample_count` fields (known before the pass starts, never estimated) and
S is `clustering_sample_target` (default 65,536). S is a target, not the
retained count: the stride `ceil(N / S)` retains `R = ceil(N / ceil(N /
S))` keys, and R equals N when N <= S but only approaches S from below
otherwise (N = 65,537 gives stride 2 and R = 32,769, barely half the
target). Every rule below that cuts or counts the sample is therefore
stated over R, the actual retained count, never over the target S; R is
itself a pure function of N and S, so determinism is unaffected. When
N = 0, or every input row's key is null, R = 0: no sampling output
exists, no stride is ever used as a divisor (`ceil(0 / S)` never reaches
one), G collapses to 1, and the merge is the null-partition (or empty)
case below. Each
retained key is truncated to `clustering_sample_key_cap` bytes (default
512 -- deliberately wider than the 128-byte wire-bound cap, which limits
what a `CompactionPart` STORES, not what a splitter may compare). The
stride is deterministic and there is no RNG anywhere in the path, so the
sample -- and every boundary derived from it -- is a pure function of the
input set and the resolved configuration, which is what lets decision 3's
identity tuple pin the partition boundaries. Sampler memory is bounded by
construction, not by the tracker's grace, and the bound is accounted in
BYTES, never in key count: each retained key is charged to
`MergeMemoryTracker` at its retained byte length plus a fixed 64-byte
per-entry charge, against a fixed sampler reservation of 36 MiB
(65,536 x (512 + 64) bytes exactly), reserved before the first block is
read. The initial pass cannot exceed the reservation (R <= S and every
key is truncated to the cap, so its worst case is the reservation's own
defining product), but the accounting runs on this pass anyway, because
the re-sampling pass below shares the reservation and does not share the
truncation: the figure the tracker enforces is bytes, and sampler tracked
bytes exceeding the reservation at any instant, on any pass, is an
enforcement defect of the same class as the sort stage exceeding
`sort_budget`.

The same pass sums the exact decoded byte size B of the input rows (it
decodes every block anyway), and the partition count is
`G = clamp(ceil(1.5 * B / sort_budget), 1, max_clustering_partitions)`,
where `sort_budget` is the merge memory budget minus the cursor floor (the
cursor-bound subsection above) and the sampler reservation, the 1.5 is
headroom for sampling error, and `max_clustering_partitions` defaults to
1,024. `sort_budget > 0` is a checked precondition, not an assumption:
before the sampling pass starts, a resolved merge memory budget less than
or equal to the cursor floor plus the 36 MiB sampler reservation REFUSES
with a typed configuration error naming all three figures (budget, floor,
reservation). The G formula divides by `sort_budget`, and a non-positive
divisor is a misconfiguration to reject at the boundary, never a value to
compute with; the violation condition is exactly `sort_budget <= 0`, and
the refusal fires before any block is read, so a misconfigured merge
costs zero I/O. A merge whose unclamped G exceeds the cap fails loudly
naming B, `sort_budget`, and the cap -- the scheduler must dispatch fewer
inputs per merge -- mirroring the cursor-floor rule above. The sorted
sample is cut at every `ceil(R / G)`-th retained key -- R, the actual
retained count, never the target S: cutting a 32,769-key sample (the
N = 65,537 case above) at `ceil(S / G)`-key strides with G = 7 would
yield three splitters where six are wanted, silently under-partitioning
-- to yield up to G - 1 splitters; adjacent equal splitters collapse, so
no partition is empty by construction. When the sample yields fewer than
G - 1 distinct splitters (small R, or heavy collapse), the effective
partition count is capped at the available distinct splitters plus one.
That cap is a sampling-resolution event, never a refusal and never a
memory hazard: the resulting oversized partitions are absorbed by the
enforced split path below, exactly as the cardinality degenerate case
describes, so an undersized splitter set costs extra split passes, not
the bound and not an unnecessary refusal.

**Why this keeps the merge inside the `MergeMemoryTracker` bound: the
bound is enforced, not predicted.** The memory claim does not rest on the
sample being representative. Each partition's rows accumulate under the
tracker, and a partition that reaches `sort_budget` before its last input
row stops, splits its key range, and re-runs the sub-ranges: sub-splitters
come first from the retained sample restricted to the range; when that
yields fewer than two distinct keys, one range-restricted re-sampling pass
runs: same sampler, keys filtered to the range, retaining UNTRUNCATED
keys so that keys sharing a common prefix longer than the initial cap
become distinguishable. Untruncated keys void the initial pass's
worst-case product, so the re-sample is governed by the byte accounting
alone, under the same 36 MiB reservation, and the accounting is enforced
per retention, never predicted from a key count: retaining a key charges
its full byte length plus the 64-byte entry charge BEFORE the key is
kept, and a retention that would push tracked sampler bytes past the
reservation instead doubles the stride -- the keys at odd ordinal
positions of the current retained sequence are evicted and their bytes
released, halving retained bytes, and retention continues at the doubled
stride. Stride doubling is a pure function of the input sequence and the
resolved configuration, so re-sampled splitters stay deterministic and
decision 3's identity tuple still pins them; doubling repeats as needed,
and a re-sample that ends with fewer than two distinct retained keys
falls into the unsplittable case below. One state cannot be degraded
around: a single key whose own bytes plus the entry charge exceed the
whole reservation cannot be retained at any stride, and the re-sample
REFUSES with a typed error naming the key's byte length and the
reservation -- deterministic, before allocation, the same
refuse-not-spill posture as the unsplittable case. There is therefore no
path, initial or re-sampling, on which sampler allocation precedes
accounting: the merge memory bound stays enforced, in bytes, on every
path. Recursion past
`clustering_resplit_depth` (default 2) refuses, below. Sampling quality
therefore affects only how often the split path runs (one extra read pass
per split), never the peak heap. The peak-heap figures and their bands:
peak tracked bytes for the sort stage stay within `[0.25, 1.0] x
sort_budget` on the acceptance corpus -- above 1.0 the enforcement is
broken, below 0.25 the partitioner overcut by more than 2x; peak tracked
sampler bytes never exceed the 36 MiB reservation on any pass (a single
byte over is an enforcement defect; there is no tolerance band above);
and the expected resplit and stride-doubling counts on that corpus are
stated per test (a count outside the stated band is a pre-registration
miss, not noise).

Degenerate cases, each with a defined outcome:

- **One distinct key across every block.** The sample holds one distinct
  value, G collapses to 1, and the single-key-value rule above applies:
  rows order by `(ts_ns, stream_ref)`, per-stream runs are already
  ts-sorted, and the run machinery finishes the merge under the existing
  cursor bound with no sort buffer at all. The same rule catches skew
  inside a wider merge: a range that cannot split because it is one key
  value is never sorted in memory, so one hot key holding more than
  `sort_budget` of rows never reaches the refuse path.
- **Keys sharing a long common prefix.** Distinct keys that collide within
  the 512-byte initial sampler cap yield equal splitters, which collapse;
  their rows land in one partition. If that partition exceeds
  `sort_budget`, the split path runs with untruncated keys from the
  range-restricted re-sample, under the byte-accounted reservation above
  (stride doubling absorbs many long keys; a single key wider than the
  whole reservation is the typed re-sample refusal). Only keys
  indistinguishable at full length
  are truly unsplittable, and those are by definition one key value --
  the previous case.
- **Cardinality far above the sample's resolution.** Expected and
  harmless: splitters cut row-count quantiles, not distinct values, so the
  sample needs to resolve byte mass, not cardinality. A resolution miss
  surfaces as an oversized partition and is absorbed by the enforced
  split, never by the heap.
- **The bound cannot be met.** When an over-budget partition's range
  cannot split (more than one distinct full-length key, but fewer than two
  distinct splitter candidates; `clustering_resplit_depth` exhausted; or
  the re-sample's single-key-over-reservation refusal above), the merge
  REFUSES: a typed error naming the range's key
  prefix, its accumulated bytes, and `sort_budget`; nothing is published,
  and the inputs stay live and queryable. Refuse, not spill: a
  spill-to-object-storage path would need a scratch key namespace and a
  cleanup path, rejected on the same grounds as the multi-pass external
  merge (alternatives below), and a refused clustering pass costs
  clustering quality on that bucket, never availability or correctness.
  "May not satisfy the bound" is therefore not an outcome this design
  has: every merge ends clustered under the bound, finished by the run
  machinery (the single-value case), or refused with a typed error.

Null rows (and NaN rows under an `f64` key) are never sampled; they form
the trailing partition per the nulls-last rule, need no sort (rows order
by `(ts_ns, stream_ref)`, the single-value case again), and leave their
parts' bounds absent per the fail-open rule.

Summary: modifies L1's ordering choice, composes with #593 (its natural home)
and #118, subsumes none, conflicts with none.

### 5. The conflicting-key problem, stated honestly

A sort key leads with exactly one dimension. Time clustering answers a genuinely
narrow EventTime window (of the measured statements, only q43 has one). A
high-cardinality point lookup (`WHERE UserID = c`, q20) wants the surviving rows
gathered by UserID, which a time-leading sort scatters across every part. The
same scattering is what Context describes for `CounterID = 62` in q37-q42: a
low-cardinality entity predicate is served by locality on that entity, and a
time-leading sort destroys it. A compound key `(ts, UserID)` does not fix this:
within each contiguous ts run UserID is still spread across the run's whole
value domain, so a UserID predicate prunes nothing at object granularity; and
`(UserID, ts)` destroys the time clustering a real window needs. One
key cannot serve both, and this ADR does not claim it can.

The answer for point lookups is the **POSTINGS attribute index (ADR-0049,
#19)**, not a second sort. POSTINGS maps an indexed field's term to the block
indices that hold it, giving exact block-level exclusion for an equality/`IN`
predicate without reordering any data, and it is rebuilt on every merge from
the merged contents (rlog.rs, ADR-0049 decision 6). A point-lookup tenant
indexes UserID and prunes on postings; a narrow-window tenant clusters on time.
The two mechanisms are independent and both compose with a clustered object:
clustering narrows the object's time bounds, postings narrows its per-field
block set, and a statement with both predicates uses both. What POSTINGS does
*not* give today is object-level (pre-GET) exclusion for a point lookup, since
posting lists live inside the object; extending object-level point-lookup
exclusion is a separate ADR (a per-object bloom or per-object term summary in
the catalog), explicitly out of scope here. This ADR takes the position that
object-level clustering is for range/window keys and object-level point-lookup
exclusion is an index problem, and it solves only the former.

### 6. Invariants and format bump

- Objects are immutable: compaction writes new content-addressed objects and
  never mutates one. Clustering is a choice of what rows a new object holds, so
  it does not touch the immutability invariant.
- Object storage is the source of truth: the clustering-key bounds the planner
  reads are derived from, and re-verifiable against, the objects; an index
  failure degrades to reading the object, never to a wrong answer.
- **Segment format:** no bump. Clustering reorders rows within the frozen
  RLOG/RSPAN layout; the on-object bytes a clustered part writes are a valid
  current-version segment, produced by the same writer an L0 flush uses
  (rlog.rs re-blocks through a fresh `RlogWriter`). One inference this
  bullet must stop a careful reader from making: ADR-0032's Context says
  `LogFooter` has no compaction identity, which, read as current state,
  would make an L1 `.rlog` indistinguishable from L0 and this no-bump
  claim unsound. That sentence describes the pre-ADR-0032 state, and its
  decision shipped: the RLOG trailer is at version 4 today
  (`crates/ravel-logseg/src/footer.rs::VERSION`; v2 added the identity
  fields per ADR-0032, v3 per ADR-0095, v4 per ADR-0699), and `LogFooter`
  carries `level`, `input_set_hash`, and `part_index` on every RLOG
  object, so an L1 `.rlog` is distinguishable from L0 by the footer's
  `level` alone and a clustered part needs nothing new from the trailer.
  This is a compaction
  output-choice change, not a page-grammar or trailer change, so it is not a
  migration class under ADR-0066 decision 4: nothing on an existing object
  changes meaning, old objects stay readable and are re-clustered
  opportunistically through the normal rewrite-on-touch force, and there is no
  version floor to raise.
- **Catalog record:** this ADR adds per-part bound and descriptor fields,
  per-record descriptor fields, and changes what `input_set_hash` is computed
  over for a clustering-eligible signal. Each is judged against the same test
  ADR-0066 decision 4 sets: does an old reader that does not know the new
  field still decode the record correctly and reach the same answer for the
  bytes it does understand?
  - The additive proto fields: on `CompactionPart`, the bound pairs
    (`clustering_min_bits`/`clustering_max_bits`,
    `clustering_min_bytes`/`clustering_max_bytes`) and the descriptors
    (`clustering_key`, `clustering_key_type`, `clustering_order_version`);
    on `CompactionRecord`, the
    same three descriptors and decision 7's
    `covered_hour_min`/`covered_hour_max` (additive only on an L1 record,
    where both equal its single bucket -- a cross-hour record is
    deliberately NOT in this class, next bullet); on `TenantConfigRecord`,
    `clustering_key`. All are the same class as the `enc`/`config` histories
    (docs/catalog-and-mvcc.md) and as this ADR's own EventTime precedent
    (`CompactionPart.min_event_ts_ns`/`max_event_ts_ns` are already additive
    fields on the same message, added without a bump when ADR-0018 shipped
    compaction). An old reader ignores an unknown field on a message it
    already knows how to decode and reads the object -- which is exactly the
    fail-open direction; no bump, no migration. Additive is safe here
    precisely and only because the QUERY path is the consumer and its
    unknown-field behavior (read everything, prune nothing) is the designed
    degradation; the two consumers for which ignoring an unknown field is
    NOT safe each get a non-additive guard: the compactor gets the
    readers-before-writers rollout gate (decision 4's rollout subsection),
    and retention gets the reader-floor bump in the next bullet. One caveat
    inherited from ADR-0090, not created here: `clustering_key_type` reuses
    the `TypedAttrColumnType` vocabulary as a field VALUE, not a new enum
    case, so it adds no new decode-refusal path; only ADR-0101's own `f64`
    enum addition carries that rollout ordering, on ADR-0101's schedule.
  - The one deliberate exception to the additive class: a CROSS-HOUR
    record (decision 7: `covered_hour_max > covered_hour_min`) is published
    at `format_version = 2` with one replica per covered hour, the home
    replica written last as the all-or-nothing visibility barrier
    (decision 7's barrier paragraph). That is a
    reader-floor raise on `CompactionRecord` -- ADR-0066 decision 4 Class
    C's own lever for a change old readers must refuse rather than
    misread, using the typed `format_version` check the record already
    carries -- taken because the retention sweep acts on a bucket's
    LISTING, and an additive field on a record the sweep never lists can
    carry no signal to it (decision 7 states both blindnesses). Rollout is
    readers-before-writers (ADR-0066 decision 1): every sweep, resolver,
    and fold in the fleet decodes v2 and enforces decision 7's coverage
    gate before the first cross-hour record is published. Until that first
    publication nothing anywhere changes: no v2 record exists, and every
    v1 record is single-hour by construction.
  - The `input_set_hash` domain-separation change (identity, decision 3) is
    NOT a segment-format or catalog-schema change -- no wire field changes
    shape -- but it IS a change to how a value in an existing field is
    computed, and unlike an additive field, an old reader cannot tell a
    domain-separated hash from an undifferentiated one just by decoding it;
    it can only fail to find the record it expects at the key it computes.
    This is scoped narrowly: it applies only to a (tenant, signal) that is
    clustering-eligible AND has a `clustering_key` resolved, i.e. only once
    this feature is turned on for that tenant, so a tenant that never sets
    `clustering_key` computes `input_set_hash` exactly as today, byte for
    byte, and needs nothing. For a tenant that turns clustering on, this
    changes a frozen identity derivation for that tenant's future L1/L2
    records; new records simply key differently than an unclustered record
    over the same inputs would have, which is safe (nothing old ever
    collides with it) but is still a derivation change, not an additive
    field, so it is called out explicitly rather than folded into the
    additive bucket above. Widening the domain from the key name to the full
    execution tuple does not change this classification: it is the same
    derivation change with a larger input, and a change to any tuple element
    (a declared-type change, a cursor-cap or part-cap change mid-deploy)
    re-keys future records into coexisting-then-superseded records, the safe
    direction. It needs no reader-visible migration (nothing stored changes
    shape, and no existing record is reinterpreted), so it does not trigger
    ADR-0066 decision 4's migration class, but it is a one-way switch per
    tenant: once a tenant's config carries a `clustering_key`, its
    compaction records key differently for as long as clustering stays on.

Stated plainly: no segment-format bump; the per-part bounds and
every descriptor stay additive because each is an unknown-field-tolerant
addition to a message old readers already decode, with fail-open as the
old-reader behavior on the query path. The identity change stays out of the
bump/migration
system entirely because it is opt-in per tenant and touches no wire shape --
but it is a real, one-way change to a frozen derivation for that tenant, and
this ADR states that plainly rather than folding it into "just another
additive field." The single catalog-record reader-floor raise this ADR
takes is decision 7's cross-hour v2 record, ADR-0066 Class C, gated
readers-before-writers; everything else carries no bump and no migration
class.

### 7. Erasure and retention

Both walk objects. Erasure keys on the ingest-hour bucket exactly as before;
clustering within a bucket does not change that. Retention also keys on the
ingest-hour bucket for L1, unchanged -- but not for L2, which introduces the
first object shape whose retention lifetime spans more than one bucket (see
below):

- **Retention (ADR-0019, age-based):** unchanged for within-hour L1
  clustering -- a tombstone still retires a whole ingest hour, and the
  objects it retires are that hour's clustered parts, each covering exactly
  one hour as today. **L2 cross-hour (#593) needs an explicit rule, not a
  note.** An L2 object spans an ingest-hour range, and ADR-0019 decision 4
  deletes a bucket's data objects, compaction records, and parts as a unit
  once every record in the bucket is expired. An L2 object covering one
  expired hour and one still-live hour can neither be dropped (deletes rows
  still inside the retention window, violating ADR-0019's exact floor: "no
  sample younger than R is ever excluded") nor kept (retains expired rows
  past R, violating the same floor from the other side). Of the four shapes a
  rule here could take -- splitting the object at compaction time, a
  selective rewrite that drops the expired hour's rows and re-emits a
  narrower object (repurposing the ADR-0064 erasure-rewrite mechanism),
  deferring the whole bucket's tombstone until every hour the object covers
  is expired, or forbidding a clustered part from ever spanning a retention
  boundary -- this ADR chooses **deferral**:

  `CompactionRecord` gains a `covered_hour_min`/`covered_hour_max`
  pair (u32, unix hours, same encoding as `ingest_hour_bucket`) recording the
  full range of ingest hours whose rows an L2 record's parts contain; for an
  L1 record both equal the single `ingest_hour_bucket` it already carries
  and the pair is a plain additive field (`format_version` stays 1), so
  no L1 behavior or reader changes. For a cross-hour record the pair alone
  cannot carry the rule; the compatibility gate below states what does.
  ADR-0019 decision 1's expiry test --
  "sealed, and every record in it has `max_event_ts < now - R`" -- extends
  to: a bucket is expired only when every record whose
  `covered_hour_min..covered_hour_max` range includes that bucket's hour is
  ALSO expired by the same test, checked over its own full covered range. A
  bucket named by a live-spanning L2 record's covered range therefore does
  not tombstone -- not that hour alone, and not any other hour the same
  record covers -- until the record's newest covered hour also expires, at
  which point every hour it covers tombstones together in the same sweep
  pass. This is bounded over-retention only (the record's oldest covered
  hour stays live slightly past its own individual expiry, for at most the
  width of the record's covered-hour range), the same class of cost
  ADR-0019 decision 1 already accepts for bucket granularity ("the cost of
  bucket granularity is bounded over-retention, not under-retention"); this
  just widens the bound from one bucket to one L2 record's covered-hour
  range, with no new correctness mechanism and no new deletion path.
  Splitting and selective rewrite are both rejected for this ADR's scope:
  splitting couples the compactor to the retention frontier at merge time (a
  merge would have to know, and re-check, R for every input hour it touches,
  for a benefit that only matters at the one boundary a corpus happens to
  straddle), and selective rewrite would stand up a whole ADR-0064-shaped
  rewrite path scoped to retention alone; deferral needs neither and costs
  only slightly stale deletion, which ADR-0019 already treats as an
  acceptable failure direction everywhere else. Forbidding a clustered part
  from ever spanning a retention boundary is rejected because it is not
  enforceable at merge time without the same frontier-coupling splitting
  would need.

  **Why additive fields cannot carry this rule.** Two independent
  blindnesses, either one fatal on its own. First, protobuf-additive:
  a sweep built before the covered pair existed decodes a cross-hour
  record cleanly, silently drops the unknown fields (proto3 behavior, not
  a bug), sees only `ingest_hour_bucket`, and applies the per-bucket
  expiry test -- the record itself cannot warn a reader that does not
  know to look, so "older readers detect the additive field" is not a
  mechanism, it is the absence of one. Second, placement: a
  `CompactionRecord` is stored under one `ingest_hour_bucket`, and the
  sweep evaluating any OTHER covered hour never reads it at all -- even a
  field the sweep fully understands cannot gate a bucket whose listing
  does not contain the record. A working gate must defeat both.

  **The gate's mechanism: a reader floor plus one replica per covered
  hour.** A cross-hour record (`covered_hour_max > covered_hour_min`) is
  published at `format_version = 2` -- the reader-floor field
  `CompactionRecord` already carries, with the typed refusal ADR-0066
  decision 2 mandates -- and is written once under EVERY hour in its
  covered range: one record object per covered hour, deterministic keys,
  `CreateIfAbsent`, bodies identical except that each replica's
  `ingest_hour_bucket` names its own bucket (so ADR-0010 §7 key/field
  validation holds per replica), all replicas sharing the record's
  `input_set_hash` so any reader that discovers several dedupes by it.
  This defeats both blindnesses at once. Placement: the sweep evaluating
  hour H finds a replica in H's OWN listing, so the per-bucket evaluation
  model stays intact. Additive-blindness: a pre-coverage sweep that lists
  H hits the v2 typed `UnsupportedVersion` refusal, its sweep of that
  bucket FAILS loudly, and no tombstone is written -- fail-closed, never
  fail-oblivious. The alternatives argued and rejected: a required field
  cannot exist (proto3 has no required fields; an unset scalar is
  indistinguishable from zero, so absence cannot signal anything); a new
  record kind under a new key suffix is equally fail-loud for old readers
  (an unknown key shape is a fail-loud error, resolution step 2) but
  forks the record schema and every discovery, inclusion, and audit path
  for what is semantically still a compaction record, where the version
  floor reuses the existing typed check byte for byte; a single-placement
  record plus a sweep look-back window ("also list the previous W
  buckets") keeps old sweeps blind -- an old sweep does not know to look
  back, which is the additive-field trap restated in time. Replication
  also makes a cross-hour record discoverable to a resolve whose window
  overlaps ANY covered hour, which #593 needs independently; the resolver
  dedupes replicas by `input_set_hash` and applies input exclusion
  snapshot-wide (both only once the record is live per the barrier
  paragraph below), and the corresponding amendment to
  docs/catalog-and-mvcc.md's resolution section ships with #593's
  implementation, not silently here. Two scheduling guards keep
  publication and the sweep from racing: the compactor selects only input
  hours at least `l2_retention_margin` (default 24 h) short of their own
  expiry, and once the home replica is durable every covered bucket's
  listing already carries a replica (the home is written last, per the
  barrier paragraph below), so the coverage gate defers each of them; the
  window before the home lands is publication itself, which the margin
  covers and the barrier keeps invisible.

  **Publication is all-or-nothing, and the barrier is one object.** The
  replica writes above are independent `CreateIfAbsent` PUTs, and object
  storage offers no multi-object atomic write, so without a further rule
  a publication that stops partway would leave some covered hours seeing
  a replica and others seeing nothing: an hour holding a replica would
  treat the record's inputs as superseded while a missing hour's resolve
  cannot exclude them, and a replica-less expired hour could tombstone
  inputs a later-completed record still claims. The rule: a cross-hour
  record, and the supersession of its inputs, is live if and only if its
  HOME replica exists -- the replica stored under the record's own
  `ingest_hour_bucket`, at its deterministic key. The compactor's
  publication order is normative: the content-addressed part objects
  first (durable before any replica references them, as in every
  compaction publish), then every non-home replica in ascending
  covered-hour order, then the home replica LAST, only after every other
  replica is durable. That single `CreateIfAbsent` PUT of the home
  replica is the atomic visibility barrier: one object, one existence
  bit, flipped by the one write shape object storage does make atomic.
  Until it exists, nothing is live. A non-home replica discovered by any
  consumer (resolver, sweep, compactor, erasure rewriter) confers no
  supersession and no coverage claim until that consumer confirms the
  home replica exists, by GET on the home key derived from the replica's
  own `ingest_hour_bucket` and `input_set_hash` fields (one GET per
  discovered cross-hour record per pass, cacheable per snapshot). Home
  present: the record participates everywhere, replicas deduped by
  `input_set_hash` as above. Home absent: the replica is an orphan of an
  interrupted publication -- the resolver includes the record's inputs
  exactly as if no record existed (nothing superseded, no rows lost, no
  rows duplicated), and the sweep DEFERS the bucket (gate case 6 below).
  An interrupted publication therefore leaves every covered hour agreeing
  the record does not exist yet; the failure this closes is one hour
  believing its inputs superseded while another does not, and no partial
  state can express that any longer. Convergence is the compactor's
  normal idempotent retry: keys are deterministic and every PUT is
  `CreateIfAbsent`, so a re-run over the same sealed inputs re-writes
  whatever replicas are missing and then the home replica, completing the
  original publication rather than forking it. One pre-flight guards the
  flip itself: immediately before the home write, the compactor re-checks
  every covered bucket for a retention tombstone and REFUSES to publish
  the home replica if one exists (the inputs it would supersede are
  already retired); the window between that check and the home PUT sits
  inside `l2_retention_margin` by the scheduling guard below, so a
  tombstone cannot land inside it. An orphan replica older than the
  margin is a defect deferral with the standard alarm, remedied by the
  retry or by operator supersession, never by a guess.

  **The gate, enumerated.** For a coverage-aware sweep evaluating bucket
  H, every record listed under H is checked before H may tombstone, and
  each of the following states DEFERS the tombstone -- H is not
  tombstoned this pass, a typed per-case defect counter increments, and
  an alarm names the record key and the case. This is the exhaustive
  list, not an illustration:

  1. **Absent**: a record with `format_version >= 2` whose covered pair
     is unset (both zero). Coverage unknown; defer.
  2. **One-sided**: exactly one of `covered_hour_min`/`covered_hour_max`
     set. Malformed; defer.
  3. **Reversed**: `covered_hour_min > covered_hour_max`. Malformed;
     defer.
  4. **Out-of-range**: H outside `[covered_hour_min, covered_hour_max]`
     on a replica listed under H; or the record's own
     `ingest_hour_bucket` outside its covered range; or the range wider
     than `max_l2_covered_hours` (config, default 168); or a
     `format_version = 1` record claiming a covered range wider than its
     own single bucket (a cross-hour claim without the floor bump -- the
     gate violation itself, made visible). Defer.
  5. **Unknown**: `format_version` above the sweep's supported set. The
     typed refusal fires and the sweep for H fails; it never decodes
     what it can of the record and proceeds, and it never treats the
     record as v1. Defer. (This is also the pre-coverage sweep's path,
     for free: its supported set is `{1}`.)
  6. **Orphaned**: a cross-hour replica listed under H whose home
     replica (the record object under the replica's own
     `ingest_hour_bucket`) does not exist -- an interrupted publication,
     per the barrier paragraph above. The record is not live and
     supersedes nothing, but H's listing carries a record whose
     lifecycle is unresolved; defer.

  A record that passes all six checks participates in the extended
  expiry test above: H tombstones only when every record whose covered
  range includes H is expired over its own FULL covered range, and then
  every hour that record covers tombstones together in the same pass.

  **What bounds the deferral.** Deferral is correctness-preserving
  (over-retention, ADR-0019's accepted failure direction), but it must
  not silently halt retention forever, so each path is bounded and
  visible. The healthy defer -- valid coverage, newest covered hour not
  yet expired -- is bounded by the covered width: at most
  `max_l2_covered_hours` hours (default 168 h) of over-retention past the
  hour's own expiry, the figure decision-side text above stated as a
  class and now states as a number; the compactor refuses at merge time
  to build a record wider than the cap, so the bound is enforced at the
  writer, not hoped for at the reader. Defect defers (cases 1-4) do not
  self-heal: each is reported per pass with record key and case, a
  defect deferral persisting past `retention_defect_alarm_after`
  (default 24 h) escalates to an error-level alarm, and the remedy is
  operator action (supersede or fix the writer). Case 6 is the one defect
  defer with a self-heal path -- the compactor's idempotent republication
  (the barrier paragraph above) completes the interrupted publication --
  and it escalates on the same clock if it persists. A defective record
  blocks exactly the buckets whose listings carry one of its replicas;
  every other bucket's retention progresses unaffected, so a single bad
  record can never stop the sweep fleet-wide. Case 5 converges by
  upgrading the lagging process, which the readers-before-writers
  rollout (decision 6) makes a transient of the deployment window, not a
  steady state.
- **Erasure (ADR-0064, selective rewrite):** a rewrite drops a subject's rows
  and re-emits the object. Two consequences. First, a rewrite that removes rows
  can only narrow a clustered part's key bounds, never widen them, so the
  catalog bounds stay conservatively valid without recomputation being a
  correctness requirement (though the rewrite should recompute them so exclusion
  stays tight). Second, which objects hold a subject is unchanged by clustering
  unless the subject correlates with the clustering key; erasure still discovers
  candidate objects by its predicate over the catalog, not by arrival order, so
  clustering neither helps nor hurts erasure discovery. The rewrite record's
  effective-input chasing (docs/catalog-and-mvcc.md step 3) is unaffected: a
  clustered part is superseded and re-emitted exactly as an unclustered one.

## Acceptance

Each band is pre-registered before any measurement, on the tracking issue, with
its reasoning. A figure must be emitted exactly once and inside its band; an
absent or duplicated figure fails the same as one outside the band.

1. **Objects opened for a genuinely narrow-window statement fall in proportion
   to the window, reaching an order of magnitude only when the window is at or
   under a tenth of the corpus.** The reduction is proportional to `f`, not a
   fixed order of magnitude at every width: claiming 10x for a window keeping
   20% of rows would be arithmetically impossible, and the gate below is
   proportional for that reason. The measurement corpus is an organic-arrival
   telemetry corpus whose ingest order correlates with event time, never
   `clickbench-v4`: Context states why that corpus's q37-q42 are selected by
   `CounterID` and would regress, and acceptance 12 is the separate gate that
   applies to it. The measured statement is a window keeping fraction `f` of
   the corpus's rows with no other prunable predicate, and `f` is registered
   with the band. Hard gate: objects opened `<= max(0.1, 4f) x` the
   corpus's live object count. The gate is proportional to `f` rather than a
   fixed fraction because a fixed ceiling and a proportional expectation
   contradict each other whenever `f` exceeds the ceiling: a window keeping 20%
   of rows cannot also open under 10% of objects, so a flat `0.1` would fail a
   correct implementation on a wide window. Pre-registered expectation: within
   `[f, 2f] x` the live object count, plus at most `G` boundary parts per
   (shard, ingest hour) the window touches, where `G` is decision 4's stream
   batching count for that bucket. The allowance must track `G` rather than
   being fixed at one: decision 4 permits up to `G` boundary parts, so a fixed
   allowance of one would reject a conforming clustered output whenever
   `G > 1`. A statement whose registered `f`
   exceeds `0.25` is not a narrow-window statement and does not belong in this
   acceptance at all. Reasoning: the surviving rows are contiguous in EventTime
   once clustered, so a window keeping `f` of the rows keeps about `f` of the
   parts at the same block size and byte cap, and decision 4's stream batching
   contributes at most one boundary part per batch, hence up to `G` in total. A result inside the hard
   gate but outside that band passes but is a pre-registration miss to
   investigate (clustering weaker than the contiguity argument predicts, e.g.
   L2 has not run and the window still spans multiple within-hour part sets).
   Stated as an exact object count from the resolve accounting, never as a
   wall-clock improvement.
2. **An excluded object costs ZERO GETs, asserted per phase.** For every object
   whose clustering-key bounds miss the query window, the data GETs attributed
   to it in the probe, scan, and decode phases are exactly 0; the only cost of
   its existence is the per-bucket resolve LIST and its cached commit/part
   record, both of which are per-bucket and already paid. Asserted against the
   ADR-0044 per-phase cost accounting, not by review.
3. **Full-scan statements do not regress.** A full-scan statement (e.g.
   `COUNT(*)` with no prunable predicate) must not open more objects because
   clustering fragmented the corpus. Budget: full-scan whole-object GETs `<=
   1.1 x` the pre-clustering live-object count (`<= 9,266`). Clustering keeps
   the same `max_l1_part_bytes` cap and compaction merges L0 into fewer L1
   parts, so the count should drop, not rise; a full scan opening more than the
   budget detects a part-fragmentation regression and fails.
4. **Rows returned are identical to the unclustered path, verified
   differentially.** Every corpus statement is run against a clustered snapshot
   and an unclustered snapshot of the same data; the two result sets must be
   byte-identical after a canonical ordering (a differential test, not review).
   Clustering is exact-conservation (`conserve_exact`: the merged output is the
   exact multiset of inputs, reordered), so any divergence is a bug in the
   clustered merge, and the differential harness is what proves the
   reordering-only claim end to end.
5. **A missing or malformed clustering bound always reads the object.** A
   targeted test constructs a part whose clustering bounds are absent, a
   second case where the populated pair does not match the part's
   `clustering_key_type` (the wrong-pair malformation), a third where
   the part's recorded type differs from the type the predicate was planned
   under, and a fourth where the part's `clustering_order_version` is
   outside the resolver's supported set,
   and asserts the resolve path issues a data GET for the part under
   a narrow-window predicate on that column in all four cases -- the
   fail-open branch actually executing, not merely present in code. The test
   is demonstrated failing first: run against a deliberately fail-closed stub
   (one that skips the part when its bound cannot be read) and confirm the
   GET-count assertion fails there, before trusting the same assertion to
   guard the real implementation.
6. **The merge's live-cursor count never exceeds `max_merge_cursors`.** A
   test builds a bucket whose per-input stream cardinality forces run count
   past a deliberately small cap, asserts the batching path executes (G > 1,
   reported by the merge), asserts the cursor high-water mark stays at or
   under the cap for the whole merge, and runs the acceptance-4 differential
   harness over the batched output so batching is proven order-preserving
   per part and row-conserving, not assumed. A merge dispatched with more
   inputs than the cap must fail loudly naming both numbers, asserted as a
   typed error, not a panic.
7. **A bound is interpreted only under the rules that produced it**
   (`clustering_order_version_gates_prune`). Mixed-version, both
   directions, versions named. (a) Writer: a `clustering-order-v1` build
   stamps a part's bounds `clustering_order_version = 1`. Reader: a build
   supporting `{1, 2}`, where the test's synthetic v2 flips the `str`
   comparison so v1 and v2 rules classify the part differently. On a
   corpus where the part holds in-window rows under v1 rules, the reader
   must apply v1 comparison to the v1-stamped bound: data GETs for the
   part exactly 1 (0 is the prune-with-wrong-rules failure this pins; 2+
   is a retry leak), rows returned equal to the acceptance-4 differential
   baseline exactly (band: +-0 rows). (b) Writer: a v2-stamping build.
   Reader: a `{1}`-only build. Every v2-stamped part is read, never
   pruned: prune count for those parts exactly 0, data GETs exactly 1 per
   part, rows equal to baseline (+-0). Case (a) is demonstrated failing
   first against a stub reader that ignores the stamp and applies its
   newest rules to every bound.
8. **Mixed-version compactors cannot expose duplicate log rows**
   (`mixed_compactor_same_inputs_no_duplicates`). Writer side: a
   pre-clustering compactor build (plain `input_set_hash`) and a
   clustering build (tuple-separated hash) each publish a record over the
   identical sealed three-input logs bucket; both records are live.
   Reader side: a clustering-build resolve over the bucket. Asserts rows
   returned equal the corpus row count N exactly (pre-registered; band
   +-0 -- the failure this pins returns exactly 2N), the duplicate-record
   alarm fires exactly once, and the included record is the one with the
   lexically greater `input_set_hash` (asserted by record key, so the
   deterministic pick is pinned, not just "one of them"). Companion
   (`descriptor_aware_compactor_refuses_unknown`): a compactor whose
   supported set lacks the input record's `clustering_key_type` (and, in
   a second case, its `clustering_order_version`) returns the typed
   refusal error naming the record and the unsupported value, and
   publishes nothing: compaction-record PUTs exactly 0, part PUTs exactly
   0.
9. **High-cardinality `str` override merge holds its memory bound**
   (`str_override_high_cardinality_bound`). Corpus: 1,048,576 rows,
   at least 1,000,000 distinct `str` keys of 24-4,096 bytes, including
   one cohort sharing a 600-byte common prefix sized to defeat the
   512-byte initial sampler cap and force exactly one range-restricted
   re-sample. Asserts: rows out exactly 1,048,576 (conservation, +-0);
   adjacent-row order violations under the full override comparator
   across the concatenated parts exactly 0; sort-stage peak tracked bytes
   within `[0.25, 1.0] x sort_budget` (above: bound broken; below:
   partitioner overcut by more than 2x); peak tracked sampler bytes at
   most the 36 MiB reservation on both passes (one byte over is
   enforcement broken; no upper tolerance band exists); stride doublings
   during the planted re-sample exactly 0 (the cohort's untruncated keys
   fit the reservation by construction of the corpus; 1+ is a
   pre-registration miss); resplit passes exactly 1 (the
   planted cohort; 0 means the plant did not exercise the path, 2+ is a
   sampler-quality miss); partition count G within `[G_ideal, 2 x
   G_ideal]` for `G_ideal = ceil(1.5 * B / sort_budget)`. Companions:
   `lexical_partition_determinism` merges the same inputs twice and
   asserts byte-identical output (differing part content hashes exactly
   0); `str_override_refuses_unsplittable` sets
   `clustering_resplit_depth = 0` against an over-budget multi-key
   partition and asserts the typed refusal (naming key prefix,
   accumulated bytes, and `sort_budget`) with part PUTs exactly 0 and
   inputs still live; `non_positive_sort_budget_refuses_before_sampling`
   resolves a merge memory budget exactly equal to the cursor floor plus
   the sampler reservation (`sort_budget = 0`) and asserts the typed
   configuration error naming budget, floor, and reservation, with input
   blocks read exactly 0 and part PUTs exactly 0;
   `splitters_derive_from_retained_count` builds N = 65,537 rows (stride
   2) and forces G = 7, then asserts the retained count is exactly
   32,769, the sample is cut at `ceil(R / G)` = 4,682-key strides
   yielding exactly 6 splitters and 7 partitions (demonstrated failing
   first against the `ceil(S / G)` arithmetic, which yields 3 splitters
   on the same sample); and `resample_key_over_reservation_refuses`
   plants a single key wider than the whole reservation in a range
   forced to re-sample and asserts the typed refusal naming the key's
   byte length and the reservation, with part PUTs exactly 0 and inputs
   still live.
10. **The retention coverage gate defers on every defect, mixed-version**
    (`retention_coverage_gate_defects_defer` and
    `v1_sweep_fails_closed_on_v2_replica`). Writer: a coverage-aware (v2)
    build. Six otherwise-fully-expired buckets are constructed, five each
    carrying one defective record -- coverage absent on a v2 record;
    one-sided; reversed; out-of-range (a replica listed under an hour
    outside its own covered range); unknown (`format_version = 3`) -- and
    one control bucket covered by a valid v2 cross-hour record whose
    whole covered range is expired. Reader: the same build's sweep.
    Asserts tombstones written for the five defect buckets exactly 0;
    the per-case deferral counter exactly 1 for each of the five
    cases it constructs (asserted per case, not as a sum of 5, so a case
    silently falling through to another's branch fails; gate case 6, the
    orphaned replica, is pinned by acceptance 11's fault-injection test
    rather than re-planted here); the control
    record's covered range tombstones in one pass, tombstone count
    exactly its covered width. Mixed-version half: writer a v2 build
    publishing a cross-hour record with replicas; reader a v1-only
    sweep evaluating a covered bucket; asserts the typed
    `UnsupportedVersion` refusal fires exactly once and tombstones
    written exactly 0 -- the sweep fails loudly rather than skipping the
    record and proceeding. Demonstrated failing first against a stub
    sweep that drops unknown fields and applies the per-bucket test: the
    stub tombstones the covered bucket, and the zero-tombstone assertion
    catches exactly that.
11. **A partial cross-hour publication is invisible everywhere**
    (`cross_hour_partial_publication_stays_invisible`). Writer: a
    coverage-aware build publishing a three-hour L2 record over sealed
    inputs of known row count. `FaultStore` kills the publication after
    the FIRST replica write -- keyed to the PUT operation kind and the
    non-home replica's key substring, first occurrence -- and the test
    asserts the fault counter fired exactly once, so the injection is
    proven, per the repo's FaultStore rule. With only that orphan replica
    durable, asserts: a resolve over EVERY covered hour returns rows
    equal to the pre-publication baseline exactly (+-0 -- no hour treats
    the inputs as superseded, no hour loses or duplicates a row); the
    retention sweep over every covered bucket writes tombstones exactly 0
    and increments the case-6 (orphaned) deferral counter exactly 1 on
    the one bucket listing the orphan. Then a compactor retry over the
    same sealed inputs completes publication (remaining replicas, home
    last) and the test asserts: every covered hour's resolve now excludes
    the superseded inputs, rows still equal baseline exactly (+-0), and
    the replicas dedupe to one record by `input_set_hash`. Companion for
    the pre-flight (`tombstoned_covered_hour_blocks_home_publication`):
    with a retention tombstone planted on one covered hour before the
    retry, the retry returns the typed refusal and home-replica PUTs are
    exactly 0. Demonstrated failing first against a stub sweep that
    treats any replica as live coverage without confirming the home
    replica: on a fully-expired covered range holding only the orphan,
    the stub applies the extended expiry test, finds the range expired,
    and tombstones -- retiring inputs whose superseding record never
    became live -- and the tombstones-exactly-0 assertion catches exactly
    that.
12. **EventTime-leading clustering never touches a measured tenant without a
    pre-registered A/B.** This is a process gate on enabling the feature, not
    a unit test, and it is a precondition for any reported number. Before
    `clustering_key` resolves to EventTime for a tenant whose figures are
    reported anywhere, an A/B is registered on the tracking issue and then
    run: same binary, same tenant, same corpus, one snapshot before
    clustering and one after, with the expected direction and the pass band
    for every figure written down BEFORE any number exists. Statement
    coverage differs by which A/B this is, and the two must not be conflated.

    For the **`clickbench-v4` negative control**, coverage is q37 through q43
    inclusive and q43 is required rather than optional: it is the only statement
    in the class with a genuine time window
    (`EventDate BETWEEN 15900 AND 15901`) and therefore the only one expected to
    improve, so a control that drops it cannot show the mechanism working at
    all.

    For the **telemetry enablement A/B**, q37-q43 do not apply: those
    identifiers are defined solely by the ClickBench corpus, so requiring them
    of a telemetry workload would require statements that workload does not
    contain. That A/B names its own recent-window statements over
    organically-arriving data and registers their own metric directions, in the
    implementing ticket. q43's own registered directions, which differ BY METRIC and
    are not a single "improves": its two-day window is about 6.5% of a 31-day
    corpus, so
    - **surviving blocks RISE** toward roughly 6.5%, because clustering kills
      the `CounterID` arm here exactly as it does for q37-q42. **q43's own
      pre-clustering baseline must be measured and registered first: it is NOT
      the 144 of 17,731 figure**, which is q37-q42's. q43 carries extra arms
      (`EventDate >= 15900`, `EventDate <= 15901`, `IsRefresh = 0`,
      `DontCountHits = 0`), so its surviving-block count before clustering is
      its own quantity, and registering a band against q37-q42's baseline would
      measure from the wrong starting point;
    - **objects opened FALL**, from every object toward roughly the same 6.5%,
      because the two days become contiguous instead of scattered;
    - **data GETs per phase FALL** with objects opened, not with blocks.
    Registering "q43 improves" without splitting the metrics would pass on a
    blocks regression or fail on an objects win. The mechanism is working only
    if objects opened fall for q43 while q37-q42 show no object-level
    improvement; if q43's objects do not fall, the regression bought nothing. Per-statement figures: surviving blocks, objects opened, and data
    GETs per phase, each emitted exactly once. For `clickbench-v4` the
    pre-registered direction is fixed by Context's arithmetic and is a
    regression: q37-q42 surviving blocks rise from 144 toward 17,731 and
    objects opened do not fall, while q43 improves. **That run is a NEGATIVE
    CONTROL and is explicitly excluded from default-enablement evidence.** Its
    purpose is to confirm the mechanism is understood — a `clickbench-v4` A/B
    that did NOT regress q37-q42 would mean this ADR's Context is wrong and
    would itself be the finding. Evidence FOR enabling EventTime as a default
    must come from a telemetry-shaped workload (organic arrival order,
    recent-window queries), never from this corpus. This is what ADR-0849 means
    when it says clustering must not be justified by ClickBench: the corpus may
    be measured, and must not be used as the argument. Numbers that arrive
    before the registration are not a result (the repo's pre-registration
    rule), and enabling the key on a measured tenant without a registered
    A/B is a process violation of the same class as breaking decision 4's
    rollout gate. **Registration and execution are not sufficient: enabling
    EventTime as a default requires a telemetry A/B that PASSED its own
    pre-registered band.** A registered A/B that ran and missed is a reason not
    to enable, not a box ticked. The `clickbench-v4` negative control passing
    (that is, regressing as predicted) is necessary but never sufficient on its
    own.

## Alternatives considered

- **Cluster at ingest.** Rejected in decision 1: it raises ack latency by the
  clustering-window width and breaks the pinned-flush-identity retry contract
  and the ADR-0069 memory bound.
- **A compound `(ts, point-key)` sort to serve both classes.** Rejected in
  decision 5: the second dimension is scattered within each leading-key run, so
  it prunes nothing at object granularity; point lookups are an index problem
  (ADR-0049/#19), not a sort problem.
- **A segment-format bump to carry richer per-object stats.** Unnecessary
  (decision 6): the EventTime bounds the default path needs already live in the
  commit and part records, and an override key needs only additive proto
  fields. A frozen-format change would be a cost with no benefit here.
- **Record-level clustering bounds.** Rejected (decision 3):
  the catalog mints one `SegmentRef` per `CompactionPart`, and a record
  spanning two disjoint clustering-key ranges has no single min/max that is
  both correct and excludes anything; only per-part bounds can represent
  that.
- **An unpinned `input_set_hash` (today's definition, unchanged) plus a
  mid-flight clustering-configuration change.** Rejected (decision 3): the
  same input set under two different resolved configurations -- key name,
  declared type, encoding rules, batch partition, or part cap -- would try
  to publish two different `parts` bodies under the identical record key,
  which the commit-record protocol's split-brain rule crashes on -- and
  because the cause is a durable config change rather than a transient
  retry, the crash would loop rather than converge. Pinning only the key
  name closes one of those five holes and leaves the other four open.
- **A new order-preserving memcomparable byte encoding for all bound types**
  (sign-flipped big-endian integers, totalOrder-mapped floats, one `bytes`
  field, memcmp everywhere). Rejected (decision 3's eligibility subsection):
  it would be a second frozen per-type ordering in a codebase that already
  freezes one -- the SKIP_IDX `NumStat` bit-pattern vocabulary, shared with
  the query planner's `NumRangeArm` -- and two orderings that must agree
  forever is exactly the class of drift the existing comment "nothing
  re-implements per-type comparison" exists to prevent.
- **Leaving the global merge's cursor count to `MergeMemoryTracker`.**
  Rejected (decision 4): the tracker bounds accumulated heap but every live
  run needs its head resident, so cursor count times the per-cursor floor is
  a working-set minimum the tracker can observe but not reduce; a
  high-cardinality bucket becomes a deterministic crash-loop. The bound must
  cap the cursor count itself.
- **A multi-pass external merge (merge run groups into intermediate objects,
  then merge the intermediates) to bound cursors.** Rejected (decision 4) in
  favor of stream batching: intermediates cost a full extra write and read
  of the bucket through object storage and need a scratch key namespace and
  a cleanup path, while stream batching bounds cursors with zero extra I/O
  and, to first order, identical exclusion power (a window keeps the same
  fraction of parts either way, at the cost of up to G boundary parts).
- **Splitting a clustered part at merge time so it never spans a retention
  boundary, or a selective rewrite that trims an expiring L2 object.**
  Rejected (decision 7) in favor of deferral: both couple the compactor (or a
  new rewrite path) to the retention frontier for a benefit that only
  matters at the one hour range a corpus happens to straddle; deferral gets
  the same correctness with no new mechanism, at the cost of bounded
  over-retention ADR-0019 already accepts elsewhere.
- **Carrying the clustering-order revision only in the identity tuple's
  hashed tag.** Rejected (decision 3's identity subsection): the tag is
  hashed into `input_set_hash` and never stored, and ADR-0066's Class D
  names what a hashed-never-stored version does -- a silent split, not a
  decode error. A reader deciding whether to prune must be able to READ
  which rules produced a bound, so the version is a stored field on
  `CompactionRecord`, `CompactionPart`, and `SegmentRef`, single-sourced
  with the tag.
- **Falling through to `stream_blocks` when a compactor meets a clustering
  descriptor it does not understand.** Rejected (decision 4's run model and
  rollout subsection): the fall-through silently consumes
  `(key, ts)`-ordered input as ts-runs and publishes mis-ordered durable
  replacements, plus a forked `input_set_hash` that can expose duplicate
  rows on signals with no query-time dedup. Typed refusal plus the
  readers-before-writers gate (ADR-0066 decision 1) is loud, bounded, and
  convergent.
- **Spilling an oversized lexical partition to scratch objects instead of
  refusing.** Rejected (decision 4's override-key subsection): a spill path
  needs a scratch key namespace and a cleanup path, the same grounds on
  which the multi-pass external merge above is rejected; a typed refusal
  keeps the inputs live and costs clustering quality on one bucket, never
  availability or correctness.
- **Carrying L2 cross-hour coverage as additive fields on a single-bucket
  record, with or without a sweep look-back window.** Rejected (decision
  7): additive fields are silently dropped by pre-coverage readers, and a
  record stored under one bucket is invisible to every other covered
  bucket's per-bucket sweep -- two independent blindnesses, and a look-back
  window merely restates the first one in time (an old sweep does not know
  to look back). The reader-floor bump to `format_version = 2` plus one
  replica per covered hour makes the gate mechanical for old and new
  sweeps alike.
- **Independently live cross-hour replicas (each `CreateIfAbsent` write
  authoritative the moment it lands).** Rejected (decision 7's barrier
  paragraph): object storage has no multi-object atomic write, so a
  publication interrupted between replicas would leave covered hours
  disagreeing -- one hour treating the record's inputs as superseded
  while another cannot exclude them, and a replica-less expired hour free
  to tombstone inputs a later-completed record still claims. Making the
  home replica, written last, the single liveness bit turns "is this
  record published" into a one-object existence check every consumer
  answers identically, at the cost of one GET per discovered cross-hour
  record.
