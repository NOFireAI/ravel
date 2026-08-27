# ADR-0815: Clustered compaction and object-level pruning

Status: Proposed.

The number is issue #815's, the ticket that produced this ADR, per the
README's issue-number rule.

## Context

On the ClickBench reference tenant `clickbench-v4` (8,424 objects, 17,731
blocks, 11.116 GB, 8 shards, RLOG v4), the narrow-time-window statement class
q37-q43 already prunes well at block granularity and terribly at object
granularity:

| figure | value |
|---|---|
| blocks kept after pruning | 144 of 17,731 (0.8%) |
| bytes read | 2.1 GB of 11.1 GB |
| objects opened | 8,424 of 8,424 (100%) |
| data GETs | ~15,800 (~1.88 per object) |
| wall | 40-46 s |
| competitor wall, same shapes | 0.06-0.09 s |

Block pruning works; object pruning does not exist. If the surviving 0.8% of
blocks were clustered they would occupy roughly 0.8% of objects (~67); touching
100% of objects to keep 0.8% of blocks means every object holds a few rows
matching any narrow window. The corpus explains it: `hits.parquet` is
CounterID-sorted (#560) and EventTime does not correlate with CounterID, so
every flushed batch spans nearly the whole EventTime range, and every object's
catalog-level `[min_event_ts, max_event_ts]` is therefore nearly the whole
range. The resolve-time event-overlap filter (docs/catalog-and-mvcc.md,
"Snapshot resolution" step 4, and the per-part event bounds on an L1
`SegmentRef`) already excludes an object whose event range misses the query
window without a data GET, but with every object's bounds spanning the whole
range it excludes nothing.

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
change to *which order* that merge sorts in and *where* it cuts parts, not a
new pass and not new I/O. It is stateless and idempotent exactly as compaction
is today: content-addressed part keys and a `CreateIfAbsent` record mean a
re-run converges.

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
instead of on time; the default remains EventTime because the measured pain is
narrow time windows.

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
should have excluded. Every field this decision adds therefore lives on
`CompactionPart` and is copied to its `SegmentRef` exactly as today's
per-part `min_event_ts_ns`/`max_event_ts_ns` already are -- `SegmentRef`
carries that pair flat on the struct, not nested under the L1 variant, and
`CompactionPart` already carries the same pair per part
(proto/ravel/commit.proto). No field this ADR adds is ever read or written at
record granularity.

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
    half built: two additive fields on `CompactionPart` --
    `clustering_min`/`clustering_max` (typed per the column's declared type,
    ADR-0101 vocabulary) -- populated by the compactor from that part's own
    merged rows and copied onto the part's `SegmentRef` the same way the
    EventTime pair already is. The planner reads them in resolve step 4
    beside the event-bound filter. This is an additive proto field on
    `CompactionPart`, not a segment-format change (decision 6).

Say it plainly: for the default (time) key this ADR ships clustering alone and
reuses existing per-part bounds; for an override key it ships clustering plus
two additive per-part catalog fields. Neither reads a data object to exclude
one, and neither is ever read or written at record granularity.

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

Decision: for a clustering-eligible (tenant, signal), `input_set_hash` is
computed over the sorted `inputs` list domain-separated by the resolved
`clustering_key` value that produced this attempt's parts -- the same
domain-separation idiom the ADR-0064 rewrite record's `input_set_hash`
already uses to stay distinct from the compaction record's
(docs/catalog-and-mvcc.md: "a distinct domain from the compaction
`input_set_hash` so the two can never collide"). A clustering-key change
between attempts therefore produces a genuinely different record key, never a
body collision on the same key: two attempts under two configs coexist
exactly as two racing compactors already do today under ADR-0018's
overlap-harmlessness rule, and whichever publishes second is superseded
through the ordinary supersession path the next time compaction runs, not
through a crash. Unclustered signals (metrics) keep the existing hash
definition byte-for-byte -- untouched, no migration -- because they never
resolve a `clustering_key` in the first place.

That still leaves: given a `clustering_min`/`clustering_max` on some
`SegmentRef`, how does a reader know which key produced it? Answer:
`CompactionRecord` gains an additive `clustering_key` field (the sentinel for
"none" on an unclustered signal, the sentinel for EventTime, or the declared
override column name) recording the resolved value used for that record's
merge, and `CompactionPart` mirrors it onto each part alongside its bounds,
so a `SegmentRef` is self-describing: the resolver never has to re-derive the
key from the CURRENT tenant config (which may have moved on since this
record was written) to know what column its own bounds mean. A resolver
filtering a narrow-window predicate on column `X` matches it only against a
`SegmentRef` whose own `clustering_key` names `X`; a `SegmentRef` produced
under a different or since-changed key is read as EventTime-only-bounded (or
unbounded, see the fail-open rule below) for that predicate, never assumed to
also bound `X`.

#### Fail-open: missing, malformed, or unvalidated bounds

`clustering_min`/`clustering_max` (the EventTime pair or an override-column
pair) is a pruning hint, never a correctness gate. If a `SegmentRef`'s
clustering bounds for the predicate's key are absent, fail to decode, or were
never validated as having been computed from that part's own final merged
rows, the resolver MUST treat the bound as unknown and include the part:
read the object. The failure mode is **fail-open (read), never fail-closed
(skip)** -- skipping on an unknown bound is silent data loss (a query returns
fewer rows than exist, with no error), not a slow query, and this ADR's
entire value proposition depends on exclusion being provably safe, so an
unsafe exclusion is strictly worse than no exclusion at all.

- **Null handling.** If any row the compactor merges into a part has a null
  in the clustering column, the compactor does not populate that part's
  `clustering_min`/`clustering_max` at all (leaves the field absent), rather
  than encoding a sentinel or silently excluding the null rows from the
  computed range. An absent field is unknown by the rule above, so the part
  is always read whenever it contains a row the compactor could not bound.
  This applies only to the override-column pair; the EventTime pair has no
  null case (EventTime is a required field on every admitted row, ADR-0010
  §8's admission bound).
- **Validated means computed from this part's own output rows.** A
  `clustering_min`/`clustering_max` value is populated only at the moment the
  compactor writes the part it describes, directly from the rows that landed
  in it. Nothing ever carries a bound forward from an input or from a
  different part, and there is no code path that populates the field without
  computing it fresh; "validated" and "present" are therefore the same
  condition by construction, and there is no partially-trusted intermediate
  state to define semantics for beyond "absent, therefore unknown, therefore
  read."

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
  record can be the global minimum at any step: the concurrently open
  decode-unit count grows from O(inputs) to O(inputs x streams active in the
  bucket). `MergeMemoryTracker` already bounds total accumulated heap
  regardless of cursor count, so this is not unbounded, but it is a real
  change in shape from today's bound and must be sized against a tenant's
  actual per-hour stream cardinality at implementation time, not assumed away
  by the tracker's existing ceiling.

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
  composes with it.
- **#118 (rollup/downsampling tier):** orthogonal. Rollup is a lossy
  pre-aggregation that reduces row count; clustering is an exact reordering that
  preserves rows. A rollup tier's outputs can themselves be clustered by the
  same rule. No subsumption, no conflict.

Summary: modifies L1's ordering choice, composes with #593 (its natural home)
and #118, subsumes none, conflicts with none.

### 5. The conflicting-key problem, stated honestly

A sort key leads with exactly one dimension. Time clustering answers the narrow
EventTime window (q37-q43). A high-cardinality point lookup (`WHERE UserID =
c`, q20) wants the surviving rows gathered by UserID, which a time-leading sort
scatters across every part. A compound key `(ts, UserID)` does not fix this:
within each contiguous ts run UserID is still spread across the run's whole
value domain, so a UserID predicate prunes nothing at object granularity; and
`(UserID, ts)` destroys the time clustering the measured statements need. One
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
  (rlog.rs re-blocks through a fresh `RlogWriter`). This is a compaction
  output-choice change, not a page-grammar or trailer change, so it is not a
  migration class under ADR-0066 decision 4: nothing on an existing object
  changes meaning, old objects stay readable and are re-clustered
  opportunistically through the normal rewrite-on-touch force, and there is no
  version floor to raise.
- **Catalog record:** finding 1 moves the bounds from `CompactionRecord` to
  `CompactionPart`; finding 2 adds `clustering_key` to `CompactionRecord` and
  changes what `input_set_hash` is computed over for a clustering-eligible
  signal. Each is judged against the same test ADR-0066 decision 4 sets: does
  an old reader that does not know the new field still decode the record
  correctly and reach the same answer for the bytes it does understand?
  - `CompactionPart.clustering_min`/`clustering_max`,
    `CompactionPart.clustering_key`, `CompactionRecord.clustering_key`, and
    `TenantConfigRecord.clustering_key`: additive proto fields, the same
    class as the `enc`/`config` histories (docs/catalog-and-mvcc.md) and as
    this ADR's own EventTime precedent (`CompactionPart.min_event_ts_ns`/
    `max_event_ts_ns` are already additive fields on the same message, added
    without a bump when ADR-0018 shipped compaction). An old reader ignores
    an unknown field on a message it already knows how to decode and reads
    the object; no bump, no migration.
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
    additive bucket above. It needs no reader-visible migration (nothing
    stored changes shape, and no existing record is reinterpreted), so it
    does not trigger ADR-0066 decision 4's migration class, but it is a
    one-way switch per tenant: once a tenant's config carries a
    `clustering_key`, its compaction records key differently for as long as
    clustering stays on.

Stated plainly: no segment-format bump, no catalog-schema bump, and no
ADR-0066 decision 4 migration class for either path. Finding 1 stays additive
because it moves an already-additive field to a sibling message inside the
same additive-by-contract record. Finding 2 stays out of the bump/migration
system entirely because it is opt-in per tenant and touches no wire shape --
but it is a real, one-way change to a frozen derivation for that tenant, and
this ADR states that plainly rather than folding it into "just another
additive field."

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

  `CompactionRecord` gains an additive `covered_hour_min`/`covered_hour_max`
  pair (u32, unix hours, same encoding as `ingest_hour_bucket`) recording the
  full range of ingest hours whose rows an L2 record's parts contain; for an
  L1 record both equal the single `ingest_hour_bucket` it already carries, so
  no L1 behavior or reader changes. ADR-0019 decision 1's expiry test --
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

1. **Objects opened for a narrow-window statement falls by at least an order of
   magnitude.** Hard gate: q37-q43 opens `<= 842` objects (an order of
   magnitude below the measured 8,424). Pre-registered expectation: `40-150`
   objects. Reasoning: 144 surviving blocks are contiguous in EventTime once
   clustered; at the corpus's block density (17,731 blocks / 8,424 objects =
   2.1 blocks/object), preserved under the same block size and byte cap, 144
   blocks occupy ~68 objects, plus at most a couple of boundary parts. With
   256-MiB parts the count is lower still. A result in `(150, 842]` passes the
   gate but is a pre-registration miss to investigate (clustering weaker than
   the density argument predicts, e.g. L2 has not run and the window still
   spans multiple within-hour part sets). Stated as an exact object count from
   the resolve accounting, never as a wall-clock improvement.
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
   targeted test constructs a part whose `clustering_min`/`clustering_max` is
   absent, and a second case where it is present but fails to decode against
   its declared type, and asserts the resolve path issues a data GET for the
   part under a narrow-window predicate on that column in both cases -- the
   fail-open branch actually executing, not merely present in code. The test
   is demonstrated failing first: run against a deliberately fail-closed stub
   (one that skips the part when its bound cannot be read) and confirm the
   GET-count assertion fails there, before trusting the same assertion to
   guard the real implementation.

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
- **Record-level `clustering_min`/`clustering_max`.** Rejected (decision 3):
  the catalog mints one `SegmentRef` per `CompactionPart`, and a record
  spanning two disjoint clustering-key ranges has no single min/max that is
  both correct and excludes anything; only per-part bounds can represent
  that.
- **An unpinned `input_set_hash` (today's definition, unchanged) plus a
  mid-flight clustering-key change.** Rejected (decision 3): the same input
  set under two different clustering-key values would try to publish two
  different `parts` bodies under the identical record key, which the
  commit-record protocol's split-brain rule crashes on -- and because the
  cause is a durable config change rather than a transient retry, the crash
  would loop rather than converge.
- **Splitting a clustered part at merge time so it never spans a retention
  boundary, or a selective rewrite that trims an expiring L2 object.**
  Rejected (decision 7) in favor of deferral: both couple the compactor (or a
  new rewrite path) to the retention frontier for a benefit that only
  matters at the one hour range a corpus happens to straddle; deferral gets
  the same correctness with no new mechanism, at the cost of bounded
  over-retention ADR-0019 already accepts elsewhere.
