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

- **Logs, spans, alerts, audit (RLOG/RSPAN):** lead with EventTime. The merge
  sorts by `(ts, <existing tiebreak>)` for a clustering-eligible (tenant,
  signal) instead of `(stream_ref, ts)`. Stream identity stays a lower-order
  tiebreak so the STREAM_DIR remap and the cross-object stream-identity check
  (rlog.rs) are unchanged; only the leading sort column moves.
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
Both halves are required and must be stated separately:

- **The clustering half** (this ADR's new work): compaction emits parts whose
  per-part span of the clustering key is narrow, because the merge is sorted by
  that key and parts are cut on its boundaries (decision 4).
- **The exclusion half** (where the per-object min/max lives, and how the
  planner reads it):
  - **For the EventTime key, the exclusion half already exists.** The per-part
    `event bounds` on the L1 `SegmentRef`, reconstructed from the compaction
    record and never trusted from a stored string (ADR-0010 §7), are the
    per-object min/max of the clustering key. Resolve step 4 filters on them
    before any data GET. Today's block stats also live *inside* the object
    (SKIP_IDX), which is exactly why discovering a block has nothing still
    costs a request; the commit/part-record bounds are the copy that lives in
    the catalog and costs no data GET. So for EventTime the two halves reduce
    to one new thing: clustering. No new catalog field.
  - **For a non-EventTime clustering key** (the per-tenant override on a
    declared column), the catalog carries no per-object min/max of that column
    today; it lives only in the object's per-block column stats, so discovering
    an object has nothing costs a GET. That key needs the exclusion half built:
    an additive `clustering_min`/`clustering_max` pair (typed per the column's
    declared type, ADR-0101 vocabulary) on the compaction record and mirrored
    onto the part `SegmentRef`, populated by the compactor from the merged
    rows. The planner reads it in resolve step 4 beside the event-bound filter.
    This is an additive proto field, not a segment-format change (decision 6).

Say it plainly: for the default (time) key this ADR ships clustering alone and
reuses existing bounds; for an override key it ships clustering plus one
additive catalog field. Neither reads a data object to exclude one.

### 4. Interaction with the compaction tiers

- **L1 compaction (ADR-0018, `compact.rs`, `max_l1_part_bytes` default 256
  MiB, parts cut on stream/series boundaries at the byte cap):** modified, not
  replaced. The exact-conservation contract (`conserve_exact`, rows in = rows
  out) is unchanged; only the merge sort order and the part-cut predicate
  change. A part is still flushed at the byte cap; clustering only guarantees
  that a byte-cap-cut part covers a contiguous clustering-key range, because
  the merge feeding it is sorted by that key. Within one ingest hour this
  narrows per-part bounds for any hour that holds more than one part.
- **#593 (L2 cross-hour exact compaction):** composes, and is where clustering
  pays off most. A bulk-imported corpus puts the whole EventTime range into a
  few ingest hours, so within-hour L1 clustering alone cannot separate hours
  that do not exist as a partition; L2 merges across hours and is the tier that
  produces objects whose EventTime bounds each cover a narrow, corpus-wide time
  slice. L2 applies the identical sorted-merge-and-cut; it is the same rewrite
  primitive over a wider input set. This ADR specifies the output invariant; it
  does not subsume #593, it requires #593 to reach full effect on a
  time-uncorrelated ingest order and composes with it.
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
- **Catalog record:** for the EventTime default, no field is added (existing
  event bounds serve). For a per-tenant override key, the additive
  `clustering_min/max` on the compaction record and the `clustering_key` on
  `TenantConfigRecord` are additive proto fields on additive-by-contract
  records (the same class as the `enc`/`config` histories in
  docs/catalog-and-mvcc.md); an old reader ignores them and reads the object,
  so they need no bump and no migration either.

Stated plainly: the default path needs no format version bump and no migration
class; the override path needs two additive proto fields and still no bump.

### 7. Erasure and retention

Both walk objects, and both key on the ingest-hour bucket, which clustering
within a bucket does not change:

- **Retention (ADR-0019, age-based):** unchanged for within-hour L1 clustering
  -- a tombstone still retires a whole ingest hour, and the objects it retires
  are that hour's clustered parts. It interacts with clustering only through
  **L2 cross-hour** (#593): an L2 object spans an ingest-hour *range*, so the
  retention frontier must key on the object's newest covered hour, not a single
  hour. That is #593's own concern (a cross-hour object already breaks the
  one-object-one-hour assumption); this ADR adds nothing to it beyond noting
  that a clustered L2 object's covered-hour set must be recorded on its record
  so retention and the retention-frontier reconcile (docs/catalog-and-mvcc.md)
  see every hour it holds.
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
