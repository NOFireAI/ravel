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
instead of on time; the default remains EventTime because the measured pain is
narrow time windows. Which declared types are eligible as an override key,
and the canonical order each clusters under, are defined in decision 3's
eligibility subsection.

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
meaningful only at that granularity (`clustering_key` and
`clustering_key_type` in the identity subsection below, and decision 7's
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
function of the input set and this value), and the resolved part byte cap
(`max_l1_part_bytes` or the tier's equivalent). The tuple is serialized with
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
under two configs coexist exactly as two racing compactors already do today
under ADR-0018's
overlap-harmlessness rule, and whichever publishes second is superseded
through the ordinary supersession path the next time compaction runs, not
through a crash. Unclustered signals (metrics) keep the existing hash
definition byte-for-byte -- untouched, no migration -- because they never
resolve a `clustering_key` in the first place.

That still leaves: given a clustering bound on some `SegmentRef`, how does a
reader know which key, and which type's order, produced it? Answer:
`CompactionRecord` gains additive `clustering_key` and `clustering_key_type`
fields (the key is the sentinel for "none" on an unclustered signal, the
sentinel for EventTime, or the declared override column name; the type reuses
the `TypedAttrColumnType` vocabulary and is meaningful only for an override
key) recording the resolved values used for that record's merge, and
`CompactionPart` mirrors both onto each part alongside its bounds, so a
`SegmentRef` is self-describing: the resolver never has to re-derive the key
or its type from the CURRENT tenant config (which may have moved on since
this record was written) to know what column its own bounds mean or which
canonical order they were computed under. A resolver filtering a
narrow-window predicate on column `X` matches it only against a `SegmentRef`
whose own `clustering_key` names `X` AND whose `clustering_key_type` equals
the type the planner resolved the predicate under -- bounds encoded under a
since-changed declared type compare in a different order and prove nothing. A
`SegmentRef` produced under a different or since-changed key or type is read
as EventTime-only-bounded (or unbounded, see the fail-open rule below) for
that predicate, never assumed to also bound `X`.

#### Fail-open: missing, malformed, or unvalidated bounds

A clustering bound (the EventTime pair or an override-column
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
  composes with it.
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
  sequential run is what makes L2-over-clustered-L1 cost one pass. Fail-open
  on shape: an input whose clustering descriptor is absent or unknown is
  driven as stream-major. For an unclustered or time-clustered input the
  per-stream runs are ts-ascending and the merge's sorted-run precondition
  holds, so the cost is read amplification only. For an input that was in
  fact clustered by an override key, per-stream rows are `(key, ts)`-ordered
  -- not ts-runs -- and a merge that consumed them as ts-runs would emit
  imperfectly sorted output; that outcome is fail-open, not fail-broken:
  `conserve_exact` still holds (no row added or dropped), every emitted
  part's bounds are still computed from its own actual rows and stay
  truthful (merely wide), and exclusion stays safe. A missing descriptor
  costs clustering quality, never a wrong answer. The erasure rewrite of
  a clustered object reads it through the same sequential run, drops the
  matched rows, and re-emits in the same order; its rewrite record copies
  the input record's clustering descriptors, so a rewritten clustered part
  stays self-describing.

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
is a range-partitioned rewrite instead. The compactor unions the inputs'
SKIP_IDX NumStats for the key column into a coarse histogram, cuts key-range
partitions from it, and processes partitions in canonical key order:
each partition's rows are selected by streaming the inputs' candidate blocks
(`candidate_blocks` numeric arms for an `i64`/`f64`/`bool` key; every block
for a `str`/`bytes` key, which carries no NumStat), sorted in memory under
the `MergeMemoryTracker` bound by the full override comparator, and appended
to the same `PartSink`. A partition whose rows exceed the memory budget
splits recursively by key range; a partition that cannot split because it is
one single key value needs no sort at all -- rows sharing a key value order
by `(ts_ns, stream_ref)` per the comparator, per-stream runs are already
ts-sorted, so the run machinery above finishes that value under its existing
cursor bound. Partition boundaries are a deterministic function of the input
set and the resolved configuration, so the identity tuple of decision 3
already pins them. The cost shape is stated honestly: an override-key merge
re-reads any input block whose key range straddles partitions once per
partition that keeps rows from it -- and for a `str`/`bytes` key, with no
block stats to select on, it re-reads every block once per partition. That
read amplification is the price of clustering by a key that ingest order
does not correlate with; it is paid once per compaction, off the query and
ack paths, and an L2 re-merge of already-clustered inputs does not pay it
again (a clustered input is key-sorted, contributes one run, and the run
model applies).

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
- **Catalog record:** this ADR adds per-part bound and descriptor fields,
  per-record descriptor fields, and changes what `input_set_hash` is computed
  over for a clustering-eligible signal. Each is judged against the same test
  ADR-0066 decision 4 sets: does an old reader that does not know the new
  field still decode the record correctly and reach the same answer for the
  bytes it does understand?
  - The additive proto fields: on `CompactionPart`, the bound pairs
    (`clustering_min_bits`/`clustering_max_bits`,
    `clustering_min_bytes`/`clustering_max_bytes`) and the descriptors
    (`clustering_key`, `clustering_key_type`); on `CompactionRecord`, the
    same two descriptors and decision 7's
    `covered_hour_min`/`covered_hour_max`; on `TenantConfigRecord`,
    `clustering_key`. All are the same class as the `enc`/`config` histories
    (docs/catalog-and-mvcc.md) and as this ADR's own EventTime precedent
    (`CompactionPart.min_event_ts_ns`/`max_event_ts_ns` are already additive
    fields on the same message, added without a bump when ADR-0018 shipped
    compaction). An old reader ignores an unknown field on a message it
    already knows how to decode and reads the object -- which is exactly the
    fail-open direction; no bump, no migration. One caveat inherited from
    ADR-0090, not created here: `clustering_key_type` reuses the
    `TypedAttrColumnType` vocabulary as a field VALUE, not a new enum case,
    so it adds no new decode-refusal path; only ADR-0101's own `f64` enum
    addition carries that rollout ordering, on ADR-0101's schedule.
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

Stated plainly: no segment-format bump, no catalog-schema bump, and no
ADR-0066 decision 4 migration class for either path. The per-part bounds and
every descriptor stay additive because each is an unknown-field-tolerant
addition to a message old readers already decode, with fail-open as the
old-reader behavior. The identity change stays out of the bump/migration
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
   targeted test constructs a part whose clustering bounds are absent, a
   second case where the populated pair does not match the part's
   `clustering_key_type` (the wrong-pair malformation), and a third where
   the part's recorded type differs from the type the predicate was planned
   under, and asserts the resolve path issues a data GET for the part under
   a narrow-window predicate on that column in all three cases -- the
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
