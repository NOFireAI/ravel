# ADR-0020: Metric index: catalog snapshots as the commit index, async fold, name postings gated

Status: Accepted (2026-07-27). Plan, formats, and phased tickets:
docs/metric-index-plan.md. This ADR builds the "second" half of ADR-0003
(immutable catalog snapshots); it does not revisit that accepted
direction.

## Context

Phase 1 discovery is pure listing (ADR-0003 option 2): every
`Catalog::resolve` issues one LIST per (shard, ingest-hour bucket) in the
window `[range.start - max_ingest_lag, now + clock_skew_allowance]` and
GETs every commit record it has not cached
(crates/ravel-catalog/src/catalog.rs). PROGRESS.md names the bottleneck:
this will not scale past ~10^4 commits per (tenant, shard, hour) bucket,
and it must be fixed before Phase 2 load tests. The cost has two axes:
commit density per bucket (paginated LISTs plus per-record GETs) and
window width (a 24 h query over 4 shards lists ~108 buckets because the
window's upper bound is anchored at `now`).

"Metric index" could mean (a) an index from commit history to segment
sets (discovery), (b) an index from matchers to segments (pruning), or
(c) a label-name/value index for the labels endpoints. The stated
bottleneck is (a). (b) and (c) become visible only after (a) is fixed,
and their cost model depends on the L0->L1 compaction plan (being
designed in parallel, not merged at the time of this ADR).

## Alternatives

1. Eager index update on the ingest path (writer updates a catalog
   object per commit). Adds a third dependent write and a CAS hot spot
   to the strict-ack path, violating the ack latency budget
   (docs/consistency-model.md); at 10^4 commits/hour the HEAD object
   becomes a contention point with no coordinator to serialize it.
   Rejected.
2. Merge-on-read: the resolver folds and publishes a snapshot as a side
   effect of a query. Puts writes and CAS races on the query path and
   makes query tail latency depend on fold size. Rejected.
3. Tighten the listing window to the event-skew bounds
   (`[range.start - max_future_skew, range.end + max_ingest_lag + ...]`)
   instead of indexing. Sound (admission bounds imply it) and it would
   help old-range queries, but it does nothing for commit density in
   recent buckets, which is the named 10^4 wall. Insufficient alone;
   unnecessary once snapshots exist. Rejected.
4. Immutable per-hour manifest objects (fold each sealed bucket into one
   object, no HEAD). Create-if-absent only, no CAS, but long-range
   queries pay one GET per (shard, hour) forever, and there is no single
   place to fold compaction/retention transactions into later. Rejected
   as the primary mechanism; the sealed-hour idea it rests on is kept.
5. A full inverted label index (Prometheus-TSDB-style postings for every
   label pair) built now. Build cost is a full catalog decode of every
   segment, size is unbounded on high-cardinality tenants, and no
   measurement yet shows the labels endpoints are the bottleneck.
   Rejected now; a narrow metric-name postings index is planned but
   gated on Phase 2 load-test measurements.
6. External index service or database: forbidden (object storage is the
   only durable backend; a catalog service must be an optimization,
   never a durability dependency, ADR-0003).

## Decision

Build the ADR-0003 snapshot mechanism as the metric index, scoped to
commit discovery first:

- Per (tenant, signal): immutable snapshot part objects under
  `t/<tenant_hash>/catalog/<signal>/snap/`, and one mutable HEAD object
  `t/<tenant_hash>/catalog/<signal>/HEAD` updated by CAS
  (etag/generation precondition, already in the store contract). HEAD
  names the current snapshot parts, their blake3 hashes, and the
  watermark. This amends the reserved `(later)` rows of the key layout
  in docs/catalog-and-mvcc.md and resolves the shape difference between
  ADR-0003's `HEAD/<shard-group>` and that doc's flat `HEAD`: one HEAD
  per (tenant, signal), with multi-part snapshots as the sharding
  escape hatch (parts are listed in HEAD, so splitting never changes
  the key layout again).
- Sealed-hour watermark: an ingest-hour bucket H is sealed once
  `now >= end(H) + max_flush_lifetime + clock_skew_allowance +
  fold_safety_margin`. The GC interlock (ADR-0010 §11) already forbids
  publishing a commit record after `max_flush_lifetime`, so a sealed
  bucket's commit set is immutable; folding it once is folding it
  forever. Snapshots only ever cover sealed hours.
- Async fold, never on the ingest or query path: a catalog worker
  (ravel-server background task; also a ravel-cli subcommand) folds
  previous parts plus LISTs of newly sealed hours into a new part,
  PUTs it create-if-absent (parts are content-addressed), and
  CAS-swaps HEAD. Any number of folders may race; CAS picks one, losers
  retry or stand down. Ack semantics, visibility atomicity, and
  visibility latency are untouched: the commit record remains the sole
  visibility event.
- Resolution: read HEAD -> parts for window hours <= watermark, LIST
  only hours > watermark (a bounded suffix of ~3-4 buckets per shard
  regardless of range width or history depth), min-token exact GETs
  unchanged. Missing, stale, or corrupt HEAD/parts degrade to Phase 1
  full listing: the index is a pure optimization and can be rebuilt
  from commit records at any time. Commit records are never deleted by
  this design; deleting them would promote snapshots to source of
  truth and requires its own ADR.
- Second, measurement-gated scope: a metric-name postings object
  (distinct `__name__` values per snapshot entry) built by the same
  folder from segment catalogs, used to prune snapshot-sourced segments
  for name-equality selectors. Exact sets, not sketches: pruning must
  never introduce approximation into query results. It ships only if
  Phase 2 load tests show segment fan-out dominating after snapshots
  land.

New persistent formats (snapshot part envelope, HEAD record, postings
envelope; one new protobuf file proto/ravel/catalog.proto) follow the
format-change procedure: explicit version bytes, checksum coverage
review, corrupt-input property tests, inspector support. Full grammars
in the plan.

## Consequences

- Resolve cost for a query becomes O(open-window buckets) LISTs plus
  one HEAD GET and cached part GETs, independent of tenant history
  depth; the 10^4-commits-per-bucket wall moves from every query to a
  once-per-fold cost.
- A new mutable object class (HEAD) exists, but correctness never
  depends on it: every failure mode falls back to listing. Staleness is
  a performance property (a folder down for D hours widens the listed
  suffix by D buckets), never a correctness property.
- The seal rule adds a clock assumption on the folder bounded by
  fold_safety_margin (default 15 m), the same class as the existing
  writer clock_skew_allowance; violation is detectable by a re-listing
  audit (`ravel-cli catalog verify`) and repairable by rebuild, since
  commit records remain the ground truth.
- Compaction and retention (planned in parallel, not yet merged) must
  publish durable, listable transaction records; the folder applies
  them so snapshots stop referencing compacted-away or retired
  segments, and GC must treat reachability from HEAD-referenced
  snapshots (within the protection horizon) as a delete blocker. The
  plan pins this contract; the snapshot entry format carries a `level`
  field from day one so L1 outputs need no format bump.
- Old snapshot parts and CAS-loser parts become garbage; they are
  GC-eligible once unreferenced by HEAD beyond the protection horizon
  (folded into the GC track's rules).
