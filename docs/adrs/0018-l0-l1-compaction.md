# ADR-0018: L0 to L1 compaction: verbatim rewrite of sealed ingest-hour buckets

Status: Accepted

Amended by ADR-0026 (v5 replaces v4 as the compaction output) and
ADR-0027 (v4 support removed pre-release). The compaction design itself
stands. This ADR records the decision; docs/consistency-model.md,
docs/catalog-and-mvcc.md, and docs/segment-format.md remain authoritative
as written until their amendments land. Nothing in this ADR changes any
stored byte today.

**Amended by ADR-0092 (run-merged L1, RSEG v7).** This ADR's verbatim
run-preservation decision -- an L1 object holds one run per input segment per
series, page bytes copied unchanged -- is reversed for the metrics path.
Since ADR-0092 (issue #315) L1 compaction decodes every contributing run,
merges the samples in timestamp order, and re-encodes one run per series,
carrying each sample's dedup key in the RSEG v7 per-sample provenance columns.
The exactness property this ADR relies on (a snapshot with an L1 part plus any
subset of its inputs answers identically to the pre-compaction snapshot) is
preserved: the merged run reproduces the same candidate multiset with the same
priorities, proven by
`crates/ravel-query/tests/differential_compaction.rs`. Part splitting also
moves from predicted input bytes to encoded output bytes (ADR-0092 decision 3),
since re-encoding decouples output size from input size. The overlap-harmlessness
and non-atomic-swap reasoning below still holds; only the "page bytes copied
verbatim" mechanism is superseded.

## Context

Phase 1 never deletes or rewrites anything. Every flush leaves one L0
object and one commit record forever. At one flush per second per shard
that is 3,600 objects per (tenant, shard, hour) and 86,400 per day;
listing-based discovery is known to be unscalable past ~10^4 commits
per bucket, and the query budget (max_segments = 1024,
docs/query-engine.md) makes an hour of such data unqueryable outright.
ADR-0027 (RSEG v2) additionally accepted a permanent dual-reader burden
"until a compactor exists". This ADR is that compactor.

This is the first rewrite path and the first deletion trigger beyond the
specified-but-unbuilt orphan GC. The invariants that must survive it:

- Objects stay immutable. Compaction writes new objects and eventually
  deletes superseded ones; it never modifies any object in place.
- Object storage remains the only durable backend. The compactor is a
  disposable process with no local state and no recovery path that reads
  anything a process wrote locally.
- Exact semantics: query results are bit-identical before, during, and
  after a compaction, including duplicate-timestamp resolution, NaN
  payloads, and -0.0.
- Commit-record visibility atomicity and read-your-write tokens
  (ADR-0002, ADR-0010 §2) keep working.
- Frozen contracts change only via the format-change procedure: explicit
  version bump, additive protos, documented key-layout extension.

## The correctness core

Cross-segment duplicate resolution (ADR-0010 §5; implemented in
crates/ravel-query/src/engine.rs `is_greater` and `merge_series_runs`)
orders candidates by `(created_unix_ns, writer_epoch, writer_seq,
in_page_index)`, greatest wins, with the raw f64 bit pattern as the final
tiebreak. The first three fields come from the commit record of the
segment the sample was read from; the fourth is the sample's position in
its series' page.

Any design that resolves duplicates at compaction time must preserve the
winning sample's original priority tuple, because duplicates of the same
(series_id, event_ts) can still arrive after compaction: admission allows
event time up to max_ingest_lag behind ingest time, so a client retry
lands in a later ingest-hour bucket, and `created_unix_ns` is a writer
clock, so a later arrival can carry an earlier or later timestamp than
any input. If a compacted sample carries a synthesized priority (for
example the compaction record's own `created_unix_ns`), the merge between
the L1 sample and a late duplicate can pick a different winner than the
merge between the original L0s and that duplicate would have. That is a
silent exactness violation, and it is why "dedup during compaction" is
not almost-free even though the merge code already exists.

The design below therefore does not deduplicate at rest at all. L1
preserves every input sample with its exact priority tuple. That buys the
property everything else in this ADR leans on:

**Overlap harmlessness.** An L1 object's content is the exact multiset of
its inputs' samples with identical dedup priorities. Therefore a snapshot
that contains the L1 output plus any subset of its inputs is
query-equivalent to the pre-compaction snapshot: the query-time merge
collapses the duplicated candidates to the same winners. The only
inconsistent state is *losing* both sides. Swap atomicity collapses from
"readers must never see a mixed state" to a single ordering rule: the L1
record must be durable before any input record is removed.

## Alternatives

1. Dedup on compact with synthesized provenance (compactor identity or
   record timestamp as the priority). Rejected: inexact under late
   duplicates and writer clock skew, per the scenario above.
2. Dedup on compact with per-sample provenance columns (store the winning
   `(created_unix_ns, epoch, seq, in_page_index)` per sample). Exact, but
   ~4 varint columns per sample dwarf the sample data itself at typical
   2-sample runs, and the query fetcher's provenance model changes from
   per-segment to per-sample. Rejected for L1; recorded as the only known
   exact path for a future run-merging L2, where the tradeoff may invert.
3. Single merged run per series without dedup (concatenate and re-sort
   samples into one page pair). Rejected: same per-sample provenance
   problem as option 2, plus it re-encodes pages, forfeiting verbatim
   byte preservation and its audit properties.
4. Verbatim run preservation (chosen). L1 is a re-layout, not a rewrite,
   of its inputs: per series, one run per input segment, page bytes
   copied verbatim, per-run provenance copied from the input commit
   records into the output catalog. Compaction never decodes or
   re-encodes a sample. Page crcs (which bind series_id, enc, comp, and
   payload, ADR-0010 §4) remain valid bytes-for-bytes across the copy, so
   corruption in flight fails closed on the same checks readers already
   run.

## Decision

1. **Unit of work.** One sealed bucket: (tenant, signal, shard,
   ingest_hour). A bucket is sealed when
   `now >= bucket_end + max_flush_lifetime + clock_skew_allowance`. The
   ADR-0010 §11 writer interlock (a flush older than max_flush_lifetime
   is abandoned and never published) is what makes a sealed bucket's
   commit-record set immutable, so one strongly consistent LIST of the
   bucket is a complete, stable input set. Trigger: sealed and L0 record
   count >= `min_compaction_inputs` (default 2; 1 is legal config for v1
   retirement campaigns). Compaction across multiple hours (L2) is out of
   scope and gets its own ADR.
2. **Output format: RSEG v4.** Trailer version 4, magic unchanged. Page
   format, encodings, checksums, and the suffix protocol are unchanged
   from v1/v2/v3. The v2 catalog (SERIES_IDS + SERIES_META) is extended so
   a series holds an ordered list of runs, each run carrying its
   provenance `(created_unix_ns, writer_epoch, writer_seq)` and its own
   page pair (or HIST_PAGES pair, for histogram series) and event-time
   bounds. Inputs may be RSEG v1, v2, or v3; their page bytes are copied
   verbatim regardless of input version (the page grammar is identical
   across versions, and v3's HIST_PAGES bytes are copied as an opaque
   per-run blob, never re-encoded), and raw-f64 alignment (ADR-0027)
   is applied to the output via the existing gap columns. Trailer
   version 3 was already claimed by ADR-0017's native-histogram writer
   before this ADR's format work landed; version 4 is the resolution.
   The format-change
   procedure applies in full: spec amendment, checksum coverage review,
   fuzz and property coverage over all four versions, inspector support.
3. **Output partitioning.** 1..N part objects per bucket, split by
   disjoint series-id ranges when a size cap (`max_l1_part_bytes`) would
   be exceeded. As shipped, each part is assembled whole in memory and
   written with a single `CreateIfAbsent` PUT (crates/ravel-maintain/
   src/build.rs `put_part`); no multipart method exists on the
   object-store trait, so streaming/multipart assembly of large parts is
   deferred. The maintain-mode capability gate does add
   `multipart` to its required set (crates/ravel-object-store/src/lib.rs),
   but no shipped backend reports that capability (MemoryStore and S3 both
   report `multipart: false`), so the gate as written is a future
   requirement, not a satisfied one.
4. **Publication.** New protobuf message `CompactionRecord` (additive,
   proto/ravel/commit.proto): identity fields, ingest_hour_bucket,
   level = 1, the full input identity list [(writer_id, epoch, seq)],
   `input_set_hash` = blake3 over the sorted input identities, per-part
   entries (part_index, series-id range, content_hash, size, counts,
   event-time bounds, segment_format_version = 4), created_unix_ns.
   Key layout extension (documented in catalog-and-mvcc.md):

   ```
   t/<th>/m/l1/<shard>/<hour>/<input_set_hash16>.<part:04>.<hash16>.rseg
   t/<th>/m/c/<shard>/<hour>/l1.<input_set_hash16>.cmt
   ```

   The record lives in the same prefix as L0 commit records so the
   existing one LIST per bucket discovers both. Sequence: PUT every part
   with CreateIfAbsent (content-addressed keys), then PUT the record with
   CreateIfAbsent. On record `AlreadyExists`: GET it; equal
   input_set_hash means a racing compactor won and this run's parts
   become orphans (success, verify the winner's parts exist via HEAD and
   re-PUT any missing, which CreateIfAbsent makes safe); different
   input_set_hash is an invariant breach (a sealed bucket cannot yield
   two input sets): alarm loudly, delete nothing. Correctness never
   depends on deterministic output bytes; determinism (canonical input,
   series, and run ordering) is still specified so that racing compactors
   usually converge on identical part keys.
5. **Resolver rule.** When a bucket's listing contains a compaction
   record: include its parts (event-range filtered per part), and
   exclude exactly the L0 commit records named in its input list. An L0
   record present in the bucket but absent from the input list stays
   included. This makes query correctness independent of the interlock:
   an interlock violation (a late commit landing in a sealed bucket)
   degrades to an extra segment or harmless duplication, never to data
   loss, and the sweeper alarms on it. Read-your-write: if a
   min_commit_token's record GET returns NotFound, the resolver GETs the
   bucket's compaction record(s); a token whose (writer_id, epoch, seq)
   appears in an input list is satisfied by including that record's
   parts. Only a token covered by neither is `unsatisfiable`.
6. **Superseded-input deletion.** A separate sweep, anchored on the
   compaction record's own created_unix_ns: when
   `now >= record.created_unix_ns + protection_horizon` (horizon >=
   max_query_duration + grace, unchanged from consistency-model), delete
   the input commit records, then the input data objects. Crash between
   the two leaves record-less data objects that the orphan GC rule
   (ADR-0010 §11: age gate plus commit-record absence re-verified
   immediately before delete) already converges. Unreferenced loser
   parts are deleted only when the bucket has a durable compaction
   record, the part is referenced by none, its age exceeds
   grace + max_compaction_lifetime, and non-reference is re-verified
   immediately before the delete. A reader that outlives the horizon and
   hits NotFound on a pinned segment gets SnapshotInvalidated and one
   re-resolve, exactly as already specified. No reader leases are
   introduced; the consistency-model's "not lease-protected" clause
   stays vacuously true, and the sweeper gains a named no-op hook where
   a future lease check would slot.
7. **Placement.** New crate `ravel-maintain` (compaction planner and
   builder, sweeper, retention per ADR-0019), driven by
   `ravel-server --mode maintain` per the existing single-binary mode
   precedent, plus ravel-cli subcommands for one-shot and dry-run. The
   loop is stateless: work is discovered by listing, outcomes are
   durable objects, and a per-(tenant, shard) advisory cursor object
   (`t/<th>/m/maint/<shard>/cursor`, CAS-updated, the ADR-0003 HEAD
   precedent) only bounds rescans. Losing the cursor costs a rescan,
   never correctness. No leader election, no locks: concurrent
   maintainers converge through CreateIfAbsent.

   As shipped, the advisory cursor is unused by the running worker.
   `scan_and_compact` (its only consumer) has no non-test caller; the
   `--mode maintain` service deliberately full-scans every sealed bucket
   each pass via `scan_and_maintain`, which does not touch the cursor,
   because retention must re-evaluate every bucket on every pass rather
   than skip past a monotonic cursor (crates/ravel-maintain/src/scan.rs
   retention rationale). The cursor and its CAS plumbing remain for the
   compaction-only driver.
8. **v1 retirement is in scope.** Compaction output is always v4, so
   every compacted bucket removes v1 (and v2, and v3) objects from the
   population once the sweep completes, and retention (ADR-0019) bounds
   the tail of never-compacted buckets. This is the concrete path
   ADR-0027 left open: once an audit shows the stored population no
   longer contains v1 objects, a follow-up ADR can retire the v1 decode
   path. That removal is not this ADR's deliverable; the audit procedure
   is.

## Crash analysis (summary)

| Crash point | State left | Convergence |
|---|---|---|
| during input list/fetch | nothing written | re-run |
| after some/all part PUTs, before record | record-less parts | re-run reuses them via CreateIfAbsent; divergent bytes orphan the old parts, orphan sweep deletes them |
| after record PUT | compaction complete | re-run sees AlreadyExists + equal input_set_hash, verifies parts, no-op |
| sweep: after some input record deletes | remaining inputs still listed but excluded via input list | re-sweep; delete is idempotent (NotFound = Ok) |
| sweep: between record and data deletes | record-less L0 data objects | orphan GC re-verifies and deletes |

Every step is idempotent and every retry is a full re-run from listing;
there is no checkpoint to recover and no partial state a reader can
misinterpret, because visibility is carried entirely by the one
compaction record.

## Consequences

- docs/consistency-model.md and docs/catalog-and-mvcc.md are amended:
  the compaction paragraph is replaced by the
  overlap-harmless publish-then-supersede protocol, deletion/GC gains the
  supersession trigger, and token resolution gains the coverage fallback.
- docs/segment-format.md gains the v4 amendment; the property/fuzz
  matrix, golden corpus, and inspector permanently carry four grammars
  (until v1 retirement lands, then three).
- ravel-query's fetcher stops being fully version-blind: for v4 it emits
  one SoA run per (series, run) with per-run provenance from the catalog
  instead of one per series with provenance from the commit record. The
  change is contained in ravel-catalog's SegmentRef and ravel-query's
  fetcher; the merge itself (`is_greater`, `merge_series_runs`) is
  untouched, and a differential test proves query-over-inputs equals
  query-over-L1 bit-for-bit.
- Catalog metadata per L1 series scales with run count (~20 bytes per
  run) rather than being amortized to one entry; a measured stored-byte
  gate applies, and a run-merging L2 is the named follow-up if run counts
  dominate.
- Segment-level event-time pruning coarsens from per-flush bounds to
  per-part bounds; series-level and page-level pruning are unchanged
  because per-run bounds are preserved.
- Compaction removes the max_segments and listing-cost ceilings on
  historical buckets but does not reduce LIST-call counts across many
  buckets; that remains the ADR-0003 catalog-snapshot work, which must
  fold compaction records and tombstones when it lands.
