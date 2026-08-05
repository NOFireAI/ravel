# ADR-0056: Prefix-list traversal for catalog snapshot resolution

Status: Accepted
Date: 2026-08-05

## Context

`Catalog::resolve` (crates/ravel-catalog/src/catalog.rs) discovers commit
records by issuing one LIST per `(shard, ingest-hour)` bucket across the
padded query window `[range.start_ns - max_ingest_lag, now_ns +
clock_skew_allowance]` (docs/catalog-and-mvcc.md "Snapshot resolution (Phase
1)"). The request count is therefore `shard_count * hour_buckets`: it grows
linearly with the window width in hours and is completely independent of how
many objects actually exist.

The window's upper bound is anchored on `now_ns`, not `range.end_ns`, by
design (ADR-0010 section 8): objects are keyed by *ingest* hour, and
admission bounds event-time skew, so late and backfilled data can land in the
current ingest hour and stays discoverable only by scanning to now. A
client-supplied `range.start_ns` near the epoch therefore makes step 1 list
one prefix per bucket from the epoch to the current hour regardless of how
narrow `range.end_ns` is.

The measured cost (issue #634's closing comment): an epoch-width query issues
**496,089 LISTs for a single shard**, of which 496,088 return nothing, and
that count grows by one every wall-clock hour, forever, and multiplies by
`shard_count`. Per-bucket work is already minimal -- `list_hour_bucket` does
one `guarded_list_all` plus cheap partitioning, and nothing inside the loop
is reducible. The cost is the request *count*, not the per-request work.

Issue #635 (ADR-0044 decision 3, amended 2026-08-05) capped this by refusing
any window whose pre-execution estimate `shard_count * hour_buckets +
SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND` exceeds
`CatalogConfig::max_catalog_list_requests` (default 100,000). That stops the
runaway but leaves every wide-but-sparse window refused rather than served,
even though such a window's real cost is tiny.

### Measurement

Measured on `MemoryStore` with a LIST-counting wrapper, `page_size = 1000`,
`shard_count = 1`, one shard-0 commit subtree, over corpora of varying total
object count. Per-bucket count is the current `resolve`; prefix count is the
pages a single recursive `list_all` over the shard commit subtree
(`t/<hash>/<sig>/c/<shard>/`) drains.

| total commit objects | window hours | per-bucket LISTs | prefix pages |
|---|---|---|---|
| 200   | 25     | 25     | 1  |
| 200   | 2,001  | 2,001  | 1  |
| 2,000 | 721    | 721    | 3  |
| 10,000| 2,001  | 2,001  | 11 |
| 20,000| 20,001 | 20,001 | 21 |
| 20,000| 25,001 | 25,001 | 21 |

Two facts fall straight out and drive the decision:

- **Per-bucket LISTs = window hours exactly** (one LIST per bucket, and empty
  buckets dominate). Extrapolated to the epoch-width window this is the
  496,089 of issue #634.
- **Prefix pages = `floor(total_objects / page_size) + 1`**, independent of
  window width. The 5,000 extra empty trailing hours between the last two
  rows cost the prefix scan nothing and cost the per-bucket loop 5,000 more
  LISTs.

So the prefix scan's cost is `O(objects / page_size)` and the per-bucket
loop's is `O(shard_count * hours)`. The crossover is where the window is wide
enough (or empty enough) that `shard_count * listing_hours` exceeds
`total_objects / page_size`.

## Decision

Replace the per-`(shard, hour)` LIST loop with a **single recursive prefix
LIST per shard** over the existing `commit_shard_prefix`
(`t/<hash>/<sig>/c/<shard>/`, ravel-commit `keys.rs`) plus client-side hour
bucketing, for the window shapes where that wins; keep the per-bucket loop
for narrow/warm windows where it prunes better. This is a **hybrid**, and the
per-bucket-processing logic is shared byte-for-byte between the two paths.

The recursive LIST is issued per shard rather than once over the whole
`t/<hash>/<sig>/c/` subtree for two reasons, both correctness- or
scope-preserving: it reuses the existing public `commit_shard_prefix` key
builder rather than adding a new one to ravel-commit (out of this task's
scope), and its shard domain is `max_scan_count_over_range` -- the widest
shard count any generation active over the queried range holds, per the
generation-aware read-side scan rule (EK2, `scan_count`) -- not a static
`0..shard_count` (issue #659: the static bound silently missed stragglers in
a retiring generation's higher shard indices during a shard-count decrease).
This makes the listed key set a strict superset of what the per-bucket loop
lists per hour: the prefix path can list a shard index no in-range hour
actually needs, but it never lists fewer than the per-bucket loop would for
that hour, so the two paths' resolved snapshots always converge on the same
segment set. For the common case of one stable `shard_count` this is one or a
handful of recursive LISTs, versus `O(hours)` per shard before -- the "single
recursive prefix LIST" of the ticket, generalized to the provisioned shard
set's full generation history.

### The traversal

1. Extract the current bucket-processing body of `list_hour_bucket`
   (partition by key shape, tombstone handling, record prewarm, compaction
   interlock, L0/L1 include-and-filter) into `process_bucket(shard, hour,
   objects, range)`, taking the bucket's already-listed keys. Both paths call
   it. Because the per-bucket compaction/tombstone/interlock logic is a pure
   function of the set of keys in one `(shard, hour)` bucket, grouping the
   prefix scan's keys by `(shard, hour)` and running `process_bucket` per
   group yields the identical snapshot the per-bucket loop yields.
2. Per-bucket path (unchanged): one `guarded_list_all` per `(shard, hour)` in
   `listing_start_hour..=window_end_hour`, then `process_bucket`.
3. Prefix path: one drained recursive LIST per shard over
   `commit_shard_prefix`; classify each key with `partition_bucket_entry` to
   recover its `(shard, ingest_hour_bucket)`; keep only buckets with `hour` in
   `[listing_start_hour, window_end_hour]` (the same inclusive range the
   per-bucket loop iterates, so any snapshot-watermark suffix shortening and
   the `now`-anchored upper bound both carry over unchanged); run
   `process_bucket` per surviving bucket.

`listing_start_hour` is whatever the snapshot-window step already computed
(`window_start_hour`, or `watermark_hour + 1` when a usable folded snapshot
covers the low end). The prefix path does not change the range that is
scanned -- only how it is scanned (INTERACTION 2, below).

### The crossover

The choice is made on the **listing suffix** actually to be scanned, i.e.
after the snapshot-window step: `listing_suffix_buckets = shard_count *
(window_end_hour - listing_start_hour + 1)`. Take the prefix path when

```
listing_suffix_buckets >= prefix_list_crossover_requests
    || listing_suffix_buckets > max_catalog_list_requests
```

`prefix_list_crossover_requests` is a new `CatalogConfig` field, default
**720** (thirty days of hourly buckets at `shard_count = 1`). Rationale, from
the measurement: 720 caps the per-bucket path's request amplification at 720
LISTs before it hands off to the flat prefix scan, and at that width the
prefix scan issues 1-21 pages for the 200-20,000-object corpora measured -- a
34x to 720x reduction, growing without bound as the window widens. Below 720
the per-bucket path is retained because (a) it never exceeds 720 requests, so
it cannot become a runaway, and (b) it prunes better on a *folded, warm*
window: a snapshot watermark shortens its listed suffix to the handful of
post-watermark buckets, whereas the prefix scan must still read every commit
key in the shard subtree (it cannot seek past the watermark; the store's
`list` has no start-after, only a prefix and a continuation token). Keying the
crossover on the post-watermark suffix means a folded tenant keeps the
per-bucket path for warm windows exactly where it is cheapest.

The value is a **performance heuristic only**. Both paths return identical
snapshots and both respect the request ceiling (below), so any crossover value
is correct; the default trades a bounded amount of per-bucket amplification
against a bounded amount of wasted whole-history scanning, and is tunable per
deployment. The measurement shows the prefix path already winning far below
720 for every corpus measured, so the default is deliberately conservative:
it errs toward keeping the familiar, watermark-pruning per-bucket path.

### The request ceiling (INTERACTION 1)

Issue #635 made `estimated_catalog_requests` load-bearing: it gates admission,
refusing any window whose estimate exceeds `max_catalog_list_requests`. That
estimate is `shard_count * hours + SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`,
which describes the *per-bucket* traversal. Once the prefix path exists, that
formula no longer describes what a wide resolve does, and a ceiling that
counts hours would refuse a prefix scan that issues three-orders-of-magnitude
fewer requests -- the dangerous direction, a cheap query refused.

The resolution keeps the ceiling and keeps its guarantee ("a single resolve
never issues more than `max_catalog_list_requests` catalog LISTs") while
letting the now-cheap wide windows run:

- `estimated_catalog_requests` is **unchanged in formula** and remains a true
  upper envelope of whichever path runs (see "envelope" below). It stays the
  reported, worst-case pre-execution number.
- The pre-execution *refusal* is replaced by **path selection**: a window
  whose per-bucket cost would exceed the ceiling is not refused, it is routed
  to the prefix path (the `|| listing_suffix_buckets > max_catalog_list_requests`
  clause), which does not amplify.
- The prefix path carries a **runtime LIST cap**: it counts the pages it
  drains and aborts with `CatalogError::WindowTooWide { estimate: <pages
  issued>, limit }` if the count would exceed `max_catalog_list_requests`.
  This preserves #635's hard bound -- now enforced exactly, at runtime, on the
  one path whose cost is not knowable before listing -- and refuses only a
  scan whose *actual data volume* (not its window width) is unsustainable.
- The per-bucket path is chosen only when `listing_suffix_buckets <=
  max_catalog_list_requests`, so it can never exceed the ceiling and needs no
  separate refusal.

Net effect on admission: a wide-but-sparse window that #635 refused (e.g. an
epoch-width window over a tenant with a few thousand objects) is now **served
cheaply** via the prefix path; a wide window over a genuinely enormous corpus
is still refused, but at runtime by object volume rather than pre-execution by
hour count. The typed error, its fields, and its HTTP-422 mapping
(crates/ravel-query, crates/ravel-sql) are reused unchanged.

**Why the envelope still holds.** `estimated_catalog_requests` must stay
`>= actual` for the ceiling's guarantee to mean anything. Under the
sparse-bucket assumption #635's estimate already rests on (at most
`page_size` objects per `(shard, ingest-hour)` bucket -- the estimate already
counts one LIST per bucket and ignores intra-bucket pagination):

- Per-bucket path: actual `= shard_count * hours` (each in-window bucket one
  page) `=` estimate. Tight.
- Prefix path: actual `= shard_count + sum_s floor(N_s / page_size)`. The
  per-shard terminal page and the page-aggregation across hours make this
  strictly smaller than the per-bucket loop's `shard_count * hours` base for
  any window the prefix path is chosen for, so actual `<` estimate. The
  measurement bears this out (every `prefix pages` cell is far below its
  `per-bucket LISTs` cell). The runtime cap makes it a hard bound regardless:
  actual `<= max_catalog_list_requests <=` any admitted window's estimate is
  not required, because the cap refuses before the bound is crossed.

The one regime where the raw formula could under-count either path is dense
buckets (more than `page_size` objects in a single `(shard, ingest-hour)`),
which paginate. That is a **pre-existing** property of #635's estimate, not
introduced here, and the prefix path's runtime cap is a strictly stronger
backstop against it than the per-bucket path ever had. Verified by tests
(deliverable 3d) asserting `estimated_catalog_requests >= actual requests
issued` across window shapes.

### The listing window is unchanged (INTERACTION 2)

The window end stays `now_ns + clock_skew_allowance` and the window start
stays `range.start_ns - max_ingest_lag` (or the snapshot watermark). This
change alters *how* that range is scanned, never the range. Late and
backfilled data that landed in a low ingest-hour bucket well below the window
end is still discovered: the prefix scan reads the entire shard commit subtree
and the client-side filter keeps every bucket whose hour is in
`[listing_start_hour, window_end_hour]`, exactly the buckets the per-bucket
loop would have listed. Narrowing the window to cut cost would trade a
performance problem for a correctness one and is explicitly not done.

## Rejected alternatives

1. **Keep #635's pre-execution refusal; only optimize inside the admitted
   region.** Simplest, and preserves #635 untouched. Rejected: it leaves the
   dangerous direction of INTERACTION 1 unfixed -- an epoch-width window whose
   real cost is a handful of pages stays refused because the hour-counting
   ceiling still rejects it. The ticket calls this out specifically.

2. **Pure replacement: always use the prefix scan, delete the per-bucket
   loop.** Rejected: it regresses the folded, warm window. A tenant with a
   folded snapshot answers a one-hour dashboard query by listing a few
   post-watermark buckets; the prefix scan would instead read every commit key
   in the shard subtree, because the store's `list` cannot seek past the
   watermark (prefix + continuation token only, no start-after). The hybrid
   keeps the cheap path for the case that motivated folding.

3. **A single recursive LIST over the whole `t/<hash>/<sig>/c/` subtree
   (all shards at once).** Rejected in favor of per-shard: it would require a
   new key-prefix builder in ravel-commit (out of scope) or duplicating the
   key layout in ravel-catalog, and it would list keys under any shard beyond
   `max_scan_count_over_range` -- the generation-aware bound the prefix path
   actually scopes to (issue #659) -- changing the listed key set versus the
   per-bucket loop. Per-shard listing reuses `commit_shard_prefix` and scopes
   to the provisioned shards for free.

4. **A pre-execution estimate of the prefix path's cost, gating admission on
   it.** Rejected: the prefix path's cost is `O(objects / page_size)`, and the
   object count is unknowable before the LIST runs. Any a-priori bound is
   either the loose `shard_count * hours` (which re-introduces the false
   refusal) or unsound. The runtime cap is the only honest bound on a scan
   whose size is data-dependent.

5. **Narrow the window (cap how far back `range.start_ns` reaches).** Rejected:
   INTERACTION 2. The `now`-anchored window is a correctness property for
   ingest-hour-keyed late data; narrowing it silently drops backfilled
   segments.

## Consequences

- No persistent format changes. No stored bytes, object key layout, protobuf
  schema, segment format, or commit token encoding is touched. The dual-reader
  question of the format-change procedure is therefore N/A: both paths read
  the identical, unchanged objects, and either path can read data written by
  any writer version. This ADR goes through the format-change discipline
  because it changes a *normative traversal* documented in
  docs/catalog-and-mvcc.md, not because any byte changed.

- `resolve`'s observable behaviour is identical on both paths: same snapshot
  segment set, same deterministic total order, same read-your-write min-token
  semantics (min-token resolution is untouched -- it never listed). Proven by
  a differential test (deliverable 3a) reconstructing the expected set
  independently.

- **INTERACTION 1**: `estimated_catalog_requests` keeps its formula and its
  envelope property but is no longer the admission *gate* for wide windows;
  admission for the prefix path moves to a runtime LIST cap against the same
  `max_catalog_list_requests`. ADR-0044 decision 3 is amended to record this.
  Wide-but-sparse windows #635 refused are now served; only genuinely
  oversized scans are refused, at runtime.

- **INTERACTION 2**: the scanned range is unchanged; only the scan method
  changes. Late data below the window end is still found.

- New `CatalogConfig::prefix_list_crossover_requests` (default 720),
  inherited through `CatalogConfig::default()`, so `services/ravel-server`
  gets it without a wiring change. Not yet a CLI flag; a mechanical follow-up
  if an operator needs to tune it without a rebuild, exactly as
  `max_catalog_list_requests` is today.

- The prefix path reads every commit key in a shard subtree, including keys
  below a folded watermark that the per-bucket path would have skipped. This
  is why the crossover is keyed on the post-watermark suffix: a folded tenant
  stays on the per-bucket path for warm windows and only reaches the prefix
  path for windows wide enough that reading the whole subtree is the cheaper
  option anyway.
