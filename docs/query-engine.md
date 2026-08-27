# Query Engine Design (Phase 1)

Implementer contract for `ravel-query` and the query side of `ravel-server`.
ADR-0006/0007 give the reasoning; docs/consistency-model.md the semantics.

## Flow

```
HTTP /api/v1/query, /query_range, /labels, /label/{name}/values, /series
  -> auth + tenant resolve
  -> ravel_promql::plan_selectors(query, eval_start, eval_end): a static AST
     walk, independent of evaluation, reporting every selector's matchers,
     range, offset, and @-anchor (ADR-0021 SS1)
  -> Catalog::resolve(tenant, Metrics, padded_range, min_commit_token), one
     shared snapshot resolved against the union of every selector's own
     fetch window
  -> per selector (concurrency-bounded, default 8), against that one
     snapshot:
       for each snapshot segment: suffix GET (64 KiB) -> Reader::parse
         -> prune series by that selector's own matchers (SERIES_META +
            LABEL_DICT)
         -> plan page ranges, coalesce adjacent (gap <= 64 KiB)
         -> ranged GETs -> decode pages -> per-series samples
       -> selective-erasure exclusion: every series or sample matching a
          predicate the snapshot carries in `pending_erasure` is dropped
          from the decoded per-segment results (ADR-0064 decision 2)
       -> the selector's per-segment scalar SoA runs are kept UNMERGED
          (native-histogram runs are merged per selector here); the raw
          scalar runs join a single query-wide pool so the one k-way merge
          below unions across selectors AND cross-cluster federation runs
          (ADR-0071) under one total order
  -> all selectors' raw scalar runs (plus every remote cluster's federated
     runs) pooled and merged once by series id: one lazy k-way merge (each
     run already ascending by ts; emits ascending, one sample per ts, no
     final sort). Duplicate timestamps (across segments, within one run,
     and across clusters) resolve under the total order in
     docs/catalog-and-mvcc.md, not by arrival order. max_series/max_samples
     are enforced ONCE over this pooled union -- a query-wide bound, not a
     per-selector budget: the caps guard the coordinator's peak memory for
     the whole query, so a multi-selector (or federated) query cannot hold
     N times the cap. Pooling is loss-free: two selectors sharing a series
     id decode byte-identical raw runs (the decode is a pure function of
     segment bytes + series id, with no per-selector window trim), so
     merging them once yields exactly what merging each alone would, and
     per-selector window clipping still happens downstream in
     `SeriesSource::query`. max_segments stays a per-query cap on the shared
     snapshot.
  -> every merged series combined into one flat SeriesSource (later
     selectors sharing a series id keep the first merge seen; a later
     per-selector SeriesSource::query call still clips
     to that selector's own window, so which selector "wins" a shared
     series id in the combine step does not affect any single selector's
     result -- and since every selector in one query fetches against the
     same shared snapshot with no per-selector sample-level window trim,
     two selectors sharing a series id always resolve identical merged
     data, so "wins" only ever breaks a tie between equal values, never
     drops one. The combined series/histogram-series lists are then sorted
     by label set, so their order is deterministic across runs regardless
     of `HashMap` iteration order or fetch completion order)
  -> ravel-promql Evaluator -> Value (scalar / string / instant vector /
     range matrix)
  -> Prometheus JSON envelope {status, data:{resultType, result},
     warnings, infos}
     (warnings/infos are the evaluator's Annotations: two distinct
     Prometheus fields, both omitted when empty. A quantile argument outside
     [0,1] and a malformed classic histogram are warnings; a forced
     histogram monotonicity fixup is an info.)
```

`padded_range`: the union, over every selector `plan_selectors` reports, of
that selector's own fetch window (its lookback or matrix range, plus its
own offset, anchored per `PlanAnchor::Window`/`Pinned`), so lookback never
misses samples stored in an earlier-only segment for any selector, however
widely the selectors' own windows differ.

Selective erasure (ADR-0064 decision 2): `Catalog::resolve` lists the
tenant's `del/` prefix once per resolve and attaches every pending erasure
request to the snapshot. The engine maps each request into one scan-time
predicate (a conjunction of exact `key = value` matchers plus an optional
half-open `[window_start_ns, window_end_ns)` event-time window, zero on a
bound meaning unset) and applies them in `fetch_all_samples_and_histograms`
and `fetch_all_series`, on decoded results, after the fetch and after any
cache tier. Filtering there rather than at fetch time is what makes stale
cached bytes of a since-erased subject unreachable: a cache hit is filtered
exactly like a fresh GET. A series matching a windowless predicate is
dropped whole, including from `/labels`, `/label/{name}/values`, and
`/series`; a windowed predicate drops only that series' in-window samples
and leaves it enumerable. The equivalent hop for the SQL logs, spans, and
metrics surfaces in `ravel-sql` is not wired yet: those scans build their
own queries and do not read `Snapshot::pending_erasure`.

## Logs `COUNT(*)` from catalog row counts

A predicate-free `SELECT COUNT(*) FROM logs` is answered from the catalog's
committed row counts, not by reading any log object. `LogsScanExec` reports
`num_rows = Precision::Exact(sum of SegmentRef::sample_count)` over the
resolved snapshot, so DataFusion's `AggregateStatistics` physical-optimizer
rule rewrites the aggregate into a literal and the scan is never executed:
the plan contains no `LogsScanExec` and the query issues zero object-store
GETs. Every `sample_count` is written by the commit record, so the sum is
known the moment `Catalog::resolve` returns; before this, counting the full
ClickBench tenant (issue #680, 8424 objects, 100M rows) took 142 s and moved
23 GB from object storage to add up numbers the resolve already had. The leaf
also reports an `Exact` `num_rows`/`ts` span (issue #723) when a `ts` bound is
present but fully CONTAINS every resolved segment -- the bound removes no
row, so the sum is still exact -- not only in the no-bound case. The leaf
falls back to `Absent` (the rule does not fire on it, `LogsScanExec` stays in
the plan) whenever the count of committed rows is not the query's answer: a
`ts` bound that clips at least one segment, any `has_word` content predicate
or attribute-equality prune, or any pending selective erasure in the snapshot
(ADR-0064), which removes rows the committed counts still include.

This exact leaf statistic only reaches DataFusion's `AggregateStatistics`
rule end to end when no `FilterExec` survives above the scan to intercept
it, because a `FilterExec` reports its own (non-exact) statistics rather
than passing the leaf's `Exact` count through. So
`LogsTableProvider::supports_filters_pushdown` (crates/ravel-sql/src/
logs_provider.rs) reports `Exact` for a filter that resolves purely to a
`ts` bound and/or a `has_word` content predicate (issue #733), which
deletes it from the plan: both land in the channel the RLOG reader
re-verifies against each decoded row's own value (`ravel_logseg`'s `eval`,
its `Predicate::TsRange` and `Predicate::HasWord` arms), so no residual is
needed. A `COUNT(*)` over a contained `ts` bound therefore takes the same
zero-GET path the genuinely predicate-free query does.

Every other filter stays `Inexact`. An `attrs['k'] = 'v'` equality and
every declared typed column predicate (ADR-0093) go to the prune-only
channel, which the reader uses for block pruning ONLY and never evaluates
per row, so DataFusion's residual remains the sole exact evaluator; a
filter recognized only in part (a `ts` bound AND-ed with an unrecognized
sub-expression in one unsplit expression) is `Inexact` too, never
partially credited. A distributed coordinator (ADR-0071) also reports
every filter `Inexact`, since its fan-out pushes no filter to the workers
and needs each one back as a residual.

## Predicate-free full-window logs scan: request count

Every OTHER predicate-free, full-window logs statement — `SUM`, `AVG`,
`GROUP BY`, `ORDER BY ... LIMIT` over the whole table — still executes
`LogsScanExec`, but issues exactly one whole-object GET per relevant segment and
ZERO suffix probes (#693 part 3). When the query carries no block-level
predicate and no pending erasure, its window fully contains every relevant
segment, and there are at least `target_partitions` of them, `LogsScanExec`
skips its plan phase entirely: no block can be pruned,
so it assigns whole segments round-robin (one owner per segment) and reads each
in a single `GetRange::Full`. That removes both probe classes the pre-#693-part-3
path paid above the block-range threshold — the plan-phase footer probe and the
per-open scan-side re-probe — which on the 8424-object ClickBench tenant (#680)
were a combined ~24,700 probes on top of the 8,424 whole-object reads for one
`SELECT`. When any of those conditions fails the unchanged plan-then-stripe path
runs (its plan phase probes each segment once), and the scan publishes a
`fast_path_rejected_*` counter naming the conjunct that sent it there; a footer
read there is carried to the per-partition subset opens so they skip re-probing
(ADR-0107 amendment 2026-08-26). Object size is not one of the conditions: a
segment at or below the block-range threshold is read whole by the whole-segment
entry and by the striped path alike, on the same `(0, object_size)` cache key,
so it joins the assignment rather than vetoing it (ADR-0102 amendment
2026-08-26, #739 -- as a query-wide conjunct the threshold let one small tail
object per `(shard, hour)` disqualify an entire 8,424-object snapshot).

## Selective (predicated) logs scan: request count

A statement with a block-level predicate — a declared-column or `attrs`
numeric comparison (the ClickBench q20 and q37-q43 shapes), a text
`has_word`, a stream filter — is not block-predicate-free, so it takes the
plan-then-stripe path, never the whole-segment fast path above. Before #761
that path read every relevant object WHOLE twice over: the plan phase opened
each segment to count survivors, and the scan's `fetch_object_with_footer`
resolved candidate blocks by `SkipIndex::candidate_blocks(ts, ts, None, &[])`
— no numeric arms — so every block was a ts candidate, the coverage crossover
(`candidate_bytes / BLOCKS-section bytes >= 0.75`) fired, and the read
collapsed to one whole-object GET. The predicate pruned blocks only at decode,
shrinking `blocks_scanned` but not the bytes moved. On the 8,424-object
ClickBench tenant (#680) q37 moved 19,690 GETs and 11.7 GB to decode 144 of
17,731 blocks, and q20 moved 29,614 GETs and 17.9 GB — more than the 11.1 GB
of objects on disk, because a cold cache re-read each surviving segment once
per owning partition.

Issue #761 makes the prune-only `NumRange` arms drive candidate selection. Each arm
is resolved against the object's own FIELD_DIR
(`FieldDir::numeric_range_arms`) to its column id, then applied to
`candidate_blocks` in two places:

- **Fetch side.** `fetch_object_with_footer` prunes the candidate set to the
  surviving blocks before it weighs the coverage crossover, so a selective
  query reads only those blocks (and the crossover fires only when the
  survivors genuinely cover >= 75% of the BLOCKS section — the threshold is
  unchanged). On a version-4 object the unit is the surviving blocks' pages in
  the projected columns' chunks rather than whole blocks; see "Requests per
  object on a version-4 object" below. This is byte-identical to the unpruned
  read: the skip index's
  per-block bounds are conservative (ADR-0013), so a block dropped at fetch is
  one the decode-side prune would drop anyway, and the reader still runs the
  full skip/POSTINGS/bloom prune over the fetched buffer.
- **Plan side.** `plan_segment` counts survivors from the skip index alone
  (footer + SKIP_IDX via the 256 KiB suffix probe, plus the object's FIELD_DIR
  to resolve the arms) and fetches no block, carrying the footer forward so
  each per-partition subset open skips its own probe (#693 part 3). This is
  sound because for a query the skip index can decide — ts bounds and NumRange
  arms only — the reader's full prune reduces to its skip step, so the count
  equals the survivor list the scan stripes. It takes at least one NumRange arm
  to qualify: a query with no prune arm is skip-decidable too, but its plan read
  already fetches only the ts-candidate blocks and warms exactly the extents the
  subset opens stripe, so planning it this way would trade one shared read for
  N per-partition ones over the same bytes. This branch's `fetch_plan_sections`
  read is wrapped in its own `page_fetch` span (#782), recording the probe and
  section GET count and `BlockRangeStats::block_bytes_fetched` on it, the same
  way `plan_segment_fast` and `plan_segment_block_stats` already do — before
  #782 this branch's read was the one plan-phase GET on the query path with no
  span at all, invisible to a trace over a statement that took it.

A predicate the skip index cannot decide — a `has_word`/text arm (bloom prunes
it only at decode), an `attrs['k']='v'` POSTINGS equality, a stream filter —
still reads the whole object in the plan phase and is counted in the
`plan_full_reads` metric, so a report can see which statements still pay it. A
segment at or below the block-range threshold counts there too: the fetch reads
such an object whole in one GET regardless, so planning it from the skip index
would cost a second read rather than save one.

### Requests per object on a version-4 object

A version-4 RLOG object (ADR-0699) stores each row group's pages column-major
and lists every page in PAGE_DIR, so the fetch unit is a column chunk rather
than a block. The request law for one statement over one such object:

**One suffix probe, plus one coalesced range per surviving `(row group,
projected column)`, plus front-section ranges (STREAM_DIR always, FIELD_DIR
when the query carries numeric arms) only when the probe's cached suffix does
not already cover them.** That is one to four GETs per object for a typical
narrow projection over a small object, and it grows with `row_groups x
projected_columns` rather than with the object's block count.

The pieces, and why each is where it is:

- The 256 KiB suffix probe brings the footer, SKIP_IDX and PAGE_DIR in one GET.
  It was 64 KiB until issue #766: BLOOM (86 KB mean on the reference tenant)
  sits between SKIP_IDX and the footer, so the shorter probe missed SKIP_IDX on
  68.8% of above-threshold objects and cost 4,415 extra GETs per predicated
  statement. A probe that still falls short costs one extra GET, not one per
  section: SKIP_IDX and PAGE_DIR are adjacent and are fetched as one range, and
  `BlockRangeStats::probe_misses` reports the residual rate.
- STREAM_DIR and FIELD_DIR sit at the object's *front*, so no suffix probe of
  any length reaches them. FIELD_DIR is read only when the query needs it (a
  NumRange arm to resolve, or a projection narrower than every column), and it
  is an extra GET only when the probe's cached suffix does not cover it — which
  on a small object, where the probe spans the whole object, it does.
- Chunk ranges: one per surviving `(row group, projected column)`, fewer when
  adjacent chunks coalesce, and one for the whole group when the projection
  keeps every column and every block of the group survives. Pruned blocks' pages
  are the holes inside those runs, read through or split around by the
  `coalesce_gap` policy.
- The 75% coverage crossover still applies, now against the projected page
  bytes: an all-columns read of every block takes a single whole-object GET
  instead.

Reference figures on the 8,424-object ClickBench tenant, at version 4. A
single-column, predicate-free statement reads 8,424 probes plus about one
coalesced range per object (these objects hold roughly two blocks, so one row
group), moving on the order of `1/N` of each object for `N` columns of similar
width — a few hundred MB against the 11.1 GB the version-3 whole-object reads
moved for the same statement. A selective statement adds its FIELD_DIR range
where the probe did not cover it and reads only the surviving blocks' pages:
q37-class drops from 19,690 GETs and 11.7 GB, and q20-class from 29,614 GETs and
17.9 GB, to between one and three GETs per relevant object either way. The
object count is the floor in every case: the probe is one per relevant object
however selective the predicate or narrow the projection, and only the chunk
ranges scale with them. These are predictions; the measured pass on the
reference tenant replaces them.

Staleness: the evaluator recognizes the Prometheus staleness marker (the
exact NaN bit pattern `0x7ff0_0000_0000_0002`, compared via
`f64::to_bits()`, never `is_nan()`). A selector whose newest in-window
sample is the marker treats the series as absent at that instant; range
windows exclude marker samples. Every other NaN payload is a live value
and passes through bit-exactly.

## Time-range and matcher pruning order

1. Segment level: commit-record event-time bounds vs padded range (already
   done by Catalog::resolve).
2. Series level: SERIES_META entry ts bounds vs padded range, then matcher
   evaluation against the decoded LabelSet. Equality matchers use dictionary
   ordinal lookups (resolve value -> ordinal once, compare ordinals);
   regex/negative matchers evaluate on materialized label sets.
3. Page level: v1 has one page pair per series; nothing further to prune.

## Segment catalog fetch: whole-object vs sparse catalog-probe

`SegmentFetcher::decode_selected` decodes one segment's catalog before it
plans page ranges. How it fetches the catalog depends on the object's v5
shape (docs/segment-format.md, ADR-0026):

- **Below the 4096-series sparse threshold** the object carries the
  whole-section SERIES_META (kind 6). The fetcher GETs LABEL_DICT +
  SERIES_IDS + SERIES_META and decodes the run-major catalog. No page bytes
  are pulled.
- **At or above the threshold** SERIES_META is replaced by the SERIES_IDX
  directory (kind 8) and the chunked SERIES_META_CHUNKS (kind 9). These four
  catalog sections (LABEL_DICT, SERIES_IDS, SERIES_META_CHUNKS, SERIES_IDX)
  sit contiguously at the object front, ahead of the TS/VAL/HIST page
  sections. When the object qualifies for the **catalog-probe path**, the
  fetcher range-GETs only that catalog prefix (coalesced into as few GETs as
  the layout allows) and decodes via `decode_catalog_v5_chunked`, skipping
  the page bytes a whole-object GET would move. The matched series' pages are
  fetched selectively afterward, exactly as the below-threshold path already
  does. Each catalog section's crc32c is re-verified on the fetched bytes, so
  a mis-ranged or corrupt fetch is a typed error, never wrong data.
- **Otherwise** (a sparse object that does not qualify) the fetcher keeps the
  unchanged whole-object GET and `decode_catalog_v5`.

Qualification (`SegmentFetcher::sparse_probe_qualifies`) requires all of: the
object carries both sparse sections; the query's matcher set is non-empty (an
empty matcher wants every series, so one whole-object GET beats a catalog GET
plus a page GET spanning the object); and the object is at least
`SPARSE_PROBE_MIN_OBJECT_SIZE`. The size floor is the crossover between one
whole-object GET and one catalog GET plus the extra selective page-range round
trips. Measured per-request latency (~1-5 ms loopback, ~15-80 ms projected
real S3) did not meter this specific within-segment crossover, so the floor
is set
conservatively (256 KiB: above the four fixed 64 KiB suffix/gap probes, far
below any real compacted sparse L1 part) rather than fit to a measured point.
The within-segment GET/byte model is the `selective_read_accounting` bench.

## Endpoints (Prometheus compatibility subset)

- `POST/GET /api/v1/query` (params: query, time, timeout) instant.
- `POST/GET /api/v1/query_range` (query, start, end, step, timeout).
- `GET /api/v1/labels`, `/api/v1/label/{name}/values` (match[] optional,
  start/end optional): from snapshot SERIES_META label dictionaries, unioned
  across configured remote clusters when federation is on (see Cross-cluster
  federation).
- `GET/POST /api/v1/series` (match[] required, start/end): likewise unioned
  across remote clusters under federation.
- `GET /api/v1/status/buildinfo`: Ravel's own crate version under `version`,
  never a Prometheus version string. `revision` carries the build's git SHA
  when the build environment exported `RAVEL_GIT_SHA`, otherwise it is empty,
  as are `branch`, `buildUser`, `buildDate`, and `goVersion`.
- `GET /api/v1/metadata`: per-tenant metric type/help/unit metadata (ADR-0085
  decision 1), served from a per-process, per-tenant, on-demand cache over the
  catalog record at `t/<tenant_hash>/m/meta`, never a per-request object-store
  read. The tenant is resolved from the same bearer credential `/api/v1/labels`
  uses; a request with no resolvable tenant keeps the pre-ADR
  `{"status":"success","data":{}}` (the endpoint never `401`s). Response shape
  is Prometheus' own: `data` maps each family name to a length-1 array of
  `{type, help, unit}`, with `type` one of `counter`/`gauge`/`histogram`/
  `summary`/`unknown` and names in deterministic sorted order. The optional
  `metric=<name>` param filters to one family (an unknown name yields an empty
  object, still `200`) and `limit=<n>` caps the number of names. The cache fills
  on the first request for a tenant (one GET), serves within a 60 s refresh
  horizon from memory, and past the horizon serves the cached record
  immediately while one background refresh GET runs (stale-while-revalidate);
  cost is one GET per (queried tenant, horizon, query process). When no cache is
  attached the endpoint returns the empty object exactly as before.
- All accept `min_commit_token`. Errors use the Prometheus JSON error
  envelope (`status:"error"`, `errorType`, `error`) with correct HTTP codes
  (400 bad_data, 422 unprocessable for unsupported constructs, 503
  unavailable for store failures).
- Timestamps: Prometheus float seconds in, RFC3339 or float accepted like
  Prometheus; responses use float seconds with ms precision.
- `resultType`: `vector`, `matrix`, `scalar`, `string`. A top-level scalar
  or string result from `/api/v1/query_range` is rendered Prometheus-style
  as a `matrix` with one synthetic empty-labeled series repeating that
  value at every evaluated grid step (`ravel-promql`'s `eval_range`
  resolves it once; materializing the repetition is this HTTP layer's
  job, not the evaluator's). `/api/v1/query` (instant) can also return
  `matrix`, not just `vector`/`scalar`/`string`: when the top-level
  expression is itself a range vector (for example a bare subquery, or a
  range function nested inside an outer subquery), Prometheus renders
  `resultType: matrix`, and Ravel matches that. Such a matrix already
  carries its own per-series timestamps from the evaluator, so no grid
  repetition is synthesized.
- Matrix rendering (both `/api/v1/query_range` and an instant query that
  returns a matrix) carries native-histogram steps in a `histograms` array
  alongside the float `values` array, per element type per timestamp, using
  Prometheus' `[ts, {count, sum, buckets}]` encoding (ADR-0108); a
  float-only series omits `histograms` and a histogram-only series omits
  `values`. One exception on the instant endpoint: an instant query whose
  top-level expression is a range vector *carrying native histograms* (a bare
  histogram matrix selector such as `h[5m]`, or a subquery over histogram
  data) is not rendered as a matrix with dropped histograms; it returns a
  typed 422 `Unsupported` instead (ADR-0108 decision 8), because the instant
  matrix channel is float-only. Range queries render histogram matrices
  normally.

## Budgets (Phase 1: static config)

Per query: max segments touched (1024, a cap on the one shared snapshot,
not per selector), max concurrent GETs (8, applied both across selectors
and across one selector's own segments), max matched series (10k) and max
samples (10M) enforced independently per selector (a query with several
selectors, e.g. a future binary/aggregate expression, grants each its own
full budget rather than splitting one budget across them), max
range-evaluation points per query (11,000, matching the Prometheus
resolution limit), wall deadline (server maximum, default
30 s). The `timeout` param can only lower the deadline: values above the
server maximum are clamped to it. Exceeding a budget returns a
Prometheus-style error, never a partial silent result.

### Segment admission (ADR-0073)

`max_segments` no longer counts every segment in the resolved snapshot. A
resolve tags each segment with an origin (sealed-below-watermark, recent, or
token-resolved via an explicit `min_commit_token`) and `max_segments` applies
to the sealed count only; recent and token-resolved segments are exempt, so a
hot tenant's open hour and a read-your-write query no longer 422 on count.
Their cost is bounded instead by a per-query S3 request budget
(`EngineConfig::max_s3_requests`, default 25,000), checked incrementally at
the same points `max_bytes_scanned` already is, and reported as
`RequestBudgetExceeded` (HTTP 422) when tripped.

`crates/ravel-query/src/segment_admission.rs` is the one seam both checks go
through: `admit(&snapshot, &origins, &config)` for the sealed-count check,
`request_budget_exceeded(requests, max_s3_requests)` for the incremental
budget check. `QueryEngine::resolve_bounded` (`engine.rs`) is the call site
for PromQL; the SQL executor, the five SQL table providers, and the
exemplars state moved onto the same seam — no site still runs a
pre-ADR-0073 per-surface check.

An end-to-end test proves this seam through both real HTTP query
surfaces rather than at the seam's own unit level: a real `IngestRouter`
sustains flushes past `max_segments`-worth of L0 objects in a tenant's
open hour against `MemoryStore`, and PromQL and SQL reads over that hot
window keep succeeding, returning results bit-identical
(`f64::to_bits`) to a post-compaction read of the same data once the hour
is folded sealed. A deliberately low `max_s3_requests` on the same hot
data trips the typed `RequestBudgetExceeded` rather than hanging or
truncating the result
(`services/ravel-server/tests/recent_hours_reachability_e2e.rs`).

Per-tenant max bytes scanned (`ByteLimit`, default `Unlimited`; ADR-0061
decision 1) bounds the total S3 bytes one query may fetch
across every segment its shared snapshot resolves to, protecting against a
selector that passes the count caps but whose covering segments are large.
It is checked once per completed segment fetch, inside the two fetch
fan-outs (`fetch_all_series` for the labels/series endpoints,
`fetch_all_samples_and_histograms` for instant/range queries) rather than
after the merge, so a tripped budget cancels the query mid-scan: returning
early drops the fetch stream and stops polling the remaining segments'
in-flight GETs. Exceeding it returns `TooManyBytesScanned` (HTTP 422),
distinct from the count-cap errors. The default is `Unlimited` so an
existing deployment is never silently rejected on upgrade; opting in to a
bound is explicit. Like every count cap, enforcement is per completed
segment, so a single very large segment can overshoot the bound by up to
its own size before the next check fires.

An operator configures the budget in `ravel-server`'s `--limits-file`, the
same TOML file that already carries the ingest admission caps (ADR-0051
section 3): a `max_bytes_scanned` key in the `[defaults]` table, set to a
positive byte count or the string `"unlimited"`, sits beside
`max_active_series` and the other per-tenant knobs. It is read once at
startup and threaded into the one process-wide `EngineConfig` that both the
PromQL/HTTP and SQL/HTTP query surfaces share, so the configured budget is
the enforced budget on both. Absent, it stays `Unlimited`. A
`[tenants.<id>]` table accepts the same key in the operator-familiar
per-tenant shape, but the process-wide engine holds a single `EngineConfig`
and is not tenant-parameterized: it enforces the `[defaults]` budget for
every tenant, so a per-tenant override is parsed and validated but not yet
enforced differently from the default (the server logs a startup warning
naming any tenant that set one). Enforcing a distinct budget per tenant
needs a tenant-aware `EngineConfig` lookup inside the engine, not yet built;
until then, set the budget in `[defaults]`.

The point cap is
enforced independently at every subquery evaluation node (`expr[5m:1m]`),
checked against that node's own grid before it is built, so a nested
subquery whose own grid alone exceeds the cap is rejected before any
allocation or recursive evaluation, at whatever nesting depth it occurs.

A per-node point cap alone does not bound the total cost of nested or
repeatedly re-evaluated subqueries: an outer range query re-evaluates a
matrix-function argument's subquery from scratch at every one of its own
grid steps, and a subquery may itself nest another subquery, so a query
whose every individual node sits safely under 11,000 points can still
multiply that cost across nesting levels and re-evaluations.
`ravel_promql::Evaluator` therefore also charges a shared, cross-level
evaluation budget (`Evaluator::with_max_total_eval_points`, default
`DEFAULT_MAX_TOTAL_EVAL_POINTS` = 1,000,000 grid points): every subquery
grid evaluation, at any nesting depth and however many times an enclosing
grid step re-triggers it, debits this one query-wide counter, and
exhausting it returns `Error::EvalBudgetExhausted` before the excess work
runs.

The evaluator is otherwise fully synchronous, so the wall deadline above
used to only take effect at the `tokio::time::timeout` wrapping an
instant/range query: a runaway evaluation ran to completion regardless,
and the deadline only changed how long the caller waited before seeing
the error. The evaluator now also carries its own copy of that deadline
(`Evaluator::with_deadline`) and checks it between subquery grid steps
(inside a subquery's own grid loop and inside the per-outer-step
re-evaluation loop that drives it), returning `Error::DeadlineExceeded` as
soon as it is past due. `QueryEngine::{instant,range}_with_stats` derive
this deadline from their own `Duration` parameter before evaluation starts
and fold whichever error surfaces first, the evaluator's own or the outer
timeout's, into the same `QueryError::DeadlineExceeded`.

A subquery that omits its own step (`expr[5m:]`) defaults to
`EngineConfig::default_evaluation_interval` (60 s, matching Prometheus'
global `evaluation_interval` default). Subquery grids are epoch-aligned:
the grid start is the smallest multiple of the step (measured from Unix
time zero) that is strictly greater than `end - range`, left-open like
this crate's own matrix-selector window, matching Prometheus' own
subquery alignment, not the query's own step or window start.

A subquery whose inner expression matches native-histogram data is not yet
supported: the subquery grid reducer keeps only the float value of each
step, so a histogram element would be silently dropped. When histogram data
is actually present in the fetched window, the evaluator returns
`Error::Unsupported` (`subquery over native histograms`, HTTP 422) instead
of a wrong empty answer. The trigger is the presence of matched histogram
data, not the syntactic shape: a float-only subquery, including
`rate(x[5m:1m])` over float series, is unaffected. Real histogram subquery
support is future work.

Range evaluation carries native-histogram elements end to end inside the
evaluator (ADR-0108, issue #577). The internal range result is a
histogram-aware matrix (`RangeSample`, a per-step float-or-histogram element
mirroring the instant `InstantSample`), filled by three paths: a bare
histogram selector fetches its series once per query window via
`query_histograms` and picks a histogram per grid step under the same
left-open lookback rule the instant selector uses; the grid path
(`RangeCore::Generic`, top-level aggregates and binary expressions)
materializes each step's histogram element instead of collapsing it to a
`0.0` placeholder float, so range output stays faithful to the instant path
per step. Per-operator aggregation follows Prometheus 3.x: `sum`/`avg` over
histogram elements produce histogram elements, `count` a float, and the
operators Prometheus leaves undefined for histograms (`min`, `max`,
`stddev`, `stdvar`, `quantile`, `topk`, `bottomk`) drop the histogram
elements and annotate the drop with an info
(`HistogramIgnoredInAggregationInfo`, "ignored histogram in <op>
aggregation"), never a silent drop. `by`/`without` grouping preserves
element type.

The range counter functions and the `_over_time` family carry histograms too
(ADR-0108 decisions 4/5, issue #578). `rate`/`increase`/`delta` reduce a
histogram window to a histogram element through the same
`histogram_extrapolated_rate` reducer the instant arm uses.
`sum_over_time`/`avg_over_time` produce a histogram element,
`count_over_time` counts histogram samples as a float, `last_over_time`
returns the newest histogram, and `present_over_time` is 1 for any non-empty
window. The float-only members (`min`/`max`/`stddev`/`stdvar`/
`quantile_over_time`, `predict_linear`, `irate`/`idelta`/`resets`/`changes`/
`deriv`) drop a histogram-only window, matching the pinned Prometheus
v3.13.1 binary exactly: its `if len(samples.Floats) == 0 { return }` early
exit yields no element and, because that return precedes the
histogram-ignored annotation, no annotation either. Both endpoints answer
the same class of value: the instant dispatch fetches the histogram matrix
alongside the float one, so a histogram-only series is no longer read as
empty. `absent`/`absent_over_time` count a native-histogram sample as
presence, so a histogram-only stream never reads as absent.

The histogram-aware result is exposed by
`Evaluator::eval_range_hist_annotated`; `Evaluator::eval_range_annotated`
(and any float-only caller) projects histogram elements away rather than
render them as `0.0` floats.

The max-samples budget is **count-yielded**: samples are counted as the
lazy k-way merge emits them (post-dedup), and the budget trips at exactly
`max + 1`. It does not count a fully materialized per-timestamp window
before checking, so it does not silently pass an over-budget query that
happens to dedup down to size. The count is independent of segment or
series iteration order, so the error is deterministic. It bounds only the
output size: every matched series in every matched segment is still fully
fetched and SoA-decoded before the merge runs, so peak fetch/decode memory
scales with the query's matched input, not with `max_samples`.

## Intra-cluster read fan-out (ADR-0071)

Within one cluster, a read can be spread across peer query nodes so more than
one process's NIC, CPU, and memory serve it. This is off by default: a
`QueryEngine` built without a distributed context runs the byte-identical
local path, and `--distributed-query` (plus a fragment-credential file) is
what attaches one. The fan-out changes only *where* bytes are fetched and
decoded; aggregation and evaluation do not move, so the coordinator-merged
result is bit-for-bit identical to local execution, enforced by the
`distributed_merge_equals_local_bitwise` differential test over arbitrary
corpora and slice partitions.

### Coordinator, cost gate, and slices

The query node that receives a request is that query's coordinator (a
per-query role, not a process type). It resolves ONE pinned snapshot exactly
as the local path does, then consults the same pre-execution `CostEstimate`
the accounting layer already computes: distribution trips only when the
estimate reaches 256 MiB of estimated store bytes **or** 256 segments (either
axis, `DistribThresholds`). A query below both stays fully local, so a cheap
query pays nothing for the machinery. When the gate trips, the coordinator
partitions the snapshot **shard-major** (`partition_snapshot`): a segment's
ingest shard is the primary grouping key, a shard's segments are never split
across slices, and whole shard groups are packed so the slice count never
exceeds `max_parallel_slices`. Partitioning is total and disjoint — every
segment lands in exactly one non-empty slice — and because the k-way merge is
order-insensitive over the flat pool of decoded runs, the specific
shard-to-slice assignment never changes the result.

Each slice is dispatched to a worker over the internal `SeriesFetch` gRPC
service. The worker executes the existing fetch path over only its listed
segments (reconstructing each `SegmentRef` from the shipped durable identity,
never trusting a wire key), applies the request's erasure predicates exactly
as the local path would, pre-merges its slice, and streams series runs back
(f64 values as raw bit patterns, so NaN payloads, -0.0, and the staleness
marker survive) ending with one terminal summary frame. A slice contributes
to the merge only after its summary arrives; partial frames from a failed
attempt are discarded whole, which is what makes re-dispatch safe with no
dedup bookkeeping. Worker membership is a heartbeat key per process with
rendezvous hashing of `(tenant_hash, signal, shard)` over the live set — no
leader, no assignment object, no new durable state — and the per-process
content-addressed read caches behave as one aggregate cache because segments
are immutable.

### Signals that fan out

The queryfrag lane distributes all five queryable signals: Metrics, Logs,
Alerts, Audit, and Spans. The worker's `run_slice_inner` decodes the request's
`signal` discriminant and dispatches to a per-signal fetch path
(`crates/ravel-query/src/distrib/service.rs`):

- **Metrics** resolves the pinned scope and fetches both scalar series (one
  `SeriesFrame` each) and native-histogram series (one `HistogramFrame` each)
  through the metric `SegmentFetcher`. As of `PROTOCOL_VERSION` 3 (ADR-0096
  decision 3 step 4) a run-merged L1 run and a native-histogram run cross the
  wire bit-exactly: `Run` and `HistogramRun` carry the four packed per-sample
  provenance columns, and `HistogramRun` also carries typed `HistogramRecord`s.
  Erasure predicates are applied to the decoded scalar and histogram series
  before they are encoded, so a fully-erased series is dropped rather than sent.
  The version gate (the request-level check plus the intra-cluster routing
  filter) guarantees only a same-version coordinator ever receives these frames,
  so the columns and records are never silently dropped by an older decoder.
- **Logs, Alerts, Audit** are one RLOG object family and share a single fetch
  path over `LogSegmentFetcher`, distinguished only by the object-key prefix the
  coordinator already resolved (the worker never re-derives it, it fetches the
  pinned segments the resolver maps). Each yields one `LogRecordFrame` per
  decoded record.
- **Spans** fetch through `SpanSegmentFetcher` (promoted into ravel-query from
  the SQL crate for this lane), the same funnel a local `spans` read uses, and
  yield one `SpanFrame` per surviving span.
- **Profiles** has no distributed path and stays single-process: the worker
  answers `Unsupported` exactly as every non-Metrics signal did before this
  amendment.

Two per-signal preconditions on the worker also degrade to local fallback
(`Unsupported`), never a wrong or partial result: a Logs/Alerts/Audit or Spans
slice on a worker with no log/span fetcher wired (both are opt-in builder
extras, `with_log_fetcher`/`with_span_fetcher`), and a log/span slice carrying
matchers (matcher pushdown for those signals has no mapping in this lane yet, so
it fails closed rather than under-filter).

### Worker-side aggregation pushdown

A Metrics `FetchRequest` may carry a `PartialAggregateRequest`
(`want_count`/`want_min`/`want_max`, plus `reduce_start_ns`/`reduce_end_ns`,
ADR-0103 decision 2 and its Amendment). The worker then returns one
`PartialAggregate` frame per series — series identity, labels, and whichever of
count/min/max was asked for, the bounds as raw f64 bit patterns — in place of
every `SeriesFrame`, under the same slice atomicity. All three value flags
false is the group-only request: identity frames with no value fields, the
distinct-group enumeration. The branch is per request, so a slice returns all
partials or all raw frames, never a mix, and a request with no
`partial_aggregate` is the unchanged raw-frame path.

`reduce_start_ns`/`reduce_end_ns` restrict the reduction to an exact
`(start, end]` window, matching the evaluator's own matrix-selector convention
(`crates/ravel-promql/src/eval.rs`'s `eval_matrix_selector`) rather than
whatever the fetched segments happen to span — the two bounds must both be
present or both absent, or the worker refuses with a typed `Internal` error.
The worker also unconditionally filters `STALE_NAN_BITS` samples out of its
merged set before counting, *after* `merge_soa_runs` runs, never before: the
merge's own dedup tie-break decides which candidate survives a shared
timestamp exactly as it does on the raw path, and filtering staleness first
could let a different candidate win that slot than the raw path resolves for
the same query.

The worker merges its own runs through the same total-order per-series merge the
coordinator runs (`merge_soa_runs`) *before* reducing, so a sample two of its
segments both carry is counted once. That local merge is what makes the partial
exact, given ADR-0103 decision 1's eligibility gate (not federated, every
resolved segment inside one shard generation's stable interval): under that gate
no other worker and no remote cluster holds runs of the same series, so nothing
is left for the coordinator's cross-worker dedup belt to reconcile. `min`/`max`
fold under `f64::total_cmp`, the ADR-0023 total order, never `PartialOrd` — but
per the Amendment, no caller combines min/max yet: `total_cmp` disagrees with
PromQL's own `min_over_time`/`max_over_time` (plain IEEE, NaN-overwrite) on NaN
and `-0.0` windows, so only `count` pushdown is wired to a caller. The terminal
summary reports the slice's real, POST-staleness-filter merged sample count
(lower than main's pre-amendment count whenever the window held a marker), so
the coordinator's sample-budget re-check works even though no sample crosses
the wire. Pushdown is metrics-only: an aggregate request on a log or span
slice, or on a slice holding native-histogram series (which have no scalar
count/min/max shape), is refused with `Unsupported` (with the real accounting
already spent, never dropped) and falls back to raw fetch.

Reachable end to end: `QueryEngine::prefetch` runs the eligibility gate,
computes the pushdown target, and sends the resulting request live on the
wire for the one eligible plan (epic #64 T4c/T4d); `MergedSource` overrides
`SeriesSource::query_precomputed_count` to serve the collected partials back
to the PromQL fast path (T4b), with `PROTOCOL_VERSION` bumped to 4 in the
same commit as the wire opt-in. A worker's `count: Some(0)` (a real,
correctly-computed zero-in-window count) is dropped before it reaches
`MergedSource`, never surfaced as a phantom zero-valued series — the same
absence-of-output-sample contract the raw path already has.

This is the engine-level (queryfrag) fetch, merge, and federation machinery for
all five signals — shipped and covered by the per-signal differential, erasure,
skew, and federation tests. The coordinator caller that actually dispatches a
Logs/Alerts/Audit/Spans distributed *search* is the SQL surface (log and trace
search runs through the `logs`/`alerts`/`audit`/`spans` tables, not PromQL); the
PromQL engine's own fetch/federation flow drives `Signal::Metrics` only today.
The SQL-lane distributed scan that drives that log and trace search is a
separate step, now landed but not yet wired into a running server; see "The
SQL-lane distributed scan" below.

### The log/span coordinator merge: order-independent, no dedup

The metrics coordinator merge is the order-insensitive k-way merge keyed on
`(series_id, ts)` with the provenance tie-break already described under Flow;
duplicates there are harmless and collapse. The RLOG-family and span merges
(`merge_log_records`, `merge_spans` in `distrib/mod.rs`) differ in one decisive
way: **they never dedup.** `docs/consistency-model.md` ("logs and spans") and
ADR-0051 section 5 are explicit that logs, alerts, audit, and spans carry no
query-time dedup — a retry after a lost ack produces byte-identical rows that
are legitimately duplicate user data and must stay visible. Every record in the
pool is returned.

Correctness rests on two facts, neither of which is slice or shard atomicity
(ADR-0052 online resharding routes one stream's or one trace's segments to
different shard indices across generations, so a query window spanning a reshard
activation can land one stream's or one trace's segments in two different
slices):

- **Segment self-containment.** Every RLOG segment embeds the resource+scope
  `stream_attrs` blob for the streams it carries, and RSPAN rebuilds a span's
  whole merged `attrs` from one segment alone, so a worker reading only its
  slice's segments produces the exact per-record/per-span merged view a local
  read produces. The coordinator never re-derives attribute merging; it only
  orders.
- **Order defined on record identity, not arrival order.** The coordinator
  stable-sorts the flat pool under a stated total order over the whole record
  content (for logs: `(ts_ns, stream_id, stream_attrs, observed_ts_ns,
  severity_num, severity_text, body, trace_id, span_id, flags, attrs)`, with
  `attrs` compared by the frozen `canonical_attr_bytes` encoding so f64 values
  compare by bit pattern; for spans: `(trace_id, span_id, start_ts_ns,
  end_ts_ns, parent_span_id, name, status_code, status_message, service_name,
  attrs)`). Sorting the flattened multiset is a pure function of that multiset,
  so the shard-major slice grouping — which differs from the local per-segment
  grouping, and which a reshard-straddling stream or trace makes differ further
  — never changes the result. The output is bit-identical to a local
  multi-segment read merged under the same order, which the differential test
  exercises with at least one generated case placing one stream's or trace's
  segments in two slices.

Erasure is applied worker-side, per segment, through the same funnel the local
path uses (`retain_series_soa` for metrics, `LogQuery` erasure for the RLOG
family, `is_erased_span` for spans); the coordinator never re-applies it.
Because each segment is self-contained, a resource-attribute-only exclusion
evaluates identically wherever the segment is read, including when one stream's
segments straddle two slices — proven by an erasure property test that diffs a
distributed slice set against a local read of the same segments.

### The SQL-lane distributed scan (logs, alerts, audit, spans)

The two sections above describe the engine-level (queryfrag) machinery. The SQL
surface has its own distributed scan that drives log and trace *search* over the
`logs`, `alerts`, `audit`, and `spans` tables (ADR-0071 task T6, shipped as T6a
#326 for the RLOG family and T6b #327 for spans;
`crates/ravel-sql/src/distributed_rlog.rs`). It fans a table scan out to worker
slices instead of scanning the local snapshot, then merges the per-slice streams
at the coordinator.

It reproduces the same total-order, no-dedup merge rule the queryfrag lane uses,
one layer up in the DataFusion plan. Each worker returns its slice as one
globally-sorted partition under the table's total-order key, and the coordinator
runs a `SortPreservingMergeExec` under that key with **nothing above it** — no
dedup node, no distinct. Logs, alerts, audit, and spans have no query-time dedup
(`docs/consistency-model.md`, ADR-0051 section 5), so every row every slice
returns stays visible, exactly as `merge_log_records`/`merge_spans` do in the
queryfrag lane. The scan machinery (`DistributedSliceScanExec`, the merge
assembly, byte accounting, schema validation) is signal-neutral and shared
across all four tables; the only per-signal surface is each table's ordering-key
column list. The slice-ticket plumbing (`WorkerSlice`, `WorkerSliceClient`,
`plan_distributed_slices`) is reused unchanged from the metrics SQL lane, since
a slice ticket pins a snapshot subset and carries no signal discriminator.

Reachability: the distributed scan is installed on a table provider through each
provider's `with_distributed_scan` (`LogsTableProvider` and the
alerts/audit/spans siblings) and is exercised end to end by the acceptance tests
(`crates/ravel-sql/tests/flight_distributed.rs`) driving `provider.scan(..)`. It
is **not yet wired into a running server's coordinator and worker paths**: the
server-side coordinator that installs the distributed context from
`get_flight_info_statement`, and the worker `do_get` slice-fragment branch that
runs each provider's `worker_fragment`, are still later wiring. The SQL lane
exists and is tested, but no live server binary reaches it yet.

### Budgets and the fault matrix

The coordinator re-enforces the query's budgets over the folded per-slice
accounting, so a worker that under-reports or lies about its own spend cannot
overrun the caps: the distinct-series cap is checked as each slice's series
arrive, and the bytes-scanned cap is checked against the saturating fold of
every slice's reported cost (saturating, never wrapping, so a counter near
`u64::MAX` clamps rather than slipping under the cap). Every slice's real
spend is folded into the query's live accounting handle before any failure or
fallback, so the reported cost reflects work already paid for — a query that
fetches remotely and then re-runs locally (a version-skew fallback) reports
both, never one.

Failures map to the same typed outcomes the local path produces:

- **Worker unreachable or mid-stream loss:** re-dispatch the slice to the
  next rendezvous worker once, then run it on the coordinator, then fail
  typed. Never a partial merge.
- **`SnapshotInvalidated`** (a pinned segment vanished under concurrent
  GC/compaction): all invalidated slices collapse to the single retryable
  error the local path raises for a vanished segment, so the engine
  re-resolves the snapshot and re-dispatches the whole query once, not once
  per slice; a second occurrence fails.
- **`Corrupt`:** terminal immediately, no retry, matching local semantics
  (retrying could mask corruption behind a clean local read).
- **Budget trip** on a slice or on the folded total: the same typed
  `TooManySeries` / `TooManyBytesScanned` errors the local path produces.
- **Deadline:** the coordinator deadline (already bounded by the `sys/gc`
  `max_query_duration` that keeps workers inside the GC protection horizon)
  cancels the fan-out; stream teardown reaches workers and drop-based
  cancellation frees their GETs and permits.
- **Protocol version mismatch** (rolling deploy), a **`Profiles` signal**, an
  **unwired log/span fetcher**, or a **matcher-bearing log/span slice:** the
  worker answers `Unsupported`, and the
  coordinator silently falls back to fully local execution for the whole
  query — never an error, never a partial result. This is the load-bearing skew
  direction: a *new* coordinator against an *old* worker that predates the
  log/span fan-out sees `Unsupported` for Logs/Alerts/Audit/Spans and degrades
  to local execution, so the new `LogRecordFrame`/`SpanFrame` variants never
  reach a coordinator that cannot decode them (an *old* coordinator never
  dispatches these signals). Intra-cluster execution is all-or-nothing; only
  cross-cluster federation (below) ever returns partial.

Fragment admission runs under a separate internal workload class with its own
cap, so a coordinator holding a client-query permit can never deadlock
waiting on fragments that need the same pool.

## Cross-cluster federation (ADR-0071)

A coordinator can fan a read out to remote clusters and merge their series
into the same k-way merge that unions its own selectors (the pool described
under Flow). Federation is opt-in per deployment: each remote is one
repeatable `--remote-cluster` flag on `ravel-server`, whose value is a
comma-separated spec (`name`, `endpoint`, and `credential-file` required;
`tls`, `tls-ca-file`, `skip-unavailable`, and `soft-timeout` optional).
`--remote-cluster-soft-timeout <duration>` sets the default bound on how
long the coordinator waits on a remote before treating it as partially
unavailable, and a remote's own `soft-timeout` key overrides it. A
deployment with no `--remote-cluster` flag runs exactly the single-cluster
path. The operator guide is docs/guides/distributed-query.md.

Both the value-bearing endpoints (`/api/v1/query`, `/api/v1/query_range`)
and the discovery endpoints (`/api/v1/series`, `/api/v1/labels`,
`/api/v1/label/<name>/values`) federate, through the SAME `Federation`
coordinator and with identical semantics. A discovery request
sends its matchers and window to each remote; the remote resolves under its
own tenant auth, enforces its own admission/limits/erasure, and returns
series whose identities are unioned into the local discovery pool. The only
difference is what each keeps from the remote's response: the query path
keeps sample runs and merges them per timestamp, while the discovery path
keeps only `(series_id, labels)` identities (it enumerates series, not
samples). `skip_unavailable`, the skipped-cluster warnings, and the partial
marker behave identically on both paths: `skip_unavailable=false` fails the
request typed on a remote failure or timeout, and `=true` continues, names
the skipped cluster in the response envelope's `warnings`, and sets the
partial-coverage marker on the query stats. A single-cluster deployment runs
the byte-identical local discovery path (`fetch_all_series`); no federation
context means an empty, cheap fan-out.

### Two credential models, one wire type

ADR-0071 fetches carry a `Scope` on the `FetchRequest`. The two values are
not two transports; they are two trust boundaries over the same gRPC
service, and the distinction is a security invariant, not an optimization:

- **`Pinned` (intra-cluster slice).** A worker inside the coordinator's own
  cluster is handed a short-lived fragment token and the already-resolved
  `tenant_hash` on the wire. The worker trusts the coordinator: it uses the
  wire `tenant_hash` directly. This token is a slice credential, never a
  cross-cluster credential — a remote cluster must reject it (see the
  `federation_rejects_the_fragment_token` test in `distrib.rs`).
- **`Resolve` (cross-cluster federation).** A remote cluster is a separate
  trust domain. The coordinator authenticates to it with an ordinary
  per-remote tenant credential (the `credential` on `RemoteClusterConfig`),
  and the remote resolves the tenant from *its own* `TenantResolver` applied
  to that credential — never from the wire `tenant_hash`, which it
  overwrites with the locally resolved value. A federated request therefore
  cannot name a tenant the presented credential does not authorize on the
  remote, exactly as a direct client request to that remote could not.

`RemoteClusterConfig`'s `Debug` is hand-written to print `credential:
<redacted>` so a config dump never leaks the operator secret.

### Partial coverage is always surfaced, never silent

A federated query can return a correct-but-incomplete result when a remote
is slow (soft-timeout) or unreachable (`skip_unavailable`). This is never
silent: the query stats carry `partial: true` and one operator-facing
warning per degraded remote, merged into the Prometheus JSON envelope's
`warnings`. The warnings name only the operator-facing cluster name; remote
IP:port and errno are redacted out, because a client that reads the envelope
is not entitled to the coordinator's internal topology.

As of `PROTOCOL_VERSION` 3 (ADR-0096 decision 3 step 4) a native-histogram
frame is real data this build decodes and merges, so a malformed `Hist` frame
is corruption, never a coverage gap: it fails the query typed as a
`Federation` "malformed response" fault regardless of `skip_unavailable`, the
same as a malformed scalar frame. This is sound because the version gate (the
request-level check on the remote and, intra-cluster, the routing filter)
guarantees a `Hist` frame only ever reaches a coordinator whose version matches
the sender's, so if one arrives and fails to decode that is real corruption, not
a version-skew coverage gap. A version-skewed remote instead answers
`Unsupported` for the whole request (below), which is the coverage-gap path.

A remote that answers `Unsupported` for the whole query is the federation analog
of the intra-cluster skew fallback: a
remote resolves its own snapshot, so `Unsupported` can only mean "this cluster
does not serve this query kind yet" — an availability/coverage property, not a
wrong-data one. The concrete cases are a remote running code that predates the
log/span fan-out (it rejects a Logs/Alerts/Audit/Spans query with `Unsupported`,
its `run_slice_inner` rejecting every non-Metrics signal) and a remote on an
older `PROTOCOL_VERSION` (its request-level version check answers `Unsupported`).
Federation routes that
through the same `skip_unavailable` path an unavailable
remote takes: under `skip_unavailable=true` the cluster is skipped, coverage is
marked partial, and the warning names only the operator-facing cluster (the
remote's internal status text is redacted out); under `=false` it fails with a
typed `Federation` error naming the cluster. Treating it as a non-skippable hard
fault would defeat exactly the availability `skip_unavailable` exists to provide
during a mixed-version fan-out rollout. Federation composes over all five fan-out
signals with no code beyond the per-signal `SliceFetcher`, and a per-signal
federation test proves it rather than assuming it.

#### The engine API cannot hand a caller a value without its coverage

Warnings in a response envelope only help a caller that reads an envelope,
so the engine API carries the same signal in its types (ADR-0071 "partial
results are consent-gated and envelope-visible" amendment, decision 4). The
bare convenience wrappers `QueryEngine::{instant, range, resolve_series}`
return their value paired with a `#[must_use] Coverage`
(`Complete | Partial { skipped }`), derived by `Coverage::from_stats` from
the same `QueryStats` their `_with_stats` sibling returns. Nothing new is
tracked; the wrappers simply cannot drop what the fan-out already recorded.
A caller that does not care must bind the coverage to a name, which puts the
decision to ignore it where review can see it. The `_with_stats` and
`_with_stats_annotated` variants are unchanged and remain the source for the
wire rendering.

The alert evaluator (`services/ravel-server/src/alerting.rs`, `run_query`)
is the caller this matters most for, and it treats `Coverage::Partial` as a
failed evaluation (amendment decision 5): the rule is logged, counted in the
tick's `rules_failed`, its prior alert state is left untouched, no transition
record is written, and the next tick retries. A rule can therefore never fire
or resolve on data a skipped cluster never returned. There is deliberately no
per-rule opt-in to evaluate on partial coverage; that policy belongs with the
alerting surface's own work.

### Cross-cluster duplicate tie-break limitation

The merge resolves a duplicate `(series_id, ts)` under the total order in
docs/catalog-and-mvcc.md (created_unix_ns, writer_epoch, writer_seq,
in-page index). Those provenance fields are meaningful only *within* one
cluster: two clusters can mint the same `(writer_epoch, writer_seq)` for
unrelated writes. Federation therefore assumes disjoint series identity
across clusters — the intended deployment is region- or tenant-sharded, so
one series lives in exactly one cluster. When the same `(series_id, ts)`
does arrive from two clusters with different values, the winner is
unspecified (whichever the total order happens to order first); the merge
still emits exactly one sample per timestamp and never duplicates or
crashes. Making a cross-cluster tie-break deterministic on value would need
a cluster-identity component in the order and is out of scope for this wave.
Inventing a cross-cluster provenance scheme is a separate ADR, deliberately
not attempted here: the behavior above is *defined* only for
disjoint cross-cluster series identity, and this limitation is recorded both
here and as a code comment at the merge site (`is_greater` in
crates/ravel-query/src/engine.rs).

The discovery endpoints (`/api/v1/series`, `/api/v1/labels`,
`/api/v1/label/<name>/values`) federate too, but their
cross-cluster union carries no such ambiguity. Discovery enumerates series
*identity*, and a `SeriesId` is a canonical function of a series' labels, so
the same id from two clusters always carries the same `LabelSet`: the union
is a plain set union of identities with no per-sample provenance to tie-break
on, and duplicate cross-cluster identities collapse cleanly regardless of
which cluster's writers minted the samples. The provenance-tuple limitation
above is therefore specific to the sample-merge path; the discovery union is
well-defined even for mirrored-ingest deployments.

## Query cost accounting and estimate (ADR-0044)

Every query carries a `ravel_types::accounting::QueryAccounting` handle,
created once per query attempt and passed explicitly (never thread-local
or task-local, since the fetch path fans out across `buffer_unordered`).
It is recorded at existing funnels only, never at scattered call sites:

- `SegmentFetcher::guarded_get` (crates/ravel-query/src/fetcher.rs), for
  every metric-path store call: `s3_requests`/`s3_bytes` split by op
  (get/list/head), `bytes_reused` (bytes served from an already-fetched
  region without a second GET), `decompressed_bytes`, `segments_opened`,
  `series_matched`.
- `LogSegmentFetcher::fetch_accounted` (crates/ravel-query/src/
  log_fetcher.rs), so the log path can be accounted like the metric path
  instead of being silently free. `engine.rs` has no references to
  `LogSegmentFetcher` at all: the real production callers (ravel-sql's
  `logs_provider`, `alerts_scan`, `audit_scan`, `audit_provider`) still
  call the unaccounted `fetch`. Wiring them onto this funnel is future work.
- `Catalog::guarded_get`/`guarded_list_all` (crates/ravel-catalog), for
  every catalog-side store call `Catalog::resolve` makes on the query's
  behalf: `s3_requests`/`s3_bytes` for the resolve LISTs and GETs, plus
  `cache_hits`/`cache_misses`/`cache_bytes` for the record cache.
  `QueryEngine::resolve_bounded` calls `resolve_pruned_with_accounting`
  with the same handle the query attempt reports, so these counters are
  live in the snapshot today, not zero.

A `QueryAccountingSnapshot` (point-in-time copy via `.snapshot()`) is
attached to `QueryStats` alongside the existing `segments_fetched`/
`segments_pruned`/per-run `FetchStats` (`raw_f64_pages`/`raw_f64_bytes`,
narrower than `decompressed_bytes`: only `ValPageKind::RawF64` pages).

### Pre-execution cost estimate

The estimate has two parts, computed at two different moments, because
one moment cannot cover both (ADR-0044 decision 3, amended).

**The catalog term**, computed before `Catalog::resolve` runs:
`Catalog::estimated_catalog_requests` bounds the store requests resolve
will issue before it has listed anything, from inputs the planner already
has going in. Two pieces: one LIST per `(shard, hour)` pair — `shard_count`
times the number of hour buckets the padded window spans, with no snapshot
HEAD to shorten the listed suffix — plus
`SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`, a fixed constant covering the
snapshot-window path (`Catalog::resolve_snapshot_window`) that
`resolve_impl` always tries first whenever the window is non-empty, before
any LIST runs. This term is folded into `estimated_requests`
unconditionally, including when the window resolves to zero segments, so a
fully-pruned window still carries a non-zero request estimate against its
non-zero actual request count.

`SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND` is not a structural bound and is the
one open gap in this estimate (see its doc comment in catalog.rs): it
covers one HEAD GET (always attempted), one part GET (capped at 1 because
every writer today emits exactly one part (v1 writes exactly one part,
even though the wire format's `repeated SnapshotPartRef parts` allows
more)), and one postings GET (worst case, for
an equality `__name__` filter). Part count is only knowable after the HEAD
GET this constant exists to avoid, so a future multi-part writer (the
sharding escape hatch the format already reserves) would silently make
this an under-estimate again. ADR-0044 decision 3 does not account for the
snapshot-acceleration path at all; this is flagged there as an open
question, not resolved by a data-dependent bound here.

**The segment term**, computed after `Catalog::resolve` returns a pinned
snapshot and before any page fetch: `engine::estimate_cost` computes the
rest of the `CostEstimate` from fields already on that snapshot's
`SegmentRef`s (`object_size`, `sample_count`, `series_count`); nothing
about the fetch plan needs to run first because it is deterministic
given the matchers.

Both terms are an upper envelope, never a prediction: every quantity the
planner cannot bound exactly takes the worst case, because
under-estimating is the failure that matters once a later ADR enforces a
budget on this number.

Constants (crates/ravel-query/src/engine.rs):

- `OPEN_REQUESTS_PER_SEGMENT = 2` — `open_segment`'s footer GET plus the
  worst-case one extra `NeedRange` chase.
- `CATALOG_REQUESTS_PER_SEGMENT = 4` — the sparse catalog-probe path's
  worst-case coalesced GET count over LABEL_DICT/SERIES_IDS/
  SERIES_META_CHUNKS/SERIES_IDX; the whole-object and below-threshold
  paths cost strictly less.
- `PAGE_REQUESTS_PER_RUN = 2` per matched series — a TS run and a VAL (or
  HIST) run, before `ensure_ranges`' `covers()` dedup can ever reduce it.
- `STORE_BYTES_SAFETY_FACTOR = 2` applied to `object_size` — covers
  re-reading a segment across a retry (snapshot invalidation) without
  claiming a tighter per-page bound than the catalog can prove pre-fetch.
- `Catalog::estimated_catalog_requests(window, now_ns)` — the catalog term:
  `shard_count * hour_buckets_spanned` LISTs plus
  `SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND` (3) for the snapshot-window path,
  computed pre-resolve.
- `segment_decompressed_bytes_upper_bound` applied per segment for
  `estimated_decompressed_bytes` — derived from the segment's own value
  kinds rather than one constant, per ADR-0044's amended requirement.
  `SegmentRef::sample_count` aggregates scalar and native-histogram
  samples with no split by kind, so this bounds every sample at
  `ReaderLimits::max_page_uncompressed_bytes` (the page-size ceiling no
  decoded sample can structurally exceed), capped again by twice
  `ReaderLimits::max_section_uncompressed_bytes` (a segment carries at
  most one VAL_PAGES and one HIST_PAGES section). This never
  under-shoots, but is deliberately loose for scalar-only segments —
  closing that gap needs a persisted per-kind sample count, which is a
  frozen-format change, not something this estimate can derive today.

`prefetch` fans out one independent fetch per selector against the same
shared snapshot, so `estimate_cost` takes a `fetch_multiplier` (the
selector/plan count; 1 for the single-fetch `resolve_series_inner` path)
and scales every estimated quantity by it — an N-selector query can cost
up to N times a single per-segment pass.

Each query records the estimate and the actual accounting snapshot side
by side, so the estimate's accuracy is itself measurable before anything
enforces it. `CostEstimate::divergence` computes the actual/estimated
ratio per quantity for that purpose. The estimate is recorded but never
enforced: nothing in this change rejects a query that runs today.

### Tracing spans

`tracing::instrument`/`debug_span!` spans on the query path, named for the
phase they cover: `catalog_resolve` (`Catalog::resolve_impl` in
`ravel-catalog`), `segment_open` (`open_segment`), `catalog_decode`
(`decode_selected`), `page_fetch`
(`fetch_scalar_pages`/`fetch_histogram_pages`), `decode`
(`build_scalar_decodes`/`build_histogram_decodes`), `evaluate` (wrapping
the evaluator call in `instant_inner`/`range_inner`). `catalog_resolve`
lives on the catalog's own resolve body rather than ravel-query's
`resolve_bounded` wrapper, so every caller of `Catalog::resolve*` gets it
— including ravel-sql's executor, which calls
`resolve_pruned_with_accounting` directly and never reaches the ravel-query
wrapper.

Each phase span also records the per-span byte/request counts ADR-0044
decision 5 requires, scoped to that call's own work rather than the whole
query. Except on `catalog_resolve`, these come from values local to the
phase — the bytes and request count of the GET/decode calls that
invocation makes itself — not from a before/after `QueryAccounting`
snapshot delta. A delta would be wrong for every fetch/decode phase: the
segment futures run concurrently over one shared `QueryAccounting` handle
(`buffer_unordered` in `engine.rs`), so a sibling segment's GETs land
between one phase's two snapshots and the delta would fold their cost into
this span. These GET counts are store-sourced only: `guarded_get` routes
cache-eligible ranges through the ADR-0046 read cache, and a cache hit
serves bytes with no store round trip at all (`record_cache_hit`, never an
`AccountedOp::Get`), so it contributes zero requests and zero bytes to the
span. Each `guarded_get` call returns its own `{requests, bytes}` cost —
`{1, len}` for a store GET (the uncached path, a cache miss's leader, or a
single-flight follower riding another caller's in-flight GET, matching the
log path's rule below), `{0, 0}` for a cache hit — and the caller folds
those in, so the store-vs-cache decision lives once, at the seam that
already knows it. `segment_open` sums the store-sourced cost of its own one
or two `guarded_get` calls; which of those can reach zero on a warm cache
depends on the first GET's shape. A segment at or below
`DEFAULT_WHOLE_OBJECT_THRESHOLD` reads its whole object with a
`GetRange::Full` first GET, which is cache-eligible, so both its GETs (and
thus the whole `segment_open` cost) are zero on a fully warm cache. A
segment above the threshold reads its footer with a `GetRange::Suffix`
first GET, which `guarded_get`'s `cacheable_range` never routes through the
cache (a suffix has no total size to fabricate a hit from), so that GET
crosses the network on every run and `segment_open`'s cost is never zero
for such a segment regardless of cache warmth. `page_fetch`
records the store-sourced count and bytes `ensure_ranges` reports for the
coalesced GETs it issued on that call; those are `GetRange::Range` GETs,
which are cache-eligible, so this phase's cost is the one that reaches zero
on a warm cache even for an above-threshold segment (a warm range adds
nothing); `decode`
sums each `decode_run`/`decode_histogram_run`'s own decompressed output. Only `catalog_resolve` still uses a snapshot delta, and correctly:
it runs once per query handle, sequentially, before any concurrent fetch
work starts, so nothing else writes the handle across its window. The
fields: `s3_requests`/`s3_bytes` on `catalog_resolve`, `segment_open`, and
`page_fetch`; `segments_pruned` on `catalog_resolve`; `series_matched` on
`catalog_decode`; `decompressed_bytes` on `decode`. The log-signal path
(`LogSegmentFetcher::fetch_accounted` and the production
`fetch_accounted_with_tenant`) carries the same `page_fetch` and `decode`
span names. What `page_fetch` records there depends on the object's size
(ADR-0107): at or below the block-range threshold, its one whole-object GET,
or zero on a cache hit; above it, the total store GETs the block-range
sequence issued (probe, directory sections, coalesced candidate-block
ranges) and the bytes they moved, again zero when every extent was a cache
hit.

Span fields otherwise carry only bounded values — `tenant_hash` as a hex
string, `object_size`, matcher/series counts, and fixed-set kind strings
(`page_kind`, `eval_kind`) — never query text, label values, object keys,
or `shard` (ADR-0044 decision 5's rejected alternative 6: shard is not a
label), the same allowlist ADR-0044 sets for `/metrics` labels.

### JSON response shape

`GET /api/v1/query` and `/query_range`'s `stats` object gains two new
fields alongside the existing `segmentsFetched`/`segmentsPruned`:

- `stats.accounting` — the `QueryAccountingSnapshot`, split by op for the
  S3 counters (`s3GetRequests`/`s3GetBytes`, `s3ListRequests`/
  `s3ListBytes`, `s3HeadRequests`/`s3HeadBytes`), plus `cacheHits`/
  `cacheMisses`/`cacheBytes`, `decompressedBytes`, `segmentsOpened`,
  `seriesMatched`, `bytesReused`, `peakIntermediateBytes`, and the
  pre-existing per-run `rawF64Pages`/`rawF64Bytes`. No `segmentsPruned`
  here: `stats.segmentsPruned` (below) is the sole source, sourced from
  `Catalog::resolve`'s own count; `QueryAccounting`'s own
  `segments_pruned` counter has no caller in `ravel-query` or
  `ravel-catalog` and would only ever render 0.
- `stats.estimate` — the `CostEstimate`: `estimatedRequests`,
  `estimatedStoreBytes`, `estimatedDecompressedBytes`, `segments`,
  `series`.

## PromQL conformance (ADR-0035)

What Ravel supports, what it deliberately refuses, and what is simply
untested, one row per construct. The classification is ADR-0035's:

1. **supported** -- implemented, with a passing test proving it. The Evidence
   column names that test: a corpus file in
   `crates/ravel-promql-difftest/corpus/` when the differential harness
   exercises the construct, or a `ravel-promql` unit test when it does not
   (which the row also reports as a corpus gap).
2. **intentionally rejected** -- refused with a typed error, never a panic and
   never silently wrong data. Every row here is proved by a rejection case in
   `crates/ravel-promql-difftest/tests/conformance_table.rs`, which asserts
   the response is a Prometheus error envelope with a stable `errorType`, a
   non-empty message, and no data payload.
3. **accepted divergence** -- a difference from Prometheus an ADR already
   investigated and accepted. These are not misses and are not re-triaged;
   the row cites the ADR.
4. **unclassified** -- implemented but untested, or claimed-supported but
   actually failing. The actionable bucket: every row here is a ticket.

The score is states 1, 2, and 3 over the full enumerated surface, so it can
neither be inflated by defining misses out of the surface (unclassified rows
stay in the table) nor deflated by constructs Ravel never claimed (a verified
typed rejection is conformant).

Surface: the 72 functions and 12 aggregation operators promql-parser 0.10
marks non-experimental, all 16 binary operators, and the AST node and modifier
categories below. Prometheus' experimental functions and aggregators
(`limitk`, `limit_ratio`, `info`, `double_exponential_smoothing`,
`sort_by_label`, `mad_over_time`, `first_over_time`, `histogram_quantiles`,
the `ts_of_*` family) are out of the scored surface: they are not part of the
stable language, so including them would move the denominator every time
Prometheus promotes one. `Expr::Extension` is out of scope for the same kind
of reason -- it is promql-parser's hook for downstream dialects and has no
Prometheus counterpart -- and Ravel rejects it with a typed
`Error::Unsupported` regardless.

One note on those out-of-surface constructs: promql-parser 0.10 parses the
experimental aggregation operators `limitk` and `limit_ratio`, so a tenant
can reach them on the query path even though they are outside the scored
surface. `ravel-promql`'s aggregation dispatch rejects both with a typed
`Error::Unsupported` naming the operator, never a panic, honoring the state-2
"rejected, not panicking" guarantee. They stay out of the scored
surface (they are not part of the stable language), and are not implemented;
the clean rejection is what the guarantee requires.

The table below is generated from a run, not hand-maintained: the state
column is recomputed from which corpus entries actually exercise each
construct and whether they actually passed, so a regression appears as a diff
in the same change that caused it. Everything between the markers is
overwritten on regeneration; the prose above and below survives.

<!-- BEGIN GENERATED PROMQL CONFORMANCE TABLE -->

Generated by `cargo test -p ravel-promql-difftest --test
conformance_table`; regenerate that same command with
`RAVEL_UPDATE_CONFORMANCE_TABLE=1`. Do not edit the block between
the markers by hand.

Surface: 133 constructs over 242 corpus entries in 10 corpus files.

| State | Constructs |
| --- | --- |
| supported | 124 |
| intentionally rejected | 7 |
| accepted divergence | 2 |
| unclassified | 0 |
| **score** (supported + intentionally rejected + accepted divergence / total) | **133/133 = 100%** |

| Construct | Category | State | Evidence |
| --- | --- | --- | --- |
| aggregate expression | ast node | supported | `corpus/aggregate.txt`, 20 entries |
| binary expression | ast node | supported | `corpus/binop.txt`, 31 entries |
| function call | ast node | supported | `corpus/transform.txt`, 59 entries |
| matrix selector | ast node | supported | `corpus/selectors.txt`, 4 entries |
| number literal | ast node | supported | `corpus/binop.txt`, 16 entries |
| paren expression | ast node | supported | `corpus/selectors.txt`, 2 entries |
| string literal | ast node | supported | `corpus/transform.txt`, 6 entries |
| `subquery` | ast node | supported | `corpus/subquery.txt`, 11 entries |
| unary expression | ast node | supported | no difftest corpus entry; proven by `ravel-promql`'s `unary_minus_negates_value_and_drops_metric_name` |
| vector selector | ast node | supported | `corpus/selectors.txt`, 31 entries |
| @ <timestamp> | modifier | supported | `corpus/selectors.txt`, 2 entries |
| @ end() | modifier | supported | `corpus/selectors.txt`, 1 entry |
| @ start() | modifier | supported | `corpus/selectors.txt`, 1 entry |
| binary operator over native histograms | modifier | intentionally rejected | `Unsupported: binary operator over native histograms (422 execution)`; rejection verified; the binop evaluator (`combine_value` and its callers) only ever reads a sample's plain float `value`, which is a meaningless 0.0 placeholder for a histogram element (issue #524); guarded before any value combination so the fabricated-zero result is never produced. `corpus/binop.txt`'s `error_*_over_histogram` entries (mode: ravel_error_prom_success) additionally pin that Prometheus itself succeeds here, so this is a real capability gap, not a shared limitation |
| `bool` | modifier | supported | `corpus/binop.txt`, 7 entries |
| `by` | modifier | supported | `corpus/aggregate.txt`, 11 entries |
| `group_left` | modifier | supported | `corpus/binop.txt`, 2 entries |
| `group_right` | modifier | supported | `corpus/binop.txt`, 1 entry |
| `ignoring` | modifier | supported | `corpus/binop.txt`, 1 entry |
| instant matrix selector over native histograms | modifier | intentionally rejected | `Unsupported: instant query returning a range vector of native histograms (422 execution)`; rejection verified; a bare matrix selector is a valid instant query whose top-level result is a range vector; Ravel's instant `Value::Matrix` is float-only (the histogram-aware channel is the range endpoint's `RangeValue`, ADR-0108 decision 8), so a selector matching native-histogram data refuses in `eval_expr`'s `MatrixSelector` arm (issue #643) rather than silently dropping the histograms at HTTP 200; distinct from the subquery-over-histograms refusal, which triggers inside `eval_subquery_matrix` |
| label matcher != | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher !~ | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher = | modifier | supported | `corpus/selectors.txt`, 8 entries |
| label matcher =~ | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher or-group | modifier | intentionally rejected | `Unsupported: label matcher or-group (422 execution)`; rejection verified; `{a="1" or b="2"}` is a Prometheus experimental matcher form; ravel-promql's `matchers::has_or_group` refuses it before any matching runs rather than silently dropping a branch |
| negative offset | modifier | supported | `corpus/selectors.txt`, 1 entry |
| `offset` | modifier | supported | `corpus/selectors.txt`, 4 entries |
| `on` | modifier | supported | `corpus/binop.txt`, 7 entries |
| subquery over native histograms | modifier | intentionally rejected | `Unsupported: subquery over native histograms (422 execution)`; rejection verified; the subquery grid reducer keeps only each step's float value, so a histogram element would be silently dropped; the trigger is matched histogram data, not the syntactic shape |
| vector matching fill values | modifier | intentionally rejected | `Unsupported: vector matching fill-in values (422 execution)`; rejection verified; `fill`/`fill_left`/`fill_right` are a promql-parser dialect extension with no Prometheus counterpart; ravel-promql's `binop` refuses the modifier rather than evaluating it as plain matching |
| `without` | modifier | supported | `corpus/aggregate.txt`, 1 entry |
| `!=` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `%` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `*` | binary operator | supported | `corpus/binop.txt`, 6 entries |
| `+` | binary operator | supported | `corpus/binop.txt`, 5 entries |
| `-` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `/` | binary operator | supported | no difftest corpus entry; proven by `ravel-promql`'s `scalar_scalar_arithmetic_and_bool_comparison` |
| `<` | binary operator | supported | no difftest corpus entry; proven by `ravel-promql`'s `scalar_vector_filter_and_bool_both_directions` |
| `<=` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `==` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `>` | binary operator | supported | `corpus/binop.txt`, 4 entries |
| `>=` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `^` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `and` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `atan2` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `or` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `unless` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `avg` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `bottomk` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `count` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `count_values` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `group` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `max` | aggregation operator | supported | `corpus/aggregate.txt`, 2 entries |
| `min` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `quantile` | aggregation operator | supported | `corpus/aggregate.txt`, 2 entries |
| `stddev` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `stdvar` | aggregation operator | supported | `corpus/aggregate.txt`, 1 entry |
| `sum` | aggregation operator | supported | `corpus/aggregate.txt`, 6 entries |
| `topk` | aggregation operator | supported | `corpus/aggregate.txt`, 2 entries |
| `abs` | function | supported | `corpus/transform.txt`, 1 entry |
| `absent` | function | supported | `corpus/transform.txt`, 3 entries |
| `absent_over_time` | function | supported | `corpus/over_time.txt`, 3 entries |
| `acos` | function | supported | `corpus/transform.txt`, 1 entry |
| `acosh` | function | supported | `corpus/transform.txt`, 1 entry |
| `asin` | function | supported | `corpus/transform.txt`, 1 entry |
| `asinh` | function | supported | `corpus/transform.txt`, 1 entry |
| `atan` | function | supported | `corpus/transform.txt`, 1 entry |
| `atanh` | function | supported | `corpus/transform.txt`, 1 entry |
| `avg_over_time` | function | supported | `corpus/over_time.txt`, 2 entries |
| `ceil` | function | supported | `corpus/transform.txt`, 1 entry |
| `changes` | function | supported | `corpus/rate.txt`, 2 entries |
| `clamp` | function | supported | `corpus/transform.txt`, 6 entries |
| `clamp_max` | function | supported | `corpus/transform.txt`, 2 entries |
| `clamp_min` | function | supported | `corpus/transform.txt`, 2 entries |
| `cos` | function | supported | `corpus/transform.txt`, 1 entry |
| `cosh` | function | supported | `corpus/transform.txt`, 1 entry |
| `count_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `day_of_month` | function | supported | `corpus/transform.txt`, 1 entry |
| `day_of_week` | function | supported | `corpus/transform.txt`, 1 entry |
| `day_of_year` | function | supported | `corpus/transform.txt`, 1 entry |
| `days_in_month` | function | supported | `corpus/transform.txt`, 2 entries |
| `deg` | function | supported | `corpus/transform.txt`, 1 entry |
| `delta` | function | supported | `corpus/rate.txt`, 2 entries |
| `deriv` | function | supported | `corpus/rate.txt`, 1 entry |
| `exp` | function | supported | `corpus/transform.txt`, 1 entry |
| `floor` | function | supported | `corpus/transform.txt`, 1 entry |
| `histogram_avg` | function | supported | `corpus/histogram_native.txt`, 1 entry |
| `histogram_count` | function | supported | `corpus/histogram_native.txt`, 5 entries |
| `histogram_fraction` | function | supported | `corpus/histogram_classic.txt`, 3 entries |
| `histogram_quantile` | function | supported | `corpus/histogram_classic.txt`, 8 entries |
| `histogram_stddev` | function | intentionally rejected | `Unsupported: function call (422 execution)`; rejection verified; not in ravel-promql's function registry; native-histogram dispersion is not implemented |
| `histogram_stdvar` | function | intentionally rejected | `Unsupported: function call (422 execution)`; rejection verified; not in ravel-promql's function registry; native-histogram dispersion is not implemented |
| `histogram_sum` | function | supported | `corpus/histogram_native.txt`, 3 entries |
| `hour` | function | supported | `corpus/transform.txt`, 2 entries |
| `idelta` | function | supported | `corpus/rate.txt`, 1 entry |
| `increase` | function | supported | `corpus/rate.txt`, 2 entries |
| `irate` | function | supported | `corpus/rate.txt`, 1 entry |
| `label_join` | function | supported | `corpus/transform.txt`, 2 entries |
| `label_replace` | function | supported | `corpus/transform.txt`, 4 entries |
| `last_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `ln` | function | supported | `corpus/transform.txt`, 1 entry |
| `log10` | function | supported | `corpus/transform.txt`, 1 entry |
| `log2` | function | supported | `corpus/transform.txt`, 1 entry |
| `max_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `min_over_time` | function | supported | `corpus/over_time.txt`, 2 entries |
| `minute` | function | supported | `corpus/transform.txt`, 1 entry |
| `month` | function | supported | `corpus/transform.txt`, 1 entry |
| `pi` | function | supported | `corpus/transform.txt`, 1 entry |
| `predict_linear` | function | supported | `corpus/rate.txt`, 2 entries |
| `present_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `quantile_over_time` | function | supported | `corpus/over_time.txt`, 3 entries |
| `rad` | function | supported | `corpus/transform.txt`, 1 entry |
| `rate` | function | supported | `corpus/rate.txt`, 5 entries |
| `resets` | function | supported | `corpus/rate.txt`, 1 entry |
| `round` | function | supported | `corpus/transform.txt`, 2 entries |
| `scalar` | function | supported | `corpus/transform.txt`, 2 entries |
| `sgn` | function | supported | `corpus/transform.txt`, 1 entry |
| `sin` | function | supported | `corpus/transform.txt`, 1 entry |
| `sinh` | function | supported | `corpus/transform.txt`, 1 entry |
| `sort` | function | supported | `corpus/transform.txt`, 1 entry |
| `sort_desc` | function | supported | `corpus/transform.txt`, 1 entry |
| `sqrt` | function | supported | `corpus/transform.txt`, 1 entry |
| `stddev_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `stdvar_over_time` | function | supported | `corpus/over_time.txt`, 1 entry |
| `sum_over_time` | function | supported | `corpus/over_time.txt`, 2 entries |
| `tan` | function | supported | `corpus/transform.txt`, 1 entry |
| `tanh` | function | supported | `corpus/transform.txt`, 1 entry |
| `time` | function | supported | `corpus/transform.txt`, 1 entry |
| `timestamp` | function | supported | `corpus/transform.txt`, 2 entries |
| `vector` | function | supported | `corpus/transform.txt`, 6 entries |
| `year` | function | supported | `corpus/transform.txt`, 1 entry |
| float-precision residue (ULP tolerance) | accepted divergence | accepted divergence | ADR-0025, 11 corpus entries; arithmetic and transcendental residue between two independent implementations of the same formula; each entry carries a written justification for its tolerance in the corpus file |
| subquery per-node point cap | accepted divergence | accepted divergence | ADR-0030, 2 corpus entries; Ravel's per-subquery-node 11,000-point budget has no Prometheus counterpart, so Ravel rejects by design where Prometheus accepts; the comparator asserts exactly that shape |
<!-- END GENERATED PROMQL CONFORMANCE TABLE -->

## SQL over logs (the `logs` table, ADR-0033)

`POST /api/v1/sql` serves two tables from one endpoint: `samples`
(`Signal::Metrics`) and `logs` (`Signal::Logs`). There is no separate logs
endpoint and no protocol change. DataFusion does not choose between two
registered tables: `SqlExecutor` decides which single table a query targets by
parsing its `FROM` clause *before* planning (through the same parser the
read-only gate uses, never a raw-text scan), resolves a snapshot for that one
signal, and registers exactly that one table in the per-query `SessionContext`
-- `Signal::Logs` when the query references `logs`, `Signal::Metrics`
otherwise. A `WITH <name> AS (...)` CTE that happens to be named `logs` or
`samples` is a query-local name, not a base-table reference, and does not
change the target (`WITH logs AS (SELECT value FROM samples) SELECT count(*)
FROM logs` is a metrics-only query). A query naming both real tables is
rejected before any catalog listing (HTTP 400): v1 admits one signal per
query, and no query needs to scan or join metrics and logs together.

Schema (fixed columns plus one map):

- `ts`, `observed_ts` — `Timestamp(ns)`.
- `severity_num` — `UInt8`; `severity_text`, `body` — `Utf8`.
- `trace_id` — `FixedSizeBinary(16)`, `span_id` — `FixedSizeBinary(8)`, both
  nullable; `flags` — `UInt32`.
- `attrs` — `Map(Utf8, Utf8)` carrying each record's resource, scope, and
  per-record dynamic attributes merged into one map. On a key collision the
  per-record value wins over the resource/scope value. Positional scope name
  and version are excluded (no synthetic `scope.name`/`scope.version` keys).

Supported predicates (all pushdown is widen-only; DataFusion always re-applies
the original predicate above the scan, so pruning can only ever widen the fetch,
never drop a true result):

- `ts` range comparisons — exact segment-level pruning from the catalog
  summary, the same shape as the metrics table's `ts` bounds.
- `has_word(body, 'literal')` — a word/phrase content search whose SQL
  semantics equal the RLOG reader's exact token filter, so it both pushes down
  (bloom-accelerated pruning inside the scan) and needs no residual correction.
  A plain `LIKE '%word%'` pattern's literal is recognized but is **not** pushed
  as a prune, because token matching is not a superset of SQL substring `LIKE`.
- `attrs['k'] = 'v'` — an attribute equality. `extract_logs` sends it to a
  prune-only channel that drives POSTINGS block pruning (ADR-0049). The channel
  never becomes the reader's per-row filter, so it cannot drop a resource-only
  match. The merged `attrs` residual evaluates the equality exactly.
  `attrs['k'] IN (...)` stays unextracted, because an `IN` list is a
  disjunction the intersecting prune channel cannot represent soundly.
- A predicate on a **declared typed column** (ADR-0093). `extract_logs` resolves
  a bare `Expr::Column` whose name is not one of the nine fixed columns against
  the tenant's declared vocabulary and dispatches on the declared type:
  - `i64`/`bool` with `<`, `<=`, `>`, `>=`, `=`, or `BETWEEN` against a
    matching-type literal → a prune-only `Predicate::NumRange` with bit-pattern
    bounds, driving the RLOG skip index's per-block `NumStat` min/max (ADR-0095,
    #331). `status_code > 500` and `is_active = true` now skip blocks that
    provably hold no matching value.
  - `i64` `IN (v1, v2, ...)` → ONE envelope `NumRange` spanning `[min, max]`, not
    one arm per value (the reader intersects prune arms, so a per-value arm would
    drop rows the full `IN` keeps). The envelope is coarser than the exact set,
    so a value between the listed ones is excluded by the residual, never the
    prune. DataFusion's simplifier rewrites a small `IN` into a same-column
    `col = v1 OR col = v2 OR ...` disjunction before the scan; that shape maps to
    the same envelope.
  - `str`/`bytes` `=` against a matching-type literal → the same
    `Predicate::Equals` the `attrs['k'] = 'v'` shape builds, driving POSTINGS.
  Everything else — `!=`, `NOT`, negated `BETWEEN`, `IS [NOT] NULL`, a general
  `OR`, a range operator on a `str`/`bytes` column, a `str`/`bytes` `IN`, or a
  type-mismatched literal (including one DataFusion coercion has already wrapped
  in a `Cast`, which is no longer a bare column) — is not extracted and scans as
  before. Two soundness caveats are inherited, not new: the equality half
  declines all pruning on a POSTINGS section written before the #333 write-path
  fix (a section-version gate, so no per-predicate code carries it), and inherits
  the over-conservative rule that a name also carrying a non-`str` column
  anywhere declines equality pruning entirely (a performance no-op, not a
  correctness gap). The `NumRange` half has no version gate: under ADR-0095's
  single-version RLOG regime every stat is merged-view-correct, and a block with
  no stat for the column (an object predating the declaration) is scanned, never
  pruned (ADR-0013). `f64` is not reachable yet (no declared `f64` type); its
  future arm must honor `NumRange`'s float contract (widen a zero-including range
  across both `0.0`/`-0.0` bit patterns, never build a bound from a NaN literal),
  which the i64/bool code does not need.

### Declared typed attribute columns (ADR-0090)

An operator can declare a per-tenant set of attribute keys as native typed
columns, appended after `attrs` (schema index 9 onward) in declaration order.
The column name is the attribute key verbatim, never mangled, so a key
containing `.` or uppercase characters needs double-quoting in SQL. Four
declarable types: `str` (`Dictionary(Int32, Utf8)`), `i64` (`Int64`), `bool`
(`Boolean`), and `bytes` (`Binary`). A declared `str` column is
dictionary-encoded end to end (ADR-0099 decision 5): a dict-encoded RLOG page
becomes the Arrow dictionary and its ids with no per-row allocation, a plain
page becomes a degenerate identity dictionary, and the row fallback path builds
the same dictionary type so every batch validates against the one schema
DataFusion checks. The public Flight statement stream sets
`DictionaryHandling::Resend`, so the column stays a dictionary on the wire
rather than being hydrated back to plain `Utf8`. Over HTTP JSON the row *values*
are identical to the pre-dictionary form (`output.rs` unwraps the
`Dictionary(Int32, _)` cell and recurses to a string per row), but the JSON
envelope's declared `columns[].type` changes from `Utf8` to
`Dictionary(Int32, Utf8)`, and the Arrow IPC schema and batch columns carry the
dictionary type verbatim (`to_arrow_ipc` hands the schema and batches to the
writer unchanged). Both are client-visible: a consumer reading the reported
column type or the IPC schema must expect the dictionary type. `f64` is deferred
(a declared float aggregate is
order-sensitive under ADR-0013/ADR-0022, which #277 decides), and so are date
and timestamp (they need a lifting rule from `i64` storage plus a declared
unit); the four shipped types all aggregate exactly under any partitioning.

A declared key stays in `attrs` too, so `SELECT attrs` and `SELECT *` keep
working for a tenant that adopts typed columns after querying through the map.
A row whose stored value has a different type than the declaration reads NULL:
there is no cast, ever, and the raw value stays reachable through
`attrs['key']`. A query naming a column the tenant has not declared fails at
planning with an unknown-column error, never silently as NULLs.

Resolution happens once per plan, at the entry point that carries both the
tenant and the query's `now_ns` (`SqlExecutor::execute`/`run` for HTTP, Flight
SQL's `get_flight_info`), and the resolved list is threaded down as a plain
parameter. Flight pins it into the ticket, so the paired `DoGet` streams
against the exact schema `get_flight_info` planned; a refresh in between cannot
change it. The declaration itself comes from two places: the process flags
`--typed-attr-column`/`--typed-attr-column-tenant`, and the durable per-tenant
`TenantConfig.typed_attr_columns` override (written by `ravel-cli
typed-attr-column set`), which replaces the flag-derived list for that tenant
when present -- including when present and empty, which means "declares
nothing". A query process reads that override cache-aside on a 60s staleness
horizon and never fails a query on an unreadable config object; it serves the
last resolved declaration instead and counts
`ravel_typed_attr_columns_stale_fallback_total`. See
[guides/operations.md](guides/operations.md#declared-typed-attribute-columns-adr-0090)
for the operator-facing contract.

Predicates on a declared column are pushed down as prune-only arms (ADR-0093):
an `i64`/`bool` comparison or `BETWEEN` and an `i64` `IN` drive the skip index,
and a `str`/`bytes` equality drives POSTINGS, exactly as the "Supported
predicates" list above describes. The pushdown is always `Inexact`, so the typed
comparison is still re-evaluated exactly as a residual filter above the scan; the
prune only changes which blocks the fetch decodes. A declared column referenced
by the query's projection -- which DataFusion also folds residual-filter columns
into -- is decoded by the scan's column selection, so `WHERE` on a declared
column decodes only the pages it needs. Moving an equality predicate from
`attrs['k'] = 'v'` to a declared `k = 'v'` now prunes through the same POSTINGS
index (subject to the section-version caveat above). Declare for typed
comparisons and aggregates, which the map cannot express at all.

There is also a wire-size trade for a *high-cardinality* `str` column. ADR-0099
decision 5's "leaves that case exactly as expensive as it is today" is a
statement about CPU, not egress: a plain (all-distinct) page becomes a
degenerate identity `Dictionary(Int32, Utf8)`, and with `DictionaryHandling::Resend`
that dictionary is sent as its values *plus* a 4-byte `Int32` key per row, and
re-emitted for every batch a block larger than the batch size splits into. For a
column whose values barely repeat, that is larger on the wire than the old plain
`Utf8` column, which sent each value once with no key. Declaring a
high-cardinality attribute (a URL, a request id) as `str` is therefore a
CPU-neutral but egress-heavier choice; it is the low-cardinality case (a status,
a region) where the dictionary both saves allocation and shrinks the wire. This
is a known trade, not a regression to fix here.

### Scan execution: streaming and column projection (ADR-0087)

`LogsScanExec` streams. A partition opens its segments one at a time, decodes
one RLOG block, emits that block's rows as a batch, and releases the block
before decoding the next. Peak memory is therefore a function of block size and
partition count, not of table size. Before ADR-0087 the scan collected a whole
partition's records in row form, sorted them, and only then emitted batches, so
a full-table scan's peak memory grew with the table.

#### How many partitions, and the read-cache precondition (ADR-0102)

What a partition owns is *blocks*, not whole segments: every
`(segment, surviving-block)` pair is flattened into one list and unit `i` goes to
partition `i % n`. So a query touching fewer segments than `target_partitions`
can still fan out past the segment count, which the old segment-granular rule
(`min(target_partitions, segment_count)`) made impossible.

That fan-out is gated on the logs fetcher carrying ADR-0046's read cache, which
is the precondition ADR-0102 decision 1 names for it:

- **Cache wired** (`ravel-server` built a read cache and called
  `LogSegmentFetcher::with_cache`, i.e. `--disable-cache` is off): the scan
  declares `target_partitions` partitions, even where the snapshot has fewer
  segments than that.
- **No cache** (`--disable-cache`, or any embedding that never calls
  `with_cache`): the scan declares `min(target_partitions, segment_count)`, the
  pre-ADR-0102 bound.

The reason is the fetch unit. Each partition that owns blocks in a segment opens
that segment itself, so each issues its own read sequence at that key. Which
sequence depends on the object's size (ADR-0107,
`--logs-block-range-threshold`, 512 KiB by default): at or below the threshold
one whole-object GET; above it a suffix probe, one GET per directory section,
and coalesced GETs covering only the candidate blocks skip-index pruning kept.
With the cache, every GET of either shape is keyed by the extent it fetched and
coalesces through single-flight — the whole object below the threshold, and the
probe, each section, and each block above it — so `n` partitions striping one
segment cost one request per distinct extent rather than `n` sequences. Without
a cache each is a real object-store GET, and partitions beyond the segment count
would multiply GETs with nothing absorbing them. Note what the cap does and does
not buy: it stops `target_partitions` from setting the multiplier, but below the
cap a plan with `n` partitions still opens each sufficiently-blocky segment `n`
times, and the planning prune that counts surviving blocks adds one more read
per segment. `ravel-bench`'s `logs_scan_scaling` report measures the cached and
un-cached request counts side by side, on above-threshold and whole-object read
shapes alike; its figures describe that fixture (a `MemoryStore`, and on the
cached rows a cache sized to hold the whole dataset), not striping in general.
Its rows put a number on the residual multiplier and show where it no longer
applies: each row carries reads per segment and bytes fetched over dataset
bytes, derived in the report rather than left to the reader. For a
block-predicate-free statement whose window contains every relevant segment —
the report's `SELECT ts, body FROM logs`, and the shape the whole-segment fast
path serves (#693 part 3, amended by #739) — those two figures are about 1
(the reads figure divides every accounted GET by the segment count, so the
resolve's one catalog probe lifts it slightly above 1: 1.03 on the 32-segment
fixture) and 1.0 at every partition count that fits inside the segment count,
on both read shapes and with or without a cache: since #739 dropped the block-range-threshold
conjunct, object size no longer gates the fast path, so the plan phase is skipped
and each segment is read whole once, and there is nothing left for the cache to
absorb. The multiplier reappears only on the cache-wired row whose partition
count exceeds the segment count, which falls back to plan-then-stripe — and even
there only on the above-threshold read shape, since a whole-object stripe
coalesces onto the one `(0, object_size)` cache key.
`crates/ravel-query/tests/log_block_range.rs` pins the
above-threshold cached case exactly: on its 906,791-byte fixture, 2 partitions
and 8 partitions striping one segment both cost 6 store GETs (one probe, four
directory sections, one coalesced candidate run), and 8 concurrent cold
partitions cost the same 6.

The peak-memory consequence follows the same gate, with a second term above the
block-range threshold. Concurrently-held *decoded* memory is bounded by block
size times the number of partitions decoding at once, so only the cached
configuration raises that bound above the old
`min(target_partitions, segment_count)` one — an un-cached deployment's bound is
unchanged by ADR-0102. Raw bytes behave differently on the two read shapes: at
or below the threshold every partition shares one cached whole-object `Bytes`
(a cheap clone), while above it each partition assembles its own object-sized
buffer with only the fetched extents populated, so resident raw bytes are
`n × object_size` even though bytes on the wire are pruning-proportional. The
same test measures it: 8 partitions × 906,791 bytes = 7,254,328 resident bytes
for one segment. The per-query DataFusion pool enforces the decoded bound either
way: a partition count that would exceed the budget fails the query rather than
spilling (ADR-0013).

Two consequences an operator and a plan reader both see:

- **The scan declares no output ordering.** It used to declare `ts` ascending
  per partition, which it earned by sorting the collected partition.
  `RlogReader` emits a segment's records grouped by `(stream_ref, ts)`, not
  globally by `ts`, and a partition spans several segments, so a
  block-at-a-time scan cannot truthfully claim that order. `ORDER BY ts` still
  returns correctly sorted results; the ordering now comes from a `SortExec`
  DataFusion inserts above the scan, visible in `EXPLAIN`. Any downstream
  operator that relied on a sort-preserving merge over logs-scan partitions
  gets that explicit sort instead.
- **Projection reaches the reader.** The scan's output schema *is* DataFusion's
  requested projection; there is no `ProjectionExec` above it discarding
  columns the scan already decoded. The decoded column set is the projected
  columns, plus `ts`/`stream_ref` (always), plus every field a pushed content
  predicate names, plus every attribute key a pending erasure predicate names
  (ADR-0064). `read_block` decompresses and decodes only those columns' pages.
  Because `attrs` is one merged map column, a query referencing `attrs` at all
  -- `SELECT *` included -- resolves to every dynamic column plus the
  `attrs_raw` overflow; per-key `attrs['k']` projection is not implemented.
  Skip-index, POSTINGS, and bloom pruning are unchanged: they read stored
  statistics, not decoded pages.

`LogsScanExec` publishes `pages_decoded` and `pages_skipped` per partition
alongside its block counters, so `EXPLAIN ANALYZE` shows how much of each block
the projection avoided touching.

Two decode-time byte counters sit next to those page counts on the query's
`QueryAccounting` handle (ADR-0107 decision 4): `page_bytes_fetched`, the stored
(post-compression) bytes of every page present in the blocks a scan decoded,
regardless of the projection; and `page_bytes_decoded`, the stored bytes of only
the pages the `ColumnSelection` kept. `LogSegmentScan::finish` folds each scan's
`ScanStats` totals into the handle once, at exhaustion. Their gap is the
column-filtering waste: for an all-columns scan they are equal; for a
wide-schema, narrow-projection query `page_bytes_decoded` can be a small
fraction of `page_bytes_fetched`.

These are a decode-time measurement, a **different axis** from the wire bytes the
`page_fetch` span and `s3_bytes` record (and from `BlockRangeStats::
block_bytes_fetched`, the T1 block-range fetch counter above): they count stored
page bytes that a fetched block already holds, not bytes moved over the network,
so under version 3 a projection changes `page_bytes_decoded` without changing
any wire counter. Under version 4 (ADR-0699 decision 5) the projection IS the
fetch selection, so the wire counters shrink with it and the two axes move
together; the gap between them is then only the pruned-page holes coalescing
chose to fetch. Reading them together tells both stories in one place -- how
many bytes the fetch brought in, and how many of the bytes already resident
the decode actually needed. The instrument predates the version-4 fetcher and
was what measured whether block-level pruning alone captured enough before
PAGE_DIR shrank the wire fetch to columns.

The predicate-free full-window whole-segment fast path (#693 part 3, above)
is the one shape where the two axes never move together, by design (#790).
It issues exactly one whole-object `GetRange::Full` GET per segment
regardless of projection -- that request count is the fast path's entire
reason to exist, and #790 does not change it -- so `page_bytes_fetched`
always equals the object's full stored page bytes, on every projection.
What #790 fixed is `page_bytes_decoded`: `scan_whole_accounted_with_tenant`
already threaded the caller's `ColumnSelection` into the same
`decode_v4_block` page-level skip every other read path uses, so a one-column
scan (`SELECT ts, body FROM logs`, the report's own statement shape) was
already decoding only that column's pages on a version-4 object -- but
nothing pinned it, so the fast path's decode-time narrowing had no regression
coverage and could have silently regressed to decoding every page without
any test failing. `narrow_projection_decodes_only_wanted_columns`
(`crates/ravel-query/src/log_fetcher.rs`) now pins it: a one-column
projection over a 6-block, 8-column-per-block fixture decodes exactly 3
pages per block (`ts`, `stream_ref`, `body`) against 8 for
`ColumnSelection::all()` on the same object, and the difference matches
`pages_skipped` exactly. A user observes the effect in `pages_decoded`/
`page_bytes_decoded` on `ScanStats` (`EXPLAIN ANALYZE`'s per-partition page
counters) and in reduced peak decode memory, never in request count or wire
bytes -- those stay exactly where the #693-part-3 fast path already put them. The
"one GET per segment" figure counts one logical `GetRange::Full` read; a warm
read cache serves it with zero store requests and zero wire bytes, as the
tracing section states.
Reachable from any predicate-free, fully-contained, narrow-projection
statement that takes the fast path. `SELECT ts, body FROM logs` is the
worked example below: two selected SQL columns, and the reader also decodes
the required `stream_ref`, which is why three pages per block survive and not
one.
Object size does not gate it: since #739 the fast path is chosen for a
segment at or below the block-range threshold too, which is what the
request-count paragraph above already states.

The per-query DataFusion memory pool now bounds concurrently-held scan memory:
the reservation grows when a decoded block and the batch built from it are held
and shrinks as each is released. It is not a cumulative-output budget, and
raising it does not change how much a full-table scan holds at one instant.

Whole-object GET is unchanged: this bounds decoded memory, not the raw bytes an
object fetch brings into RAM. Per-block ranged reads are
`RlogRangeReader`'s territory (used by compaction) and are not on the SQL read
path.

#### TopK late materialization (ADR-0774)

Projection pushdown only helps a query that asks for few columns. A
`SELECT <wide projection> ... WHERE ... ORDER BY ... LIMIT k` asks for all of
them, and gets them for every row, to return `k`. On the ClickBench reference
tenant (100M rows, 8,424 objects, 105 declared columns) that is the difference
between a query and a timeout:

| statement | before | expected after |
|---|---|---|
| `SELECT COUNT(*) FROM logs WHERE URL LIKE '%google%'` | 22.9 s | unchanged (the rule does not fire) |
| `SELECT ts, URL FROM logs WHERE URL LIKE '%google%' ORDER BY ts LIMIT 10` | 24.8 s | unchanged (projection already narrow) |
| `SELECT * FROM logs WHERE URL LIKE '%google%' ORDER BY ts LIMIT 10` (q24) | exceeds a 900 s deadline | within 1.5x of the two-column variant |

The first two bound the third: the count proves the scan and the substring
`LIKE` cost about 23 s, and the two-column ordered variant proves the sort adds
about 2 s. `prune=0`/`content=0` in that plan is correct and not a missed
pushdown -- a substring `LIKE` is neither a block prune nor a bloom probe -- so
every block is a candidate and the scan decodes all 105 columns of every one of
them before the filter or the TopK sees a row. The whole difference is the 103
columns nobody looks at until ten rows have been chosen.

`TopKLateMaterialization` is a physical optimizer rule (installed by
`build_session` when `SqlConfig::late_materialization_extra_columns` is `Some`,
which it is by default at 8) that splits such a plan in two. It fires only on a
`SortExec` that has a `fetch`, does not preserve partitioning, reaches a
`LogsScanExec` through nothing but filters and schema-preserving plumbing, and
projects more than the threshold's worth of columns beyond what its filter and
sort read. An aggregate or join in between, a sort with no fetch, an
already-narrow projection, and any RSEG or spans scan all leave the plan alone.

```text
LogsRowFetchExec: row_ref=__ravel_row_ref, restored_columns=41
  SortExec: TopK(fetch=10), expr=[ts@0 ASC NULLS LAST], preserve_partitioning=[false]
    FilterExec: like(body@1, %NEEDLE%)
      CooperativeExec
        LogsScanExec: partitions=1, content=0, prune=0, projection=[ts, body, __ravel_row_ref]
```

Both phases are visible in one `EXPLAIN`. Phase 1 is the scan line: the same
TopK over the same filter, but over a scan projecting only `ts` and `body` plus
`__ravel_row_ref`, a synthetic non-nullable `UInt64` column packing each row's
`(segment ordinal, surviving-block position, surviving-row position)` -- all
three cursor state the scan already holds, so nothing is decoded to produce
them. Phase 2 is `LogsRowFetchExec`: it groups the at-most-`k` surviving row
refs by block, re-opens exactly those blocks with the original column selection
through the same accounted fetch entry point the striped scan path uses, and
emits the rows in phase-1 order under the original schema. `restored_columns`
is that schema's width. No projection node above it drops the row ref, because
the fetch node's own output schema is the restored one; the rule runs under
DataFusion's `schema_check`, so that equality is asserted for every query.

The result is identical to the un-rewritten plan's, ties included: phase 1's
TopK is the same operator over the same rows in the same order, and phase 2
does not sort. What makes a row ref resolvable is that pruning never consults
the column selection, so both phases see the same surviving blocks in the same
order, and the object is immutable with the etag pinned across the read. The
rule declines under a pending selective erasure, where scan-layer exclusion
would shift surviving-row positions between the phases.

What it costs is at most `k` logical block fetches (winners in one block
share a fetch); the object-store requests behind them depend on the fetch
path, as the next paragraph says. Those are accounted like any other scan read,
and `LogsRowFetchExec` publishes `row_refs`, `blocks_fetched`, and
`segments_fetched` per `EXPLAIN ANALYZE` so a report can state phase 2's cost
instead of inferring it. With a read cache wired and an object below the
block-range threshold they land on the cache key phase 1 already admitted and
cost no request at all; with `--disable-cache` they are one GET each.

Requests and bytes do not say the same thing here. The fetch entry point phase
2 uses restricts the *decode* to the named block, not the byte fetch, which is
the query's ordinary fetch for that object: a whole-object GET at or below the
block-range threshold, and above it the version-4 coalesced ranges over the
fetch-side candidate set -- every block, for a query whose only predicate is a
residual the skip index cannot decide. So phase 2 moves up to `k` objects'
bytes, not `k` blocks'. Narrowing that is a ravel-query follow-up (ADR-0774
consequences).

`crates/ravel-sql/tests/logs_topk_late_materialization.rs` pins all of it: on
its 16-block, 41-column fixture phase 1 decodes 3 pages per block against the
single-phase plan's 39 over the same 16 blocks, and un-cached, a `k = 10`
statement moves 103,606 bytes in 14 GETs against a single pass's 29,645 in 4.

Both gaps ADR-0033 recorded are now closed. Both were deliberate, not
oversights.

1. ~~**`attrs['k']` subscript planning is not wired.**~~ **Closed.** This
   crate's DataFusion is built with `features = ["sql"]` only, so no
   nested-expression `ExprPlanner` (`GetFieldAccess`/`NamedStructField`) came
   registered, and the `attrs['k']` subscript failed query planning with a
   loud `GetFieldAccess not supported` error. `map_field_planner.rs` now
   registers a hand-written `ExprPlanner` for the map-field case, so
   `attrs['k'] = 'v'` plans and answers. Enabling DataFusion's `nested_expressions`
   feature would have done the same thing at the cost of a much larger
   dependency surface for one expression form.

   The planner is registered in `session.rs`'s logs session and covered end to
   end at the HTTP boundary
   (`services/ravel-server/tests/sql_endpoint.rs::a_logs_attrs_subscript_query_succeeds_over_http`),
   not only against a session the crate's own tests build. Gap 2 below is
   what remains, and it is about pruning, not planning.
2. **A stream-attribute equality prunes through a prune-only channel.**
   `attrs` merges resource, scope, and per-record attributes. On a key
   collision the record wins. A stream-level (STREAM_DIR) match therefore
   cannot prune the equality: it would drop a record whose match lives only in
   its per-record attributes, and no residual restores a dropped record. The
   reader's per-record `Equals` reads a record's own column and `attrs_raw`
   overflow only. It never reads the resource or scope blob. So it matches a
   strict subset of the merged equality. To evaluate it as the reader's exact
   per-row filter drops every resource-only match.

   ADR-0049 closes the gap in two layers. Layer 1 is the index: a version 2
   POSTINGS section prunes blocks exactly on an indexed attribute value in the
   merged view (resource, scope, and per-record attributes, the record winning
   on a key collision) -- the same view `attrs` exposes. Layer 2 is a
   prune-only channel: `extract_logs` sends `attrs['k'] = 'v'` to
   `LogsPushdown::prune`, and `RlogReader::scan_pruned` applies that channel to
   POSTINGS block pruning alone. The channel never feeds the per-row filter. If
   the POSTINGS index does not cover a field, the channel prunes nothing, so it
   stays widen-only (ADR-0013). DataFusion's `Inexact` residual still evaluates
   the equality exactly over the merged `attrs` column. The prune changes which
   blocks the fetch reads. It does not change which rows the query returns.

   Version 1 (the description below applies to version 1 objects only) indexed
   the per-record layer only. For those objects the channel declines one arm: a
   key that also appears at resource or scope level anywhere in the object. One
   record carrying a key per-record makes it an indexed column for the whole
   object, including for records whose value for that key lives in their
   resource blob. Those records are in no version-1 posting list, so probing
   the term would prune their block away. An exact index over one layer cannot
   prune a query over the union of two, so the reader declines to prune that
   key on a version 1 object and the residual does the work alone. Declining is
   widen-only; pruning wrongly is not.

   That exclusion cost the common case on version 1. `service.name` is a
   resource attribute in ordinary OTLP, so a version 1 object holding it that
   way prunes nothing for it. Version 2 removes the cost by indexing the merged
   view, so a merged-view prune on such a key is sound and applied directly; the
   exclusion is kept for version 1 objects, which are not rewritten. The move to
   the merged view is the ADR-0049 amendment, a change
   to the POSTINGS grammar version only, not the trailer version.

   `ravel_query::LogQuery` carries a `prune` field (with
   a `with_prune` builder) that the fetch hands to `RlogReader::scan_pruned`,
   and `LogsScanExec` fills it from `LogsPushdown::prune`. A live
   `SELECT ... FROM logs WHERE attrs['k'] = 'v'` now prunes blocks through
   POSTINGS. A `LogQuery` with an empty `prune` reads exactly what it read
   before the channel existed.

   What an operator sees change is cost, not answers. `LogsScanExec` publishes
   `blocks_total`, `blocks_scanned`, and `blocks_pruned_by_postings` per
   partition, so `EXPLAIN ANALYZE` shows whether a query pruned; a selective
   equality on an indexed key reads a fraction of the blocks it read before, and
   returns the same rows. A key the object does not index reports
   `blocks_pruned_by_postings=0` and reads everything, which is the widen-only
   fallback, not a failure.

## SQL over spans (the `spans` table, ADR-0045)

`POST /api/v1/sql` serves a third table alongside `samples` and `logs`:
`spans` (`Signal::Spans`). It follows the same one-signal-per-query rule the
logs table established. `SqlExecutor` decides the target table from the `FROM`
clause before planning, resolves a snapshot for that one signal, and registers
exactly the one table in the per-query `SessionContext`. A query naming two real
tables is rejected before any catalog listing (HTTP 400). A CTE named `spans` is
a query-local name, not a base-table reference.

The operator-facing walkthrough, with worked queries, is
[docs/guides/traces.md](guides/traces.md). This section is the reference: the
exact schema, the exact pushdown, and which predicate prunes where.

Schema (fixed columns plus one map, `crates/ravel-sql/src/spans_schema.rs`):

- `trace_id` — `FixedSizeBinary(16)`, non-null; `span_id` —
  `FixedSizeBinary(8)`, non-null.
- `parent_span_id` — `FixedSizeBinary(8)`, nullable (NULL on a root span, never
  zero-filled).
- `name` — `Utf8`, non-null (the span/operation name).
- `start_ts`, `end_ts` — `Timestamp(Nanosecond, None)`, non-null.
- `status_code` — `UInt8`, non-null: the stored OTLP byte, `0=Unset`, `1=Ok`,
  `2=Error`. A `UInt8` for the same reason `logs.severity_num` is one: a tiny
  fixed enum a plain integer round-trips exactly. Map it to text in SQL when a
  caller wants the string.
- `status_message` — `Utf8`, nullable.
- `attrs` — `Map(Utf8, Utf8)`: the span's already-merged resource, scope, and
  span attributes. RSPAN stores that merged map directly, so unlike `logs`
  there is no separate stream-identity blob to fold in at scan time.
- `service_name` — `Utf8`, nullable: populated from `attrs["service.name"]`
  (NULL when the span has no such attribute). RSPAN v4 stores it as a
  block-local dictionary column (ADR-0054); the scan exposes it as a plain
  column so `WHERE service_name = '...'` is pushdown-eligible.
- `duration_ns` — `Int64`, non-null: **computed** `end_ts - start_ts`, never a
  stored column (ADR-0045 decision 5, rejected alternative 3). Both endpoints
  are already stored, so materializing the difference per row would add bytes to
  answer a question the block bounds answer for free. It is exposed as a SQL
  column so `WHERE duration_ns > 5e8` is expressible, and the pushdown maps it
  onto each block's stored duration bounds.

Supported predicates (all pushdown is widen-only; `supports_filters_pushdown`
returns `Inexact` for every filter, so DataFusion always re-applies the original
predicate above the scan, and pruning can only ever widen the fetch, never drop
a true result). Six shapes are recognized; every other predicate contributes no
prune and is evaluated as a residual only:

- `trace_id = <literal>` — the RSPAN fast path (ADR-0041). The literal is a
  16-byte binary or a 32-character hex string. It compiles to a
  `SpanQuery::trace` lookup, and the skip index drops every block whose
  `[min_trace_id, max_trace_id]` range excludes the target: a bounded
  single-trace scan instead of a full window scan.
- `start_ts` / `end_ts` range comparisons (`>=`, `>`, `<`, `<=`, `=`, and
  `BETWEEN`) — both columns fold into one inclusive `[ts_min, ts_max]` window
  the reader prunes blocks against by time-interval overlap. Folding both
  endpoints into one window is a widen, never a narrow, because `end_ts >=
  start_ts` on every record.
- `duration_ns` range comparisons — fold into a `[lo, hi]` window exactly as
  the ts window folds, pruned against each block's `min/max_duration_ns` (RSPAN
  v2 skip-index fields). Strict `>`/`<` use `checked_add`/`checked_sub`.
- `status_code = <literal>`, `status_code IN (...)`, and a same-axis `OR` of
  `status_code` equalities — map to the skip index's status bits and prune
  against each block's one-byte `status_mask` (RSPAN v2). `status_code = 2`
  skips every block with no Error span. Multiple sibling equalities
  AND-intersect; a same-axis `OR`/`IN` unions.
- `service_name = <literal>` — a per-block `service.name` bloom probe (RSPAN
  v3, ADR-0054). A block whose bloom proves the token absent is skipped before
  decode.
- `name = <literal>` — the span-name sibling of `service_name`, the other field
  the v3 per-block bloom is built over.

Prune site by predicate: `trace_id`, `start_ts`/`end_ts`, `duration_ns`, and
`status_code` prune at the **skip index** (exact min/max ranges and the status
mask). `service_name` and `name` prune at the **bloom** (a false-positive-only
membership test, so a negative excludes a block and a positive changes nothing,
under the widen-only rule ADR-0013 established). Everything else, including
`attrs['k'] = 'v'` and any predicate on `span_id`, `parent_span_id`, or
`status_message`, prunes nothing and is evaluated exactly as a DataFusion
residual. Span attribute pruning (postings, analogous to RLOG's) is a later,
undecided epic, not a current capability.

Conjunctive shape: the pruning predicates must be top-level `AND` conjuncts. A
disjunction inside a conjunct's subtree drops that whole conjunct from pruning
(refusing is always widen-safe: the residual re-applies it). The exception is a
**same-axis** `OR`/`IN` on `status_code` or `duration_ns`, pushed as the union
of its disjuncts; a **cross-axis** disjunction (`status_code = 2 OR duration_ns
> 5e8`) is refused. `trace_id` is a single-point lookup with no range primitive
to union, so a `trace_id` disjunction is refused too.

Scan paths and their partition metrics (ADR-0110). `SpansScanExec` runs one of
two paths per partition: a columnar fast path that builds Arrow arrays straight
from RSPAN's block view, taken when the projection excludes `attrs`, no pending
erasure predicate applies, and no scanned block carries an `attrs_raw` overflow
page; and the row path, which rebuilds each `SpanRecord` and is what a query
touching `attrs` (`SELECT *` included) runs. Four partition metrics show what
happened, and `EXPLAIN ANALYZE` prints them:

- `columnar_batches` / `rowpath_batches` — batches emitted by each path. The two
  paths' output is identical by construction, so these are the only external
  proof of which one ran.
- `pages_decoded` / `pages_skipped` — column pages the partition's decode
  decompressed and walked past. **Both counters are written on both paths.** The
  row path decodes every page of every block it scans, so a direct row scan
  reports its whole page count as decoded and 0 as skipped; the columnar path
  reports the split its projection produced. One case is neither: when an
  `attrs_raw` block makes the columnar attempt fall back (ADR-0110 decision 3),
  the partition carries the abandoned attempt's counts into the row path's, so
  it reports that attempt's skipped pages as skipped and its decoded pages on
  top of the row decode. Those pages were really decompressed -- the query's
  `page_bytes_decoded` counts them too -- so the totals are the partition's
  decode work, not one arm's. A zero therefore always means the decode did that
  much, never that the arm did not count (the row arm left both unwritten before
  issue #669, and an attrs-including `EXPLAIN ANALYZE` read as a decode that
  touched nothing).

The page-byte counters on `QueryAccounting` (`page_bytes_fetched` /
`page_bytes_decoded`, ADR-0107 decision 4) sit next to these on both paths, as
they do for logs. They are the byte-weighted view of the same decode: page
counts weight every page equally, while an attribute or event page usually
carries far more bytes than a fixed-width one.

## Parallel final aggregation for exact-typed queries (ADR-0094)

A query whose aggregation is provably order- and partition-independent
(exact-typed) fans its `Final` stage across partitions behind a
hash-partitioning `RepartitionExec`; every other query plans single-partitioned:
DataFusion's `repartition_aggregations` knob stays off, so a `Partial`
`AggregateExec` runs per scan partition, a `CoalescePartitionsExec` collapses
them into one stream, and a single serial `Final` `AggregateExec` produces the
result. The single-partition plan keeps float aggregation bit-exact against the
differential gate (ADR-0013), whose reference depends on a deterministic fold
order.

This behavior is on by default (`SqlConfig::parallel_final_aggregation`, amended
to `true` on 2026-08-26 by issue #741; see the ADR-0094 amendment for the
ClickBench measurement that motivated it). It is process-wide with no
live-reload, so flipping it needs a restart, and
`--sql-parallel-final-aggregation=false` is the operator opt-out that restores
the pre-amendment single-partition final for every query (the bare
`--sql-parallel-final-aggregation` stays accepted and still means on). Admission
is per-query and decided from the query's fully type-coerced (analyzed) plan:

- **Eligible:** `count(...)` and `count(DISTINCT ...)` over any type; `sum`,
  `min`, `max` over a resolved **non-float** input; `avg`/`mean` over a
  resolved **integer** input (ADR-0825, amending the `#771` rejection below).
- **Never eligible:** `avg`/`mean` over a resolved **Float64** input (its UDAF
  runs plain IEEE f64 addition for that kind, which is not associative once a
  running sum exceeds 2^53, so it depends on the single-partition fold order);
  any `sum`, `min`, `max` over a `Float16`/`Float32`/`Float64` input; and any
  **float GROUP BY key** (including a bare `SELECT DISTINCT float_col`),
  because `-0.0`/NaN payloads are bit-significant here and no
  merge-order-stable representative bit pattern for a float group key is
  proven.

Originally `avg` over an **integer** column was excluded by the same
fold-order rule that excludes Float64 `avg`, for a reason the ADR-0094
amendment for issue `#771` describes at length: DataFusion's built-in `avg`
coerces every non-decimal, non-duration argument to `Float64` before planning
finishes, so an integer argument never reached an accumulator as an integer,
and the classifier had no resolved type by which it could tell `avg(int_col)`
from `avg(float_col)` apart. ADR-0825 changes the premise: the `avg`/`mean`
UDAF (`crate::avg`) now coerces an admitted integer argument (`Int8`-`Int64`,
`UInt8`-`UInt32`) to `Int64` and keeps it there instead of widening to
`Float64`, and sums it exactly in `i128` with checked addition. That partial
state is the real `(exact sum, count)` pair issue `#771` found missing --
carried as `(Decimal128(38, 0), Int64)` -- and integer addition is
associative, so merging it across partitions in any order reproduces the
single-partition result exactly. `avg(int_col)` and `avg(float_col)` are
therefore no longer the same analyzed node: the integer argument keeps its
Int64 type through analysis instead of widening, which is exactly what the
classifier now keys on. `avg` over a Float64 argument is unaffected and stays
excluded for the reason above.

A single disqualifying aggregate or key anywhere in the query -- including inside
a scalar/`IN`/`EXISTS` subquery -- forces the whole query onto the
single-partition plan: `repartition_aggregations` is one session-wide switch, not
a per-node choice. Any error building or analyzing the classification plan
fails closed to the single-partition plan (a missed optimization, never a wrong
result). With the opt-out set, no classification runs and every query is
single-partition, byte-identical to before this feature existed.

A **non-float `GROUP BY` key, string keys included, is eligible**: the gate
rejects only *float* group keys (and float/`avg` aggregates), so a string-keyed
`GROUP BY count(...)` is admitted and its final is repartitioned. This governs
the **final** stage only. It does not address the **partial** stage: a
high-cardinality `GROUP BY` can still exhaust the per-query memory pool at the
`Partial` `AggregateExec` -- the `partitions x distinct` pre-final state
materialized per scan partition -- even with the final repartitioned. That
partial-stage memory problem is a separate concern, tracked as issue #737 (the
`skip_partial_aggregation` mitigation); it is why some high-cardinality
statements still fail with parallel final aggregation on. Do not read this
feature as making every high-cardinality `GROUP BY` complete.

Row order is unaffected. A `GROUP BY` without an explicit `ORDER BY` has **no
row-order guarantee** in this engine -- single-partition or repartitioned, and
unchanged by ADR-0094. Under repartitioned final aggregation the physical merge
order is completion-order-dependent (nondeterministic even at a fixed partition
count), so a client that needs a stable order must add `ORDER BY`. The result
*content* is identical across partition counts for an admitted query; only
arrival order varies.

## String-keyed `GROUP BY`, and the panic boundary under it

A declared `Str` column reaches Arrow as `Dictionary(Int32, Utf8)` (ADR-0099
decision 5). That is the type a client receives, and it is deliberately not the
type the engine groups on. DataFusion 54 has no specialized group-value table
for `Dictionary`: it is absent from both the single-column dispatch in
`aggregates::group_values::new_group_values` and the `supported_type` list the
multi-column `GroupValuesColumn` is built from, so a `GROUP BY` over one falls
through to `GroupValuesRows`. That table encodes each key into arrow's
comparable row format and decodes the whole table back into one array on emit,
through `arrow_row`'s `i32` offsets. A tenant with enough distinct keys to
cross `i32::MAX` bytes of key data crosses it inside that single decode, and
nothing bounds it first: the aggregate stream slices its output to `batch_size`
only after the emit has already built the batch, and the memory pool
reservation is released before it.

`DictionaryGroupKeysAsViews` (`crates/ravel-sql/src/group_keys.rs`), a physical
optimizer rule every session installs, casts dictionary-encoded string group
keys to `Utf8View` on the aggregate that first reads them and casts the emitted
group column back to its declared type in a projection directly above the
aggregate that produces final values. `Utf8View` is in both of DataFusion's
dispatch tables, so the grouping runs on `GroupValuesBytesView`, which stores
its values in a list of 2 MiB blocks addressed per view and has no single
offset buffer to overflow. `Utf8` would also leave the row-converter path and
is deliberately not what the rule casts to: it carries the same `i32` offsets
in one buffer, so it would move the limit rather than remove it. Both casts are
cheap -- arrow builds views directly over an existing dictionary's values
buffer without copying string data -- and neither is visible to a caller: the
projection restores the column's name, position, and declared type, so the
query's result schema is identical with and without the rule. Grouping sets and
aggregates that already carry an output ordering are left on the original path;
an ordered aggregate emits incrementally (`EmitTo::First`) and is bounded
already.

Underneath that sits a guarantee that does not depend on the rewrite being
right. Every batch from every SQL transport passes through `PinnedStream`, and
its poll catches an unwinding panic raised anywhere in the plan and returns
`SqlError::OperatorPanic` instead, then fuses the stream. A panic in an
operator or in an arrow kernel it calls is a bug, and the fix belongs where it
is raised; what the boundary guarantees is that such a bug fails one query with
a typed error rather than unwinding out of the task serving it. The panic
message reaches the server log only: like `SqlError::Internal` it redacts to a
fixed client string, because a panic payload can quote whatever values the
operator was holding.

## Caching note

ADR-0046 added a content-addressed RAM read-cache tier (`ravel-cache`,
S3-FIFO eviction, single-flighted) consulted at four funnels:
`SegmentFetcher::guarded_get`, `Catalog::guarded_get`,
`LogSegmentFetcher::fetch`, and — for an RLOG object above the block-range
threshold — `BlockRangeFetcher` (ADR-0107), which routes its suffix probe, each
directory section, and each candidate block through the cache on that extent's
own key, admitting one entry per verified block and never a coalesced range.
Cache keys are `(tenant_hash, content_hash, offset,
len)`, so entries survive object-key churn and two writers producing
identical bytes share one entry. Each funnel credits its own hit/miss and
byte counters to `QueryAccounting` (ADR-0044), so a query's `EXPLAIN
ANALYZE` output distinguishes cache hits from store round trips.

Alongside the byte cache, `ravel-catalog` keeps its five decoded-structure
caches (commit records, compaction records, HEAD with a TTL, snapshot
parts, postings), now with hit/miss/byte counters and a capacity bound on
`HeadCache`.

Deliberately not cached: a suffix GET (the footer-first read on segment
open) always bypasses the byte cache, because a suffix has no total object
size to key a `(offset, len)` entry on. `ravel-cache`'s disk tier is wired
to both the RSEG/RLOG fetcher cache and the catalog byte cache: passing
`--cache-dir` attaches a RAM-over-disk `TieredCache` to each, single-
flighted and corruption-gated per ADR-0046 decisions 3-5, instead of
failing startup.

See docs/guides/caching.md for CLI flags, metrics, and known gaps, and
docs/adrs/0046-read-cache-tier.md for the funnel/keying/eviction design
and its rejected alternatives.
