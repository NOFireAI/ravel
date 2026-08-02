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
       -> lazy k-way merge of that selector's per-segment SoA runs per
          series id (each run already ascending by ts; the merge emits
          ascending, one sample per ts, no final sort). Duplicate
          timestamps (across segments and within one run) resolve under
          the total order in docs/catalog-and-mvcc.md, not by arrival
          order; max_series/max_samples are enforced independently per
          selector (each selector gets the full budget, not a shared
          split; max_segments stays a per-query cap on the shared
          snapshot)
  -> every selector's already-merged series combined into one flat
     SeriesSource (later selectors sharing a series id keep the first
     merge seen; a later per-selector SeriesSource::query call still clips
     to that selector's own window, so which selector "wins" a shared
     series id in the combine step does not affect any single selector's
     result)
  -> ravel-promql Evaluator -> Value (scalar / string / instant vector /
     range matrix)
  -> Prometheus JSON envelope {status, data:{resultType, result},
     warnings, infos}
     (warnings/infos are the evaluator's Annotations: two distinct
     Prometheus fields, both omitted when empty. A quantile argument outside
     [0,1] and a malformed classic histogram are warnings; a forced
     histogram monotonicity fixup is an info. Issue #178.)
```

`padded_range`: the union, over every selector `plan_selectors` reports, of
that selector's own fetch window (its lookback or matrix range, plus its
own offset, anchored per `PlanAnchor::Window`/`Pinned`), so lookback never
misses samples stored in an earlier-only segment for any selector, however
widely the selectors' own windows differ.

Staleness: the evaluator recognizes the Prometheus staleness marker (the
exact NaN bit pattern `0x7ff0_0000_0000_0002`, compared via
`f64::to_bits()`, never `is_nan()`). A selector whose newest in-window
sample is the marker treats the series as absent at that instant; range
windows exclude marker samples. Every other NaN payload is a live value
and passes through bit-exactly (issue #75).

## Time-range and matcher pruning order

1. Segment level: commit-record event-time bounds vs padded range (already
   done by Catalog::resolve).
2. Series level: SERIES_META entry ts bounds vs padded range, then matcher
   evaluation against the decoded LabelSet. Equality matchers use dictionary
   ordinal lookups (resolve value -> ordinal once, compare ordinals);
   regex/negative matchers evaluate on materialized label sets.
3. Page level: v1 has one page pair per series; nothing further to prune.

## Segment catalog fetch: whole-object vs sparse catalog-probe (#276)

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
trips. Epic #264 Wave 1 (BENCHMARKS.md, 2026-07-31 MinIO panel) measured
per-request latency (~1-5 ms loopback, ~15-80 ms projected real S3) but did
not meter this specific within-segment crossover, so the floor is set
conservatively (256 KiB: above the four fixed 64 KiB suffix/gap probes, far
below any real compacted sparse L1 part) rather than fit to a measured point.
The within-segment GET/byte model is the `selective_read_accounting` bench.

## Endpoints (Prometheus compatibility subset)

- `POST/GET /api/v1/query` (params: query, time, timeout) instant.
- `POST/GET /api/v1/query_range` (query, start, end, step, timeout).
- `GET /api/v1/labels`, `/api/v1/label/{name}/values` (match[] optional,
  start/end optional): from snapshot SERIES_META label dictionaries.
- `GET/POST /api/v1/series` (match[] required, start/end).
- `GET /api/v1/status/buildinfo`: Ravel's own crate version under `version`,
  never a Prometheus version string. `revision` carries the build's git SHA
  when the build environment exported `RAVEL_GIT_SHA`, otherwise it is empty,
  as are `branch`, `buildUser`, `buildDate`, and `goVersion`.
- `GET /api/v1/metadata`: always `{"status":"success","data":{}}`. Ravel
  captures no OTLP metric type/help/unit metadata, and an empty object is a
  valid Prometheus response; inventing entries would not be.
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

## Budgets (Phase 1: static config)

Per query: max segments touched (1024, a cap on the one shared snapshot,
not per selector), max concurrent GETs (8, applied both across selectors
and across one selector's own segments), max matched series (10k) and max
samples (10M) enforced independently per selector (a query with several
selectors, e.g. a future binary/aggregate expression, grants each its own
full budget rather than splitting one budget across them), max
range-evaluation points per query (11,000, matching the Prometheus
resolution limit; issue #77), wall deadline (server maximum, default
30 s). The `timeout` param can only lower the deadline: values above the
server maximum are clamped to it (issue #58). Exceeding a budget returns a
Prometheus-style error, never a partial silent result. The point cap is
enforced independently at every subquery evaluation node (`expr[5m:1m]`),
checked against that node's own grid before it is built, so a nested
subquery whose own grid alone exceeds the cap is rejected before any
allocation or recursive evaluation, at whatever nesting depth it occurs.

A per-node point cap alone does not bound the total cost of nested or
repeatedly re-evaluated subqueries: an outer range query re-evaluates a
matrix-function argument's subquery from scratch at every one of its own
grid steps, and a subquery may itself nest another subquery, so a query
whose every individual node sits safely under 11,000 points can still
multiply that cost across nesting levels and re-evaluations (issue #193).
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
support is tracked by issue #220.

The max-samples budget is **count-yielded**: samples are counted as the
lazy k-way merge emits them (post-dedup), and the budget trips at exactly
`max + 1`. It does not count a fully materialized per-timestamp window
before checking, so it does not silently pass an over-budget query that
happens to dedup down to size. The count is independent of segment or
series iteration order, so the error is deterministic. It bounds only the
output size: every matched series in every matched segment is still fully
fetched and SoA-decoded before the merge runs, so peak fetch/decode memory
scales with the query's matched input, not with `max_samples`.

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
  call the unaccounted `fetch`. Wiring them onto this funnel is issue
  #424.
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
`Catalog::estimated_list_requests` bounds the LISTs resolve will issue
from inputs the planner already has going in — `shard_count` and the
number of hour buckets the padded window spans — with no snapshot HEAD
to shorten the listed suffix, one LIST per `(shard, hour)` pair. This
term is folded into `estimated_requests` unconditionally, including when
the window resolves to zero segments, so a fully-pruned window still
carries a non-zero request estimate against its non-zero actual LIST
count.

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
- `Catalog::estimated_list_requests(window, now_ns)` — the catalog term:
  `shard_count * hour_buckets_spanned`, computed pre-resolve.
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

`tracing::instrument` spans on the query path, named for the phase they
cover: `catalog_resolve` (`resolve_bounded`), `segment_open`
(`open_segment`), `catalog_decode` (`decode_selected`), `page_fetch`
(`fetch_scalar_pages`/`fetch_histogram_pages`), `decode`
(`build_scalar_decodes`/`build_histogram_decodes`), `evaluate` (wrapping
the evaluator call in `instant_inner`/`range_inner`). Span fields carry
only bounded values — `tenant_hash` as a hex string, `object_size`,
matcher/series counts, and fixed-set kind strings (`page_kind`,
`eval_kind`) — never query text, label values, object keys, or `shard`
(ADR-0044 decision 5's rejected alternative 6: shard is not a label),
the same allowlist ADR-0044 sets for `/metrics` labels.

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

## PromQL conformance (ADR-0035, issue #133)

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
"rejected, not panicking" guarantee (#260). They stay out of the scored
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

Surface: 133 constructs over 216 corpus entries in 10 corpus files.

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
| binary expression | ast node | supported | `corpus/binop.txt`, 28 entries |
| function call | ast node | supported | `corpus/transform.txt`, 59 entries |
| matrix selector | ast node | supported | `corpus/selectors.txt`, 4 entries |
| number literal | ast node | supported | `corpus/binop.txt`, 14 entries |
| paren expression | ast node | supported | `corpus/selectors.txt`, 2 entries |
| string literal | ast node | supported | `corpus/transform.txt`, 6 entries |
| `subquery` | ast node | supported | `corpus/subquery.txt`, 11 entries |
| unary expression | ast node | supported | no difftest corpus entry; proven by `ravel-promql`'s `unary_minus_negates_value_and_drops_metric_name` |
| vector selector | ast node | supported | `corpus/selectors.txt`, 31 entries |
| @ <timestamp> | modifier | supported | `corpus/selectors.txt`, 2 entries |
| @ end() | modifier | supported | `corpus/selectors.txt`, 1 entry |
| @ start() | modifier | supported | `corpus/selectors.txt`, 1 entry |
| `bool` | modifier | supported | `corpus/binop.txt`, 7 entries |
| `by` | modifier | supported | `corpus/aggregate.txt`, 11 entries |
| `group_left` | modifier | supported | `corpus/binop.txt`, 2 entries |
| `group_right` | modifier | supported | `corpus/binop.txt`, 1 entry |
| histogram_fraction at range-query top level | modifier | intentionally rejected | `Unsupported: histogram_fraction in a range query (422 execution)`; rejection verified; same range-path boundary as `histogram_quantile` above |
| histogram_quantile at range-query top level | modifier | intentionally rejected | `Unsupported: histogram_quantile in a range query (422 execution)`; rejection verified; `eval_range`'s per-step matrix reduction has no shape for a function that groups a whole instant vector; still fully usable in an instant query and nested inside one |
| `ignoring` | modifier | supported | `corpus/binop.txt`, 1 entry |
| label matcher != | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher !~ | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher = | modifier | supported | `corpus/selectors.txt`, 8 entries |
| label matcher =~ | modifier | supported | `corpus/selectors.txt`, 1 entry |
| label matcher or-group | modifier | intentionally rejected | `Unsupported: label matcher or-group (422 execution)`; rejection verified; `{a="1" or b="2"}` is a Prometheus experimental matcher form; ravel-promql's `matchers::has_or_group` refuses it before any matching runs rather than silently dropping a branch |
| negative offset | modifier | supported | `corpus/selectors.txt`, 1 entry |
| `offset` | modifier | supported | `corpus/selectors.txt`, 4 entries |
| `on` | modifier | supported | `corpus/binop.txt`, 7 entries |
| subquery over native histograms | modifier | intentionally rejected | `Unsupported: subquery over native histograms (422 execution)`; rejection verified; the subquery grid reducer keeps only each step's float value, so a histogram element would be silently dropped; the trigger is matched histogram data, not the syntactic shape (issue #220) |
| vector matching fill values | modifier | intentionally rejected | `Unsupported: vector matching fill-in values (422 execution)`; rejection verified; `fill`/`fill_left`/`fill_right` are a promql-parser dialect extension with no Prometheus counterpart; ravel-promql's `binop` refuses the modifier rather than evaluating it as plain matching |
| `without` | modifier | supported | `corpus/aggregate.txt`, 1 entry |
| `!=` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `%` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `*` | binary operator | supported | `corpus/binop.txt`, 5 entries |
| `+` | binary operator | supported | `corpus/binop.txt`, 4 entries |
| `-` | binary operator | supported | `corpus/binop.txt`, 1 entry |
| `/` | binary operator | supported | no difftest corpus entry; proven by `ravel-promql`'s `scalar_scalar_arithmetic_and_bool_comparison` |
| `<` | binary operator | supported | no difftest corpus entry; proven by `ravel-promql`'s `scalar_vector_filter_and_bool_both_directions` |
| `<=` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `==` | binary operator | supported | `corpus/binop.txt`, 2 entries |
| `>` | binary operator | supported | `corpus/binop.txt`, 3 entries |
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
| `histogram_count` | function | supported | `corpus/histogram_native.txt`, 3 entries |
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
| float-precision residue (ULP tolerance) | accepted divergence | accepted divergence | ADR-0025, 10 corpus entries; arithmetic and transcendental residue between two independent implementations of the same formula; each entry carries a written justification for its tolerance in the corpus file |
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

Two known gaps, both inherited from ADR-0033 and deliberate, not oversights:

1. **`attrs['k']` subscript planning is not wired.** This crate's DataFusion is
   built with `features = ["sql"]` only, so no nested-expression `ExprPlanner`
   (`GetFieldAccess`/`NamedStructField`) is registered and the `attrs['k']`
   subscript **fails query planning** with a loud `GetFieldAccess not supported`
   error rather than returning a wrong answer. Filtering by an attribute value
   over SQL therefore is not usable end to end yet; `has_word` and `LIKE` do not
   depend on this and work today.
2. **A stream-attribute equality has no fetch-time prune.** Because `attrs`
   merges resource, scope, and per-record attributes with record-wins
   precedence, no stream-level (STREAM_DIR) match can soundly prune it: a
   stream-level `StreamIn` would drop a record whose match lives only in its
   per-record attributes, which no residual can recover. So `attrs['k'] = 'v'`
   is not extracted into a fetch prune at all; it is evaluated **entirely by
   DataFusion's residual** over the merged `attrs` column. The fetch still
   prunes exactly on the `ts` range and on content (`has_word`). Restoring a
   sound stream-attribute prune needs a record-attribute-aware index and is a
   deliberate follow-up (ADR-0033 Rejected Alternative A).

## Caching note

Phase 1 caches only decoded commit records (immutable) and parsed footers
keyed by (object key, etag), both in-memory LRU. Anything else waits for
measurements.
