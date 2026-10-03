# Stage 0 per-phase query time results (issue #2468, epic #2466)

## Host

```
Linux ip-172-31-18-6 7.0.0-1011-aws #11~24.04.1-Ubuntu SMP PREEMPT Mon Aug 10 15:20:57 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux
```
16 cores (`nproc`).

uptime before run:
```
 19:38:37 up 29 days, 23:18,  5 users,  load average: 0.53, 1.05, 1.12
```
uptime after run:
```
 19:42:24 up 29 days, 23:21,  5 users,  load average: 0.96, 1.10, 1.13
```
Run wall time: ~3m47s for 2 warm-up rounds + 5 runs x 5 queries x 2 queries (Q_AGG, Q_MATCH), in-memory object store (no network cost).

Command (release binary, `CARGO_TARGET_DIR` pinned by the harness to a path outside the checkout):
```
/var/lib/fleet/cache/cargo-target/243655cb-d4e3-43aa-8f56-77c15dd23c5d/release/promql_operator_share
```

Build: `CARGO_BUILD_JOBS=4 cargo build --release -p ravel-bench --bin promql_operator_share` (16 cores / 4, 30 GB RAM so above the 11 GB floor for `--jobs 2`). Compiled clean, no fixes needed. Bin ran to completion and exited 0 (its own exactness/call-count assertions all passed): matched counts were exact (Q_AGG 10000/10000 groups, Q_MATCH 50000/50000 series), and every phase timer fired the expected number of times per query.

## What each timer brackets

| Timer | File:function | Overlap? |
|---|---|---|
| `RESOLVE_NS`/`RESOLVE_CALLS` | `ravel-query/src/engine.rs`, inside `Engine::prefetch` around both the first `resolve_bounded` call and the not-found retry's `resolve_bounded` call | No — sequential, summed = wall span for this phase |
| `FETCH_NS`/`FETCH_CALLS` | `ravel-query/src/fetcher.rs`, `SegmentFetcher::store_get`'s `self.store.get(...).await` (the single funnel every ranged GET passes through; excludes the `get_limiter` semaphore-acquire wait) | Yes — segments fetch concurrently (`buffer_unordered(promql_fetch_fanout)`, default 8, across `snapshot.segments` inside `fetch_all_samples_and_histograms`); summed time can exceed wall time for this sub-span |
| `DECODE_NS`/`DECODE_CALLS` | `ravel-query/src/fetcher.rs`, 5 call sites: `decode_series_meta_catalog`/`decode_whole_object_catalog` (gated and inline catalog-decode branches), `decode_catalog_v5_chunked` (sparse catalog), `decode_run_pages_soa` (TS/VAL pages, scalar), `decode_run_histogram_pages` (TS/HIST pages, not exercised by this bench's scalar-only metrics) | Yes — same concurrent-segment reason as fetch |
| `FETCH_DECODE_WALL_NS`/`..._CALLS` | `ravel-query/src/engine.rs`, wraps the whole `stream::iter(distinct_plans).map(...).buffer_unordered(concurrency).collect().await` fan-out in `Engine::prefetch_metric_plans` | This itself is the wall-clock span for the (overlapping) fetch+decode stage; used for share-of-wall and for the remainder calculation instead of the `FETCH_NS`/`DECODE_NS` sums |
| `MATERIALIZE_NS`/`..._CALLS` | `ravel-query/src/engine.rs`, `merge_soa_runs` plus the immediately-following label-set sort, in `Engine::prefetch_metric_plans`, after the fetch/decode fan-out | No — sequential, summed = wall span |
| `RESULT_ASSEMBLY_NS`/`..._CALLS` | `ravel-query/src/engine.rs`, `Engine::instant_inner`'s tail after `evaluate` returns (building the `Ok((value, annotations, stats))` tuple) | No — sequential, summed = wall span |

Because fetch and decode overlap across concurrently-processed segments, their **summed** times (`FETCH_NS`, `DECODE_NS`) are reported separately from the **wall span** they occupy (`FETCH_DECODE_WALL_NS`). The wall span, not the sums, is what the unattributed-remainder row and the overall accounting below use, so phases are never double-counted. The individual `object_fetch`/`segment_decode` share-of-wall figures below still divide the summed (not wall-span) time by total wall time, per the bin's pre-registered bands; this is the bin's own choice, inherited as-is (not changed for this run).

The operator phase (`sum by` aggregation / `+` matching) is timed separately by the pre-existing `ravel_promql::op_timers` (issue #2445), not by `phase_timers`; its figures are included below for completeness since the bin reports them in the same run.

## Q_AGG: `sum by (grp_major, grp_minor) (promql_op_share_a)` — 10,000 groups

Wall time (ns, per-run means, n=5): min=2,326,341,346 median=2,521,141,473 max=2,609,013,539

| Phase | time_ns (min/median/max) | share% (min/median/max) | band | verdict |
|---|---|---|---|---|
| catalog_resolve | 102,188 / 103,655 / 112,307 | 0.004 / 0.004 / 0.004 | <5% | inside |
| object_fetch (summed) | 4,386,396 / 4,387,780 / 4,425,051 | 0.170 / 0.174 / 0.189 | <15% | inside |
| segment_decode (summed) | 264,954,786 / 266,046,274 / 295,357,488 | 10.496 / 10.564 / 11.592 | 25-50% | **below band** |
| fetch+decode (wall span, informational) | 1,673,988,551 / 1,860,703,317 / 1,876,325,241 | 71.917 / 73.708 / 73.980 | n/a | n/a (not a pre-registered phase) |
| series_materialisation | 292,894,732 / 294,385,397 / 328,013,633 | 11.618 / 11.713 / 12.595 | 25-50% | **below band** |
| operator (`sum by`, `ravel_promql::op_timers`) | 119,336,250 / 120,203,174 / 126,582,294 | 4.752 / 4.797 / 5.130 | 4-5% | inside |
| result_assembly | 484 / 542 / 554 | 0.000 / 0.000 / 0.000 | <10% | inside |
| unattributed_remainder (wall - resolve - fetch_decode_wall - materialize - operator - assembly) | 239,913,235 / 245,144,398 / 277,981,611 | 9.601 / 9.824 / 10.655 | <5% | **above band** |

object_fetch requests/bytes: 16 requests (median), 8,649,655 bytes (median), 8 segments fetched (median).

Derived (not printed by the bin, computed here from the two rows above): the fetch+decode wall span (median 1,860,703,317 ns, 73.708% of wall) minus the summed object_fetch and segment_decode time (median 4,387,780 + 266,046,274 = 270,434,054 ns) leaves **1,590,269,263 ns (62.97% of wall) inside the fetch+decode fan-out that neither `FETCH_NS` nor `DECODE_NS` accounts for.** This is larger than the stand-alone unattributed_remainder row above, which only covers time *outside* the fetch+decode span. See "Deviations" below.

## Q_MATCH: `promql_op_share_a + promql_op_share_b` — 50,000 matched series

Wall time (ns, per-run means, n=5): min=4,932,515,115 median=5,205,907,518 max=5,396,248,417

| Phase | time_ns (min/median/max) | share% (min/median/max) | band | verdict |
|---|---|---|---|---|
| catalog_resolve | 112,157 / 114,426 / 116,837 | 0.002 / 0.002 / 0.002 | <5% | inside |
| object_fetch (summed) | 8,788,636 / 8,792,904 / 8,903,591 | 0.165 / 0.169 / 0.178 | <15% | inside |
| segment_decode (summed) | 526,526,584 / 531,149,279 / 579,912,152 | 10.119 / 10.194 / 10.768 | 20-40% | **below band** |
| fetch+decode (wall span, informational) | 2,929,652,485 / 3,210,338,670 / 3,238,312,375 | 59.395 / 61.370 / 61.667 | n/a | n/a |
| series_materialisation | 666,465,207 / 677,856,882 / 712,105,829 | 12.808 / 13.043 / 13.743 | 25-50% | **below band** |
| operator (`+` matching, `ravel_promql::op_timers`) | 706,372,433 / 727,109,800 / 776,471,067 | 13.569 / 14.029 / 14.646 | 11-13% | **above band** |
| result_assembly | 448 / 526 / 564 | 0.000 / 0.000 / 0.000 | <10% | inside |
| unattributed_remainder | 602,495,302 / 611,162,444 / 669,241,745 | 11.678 / 11.790 / 12.402 | <5% | **above band** |

object_fetch requests/bytes: 32 requests (median), 17,299,252 bytes (median), 8 segments fetched (median).

Derived: fetch+decode wall span (median 3,210,338,670 ns, 61.370%) minus summed object_fetch + segment_decode (8,792,904 + 531,149,279 = 539,942,183 ns) leaves **2,670,396,487 ns (~51.3% of wall) inside the fan-out unaccounted for by either named phase.**

## Store is in-memory

`store_from_env(StoreKind::Memory)` is used throughout, so every `object_fetch` GET is served from an in-process map with zero network latency. `object_fetch`'s measured share (0.17-0.18%, well inside its <15% band) is a **floor**: a real S3-backed store would add real round-trip latency to every one of the 16-32 GETs per query, which would raise `object_fetch`'s share and correspondingly lower every other phase's share. Nothing else in this measurement (decode CPU cost, materialize CPU cost, operator CPU cost) depends on the store backend.

## Deviations from pre-registered bands

- `segment_decode` (summed): below band for both queries (10.5-10.6% vs 25-50% expected for Q_AGG, 10.1-10.2% vs 20-40% for Q_MATCH).
- `series_materialisation`: below band for both queries (11.6-11.7% vs 25-50% for Q_AGG, 12.8-13.0% vs 25-50% for Q_MATCH).
- `unattributed_remainder`: above band for both queries (9.6-10.7% vs <5% for Q_AGG, 11.7-12.4% vs <5% for Q_MATCH).
- Q_MATCH operator share: above band (13.6-14.6% vs 11-13% pre-registered). Q_AGG operator share is inside its 4-5% band.
- The biggest single finding is not a band miss on a named phase but a gap the named phases do not cover at all: **the fetch+decode wall span is 62-74% of total query wall time, but the summed `FETCH_NS`+`DECODE_NS` inside that same span account for only about 10.7% (Q_AGG) / 10.4% (Q_MATCH) of wall time.** Roughly 51-63 percentage points of wall time sit inside the `buffer_unordered` fetch/decode fan-out (`Engine::prefetch_metric_plans` in `engine.rs`, calling down into `fetch_all_samples_and_histograms`/`fetch_soa_and_histograms_phase_accounted` in `fetcher.rs`) without being bracketed by any phase timer on this branch — candidates include per-series page-range planning (`plan_ranges_v4`), `FetchedSeriesSoa`/`FetchedHistogramSeries` construction and vector growth outside the `decode_run`/`decode_histogram_run` calls, and `buffer_unordered` scheduling overhead across the up-to-8-way segment concurrency. This is a measurement finding (a real, large untimed middle layer), not a code bug — the phase timers correctly measure what they document measuring; they just do not yet cover this layer. I have not modified the timers or bin to add a new one, per the task's scope (measurement only, defects fixed only if they block compiling or break the bin's own assertions — this blocks neither).
- No assertion inside the bin failed (exit 0), so no scaffolding changes were needed or made beyond what is already on the branch.

## Raw per-run means (appendix, ns unless noted; n=5 runs x 5 queries per run, warm-ups discarded)

```
Q_AGG   wall_ns per run   : [2609013539.4, 2521141472.8, 2518504852.4, 2524429525.6, 2326341346.4]
Q_AGG   op_ns per run     : [126582294.4, 120941560.6, 120203174.2, 119948924.2, 119336250.4]
Q_AGG   share_pct per run : [4.851730069178191, 4.797095359574619, 4.77279899164986, 4.751525957988107, 5.129782462262994]
Q_MATCH wall_ns per run   : [5396248416.6, 5203592187.0, 5205907518.4, 5233596378.8, 4932515115.2]
Q_MATCH op_ns per run     : [776471067.0, 727109800.4, 706372433.2, 734231471.0, 722396414.0]
Q_MATCH share_pct per run : [14.389090476476415, 13.973227998468436, 13.568670413436365, 14.029195563765471, 14.645599600371602]
Q_AGG   sort_ns per query : [10920457, 10988519, 13038532, 12647172, 11115873, 11095823, 11786420, 11608216, 11178170, 10930044, 11193955, 11138423, 11225537, 11078062, 11427004, 11232689, 11187968, 11241928, 10831018, 11284333, 11221916, 10938079, 11131514, 10932387, 10804305]
Q_AGG   sort_ns   : total=282178344 mean_per_call=11287134
accepted_points   : 200000
```

Full stdout retained at `.gate-logs/run.log` for this run (not committed; `.gate-logs/` is gitignored).
