# Stage 0b per-segment fan-out step measurement (issue #2479, epic #2466)

## Host

```
Linux ip-172-31-18-6 7.0.0-1011-aws #11~24.04.1-Ubuntu SMP PREEMPT Mon Aug 10 15:20:57 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux
```
16 cores (`nproc`), 30 GB RAM (`free -g`). `CARGO_BUILD_JOBS=4` (16/4, above the 11 GB floor for `--jobs 2`).

uptime before the authoritative run:
```
 20:34:54 up 30 days, 14 min,  4 users,  load average: 1.61, 1.42, 1.44
```
uptime after:
```
 20:38:16 up 30 days, 17 min,  4 users,  load average: 1.05, 1.26, 1.37
```
Run wall time: ~3m22s for 2 warm-up rounds + 5 runs x 5 queries x 2 queries (Q_AGG, Q_MATCH), in-memory object store (no network cost).

Command (release binary, `CARGO_TARGET_DIR` pinned by the harness to a path outside the checkout):
```
"$CARGO_TARGET_DIR/release/promql_operator_share"
# resolves to:
/var/lib/fleet/cache/cargo-target/303723a7-4394-44b0-b42a-9185f24c3df4/release/promql_operator_share
```
Build: `CARGO_BUILD_JOBS=4 cargo build --release -p ravel-bench --bin promql_operator_share`.

This measurement went through three build/run cycles, each gated by a successful release build committed before the bin ran (per the task's ordering rule). The first run (commit `b4f248c`) surfaced two defects in the instrumentation itself, both fixed and re-verified before the authoritative run used for the tables below (commits `1aa0cbf`, `e6e6283`); see "Deviations and findings" for both.

## What each new step timer brackets

The per-segment future is `SegmentFetcher::fetch_soa_and_histograms_phase_accounted` (the function actually awaited inside the `FUTURE_NS`-timed span in `engine.rs`), which is a thin wrapper over `fetch_runs_and_histograms`. All file:function references below are in `crates/ravel-query/src/fetcher.rs` unless noted.

| Step | File:function, statements bracketed | Overlap? |
|---|---|---|
| `FUTURE_NS`/`FUTURE_CALLS` | `engine.rs`, `Engine::fetch_all_samples_and_histograms`'s per-segment fan-out closure: entry (before `fetch_soa_and_histograms_phase_accounted` is awaited) to return | Yes — concurrent per-segment futures inside one `buffer_unordered`; sum exceeds `FETCH_DECODE_WALL_NS` by design |
| `LIMITER_WAIT_NS`/`..._CALLS` | `SegmentFetcher::store_get`'s `self.get_limiter.acquire().await`, ahead of every GET | No within one future (sequential ahead of `FETCH_NS`) |
| `FOOTER_PARSE_NS`/`..._CALLS` | `open_segment`, both `open_from_suffix` calls (first-GET parse, and the second parse on a `NeedRange` footer chase) | No |
| `CATALOG_RETAIN_NS`/`..._CALLS` | `decode_selected`'s `shrink_to_retained` call (retained-catalog-length summation plus the memory-budget shrink), the one statement in `decode_selected` after `DECODE_NS`'s own timer closes | No |
| `SELECTED_SPLIT_NS`/`..._CALLS` | `fetch_runs_and_histograms`, the `scalar`/`histogram` `selected.iter().filter(..).collect()` pair, between `decode_selected` returning and `fetch_pages` starting. **Found during this task's remainder investigation; not in the original tiling plan.** | No |
| `PAGE_PLAN_NS`/`..._CALLS` | `fetch_pages`'s local `plan` closure, the `plan_ranges_v4` calls (one for the scalar slice, one for the histogram slice; skipped on an empty slice) | No |
| `RUN_PLAN_LOOKUP_NS`/`..._CALLS` | `build_scalar_decodes`/`build_histogram_decodes`, `find_run_plan`'s linear scan over `planned`, once per (series, run) pair. This is the closest real analog to the pre-registered "per-series map/set inserts" step: there is no actual map or set here, just an O(n) `Iterator::find` re-scanning a same-order-of-magnitude slice once per series | No |
| `LABEL_CLONE_NS`/`..._CALLS` | `build_scalar_decodes`/`build_histogram_decodes`, `entry.entry.labels.clone()`, the per-series (per-run, for an L1 part) deep clone of a `LabelSet` | No |
| `SAMPLE_ASSEMBLY_NS`/`..._CALLS` | `build_scalar_decodes`/`build_histogram_decodes`, the `RunDecode`/`RunHistogramDecode` struct construction plus its priority-column call and `Vec::push`, excluding the label clone and page decode | No |
| `SOA_CONVERT_NS`/`..._CALLS` | `fetch_soa_and_histograms_phase_accounted`, the `into_soa`/`into_fetched` map-collect pair run after `fetch_runs_and_histograms` returns but still inside the `FUTURE_NS` span. **Found during this task's remainder investigation; not in the original tiling plan.** | No |
| `PRE_FANOUT_NS`/`..._CALLS` (gap a) | `engine.rs`, `Engine::prefetch_metric_plans`: window/padding/name-filter/multiplier setup once per call, plus `distinct_plans_by_matcher`/`is_pushdown_eligible`/`count_over_time_pushdown_target` setup once per attempt, both before `fetch_decode_start`. Excludes `RESOLVE_NS`. | No |
| `POST_FANOUT_NS`/`..._CALLS` (gap b) | `engine.rs`, `Engine::prefetch_metric_plans`: per-plan results collection plus `federate_scalar` (before `MATERIALIZE_NS`), and the histogram sort plus `precomputed_count` construction and the closure's return (after it). Excludes `MATERIALIZE_NS`. | No |
| Gap (c) | `instant_inner`'s zero-width gap between the operator call returning and `RESULT_ASSEMBLY_NS` starting | Not separately instrumented: `RESULT_ASSEMBLY_NS` starts immediately where the operator returns in this code path, so gap (c) is zero by construction, not an unmeasured quantity. |
| `matcher evaluation` | Not separately instrumentable from `ravel-query`: runs inside `ravel_segment::decode_catalog_matching_v4`, out of this task's crate scope, folded into the pre-existing `DECODE_NS`. |

Existing stage 0 timers (`RESOLVE_NS`, `FETCH_NS`, `DECODE_NS`, `FETCH_DECODE_WALL_NS`, `MATERIALIZE_NS`, `RESULT_ASSEMBLY_NS`) are unchanged; see `stage0-promql-phases.md`.

## Multiplier counts (deliverable 4)

Per-query means, n=25 samples per query (5 runs x 5 queries per run):

| Count | Q_AGG | Q_MATCH |
|---|---|---|
| series-run materialisations (`label_clone_calls`) | 100,000.0 | 200,000.0 |
| `run_plan_lookup_calls` (cross-checked equal to `label_clone_calls`) | 100,000.0 | 200,000.0 |
| `label_sets_materialized` (engine-cumulative, catalog-decode path) | 200,000.0 | 400,000.0 |
| ground-truth distinct series (compile-time: `SERIES_PER_METRIC`, x2 for Q_MATCH's two metrics) | 100,000 | 200,000 |
| multiplier = `label_clone_calls` / ground-truth distinct | **1.000** | **1.000** |
| label strings allocated (`label_clone_calls` x 7 labels/series) | 700,000 | 1,400,000 |
| samples decoded | not separately instrumented; see deviation below |

**A series matched in k segments clones its label set once per segment, by construction** (`LABEL_CLONE_NS` sits inside the per-segment `build_scalar_decodes`/`build_histogram_decodes` loop, which runs once per fetched segment) — never once per query. This benchmark's measured multiplier is 1.000x, not because the code deduplicates across segments, but because this benchmark's shard layout does not place any single series in more than one of the 8 segments each query fetches: every matched series, across both queries, is matched in exactly one segment. The 1.000x figure is a property of this benchmark's data layout, not evidence that the per-segment-clone architecture is free of the k-times cost in general; a tenant whose series span multiple segments (compaction boundaries, multiple ingest windows for one series) would show a multiplier above 1x with this same code, and the counters above would be the correct way to confirm it then.

`samples decoded` has no exact counter anywhere reachable from `ravel-query`/`ravel-types` (`QueryStats`/`FetchStats`/`QueryAccountingSnapshot` carry request/byte counts, not series or sample counts); `decode_calls` is reported instead as an explicitly-labeled proxy (one call per series-run, not per sample within it): mean 100,008.0 (Q_AGG) / 200,016.0 (Q_MATCH).

## Q_AGG: `sum by (grp_major, grp_minor) (promql_op_share_a)` — 10,000 groups, 8 segments fetched (median)

`future_ns` (sum of per-segment futures): min=1,665,440,512 median=1,751,617,532 max=2,092,612,545 ns, share-of-wall% median=76.22 (informational — futures run concurrently, so >100% of `fetch_decode_wall_ns` would not be a bug either).
`fetch_decode_wall_ns` (fan-out wall span): median=1,668,802,719 ns. concurrency_factor = future_sum/wall = **1.05**.

| Step (share of future) | time_ns (min/median/max) | share% (min/median/max) | pre-registered band | verdict |
|---|---|---|---|---|
| limiter_wait + page_plan | 4,939,459 / 88,851,880 / 429,022,175 | 0.297 / 5.073 / 20.502 | <5% | above band |
| &nbsp;&nbsp;limiter_wait alone | 11,498 / 83,916,038 / 423,866,584 | 0.001 / 4.791 / 20.255 | n/a | not pre-registered |
| &nbsp;&nbsp;page_plan alone | 4,927,961 / 5,037,830 / 5,155,591 | 0.246 / 0.286 / 0.305 | n/a | not pre-registered |
| matcher_evaluation | not separately instrumentable (see above) | n/a | 5-20% | cannot be checked |
| run_plan_lookup (map/set-insert analog) | 1,148,809,935 / 1,159,149,099 / 1,164,084,066 | 55.133 / **66.359** / 69.083 | 10-30% | **above band** |
| label_clone (label-set construction) | 47,899,311 / 50,338,368 / 51,115,833 | 2.430 / 2.788 / 3.046 | 40-70% | **below band** |
| sample_assembly | 4,404,218 / 4,480,522 / 4,504,004 | 0.214 / 0.252 / 0.270 | <15% | inside |
| footer_parse | 43,541 / 567,845 / 912,623 | 0.002 / 0.032 / 0.053 | n/a | not pre-registered |
| catalog_retain | 6,679,767 / 6,829,315 / 6,876,092 | 0.327 / 0.387 / 0.410 | n/a | not pre-registered |
| selected_split (found this task) | 13,322,376 / 14,633,627 / 17,827,604 | 0.794 / 0.835 / 0.950 | n/a | not pre-registered |
| soa_convert (found this task) | 1,061,083 / 1,066,540 / 1,068,423 | 0.051 / 0.061 / 0.064 | n/a | not pre-registered |
| remainder_inside_futures | 158,527,516 / 160,575,444 / 163,569,168 | 7.579 / 9.160 / 9.748 | <5% | **above band** |

Gaps outside the fan-out: `pre_fanout_gap` time_ns min=2,534 median=2,620 max=2,762 (share of wall ~0.000%); `post_fanout_gap` time_ns min=2,930 median=3,106 max=3,338 (share of wall ~0.000%); `remainder_outside_stage0b` (wall minus resolve, fetch_decode_wall, materialize, result_assembly, pre/post_fanout): time_ns min=351,403,454 median=353,173,346 max=358,569,957, share% min=15.291 median=15.295 max=15.441.

## Q_MATCH: `promql_op_share_a + promql_op_share_b` — 50,000 matched series, two distinct matcher sets, 8 segments fetched (median)

`future_ns`: min=2,935,084,245 median=3,295,094,179 max=4,241,722,272 ns, share-of-wall% median=68.65.
`fetch_decode_wall_ns`: median=2,933,741,041 ns. concurrency_factor = future_sum/wall = **1.12**.

Q_MATCH has no pre-registered bands (the task pre-registered only Q_AGG's); every row below is "not pre-registered".

| Step (share of future) | time_ns (min/median/max) | share% (min/median/max) |
|---|---|---|
| limiter_wait + page_plan | 11,016,265 / 399,152,703 / 1,333,857,084 | 0.375 / 12.114 / 31.446 |
| &nbsp;&nbsp;limiter_wait alone | 23,620 / 388,072,093 / 1,322,849,180 | 0.001 / 11.777 / 31.187 |
| &nbsp;&nbsp;page_plan alone | 10,448,954 / 10,992,645 / 11,080,610 | 0.260 / 0.332 / 0.375 |
| matcher_evaluation | not separately instrumentable | n/a |
| run_plan_lookup | 1,876,819,696 / 1,898,269,253 / 1,911,443,527 | 44.487 / **56.958** / 64.969 |
| label_clone | 99,504,306 / 103,145,310 / 104,569,711 | 2.453 / 3.130 / 3.447 |
| sample_assembly | 8,779,726 / 8,888,461 / 8,944,649 | 0.211 / 0.271 / 0.301 |
| footer_parse | 381,142 / 1,351,724 / 2,329,904 | 0.012 / 0.034 / 0.065 |
| catalog_retain | 12,908,667 / 12,996,560 / 13,358,153 | 0.306 / 0.394 / 0.441 |
| selected_split (found this task) | 32,845,016 / 35,054,234 / 38,991,677 | 0.808 / 1.064 / 1.182 |
| soa_convert (found this task) | 2,095,650 / 2,111,426 / 2,123,607 | 0.050 / 0.064 / 0.071 |
| remainder_inside_futures | 317,199,584 / 319,989,537 / 332,025,892 | 7.478 / 9.645 / 10.976 |

Gaps outside the fan-out: `pre_fanout_gap` time_ns min=3,280 median=3,586 max=3,772 (share of wall ~0.000%); `post_fanout_gap` time_ns min=356,651 median=411,975 max=461,793 (share of wall ~0.009%); `remainder_outside_stage0b`: time_ns min=1,204,598,499 median=1,244,571,946 max=1,250,702,518, share% min=25.267 median=25.718 max=26.023.

Q_MATCH's `FUTURE_CALLS`/`CATALOG_RETAIN_CALLS` fire `segments_fetched * 2` times per query (16 for 8 segments), not `segments_fetched` once: see "Deviations and findings" below.

## Deviations and findings

**The pre-registration's two headline guesses were swapped for both queries — the single most important result of this measurement.** The task pre-registered per-series label-set construction (`label_clone`) at 40-70% of the fan-out's unattributed time and per-series map/set inserts at 10-30%, explicitly flagging a per-segment multiplier on label-set construction as "the finding worth the most." Measured: `label_clone` is 2.4-3.4% (**far below** its 40-70% band), while `run_plan_lookup` — the closest real analog to "per-series map/set inserts", because there is no actual map or set in this code path, just `find_run_plan`'s O(n) linear scan over the segment's planned-run list, called once per (series, run) pair — is **55-69%** of future time (Q_AGG) and **44-65%** (Q_MATCH), 2-7x its 10-30% pre-registered band. The per-series label clone is cheap; the per-series linear re-scan of a same-order-of-magnitude slice is the dominant cost, exactly the O(n²) hazard `phase_timers.rs`'s doc comment for `RUN_PLAN_LOOKUP_NS` names as a thing "this timer exists to confirm or rule out" — it confirms it. This is a genuine, actionable finding for a follow-up task (replace `find_run_plan`'s linear scan with a map keyed by series/run), not a tuning of the measurement: the orchestrator's pre-registration was wrong in the way it explicitly warned itself it might be, and the counters say which way.

**Q_MATCH's `FUTURE_CALLS`/`CATALOG_RETAIN_CALLS` fire `segments_fetched * 2` times, not `segments_fetched`.** Root cause: Q_MATCH (`promql_op_share_a + promql_op_share_b`) has two distinct matcher sets (one per metric), and the OUTER per-distinct-matcher-set fan-out in `prefetch_metric_plans` (`distinct_plans_by_matcher`) runs the entire INNER per-segment fan-out once per matcher set — so the per-segment future, and everything inside it, legitimately runs twice per query for a 2-matcher-set query. `stats.segments_fetched` itself reports the distinct segment count, not multiplied by matcher-set passes. This was caught by the bin's own assertions on the first build/run cycle (commit `b4f248c`): `FUTURE_CALLS was 16 (want exactly segments_fetched=8)` on every Q_MATCH sample, no such failure on Q_AGG (one matcher set). Fixed in commit `1aa0cbf` by asserting `segments_fetched * matcher_set_count` with `matcher_set_count` hardcoded per query label (1 for Q_AGG, 2 for Q_MATCH — compile-time known from each query's own literal text in this bin); no product-code change, this was purely a bug in the new assertion, not in `ravel-query`.

**The in-future unattributed remainder is 7.5-10.7% (Q_AGG) and 6.6-11.4% (Q_MATCH), still above the 5% pre-registered band after fixing two newly-found untimed statements.** The first run (commit `b4f248c`) measured 9.9-13.1% (Q_AGG) / 7.1-15.8% (Q_MATCH) before any fix. Reading `fetch_runs_and_histograms` and its wrapper `fetch_soa_and_histograms_phase_accounted` in full found two untimed statement pairs inside the `FUTURE_NS` span: the `scalar`/`histogram` filter-collect pair (now `SELECTED_SPLIT_NS`, ~0.8-1.2% of future) and the `into_soa`/`into_fetched` conversion pair (now `SOA_CONVERT_NS`, ~0.05-0.07% of future). Adding both (commit `1aa0cbf`) narrowed the remainder by roughly 1-3 percentage points but did not close it. The remaining ~7-11% is not attributed to a specific statement by this task: candidates not individually timed include `open_segment`'s `Bytes::clone()` calls on fetched regions and its L0/L1 identity verification (`check_identity`/`verify_l1_identity`), and the inherent poll/wake overhead of a future awaited inside `buffer_unordered` (scheduler bookkeeping that is not attributable to any one application statement). Per deliverable 6's own stated gate — steps-sum within 5% OR the bin prints the remainder row and the report names what is unplaced — this is reported here rather than closed by further instrumentation or by loosening the comparison; the bin's assertion for this specific case was changed (commit `e6e6283`) from a hard failure to a printed, non-fatal note for exactly this reason, while the assertion that steps-sum must never *exceed* `FUTURE_NS` (true double-counting) remains a hard failure. A follow-up task that wants to close this gap further should time `open_segment`'s clone/identity-check statements next; they were not added here because the investigation budget for this task was the remainder question, not a full re-tiling of `open_segment`.

**`limiter_wait + page_plan`'s pre-registered band (<5%) sits right at its Q_AGG boundary (median 5.073%) and is pulled upward by `limiter_wait` alone, which ranges from near-zero to 20.5% (Q_AGG) / 31.4% (Q_MATCH) across the five runs.** This is concurrency-limiter queueing variance (the fetch semaphore's width relative to how many of the 8 segments' futures are runnable at once each run), not a fixed cost; `page_plan` alone is stable at ~0.25-0.37%.

**Matcher evaluation cannot be separately measured from `ravel-query`:** it runs inside `ravel_segment::decode_catalog_matching_v4`, outside this task's crate scope, and stays folded into the pre-existing `DECODE_NS`. Reported as an explanatory line with no computed verdict rather than a fabricated number.

**Gap (c)** (operator end to `RESULT_ASSEMBLY_NS` start) is zero by construction in this code path (`RESULT_ASSEMBLY_NS` begins exactly where the operator call returns in `instant_inner`), not an unmeasured quantity — no separate timer was added for it.

No `unsafe` was added. No crate outside `ravel-query`/`ravel-bench` was modified. No new external dependency was added. Scaffolding and instrumentation (new atomics, new timers, new bin fields/assertions/report) are left on the branch per the task's instruction; this branch is a measurement result, not intended to be merged, and normal merge gates (workspace clippy, changelog) were not run.
