# Stage 0: PromQL end-to-end operator-share measurement

Issue: #2445 (epic #2425).

## Host

```
Linux rp1 6.12.25+rpt-rpi-2712 #1 SMP PREEMPT Debian 1:6.12.25-1+rpt1 (2025-04-30) aarch64 GNU/Linux
```
4 cores (`nproc`), arm64 Pi class. `CARGO_BUILD_JOBS=2` for every cargo invocation.

`uptime` immediately before the measurement run:
```
15:11:07 up 28 days, 19:50,  0 user,  load average: 1.21, 1.25, 1.49
```
`uptime` immediately after:
```
15:17:19 up 28 days, 19:56,  0 user,  load average: 0.85, 1.06, 1.33
```
Run wall time: ~6m12s for 200,000-series ingest + flush + 2 warm-ups + 5 runs x 5 queries x 2 query types.

## Exact command

Build:
```
cargo build --release -p ravel-bench --bin promql_operator_share --jobs 2
```
Run (no arguments; the bin hardcodes the in-memory store):
```
/var/lib/fleet/cache/cargo-target/782d5537-a281-4428-8f93-46bcf5c3eee3/release/promql_operator_share
```
(`CARGO_TARGET_DIR` in this environment is not the repo-local `target/`; the binary is produced under the path above.)

## Data path

**Flushed segments.** The bin ingests 200,000 points through the normal
`IngestRouter::write` path (the same entry point `ravel_bench::query_latency`
uses), into the shard write-ahead buffer, then calls `router.flush_all()`
once before constructing the `QueryEngine`. The query engine has no
live-buffer read path: every instant query below reads durable, stored RSEG
segments in the in-memory object store, never the unflushed buffer.

## Method

- Two metrics, `promql_op_share_a` / `promql_op_share_b`, 100,000 series
  each (`SERIES_PER_METRIC`), one sample per series at a single timestamp.
- Each series carries 6 labels besides `__name__` (`grp_major`, `grp_minor`,
  `dc_zone`, `host_pool`, `svc_tier`, `series_key`), label names 6-12 bytes,
  values 8-24 bytes.
- `grp_major`/`grp_minor` are derived from `i % 10_000` so that
  `sum by (grp_major, grp_minor) (promql_op_share_a)` produces exactly
  10,000 groups.
- The first 50,000 series (by ingest index) of each metric share identical
  labels except `__name__` (`series_key = paired-NNNNNN`), giving metric A
  exactly 50,000 series with an exact-label-match partner in metric B; the
  other 50,000 of each metric carry a metric-specific `series_key`
  (`solo-a-*` / `solo-b-*`) and have no partner.
- `Q_AGG = sum by (grp_major, grp_minor) (promql_op_share_a)`
- `Q_MATCH = promql_op_share_a + promql_op_share_b`
- Both queried through `QueryEngine::instant`, the same public entry point
  `query_latency_bench` uses.
- 2 warm-ups discarded, then 5 runs of 5 queries each, Q_AGG and Q_MATCH
  interleaved within each run (AGG, MATCH, AGG, MATCH, ... x5).
- Per query: wall time bracketed with `Instant::now()`/`.elapsed()` around
  `engine.instant(...)`, plus the delta of `ravel_promql::op_timers`
  counters (`AGG_NS`/`AGG_CALLS`/`AGG_SORT_NS`/`MATCH_NS`/`MATCH_CALLS`)
  across the same call.
- `OPERATOR_SHARE` = operator time / total query time, computed **per run**
  (same-run op-time-mean over same-run total-time-mean) before any sorting,
  then min/median/max taken across the 5 per-run ratios. This avoids
  cross-pairing an independently-sorted min from one run with a max from
  another.

### Deviation: `EngineConfig::max_series`

`EngineConfig::default()`'s `max_series` is `DEFAULT_MAX_SERIES` = 10,000,
enforced incrementally while the fetch path builds its per-series map,
before any aggregation runs. Q_AGG's selector (`promql_op_share_a` alone)
matches all 100,000 raw series of metric A pre-aggregation, so the default
cap rejected the first run with `TooManySeries { count: 10001, max: 10000
}`. This is a legitimate, documented `EngineConfig` field (not a hard
safety limit, not application code), so it was raised to
`SERIES_PER_METRIC * 2 + 1` (200,001) in the bin's `QueryEngine::new(...)`
call rather than worked around. This is a configuration choice for the
bench's known selector volume, not a change to engine behavior or a bend of
the measured code paths.

### Call-count assertions

All four observed as specified, no engine-structural deviation found:
- Q_AGG: exactly 10,000 series returned, `AGG_CALLS` +1, `MATCH_CALLS` +0,
  per execution.
- Q_MATCH: exactly 50,000 series returned, `MATCH_CALLS` +1, `AGG_CALLS`
  +0, per execution.

## Results

### Q_AGG (`sum by (grp_major, grp_minor) (promql_op_share_a)`)

| stat | total query time (ns) | operator time (ns, incl. sort) | OPERATOR_SHARE % |
|---|---|---|---|
| min | 4,444,810,812 | 196,538,413 | 4.418 |
| median | 4,455,218,864 | 198,237,072 | 4.449 |
| max | 4,463,725,001 | 198,825,020 | 4.463 |

`AGG_SORT_NS` (the label-set sort of the input vector, already included in
the operator time above): total across all 25 recorded calls =
444,610,027 ns, mean per call = 17,784,401 ns (~9.0% of the median
per-query operator time).

INDEX_SHARE_OF_QUERY (`OPERATOR_SHARE x 0.262`, per issue #2443's
measurement of index work as a share of operator time):
- min: 1.158%, median: 1.166%, max: 1.169%

Pre-registered bands:
- OPERATOR_SHARE expected 5%-20%: measured 4.418%-4.463% -> **below band**.
- INDEX_SHARE_OF_QUERY expected 1.3%-5.2%: measured 1.158%-1.169% ->
  **below band**.
- Decision bar (5%): median INDEX_SHARE_OF_QUERY 1.166% -> **below the
  bar**.

### Q_MATCH (`promql_op_share_a + promql_op_share_b`)

| stat | total query time (ns) | operator time (ns) | OPERATOR_SHARE % |
|---|---|---|---|
| min | 8,744,898,788 | 1,031,381,113 | 11.794 |
| median | 8,790,417,748 | 1,069,543,086 | 12.160 |
| max | 8,808,229,791 | 1,100,992,521 | 12.537 |

INDEX_SHARE_OF_QUERY (`OPERATOR_SHARE x 0.320`):
- min: 3.774%, median: 3.891%, max: 4.012%

Pre-registered bands:
- OPERATOR_SHARE expected 20%-50%: measured 11.794%-12.537% -> **below
  band**.
- INDEX_SHARE_OF_QUERY expected 6.4%-16%: measured 3.774%-4.012% ->
  **below band**.
- Decision bar (5%): median INDEX_SHARE_OF_QUERY 3.891% -> **below the
  bar**.

### Caveat: in-memory store is a floor, not a ceiling, on OPERATOR_SHARE

The in-memory object store used here has no network latency, no request
queueing, and no cold-cache penalty. Every nanosecond not spent in the
aggregation or matching operator is spent on fetch/decode/plan/merge work
that, against real S3-compatible object storage, would grow (network RTT,
TLS, throttling, cache misses) while the operator time itself would not.
So the measured `OPERATOR_SHARE` here is an **upper bound**: against real
object storage, the operator's share of total query time can only be the
same or smaller, never larger. Both queries already measure below their
pre-registered bands on this upper-bound in-memory run; the same bench
against real object storage would be expected to measure lower still.

### Largest other contributor (existing timers only, none added)

The only existing timers are the five `ravel_promql::op_timers` counters
(aggregation and one-to-one match) and the bin's own wall-clock bracket
around `engine.instant(...)`. Neither the harness nor the query engine
exposes any further phase-level timer (fetch, decode, plan, series
resolution, merge) that this bench can read without adding instrumentation,
which is out of scope for this measurement. The only figure obtainable from
existing timers is the undifferentiated remainder, wall time minus operator
time:

- Q_AGG: median remainder = 4,455,218,864 - 198,237,072 = 4,256,981,792 ns
  (95.55% of wall time).
- Q_MATCH: median remainder = 8,790,417,748 - 1,069,543,086 =
  7,720,874,662 ns (87.84% of wall time).

This remainder is a single bucket covering everything the operator timers
do not cover (series/label resolution, object-store fetch, segment decode,
plan construction, result materialization); no further attribution is
possible from this harness's existing instrumentation. It is, by a wide
margin, the largest contributor to total query time for both queries, but
it cannot be broken down further without adding timers beyond the two this
task specified.

## Appendix: raw per-run figures (5 runs, run order)

```
Q_AGG   wall_ns per run   : [4463725001.2, 4455218863.8, 4444810812.4, 4447544713.4, 4456088299.6]
Q_AGG   op_ns per run     : [197206561.8, 198825020.2, 196538413.0, 198462871.8, 198237071.8]
Q_AGG   share_pct per run : [4.417981881656782, 4.462744172132897, 4.421749795327689, 4.462301889895599, 4.448679165935619]
Q_MATCH wall_ns per run   : [8808229791.2, 8795644740.6, 8790417748.2, 8781947036.0, 8744898788.4]
Q_MATCH op_ns per run     : [1068967998.2, 1069543086.4, 1078677754.0, 1100992521.4, 1031381113.2]
Q_MATCH share_pct per run : [12.136013972614215, 12.159916844561419, 12.271063616070796, 12.536997967383325, 11.79408862419442]
Q_AGG   sort_ns per query (25 calls, 5 per run) : [17879641, 17973289, 17729215, 17449086, 17572789, 17590863, 17766863, 17747548, 17923252, 18011900, 17750863, 18075402, 17850235, 17628531, 18185087, 17510605, 17738994, 17654328, 17773976, 17853032, 17709976, 17581754, 17849087, 17885050, 17918661]
```

(Each per-run value above is already the mean of that run's 5 queries,
except the sort-ns line, which lists all 25 individual per-query values;
these are the exact numbers `run_stats`/`mean` reduced into the summary
table above.)
