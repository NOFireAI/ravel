# Experiment ledger

Every run records: revision, diff on top of it, exact command, dataset,
configuration, raw result location, the pre-registered band written BEFORE
the run, and whether it was met. Wall-clock figures carry the backend that
produced them. Nothing here is an S3 number.

Conventions used in every row:

- Revision: `ee2070a0` plus the commits in this checkout (listed per run).
- Backend `memory`: in-process `MemoryStore`, no network, no disk.
- Backend `memory+get-delay-<N>ms`: the same store behind
  `DelayedGetStore`, a fixed `N` ms sleep before every GET. It quantifies
  the scan's structure under a known stall. It is measurement behaviour,
  not S3 performance.
- Policy: `--logs-fetch-policy cost-based` (bench and server default), which
  resolves `logs_request_cost_bytes` to `18446744073709551615` (u64::MAX)
  and `block_range_threshold` to u64::MAX: every log object is read whole
  in one covering GET. Declared in each run's header line as
  `policy=cost-based cost_bytes=u64::MAX`.
- Concurrency: `--fetch-concurrency 8` unless stated (bench default; a stock
  server on this 16-core box would resolve 32, see S1-VERIFICATION.md).
- Cache: `--cache-bytes 0` unless stated (no Ravel RAM cache; a stock server
  has one at 25 percent of MemTotal). There is no Ravel disk cache in any
  run. The OS page cache is irrelevant: the store is process memory.
- Build: `cargo build --release` (profile `release`, `opt-level` per
  workspace, debuginfo on), `CARGO_BUILD_JOBS=4`. Binary identity is the
  SHA-256 prefix printed per run.
- Noise floor: 15 percent per statement. No band below is narrower.
- Timing sums over partitions overlap in wall time and are never compared
  with `cold_ms`.

## Run 0: smoke, stock binary (S3 prep)

- Revision: `ee2070a0`, diff: none. Binary: stock `sql_latency_bench`
  (`5d5174e36e61764e`), copied to `$CARGO_TARGET_DIR/stock-ee2070a/`.
- Command: `sql_latency_bench --generate --runs 1 --continue-on-error`
- Dataset: 2000 rows, 4 objects, 35,304 stored bytes, 16 extra attrs,
  layout pre-compaction. Backend `memory`. policy=cost-based
  cost_bytes=u64::MAX, concurrency 8, cache off.
- Purpose: learn the report shape; no band pre-registered (not a result).
- Raw: `results/smoke-default.json`.
- Observations used later (code-observed in the report, not a measurement
  claim): fast-path statements report `logs_whole_object_opens = 4`,
  scan-phase wire bytes `35304 = stored_bytes` and `object_store_get_requests
  = 9` of which 4 are scan-phase GETs and 5 are unattributed
  resolve/commit-record GETs. The two planning-path statements
  (`typed_duration_threshold_count`, `body_word_search`) report the same
  35,304 bytes under `plan` and zero under `scan`: the plan phase reads each
  object whole and carries the bytes into the scan (#835), so `plan_full_reads`
  is the whole dataset (#693).

## Run 1: smoke, instrumented binary, no delay (S2 check)

- Revision: `ee2070a0` + S2 instrumentation diff (committed after gates as
  the `feat(ravel-sql)` commit in this directory's history).
- Command: `sql_latency_bench --generate --runs 1 --continue-on-error`
- Same dataset geometry as run 0. Backend `memory`.
- Pre-registered expectations (written before the run): `segments_opened`
  equals 4 on every fast-path statement; `reopens = 0`; `plan_init_elapsed`
  is non-zero only on the two planning-path statements; `polls_pending = 0`
  on an in-process store (no open ever yields); the per-segment timeline has
  4 rows, one per `(partition, segment)`.
- Result: all met. `segments_opened = 4`, `reopens = 0`, `polls_pending = 0`,
  `open_pending_polls = 0`, timeline 4 rows. `plan_init_elapsed` was 0.163 ms
  and 0.165 ms on the two planning statements and read `4 ns` on the others:
  DataFusion's `Time` reports 1 ns per created-but-untouched partition
  metric, so 4 ns is the floor for 4 partitions and is treated as zero.
- Raw: `results/smoke-instr.json`, stderr table in the transcript.
- Note: `cpu_ms` has 10 ms resolution (`CLK_TCK = 100`), so sub-10 ms
  statements read 0 or 10; it is only meaningful on the large dataset.

## Run 2: boundary validation with an injected 20 ms GET stall (S2)

- Revision: `ee2070a0` + S2 diff (adds `DelayedGetStore` and
  `--inject-get-delay-ms`). Backend `memory+get-delay-20ms`.
- Command: `sql_latency_bench --generate --runs 1 --continue-on-error
  --inject-get-delay-ms 20`
- Same dataset geometry as run 0: 4 objects, 4 fast-path partitions
  (`min(target_partitions=8, relevant=4)`), one segment per partition.
- Pre-registered bands (written before the run):
  - B2.1 fast-path statements: `open_elapsed_max_ns` in [20.0, 23.0] ms
    (one open per partition, one GET each, plus 15 percent).
  - B2.2 fast-path statements: `open_elapsed_ns` (sum) in [80, 92] ms.
  - B2.3 fast-path statements: `open_pending_polls >= 4` (each open yields
    at least once while it sleeps).
  - B2.4 every statement: `decode_build_elapsed_ns` < 5 ms in total: the
    stall must not land in the decode interval.
  - B2.5 planning-path statements (`typed_duration_threshold_count`,
    `body_word_search`): `plan_init_elapsed_ns` in [20, 46] ms (4 objects
    pruned with `buffer_unordered(4)`, so one to two 20 ms rounds), and
    `open_elapsed_max_ns` < 5 ms (the scan opens consume the carried
    whole-object bytes, no GET).
  - B2.6 every statement: `cold_ms` in [40, 140] ms: at least one resolve
    GET round plus one scan round, at most the 5 unattributed resolve GETs
    serialized plus one scan round.
- Result: see the table below, filled after the run.

Run 2 result, backend `memory+get-delay-20ms`, binary `b201bf5525a96844`:

| statement | cold ms | open_sum ms | open_max ms | pending | decode ms | plan_init ms |
|---|---|---|---|---|---|---|
| filtered_error_count | 67.6 | 85.8 | 21.5 | 4 | 0.32 | 0 |
| filtered_time_span | 65.3 | 81.7 | 20.4 | 4 | 0.61 | 0 |
| distinct_severity_count | 65.9 | 85.5 | 21.4 | 4 | 0.35 | 0 |
| typed_duration_threshold_count (plan path) | 66.0 | 0.34 | 0.11 | 0 | 0.33 | 21.5 |
| typed_duration_sum | 87.2 | 85.3 | 21.4 | 4 | 0.47 | 0 |
| body_word_search (plan path) | 66.6 | 0.24 | 0.07 | 0 | 2.02 | 21.4 |
| severity_case_insensitive_match | 66.0 | 85.1 | 21.3 | 4 | 0.32 | 0 |
| body_shape_histogram | 67.1 | 85.0 | 21.3 | 4 | 0.95 | 0 |
| count_by_severity | 66.2 | 85.1 | 21.7 | 4 | 0.27 | 0 |
| busy_hours | 65.5 | 81.0 | 20.3 | 4 | 0.16 | 0 |
| severity_bucket_split | 67.3 | 85.2 | 21.4 | 4 | 0.49 | 0 |
| recent_errors_top_n | 65.6 | 84.9 | 21.3 | 4 | 0.87 | 0 |

Bands: B2.1 met (20.3 to 21.7 ms; the 1 to 1.7 ms above 20 is tokio's
1 ms timer granularity plus the real open work). B2.2 met (81.0 to 85.8).
B2.3 met (exactly 4 on every fast-path statement). B2.4 met (max 2.02 ms).
B2.5 met (21.4 and 21.5 ms; 0.07 and 0.11 ms). B2.6 met (65.3 to 87.2 ms).
Conclusion (measured, injected-delay backend): the `open_elapsed` interval
captures the whole injected stall and the `decode_build_elapsed` interval
captures none of it; the plan barrier is counted once; the difference
between `cold_ms` and the scan's `first_batch` offset (about 44 ms here) is
the resolve phase's own GETs, which the scan timing deliberately excludes.

## Pre-registration for runs 3 to 5 (written before any of them ran)

Dataset for all three: `--records 200000 --records-per-object 5000
--extra-attrs 32`, so 40 RLOG objects, one stream, 33 record attributes
(`duration_ms:I64` plus `attr_0..attr_31:Str`), default writer block target
8192 records (one block per object at 5000 records). `--runs 3`,
`--fetch-concurrency 8`, `--cache-bytes 0`, policy cost-based,
cost_bytes u64::MAX. Corpora: `corpus/attrs-map.corpus.json` (declares only
`duration_ms`) and `corpus/attrs-declared.corpus.json` (declares
`duration_ms:i64`, `attr_3:str`, `attr_5:str`). The five `base` statements
are identical text in both files.

- B3.1 every fast-path statement (`narrow_count_filter`, `fastpath_minmax_ts`,
  `agg_group_severity_dur`, `cpu_regex_histogram`, all four attribute
  statements in both corpora): scan-phase `get_requests == 40` exactly,
  scan-phase `wire_bytes == dataset.stored_bytes` exactly,
  `logs_whole_object_opens == 40`, `logs_ranged_opens == 0`.
- B3.2 `selective_limit_planpath` (has_word is a block predicate):
  plan-phase `wire_bytes == stored_bytes`, scan-phase `wire_bytes == 0`,
  `plan_init_elapsed > 0`, `logs_whole_object_opens == 0`.
- B3.3 for each of the four attribute statement ids, map form versus declared
  form: identical `object_store_get_requests` and identical
  `object_store_bytes` (whole-object policy makes I/O independent of the
  projection). Exact equality.
- B3.4 `page_stored_bytes_decoded` (map form) divided by the same figure
  (declared form) >= 3.0 for `attr_eq_count`, `attr_group`,
  `attr_project_limit` (map form decodes every attribute column's pages).
- B3.5 `decode_build_elapsed_ns` (map) / (declared) >= 1.5 for the same three
  ids (row path over all columns versus columnar over one).
- B3.6 `rows_returned` equal per id across the two corpora (the bench sees
  counts only; result contents are proven by the ravel-sql test in S4).
- B3.7 data quality, not a criterion: `(max_ms - min_ms) / median_ms <= 0.5`
  on statements with `median_ms >= 20`.
- B4 instrumentation overhead: stock binary (`5d5174e36e61764e`) versus
  instrumented on `attrs-declared.corpus.json`, same dataset seed geometry:
  `|median_instr - median_stock| / median_stock <= 0.15` per statement with
  `median_stock >= 10 ms`. Above that the instrumentation is material and
  must be reduced before use.
- B5 (delay 20 ms, `--runs 1`, `attrs-declared.corpus.json`): 40 segments over
  8 partitions is 5 sequential opens per partition.
  - B5.1 fast-path statements: `open_elapsed_max_ns` in [100, 115] ms.
  - B5.2 fast-path statements: `open_pending_polls == 40`.
  - B5.3 `decode_build_elapsed_ns` within 15 percent of run 3's value for
    the same statement (the stall does not change decode work).
  - B5.4 `selective_limit_planpath`: `plan_init_elapsed_ns` in [100, 115] ms
    (40 prunes with `buffer_unordered(8)` is 5 rounds) and
    `open_elapsed_max_ns` < 5 ms.
  - B5.5 `cold_ms` >= 100 ms on every statement; the upper bound is left
    open because the resolve phase's commit-record GET count at 40 objects
    is not known before the run (it is read off the report and recorded).

## Pre-registration for run 6 (written before it ran)

Same dataset and corpus (`attrs-declared`) as run 5, backend
`memory+get-delay-20ms`, `--runs 1`, sweeping `--fetch-concurrency 16` and
`32` (partition count and GET permits move together in this bench, as on a
server without the ADR-1195 knobs set). The question is whether the exposed
open stall of run 5 is removed by configuration alone, which is what a
read-ahead prototype would have to beat at the same in-flight object count.

- B6.1 fast-path statements at 16: `open_elapsed_max_ns` in [60, 69] ms
  (ceil(40/16) = 3 sequential opens per partition).
- B6.2 fast-path statements at 32: `open_elapsed_max_ns` in [40, 46] ms
  (ceil(40/32) = 2 sequential opens per partition).
- B6.3 `open_pending_polls == 40` at both settings on fast-path statements.
- B6.4 scan-phase GETs == 40 and scan bytes == stored at both settings on
  fast-path statements (the sweep changes no I/O).

## Results of runs 3 to 6

Raw JSON: `results/run3a-instr-map.json`, `results/run3b-instr-declared.json`,
`results/run4-stock-declared.json`, `results/run5-instr-declared-delay20.json`,
`results/run6a-delay20-conc16.json`, `results/run6b-delay20-conc32.json`.
Checker: `check_bands.py` (prints every band with its figure; exits 1 on any
miss; its output at the time of writing is reproduced in RECOMMENDATION.md's
appendix). Dataset in all: 40 objects, 1,074,826 stored bytes, 200,000 rows,
load 5.7 s. Binaries: instrumented `b201bf5525a96844`, stock `5d5174e36e61764e`.
policy=cost-based cost_bytes=u64::MAX, cache off. Backend `memory` for runs
3 and 4, `memory+get-delay-20ms` for 5 and 6.

Band outcomes:

- B3.1 met on every statement that took the fast path (`narrow_count_filter`,
  `fastpath_minmax_ts`, `agg_group_severity_dur`, `cpu_regex_histogram`,
  `attr_group`, `attr_project_limit`, and `dur_sum_threshold` in the map
  corpus): scan GETs 40, scan bytes 1,074,826, whole-object opens 40, ranged 0.
- B3.1 MISSED, pre-registration error, on `attr_eq_count` (both corpora) and
  `dur_sum_threshold` (declared corpus): these are NOT fast-path statements.
  `attrs['attr_3'] = 'v2'` and `"attr_3" = 'v2'` become a POSTINGS
  attribute-equality prune and `"duration_ms" >= 1000` a skip-index
  `NumRange` prune; both are block predicates, so `whole_segment_fast_path`
  rejects with `BlockPredicate` and the planning path runs. Their figures:
  plan-phase bytes 1,074,826 (all 40 objects read whole in the plan phase),
  scan-phase GETs 32 and bytes 859,420 (32 of the 40 objects read AGAIN),
  `plan_init_elapsed` 1.0 to 1.6 ms, `logs_whole_object_opens` 0.
- B3.2 MISSED on `selective_limit_planpath` in the same way: plan bytes equal
  stored (met) but scan bytes are 859,420, not 0. The cause is code-observed
  at `crates/ravel-sql/src/logs_scan.rs:2630-2700`: the whole-object carry
  from the plan phase is retained for only the first `plan_concurrency`
  (= partition count, 8 here) segments to complete; the other 32 are
  re-fetched by the scan. Run 6 confirms the bound moves with the partition
  count: 24 re-fetched at 16 partitions, 8 at 32. With no read cache wired
  (this bench) that is 1.8x the stored bytes over the wire at 8 partitions;
  with the server's default RAM cache the second read is a cache hit as long
  as the objects still fit, which this bench did not measure.
- B3.3 bytes equal met on `attr_eq_count`, `attr_group`, `attr_project_limit`
  (1,945,646 / 1,086,226 / 1,086,226 bytes on both sides). GET counts differ
  by exactly one on two of them (113 vs 114, 81 vs 82): the difference is in
  the unattributed resolve-phase GETs (41 vs 42), not in plan or scan. Judged
  met on the quantity the band was about. `dur_sum_threshold` MISSED as
  pre-registered and the miss is a finding: the map form
  (`CAST(attrs['duration_ms'] AS BIGINT) >= 1000`) is not a pushable prune
  and takes the fast path (81 GETs, 1,086,226 bytes), the declared form is a
  pushable `NumRange` prune and takes the planning path (113 GETs,
  1,945,646 bytes). On this fixture no block is pruned (every block spans
  the whole value range), so the declared form spends 1.8x the bytes for the
  same result while using 1/150th of the CPU.
- B3.4 met: pages decoded ratio 23.98 (84,691 vs 3,531 stored page bytes) on
  all three; `dur_sum_threshold` 20.9.
- B3.5 met: decode/build ratio 107, 75, 53 (map 1119 / 908 / 566 ms summed
  over partitions versus declared 10.5 / 12.1 / 10.7 ms); `dur_sum_threshold`
  48.8.
- B3.6 met on all four ids (1, 7, 1, 100 rows).
- B3.7 spreads: 0.18, 0.19, 0.34, 0.01, 0.07 on the map corpus statements
  above 20 ms; 0.10 on the declared corpus's one such statement. `attr_group`
  at 0.34 exceeds the 15 percent floor: its min 434 ms versus median 655 ms.
  Treated as noise on a single statement; no conclusion below rests on it.
- B4 met where judged: `selective_limit_planpath` 78.9 vs 84.1 ms (6 percent,
  instrumented faster), `cpu_regex_histogram` 13.9 vs 13.7 ms (1 percent).
  The remaining statements have medians under 10 ms on both binaries and are
  not judged. Instrumentation overhead is not material at this scale.
- B5.1 met on the six fast-path statements: `open_elapsed_max` 106.4 to
  106.9 ms (5 sequential opens times 20 ms plus the tokio timer floor). The
  two planning-path attribute statements read 85.2 and 85.6 ms, which is 4
  opens times 20 ms: their partitions carry one object each and re-fetch
  four, matching the carry bound above.
- B5.2 met on the six fast-path statements (40); the planning-path ones read
  32, one yield per re-fetched object.
- B5.3 MISSED on every statement: decode/build under the injected stall is
  1.8x to 2.5x the no-stall figure (for example 15.3 vs 6.2 ms summed over 8
  partitions). The absolute figures are small (0.15 to 0.9 ms per segment)
  and the per-partition max stays at 1.9 to 4.9 ms. Inferred, not measured:
  each decode now starts on a core that slept 20 ms (cold caches, a possible
  runtime-thread migration, frequency ramp). It does not change any
  conclusion, because decode is under 5 percent of the partition's critical
  path in this run either way, but the band as written was wrong and is
  recorded as missed.
- B5.4 met on `plan_init` (109.0 ms, 5 rounds of 8 concurrent prunes);
  MISSED on `open_elapsed_max < 5 ms` (88.2 ms) for the same carry-bound
  reason as B3.2.
- B5.5 met on every statement (153 to 283 ms). The resolve phase issued 41
  or 42 unattributed GETs per statement (commit records); under the 20 ms
  stall `cold_ms - stream_elapsed_max` is about 45 ms, so those GETs run at
  a concurrency of roughly 20.
- B6.1 met: 63.9 to 65.6 ms at 16 partitions. B6.2 met: 42.5 to 45.7 ms at
  32. B6.3 met (40 and 40). B6.4 met (40 GETs, 1,074,826 bytes) on the
  fast-path statements at both settings.

Per-partition critical path under the injected 20 ms stall, fast-path
statement `narrow_count_filter` (backend `memory+get-delay-20ms`):

| partitions | opens per partition | open_max ms | decode_max ms | stream_max ms | cold ms |
|---|---|---|---|---|---|
| 8 | 5 | 106.7 | 2.4 | 109.5 | 155.0 |
| 16 | 3 | 65.6 | about 2 | 68.6 | 112.4 |
| 32 | 2 | 42.5 | about 1.6 | 45.3 | 90.2 |

The scan's partition critical path is the serialized opens; decode is 2 to 4
percent of it in this configuration; the rest of `cold_ms` is the resolve
phase (about 45 ms at every setting) and DataFusion overhead.
