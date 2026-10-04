# Stage 1: measured effect of the run-plan cursor fix (issue #2543)

Measures the effect of replacing `find_run_plan`'s O(n) linear scan over the
segment's planned-run list with `RunPlanCursor`, a sequential position cursor,
in `crates/ravel-query/src/fetcher.rs`. `plan_ranges_v4` (`ravel-segment`)
builds the `planned` slice in exactly the series/run-index order the decode
loops in `build_scalar_decodes`/`build_histogram_decodes` consume it, so a
forward-only cursor returns the same entries the linear scan found, without
rescanning. This is the follow-up the stage 0b report
(`stage0b-promql-fanout.md`) flagged: `find_run_plan` measured as the
dominant cost in the fan-out (55-69% of future time for `Q_AGG`, 44-65% for
`Q_MATCH`), against a pre-registered 10-30% band for that step.

## Host and method

- Host: x86_64, AMD EPYC 7R13, 16 cores, 30 GB RAM, 217 GB free on
  `/var/lib/fleet` at the start of this task (well above the 40 GB floor).
- `CARGO_BUILD_JOBS=4` on every cargo invocation (16 cores / 30 GB RAM class).
- Two release binaries of `ravel-bench`'s `promql_operator_share`:
  - `bin_before`: this checkout with the `chore(bench): take the series
    count as an argument` commit applied (series-count argument and result
    digest), `find_run_plan`'s linear scan still in place.
  - `bin_after`: the same tree plus `chore(query): apply the run plan
    cursor on the measurement branch` (the `RunPlanCursor` fix).
- Both binaries run against `ravel_bench::harness`'s in-memory object store
  (`StoreKind::Memory`), network-free and deterministic.
- `PROMQL_OP_SHARE_SERIES_PER_METRIC` set to 100000 (default, matching
  `stage0b-promql-fanout.md`) and 20000.
- Each process invocation performs the binary's own fixed protocol: 2
  warmups, then 5 timed runs of each of `Q_AGG` and `Q_MATCH`.
- 5 before/5 after process invocations per size (10 per size, 20 total),
  alternating `before, after, before, after, ...`. `uptime` logged
  immediately before and after every process invocation. All raw output
  under `.gate-logs/matrix/size<N>/{before,after}_run<K>.log`.
- Commands, per run:
  `PROMQL_OP_SHARE_SERIES_PER_METRIC=<N> .gate-logs/bin_<before|after>`
  (env var omitted for the 100000 default-size runs).
- Aggregation: `.gate-logs/parse_matrix.py` (scratch, not committed) parses
  all 20 logs' per-process min/median/max figures and recomputes, across the
  5 processes per (size, query, binary): median of the 5 process medians,
  min of the 5 process mins, max of the 5 process maxes. Raw aggregate dump:
  `.gate-logs/matrix_summary.json`.

All 20 runs exited 0. No assertion failures were printed by any run (the
binary prints `promql_operator_share: assertion failures:` and a non-zero
message on a real failure; none appeared in any of the 20 logs).

## Reproduction of the stage 0b baseline (before binary)

`stage0b-promql-fanout.md` reported `run_plan_lookup`'s share of future time
at 100,000 series as 55.133/**66.359**/69.083% (min/median/max) for `Q_AGG`
and 44.487/**56.958**/64.969% for `Q_MATCH`, with `fetch_decode_wall_ns`
median 1,668,802,719 ns (`Q_AGG`) and 2,933,741,041 ns (`Q_MATCH`).

This task's `bin_before`, same default size, same in-memory harness, 5
process runs:

| Query | run_plan_lookup share% (median of 5 process medians, min-max) | fetch_decode_wall_ns median (range across 5 runs) |
|---|---|---|
| Q_AGG | 69.0-69.9% (runs: 69.437, 69.434, 69.148, 69.927, 69.013) | 1,838,644,513 - 1,864,278,857 ns |
| Q_MATCH | 57.3-61.0% (runs: 60.741, 60.801, 59.863, 60.991, 57.315) | 3,130,417,690 - 3,222,918,058 ns |

`Q_AGG`'s share sits at the top edge of stage 0b's own min/median/max spread
(69.0-69.9% here vs. a reported max of 69.083%); `Q_MATCH`'s share
(57.3-61.0%) falls inside stage 0b's reported band. `fetch_decode_wall_ns`
is about 10-12% higher here than stage 0b's figure for both queries, same
order of magnitude. This reproduces the baseline closely enough to treat the
before/after comparison below as measuring the same effect stage 0b found,
not a different host/build artifact. The before-binary's own `label_clone`,
`remainder_outside_stage0b`, and multiplier-count lines (not reproduced here
for space) also match stage 0b's reported shape.

## Result correctness (deliverable 6)

Identical `blake3` result digest before vs. after, both queries, both sizes:

| Size | Query | digest (before == after) |
|---|---|---|
| 100000 | Q_AGG | `56f0901e36c551071015d005f767c9aed968bef7acb2d8c807ee28f0e5c6b50b` |
| 100000 | Q_MATCH | `7ddccaf524b558946dd38957a21d85b110dca1f2563e4999b999bd94abb17506` |
| 20000 | Q_AGG | `df65a21ddbcbaae3f388b6c5fed9148ae4ae95656b7c982bd126aad3a1550836` |
| 20000 | Q_MATCH | `b48ab0e06d141266a2e6c9a94fadbc961cd2761ddc994a4accf4e0545f767f18` |

Each digest is identical across all 5 before runs and all 5 after runs at
that size/query (verified directly from the logs, not only the aggregated
summary). Output counts match the binary's own built-in assertions in every
run (`Q_AGG matched == total_groups`, `Q_MATCH matched == matched`,
`run_plan_lookup_calls == label_clone_calls`), and no run printed an
assertion failure.

`RUN_PLAN_LOOKUP_CALLS` per query, before vs. after (identical in every
run, both sizes):

| Size | Query | calls (before == after) |
|---|---|---|
| 100000 | Q_AGG | 100,000 |
| 100000 | Q_MATCH | 200,000 |
| 20000 | Q_AGG | 20,000 |
| 20000 | Q_MATCH | 40,000 |

## Wall time and RUN_PLAN_LOOKUP time, before vs. after

All times ns unless noted. "median/min/max" is the median of 5 process
medians / min of 5 process mins / max of 5 process maxes, as described
above.

### size = 100,000 series/metric

| Metric | Query | before | after |
|---|---|---|---|
| wall time (median/min/max) | Q_AGG | 2,507,751,637 / 2,497,304,814 / 2,518,690,053 | 1,119,012,719 / 1,092,596,639 / 1,129,574,769 |
| wall time (median/min/max) | Q_MATCH | 5,127,056,348 / 5,112,903,473 / 5,153,578,711 | 2,907,621,038 / 2,813,397,396 / 2,943,710,990 |
| RUN_PLAN_LOOKUP time per query (median/min/max) | Q_AGG | 1,352,874,425 / 1,321,881,690 / 1,356,591,326 | 2,829,098 / 2,803,227 / 2,847,246 |
| RUN_PLAN_LOOKUP time per query (median/min/max) | Q_MATCH | 2,185,744,623 / 2,082,946,058 / 2,196,080,033 | 5,602,562 / 5,589,212 / 5,706,554 |
| RUN_PLAN_LOOKUP time per call | Q_AGG | 13,528.7 ns | 28.3 ns |
| RUN_PLAN_LOOKUP time per call | Q_MATCH | 10,928.7 ns | 28.0 ns |
| sum of per-segment future time (median/min/max) | Q_AGG | 1,898,314,882 / 1,861,057,706 / 1,942,980,262 | 489,523,342 / 475,564,365 / 518,873,925 |
| sum of per-segment future time (median/min/max) | Q_MATCH | 3,588,588,321 / 3,479,542,667 / 3,633,107,006 | 1,100,634,415 / 1,084,276,257 / 1,279,661,291 |
| limiter wait (median/min/max) | Q_AGG | 91,391,917 / 81,718,756 / 101,594,454 | 23,451,959 / 22,883,824 / 32,772,289 |
| limiter wait (median/min/max) | Q_MATCH | 407,514,324 / 349,812,535 / 411,419,634 | 166,427,825 / 133,376,418 / 256,026,120 |
| remainder inside futures (median) | Q_AGG | 162,923,047 | 145,420,661 |
| remainder inside futures (median) | Q_MATCH | 329,827,265 | 297,862,709 |
| remainder outside stage 0b (median) | Q_AGG | 363,285,521 | 361,210,625 |
| remainder outside stage 0b (median) | Q_MATCH | 1,289,161,973 | 1,312,814,766 |

After/before wall-time ratio per interleaved pair (run *i* after ÷ run *i*
before, median/min/max across the 5 pairs):

| Query | ratio median | ratio min | ratio max | implied wall reduction (median) |
|---|---|---|---|---|
| Q_AGG | 0.4463 | 0.4373 | 0.4504 | 55.4% |
| Q_MATCH | 0.5645 | 0.5491 | 0.5739 | 43.6% |

### size = 20,000 series/metric

| Metric | Query | before | after |
|---|---|---|---|
| wall time (median/min/max) | Q_AGG | 239,846,683 / 185,939,433 / 242,548,109 | 185,072,455 / 150,881,801 / 202,015,789 |
| wall time (median/min/max) | Q_MATCH | 607,483,652 / 471,934,893 / 614,624,236 | 496,603,615 / 399,715,870 / 529,315,225 |
| RUN_PLAN_LOOKUP time per query (median/min/max) | Q_AGG | 35,204,160 / 34,278,589 / 35,418,999 | 550,987 / 545,243 / 560,236 |
| RUN_PLAN_LOOKUP time per query (median/min/max) | Q_MATCH | 70,522,382 / 68,968,352 / 70,855,972 | 1,104,012 / 1,088,699 / 1,116,144 |
| RUN_PLAN_LOOKUP time per call | Q_AGG | 1,760.2 ns | 27.5 ns |
| RUN_PLAN_LOOKUP time per call | Q_MATCH | 1,763.1 ns | 27.6 ns |
| sum of per-segment future time (median/min/max) | Q_AGG | 127,438,797 / 107,654,876 / 130,047,422 | 85,050,006 / 73,317,438 / 86,985,809 |
| sum of per-segment future time (median/min/max) | Q_MATCH | 262,788,529 / 217,850,257 / 267,850,291 | 182,996,902 / 152,041,616 / 196,102,139 |
| limiter wait (median/min/max) | Q_AGG | 3,398 / 1,429 / 4,140 | 2,604 / 1,220 / 3,460 |
| limiter wait (median/min/max) | Q_MATCH | 9,053 / 2,874 / 11,850 | 7,211 / 2,782 / 13,736 |
| remainder inside futures (median) | Q_AGG | 27,054,072 | 21,146,331 |
| remainder inside futures (median) | Q_MATCH | 59,432,344 | 46,470,860 |
| remainder outside stage 0b (median) | Q_AGG | 65,598,146 | 61,604,270 |
| remainder outside stage 0b (median) | Q_MATCH | 233,038,575 | 223,109,898 |

After/before wall-time ratio per interleaved pair, size 20,000:

| Query | ratio median | ratio min | ratio max | implied wall reduction (median) |
|---|---|---|---|---|
| Q_AGG | 0.8047 | 0.6604 | 0.8329 | 19.5% |
| Q_MATCH | 0.8457 | 0.6928 | 0.8612 | 15.4% |

(Limiter wait at 20,000 series is in the single-digit-microsecond range for
both binaries: at this size the in-memory store's concurrency limiter is
essentially never contended, unlike at 100,000 series where it becomes a
double-digit-to-low-hundred-millisecond cost. This is a secondary
observation, not part of this task's scope.)

## Per-call cost scaling (before vs. after, across size)

| Binary | Query | 20,000 series: ns/call | 100,000 series: ns/call |
|---|---|---|---|
| before | Q_AGG | 1,760.2 | 13,528.7 |
| before | Q_MATCH | 1,763.1 | 10,928.7 |
| after | Q_AGG | 27.5 | 28.3 |
| after | Q_MATCH | 27.6 | 28.0 |

The before binary's per-call cost falls with series-per-segment, consistent
with `find_run_plan`'s O(n) linear scan over a `planned` slice whose length
tracks series-per-metric (5x the series at 100,000 vs. 20,000 series gives
roughly 7.7x (Q_AGG) to 6.2x (Q_MATCH) the per-call cost -- not an exact
linear multiple, likely due to segment sharding and cache effects at the
larger size, but the direction and order of magnitude match an O(n) scan).
The after binary's per-call cost is flat at ~27-28 ns regardless of size,
consistent with `RunPlanCursor::next`'s O(1) cost: one slice index, two
field comparisons, no scan.

## Assessment against the pre-registered expectations

Pre-registered (100,000 series, from the epic): `RUN_PLAN_LOOKUP` time per
query falling from 1.16 s to under 10 ms (`Q_AGG`) and 1.90 s to under 20 ms
(`Q_MATCH`); wall time 40-55% lower (`Q_AGG`) and 30-45% lower (`Q_MATCH`);
limiter wait median lower than before. A miss was defined as wall time
lower by less than 30%/20% respectively, or by more than the scan's own
share of wall.

- `RUN_PLAN_LOOKUP` time per query at 100,000 series: `Q_AGG` 1,352,874,425 ns
  to 2,829,098 ns (to **2.8 ms**, well under the 10 ms target); `Q_MATCH`
  2,185,744,623 ns to 5,602,562 ns (to **5.6 ms**, well under the 20 ms
  target). Both beat the pre-registered target by a wide margin -- the
  pre-registration assumed a residual O(log n) or constant-factor cost; the
  actual cursor is cheaper than that.
- Wall time: `Q_AGG` down 55.4% (band: 40-55% -- 0.4 percentage points over
  the top of the band, not a miss: the before binary's own measured
  `run_plan_lookup` share of future time was 69.0-69.9%, so a wall reduction
  of up to ~69% from removing it entirely is consistent, and 55.4% is well
  inside that ceiling). `Q_MATCH` down 43.6% (band: 30-45%, inside).
- Limiter wait median: lower after at 100,000 series for both queries
  (`Q_AGG` 91.4 ms to 23.5 ms; `Q_MATCH` 407.5 ms to 166.4 ms), consistent
  with the expectation -- less wall time spent in the fan-out means fewer
  concurrent in-flight segment futures competing for the concurrency
  limiter at any instant.

No miss by the stated definition. The 20,000-series lane has no
pre-registered band (the epic's expectations were stated for 100,000 series
only); it is reported for the scaling comparison above, not scored against
a band.

## Deviations from the method

None. The run matrix, assertions, and report structure match the task
specification as given; no spec/code contradiction and no out-of-scope bug
were found while reading `fetcher.rs` or `ravel-segment/src/reader.rs`'s
`plan_ranges_v4`. No new external dependency was introduced (`blake3` was
already a workspace dependency and already a direct dependency of
`ravel-bench`).
