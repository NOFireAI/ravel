# Stage 2: sparse-input measurement (issue #2585, epic #2467)

**Question**: on records with few attributes drawn from many distinct keys,
how do the row builder and the columnar route (`ColumnarLogBatch::from_records`
-> drop records -> `push_columnar` -> `finish`) compare in peak heap and
encode time? All prior measurements (stage0f, stage1) put every attribute on
every record, so this shape -- memory growing with `records x distinct_keys`
rather than `records x attrs_present`, because `from_records` allocates one
dense `Vec<Option<AttrValue>>` per distinct `(name, type)` key across all
records -- was never exercised.

## Host and method

- Host: `ci-16gb-fsn1-1`, Linux 6.8.0-60-generic, x86_64, 8 cores, Ubuntu
  24.04.2 LTS. `MemTotal` 15,988,672 kB, `MemAvailable` at start 13,636,980 kB
  (~13.76 GiB of ~15.25 GiB). `df -h`: `/` (and `/var/lib/fleet`, same
  filesystem) `226G 135G 82G 63%`.
- `CARGO_BUILD_JOBS=2` (formula `min(4, max(2, cores/4))` = `min(4, max(2,
  2))` = 2; the `<=11GB` memory cap does not additionally bind since total
  RAM is ~15.6 GB, but the core-count term alone already yields 2).
- New files, `crates/ravel-bench` only: `src/bin/sparse_corpus.rs` (shared
  corpus builder via `#[path]`, not its own `[[bin]]`), `src/bin/
  logseg_peak_by_site_sparse.rs` (dhat true-peak bin, arms `row`/
  `col_dropped`), `src/bin/logseg_sparse_gate_time.rs` (interleaved wall-time
  bin), plus the two `[[bin]]` registrations in `Cargo.toml`. `ravel-logseg`
  was not touched.
- Corpus: `N` records (1 stream), each with exactly 10 attributes whose
  names are drawn from `K` distinct keys via `key_index = (record_index*10 +
  j*7) mod K` for `j` in `0..10` (stride 7, and the per-record spread `0..63`
  is always `< K` for every `K` used here, so no in-record collision is
  possible regardless of `gcd(7,K)`; a `HashSet`-based assertion checks this
  at run time rather than only relying on the argument). Each key index has
  one fixed type (~40% str / 35% i64 / 15% f64 / 10% bool by index range).
  Names are `attr.<word>.<idx:04>`, 11-19 bytes for every `K` up to 10,000.
  Body/severity/timestamps/trace-span/stream-identity formulas reproduce
  `ravel-logseg/benches/common/mod.rs` exactly for the single fixed stream
  (index 0). Pure function of `(record_index, K)`: no wall clock, no
  unseeded randomness.
- Peak heap: `dhat::Alloc` global allocator, one process per `(arm, shape)`,
  profiler started before the corpus is built (so the peak can include the
  input batch), process ends normally so `Drop` writes the report at the
  true end of run. Built with `CARGO_PROFILE_RELEASE_DEBUG=true`. Site
  attribution and the 7-category grouping (input records / columnar batch /
  row resolution / stream seeds / block encoding / sections+object / other;
  "bench harness" never surfaced as its own site, same as stage0f) follow
  `stage0f-true-peak.md`'s method exactly, adapted only to recognize this
  task's own corpus/bin source files (`sparse_corpus.rs`,
  `logseg_peak_by_site_sparse.rs`) as "input records" instead of
  `common.rs`. Analysis script: `.gate-logs/analyze_sparse.py` (not
  committed, same convention as stage0f/stage1's own scripts).
- Wall time: `logseg_sparse_gate_time`, one process per shape, both arms in
  the same process (so the harness-overhead baseline is shared), corpus
  clone built outside the timed region, `from_records` timed as a
  sub-interval of arm C. 20,000-row shapes: 2 warmups + 3 runs of 3 encodes
  per arm. The 200,000-row shape and the K=10,000 ("slow and large") shape
  use the reduced schedule: 1 warmup + 3 runs of 1 encode. `uptime` printed
  before/after each shape.
- Memory rule applied before every run (`/proc/meminfo` `MemAvailable`
  reread immediately before each command, not cached from task start): run
  a shape/arm only if available memory is at least twice its pre-registered
  expected-peak upper bound.

## Memory-safety skip (method deviation, reported as instructed)

Available memory throughout the run stayed at ~13.5-13.76 GiB (no other
process materially changed it). Two `(arm=col_dropped, shape)` cells have
pre-registered upper bounds whose 2x threshold exceeds that:

- `col_dropped / sparse_20000_10000`: band 6.3-7.0 GB -> needs >=14.0 GB.
- `col_dropped / sparse_200000_1000`: band 6.5-8.0 GB -> needs >=16.0 GB.

Both were **skipped** (no dhat run, and the corresponding arm-C timing run
skipped via a `--skip-col` flag added to `logseg_sparse_gate_time`, printing
`ARM_C_SKIPPED reason=memory-safety-check-pre-run` and a
`CORRECTNESS_ARM_R_ONLY` line in place of the two-arm hash comparison). Arm
`row` ran for both shapes (its own pre-registered upper bounds, 90 MB and
800 MB, are nowhere near the available headroom). This is the "run a shape
only if available memory is at least twice its expected peak; otherwise
SKIP that shape, say so with the available figure, run the others" rule
from the dispatch, applied literally. No peak heap or columnar timing
figure exists for K=10,000's columnar arm or for the 200,000-row shape's
columnar arm; every table below marks these cells SKIPPED rather than
inventing or estimating a number.

## Peak heap, smallest-expected-peak-first

| arm | shape | PEAK (bytes) | pre-registered band | verdict |
|---|---|---:|---|---|
| row | sparse_20000_100 | 52,222,652 | 40-80 MB | within |
| row | sparse_20000_1000 | 57,575,912 | 40-80 MB | within |
| row | sparse_20000_10000 | 56,169,039 | 40-90 MB | within |
| col_dropped | sparse_20000_100 | 91,892,944 | 100-160 MB | **below** |
| col_dropped | sparse_20000_1000 | 668,053,147 | 650-800 MB | within |
| row | sparse_200000_1000 | 486,138,900 | 400-800 MB | within |
| col_dropped | sparse_20000_10000 | SKIPPED | 6.3-7.0 GB | n/a (memory safety) |
| col_dropped | sparse_200000_1000 | SKIPPED | 6.5-8.0 GB | n/a (memory safety) |

`col_dropped/sparse_20000_100` (91,892,944 bytes = 87.6 MiB) lands just
below its 100-160 MB band; reported as measured, not adjusted. It is well
above the "under 1/3 of expected" threshold the task calls "a miss that
matters" (1/3 of 100 MB = 33 MB), so this is a near-miss, not that failure
mode.

### Per-arm site tables (>=95% of PEAK, descending)

**row / sparse_20000_100** -- PEAK 52,222,652 bytes

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 11,200,000 | 20,000 | 21.45% | `sparse_corpus::sparse_attrs` (sparse_corpus.rs:141:10) |
| 8,320,000 | 20,000 | 15.93% | `writer::emit_merged` (writer.rs:2638:18) |
| 8,000,000 | 20,000 | 15.32% | `writer::resolve_row` (writer.rs:2153:36) |
| 5,242,880 | 1 | 10.04% | `RlogWriter::push` (writer.rs:272:22) |
| 4,512,000 | 200,000 | 8.64% | `sparse_corpus::key_name` (sparse_corpus.rs:73:5) |
| 4,000,000 | 1 | 7.66% | `RlogWriter::build_object` (writer.rs:617:42) |
| 1,769,518 | 80,000 | 3.39% | `sparse_corpus::str_value` (sparse_corpus.rs:81:17) |
| 1,709,396 | 80,000 | 3.27% | `record::resolve_value` (record.rs:344:66) |
| 1,280,000 | 20,000 | 2.45% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 764,028 | 42 | 1.46% | `BlocksBuilder::intern` (writer.rs:3350:37) |

Group shares: input records 37.40%, row resolution 29.78%, block encoding
7.21%, sections/object 25.58%, stream seeds 0.00%, other 0.03%.

**row / sparse_20000_1000** -- PEAK 57,575,912 bytes

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 11,200,000 | 20,000 | 19.45% | `sparse_corpus::sparse_attrs` |
| 8,000,000 | 20,000 | 13.89% | `writer::resolve_row` (2153:36) |
| 7,584,000 | 13,200 | 13.17% | `writer::emit_merged` |
| 5,242,880 | 1 | 9.11% | `RlogWriter::push` |
| 4,545,600 | 200,000 | 7.89% | `sparse_corpus::key_name` |
| 4,000,000 | 1 | 6.95% | `RlogWriter::build_object` (617:42) |
| 2,573,040 | 402 | 4.47% | `BlocksBuilder::intern` (3350:37) |
| 1,806,527 | 80,000 | 3.14% | `sparse_corpus::str_value` |
| 1,749,099 | 80,000 | 3.04% | `record::resolve_value` |
| 1,683,216 | 3 | 2.92% | `SealedPage::append` |
| 1,638,416 | 1 | 2.85% | `BloomBuilder::finish_sized` |

Group shares: input records 34.05%, row resolution 27.08%, block encoding
12.61%, sections/object 26.26%.

**row / sparse_20000_10000** -- PEAK 56,169,039 bytes

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 11,200,000 | 20,000 | 19.94% | `sparse_corpus::sparse_attrs` |
| 7,484,036 | 20,000 | 13.32% | `logstream::put_uvarint` |
| 5,242,880 | 1 | 9.33% | `RlogWriter::push` |
| 4,547,040 | 200,000 | 8.10% | `sparse_corpus::key_name` |
| 4,194,304 | 1 | 7.47% | `ravel_logseg::varint::put_uvarint` |
| 4,000,000 | 1 | 7.12% | `RlogWriter::build_object` |
| 2,777,088 | 1 | 4.94% | `ravel_logseg::varint::put_uvarint` |
| 2,212,290 | 1 | 3.94% | `page::compress_if_smaller` |
| 2,071,733 | 7,534 | 3.69% | `Stager::string_shape::{closure#2}` |
| 1,816,155 | 80,000 | 3.23% | `sparse_corpus::str_value` |
| 1,769,600 | 11,060 | 3.15% | `writer::emit_merged` |

Group shares: input records 48.24% (now the largest group -- `key_name`'s
200,000 per-record-attribute string allocations and `sparse_attrs`'s own
Vec dominate even more at K=10,000, since only 1,000 of the 10,000 distinct
keys fit the dynamic-column budget and the rest resolve through a path that
still goes through row-resolution's uvarint encoding per rejected/overflowed
attr), row resolution 12.15%, block encoding 26.06%, sections/object 13.54%.

**col_dropped / sparse_20000_100** -- PEAK 91,892,944 bytes

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 64,000,000 | 100 | 69.65% | `ColumnarLogBatch::from_records::{closure#1}` (columnar_batch.rs:345:70) |
| 11,200,000 | 20,000 | 12.19% | `sparse_corpus::sparse_attrs` |
| 4,512,000 | 200,000 | 4.91% | `sparse_corpus::key_name` |
| 3,200,000 | 1 | 3.48% | `sparse_corpus::build_sparse_corpus` |
| 1,769,518 | 80,000 | 1.93% | `sparse_corpus::str_value` |
| 1,709,396 | 80,000 | 1.86% | `AttrValue::clone` (logstream.rs:17:9) |
| 1,280,000 | 20,000 | 1.39% | `logstream::put_uvarint` |

Group shares: input records 26.60%, columnar batch 73.40% (entirely the
dense per-key `Vec<Option<AttrValue>>` allocation site, `closure#1` at
columnar_batch.rs:345:70 -- exactly the mechanism under test). 64,000,000
bytes at that single site equals `DENSE_VECTOR_ESTIMATE_BYTES` for this
shape exactly (`K*N*size_of::<Option<AttrValue>>` = `100*20000*32` =
64,000,000).

**col_dropped / sparse_20000_1000** -- PEAK 668,053,147 bytes

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 640,000,000 | 1,000 | 95.80% | `ColumnarLogBatch::from_records::{closure#1}` (columnar_batch.rs:345:70) |

(REMAINDER 4.20% = input records; `from_records::{closure#1}` alone already
clears the 95% cutoff.) Group shares: input records 3.68%, columnar batch
96.32%. `640,000,000` equals `DENSE_VECTOR_ESTIMATE_BYTES` for this shape
exactly (`1000*20000*32`); the measured columnar-batch group total
(643,501,523) exceeds it by only 0.55% (the few remaining `from_records`
closures/`VarBytes` sites).

**row / sparse_200000_1000** -- PEAK 486,138,900 bytes

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 112,000,000 | 200,000 | 23.04% | `sparse_corpus::sparse_attrs` |
| 80,000,000 | 200,000 | 16.46% | `writer::resolve_row` (2153:36) |
| 75,840,000 | 132,000 | 15.60% | `writer::emit_merged` |
| 45,456,000 | 2,000,000 | 9.35% | `sparse_corpus::key_name` |
| 41,943,040 | 1 | 8.63% | `RlogWriter::push` |
| 40,000,000 | 1 | 8.23% | `RlogWriter::build_object` |
| 18,065,237 | 800,000 | 3.72% | `sparse_corpus::str_value` |
| 17,489,277 | 800,000 | 3.60% | `record::resolve_value` |
| 12,800,000 | 200,000 | 2.63% | `logstream::put_uvarint` |
| 10,848,192 | 24 | 2.23% | `SealedPage::append` |

Group shares: input records 40.33%, row resolution 29.95%, block encoding
4.21%, sections/object 25.51%.

### Dense-vector estimate vs. measured columnar-batch group

`size_of::<Option<AttrValue>>() = 32` bytes on x86_64.

| shape | K*N*32 (dense estimate) | measured columnar-batch group | estimate/measured |
|---|---:|---:|---:|
| sparse_20000_100 | 64,000,000 | 67,451,632 | 0.949 |
| sparse_20000_1000 | 640,000,000 | 643,501,523 | 0.9946 |
| sparse_20000_10000 | 6,400,000,000 | SKIPPED | n/a |
| sparse_200000_1000 | 6,400,000,000 | SKIPPED | n/a |

The dense estimate under-counts the measured group by under 5.5% at both
measured points -- consistent with the single dominant site being almost
exactly the dense vector, plus a small residue of other per-key allocations
(`VarBytes`, closures) that the back-of-envelope formula does not count.

## Wall time

| shape | schedule | R median | C median | C/R ratio | from_records share of C |
|---|---|---:|---:|---:|---:|
| sparse_20000_100 | 2 warmup + 3x3 | 283,846,931 ns | 311,440,047 ns | 1.1072 | 20.59% |
| sparse_20000_1000 | 2 warmup + 3x3 | 1,081,407,736 ns | 819,133,812 ns | 0.7575 | 18.28% |
| sparse_20000_10000 | 1 warmup + 3x1 (reduced) | 447,610,684 ns | SKIPPED | n/a | n/a |
| sparse_200000_1000 | 1 warmup + 3x1 (reduced) | 10,577,536,480 ns | SKIPPED | n/a | n/a |

Pre-registered time-ratio bands (columnar/row): K=1,000 expects 1.5-5;
K=10,000 expects 5-50.

- `sparse_20000_1000` measures **0.7575** -- columnar is *faster* than row
  here, well **below** the 1.5-5 band (not merely a miss, the direction
  itself is opposite what the band assumes). Per-run ratios
  (0.7575/0.7717/0.7476) are tight, so this is not measurement noise.
- K=10,000's columnar arm was not run (memory-safety skip above), so the
  5-50 band cannot be checked either way.
- No band was pre-registered for K=100's ratio (1.1072, measured for
  completeness since the shape was cheap to run both arms on).

Neither arm-C timing run exceeded the 5-minute single-measurement bailout
(the bailout did not trigger for either shape that ran arm C).

`uptime` before/after every shape showed load average moving only in the
0.5-1.2 range throughout (single-tenant box, no contention observed).

## Summary table (deliverable 7)

| shape | row peak | columnar peak | peak ratio (C/R) | dense estimate | measured columnar-batch group | row time | columnar time | time ratio (C/R) | from_records share |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| sparse_20000_100 | 52,222,652 | 91,892,944 | 1.7596 | 64,000,000 | 67,451,632 | 283,846,931 ns | 311,440,047 ns | 1.1072 | 20.59% |
| sparse_20000_1000 | 57,575,912 | 668,053,147 | 11.605 | 640,000,000 | 643,501,523 | 1,081,407,736 ns | 819,133,812 ns | 0.7575 | 18.28% |
| sparse_20000_10000 | 56,169,039 | SKIPPED | n/a | 6,400,000,000 | SKIPPED | 447,610,684 ns | SKIPPED | n/a | n/a |
| sparse_200000_1000 | 486,138,900 | SKIPPED | n/a | 6,400,000,000 | SKIPPED | 10,577,536,480 ns | SKIPPED | n/a | n/a |

Pre-registered peak-ratio bands (C/R, same shape order): 1.5-3, 8-20, 70-170,
8-20.

- `sparse_20000_100`: 1.7596, **within** 1.5-3.
- `sparse_20000_1000`: 11.605, **within** 8-20.
- `sparse_20000_10000`, `sparse_200000_1000`: not measurable (columnar arm
  skipped for memory safety); the dense estimate alone (6.4 GB for both)
  is consistent with the pre-registered upper bounds of 7.0 GB and 8.0 GB
  respectively, which is exactly why those two cells were judged unsafe to
  run on a box with ~13.6 GB available.

## Control

Existing `logseg_peak_by_site` bin (unmodified), shape `1_stream`:

| arm | measured (bytes) | required (bytes) | match |
|---|---:|---:|---|
| row | 28,644,048 | 28,644,048 | exact |
| col_dropped | 19,824,211 | 19,824,211 | exact |

## Checks

- **Object-bytes-identical (hash), per shape where both arms ran**:
  - sparse_20000_100: `c78e3ed1304ccebe945f6cb204280c3291f66e373fdda6c6862e38a2695efa87`, row == col_dropped (confirmed independently by both the dhat bin and the timing bin's own `hash_r`/`hash_c`/`hash_match=true`).
  - sparse_20000_1000: `4de4eaa963779332bfc6c5d0f420161f1e9fc56c683303bae1c9270c94744f29`, row == col_dropped (same double confirmation).
  - sparse_20000_10000: only `row` ran (`b559596407e13b51a6eeaab6954b1973cf817aac3e527c8468e5647a69bf1617`); no columnar hash to compare (memory-safety skip).
  - sparse_200000_1000: only `row` ran (`15f4ee433521469d91818db3078eaf83a4d0ecd4277c6cf23eb008a038a5469a`); no columnar hash to compare (memory-safety skip).
- **rows-per-arm == N**: confirmed for every run that executed: 20,000
  (sparse_20000_100 row+col, sparse_20000_1000 row+col, sparse_20000_10000
  row), 200,000 (sparse_200000_1000 row).
- **Dynamic columns used/overflowed per shape** (budget 1,000, both arms
  where both ran):
  - K=100: used=101, overflowed=0 (both arms).
  - K=1,000 (20,000 recs): used=1,000, overflowed=1 (both arms).
  - K=10,000: used=1,000, overflowed=9,001 (row only) -- confirms "most
    K=10,000 keys overflow" (90.01% of the 10,000 distinct keys).
  - K=1,000 (200,000 recs): used=1,000, overflowed=1 (row only).
  - Observation, not investigated further (out of scope -- `ravel-logseg`
    is off-limits for this task): `used` is consistently `distinct_keys + 1`
    rather than exactly `distinct_keys` (101 at K=100 where all 100 keys are
    used, 1,000 at K=1,000 where the budget itself is 1,000). Both figures
    are identical across arms at every shape where both ran, so this is not
    a row/columnar divergence; it looks like a reserved internal column
    rather than a defect, but this was not traced further since `writer.rs`
    is out of this task's scope.
- **dhat per-site sum == dhat's own t-gmax total**: exact match in all 6
  runs (52,222,652 / 57,575,912 / 56,169,039 / 486,138,900 / 91,892,944 /
  668,053,147), confirmed by `.gate-logs/analyze_sparse.py`'s own summed
  total against each run's stderr "At t-gmax" line.
- **CONTROL**: see table above -- both figures match exactly.

## Skipped shapes

- `col_dropped / sparse_20000_10000` (dhat peak run): skipped, expected
  band 6.3-7.0 GB needs >=14.0 GB available; measured 13.6-13.76 GB
  throughout.
- `col_dropped / sparse_200000_1000` (dhat peak run): skipped, expected
  band 6.5-8.0 GB needs >=16.0 GB available; measured 13.6-13.76 GB
  throughout.
- Arm C (columnar) of the `logseg_sparse_gate_time` wall-time run for both
  of the same two shapes: skipped for the same reason (same
  `from_records` call, same peak-memory risk), via a `--skip-col` flag
  added to the bin for this purpose. Arm R (row) still ran for both.

No shape's arm-C timing run hit the 5-minute single-measurement bailout
(it did not apply, since those two shapes' arm C was pre-emptively skipped
rather than attempted and timed out).

## Method deviations

1. **Two `(col_dropped, shape)` cells skipped for memory safety**, detailed
   above -- the task's own "apply memory rule before each run" instruction,
   applied against pre-registered upper bounds rather than optimistic point
   estimates, since dhat instrumentation itself adds bookkeeping overhead on
   top of raw heap use.
2. **`--skip-col` CLI flag added to `logseg_sparse_gate_time`** (not
   literally specified in the task text) to let the same memory-safety
   decision apply to the wall-time bin without a separate binary: when
   passed, the bin skips arm C entirely, prints `ARM_C_SKIPPED
   reason=memory-safety-check-pre-run` and `CORRECTNESS_ARM_R_ONLY` (rows
   and dynamic-column stats for arm R alone) instead of the normal two-arm
   `CORRECTNESS` line, and times only arm R.
3. **`col_dropped / sparse_20000_100`'s peak (91,892,944 bytes) lands below
   its 100-160 MB band** (measured, not adjusted) -- see "Peak heap" above.
4. **`sparse_20000_1000`'s time ratio (0.7575) lands below its 1.5-5
   band**, with columnar faster than row rather than merely under-ratio --
   see "Wall time" above.

No other deviation from the task's method. Raw dhat JSON (6 files) and
analysis/run logs live under `.gate-logs/` (gitignored, confirmed via
`git check-ignore`), **not committed**.
