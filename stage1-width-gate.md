# Stage 1 width gate (issue #2563, epic #2467, ADR-2467 decision 4)

Question: is routing row-shaped input through the columnar builder
(`ColumnarLogBatch::from_records`, drop the records, `push_columnar`,
`finish`; arm C) still cheap when the records carry 105 dynamic attributes
instead of the 4 that stages 0c and 0f measured? Arm R is `RlogWriter::push`
per record, then `finish`.

## Host

- Darwin 25.6.0 arm64 (Apple M-series, `RELEASE_ARM64_T6050`), 15 cores,
  24 GB RAM, `Darwin Panagiotiss-MacBook-Pro.local`. NOT x86_64 and NOT a
  Pi-class executor. The host is shared (14-15 logged-in users); load average
  was 3.7 to 6.4 throughout, so absolute times carry that noise. Arms are
  interleaved inside each process, so the ratios see the same load.
- `CARGO_BUILD_JOBS=3`. Release build, `CARGO_PROFILE_RELEASE_DEBUG=true`.
- Binary directory: `/Users/pmoust/.fleet/executor/cache/cargo-target/f0ad66d4-941e-40a0-82b3-a8d43af5eb45/release`.

Uptime lines (from `uptime`, each taken by the bin before and after its shape):

```
wide_1_stream          before 17:48 load 3.93 4.00 4.34   after 17:51 load 3.75 4.12 4.34
wide_1000_streams      before 17:51 load 4.02 4.16 4.35   after 17:54 load 6.39 5.60 4.98
control_1_stream       before 17:54 load 5.96 5.53 4.96   after 17:54 load 5.36 5.41 4.92
control_1000_streams   before 17:54 load 5.01 5.34 4.90   after 17:55 load 4.69 5.26 4.87
```

## Commands

```sh
export CARGO_BUILD_JOBS=3 CARGO_PROFILE_RELEASE_DEBUG=true
cargo build -p ravel-bench --release --bin logseg_peak_by_site_wide \
  --bin logseg_peak_by_site --bin logseg_width_gate_time
B=<binary directory above>
$B/logseg_width_gate_time wide_1_stream        # one process per shape;
$B/logseg_width_gate_time wide_1000_streams    # likewise control_1_stream,
$B/logseg_width_gate_time control_1000_streams # control_1000_streams
$B/logseg_peak_by_site_wide {row|col_dropped} {wide_1_stream|wide_1000_streams}
$B/logseg_peak_by_site      {row|col_dropped} {1_stream|1000_streams}   # control
```

Timing method (in the bin): 3 warmup runs, then 5 measured runs, each run the
mean of 10 iterations; each iteration times arm R then arm C back to back, and
times `from_records` as a sub-interval of arm C. Input cloning is outside the
timed region for both arms. The "run mean" columns below are those per-run
means; the ratio C/R is taken per run, then median, min and max over the 5.

## Corpus

Wide: 20,000 records, 105 dynamic attributes per record, same names in every
record (`attr.<word>.<idx>`, 11 to 19 bytes): 42 str, 37 i64, 16 f64, 10 bool.
String columns carry 5 to 300 distinct values at 4 to 40 bytes. Body,
severity, timestamps and stream identity follow
`ravel-logseg/benches/common/mod.rs`. Shapes: 1 stream x 20,000 and 1,000
streams x 20. Control: the existing 4-attribute corpus
(`http.status_code`, `duration_ms`, `retryable`, `route`), same two shapes.

Column cap: `RlogConfig::default().max_dynamic_columns = 1000`
(`crates/ravel-logseg/src/writer.rs:79`; the bin printed
`max_dynamic_columns=1000`). The cap is above 105, so the writer promotes
every attribute and overflows none; no second, sub-cap wide corpus was needed
and none was added. Measured from the finished objects (`WriteStats` and a
decode of the FIELD_DIR section):

| corpus | attributes | dynamic_columns_used | dynamic_columns_overflowed | FIELD_DIR entries |
|---|---:|---:|---:|---:|
| wide (both shapes, both arms) | 105 | 106 | 0 | 106 |
| control (both shapes, both arms) | 4 | 5 | 0 | 5 |

Promoted: all 105 (and all 4 for the control); overflowed to the attribute
blob: 0. The finished object holds one more dynamic column than the corpus has
attributes, in the wide and the control corpus alike. I did not investigate
what that extra entry is; the 105 attribute columns are not capped, which is
what the gate needs.

## Time

Milliseconds, per-run means over 5 runs: median (min to max).

| shape | arm R | arm C | from_records | from_records share of arm C | C/R per run |
|---|---|---|---|---|---|
| wide_1_stream | 1134.36 (1118.70 to 1157.99) | 980.18 (956.39 to 1059.66) | 308.39 (300.68 to 319.08) | 31.93% (30.11% to 32.26%) | median 0.8641, min 0.8521, max 0.9151 |
| wide_1000_streams | 1162.77 (1141.88 to 1302.35) | 991.41 (954.47 to 1066.49) | 311.47 (307.17 to 335.12) | 31.63% (31.40% to 32.75%) | median 0.8359, min 0.8189, max 0.8526 |
| control_1_stream | 53.83 (52.31 to 55.09) | 55.49 (54.18 to 56.89) | 8.07 (7.77 to 8.22) | 14.55% (14.28% to 14.69%) | median 1.0328, min 1.0155, max 1.0398 |
| control_1000_streams | 40.14 (40.02 to 42.64) | 40.12 (39.51 to 41.74) | 8.82 (8.67 to 9.06) | 21.94% (21.72% to 22.10%) | median 0.9978, min 0.9747, max 1.0033 |

Per-run C/R: wide_1_stream 0.8658, 0.8634, 0.8521, 0.8641, 0.9151;
wide_1000_streams 0.8359, 0.8367, 0.8526, 0.8193, 0.8189;
control_1_stream 1.0155, 1.0328, 1.0398, 1.0300, 1.0359;
control_1000_streams 0.9747, 1.0033, 0.9978, 0.9997, 0.9789.

## Peak heap (dhat t-gmax, live bytes at the global peak)

One process per (arm, shape); profiler starts before the corpus is built, so
the input records are inside the peak. Bytes are dhat `gb` live at t-gmax,
summed per site (innermost frame in a ravel crate or in these bins, skipping
allocator shims). Raw dhat JSON stays under `.gate-logs/`, not committed
(8 files, each several hundred KB to a few MB, and `.gate-logs/` is gitignored).

| shape | row t-gmax | col_dropped t-gmax | col_dropped / row | col_dropped lower by |
|---|---:|---:|---:|---:|
| wide_1_stream | 364,207,388 | 322,065,765 | 0.8843 | 11.57% |
| wide_1000_streams | 360,889,728 | 323,499,165 | 0.8964 | 10.36% |
| control_1_stream | 28,643,968 | 19,824,203 | 0.6921 | 30.79% |
| control_1000_streams | 30,295,984 | 21,256,555 | 0.7016 | 29.84% |

Sites covering at least 95% of each wide peak (bytes, blocks, share):

### row / wide_1_stream: 364,207,388

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 117,600,000 | 20,000 | 32.29% | `wide_corpus::wide_attrs` (wide_corpus.rs:142) |
| 84,000,000 | 20,000 | 23.06% | `writer::resolve_row` (writer.rs:2153) |
| 51,200,000 | 20,000 | 14.06% | `writer::emit_merged` (writer.rs:2638) |
| 47,520,000 | 2,100,000 | 13.05% | `wide_corpus::attr_name` (wide_corpus.rs:84) |
| 18,858,994 | 840,000 | 5.18% | `wide_corpus::str_value` (wide_corpus.rs:102) |
| 18,279,514 | 840,000 | 5.02% | `record::resolve_value` (record.rs:344) |
| 5,242,880 | 1 | 1.44% | `RlogWriter::push` (writer.rs:272) |
| 4,646,514 | 102 | 1.28% | `page::compress_if_smaller` (page.rs:36) |
| REMAINDER 16,859,486 | | | |

### col_dropped / wide_1_stream: 322,065,765

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 117,600,000 | 20,000 | 36.51% | `wide_corpus::wide_attrs` |
| 110,100,480 | 105 | 34.19% | `ColumnarLogBatch::from_records` (columnar_batch.rs:374) |
| 47,520,000 | 2,100,000 | 14.75% | `wide_corpus::attr_name` |
| 18,858,994 | 840,000 | 5.86% | `wide_corpus::str_value` |
| 18,279,514 | 840,000 | 5.68% | `AttrValue::clone` (logstream.rs) |
| REMAINDER 9,706,777 | | | |

### row / wide_1000_streams: 360,889,728

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 117,600,000 | 20,000 | 32.59% | `wide_corpus::wide_attrs` |
| 84,000,000 | 20,000 | 23.28% | `writer::resolve_row` |
| 51,200,000 | 20,000 | 14.19% | `writer::emit_merged` |
| 47,520,000 | 2,100,000 | 13.17% | `wide_corpus::attr_name` |
| 18,859,531 | 840,000 | 5.23% | `wide_corpus::str_value` |
| 18,280,025 | 840,000 | 5.07% | `record::resolve_value` |
| 5,242,880 | 1 | 1.45% | `RlogWriter::push` |
| 4,325,376 | 132 | 1.20% | `BlocksBuilder::intern` (writer.rs:3361) |
| REMAINDER 13,861,916 | | | |

### col_dropped / wide_1000_streams: 323,499,165

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 117,600,000 | 20,000 | 36.35% | `wide_corpus::wide_attrs` |
| 110,100,480 | 105 | 34.03% | `ColumnarLogBatch::from_records` (columnar_batch.rs:374) |
| 47,520,000 | 2,100,000 | 14.69% | `wide_corpus::attr_name` |
| 18,859,531 | 840,000 | 5.83% | `wide_corpus::str_value` |
| 18,280,025 | 840,000 | 5.65% | `AttrValue::clone` |
| REMAINDER 11,139,129 | | | |

Coarse groups (same definitions as stage0f-true-peak.md: input records =
`wide_attrs`/`attr_name`/`str_value`/corpus build; columnar batch =
`from_records` and its helpers; row resolution = `push`/`resolve_row`/
`resolve_value`; sections/object = `emit_merged`, `build_object` and kin;
block encoding = page, bloom, `BlocksBuilder`):

| run | input records | columnar batch | row resolution | sections/object | block encoding |
|---|---:|---:|---:|---:|---:|
| row/wide_1_stream | 51.08% | -- | 29.69% | 15.17% | 4.07% |
| col_dropped/wide_1_stream | 64.43% | 35.57% | -- | -- | -- |
| row/wide_1000_streams | 51.97% | -- | 29.96% | 15.37% | 2.71% |
| col_dropped/wide_1000_streams | 64.54% | 35.46% | -- | -- | -- |

Reading: in arm C the peak falls inside `from_records`, while the source
records and the batch (a `Vec<AttrValue>` per attribute column, 110.1 MB for
105 columns) are both live; the writer's block encoding is not yet running. In
arm R the peak holds the input records plus the writer's resolved rows
(`resolve_row`, `resolve_value`) plus the section build (`emit_merged`).

## Control (narrow corpus, stage 0f figures)

Reference figures are the x86_64 ones from the task. This host is arm64.

| shape | arm | reference | measured | difference |
|---|---|---:|---:|---:|
| 1_stream | row | 28,644,048 | 28,643,968 | -80 |
| 1_stream | col_dropped | 19,824,211 | 19,824,203 | -8 |
| 1000_streams | row | 30,296,064 | 30,295,984 | -80 |
| 1000_streams | col_dropped | 21,256,563 | 21,256,555 | -8 |

All four are within 80 bytes (under 0.0003%) of the reference, consistent
with the small hash-table group-width difference expected off x86_64; the
control corpus's object bytes also match stage 0f's (1 stream: 122,896 bytes,
hash `c7b75470...0806`, the value stage0f-true-peak.md records).

## Checks

- Object bytes identical between arm R and arm C, blake3 over the whole
  object, byte comparison done in the timing bin (it exits non-zero on a
  mismatch):
  - wide_1_stream: 1,788,265 bytes, `f2d3efae1404e9a9bed53221272d436793ed2bd6530b766ce8796294c608f0a7` both arms
  - wide_1000_streams: 1,836,949 bytes, `1dba6defb9384f1ed02d92dc20f0420a3da3420f8d4e5196eb465a63f1da3e1d` both arms
  - control_1_stream: 122,896 bytes, `c7b75470317e3dcd898559d99a7eb71095807cc1af7dac453a2693869a360806` both arms
  - control_1000_streams: 34,256 bytes, `32056cbe5245ebf8b6f0da0ea781bf633f1d3e4b3a689a0e1c90dd718f025699` both arms
  - The dhat bins print the same `OBJECT_HASH` for both arms on every shape
    (wide shapes confirmed equal to the timing bin's hash).
- Rows: 20,000 per arm on all four shapes (`RlogReader` scan, `rows_r=rows_c=20000`).
- Dynamic columns in the finished object: 106 (wide), 5 (control); overflowed 0.
- dhat per-site sum equals dhat's own t-gmax total, all 8 runs:
  364,207,388; 322,065,765; 360,889,728; 323,499,165; 28,643,968;
  19,824,203; 30,295,984; 21,256,555 (each sum of `gb` over all sites matches
  the `At t-gmax` line to the byte).
- Control against the x86_64 figures: see the Control section; off by 80 and 8
  bytes on arm64.

## Pre-registered expectations (low confidence, not tuned toward)

- Time C/R between 0.6 and 1.15 on the wide corpus: wide_1_stream 0.8641, inside; wide_1000_streams 0.8359, inside.
- Peak of col_dropped 10% to 40% lower than row: wide_1_stream 11.57% lower, inside; wide_1000_streams 10.36% lower, inside (near the lower edge).
- from_records 15% to 45% of arm C's time: wide_1_stream 31.93%, inside; wide_1000_streams 31.63%, inside.

## Gate

Gate: C/R over 1.15 at either stream count fails.

wide_1_stream: C/R median = 0.8641: at or under 1.15, gate passes
wide_1000_streams: C/R median = 0.8359: at or under 1.15, gate passes

## Caveats

- Shared, loaded host (load 3.7 to 6.4); run-to-run spread within a shape is
  4% to 12% on the maxima, a few outliers (arm R max 1302 ms at
  wide_1000_streams). Medians of ratios were stable across runs: every
  per-run wide C/R is under 0.92.
- One architecture (arm64 macOS); no x86_64 run.
- The control's C/R is about 1.0 (1.03 and 1.00), so the columnar route
  gains time only on the wide corpus in this measurement.
- Timing covers writer work only; input construction and cloning are outside
  the timed region.
