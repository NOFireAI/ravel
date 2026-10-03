# Stage 0f: true heap peak by allocation site (issue #2485, epic #2467)

**This replaces the retracted figures from issues #2428, #2469, #2475, and
#2477.** Those used a `stats_alloc`-based `live_bytes = allocated -
deallocated + reallocated` formula that double-counts every `realloc`'s
growth delta, inflating all four reports' numbers 4-7x (root-caused in
stage 0e, `stage0e-live-bytes-by-site.md`). Stage 0e then measured live
bytes at a *chosen point* (profiler-drop, after a stop label), using dhat's
`eb`/`ebk` (live-at-termination) fields. This report answers a different,
stricter question: the **true global peak** (dhat's t-gmax) anywhere during
the whole encode, via dhat's `gb`/`gbk` fields (live bytes/blocks for each
allocation site *at the t-gmax instant specifically*), for an RLOG encode of
20,000 records, across the row path and two columnar-path variants.

## Host and method

- Host: x86_64, 16 cores, 30 GiB RAM, 219 GB free on `/` (`df -h /`: `484G
  265G 219G 55%`); `CARGO_TARGET_DIR` and `HOME` share the same volume.
  `CARGO_BUILD_JOBS=4` (formula: `min(4, max(2, 16/4))`, not memory-limited
  since RAM > 11 GB).
- Bin: `crates/ravel-bench/src/bin/logseg_peak_by_site.rs` (new; registered
  in `crates/ravel-bench/Cargo.toml`). One process = one `(arm, shape)` pair,
  one encode. `dhat::Profiler` starts **before** `build_corpus`, so the peak
  can include the input corpus itself; the process ends normally (no stop
  label, no `process::exit`) so `Profiler`'s `Drop` writes the report at the
  true end of the run. Built with `CARGO_PROFILE_RELEASE_DEBUG=true`.
- Arms: `row` (`push` loop + `finish`), `col_dropped` (`from_records` then
  `drop(corpus)` immediately, then `push_columnar` + `finish`), `col_kept`
  (same but `corpus` is kept alive until after `finish` returns).
- Shapes (always 20,000 records total): `1_stream` (1 stream x 20,000
  records), `1000_streams` (1,000 x 20), `20000_streams` (20,000 x 1).
- Commands (one per arm/shape, 9 total):
  ```
  CARGO_PROFILE_RELEASE_DEBUG=true cargo build -p ravel-bench --release --bin logseg_peak_by_site
  <target>/release/logseg_peak_by_site <arm> <shape>
  ```
  run from the repo root with `CARGO_TARGET_DIR=/var/lib/fleet/cache/cargo-target/3213b1d4-5565-4ea0-8fa1-039c1d4f7f97`
  (this host redirects the cargo target dir off the checkout).
- dhat fields used: per program-point (`pps[]`) `gb`/`gbk` = bytes/blocks
  live for that allocation site **at the global-peak instant (t-gmax)**,
  confirmed by narrow reads of dhat-0.3.3's own source
  (`$CARGO_HOME/registry/src/*/dhat-0.3.3/src/lib.rs`), never read wholesale.
  This is distinct from `mb`/`mbk` (that site's own local peak, which can
  occur at a different instant than the global peak) and `eb`/`ebk` (live at
  termination, what stage 0e used). Backtrace frames (`fs`, indexed into
  `ftbl`) are ordered innermost-first.
- Site attribution: innermost frame that resolves to a file under `crates/`.
  dhat's `trim_path` keeps only the **last 3** path components, so a file
  nested one level deeper than `<crate>/src/<file>.rs` -- `benches/common/mod.rs`
  and `src/bin/logseg_peak_by_site.rs`, both exercised here because profiling
  starts before `build_corpus` (stage 0e's bin profiled after, so never hit
  this) -- loses its crate-name component in dhat's JSON. The analysis
  script (`.gate-logs/analyze_peak.py`, not committed) falls back to `find
  crates -path "*/<trimmed-path>"` and requires a unique match before
  attributing a site; this is a methodological extension of stage 0e's
  regex-only method, not a deviation from the task, and resolved all sites
  in all 9 runs (`unresolved = 0 bytes` everywhere, confirmed below).
- Raw dhat JSON (9 files) and run logs live under `.gate-logs/` (gitignored,
  confirmed in `.gitignore`), **not committed**: total size is 3,809,249
  bytes (3.7 MiB), over the 200 KB ceiling this task sets for committing raw
  JSON.

## Per-run results

Each table: sites covering >=95% of that run's PEAK (t-gmax total), by
innermost ravel-crate frame, func/file:line, bytes/allocs/share, sorted
descending; a REMAINDER line covers the rest. Bytes are `gb` (live at
t-gmax) summed per site; allocs are `gbk`.

### row / 1_stream -- PEAK = 28,644,048 bytes (280,037 blocks)

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 5,242,880 | 1 | 18.30% | `RlogWriter::push` (writer.rs:272:22) |
| 4,480,000 | 20,000 | 15.64% | `common::make_record` (benches/common/mod.rs:74:17) |
| 4,000,000 | 1 | 13.96% | `RlogWriter::build_object` (writer.rs:617:42) |
| 4,000,000 | 1 | 13.96% | `RlogWriter::build_object` (writer.rs:640:26) |
| 3,200,000 | 20,000 | 11.17% | `writer::emit_merged` (writer.rs:2638:18) |
| 3,200,000 | 20,000 | 11.17% | `writer::resolve_row` (writer.rs:2153:36) |
| 1,280,000 | 20,000 | 4.47% | `logstream::put_uvarint` (ravel-types/src/logstream.rs:44:17) |
| 680,000 | 20,000 | 2.37% | `common::make_record` (mod.rs:89:28) |
| 678,398 | 20,000 | 2.37% | `common::make_record` (mod.rs:68:20) |
| 505,334 | 20,000 | 1.76% | `writer::resolve_row` (writer.rs:2159:23) |
| 370,000 | 20,000 | 1.29% | `record::resolve_value` (record.rs:344:66) |
| 320,000 | 20,000 | 1.12% | `common::make_record` (mod.rs:76:32) |
| REMAINDER | | 2.40% | (20 smaller sites, incl. `ref_of` at writer.rs:428:52 = 100 bytes) |

### row / 1000_streams -- PEAK = 30,296,064 bytes (282,169 blocks)

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 5,242,880 | 1 | 17.31% | `RlogWriter::push` (writer.rs:272:22) |
| 4,480,000 | 20,000 | 14.79% | `common::make_record` (mod.rs:74:17) |
| 4,000,000 | 1 | 13.20% | `RlogWriter::build_object` (writer.rs:617:42) |
| 4,000,000 | 1 | 13.20% | `RlogWriter::build_object` (writer.rs:640:26) |
| 3,200,000 | 20,000 | 10.56% | `writer::emit_merged` (writer.rs:2638:18) |
| 3,200,000 | 20,000 | 10.56% | `writer::resolve_row` (writer.rs:2153:36) |
| 2,547,200 | 20,000 | 8.41% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 694,000 | 20,000 | 2.29% | `common::make_record` (mod.rs:68:20) |
| 680,000 | 20,000 | 2.24% | `common::make_record` (mod.rs:89:28) |
| 468,000 | 20,000 | 1.54% | `writer::resolve_row` (writer.rs:2159:23) |
| 370,000 | 20,000 | 1.22% | `record::resolve_value` (record.rs:344:66) |
| 320,000 | 20,000 | 1.06% | `common::make_record` (mod.rs:76:32) |
| 220,000 | 20,000 | 0.73% | `common::make_record` (mod.rs:80:27) |
| 180,000 | 20,000 | 0.59% | `common::make_record` (mod.rs:84:25) |
| 160,000 | 1,000 | 0.53% | `StreamSeed::build` (writer.rs:2329:21) |
| REMAINDER | | 1.78% | (incl. `ref_of` at writer.rs:428:52 = 43,024 bytes, 0.14%) |

### row / 20000_streams -- PEAK = 41,444,889 bytes (342,746 blocks)

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 5,242,880 | 1 | 12.65% | `RlogWriter::push` (writer.rs:272:22) |
| 4,480,000 | 20,000 | 10.81% | `common::make_record` (mod.rs:74:17) |
| 4,000,000 | 1 | 9.65% | `RlogWriter::build_object` (writer.rs:617:42) |
| 3,200,000 | 20,000 | 7.72% | `writer::emit_merged` (writer.rs:2638:18) |
| 3,200,000 | 20,000 | 7.72% | `StreamSeed::build` (writer.rs:2329:21) |
| 3,200,000 | 20,000 | 7.72% | `writer::resolve_row` (writer.rs:2153:36) |
| 2,916,352 | 1 | 7.04% | `StreamDir::encode` (stream_dir.rs:63:13) |
| 2,559,360 | 20,000 | 6.18% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 2,129,936 | 1 | 5.14% | `RlogWriter::build_object` (writer.rs:606:30) |
| 1,735,647 | 1 | 4.19% | `writer::compress` (writer.rs:3693:22) |
| 1,348,890 | 20,000 | 3.25% | `build_object::{closure#10}` (writer.rs:829:32) |
| 1,013,360 | 2,671 | 2.45% | `RlogWriter::build_object` (writer.rs:407:26) |
| 960,000 | 1 | 2.32% | `RlogWriter::build_object` (writer.rs:837:14) |
| 688,144 | 1 | 1.66% | `RlogWriter::build_object` / `ref_of` (writer.rs:428:52) |
| 680,000 | 20,000 | 1.64% | `common::make_record` (mod.rs:89:28) |
| 640,000 | 20,000 | 1.54% | `common::make_record` (mod.rs:68:20) |
| 540,000 | 20,000 | 1.30% | `writer::resolve_row` (writer.rs:2159:23) |
| 499,383 | 1 | 1.20% | `writer::push_section` (writer.rs:3705:12) |
| 360,000 | 20,000 | 0.87% | `record::resolve_value` (record.rs:344:66) |
| 320,000 | 20,000 | 0.77% | `common::make_record` (mod.rs:76:32) |
| 320,000 | 1 | 0.77% | `RlogWriter::build_object` (writer.rs:421:68) |
| 294,928 | 1 | 0.71% | `RlogWriter::build_object` (writer.rs:805:26) |
| 294,928 | 1 | 0.71% | `RlogWriter::build_object` (writer.rs:804:27) |
| REMAINDER | | 4.78% | (remaining `make_record`/`resolve_row` lines + ~30 tiny sites) |

### col_dropped / 1_stream -- PEAK = 19,824,211 bytes (200,034 blocks)

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 4,480,000 | 20,000 | 22.60% | `common::make_record` (mod.rs:74:17) |
| 4,194,304 | 4 | 21.16% | `ColumnarLogBatch::from_records` (columnar_batch.rs:374:31) |
| 3,200,000 | 1 | 16.14% | `common::build_corpus` (mod.rs:111:19) |
| 1,280,000 | 20,000 | 6.46% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 1,015,808 | 2 | 5.12% | `VarBytes::push` (columnar_batch.rs:110:19) |
| 680,000 | 20,000 | 3.43% | `common::make_record` (mod.rs:89:28) |
| 678,398 | 20,000 | 3.42% | `common::make_record` (mod.rs:68:20) |
| 640,000 | 1 | 3.23% | `from_records::{closure#1}` (columnar_batch.rs:345:70) |
| 524,288 | 1 | 2.64% | `from_records` (columnar_batch.rs:317:36) |
| 480,000 | 1 | 2.42% | `from_records` (columnar_batch.rs:305:32) |
| 370,000 | 20,000 | 1.87% | `AttrValue::clone` (logstream.rs:17:9) |
| 320,000 | 20,000 | 1.61% | `common::make_record` (mod.rs:76:32) |
| 262,144x4 | varies | 5.29% | 4 `from_records`/`VarBytes::new` sites, 1.32% each |
| REMAINDER | | 4.29% | (smaller `from_records` sites + `make_record` tail) |

Note: `build_object_columnar` contributes **nothing** here -- the peak falls
inside/at the end of `from_records`, before `push_columnar`/`build_object_columnar`
have allocated anything (corpus is dropped right after `from_records`
returns). See "Where the peak falls," below.

### col_dropped / 1000_streams -- PEAK = 21,256,563 bytes (201,033 blocks)

Same shape as 1_stream, with `put_uvarint` scaling up (2,547,200 bytes,
11.98%, 2nd rank after `make_record`/`from_records`/`build_corpus`) and a
new small `StreamSeed`-less entry; full ordering in
`.gate-logs/sites-col_dropped-1000_streams.txt`. Top sites: `make_record`
(74:17) 4,480,000/21.08%, `from_records` (374:31) 4,194,304/19.73%,
`build_corpus` 3,200,000/15.05%, `put_uvarint` 2,547,200/11.98%,
`VarBytes::push` 1,015,808/4.78%, remaining ~20 sites to 99.53%,
REMAINDER 0.47%. Again, no `build_object_columnar` contribution at peak.

### col_dropped / 20000_streams -- PEAK = 30,907,249 bytes (103,466 blocks)

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 4,194,304 | 4 | 13.57% | `ColumnarLogBatch::from_records` (columnar_batch.rs:374:31) |
| 3,200,000 | 20,000 | 10.35% | `StreamSeed::build` (writer.rs:2329:21) |
| 2,916,352 | 1 | 9.44% | `StreamDir::encode` (stream_dir.rs:63:13) |
| 2,129,936 | 1 | 6.89% | `RlogWriter::build_object_columnar` (writer.rs:1151:30) |
| 1,735,647 | 1 | 5.62% | `writer::compress` (writer.rs:3693:22) |
| 1,348,890 | 20,000 | 4.36% | `build_object_columnar::{closure#33}` (writer.rs:1811:32) |
| 1,348,890 | 20,000 | 4.36% | `from_records::{closure#0}` (columnar_batch.rs:332:51) |
| 1,310,720 | 1 | 4.24% | `writer::emit_merged` (writer.rs:2638:18) |
| 1,271,776 | 3,332 | 4.11% | `build_object_columnar` (writer.rs:1047:30) |
| 1,015,808 | 2 | 3.29% | `VarBytes::push` (columnar_batch.rs:110:19) |
| 960,000 | 1 | 3.11% | `build_object_columnar` (writer.rs:1819:14) |
| 786,432 | 1 | 2.54% | `from_records` (columnar_batch.rs:359:32) |
| 688,144 | 1 | 2.23% | `build_object_columnar` / `ref_of`-equivalent (writer.rs:1063:53) |
| 524,288x2 | 1 ea | 3.40% | `from_records` (358:30, 317:36) |
| 499,383 | 1 | 1.62% | `writer::push_section` (writer.rs:3705:12) |
| 480,000x2 | | 3.10% | `from_records` (305:32), `build_object_columnar` (1270:53) |
| 360,000 | 20,000 | 1.16% | `AttrValue::clone` (logstream.rs:17:9) |
| REMAINDER | | ~9% | (~45 smaller `build_object_columnar`/`from_records` sites) |

Note: `common::make_record`/`build_corpus` are **absent** from this table
(corpus already dropped by peak time). This is the one (arm, shape) cell
where the columnar-dropped arm's peak shifts to late in the encode -- see
below.

### col_kept / 1_stream -- PEAK = 30,411,197 bytes (241,234 blocks)

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 4,480,000 | 20,000 | 14.73% | `common::make_record` (mod.rs:74:17) |
| 4,194,304 | 4 | 13.79% | `from_records` (columnar_batch.rs:374:31) |
| 3,200,000 | 1 | 10.52% | `common::build_corpus` (mod.rs:111:19) |
| 1,310,720 | 1 | 4.31% | `writer::emit_merged` (writer.rs:2638:18) |
| 1,310,720 | 5 | 4.31% | `build_object_columnar::{closure#28}` (writer.rs:1564:25) |
| 1,310,720 | 5 | 4.31% | `build_object_columnar::{closure#29}` (writer.rs:1576:38) |
| 1,280,000 | 20,000 | 4.21% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 1,015,808 | 2 | 3.34% | `VarBytes::push` (columnar_batch.rs:110:19) |
| 819,216 | 1 | 2.69% | `BloomBuilder::finish_sized` (ravel-codec/src/bloom.rs:232:53) |
| 680,000 | 20,000 | 2.24% | `common::make_record` (mod.rs:89:28) |
| 678,398 | 20,000 | 2.23% | `common::make_record` (mod.rs:68:20) |
| 557,072 | 1 | 1.83% | `BloomBuilder::insert` (bloom.rs:201:25) |
| 542,040 | 3 | 1.78% | `BlocksBuilder::intern` (writer.rs:3350:37) |
| 524,288 | 1 | 1.72% | `from_records` (columnar_batch.rs:317:36) |
| 480,000x2 | | 3.16% | `build_object_columnar` (1270:53), `from_records` (305:32) |
| 412,754 | 16,434 | 1.36% | `Stager::string_shape::{closure#2}` (block.rs:255:51) |
| 370,000 | 20,000 | 1.22% | `AttrValue::clone` (logstream.rs:17:9) |
| 320,000x5 | | 5.26% | `build_object_columnar` (1163,1166,1162,1165,1164) + `make_record`(76:32) |
| 315,195 | 16,447 | 1.04% | `BloomBuilder::insert` (bloom.rs:201:56) |
| REMAINDER | | 10.71% | (~60 smaller `from_records`/`build_object_columnar`/`Stager` sites) |

### col_kept / 1000_streams -- PEAK = 30,556,448 bytes (211,604 blocks)

Same dominant sites as `col_kept/1_stream` with `put_uvarint` scaled to
2,547,200 (8.34%) and `StreamSeed::build` now visible (160,000/0.52%,
1,000 allocs). Top 4: `make_record`(74:17) 4,480,000/14.66%,
`from_records`(374:31) 4,194,304/13.73%, `build_corpus` 3,200,000/10.47%,
`put_uvarint` 2,547,200/8.34%. Full ordering in
`.gate-logs/sites-col_kept-1000_streams.txt`.

### col_kept / 20000_streams -- PEAK = 43,386,609 bytes (283,467 blocks)

| bytes | allocs | share | site |
|---:|---:|---:|---|
| 4,480,000 | 20,000 | 10.33% | `common::make_record` (mod.rs:74:17) |
| 4,194,304 | 4 | 9.67% | `from_records` (columnar_batch.rs:374:31) |
| 3,200,000 | 20,000 | 7.38% | `StreamSeed::build` (writer.rs:2329:21) |
| 3,200,000 | 1 | 7.38% | `common::build_corpus` (mod.rs:111:19) |
| 2,916,352 | 1 | 6.72% | `StreamDir::encode` (stream_dir.rs:63:13) |
| 2,559,360 | 20,000 | 5.90% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 2,129,936 | 1 | 4.91% | `build_object_columnar` (writer.rs:1151:30) |
| 1,735,647 | 1 | 4.00% | `writer::compress` (writer.rs:3693:22) |
| 1,348,890x2 | 20,000 ea | 6.22% | `build_object_columnar::{closure#33}`, `from_records::{closure#0}` |
| 1,310,720 | 1 | 3.02% | `writer::emit_merged` (writer.rs:2638:18) |
| 1,271,776 | 3,332 | 2.93% | `build_object_columnar` (writer.rs:1047:30) |
| 1,015,808 | 2 | 2.34% | `VarBytes::push` (columnar_batch.rs:110:19) |
| 960,000 | 1 | 2.21% | `build_object_columnar` (writer.rs:1819:14) |
| 786,432 | 1 | 1.81% | `from_records` (columnar_batch.rs:359:32) |
| 688,144 | 1 | 1.59% | `build_object_columnar` (writer.rs:1063:53) |
| 680,000 | 20,000 | 1.57% | `common::make_record` (mod.rs:89:28) |
| 640,000 | 20,000 | 1.48% | `common::make_record` (mod.rs:68:20) |
| 524,288x2 | 1 ea | 2.42% | `from_records` (358:30, 317:36) |
| 499,383 | 1 | 1.15% | `writer::push_section` (writer.rs:3705:12) |
| 480,000x2 | | 2.22% | `build_object_columnar` (1270:53), `from_records` (305:32) |
| REMAINDER | | ~7.8% | (~50 smaller sites down to 100.00%) |

## Where the peak falls (deliverable 3)

One line per arm/shape, determinable from which groups contribute at t-gmax
(evidence: presence/absence of `build_object`/`build_object_columnar` sites
and of `common::make_record`/`build_corpus` sites in each run's table,
above):

- **row, all 3 shapes**: late in `build_object`, after the push loop and
  `resolve_row` have fully run (allocation counts at every row-resolution
  site equal the full 20,000-record count) and while `build_object`'s own
  section-assembly buffers (`writer.rs:617/640/606` etc.) are freshly
  allocated. Determinable: both groups are large and at full-count.
- **col_dropped, 1_stream and 1000_streams**: during/at the end of
  `ColumnarLogBatch::from_records`, before `push_columnar` runs --
  `build_object_columnar` contributes 0 bytes at peak in both tables.
  Determinable: the absence is exact (no `build_object_columnar` site
  appears at all).
- **col_dropped, 20000_streams**: later than the other two shapes -- inside
  `build_object_columnar`'s section assembly, after `corpus` has already
  been dropped. Determinable: `common::make_record`/`build_corpus` fall to
  1.16% combined (vs. >50% at the other two shapes), while
  `build_object_columnar` and `StreamSeed::build` (which scales with stream
  count, matching exactly 20,000 streams here) now dominate.
- **col_kept, all 3 shapes**: within or at the end of `build_object_columnar`
  -- same destination as the row arm's `build_object`, since `corpus` is
  deliberately kept alive through the whole call and so contributes at peak
  regardless of exactly where within `build_object_columnar` the peak falls.
  Not determinable more precisely than "within/at the end of
  `build_object_columnar`" from the stacks alone, since nothing in the
  `gb`/`gbk` data pins a sub-step boundary inside that function.

## Cross-checks

### (a) KNOWN QUANTITY -- `ref_of` (row path, `build_object`, writer.rs:428:52)

| shape | `ref_of` bytes | expected | match | share of row PEAK | pre-registered band | verdict |
|---|---:|---:|---|---:|---|---|
| 1_stream | 100 | 100 | exact | 0.00035% | <0.01% | within |
| 1000_streams | 43,024 | 43,024 | exact | 0.1420% | 0.1-0.5% | within |
| 20000_streams | 688,144 | 688,144 | exact | 1.6603% | 2-6% | **below** |

All three byte counts match the task's pre-stated expected values exactly,
on x86_64. `ref_of` is live at t-gmax in all three shapes (its `gb` equals
its full allocated size, i.e. it is not partially freed by the peak
instant). Its PEAK share at 20,000 streams (1.66%) lands below the
pre-registered 2-6% band; reported as measured, not adjusted.

### (b) INTERNAL CONSISTENCY -- sum of per-site t-gmax bytes vs. dhat's own reported total

| run | sum of per-site `gb` | dhat "At t-gmax" | match |
|---|---:|---:|---|
| row/1_stream | 28,644,048 | 28,644,048 | exact |
| row/1000_streams | 30,296,064 | 30,296,064 | exact |
| row/20000_streams | 41,444,889 | 41,444,889 | exact |
| col_dropped/1_stream | 19,824,211 | 19,824,211 | exact |
| col_dropped/1000_streams | 21,256,563 | 21,256,563 | exact |
| col_dropped/20000_streams | 30,907,249 | 30,907,249 | exact |
| col_kept/1_stream | 30,411,197 | 30,411,197 | exact |
| col_kept/1000_streams | 30,556,448 | 30,556,448 | exact |
| col_kept/20000_streams | 43,386,609 | 43,386,609 | exact |

All 9 runs match exactly; `unresolved (no ravel frame found at all) = 0
bytes` in all 9, so no site was silently dropped from the sum.

### (c) BYTE IDENTITY -- finished object, blake3 hash, per shape across arms

| shape | OBJECT_LEN | blake3 hash (all 3 arms) |
|---|---:|---|
| 1_stream | 122,896 | `c7b75470317e3dcd898559d99a7eb71095807cc1af7dac453a2693869a360806`[^1] |
| 1000_streams | 34,256 | `32056cbe5245ebf8b6f0da0ea781bf633f1d3e4b3a689a0e1c90dd718f025699`[^1] |
| 20000_streams | 521,855 | `cf873bae90354e93c1330d2577014c495f49cf860a9d8ec034d9d6e10b3e964d`[^1] |

[^1]: blake3 hex digest is 64 hex chars; verified via `awk '{print
length($2)}'` on the raw `OBJECT_HASH` log line for each run. Identical
across `row`/`col_dropped`/`col_kept` for every shape.

### (d) DETERMINISM -- row, 1000_streams, 4 independent runs (1 main + 3 extra)

All 4 runs: PEAK = 30,296,064 bytes in 282,169 blocks, OBJECT_HASH
`32056cbe...025699`. Bit-for-bit identical; no variance to report.

## Summary table

| arm | shape | PEAK (bytes) | input-records share | columnar-batch share | largest non-input group | ratio vs. row |
|---|---|---:|---:|---:|---|---:|
| row | 1_stream | 28,644,048 | 28.04% | n/a (0%) | sections/object 39.11% | -- |
| row | 1000_streams | 30,296,064 | 30.74% | n/a (0%) | sections/object 37.64% | -- |
| row | 20000_streams | 41,444,889 | 22.39% | n/a (0%) | sections/object 45.16% | -- |
| col_dropped | 1_stream | 19,824,211 | 58.52% | 41.48% | columnar batch 41.48% | 0.6922 (-30.78%) |
| col_dropped | 1000_streams | 21,256,563 | 60.61% | 39.39% | columnar batch 39.39% | 0.7017 (-29.83%) |
| col_dropped | 20000_streams | 30,907,249 | 1.16% | 33.14% | sections/object 54.94% | 0.7458 (-25.42%) |
| col_kept | 1_stream | 30,411,197 | 38.15% | 24.94% | sections/object 25.97% | 1.0617 (+6.17%) |
| col_kept | 1000_streams | 30,556,448 | 42.16% | 25.17% | sections/object 26.71% | 1.0086 (+0.86%) |
| col_kept | 20000_streams | 43,386,609 | 29.59% | 23.61% | sections/object 39.14% | 1.0469 (+4.69%) |

Pre-registered-band comparison (reported as measured, not tuned toward):

- Row PEAK vs. 14-24 MB (1 & 1,000 streams) / 16-30 MB (20,000 streams):
  all three runs land **above** their band (28.64 MB, 30.30 MB, 41.44 MB).
- Input-records share vs. 40-75% (1 & 1,000 streams) / 35-70% (20,000
  streams): row/1_stream (28.04%) and row/1000 (30.74%) land **below**
  their band; row/20000 (22.39%) also lands **below** its band.
- col_dropped PEAK vs. row, expected 0-30% lower (1 & 1,000 streams) /
  within +-10% (20,000 streams): 1_stream (-30.78%) lands **just below**
  the band floor, 1000_streams (-29.83%) lands **within** the band (barely),
  20000_streams (-25.42%) lands **below** the +-10% band.
- `ref_of` share of row PEAK vs. <0.01% / 0.1-0.5% / 2-6%: 1_stream and
  1000_streams land **within** band, 20000_streams (1.66%) lands **below**
  band.

## Coarse group tables (deliverable 2c)

Group definitions, applied uniformly across all 9 runs:

- **input records**: `common::make_record` (all lines), `common::build_corpus`,
  `ravel_types::logstream::put_uvarint` (logstream.rs:44, invoked from
  `log_stream_id` which `common::make_record` calls directly -- confirmed by
  grep, not from any writer-side resolution path) and `AttrValue::clone`
  (logstream.rs:17, also inside `make_record`).
- **columnar batch**: everything under `ColumnarLogBatch::from_records` /
  `VarBytes::push`/`new` / `Bitmap::push` and their closures.
- **row resolution**: `RlogWriter::push` (writer.rs:272), `writer::resolve_row`,
  `record::resolve_value`, and the `ref_of` map (`build_object`,
  writer.rs:428:52).
- **stream seeds**: `StreamSeed::build` (writer.rs:2329/2321) -- scales with
  stream count, not record count (exactly 3,200,000 bytes at 20,000 streams
  in every arm that reaches it).
- **block encoding**: `BlocksBuilder::*`, `block::Stager::*`,
  `block::write_block_columnar`, `rlog_codec::*`, `skip_index::*`,
  `page::SealedPage::append`, `page::compress_if_smaller`,
  `bloom::BloomBuilder::*`, `writer::chunk_blocks`,
  `writer::presence_word_prefix`, `varint::put_uvarint` (both the
  `ravel-codec` and `ravel-logseg` copies).
- **sections/object**: `RlogWriter::build_object` (row path, excluding the
  `ref_of` line, which is row resolution) and `build_object_columnar`
  (columnar path), `StreamDir::encode`, `writer::compress`,
  `writer::emit_merged`, `writer::push_section`, `writer::level0`,
  `writer::intern_tracked_names`, `StampIndex::build`, `StampScratch::*`,
  `writer::bloom_coverage`.
- **bench harness**: the bin's own overhead (arg parsing, `blake3::hash`
  of the finished object). Did not surface as a distinct allocation site in
  any of the 9 runs -- negligible relative to the reporting threshold.
- **other**: everything left over (in practice just `reader::decode_attr_set`,
  3 bytes, 1 alloc, present in every run; cause not investigated, immaterial
  to PEAK).

| run | input records | columnar batch | row resolution | stream seeds | block encoding | sections/object | other |
|---|---:|---:|---:|---:|---:|---:|---:|
| row/1_stream | 28.04% | -- | 32.85% | 0.00% | -- | 39.11% | 0.00% |
| row/1000_streams | 30.74% | -- | 31.08% | 0.54% | -- | 37.64% | 0.00% |
| row/20000_streams | 22.39% | -- | 24.44% | 7.91% | 0.09% | 45.16% | 0.00% |
| col_dropped/1_stream | 58.52% | 41.48% | -- | -- | -- | -- | -- |
| col_dropped/1000_streams | 60.61% | 39.39% | -- | -- | -- | -- | -- |
| col_dropped/20000_streams | 1.16% | 33.14% | 0.01% | 10.61% | 0.14% | 54.94% | 0.00% |
| col_kept/1_stream | 38.15% | 24.94% | 0.50% | 0.00% | 10.45% | 25.97% | 0.00% |
| col_kept/1000_streams | 42.16% | 25.17% | 0.50% | 0.54% | 4.93% | 26.71% | 0.00% |
| col_kept/20000_streams | 29.59% | 23.61% | 0.00% | 7.56% | 0.10% | 39.14% | 0.00% |

(Generated by `.gate-logs/analyze_groups.py`, not committed; every row's
shares sum to each run's full PEAK, which is the same figure cross-checked
against dhat's own t-gmax total in (b) above.)

## Method deviations

1. **Site-attribution fallback for `trim_path`-truncated paths** (described
   above, under "Site attribution"): extends stage 0e's regex-only method
   with a `find`-based crate resolution for the two source files that sit
   one directory level deeper than `<crate>/src/`. Necessary because this
   task's bin profiles from before `build_corpus`, which stage 0e's bin
   never did. Without it, 23.57% of row/1_stream's PEAK would have been
   misreported as unattributed.
2. **Coarse group definitions** are this report's own construction (the
   task names the 8 category labels but not a site-to-category mapping);
   the mapping is stated in full above so it can be checked against the
   source.

No other deviation from the task's method.
