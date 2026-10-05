# Stage 3b / wave 3: heap peak effect of #2565 on the merged tree (issue #2608, epic #2467)

Repeats the stage3/wave3 measurement (issue #2602, task 7870410b) on the
merged tree, using the harness and report format from that task's result
branch (`origin/effect1`, i.e.
`task/7870410b-713e-49e6-94fd-7d487cc90a5f/result`).

## Host facts

- `uname -m`: x86_64
- `nproc`: 16
- `free -g`: 30 GiB total RAM, 0 GiB free / 25 GiB available at start (25
  GiB in buff/cache), 0 GiB swap
- `df -h .`: 484G filesystem, 193G available at start
- Matches the spec's expected x86_64 class (16 cores, 30 GiB RAM):
  `CARGO_BUILD_JOBS=4` used throughout, as instructed.

## Commits under test

- AFTER (checkout HEAD at task start, before the harness commit):
  `ce93df8159ee4a30a99928fc0039eb6e1cb32478`
  ("refactor(logseg): drop the remaining gathered row arrays early")
- BEFORE: `7ca7d8173e457ce5d0207e5078e3e44ff988ce6f`
- `git diff --stat 7ca7d8173e457ce5d0207e5078e3e44ff988ce6f HEAD --
  crates/ravel-logseg` confirmed the AFTER->BEFORE diff touches exactly
  two files: `crates/ravel-logseg/src/stream_dir.rs` and
  `crates/ravel-logseg/src/writer.rs` (313 insertions, 23 deletions).
- AFTER (`ce93df8`) is two commits ahead of BEFORE on this tree: the
  parent commit `c6cafa117` ("encode the stream directory once, drop
  state early", the same commit stage3/wave3 measured as its own AFTER)
  plus `ce93df8` itself, which additionally drops six gathered row arrays
  (severity, flags, trace id, span id, stream id, batch index) right
  after the block loop instead of at function end. This task's AFTER
  figure is therefore the combined effect of both commits, not `ce93df8`
  alone; see the site-list analysis below for how much of the combined
  effect the second commit itself contributes.
- Harness commit, cherry-picked clean (no conflicts) from
  `origin/effect1~1` onto AFTER: `6d3793b0d75855d915d2393da41f018d3bd42fbc`
  ("test(bench): add the heap peak harness for the #2565 effect
  measurement"). Committed directly on this dispatched checkout's
  (detached) HEAD, per the fleet-executor workspace-isolation exception.

## Harness provenance

- Cherry-picked unmodified from `task/7870410b-713e-49e6-94fd-7d487cc90a5f/result~1`
  (`origin/effect1~1`): adds `crates/ravel-bench/src/bin/logseg_peak_by_site.rs`
  plus the `dhat = "0.3.3"` dependency and `[[bin]]` stanza in
  `crates/ravel-bench/Cargo.toml`. No edits needed; compiled against the
  current tree unmodified.
- Method reproduced from `origin/effect1:stage3-w3-effect.md`: dhat
  `gb`/`gbk` fields (live bytes/blocks per allocation site at the t-gmax
  instant), innermost `crates/`-rooted frame as the site, `>=95%`
  coverage tables, one process per (arm, shape), profiler started before
  `build_corpus`.
- The bin writes its dhat JSON to a fixed path,
  `.gate-logs/dhat-peak-<arm>-<shape>.json` at the repo root (derived from
  `CARGO_MANIFEST_DIR`, a compile-time constant, not cwd), regardless of
  arm/shape. Each run's JSON was moved into its own run directory
  immediately after the process exited and before the next run started.
- Site attribution implemented fresh in `.gate-logs/analyze_peak.py` (not
  committed, `.gate-logs/` is gitignored): for each dhat program point
  with nonzero `gb`/`gbk`, scan its `fs` frame-stack list (confirmed by
  inspection to run innermost-to-outermost: the allocator frame first,
  the allocation call site deepest) and take the first frame whose
  function descriptor's crate prefix (after stripping any `<Type as
  Trait>`/`<Type>` wrapper) is `ravel_*` or `logseg_peak_by_site` (the
  bench bin's own crate name, which also covers the `#[path]`-included
  `benches/common/mod.rs` helper and the bin's own `src/bin/...` file,
  both of which would otherwise look path-ambiguous with libstd's own
  `src/...`-rooted frames). Cross-checked against dhat's own "At t-gmax"
  stderr line on every run (all 12 match exactly) and against the known
  BEFORE totals (assertion A, below): both checks match to the byte.

## Commands

```
git config user.email fleet-executor@nofire.ai
git config user.name "Ravel Fleet Executor"
git fetch origin refs/heads/task/7870410b-713e-49e6-94fd-7d487cc90a5f/result:refs/remotes/origin/effect1
git cherry-pick origin/effect1~1
CARGO_BUILD_JOBS=4 CARGO_PROFILE_RELEASE_DEBUG=true cargo build -p ravel-bench --release --bin logseg_peak_by_site
# (AFTER binary copied to .gate-logs/peak_after)
git checkout 7ca7d8173e457ce5d0207e5078e3e44ff988ce6f -- crates/ravel-logseg/src/writer.rs crates/ravel-logseg/src/stream_dir.rs
git diff --stat 7ca7d8173e457ce5d0207e5078e3e44ff988ce6f -- crates/ravel-logseg/src/writer.rs crates/ravel-logseg/src/stream_dir.rs   # empty: identical
CARGO_BUILD_JOBS=4 CARGO_PROFILE_RELEASE_DEBUG=true cargo build -p ravel-bench --release --bin logseg_peak_by_site
# (BEFORE binary copied to .gate-logs/peak_before)
git checkout HEAD -- crates/ravel-logseg/src/writer.rs crates/ravel-logseg/src/stream_dir.rs   # restored, git status clean
# 12 runs, one process per (arm, shape, variant), alternating before/after, each
# in its own .gate-logs/run_<variant>_<arm>_<shape>/ directory:
.gate-logs/peak_<variant> <arm> <shape> > .gate-logs/run_<variant>_<arm>_<shape>/stdout.log 2> .../stderr.log
mv .gate-logs/dhat-peak-<arm>-<shape>.json .gate-logs/run_<variant>_<arm>_<shape>/dhat.json
python3 .gate-logs/analyze_peak.py .gate-logs/run_<variant>_<arm>_<shape>/dhat.json > .../analysis.txt
```

## Results: 12 peaks

All byte figures are dhat live bytes/blocks at the global peak instant
(t-gmax), heap only, summed from the `gb`/`gbk` fields of every
allocation-site record in that run's dhat JSON. Cross-checked against
dhat's own "At t-gmax" total printed to stderr for all 12 runs: all
match exactly (`.gate-logs/run_*/stderr.log`, not committed).

| arm | shape | BEFORE bytes | BEFORE blocks | AFTER bytes | AFTER blocks | BEFORE MB | AFTER MB | change (after vs before) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| row | 1_stream | 28,643,948 | 280,036 | 28,643,108 | 280,029 | 28.644 | 28.643 | -0.0029% |
| row | 1000_streams | 30,253,040 | 282,168 | 29,955,504 | 280,163 | 30.253 | 29.956 | -0.9835% |
| row | 20000_streams | 40,756,745 | 342,745 | 32,668,456 | 322,704 | 40.757 | 32.668 | -19.8453% |
| col_dropped | 1_stream | 19,824,111 | 200,033 | 19,824,111 | 200,033 | 19.824 | 19.824 | 0.0000% |
| col_dropped | 1000_streams | 21,213,539 | 201,032 | 21,213,539 | 201,032 | 21.214 | 21.214 | 0.0000% |
| col_dropped | 20000_streams | 30,219,105 | 103,465 | 27,115,257 | 91,702 | 30.219 | 27.115 | -10.2711% |

(MB = bytes / 10^6.)

## Comparison against task 7870410b's figures (assertion A)

Task 7870410b's figures (`origin/effect1:stage3-w3-effect.md`): row
28,643,948 / 30,253,040 / 40,756,745; col_dropped 19,824,111 /
21,213,539 / 30,219,105 (1 / 1000 / 20000 streams respectively).

All six BEFORE figures measured here are **identical to the byte** to
task 7870410b's figures. No site differs; no explanation is needed (the
one explained miss in that task's own report, against an older stage0f
baseline, does not recur here because this task compares against
7870410b's own BEFORE directly, not against a different, older task).

## Assertions

- **A (BEFORE reproduces task 7870410b's figures exactly)**: all six
  BEFORE figures match to the byte (row 28,643,948 / 30,253,040 /
  40,756,745; col_dropped 19,824,111 / 21,213,539 / 30,219,105).
  **PASS.**
- **B (row AFTER within 0.5% of 28,643,508 / 29,955,904 / 32,668,456)**:
  measured 28,643,108 (diff 400, 0.0014%), 29,955,504 (diff 400,
  0.0013%), 32,668,456 (diff 0, exact). **PASS.**
- **C (col_dropped AFTER at or below 19,824,111 / 21,213,539 /
  27,115,257, never above; unchanged at 1/1000, 24.0M-27.115M at
  20000)**: measured 19,824,111 (unchanged), 21,213,539 (unchanged),
  27,115,257 (exactly at the upper bound, inside the 24.0M-27.115M
  band). **PASS.**
- **D (twelve dhat reports, each present exactly once)**: confirmed --
  `.gate-logs/run_{before,after}_{row,col_dropped}_{1_stream,1000_streams,20000_streams}/dhat.json`,
  12 directories, one file each, each produced by exactly one process
  invocation (verified with `find .gate-logs -mindepth 2 -name dhat.json
  | wc -l` = 12, and no stray `dhat-peak-*.json` left at `.gate-logs/`
  root between runs). **PASS.**
- **E (which col_dropped builder structures hold bytes at the AFTER
  peak)**: see below. **At 1_stream and 1000_streams: none of the
  tracked structures (gathered row arrays, permutation, col_dict_ids,
  global_dict, col_meta, col_rank) hold any bytes at the peak.** At
  **20000_streams, several of them do**: `g_attrs_raw` (480,000 B),
  `g_body`/`g_span`/`g_trace`/`g_sevtext`/`g_stream_id` (320,000 B
  each), `g_ts`/`g_obs`/`g_est_dyn` (160,000 B each), `g_flags`/
  `g_batch`/`g_stream_ref` (80,000 B each), `g_sev` (20,000 B),
  `col_dict_ids`/`global_dict` (120 B each) -- roughly 2.92 MB total,
  all attributed to their `Vec::with_capacity`/allocation call sites
  inside `RlogWriter::build_object_columnar` before the block loop.
  **`permutation` (`perm`) and `col_meta` hold zero bytes at this peak**
  (both absent from the full, uncapped site list for this run).
  Per the task's own reasoning: since several of these structures still
  hold bytes at the peak instant for col_dropped/20000_streams, **the
  peak instant there is before the drop block that follows the block
  loop, so `ce93df8`'s new early-drops cannot have moved it.** Combined
  with the fact that this task's AFTER figures match task 7870410b's
  AFTER figures (which measured only the parent commit, `c6cafa117`) to
  within 400 bytes at every shape and exactly at row/20000_streams, the
  entire measured improvement at col_dropped/20000_streams (and at every
  other shape) is attributable to `c6cafa117` (the stream-directory
  encode-once change); `ce93df8`'s additional row-array drops produce no
  further measurable reduction in any of the six (arm, shape) totals
  measured here, because the global peak in every run occurs earlier in
  `build_object`/`build_object_columnar` than the point those drops take
  effect.

## Site lists (>=95% coverage, innermost `crates/`-rooted frame)

### col_dropped / 1_stream

BEFORE (PEAK = 19,824,111 bytes, 200,033 blocks) and AFTER (PEAK =
19,824,111 bytes, 200,033 blocks) are byte-for-byte identical, including
every site in the table (no site added, removed, or changed count):

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 4,480,000 | 20,000 | 22.60% | `common::make_record` (benches/common/mod.rs:74:17) |
| 4,194,304 | 4 | 21.16% | `ColumnarLogBatch::from_records` (columnar_batch.rs:598:31) |
| 3,200,000 | 1 | 16.14% | `common::build_corpus` (benches/common/mod.rs:111:19) |
| 1,280,000 | 20,000 | 6.46% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 1,015,808 | 2 | 5.12% | `VarBytes::push` (columnar_batch.rs:111:19) |
| 680,000 | 20,000 | 3.43% | `common::make_record` (mod.rs:89:28) |
| 678,398 | 20,000 | 3.42% | `common::make_record` (mod.rs:68:20) |
| 640,000 | 1 | 3.23% | `ColumnarLogBatch::from_records::{closure#1}` (columnar_batch.rs:563:70) |
| 524,288 | 1 | 2.64% | `ColumnarLogBatch::from_records` (columnar_batch.rs:535:36) |
| 480,000 | 1 | 2.42% | `ColumnarLogBatch::from_records` (columnar_batch.rs:523:32) |
| 370,000 | 20,000 | 1.87% | `AttrValue::clone` (logstream.rs:17:9) |
| 320,000 | 20,000 | 1.61% | `common::make_record` (mod.rs:76:32) |
| 262,144 | 1-2 (4 sites) | 1.32% each | `ColumnarLogBatch::from_records` (526:25, 527:34, 542:35), `VarBytes::new` (104:22) |
| (cum. 95.40%) | | | |

The peak here is dominated by corpus construction and `ColumnarLogBatch`
building; no `writer.rs` allocation site appears at all in this table at
this shape.

### col_dropped / 1000_streams

BEFORE (PEAK = 21,213,539 bytes, 201,032 blocks) and AFTER (PEAK =
21,213,539 bytes, 201,032 blocks) are again byte-for-byte identical:

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 4,480,000 | 20,000 | 21.12% | `common::make_record` (benches/common/mod.rs:74:17) |
| 4,194,304 | 4 | 19.77% | `ColumnarLogBatch::from_records` (columnar_batch.rs:598:31) |
| 3,200,000 | 1 | 15.08% | `common::build_corpus` (benches/common/mod.rs:111:19) |
| 2,547,200 | 20,000 | 12.01% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 1,015,808 | 2 | 4.79% | `VarBytes::push` (columnar_batch.rs:111:19) |
| 694,000 | 20,000 | 3.27% | `common::make_record` (mod.rs:68:20) |
| 680,000 | 20,000 | 3.21% | `common::make_record` (mod.rs:89:28) |
| 640,000 | 1 | 3.02% | `ColumnarLogBatch::from_records::{closure#1}` (columnar_batch.rs:563:70) |
| 524,288 | 1 | 2.47% | `ColumnarLogBatch::from_records` (columnar_batch.rs:535:36) |
| 480,000 | 1 | 2.26% | `ColumnarLogBatch::from_records` (columnar_batch.rs:523:32) |
| 370,000 | 20,000 | 1.74% | `AttrValue::clone` (logstream.rs:17:9) |
| 320,000 | 20,000 | 1.51% | `common::make_record` (mod.rs:76:32) |
| 262,144 | 1-2 (4 sites) | 1.24% each | `VarBytes::new` (104:22), `ColumnarLogBatch::from_records` (542:35, 527:34, 526:25) |
| (cum. 95.19%) | | | |

Same pattern as 1_stream: no `writer.rs` site in the table; totals and
sites unaffected by either commit under test.

### col_dropped / 20000_streams

BEFORE (PEAK = 30,219,105 bytes, 103,465 blocks), top sites:

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 4,194,304 | 4 | 13.88% | `ColumnarLogBatch::from_records` (columnar_batch.rs:598:31) |
| 3,200,000 | 20,000 | 10.59% | `StreamSeed::build::<...>` (writer.rs:2179:21) |
| 2,916,352 | 1 | 9.65% | **`StreamDir::encode`** (stream_dir.rs:63:13) |
| 2,129,936 | 1 | 7.05% | `RlogWriter::build_object_columnar` (writer.rs:1148:30) |
| 1,735,647 | 1 | 5.74% | `writer::compress` (writer.rs:3513:22) |
| 1,348,890 | 20,000 | 4.46% | `ColumnarLogBatch::from_records::{closure#0}` (columnar_batch.rs:550:51) |
| 1,348,890 | 20,000 | 4.46% | `RlogWriter::build_object::{closure#10}` (writer.rs:815:32) |
| 1,310,720 | 1 | 4.34% | `writer::emit_merged` (writer.rs:2488:18) |
| 1,271,776 | 3,332 | 4.21% | `RlogWriter::build_object_columnar` (writer.rs:1049:30) |
| 1,015,808 | 2 | 3.36% | `VarBytes::push` (columnar_batch.rs:111:19) |
| 960,000 | 1 | 3.18% | `RlogWriter::build_object_columnar` (writer.rs:1749:14) |
| ... (20 more sites) ... | | | |
| (cum. 95.32%, 30 sites total) | | | |

AFTER (PEAK = 27,115,257 bytes, 91,702 blocks), top sites:

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 4,194,304 | 4 | 15.47% | `ColumnarLogBatch::from_records` (columnar_batch.rs:598:31) |
| 3,200,000 | 20,000 | 11.80% | `StreamSeed::build::<...>` (writer.rs:2261:21) |
| 2,129,936 | 1 | 7.86% | `RlogWriter::build_object_columnar` (writer.rs:1178:30) |
| 1,348,890 | 20,000 | 4.97% | `ColumnarLogBatch::from_records::{closure#0}` (columnar_batch.rs:550:51) |
| 1,310,720 | 1 | 4.83% | `writer::emit_merged` (writer.rs:2570:18) |
| 1,310,720 | 5 | 4.83% | `build_object_columnar::{closure#29}` (writer.rs:1590:25) |
| 1,310,720 | 5 | 4.83% | `build_object_columnar::{closure#30}` (writer.rs:1602:38) |
| 1,271,776 | 3,332 | 4.69% | `RlogWriter::build_object_columnar` (writer.rs:1079:30) |
| 1,015,808 | 2 | 3.75% | `VarBytes::push` (columnar_batch.rs:111:19) |
| 786,432 | 1 | 2.90% | `ColumnarLogBatch::from_records` (columnar_batch.rs:574:32) |
| ... | | | |
| 480,000 | 1 | 1.77% | `RlogWriter::build_object_columnar` (writer.rs:1318:53) -- `g_attrs_raw` |
| 320,000 | 1 | 1.18% (x4) | `build_object_columnar` writer.rs:1188:41/1189:38/1190:47/1191:46/1192:49 -- `g_sevtext`/`g_body`/`g_trace`/`g_span`/`g_stream_id` |
| 262,274 | 7 | 0.97% | `varint::put_uvarint` (varint.rs:17:17) |
| ... (37 sites total) ... | | | |
| (cum. 95.21%) | | | |

**`StreamDir::encode`'s 2,916,352-byte buffer (9.65% of BEFORE's peak
here) is absent from AFTER's table and from AFTER's full, uncapped site
list** -- the same finding task 7870410b made for the row arm. In its
place, two `build_object_columnar` closure sites (1,310,720 bytes each,
writer.rs:1590:25 and 1602:38) and several of the `g_*` gathered-row-array
sites (`g_attrs_raw`, `g_sevtext`, `g_body`, `g_trace`, `g_span`,
`g_stream_id`, `g_ts`, `g_obs`, `g_flags`, `g_batch`, `g_stream_ref`,
`g_sev`, `g_est_dyn`, `col_dict_ids`, `global_dict`, totalling ~2.92 MB)
appear in AFTER's table/full list, confirming (per assertion E) that
these structures are still live at the AFTER peak -- `ce93df8`'s drop of
them happens later in the function than this peak instant.

### row / 20000_streams

BEFORE (PEAK = 40,756,745 bytes, 342,745 blocks):

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 5,242,880 | 1 | 12.86% | `RlogWriter::push` (writer.rs:299:22) |
| 4,480,000 | 20,000 | 10.99% | `common::make_record` (benches/common/mod.rs:74:17) |
| 4,000,000 | 1 | 9.81% | `RlogWriter::build_object` (writer.rs:618:42) |
| 3,200,000 | 20,000 | 7.85% | `writer::emit_merged` (writer.rs:2488:18) |
| 3,200,000 | 20,000 | 7.85% | `StreamSeed::build::<...>` (writer.rs:2179:21) |
| 3,200,000 | 20,000 | 7.85% | `writer::resolve_row` (writer.rs:2024:35) |
| 2,916,352 | 1 | 7.16% | `StreamDir::encode` (stream_dir.rs:63:13) |
| 2,559,360 | 20,000 | 6.28% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 2,129,936 | 1 | 5.23% | `RlogWriter::build_object` (writer.rs:607:30) |
| 1,735,647 | 1 | 4.26% | `writer::compress` (writer.rs:3513:22) |
| 1,348,890 | 20,000 | 3.31% | `RlogWriter::build_object::{closure#10}` (writer.rs:815:32) |
| 1,013,360 | 2,671 | 2.49% | `RlogWriter::build_object` (writer.rs:435:26) |
| 960,000 | 1 | 2.36% | `RlogWriter::build_object` (writer.rs:823:14) |
| 680,000 | 20,000 | 1.67% | `common::make_record` (mod.rs:89:28) |
| 640,000 | 20,000 | 1.57% | `common::make_record` (mod.rs:68:20) |
| 540,000 | 20,000 | 1.32% | `writer::resolve_row` (writer.rs:2019:22) |
| 499,383 | 1 | 1.23% | `writer::push_section` (writer.rs:3525:12) |
| 360,000 | 20,000 | 0.88% | `record::resolve_value` (record.rs:420:66) |
| 320,000 | 1 | 0.79% | `RlogWriter::build_object` (writer.rs:449:68) |
| (cum. 95.75%) | | | |

AFTER (PEAK = 32,668,456 bytes, 322,704 blocks):

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 5,242,880 | 1 | 16.05% | `RlogWriter::push` (writer.rs:299:22) |
| 4,480,000 | 20,000 | 13.71% | `common::make_record` (mod.rs:74:17) |
| 4,000,000 | 1 | 12.24% | `RlogWriter::build_object` (writer.rs:618:42) |
| 3,200,000 | 20,000 | 9.80% | `writer::emit_merged` (writer.rs:2570:18) |
| 3,200,000 | 20,000 | 9.80% | `StreamSeed::build::<...>` (writer.rs:2261:21) |
| 3,200,000 | 20,000 | 9.80% | `writer::resolve_row` (writer.rs:2106:35) |
| 2,559,360 | 20,000 | 7.83% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 2,129,936 | 1 | 6.52% | `RlogWriter::build_object` (writer.rs:607:30) |
| 1,013,360 | 2,671 | 3.10% | `RlogWriter::build_object` (writer.rs:435:26) |
| 680,000 | 20,000 | 2.08% | `common::make_record` (mod.rs:89:28) |
| 640,000 | 20,000 | 1.96% | `common::make_record` (mod.rs:68:20) |
| 540,000 | 20,000 | 1.65% | `writer::resolve_row` (writer.rs:2101:22) |
| 360,000 | 20,000 | 1.10% | `record::resolve_value` (record.rs:420:66) |
| (cum. 95.64%) | | | |

Same finding as task 7870410b: every BEFORE site above except
`StreamDir::encode` (2,916,352 B), `writer::compress` (1,735,647 B), the
`build_object::{closure#10}` (1,348,890 B), `RlogWriter::build_object` at
writer.rs:823:14 (960,000 B), and `writer::push_section` (499,383 B) also
appears in AFTER at an identical byte count; those five drop out
together and are confirmed absent from AFTER's full (not just top-95%)
site list. The row arm has no `g_*`-named gathered-row-array structures
(those are columnar-only), so `ce93df8`'s row-array drops have no site
to remove here; the entire row/20000_streams improvement is the same
`StreamDir::encode`-removal effect task 7870410b already measured.

## Deviations from the spec

None. Host matched the expected x86_64/16-core/30GiB class; the
cherry-pick applied clean with no Cargo.lock conflict; the harness
compiled unmodified; all 12 runs completed on the first attempt with no
reruns needed.
