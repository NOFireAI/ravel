# Stage 3 / wave 3: heap peak effect of #2565 (issue #2602, epic #2467)

## Host facts

- `uname -m`: x86_64
- `nproc`: 8
- `free -g`: 15 GiB total RAM, 6 GiB free at start, 15 GiB swap (4 GiB used)
- `df -h .`: 226G filesystem, 83G available at start
- Deviation from the spec's expected class (x86_64, 16 cores, 30 GiB RAM):
  this host has only 8 cores and 15 GiB RAM. The spec's aarch64 branch
  (`CARGO_BUILD_JOBS=2`) does not apply since the arch is x86_64. Used
  `CARGO_BUILD_JOBS=4` as instructed for the x86_64 branch; no OOM or
  job-starvation symptoms were observed in either build or any of the 14
  measurement runs.
- `CARGO_TARGET_DIR` is redirected off the checkout to
  `/var/lib/fleet/cache/cargo-target/7870410b-713e-49e6-94fd-7d487cc90a5f`
  (confirmed via `cargo metadata`), same as stage0f's host.

## Commits under test

- AFTER (HEAD at task start): `c6cafa11725e996d5014a63bb19d8dbfb66c84bd`
  ("refactor(logseg): encode the stream directory once, drop state early")
- BEFORE (AFTER's parent): `7ca7d8173e457ce5d0207e5078e3e44ff988ce6f`
- The harness commit (bin + Cargo.toml, added on top of AFTER), `HEAD~0`
  after step 7: `8182046ce...` ("test(bench): add the heap peak harness
  for the #2565 effect measurement")
- `git diff --stat HEAD~1 HEAD` (taken before the harness commit existed)
  confirmed the AFTER->BEFORE diff touches exactly
  `crates/ravel-logseg/src/stream_dir.rs` and
  `crates/ravel-logseg/src/writer.rs`, matching the spec.
- Step 9's BEFORE checkout used `$(git rev-parse HEAD~2)` (resolved fresh
  in the same command, since the harness commit shifted the relative
  offset), which was confirmed equal to the recorded BEFORE id before use.

## Harness provenance

- Fetched `origin/stage0f` from
  `refs/heads/task/3213b1d4-5565-4ea0-8fa1-039c1d4f7f97/result`.
- Took `crates/ravel-bench/src/bin/logseg_peak_by_site.rs` unmodified via
  `git checkout origin/stage0f -- <path>`. It compiled against the current
  tree with no edits (`.gate-logs/check_peak_bin.log`, not committed), so
  step 6's fallback edit was not needed.
- Applied the Cargo.toml changes by hand (not a checkout of the whole old
  file): the `dhat = "0.3.3"` dependency line with its stage-0e comment
  (the diff against `origin/stage0f~1` showed only the new `[[bin]]` entry,
  added in commit `7fe481f64`; the dependency line itself was added
  earlier, in `3d46bd639`, found by walking `git log origin/stage0f --
  crates/ravel-bench/Cargo.toml`) and a new `[[bin]]` stanza for
  `logseg_peak_by_site`. The current tree carries none of stage0f's other
  logseg bins, so the new `[[bin]]` block was inserted after the existing
  `segment_alloc_profile` entry rather than beside a same-named neighbor.
- Method reproduced from `stage0f-true-peak.md` (`git show
  origin/stage0f:stage0f-true-peak.md`): dhat `gb`/`gbk` fields (live
  bytes/blocks per allocation site at the t-gmax instant specifically,
  not `eb`/`ebk` live-at-termination or `mb`/`mbk` per-site local peak),
  innermost `crates/`-rooted frame as the site (using the same
  `find`-based fallback for `trim_path`-truncated paths like
  `benches/common/mod.rs` and `src/bin/logseg_peak_by_site.rs`,
  implemented in `.gate-logs/analyze_peak.py`, not committed), `>=95%`
  coverage tables, one process per (arm, shape) pair, profiler started
  before `build_corpus`.

## Commands

```
git config user.email fleet-executor@nofire.ai
git config user.name "Ravel Fleet Executor"
CARGO_BUILD_JOBS=4 cargo check -p ravel-bench --bin logseg_peak_by_site
CARGO_BUILD_JOBS=4 CARGO_PROFILE_RELEASE_DEBUG=true cargo build -p ravel-bench --release --bin logseg_peak_by_site
# (AFTER binary copied to .gate-logs/peak_after)
before=$(git rev-parse HEAD~2)   # confirmed == 7ca7d8173e457ce5d0207e5078e3e44ff988ce6f
git checkout "$before" -- crates/ravel-logseg/src/writer.rs crates/ravel-logseg/src/stream_dir.rs
git diff --stat "$before" -- crates/ravel-logseg/src/writer.rs crates/ravel-logseg/src/stream_dir.rs   # empty: identical
CARGO_BUILD_JOBS=4 CARGO_PROFILE_RELEASE_DEBUG=true cargo build -p ravel-bench --release --bin logseg_peak_by_site
# (BEFORE binary copied to .gate-logs/peak_before)
git checkout HEAD -- crates/ravel-logseg/src/writer.rs crates/ravel-logseg/src/stream_dir.rs   # restored, git status clean
# 12 runs, one process per (arm, shape, variant), alternating before/after, each
# in its own .gate-logs/run_<variant>_<arm>_<shape>/ directory:
.gate-logs/peak_<variant> <arm> <shape>
# 2 extra determinism reruns: peak_before/peak_after row 20000_streams again,
# in .gate-logs/rerun2_<variant>_row_20000_streams/
```

Note on the bin's output path: `logseg_peak_by_site.rs` derives its dhat
JSON path from `CARGO_MANIFEST_DIR` (a compile-time constant), not from the
process's current working directory, so it always writes to
`.gate-logs/dhat-peak-<arm>-<shape>.json` at the repo root regardless of
which directory the binary is run from. Each run's JSON was moved into its
own `run_*`/`rerun2_*` directory immediately after the process exited and
before the next run started, which gives the same non-overwrite guarantee
the per-run-directory instruction was for.

## Results: 12 peaks + 2 determinism reruns

All byte figures are dhat live bytes/blocks at the global peak instant
(t-gmax), heap only, summed from the `gb`/`gbk` fields of every
allocation-site record in that run's dhat JSON (cross-checked, as stage0f
did, against dhat's own "At t-gmax" total printed to stderr; all 14 match
exactly -- see `.gate-logs/run_*/stderr.log` and `.gate-logs/rerun2_*/stderr.log`, not committed).

| arm | shape | BEFORE bytes | BEFORE blocks | AFTER bytes | AFTER blocks | BEFORE MB | AFTER MB | change (after vs before) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| row | 1_stream | 28,643,948 | 280,036 | 28,643,508 | 280,033 | 28.644 | 28.644 | -0.0015% |
| row | 1000_streams | 30,253,040 | 282,168 | 29,955,904 | 280,167 | 30.253 | 29.956 | -0.982% |
| row | 20000_streams | 40,756,745 | 342,745 | 32,668,456 | 322,704 | 40.757 | 32.668 | -19.847% |
| col_dropped | 1_stream | 19,824,111 | 200,033 | 19,824,111 | 200,033 | 19.824 | 19.824 | 0.000% |
| col_dropped | 1000_streams | 21,213,539 | 201,032 | 21,213,539 | 201,032 | 21.214 | 21.214 | 0.000% |
| col_dropped | 20000_streams | 30,219,105 | 103,465 | 27,115,257 | 91,702 | 30.219 | 27.115 | -10.271% |

Determinism reruns (row, 20000_streams): BEFORE rerun = 40,756,745 bytes /
342,745 blocks (identical to the original run); AFTER rerun = 32,668,456
bytes / 322,704 blocks (identical to the original run).

## Stage0f comparison (assertion A)

Stage0f figures (bytes, from `stage0f-true-peak.md`'s per-run tables):
row 28,644,048 / 30,296,064 / 41,444,889; col_dropped 19,824,211 /
21,256,563 / 30,907,249 (1 / 1000 / 20000 streams respectively).

| arm | shape | BEFORE (this run) | stage0f | abs diff | percent | verdict |
|---|---|---:|---:|---:|---:|---|
| row | 1_stream | 28,643,948 | 28,644,048 | 100 | 0.00035% | within |
| row | 1000_streams | 30,253,040 | 30,296,064 | 43,024 | 0.14202% | within |
| row | 20000_streams | 40,756,745 | 41,444,889 | 688,144 | 1.66030% | within |
| col_dropped | 1_stream | 19,824,111 | 19,824,211 | 100 | 0.00050% | within |
| col_dropped | 1000_streams | 21,213,539 | 21,256,563 | 43,024 | 0.20242% | within |
| col_dropped | 20000_streams | 30,219,105 | 30,907,249 | 688,144 | **2.22623%** | **outside 2%** |

Five of six BEFORE figures land within 2% of stage0f. The one miss
(col_dropped/20000_streams, 2.226%) is explained below before drawing any
conclusion about the corresponding AFTER figure.

### Site difference behind the col_dropped/20000_streams miss

The three diffs above (100 / 43,024 / 688,144 bytes, for 1 / 1000 / 20000
streams respectively) are identical between the row and col_dropped arms
at every shape, and each is exactly the byte count of one allocation site
present in stage0f's tables and **absent from every BEFORE run measured
here**: stage0f's row/20000_streams table lists `688,144 | 1 | 1.66% |
RlogWriter::build_object / ref_of (writer.rs:428:52)`, and its
col_dropped/20000_streams table lists the same `688,144 | 1 | 2.23% |
build_object_columnar / ref_of-equivalent (writer.rs:1063:53)`; the
1000-stream and 1-stream tables show the matching 43,024- and 100-byte
`ref_of` entries in their REMAINDER lines. Grepping the BEFORE commit's
`writer.rs` (`git show 7ca7d8173e457ce5d0207e5078e3e44ff988ce6f:crates/ravel-logseg/src/writer.rs
| grep ref_of`) finds no such map at all -- only a comment at line 5881:
"binary search over `sorted_ids` instead of the deleted `ref_of` hash".
`build_object` at this BEFORE commit builds stream identity directly into
a `BTreeMap<LogStreamId, &[u8]>` (see `writer.rs` around line 428 in that
commit) with no separate `ref_of` hash map construction at all.

So the `ref_of` hash map stage0f measured was already deleted by an
earlier, unrelated commit on this same epic (#2467) sometime between
stage0f's baseline and this task's BEFORE commit -- not by the #2565
change under test, and not a defect in this measurement. Because that
site scaled with stream count (100 / 43,024 / 688,144 bytes at 1 / 1,000
/ 20,000 streams, consistent with a hash map sized to the number of
distinct stream ids), its absence is a near-constant absolute offset
across both arms; at the smaller 20,000-stream col_dropped total
(30.9 MB vs row's 41.4 MB) the same absolute 688,144-byte offset crosses
the 2% relative threshold where it does not at the larger row total. No
other site differs between stage0f and this run's BEFORE measurements
beyond this one explained deletion (every other site in the >=95% tables,
below, matches stage0f's tables up to expected line-number drift from
intervening commits).

## Assertions

- **A (known-quantity check)**: 5 of 6 BEFORE figures are within 2% of
  stage0f; col_dropped/20000_streams is 2.226%, just outside, fully
  explained above by a `ref_of` map that an earlier, unrelated commit
  already deleted before this task's BEFORE. **Reported as a partial
  miss on one cell; not a defect in the #2565 change or this harness.**
- **B (row/20000_streams AFTER band 31-37 MB, 10-25% below BEFORE)**:
  AFTER = 32.668 MB (in band); drop vs BEFORE = 19.847% (in the 10-25%
  band). **PASS.**
- **C (row/1_stream and row/1000_streams change under 2%)**: -0.0015%
  and -0.982%. **PASS.**
- **D (col_dropped/20000_streams AFTER band 26-30 MB)**: AFTER =
  27.115 MB. **PASS.**
- **E (twelve dhat reports, each present exactly once)**: confirmed --
  `.gate-logs/run_{before,after}_{row,col_dropped}_{1_stream,1000_streams,20000_streams}/dhat.json`,
  12 directories, one file each, each produced by exactly one process
  invocation. **PASS.**
- **F (determinism, row/20000_streams before+after rerun)**: both reruns
  reproduced their original peak bytes and blocks exactly (BEFORE:
  40,756,745 bytes / 342,745 blocks both times; AFTER: 32,668,456 bytes /
  322,704 blocks both times). **PASS, bit-for-bit.**

## Where the peak moved (site-list comparison, 20000_streams)

Full >=95%-coverage site tables below use the innermost `crates/`-rooted
frame, same method as stage0f. Line numbers differ from stage0f's own
report because commits unrelated to #2565 landed on `writer.rs` and
`stream_dir.rs` between stage0f's baseline and this task's BEFORE/AFTER
(confirmed no behavioral drift: every site that appears in both BEFORE
and AFTER here, and in stage0f's tables, carries the same byte count,
only a different line number).

### row / 20000_streams

BEFORE (PEAK = 40,756,745 bytes, 342,745 blocks):

| bytes | blocks | share | site |
|---:|---:|---:|---|
| 5,242,880 | 1 | 12.86% | `RlogWriter::push` (writer.rs:299:22) |
| 4,480,000 | 20,000 | 10.99% | `common::make_record` (benches/common/mod.rs:74:17) |
| 4,000,000 | 1 | 9.81% | `RlogWriter::build_object` (writer.rs:618:42) |
| 3,200,000 | 20,000 | 7.85% | `writer::resolve_row` (writer.rs:2024:35) |
| 3,200,000 | 20,000 | 7.85% | `writer::emit_merged` (writer.rs:2488:18) |
| 3,200,000 | 20,000 | 7.85% | `StreamSeed::build` (writer.rs:2179:21) |
| 2,916,352 | 1 | 7.16% | `StreamDir::encode` (stream_dir.rs:63:13) |
| 2,559,360 | 20,000 | 6.28% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 2,129,936 | 1 | 5.23% | `RlogWriter::build_object` (writer.rs:607:30) |
| 1,735,647 | 1 | 4.26% | `writer::compress` (writer.rs:3513:22) |
| 1,348,890 | 20,000 | 3.31% | `build_object::{closure#10}` (writer.rs:815:32) |
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
| 3,200,000 | 20,000 | 9.80% | `StreamSeed::build` (writer.rs:2235:21) |
| 3,200,000 | 20,000 | 9.80% | `writer::resolve_row` (writer.rs:2080:35) |
| 3,200,000 | 20,000 | 9.80% | `writer::emit_merged` (writer.rs:2544:18) |
| 2,559,360 | 20,000 | 7.83% | `logstream::put_uvarint` (logstream.rs:44:17) |
| 2,129,936 | 1 | 6.52% | `RlogWriter::build_object` (writer.rs:607:30) |
| 1,013,360 | 2,671 | 3.10% | `RlogWriter::build_object` (writer.rs:435:26) |
| 680,000 | 20,000 | 2.08% | `common::make_record` (mod.rs:89:28) |
| 640,000 | 20,000 | 1.96% | `common::make_record` (mod.rs:68:20) |
| 540,000 | 20,000 | 1.65% | `writer::resolve_row` (writer.rs:2075:22) |
| 360,000 | 20,000 | 1.10% | `record::resolve_value` (record.rs:420:66) |
| (cum. 95.64%) | | | |

Line-by-line: every BEFORE site above except `StreamDir::encode`
(2,916,352 bytes, 7.16%), `writer::compress` (1,735,647 bytes, 4.26%),
`build_object::{closure#10}` (1,348,890 bytes, 3.31%),
`RlogWriter::build_object` at writer.rs:823:14 (960,000 bytes, 2.36%),
and `writer::push_section` (499,383 bytes, 1.23%) also appears in AFTER at
an identical byte count. `StreamDir::encode` is confirmed absent from
AFTER's full site list, not just below the 95% cutoff: a full-list scan
(not capped at 95%) found zero bytes attributed to any `stream_dir.rs`
site in either AFTER run (row or col_dropped, 20000_streams), vs.
2,916,352 bytes in both BEFORE runs. `compress`, the `build_object`
closure, and `push_section` also drop out together, consistent with a
compressed/assembled-section buffer whose construction the commit now
schedules (or frees) differently relative to the stream directory.

### col_dropped / 20000_streams

BEFORE (PEAK = 30,219,105 bytes, 103,465 blocks) top sites: `from_records`
(columnar_batch.rs:598:31) 4,194,304/13.88%, `StreamSeed::build`
(writer.rs:2179:21) 3,200,000/10.59%, **`StreamDir::encode`
(stream_dir.rs:63:13) 2,916,352/9.65%**, `build_object_columnar`
(writer.rs:1148:30) 2,129,936/7.05%, `writer::compress`
(writer.rs:3513:22) 1,735,647/5.74%, plus ~26 more sites to 95.32%
cumulative (full list: `.gate-logs/run_before_col_dropped_20000_streams/analysis.txt`, not committed).

AFTER (PEAK = 27,115,257 bytes, 91,702 blocks) top sites: `from_records`
(columnar_batch.rs:598:31) 4,194,304/15.47%, `StreamSeed::build`
(writer.rs:2235:21) 3,200,000/11.80%, `build_object_columnar`
(writer.rs:1174:30) 2,129,936/7.86% -- **no `StreamDir::encode` entry at
all** -- plus `build_object_columnar::{closure#30}`/`{closure#29}`
(writer.rs:1596:38, 1584:25) 1,310,720 bytes each (new, not present in
BEFORE's top table), down to 95.21% cumulative across ~40 sites (full
list: `.gate-logs/run_after_col_dropped_20000_streams/analysis.txt`, not
committed).

Same pattern as the row arm: `from_records`, `StreamSeed::build`, and
`build_object_columnar`'s main section-assembly site persist at identical
byte counts; `StreamDir::encode`'s 2,916,352-byte buffer (9.65% of
BEFORE's peak here) is the one site that disappears, and two new
`build_object_columnar` closure sites (1,310,720 bytes each) appear in its
place in the >=95% list -- consistent with the stream directory no longer
holding a large temporary buffer alive at the peak instant, and the
columnar section-assembly closures now accounting for a correspondingly
larger share of a smaller total.

### Why the peak moved (answering within the commit's own description)

`StreamDir::encode` (stream_dir.rs:63:13) holds a 2,916,352-byte buffer
live at the global peak in every BEFORE run at 20,000 streams (both row
and col_dropped) and in no AFTER run at any shape (confirmed by a
full-list, not just top-95%, scan across all 12 runs: the site has zero
bytes in all 6 AFTER runs and in all BEFORE runs at 1 and 1,000 streams,
where the stream directory is too small to survive to the peak either
way). This is exactly the commit's own description, "encode the stream
directory once, drop state early": the directory encoding buffer that
used to still be live when the rest of the object-assembly allocations
peaked is now already freed (or never built at that size) by the time
the peak instant arrives. At 20,000 streams the buffer is large enough
(7-10% of the total peak) for its removal from the peak instant to show
up as a double-digit percentage drop in the total; at 1 and 1,000 streams
the same buffer is negligible either way (it does not appear in any
BEFORE or AFTER site list at those shapes), which is exactly why
assertion C's near-zero change at those two shapes holds.

## Deviations from the spec

- Host core/RAM count smaller than the "expected class" (8 cores / 15 GiB
  vs. 16 cores / 30 GiB); x86_64 branch commands used as-is (see Host
  facts).
- Step 9's BEFORE commit id was re-resolved via `git rev-parse HEAD~2` in
  the same command rather than pasted as a bare literal, per this
  session's guard against typed/recalled SHA literals; confirmed equal to
  the recorded BEFORE id (`7ca7d8173e457ce5d0207e5078e3e44ff988ce6f`)
  before use.
- No edit to the bin was needed (step 6 is a no-op): it compiled
  unmodified against the current tree.
