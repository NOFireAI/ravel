# Loader memory at large `--batch-rows` (issue #2613, epic #2614 stage 0)

Where `ravel-cli load` puts its memory on the ClickBench corpus, measured by
allocation site, and the design that removes most of it. Every figure below
names the command that produced it. Scratch files live under `.dd-tools/`
(ignored) in the checkout that ran them; the numbers are copied here.

## Environment

- Fleet executor, amd64 class: `uname -m` x86_64, `nproc` 16, `free -g`
  30 GB total, no swap, 210 GB free on the root volume.
- Binary: this checkout's `ravel-cli`, built with
  `CARGO_PROFILE_RELEASE_DEBUG=true cargo build --release -p ravel-cli
  --features profiling` so jemalloc heap profiling is compiled in (the
  `profiling` feature now also turns on `tikv-jemallocator/profiling`; a
  default build is unchanged) and frames resolve in `jeprof`.
- Corpus: the real ClickBench `hits.parquet`
  (`https://datasets.clickhouse.com/hits_compatible/hits.parquet`,
  14,779,976,446 bytes) and the ClickBench entry's `hits.mapping.toml`
  (`raw.githubusercontent.com/pmoust/ClickBench/ravel-v0.22.0/ravel/hits.mapping.toml`).
  Nothing here is synthetic.
- Store: `--store memory`. The memory store holds every written object, about
  147 bytes per loaded row; its site (`MemoryStore::put`) is below the
  top-15 sites in every dump and is excluded from the per-site table.

### Corpus layout (scratch tool `.dd-tools/pqmeta`, `parquet` crate footer read)

```
rows 99997497 row_groups 226 cols 105
rg rows min 151669 max 819200
rg0 rows 450560 compressed 52075258 uncompressed 188921521 unc/row 419.3 comp/row 115.6
types {"BYTE_ARRAY/String": 28, "INT32": 19, "INT32/Int16": 49, "INT64": 9}
rg0 byte_array uncompressed/row 375.1   (Title 217, Referer 79, URL 73 bytes/row)
codec SNAPPY
```

A `ParquetRecordBatchReader` with `with_batch_size(300000)` over row groups
0..3 (450,560 + 450,560 + 505,678 rows) returned batches of
`[300000, 300000, 300000, 300000, 206798]` rows: Arrow batches cross
row-group boundaries, so a cursor's batch boundaries fall every
`batch_rows` rows from its partition start, not at row-group edges.

## Measurement method

```
ravel-cli --store memory --tenant-hash-unkeyed load --parquet hits.parquet \
  --tenant bench --mapping hits.mapping.toml --shards 4 --target-bytes 1 \
  --batch-rows B --read-cursors 16 --pipeline-depth 4
```

run under `timeout -s INT 300..360`, RSS (`VmRSS` from `/proc/<pid>/status`)
sampled every 2 s, and for the profiled runs
`_RJEM_MALLOC_CONF=prof:true,lg_prof_sample:20,lg_prof_interval:33` (one heap
dump per 8 GB allocated, 184 to 206 dumps per run, no stall). Dumps were
attributed with `jeprof --collapsed --inuse_space` and summed by the first
frame below the allocator (`.dd-tools/runs/*/collapsed-*.txt`). RSS
plateaus within the first two or three batches; the peak scales with B, not
with how many rows were loaded.

## Results

### Peak RSS

| B | cursors | depth | run length | peak RSS | source |
|---|---|---|---|---|---|
| 500,000 | 16 | 4 | 362 s | 22.68 GB (22,681,924 kB) | `.dd-tools/runs/b500k-k16-d4-prof/rss.log` |
| 150,000 | 16 | 4 | 301 s | 11.64 GB (11,637,604 kB) | `.dd-tools/runs/b150k-k16-d4/rss.log` |
| 150,000 | 16 | 4 | 302 s (profiled) | 12.21 GB (12,210,324 kB) | `.dd-tools/runs/b150k-k16-d4-prof/rss.log` |

The issue's figures on a 128 GB box were 20.3 GB and 8.7 GB for the same
settings; the memory store's held objects (about 2 GB after 300 s at
B=500,000) and the 30 GB box's lower purge pressure account for the gap.

A mid-run `pmap -x` of the B=150,000 run (`pmap-half.txt`) shows the RSS in
a handful of large anonymous jemalloc regions (3.0 GB, 1.5 GB, 1.3 GB, ...)
and no 64 MB glibc arenas, so the resident memory is jemalloc heap, not a C
library's allocations.

### Live heap by allocation site (jemalloc, steady state)

Sites below 90 MB omitted. "Scales with B" is read off the two columns.

| Site (first frame below the allocator) | B=500k dump 100 (21,188 MB live) | B=150k dump 137 (11,145 MB live) | per batch row, 500k | per batch row, 150k |
|---|---|---|---|---|
| Parquet decode held by the 16 cursors: `ByteArrayDecoderPlain::read` (flat string values), `coerce_i32` (Int32 to Int16 copy), `pack_values` (dictionary columns), `extend_from_dictionary` (dictionary fallback to flat), `PrimitiveArrayReader` Int32/Int64, `decode_page`, `set_dict` | 7,010 MB | 2,154 MB | 14.0 KB | 14.4 KB |
| `ravel_cli::load::columnar::build_columnar_batch` (the `ColumnarLogBatch` under construction and every finished one still alive: queued, in flight, held by the router) | 3,764 MB | 2,465 MB | 7.5 KB | 16.4 KB |
| `build_columnar_batch::{closure#16}`: the per-slot `vec![None; total_rows]` of `Option<AttrValue>` | 1,184 MB | 413 MB | 2.4 KB | 2.8 KB |
| `StrSrc::get` (one owned `String` per string cell) and `str_column_dict_from_cells` | 614 MB | 149 MB | 1.2 KB | 1.0 KB |
| `ravel_ingest::log_router::partition_columnar` (the per-shard copies) | 4,152 MB | 1,444 MB | 8.3 KB | 9.6 KB |
| `ravel_logseg::writer::RlogWriter::build_object_columnar` plus `emit_merged` (encoder working set) | 2,873 MB | 3,537 MB | 5.7 KB | 23.6 KB |

Per batch row the Parquet term is the same at both sizes: it is proportional
to `read_cursors x batch_rows`. The encoder term is not proportional to B
at all: it is bounded by how many flushes run at once (`--shards` x
`--max-inflight-flushes`, up to 16 here) times one object's working set,
and smaller objects flush more often, so it is larger at B=150,000. The
`ColumnarLogBatch` terms are proportional to B times how many copies are
alive, and more copies are alive at the smaller B because the write side
keeps up better.

The startup dump of the B=500,000 run (dump 1, 6,376 MB live, two seconds
in) is Parquet decode alone: `ByteArrayDecoderPlain::read` 3,381 MB,
`coerce_i32` 609 MB, Int32 reads 568 MB, `extend_from_dictionary` 478 MB,
Int64 reads 406 MB, `pack_values` 320 MB. That is 16 cursors each holding a
500,000-row Arrow batch at about 800 bytes per row, before a single row has
been built.

## What the memory is (code, end to end)

- **Read cursors** (`services/ravel-cli/src/load/input.rs`
  `open_stride_cursors`, `logs.rs` `cursor_take`): each of the K cursors is
  a `ParquetRecordBatchReader` built `with_batch_size(batch_rows)`. Each
  `reader.next()` therefore decodes a full B-row Arrow batch of all 105
  columns, and the cursor keeps the unconsumed remainder in `buffered`
  (zero-copy slices, which pin the whole batch). A round deals only
  `B / K` rows from each cursor, so a cursor's batch lives for about K
  rounds and K full B-row Arrow batches are alive at steady state: K x B
  x ~800 bytes, 14 KB per batch row at K=16. The Int16 columns cost an
  Int32 decode plus an Int16 copy (`coerce_i32`) at the moment they are
  read, and the string columns whose chunks are not entirely
  dictionary-encoded come back as flat Utf8 (`ByteArrayDecoderPlain`,
  `extend_from_dictionary`: Title alone is 217 bytes per row).
- **Columnar build** (`columnar.rs` `build_columnar_batch`): for each of the
  104 mapped attributes a slot gets `vec![None; total_rows]` of
  `Option<AttrValue>` (32 bytes per cell: 3.3 KB per row), filled per row,
  then compacted into `DynColumn { cells: Vec<AttrValue>, validity }`
  (another 32 bytes per present cell). Every string cell is an owned
  `String` (`StrSrc::get`), copied out of the Arrow buffer. The resulting
  `ColumnarLogBatch` costs about 4.5 KB per row for this corpus against
  419 bytes per row of raw data: the `AttrValue`-per-cell model is a 10x
  blow-up by itself.
- **Decode queue** (`logs.rs` `spawn_decode_pipeline`): a
  `tokio::sync::mpsc::channel(decode_queue_batches)` of `Prefetched::Batch`,
  default 2: up to 2 finished batches alive ahead of the writer loop.
- **Write window** (`logs.rs`, `pipeline_depth`, default 4): each spawned
  `router.write_columnar(batch)` keeps the parent batch alive until every
  shard's ack returns (`batch` is owned by `write_columnar`'s frame, which
  outlives `partition_columnar`, the shard channel send, and
  `await_strict_acks`). This is ADR-2467's "records kept" case for the
  columnar path.
- **Per-shard partition** (`crates/ravel-ingest/src/log_router.rs`
  `partition_columnar`): clones every cell (`col.cells[*slot].clone()`,
  `residual_attrs[row].clone()`) into four per-shard `ColumnarLogBatch`es
  while the parent is still alive: a second full 4.5 KB-per-row copy. It
  also drops `dyn_col_dicts`, so the loader's dictionary shape never
  reaches the encoder (the encoder re-interns; bytes are unchanged).
- **Shard actor** (`log_shard.rs` `handle_write_columnar`,
  `merge_columnar_unstamped`): pushes the per-shard batch onto
  `BufContent::Columnar(Vec<ColumnarLogBatch>)`; at `--target-bytes 1` the
  size trigger fires on the same write and `flush_tenant` moves the buffer
  into a spawned flush task (`max_inflight_flushes` permits per shard,
  acquired inside the task, so queued flushes also hold their batches).
- **Encoder** (`crates/ravel-logseg/src/writer.rs` `push_columnar`,
  `build_object_columnar`): the batches are moved into `self.batches` and
  stay alive for the whole encode (`let batches = &self.batches` in
  `build_object_columnar`), alongside the encoder's own column scratch
  (`StampScratch`, `emit_merged`) and the compressed output. Measured 2.9 to
  3.5 GB across the concurrent flushes, not proportional to B.

### Multiplicity

With K read cursors, Q decode-queue batches, D pipeline depth, F in-flight
flushes per shard and S shards, the B-row forms that can be alive at once
are:

```
Arrow (decoded Parquet):      K full B-row batches            (~800 B/row each here)
ColumnarLogBatch (4.5 KB/row): 1 building (+0.75 KB/row of Option<AttrValue> slots)
                               + Q queued
                               + D parents held by write_columnar
                               + D per-shard copies in shard buffers / flush tasks
Encoder scratch:               min(D, F) x S concurrent encodes of B/S rows
```

Upper bound at the defaults (K=16, Q=2, D=4, F=4, S=4):
16 x 0.8 + (1 + 2 + 4 + 4) x 4.5 + encoder = 12.8 + 49.5 + encoder KB per
row. Measured at B=500,000: 14.0 (Arrow) + 11.1 (build, slots, strings) +
8.3 (partition) + 5.7 (encoder) = 39 KB per batch row, so the write side is
not at its bound (about 4 of the 11 possible `ColumnarLogBatch` copies are
alive on average: the single-threaded decoder is the bottleneck and the
window rarely fills). That is also why the issue found `--pipeline-depth 4
-> 1` saving only 11 to 17%: the depth is mostly unused, and what remains is
the K x B Arrow term, the build-side copies and the concurrency-bound
encoder, none of which `--pipeline-depth` touches. Against the issue's
32 KB per row (fitted across B=150k..1M on a 128 GB box), this box measures
39 KB per row at B=500k including about 3 GB of jemalloc memory the profile
attributes but RSS counts only once purged; the attribution is consistent
with the issue's figure to within the memory store and purge slack.

Answers to the questions the task asked:

- Does each read cursor hold decoded Arrow for a whole row group? No: for a
  whole `batch_rows`-row Arrow batch (450,560-row groups vs 500,000-row
  batches here), of every column, and K of them at once.
- Are strings copied into owned `String`s? Yes, once per string cell in
  `build_columnar_batch` (`StrSrc::get`) and again per cell in
  `partition_columnar`'s clone.
- Is the batch split into 4 per-shard copies? Yes, by cloning, with the
  parent kept alive until the acks return.
- Does the encoder hold an uncompressed copy of every column plus the
  compressed output? It holds the input batches (`self.batches`), its
  column scratch and the output at once; 5.7 to 23.6 KB per row of object
  depending on how many flushes overlap.
- Does anything keep the input batch alive after its encode finishes? The
  router's parent copy lives until the ack; the encoder's input is dropped
  when `build_object_columnar` returns.

## Design: ranked reductions

Bytes are per batch row at K=16, S=4, from the table above.

1. **Bound the cursors' Arrow read-ahead** (ravel-cli only): build each
   cursor's reader `with_batch_size(ceil(B / K))` instead of `B`, and have
   `cursor_take` gather a span from several small Arrow batches up to the
   same virtual B-row boundary the old B-row batches imposed, so every batch
   has exactly the rows it has today. Saves about 13 KB per row (K x 800 B
   down to 2 x 800 B); no format or output change; no load-time cost
   expected (smaller decodes, same row sequence). Implemented below, where
   the partition-end rule that exact equality also needs is spelled out.
2. **Drop the router's parent copy and move instead of clone in
   `partition_columnar`** (ravel-ingest): take the batch by value, build the
   per-shard batches by draining the parent's columns, so one copy exists at
   a time. Saves 4.5 KB per row per in-flight write (about 5 to 9 KB per row
   measured). Small, byte-identical change; its tests sit in ravel-ingest.
3. **Build dense columns directly** (ravel-cli): replace the per-slot
   `Vec<Option<AttrValue>>` with a dense `Vec<AttrValue>` plus a `Bitmap`
   filled in row order, keeping the first-occurrence rule via
   `slot_taken_at`. Saves the 2.4 to 2.8 KB per row of `Option<AttrValue>`
   slots and the compaction pass. Byte identity follows from the existing
   row-path differential test.
4. **Bound queues by bytes** (ravel-cli): charge each queued or in-flight
   `ColumnarLogBatch` its `est_columnar_bytes` against one budget and make
   the decoder wait on it. Does not shrink the steady state measured here
   (the window is rarely full) but turns the D and Q bound into a
   predictable byte ceiling, which is what makes a small machine safe.
5. **Decouple object size from batch size**: the encoder already merges
   several batches into one object (`BufContent::Columnar(Vec<_>)`,
   `build_object_columnar` over `bases`), so `--target-bytes` above one
   slice's footprint plus a raised `--max-flush-delay` already produce
   1,000,000-row objects from 100,000-row batches. What it costs is the
   accumulated per-shard batches in the `AttrValue` form (4.5 KB per row of
   object) plus the encoder's working set, so on its own it moves the memory
   from the decoder to the shard buffers rather than removing it. It becomes
   the real answer only together with 6.
6. **A typed columnar batch** (ravel-logseg API, not format): carry Str
   columns as offsets plus bytes (or dictionary ids plus a dictionary) and
   I64/F64/Bool as plain vectors instead of `Vec<AttrValue>`. This is the
   10x: 4.5 KB per row becomes about 0.5 KB per row for this corpus, every
   copy in the pipeline shrinks with it, and together with 5 it makes
   1,000,000-row objects loadable in a few GB. It is a rewrite of the
   writer's columnar input path (`writer.rs` is 6,652 lines) and belongs in
   its own task with its own byte-identity differential test.

Order: 1 (this task), then 2 and 3 (small, independent), then 6, then 5 as
the operator-facing recipe once 6 has landed.

### Correction: the encoder site includes the memory store

The RLOG object comes out of `build_object_columnar` as the `Vec<u8>` the
flush task hands to `store.put`, and `MemoryStore` keeps that allocation
without copying it, so under `--store memory` the finished objects stay
attributed to the writer frame. No `ravel_object_store` frame appears in any
dump. Reading the "encoder" row over time (1,598 MB at dump 60, 115 s in;
2,873 MB at dump 100, 192 s; 5,104 MB at dump 179 of the after-run, 345 s)
gives a growth of about 15 MB/s, which at the corpus's 98 bytes per loaded
row (9.84 GB of RLOG for 100M rows) is about 150,000 rows/s, a 500,000-row
batch every 3.3 s. The encoder's own working set is the remainder, a few
hundred MB (`emit_merged`/`StampScratch` is 120 to 180 MB of it), not the
2.9 to 3.5 GB the table's row reads as. The per-row figures in that row are
therefore mostly the store, and the store is the same before and after the
change below at equal dump index (equal bytes allocated), so the comparison
of the other rows is unaffected.

## Implemented here: bounded cursor read-ahead (reduction 1)

`services/ravel-cli/src/load/input.rs` `reader_batch_rows(batch_rows, k)`
= `ceil(batch_rows / k)` is now the Arrow batch size each stride cursor's
`ParquetRecordBatchReader` decodes at; `open_stride_cursors` takes it as a
parameter beside `batch_rows`, and every `CursorState` carries
`block_rows = batch_rows` and `partition_rows`, its partition's row count
from the footer. `logs.rs` `cursor_take_spans` deals a cursor's share as one
or more contiguous spans, never past the next multiple of `block_rows` from
the partition start or the partition's end, whichever comes first: that is
where the old `batch_rows`-row Arrow batch ended. A cursor that has already
reached its partition's end makes one reader call, as the old dealer did,
which finds the reader exhausted and retires the cursor; a cursor that
reaches its end mid-round stays live until that next round. The single-cursor
metrics and spans loaders pass `batch_rows` for both and are unchanged. No
flag: `batch_composition_matches_the_pre_2613_dealer` (below) pins every
batch's rows to the old dealer's, so there is nothing for an operator to
choose.

The first version of this change (b9f217f97) kept asking the smaller reader
until a share was filled, so on a partition's last short slice it found the
reader exhausted in the same round instead of the next. The cursor stopped
counting as live one round early and every other cursor's share grew from
then on: no row was lost or duplicated, but batch composition and the RLOG
objects changed whenever K > 1, and at K = 1 the trailing zero-row batch
disappeared. Review found it; the differential tests below fail against it.

### Before and after, same binary flags, same corpus, same box

```
ravel-cli --store memory --tenant-hash-unkeyed load --parquet hits.parquet \
  --tenant bench --mapping hits.mapping.toml --shards 4 --target-bytes 1 \
  --batch-rows 500000 --read-cursors 16 --pipeline-depth 4
```

360 s each, RSS sampled every 2 s, jemalloc profiling on in both runs. One
run per version: the figures are a single measurement, not a distribution.
The after-run used b9f217f97's dealer, which differs from the final dealer
only in batch composition near partition ends (above), not in what a cursor
decodes or holds.

| | before (`.dd-tools/runs/b500k-k16-d4-prof`) | after (`.dd-tools/runs/b500k-k16-d4-prof-after`) |
|---|---|---|
| peak RSS | 22.68 GB (22,681,924 kB) | 15.48 GB (15,475,300 kB) |
| saving | | 7.21 GB = 14.4 KB per batch row |
| Parquet decode live (before: dump 100; after: dump 179) | 7,010 MB | about 410 MB (`decode_page` 136 MB, `ByteArrayDecoderPlain` 277 MB; no other decode frame above 90 MB) |
| `build_columnar_batch` + slots + strings at dump 100 | 5,562 MB | 3,615 MB |
| `partition_columnar` at dump 100 | 4,152 MB | 2,238 MB |
| encoder + memory store at dump 100 | 2,873 MB | 2,882 MB |
| total live at dump 100 | 21,188 MB | 10,048 MB |
| total live at the last steady dump | (not re-symbolisable: the binary was rebuilt) | 15,512 MB at dump 179 |

Load rate was not measured directly (no rows-per-second figure was recorded
for either run); it is inferred from heap-dump timing. The heap
dumps fire once per 8 GB allocated and, assuming the work per batch
allocates the same bytes in both versions, dump index is a row counter.
Dump 50/100/150 landed at +95/+192/+291 s before and at +96/+195/+296 s
after, and the encoder-plus-store figure at dump 100 is close (2,873 vs
2,882 MB, roughly the same number of objects stored), so on that inference
the load rate differs by under 2% (about 150,000 rows/s on this box, from
the store growth above). The after-run's live heap swings between about 10
GB and 15.5 GB as the write side's window fills and drains; with the
decoder no longer holding 7 GB of Arrow, the `ColumnarLogBatch` copies
(build, queue, parents, per-shard) and the store are what remain, which is
why reductions 2 and 3 are the next step.

### Byte identity

`services/ravel-cli/src/load/logs/tests/pre_2613.rs` holds a test-only copy
of the 9086a313f dealer (`pre_2613_reference_collect_spans` over
`batch_rows`-row readers) and compares today's dealer against it.

- `batch_composition_matches_the_pre_2613_dealer`: every batch's rows, in
  order and zero-row batches included, must equal the reference's. Settings
  (`batch_rows`, K, `--skip-rows`): the uneven 700/300/500/900/200 fixture
  at (1000, 4, 0), (333, 5, 0), (1000, 3, 0), (450, 7, 0) and (2600, 2, 0);
  groups of 1,000, 10, 1,000 and 37 rows at (900, 4), where one partition is
  shorter than one share; a file with no row groups at (100, 4); skips
  landing mid-partition at (1000, 4, 850) and (333, 5, 1700); and K = 1 at
  (700, 1) and (1300, 1). K is the `--read-cursors` value, clamped to the
  row-group count as the loader clamps it.
- `batch_composition_matches_the_pre_2613_dealer_on_generated_files`: a
  proptest (128 cases) over 0 to 7 row groups of 1 to 400 rows, `batch_rows`
  1 to 1,200, K 1 to 7 and a skip anywhere in the file. Its regression seeds
  are checked in under `services/ravel-cli/proptest-regressions/`.
- `rlog_objects_match_the_pre_2613_dealer`: the per-batch RLOG encodings of
  the real decode pipeline (fixed writer identity) must equal those built
  from the reference's batches, at (1000, 4), (333, 5), (1000, 3) and
  (700, 1).

Against b9f217f97's dealer the first fails on (1000, 4) with batch sizes
`[950, 917, 733, 0]` against the reference's `[950, 750, 667, 233, 0]`, the
RLOG test fails on (1000, 4), and the proptest shrinks to one one-row group
at `batch_rows` 2, K = 1 (`[1]` against `[1, 0]`). A dealer that refills
like b9f217f97 but keeps an exhausted cursor live for one more round fails
too: (1000, 4) gives `[950, 750, 667, 233, 0, 0]`, and (333, 5) produces
different RLOG bytes.

`production_dealer_rlog_bytes_do_not_depend_on_reader_batch_size` (the old
`rlog_objects_are_byte_identical_across_reader_batch_sizes`, renamed) runs
today's dealer at two reader batch sizes and compares their RLOG bytes. Both
arms share the dealer, so it says nothing about the pre-#2613 output, and it
passed against b9f217f97.

### Regression test

`stride_cursors_hold_under_one_reader_batch_of_undealt_rows`
(`services/ravel-cli/src/load/logs/tests.rs`) records each cursor's
buffered, undealt rows after every round. Four 1,000-row groups, four
cursors, 1,000-row batches: every round must leave `[0, 0, 0, 0]`. Three
1,000-row groups, three cursors, 1,000-row batches (shares of 333 and 334
against 334-row reader batches): every cursor must hold under 334. With
`reader_batch_rows` returning `ceil(2 * batch_rows / K)` it fails both:

```
assertion `left == right` failed: round 1: shares of a whole reader batch leave nothing undealt
  left: [250, 250, 250, 250]
 right: [0, 0, 0, 0]
```

```
round 1: undealt rows per cursor [333, 334, 334], each must be under one 334-row reader batch
```

With the pre-#2613 reader size (`batch_rows`) the first case leaves 750 per
cursor after round one.

## Follow-ups, in order, with the expected saving each

Per batch row at `--read-cursors 16 --shards 4` on this corpus; the
multiplicity section above gives the terms.

1. **Move, do not clone, in `partition_columnar`, and drop the parent before
   the acks** (ravel-ingest `log_router.rs`): take the batch by value and
   drain its columns into the per-shard batches. Saves one 4.5 KB-per-row
   copy per in-flight write (2.2 to 4.2 GB measured at B=500,000). Byte
   identity follows from equal values; ravel-ingest's
   `partition_columnar_matches_from_records_per_shard` already pins the
   partition's content.
2. **Build dense `DynColumn`s directly** (ravel-cli `columnar.rs`): drop the
   per-slot `Vec<Option<AttrValue>>` intermediate. Saves 2.4 to 2.8 KB per
   row (320 MB to 1.2 GB measured). The existing row-path differential test
   is the byte-identity anchor.
3. **A byte budget on the decode queue and the write window** (ravel-cli
   `logs.rs`): charge each built batch its `est_columnar_bytes` and block
   the decoder on a `--max-inflight-bytes`-style budget instead of counting
   batches. Saves nothing at the measured steady state but caps the 10 to
   15.5 GB swing at a number the operator chose; this is what makes a 16 GB
   machine refuse rather than swap.
4. **A typed columnar batch in ravel-logseg** (API, not format): Str columns
   as offsets plus bytes or dictionary ids, I64/F64/Bool as plain vectors.
   Takes the `ColumnarLogBatch` from about 4.5 KB per row to about 0.5 KB
   per row for this corpus, so every remaining copy shrinks about 9x; with
   1 and 2 landed this is the step that brings 1,000,000-row batches under
   the 8 GB target. Largest change; its own task with a byte-identity
   differential test against the current writer over the ClickBench fixture.
5. **Operator recipe for large objects from small batches** (docs): with 4
   landed, document `--batch-rows 100000 --target-bytes <one object's
   footprint> --max-flush-delay <fill time>` as the way to 25 MB objects on
   a small machine, with the memory formula from this file; the mechanism
   already exists (`BufContent::Columnar(Vec<_>)` merges batches into one
   object) and needs no code.
