# Stage 0e: live bytes by allocation site (issue #2480, epic #2467)

Resolves stage 0d's (#2477) open contradiction: a `stats_alloc` bucket said
9.49 MB stayed live inside the block loop and `finish_checked` released only
1.39 MB net; a code reading of `string_shape`/`intern`/`flush_group` says the
row-group string interner should free almost all of that at the one flush
inside `finish_checked`. This measurement uses a heap profiler that
attributes live bytes to allocation call stacks instead of hand-placed
buckets, and finds the code reading right: the stats_alloc figures were
wrong, not the code.

## Host

- aarch64, 4 cores, 7.9 GiB RAM (the fleet Pi executor class).
- `CARGO_BUILD_JOBS=2` for both the release build and every run.
- Disk:
  ```
  Filesystem      Size  Used Avail Use% Mounted on
  /dev/nvme0n1p2  468G   60G  385G  14% /
  /dev/nvme0n1p2  468G   60G  385G  14% /var/lib/fleet
  tmpfs           1.0G   64K  1.0G   1% /var/lib/fleet/secrets
  ```

## Dependency added

`dhat = "0.3.3"`, `ravel-bench`-only, for this measurement only (stated in
`crates/ravel-bench/Cargo.toml`). Not in `workspace.dependencies`.

## Method and exact commands

One new bin, `crates/ravel-bench/src/bin/logseg_live_by_site.rs`, with
`dhat::Alloc` as `#[global_allocator]`. It takes a shape
(`1_stream`/`1000_streams`/`20000_streams`) and a stop label
(`after_resolve_rows`/`after_blocks`/`after_finish_checked`/`before_return`),
installs a `stage0::HOOK` that, when the given label fires, drops the
`dhat::Profiler` (flushing its JSON) and calls `std::process::exit(0)`. One
new `stage0::fire(stage0_mode, "after_finish_checked")` call was added to
`crates/ravel-logseg/src/writer.rs`'s `build_object_columnar`, immediately
after `finish_checked()` returns.

dhat only reports heap state once, at profiler-drop, so each (shape, label)
pair is its own process: 3 shapes x 4 labels = 12 runs.

Build (once, committed before any run per the harness's method):
```sh
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_BUILD_JOBS=2 \
  cargo build -p ravel-bench --release --bin logseg_live_by_site
```

Each run:
```sh
CARGO_PROFILE_RELEASE_DEBUG=true CARGO_BUILD_JOBS=2 \
  cargo run -p ravel-bench --release --bin logseg_live_by_site -- <shape> <label>
```
for `<shape>` in `1_stream`, `1000_streams`, `20000_streams` and `<label>` in
`after_resolve_rows`, `after_blocks`, `after_finish_checked`, `before_return`.
Raw JSON written to `.gate-logs/dhat-<shape>-<label>.json`; stdout/stderr
captured to `.gate-logs/run-<shape>-<label>.log`.

### dhat field used

`PpInfoJson.eb` (`curr_bytes`) -- confirmed from `dhat-0.3.3/src/lib.rs`'s own
comment: "at termination, i.e. 'end'". This is live bytes at profiler-drop,
per program point. `ebk` (`curr_blocks`) is the matching live allocation
count.

### Site attribution

Per program point, the innermost frame whose *file path* (not symbol name)
matches `\(ravel-[a-zA-Z0-9_-]+/` -- a symbol name can carry a generic
std-iterator chain's misleading reference to a ravel type even when the
code address is actually in std, so file path is the only reliable anchor.
Confirmed 100% coverage on at least one run (1_stream/after_blocks: summed
per-site `eb` == dhat's own reported "At t-end" total, 14,501,117 bytes both
ways).

Per label, each site's bytes are the raw `eb` at that label minus the same
site's `eb` at `after_resolve_rows` (the baseline every run shares, since the
profiler starts at the same point the stats_alloc bin's baseline sample
does: immediately before `corpus.to_vec()`). This isolates what each later
stage *added*.

## Per-shape, per-label site tables

Sites listed until cumulative share >= 95% of that label's total added live
bytes; the rest is one REMAINDER row.

### 1_stream (1 stream, 20000 records/stream)

dhat "At t-end" by label: after_resolve_rows 11,103,490; after_blocks
14,501,117; after_finish_checked 12,642,094; before_return 12,680,524 bytes.

**after_blocks** (added: 3,397,627 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `emit_merged` (writer.rs:2638:18, `out.stat.push`) | 1,310,720 | 1 | 38.58% |
| `BlocksBuilder::intern` (writer.rs:3350:37, interner key insert) | 1,082,688 | 3 | 31.87% |
| `Stager::string_shape::{closure#2}` (block.rs:255:51) | 505,727 | 20,025 | 14.88% |
| `BlocksBuilder::intern` (writer.rs:3361:14, per-block gids) | 245,760 | 9 | 7.23% |
| `SealedPage::append` (page.rs:57:13) | 134,856 | 3 | 3.97% |
| REMAINDER | 117,876 | - | 3.47% |

**after_finish_checked** (added: 1,538,604 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `emit_merged` (writer.rs:2638:18) | 1,310,720 | 1 | 85.19% |
| `BlocksBuilder::flush_group` (writer.rs:3560:28, chunk bytes) | 109,868 | 1 | 7.14% |
| `ravel_codec::varint::put_uvarint` (varint.rs:22:13) | 48,228 | 3 | 3.13% |
| REMAINDER | 69,788 | - | 4.54% |

Note what is absent: every `intern`/`string_shape` site from the
`after_blocks` table (54% of that label's added bytes) is gone from this
table -- below the noise floor the REMAINDER row would show.

**before_return** (added: 1,577,034 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `emit_merged` (writer.rs:2638:18) | 1,310,720 | 1 | 83.11% |
| `push_section` (writer.rs:3705:12, object buffer growth) | 147,794 | 1 | 9.37% |
| `ravel_codec::varint::put_uvarint` (varint.rs:22:13) | 48,228 | 3 | 3.06% |
| REMAINDER | 70,292 | - | 4.46% |

### 1000_streams (1000 streams, 20 records/stream)

dhat "At t-end" by label: after_resolve_rows 11,629,112; after_blocks
13,309,790; after_finish_checked 13,169,311; before_return 13,209,905 bytes.

**after_blocks** (added: 1,680,678 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `emit_merged` (writer.rs:2638:18) | 1,310,720 | 1 | 77.99% |
| `BlocksBuilder::intern` (writer.rs:3361:14) | 245,760 | 9 | 14.62% |
| `build_object_columnar` (writer.rs:1496:34, `blk_indexed_ends.push`) | 32,768 | 1 | 1.95% |
| `build_object_columnar` (writer.rs:1497:31, `blk_stat_ends.push`) | 32,768 | 1 | 1.95% |
| REMAINDER | 58,662 | - | 3.49% |

**after_finish_checked** (added: 1,540,199 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `emit_merged` (writer.rs:2638:18) | 1,310,720 | 1 | 85.10% |
| `build_object_columnar::{closure#33}` (writer.rs:1811:32, per-block closure) | 65,890 | 1,000 | 4.28% |
| `build_object_columnar` (writer.rs:1819:14) | 48,000 | 1 | 3.12% |
| `build_object_columnar` (writer.rs:1496:34) | 32,768 | 1 | 2.13% |
| `build_object_columnar` (writer.rs:1497:31) | 32,768 | 1 | 2.13% |
| REMAINDER | 50,053 | - | 3.25% |

Again, no `intern`/`string_shape` site survives into this table.

**before_return** (added: 1,580,793 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `emit_merged` (writer.rs:2638:18) | 1,310,720 | 1 | 82.92% |
| `build_object_columnar::{closure#33}` (writer.rs:1811:32) | 65,890 | 1,000 | 4.17% |
| `push_section` (writer.rs:3705:12) | 48,794 | 1 | 3.09% |
| `build_object_columnar` (writer.rs:1819:14) | 48,000 | 1 | 3.04% |
| `build_object_columnar` (writer.rs:1496:34) | 32,768 | 1 | 2.07% |
| REMAINDER | 74,621 | - | 4.72% |

### 20000_streams (20000 streams, 1 record/stream)

dhat "At t-end" by label: after_resolve_rows 21,441,808; after_blocks
23,700,457; after_finish_checked 25,755,403; before_return 26,720,341 bytes.

**after_blocks** (added: 2,258,649 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `emit_merged` (writer.rs:2638:18) | 1,310,720 | 1 | 58.03% |
| `build_object_columnar` (writer.rs:1744:27, `first_blk.entry().or_insert`) | 294,920 | 1 | 13.06% |
| `build_object_columnar` (writer.rs:1745:26, `last_blk.insert`) | 294,920 | 1 | 13.06% |
| `BlocksBuilder::intern` (writer.rs:3361:14) | 245,760 | 9 | 10.88% |
| REMAINDER | 112,329 | - | 4.97% |

**after_finish_checked** (added: 4,313,595 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `build_object_columnar::{closure#33}` (writer.rs:1811:32, `blob.to_vec()` per stream) | 1,348,890 | 20,000 | 31.27% |
| `emit_merged` (writer.rs:2638:18) | 1,310,720 | 1 | 30.39% |
| `build_object_columnar` (writer.rs:1819:14, `stream_entries` Vec) | 960,000 | 1 | 22.26% |
| `build_object_columnar` (writer.rs:1744:27, `first_blk`) | 294,920 | 1 | 6.84% |
| `build_object_columnar` (writer.rs:1745:26, `last_blk`) | 294,920 | 1 | 6.84% |
| REMAINDER | 104,145 | - | 2.41% |

No `intern`/`string_shape` site survives here either -- at 20000 streams the
interner holds far less in absolute terms than at 1 stream (fewer repeats
per column, dict decision drops low-duplication columns at the 2x
present/distinct test in `group_dicts`), so its absence is less dramatic
but the same release happens.

**before_return** (added: 5,278,533 bytes)

| site | bytes | allocs | share |
|---|---:|---:|---:|
| `build_object_columnar::{closure#33}` (writer.rs:1811:32) | 1,348,890 | 20,000 | 25.55% |
| `emit_merged` (writer.rs:2638:18) | 1,310,720 | 1 | 24.83% |
| `push_section` (writer.rs:3705:12) | 998,766 | 1 | 18.92% |
| `build_object_columnar` (writer.rs:1819:14) | 960,000 | 1 | 18.19% |
| `build_object_columnar` (writer.rs:1744:27) | 294,920 | 1 | 5.59% |
| `build_object_columnar` (writer.rs:1745:26) | 294,920 | 1 | 5.59% |
| REMAINDER | 70,317 | - | 1.33% |

## What the sites are

- `emit_merged` (writer.rs:2638, `out.stat.push`): per-block NumStat/indexed
  column-value accumulation (`StampOut::stat`), built between
  `after_resolve_rows` and `after_blocks` (its call sites sit inside the
  per-block loop, writer.rs:2551-2568, called from `StampScratch::finish`).
  One Vec, grown to its final size by `after_blocks`, retained unchanged
  through `after_finish_checked` and `before_return`: the values it holds
  feed a later section (FIELD_DIR/column stats) that is not serialized
  until after `finish_checked`, so the Vec has to outlive the block loop.
  This is the single largest surviving site at every label past
  `after_blocks`, in all three shapes.
- `BlocksBuilder::intern` (writer.rs:3350/3361) and `Stager::string_shape`
  (block.rs:255): the row-group string interner's owned keys and the
  per-block owned string copies `string_shape` makes. Present and
  substantial at `after_blocks`, absent from every `after_finish_checked`
  and `before_return` table.
- `BlocksBuilder::flush_group` (writer.rs:3560): the column-chunk bytes
  `flush_group` copies into the BLOCKS section buffer (`self.bytes`) --
  legitimate output, not leaked interner state.
- `build_object_columnar` writer.rs:1744/1745 (`first_blk`/`last_blk`): the
  per-stream first/last-block-index maps, sized by stream count. Small at 1
  and 1000 streams (not in their top-95% tables at all), substantial at
  20000.
- `build_object_columnar::{closure#33}` writer.rs:1811 (`blob.to_vec()`) and
  writer.rs:1819 (the `stream_entries: Vec<StreamEntry>` `.collect()`): the
  STREAM_DIR section's working set -- one blob copy per stream plus the
  Vec holding all of them, built after `finish_checked`, still live at
  `before_return` because STREAM_DIR is compressed and pushed from these
  structures, not in place.
- `push_section` (writer.rs:3705, `object.extend_from_slice`): growth of
  the final assembled object buffer itself -- this *is* "the object",
  explicitly excluded by the report's own question (c).

## Answers

**(a) Which site owns most of the block loop's added bytes at
`after_blocks`?** Depends on shape. At 1 stream, `emit_merged`'s stat Vec
(38.6%) and the interner/`string_shape` family combined (1,082,688 +
505,727 + 245,760 = 1,834,175 bytes, 54.0%) are comparable in size, with the
interner family slightly ahead as a group though no single interner site
beats `emit_merged` alone. At 1000 and 20000 streams, `emit_merged` alone is
the largest single site (78.0% and 58.0%), because the interner's own
footprint shrinks as the row-group dictionary decision drops more
low-duplication columns at wider stream fan-out.

**(b) Are those bytes still live after `finish_checked`, and if so which
site holds them?** No -- not the interner/`string_shape` family. It is
present at `after_blocks` in all three shapes and absent (not even showing
in the REMAINDER-eligible tail) from every `after_finish_checked` table.
Reading `BlocksBuilder::flush_group`/`group_dicts` (writer.rs:3395-3469)
confirms why: `group_dicts` opens with
`std::mem::take(&mut self.str_group)` (writer.rs:3405), which runs inside
`flush_group`, which runs inside `finish_checked` before it returns. Every
column's `GroupStrColumn` (the interner `HashMap` and its owned `Vec<u8>`
keys) is then either dropped immediately (the `continue 'columns` paths,
when the column doesn't qualify for a dictionary) or consumed into a local
`entries`/`sorted` that is itself dropped at the end of that column's loop
iteration, after `encode_dict_page` copies the needed bytes into the sealed
dictionary page. Either way, nothing keeps the interner's `HashMap`/`Vec<u8>`
allocations alive past `flush_group` returning, which happens before
`finish_checked` returns. **What is still live after `finish_checked`, at
every shape, is `emit_merged`'s stat/indexed accumulation** (the single
largest site in every `after_finish_checked` table, 85.1%/85.1%/30.4% of
that label's added bytes for 1/1000/20000 streams respectively) --
unrelated to the interner, and expected: those values feed a section that
serializes later.

**(c) What is live at `before_return` besides the input batch and the
object?** `emit_merged`'s stat/indexed Vec (still live, same bytes as at
`after_finish_checked` in every shape -- confirming it is retained
unchanged, not regrown) and, at 20000 streams only, the STREAM_DIR working
set: the per-stream `first_blk`/`last_blk` maps (294,920 bytes each) and
the stream-entry blob copies plus their containing Vec (1,348,890 +
960,000 bytes) -- together 2,898,730 bytes, 54.9% of that label's added
bytes. `push_section`'s growth (998,766 bytes at 20000 streams, 147,794 at
1 stream) is excluded here as "the object" per the question's own framing.

**(d) Do the two instruments agree?** No. Comparing dhat's added-live-bytes
at `after_blocks` against stage 0d's `stats_alloc`-based "block stage delta
(after_blocks - after_resolve_rows)" (same two labels, same hook,
`stage0d-block-stage-memory.md`):

| shape | dhat added (after_blocks) | stats_alloc block stage delta | ratio (stats_alloc / dhat) |
|---|---:|---:|---:|
| 1_stream | 3,397,627 | 13,946,248 | 4.10x |
| 1000_streams | 1,680,678 | 11,271,775 | 6.71x |
| 20000_streams | 2,258,649 | 9,714,890 | 4.30x |

All three disagree far outside 5%. Root cause found by reading
`stats_alloc-0.1.10/src/lib.rs`'s `realloc`: it updates `bytes_allocated` or
`bytes_deallocated` by the resize delta, *and* `bytes_reallocated` by the
same signed delta, on every `realloc` call:
```rust
unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    self.reallocations.fetch_add(1, Ordering::SeqCst);
    if new_size > layout.size() {
        let difference = new_size - layout.size();
        self.bytes_allocated.fetch_add(difference, Ordering::SeqCst);
    } else if new_size < layout.size() {
        let difference = layout.size() - new_size;
        self.bytes_deallocated.fetch_add(difference, Ordering::SeqCst);
    }
    self.bytes_reallocated
        .fetch_add(new_size.wrapping_sub(layout.size()) as isize, Ordering::SeqCst);
    self.inner.realloc(ptr, layout, new_size)
}
```
`logseg_block_stage_alloc.rs`'s `live_bytes` formula is
`bytes_allocated - bytes_deallocated + bytes_reallocated`, which therefore
counts every realloc's growth or shrinkage twice: once through
`bytes_allocated`/`bytes_deallocated`, once more through
`bytes_reallocated`. A workload built mostly by `Vec`/`HashMap` growth via
repeated `realloc` (exactly this block loop: `Vec<u8>` string copies,
growing `HashMap`s, growing `Vec<StreamEntry>`) inflates the stats_alloc
figure well beyond the true live-byte delta, consistent with the observed
4-7x ratios (not a flat 2x, since not every byte of growth comes from a
`realloc` as opposed to a fresh `alloc`). This is a genuine bug in
`logseg_block_stage_alloc.rs`'s measurement, not a dhat artifact: dhat's
`eb` figure is a direct, instrument-level live-byte count with no such
double-count path. `logseg_block_stage_alloc.rs` is outside this task's
edit scope, so it is reported here, not fixed.

## Pre-registered expectations

1. **At `after_blocks`, 1 stream: string_shape copies held by the interner
   are 55%-80% of added live bytes.** Measured: 54.0% (1,834,175 /
   3,397,627) combining all three interner/`string_shape` sites. Just
   outside the band on the low side; `emit_merged`'s stat Vec (38.6%),
   which is not part of the interner at all, is large enough on its own to
   pull the interner's share just under 55%.
2. **Just after `finish_checked`, same site: under 10% (code reading
   right) or over 50% (old measurement right); 10-50% is neither.**
   Measured: 0% -- no interner/`string_shape` site appears in the
   `after_finish_checked` table for 1 stream at all (not even in the
   REMAINDER tail, which is 4.54% of 1,538,604 bytes = ~69,788 bytes, far
   below any plausible interner remnant). **Under 10%: the code reading is
   right.** `flush_group`/`group_dicts`'s `mem::take` plus per-column
   consume-or-drop does release the interner inside `finish_checked`, and
   stage 0d's `SECTION_BLOCKS_SKIP_BUILD` figure (a net *release* of
   1,390,597 bytes for 1 stream, per `stage0d-block-stage-memory.md`) was
   directionally consistent with this release happening at all, just
   undercounting its true size for the same double-counting reason as (d).
3. **At `before_return`, 20000 streams: stream directory entries and their
   blob copies are 30%-60% of added live bytes.** Measured: 54.9%
   (2,898,730 / 5,278,533, summing `first_blk`+`last_blk`+blob
   copies+`stream_entries` Vec). **Inside the band.**

## Deviation from the method

Deliverable 4 (extending the `stats_alloc` hook with new labels bracketing
`write_block_columnar`, `BlocksBuilder::push` split into `intern` vs
`pending.push`, and `finish_checked` split into `flush_group` vs rest, for
the 1-stream shape) was not attempted. Two reasons: first, it would require
editing `logseg_block_stage_alloc.rs` and/or the `stage0` bucket atomics
beyond the one hook call this task's scope restriction allows ("exactly one
extra call of the existing label hook" in writer.rs, nothing else outside
`ravel-bench`'s new bin). Second, and more directly, the question deliverable
4 exists to answer (how many live bytes the interner holds just before
`flush_group`, and how much `flush_group` releases) is already answered more
precisely by the dhat site tables above: deliverable 5's cross-check found
that the instrument deliverable 4 would have extended (`stats_alloc`'s
`live_bytes` formula) has a double-counting bug that makes its byte figures
unreliable for exactly this kind of before/after bracket. Extending a
measurement method already shown to inflate deltas 4-7x would not have
produced a trustworthy number; the dhat tables already give the two concrete
figures (interner-family bytes at `after_blocks`: 1,834,175 for 1 stream; at
`after_finish_checked`: 0) with higher confidence than a further
`stats_alloc` bracket could.

## Raw data

Raw dhat JSON (12 files, 3.9 MB total) kept under `.gate-logs/`, not
committed: the task's 200 KB threshold for committing raw dhat JSON is
exceeded. `.gate-logs/` is already gitignored (per CLAUDE.md's fleet
executor environment notes) so nothing further is needed to keep it out of
the commit.
