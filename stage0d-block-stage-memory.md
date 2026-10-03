# Stage 0d: block stage memory attribution (issue #2477)

Host: `Linux ip-172-31-18-6 7.0.0-1011-aws #11~24.04.1-Ubuntu SMP PREEMPT Mon Aug 10 15:20:57 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux`

Uptime before: `20:10:09 up 29 days, 23:49,  4 users,  load average: 0.69, 0.72, 0.83`

Uptime after: `20:10:09 up 29 days, 23:49,  4 users,  load average: 0.69, 0.72, 0.83`

Command: `cargo run --release -p ravel-bench --bin logseg_block_stage_alloc`

Method: for each shape, the columnar-with-records-dropped arm (`ColumnarLogBatch::from_records`, drop the source records, `push_columnar`, `finish_with_stats`) runs twice. Sampling OFF uses the existing `stage0::HOOK` labels for clean stage deltas (block stage = `after_blocks` - `after_resolve_rows`; trailing sections = `after_sections` - `after_blocks`). Sampling ON enables `stage0::BLOCK_SAMPLE`, which brackets every structure listed in the structure list below with `stats_alloc` samples taken directly inside `build_object_columnar`/`BlocksBuilder`, accumulated into named atomics the library exposes read-only (no dependency on `stats_alloc` in the library itself: the bucket functions take `(i64, u64)` pairs the bin computes from its own sampler). The encoded object bytes are asserted identical between the two runs, so sampling has no observable effect on the written object. `stats_alloc`'s counters are running totals; every bucket here is a net delta between two samples, not a true peak -- see `BLOCK_TRANSIENT_HIGH_WATER`'s own row for the one figure that tries to see inside an iteration instead of only at its boundaries.

Warning (epic #2467): the orchestrator's last three pre-registrations on this code missed, so expect the named buckets below to be partly wrong. The remainder row and the structure list are what make this result usable when they are.

## Structure list (deliverable 1)

### Block stage (created before/inside the block loop, alive at `after_blocks`)

| structure | type | filled at | last read | could drop earlier? |
|---|---|---|---|---|
| `blocks.pending` | `Vec<BlockWriteOut>` (field of `BlocksBuilder`) | `BlocksBuilder::push`, line 3294 (`self.pending.push(out)`) | `BlocksBuilder::flush_group`, drained when the group fills or at `finish_checked` (line 1831) | no: the final partial group is only known complete once the loop ends |
| `blocks.str_group`/`str_bytes` | `BTreeMap<u32, Option<GroupStrColumn>>` + `usize` (fields of `BlocksBuilder`) | `BlocksBuilder::intern`, called from `push` | consumed by `mem::take` inside `flush_group` | no: the dictionary decision needs the whole row group |
| `bloom_entries` | `Vec<Vec<u8>>` | line 1717 (`bloom_entries.push(builder.finish_exact())`), once per block | line 1908 (`encode_rlog_bloom_section(&bloom_covered, &bloom_entries)`) | no: needed whole-object for the BLOOM section |
| `bloom_covered` | set from `bloom_coverage()` (external, `ravel_codec`) | once before the block loop (line 1133) | line 1908, same BLOOM encode call | no |
| `postings_terms` | `BTreeMap<u32, BTreeMap<Vec<u8>, BTreeSet<u32>>>` | per-block postings loop (inside the block loop, before `after_blocks`) | line 1847 (`postings_terms.remove(&cid)`), drained building `postings_fields` | no: needed until POSTINGS assembly |
| `postings_capped` | `BTreeSet<u32>` | per-block postings loop, when a field exceeds `postings_max_distinct` | line 1844 (`postings_capped.contains(&cid)`) | no |
| `col_present` | `HashMap<u32, u64>` | per-block, after the block is placed | line 1814 (`col_present.get(cid)`), FIELD_DIR build | no |
| `col_blocks` | `HashMap<u32, u32>` | per-block, after the block is placed | line 1820 (`col_blocks.get(cid)`), FIELD_DIR build | no |
| `first_blk` / `last_blk` | `HashMap<u32, u32>` (two maps) | per-block, after the block is placed | line 1796 (`first_blk.get(&r)`/`last_blk.get(&r)`), STREAM_DIR build | no |

### Trailing sections (built after `after_blocks`, alive through `after_sections`/`before_return`)

| structure | type | filled at | last read | could drop earlier? |
|---|---|---|---|---|
| `stream_entries` -> `stream_dir` | `Vec<StreamEntry>` -> `StreamDir` | lines 1776-1796 | line 1867 (`stream_dir.encode()`) | yes: nothing reads `stream_dir` after its own `push_section` call; an explicit `drop(stream_dir)` right after line 1867 would free it one push_section call earlier than end-of-function |
| `field_entries` -> `field_dir` | `Vec<FieldEntry>` -> `FieldDir` | lines 1799-1813 | line 1877 (`field_dir.encode()`) | yes, same reasoning as `stream_dir` |
| `blocks_bytes` | `Vec<u8>` (BLOCKS section bytes) | `blocks.finish_checked()`, line 1831 | line 1882 (`Stored::raw(blocks_bytes)`), moved by value | already optimal: consumed by value at first use |
| `l0` | `Vec<Level0Entry>` | `blocks.finish_checked()`, line 1831 | `SkipIndex::build(l0)`, line 1832, moved by value | already optimal |
| `skip` | `SkipIndex` | line 1832 | line 1889 (`skip.encode()`) | yes, marginally: nothing reads it after its own push_section call |
| `page_dir` | `PageDir` | `blocks.finish_checked()`, line 1831 | line 1894-1898 (`page_dir.encode()`) | yes, marginally, same reasoning |
| `postings_fields` | `BTreeMap<u32, FieldTerms>` | lines 1838-1853 | line 1916 (`encode_postings_section(&postings_fields, ...)`) | no: it is the last section built |
| `object` | `Vec<u8>` (assembled section bytes) | every `push_section` call, lines 1863-1927 | line 1960 (`write_footer_and_trailer(&mut object, &footer)`), then returned at line 1963 | no: it is the function's return value |
| `sections` | `Vec<SectionDesc>` | every `push_section` call, lines 1863-1927 | moved into `LogFooter.sections` at line 1953 | no: needed for the footer |

## 1_stream (1 streams, 20000 records/stream)

Sampling OFF: block stage delta (after_blocks - after_resolve_rows) = 13946248 bytes; trailing sections delta (after_sections - after_blocks) = -1123831 bytes

Encoded object bytes: sampling OFF 122896 bytes, sampling ON 122896 bytes (byte-identical: true)

Number of blocks: 3; encoded object length: 122896 bytes

| section | encoded length (bytes) |
|---|---|
| STREAM_DIR | 96 |
| FIELD_DIR | 84 |
| BLOCKS | 73717 |
| SKIP_IDX | 129 |
| PAGE_DIR | 399 |
| BLOOM | 48263 |

### Block stage

| bucket | bytes | allocs | share of stage delta | pre-registered band | status |
|---|---|---|---|---|---|
| BLOCK_MATERIALIZE (per-row columnar occurrences, stamp output, plan bookkeeping) | 6803720 | 40023 | 48.79% | - | not pre-registered |
| BLOCK_VECS (column-major value pages, transient per block) | 5231896 | 78 | 37.51% | - | not pre-registered |
| BLOCK_ENCODE (write_block_columnar output, held until BlocksBuilder::push) | 9488834 | 20590 | 68.04% | encoded block bytes held before the object is assembled [under 5%] | above band |
| BLOCK_POSTINGS (postings_terms/postings_capped growth) | 0 | 0 | 0.00% | postings accumulators [5%, 25%] | below band |
| BLOCK_BLOOM (per-block BloomBuilder + bloom_entries.push) | 196560 | 280450 | 1.41% | bloom inputs [35%, 65%] | below band |
| BLOCK_DIRS_STATS (first_blk/last_blk/col_blocks growth) | 192 | 4 | 0.00% | directories and stats [under 10%] | inside |
| BLOCK_DICT_BUILDER (row-group string dictionary interning) | 852238 | 42 | 6.11% | row-group dictionaries and their builders [5%, 25%] | inside |
| BLOCK_ASSEMBLY_ENCODED (periodic flush_group, net of dict-builder release) | 0 | 0 | 0.00% | encoded block bytes held before the object is assembled [under 5%] | inside |
| BLOCK_ITERATION_TEARDOWN (per-row scratch Vecs dropped at loop body close, between one iteration's tail sample and the next iteration's start sample; mixes several structures' frees, not separable without restructuring the loop) | -8627832 | 0 | -61.86% | - | not pre-registered |
| REMAINDER (unattributed) | 640 | - | 0.00% | - | within 5% of stage delta |

| structure | len | capacity | capacity > 2x len? |
|---|---|---|---|
| blocks.pending | 3 | 4 | no |
| bloom_covered | 3 | n/a (map) | n/a |
| bloom_entries | 3 | 4 | no |
| col_blocks | 4 | 7 | no |
| col_present | 4 | 7 | no |
| first_blk | 1 | 3 | CAPACITY MISS |
| last_blk | 1 | 3 | CAPACITY MISS |
| postings_terms | 0 | n/a (map) | n/a |

Per-block transient high-water (largest live-bytes seen inside one block iteration, minus that iteration's start, max over all iterations): 10890494 bytes

### Trailing sections

| bucket | bytes | allocs | share of stage delta | pre-registered band | status |
|---|---|---|---|---|---|
| SECTION_STREAM_DIR_BUILD (StreamEntry constrution, blob.to_vec() copy) | 496 | 3 | -0.04% | stream directory copies | not pre-registered |
| SECTION_FIELD_DIR_BUILD (FieldEntry construction) | 284 | 6 | -0.03% | - | not pre-registered |
| SECTION_BLOCKS_SKIP_BUILD (BlocksBuilder::finish_checked + SkipIndex::build) | -1390597 | 88 | 123.74% | - | not pre-registered |
| SECTION_POSTINGS_FIELDS_BUILD (postings_fields assembly from postings_terms) | 0 | 0 | -0.00% | - | not pre-registered |
| SECTION_STREAM_DIR_ENCODE (StreamDir::encode second blob copy) | 418 | 4 | -0.04% | stream directory copies | not pre-registered |
| SECTION_REMAINING_DIRS_ENCODE (FIELD_DIR+BLOCKS+SKIP_IDX+PAGE_DIR push_section) | 186976 | 6 | -16.64% | - | not pre-registered |
| SECTION_BLOOM_ENCODE (BLOOM push_section) | 78976 | 1 | -7.03% | - | not pre-registered |
| SECTION_POSTINGS_ENCODE (POSTINGS push_section, when any indexed field present) | 0 | 0 | -0.00% | - | not pre-registered |
| REMAINDER (unattributed) | -384 | - | 0.03% | - | within 5% of stage delta |

| structure | len | capacity | capacity > 2x len? |
|---|---|---|---|
| object | 122688 | 147794 | no |
| postings_fields | 0 | n/a (map) | n/a |
| sections | 6 | 8 | no |

Stream directory copies (SECTION_STREAM_DIR_BUILD + SECTION_STREAM_DIR_ENCODE) share of trailing sections delta: -0.08% (pre-registered: at least 70% of the trailing sections delta at 20000 streams is stream directory copies and per-stream block-range bookkeeping): below band

Method note: "per-stream block-range bookkeeping" (`first_blk`/`last_blk`) is built incrementally during the BLOCK stage, under `BLOCK_DIRS_STATS`, not during the trailing sections measured here; if the pre-registration meant to count it against the sections delta, this figure alone understates it and `BLOCK_DIRS_STATS`'s own row (above) is the other half.

## 1000_streams (1000 streams, 20 records/stream)

Sampling OFF: block stage delta (after_blocks - after_resolve_rows) = 11271775 bytes; trailing sections delta (after_sections - after_blocks) = 568000 bytes

Encoded object bytes: sampling OFF 34256 bytes, sampling ON 34256 bytes (byte-identical: true)

Number of blocks: 3; encoded object length: 34256 bytes

| section | encoded length (bytes) |
|---|---|
| STREAM_DIR | 24397 |
| FIELD_DIR | 84 |
| BLOCKS | 8566 |
| SKIP_IDX | 119 |
| PAGE_DIR | 429 |
| BLOOM | 450 |

### Block stage

| bucket | bytes | allocs | share of stage delta | pre-registered band | status |
|---|---|---|---|---|---|
| BLOCK_MATERIALIZE (per-row columnar occurrences, stamp output, plan bookkeeping) | 6803720 | 40023 | 60.36% | - | not pre-registered |
| BLOCK_VECS (column-major value pages, transient per block) | 5231896 | 78 | 46.42% | - | not pre-registered |
| BLOCK_ENCODE (write_block_columnar output, held until BlocksBuilder::push) | 7327815 | 653 | 65.01% | encoded block bytes held before the object is assembled [under 5%] | above band |
| BLOCK_POSTINGS (postings_terms/postings_capped growth) | 0 | 0 | 0.00% | postings accumulators [5%, 25%] | below band |
| BLOCK_BLOOM (per-block BloomBuilder + bloom_entries.push) | 248978 | 241340 | 2.21% | bloom inputs [35%, 65%] | below band |
| BLOCK_DIRS_STATS (first_blk/last_blk/col_blocks growth) | 36984 | 22 | 0.33% | directories and stats [under 10%] | inside |
| BLOCK_DICT_BUILDER (row-group string dictionary interning) | 249574 | 32 | 2.21% | row-group dictionaries and their builders [5%, 25%] | below band |
| BLOCK_ASSEMBLY_ENCODED (periodic flush_group, net of dict-builder release) | 0 | 0 | 0.00% | encoded block bytes held before the object is assembled [under 5%] | inside |
| BLOCK_ITERATION_TEARDOWN (per-row scratch Vecs dropped at loop body close, between one iteration's tail sample and the next iteration's start sample; mixes several structures' frees, not separable without restructuring the loop) | -8627832 | 0 | -76.54% | - | not pre-registered |
| REMAINDER (unattributed) | 640 | - | 0.01% | - | within 5% of stage delta |

| structure | len | capacity | capacity > 2x len? |
|---|---|---|---|
| blocks.pending | 3 | 4 | no |
| bloom_covered | 3 | n/a (map) | n/a |
| bloom_entries | 3 | 4 | no |
| col_blocks | 4 | 7 | no |
| col_present | 4 | 7 | no |
| first_blk | 1000 | 1792 | no |
| last_blk | 1000 | 1792 | no |
| postings_terms | 0 | n/a (map) | n/a |

Per-block transient high-water (largest live-bytes seen inside one block iteration, minus that iteration's start, max over all iterations): 9696951 bytes

### Trailing sections

| bucket | bytes | allocs | share of stage delta | pre-registered band | status |
|---|---|---|---|---|---|
| SECTION_STREAM_DIR_BUILD (StreamEntry constrution, blob.to_vec() copy) | 114274 | 1002 | 20.12% | stream directory copies | not pre-registered |
| SECTION_FIELD_DIR_BUILD (FieldEntry construction) | 284 | 6 | 0.05% | - | not pre-registered |
| SECTION_BLOCKS_SKIP_BUILD (BlocksBuilder::finish_checked + SkipIndex::build) | 298019 | 109 | 52.47% | - | not pre-registered |
| SECTION_POSTINGS_FIELDS_BUILD (postings_fields assembly from postings_terms) | 0 | 0 | 0.00% | - | not pre-registered |
| SECTION_STREAM_DIR_ENCODE (StreamDir::encode second blob copy) | 113637 | 4 | 20.01% | stream directory copies | not pre-registered |
| SECTION_REMAINING_DIRS_ENCODE (FIELD_DIR+BLOCKS+SKIP_IDX+PAGE_DIR push_section) | 41538 | 6 | 7.31% | - | not pre-registered |
| SECTION_BLOOM_ENCODE (BLOOM push_section) | 632 | 1 | 0.11% | - | not pre-registered |
| SECTION_POSTINGS_ENCODE (POSTINGS push_section, when any indexed field present) | 0 | 0 | 0.00% | - | not pre-registered |
| REMAINDER (unattributed) | -384 | - | -0.07% | - | within 5% of stage delta |

| structure | len | capacity | capacity > 2x len? |
|---|---|---|---|
| object | 34045 | 48794 | no |
| postings_fields | 0 | n/a (map) | n/a |
| sections | 6 | 8 | no |

Stream directory copies (SECTION_STREAM_DIR_BUILD + SECTION_STREAM_DIR_ENCODE) share of trailing sections delta: 40.13% (pre-registered: at least 70% of the trailing sections delta at 20000 streams is stream directory copies and per-stream block-range bookkeeping): below band

Method note: "per-stream block-range bookkeeping" (`first_blk`/`last_blk`) is built incrementally during the BLOCK stage, under `BLOCK_DIRS_STATS`, not during the trailing sections measured here; if the pre-registration meant to count it against the sections delta, this figure alone understates it and `BLOCK_DIRS_STATS`'s own row (above) is the other half.

## 20000_streams (20000 streams, 1 records/stream)

Sampling OFF: block stage delta (after_blocks - after_resolve_rows) = 9714890 bytes; trailing sections delta (after_sections - after_blocks) = 6962836 bytes

Encoded object bytes: sampling OFF 521855 bytes, sampling ON 521855 bytes (byte-identical: true)

Number of blocks: 3; encoded object length: 521855 bytes

| section | encoded length (bytes) |
|---|---|
| STREAM_DIR | 499383 |
| FIELD_DIR | 84 |
| BLOCKS | 21529 |
| SKIP_IDX | 120 |
| PAGE_DIR | 279 |
| BLOOM | 255 |

### Block stage

| bucket | bytes | allocs | share of stage delta | pre-registered band | status |
|---|---|---|---|---|---|
| BLOCK_MATERIALIZE (per-row columnar occurrences, stamp output, plan bookkeeping) | 6793720 | 40023 | 69.93% | - | not pre-registered |
| BLOCK_VECS (column-major value pages, transient per block) | 5231896 | 78 | 53.85% | - | not pre-registered |
| BLOCK_ENCODE (write_block_columnar output, held until BlocksBuilder::push) | 3543688 | 395 | 36.48% | encoded block bytes held before the object is assembled [under 5%] | above band |
| BLOCK_POSTINGS (postings_terms/postings_capped growth) | 0 | 0 | 0.00% | postings accumulators [5%, 25%] | below band |
| BLOCK_BLOOM (per-block BloomBuilder + bloom_entries.push) | 1920594 | 260058 | 19.77% | bloom inputs [35%, 65%] | below band |
| BLOCK_DIRS_STATS (first_blk/last_blk/col_blocks growth) | 589944 | 30 | 6.07% | directories and stats [under 10%] | inside |
| BLOCK_DICT_BUILDER (row-group string dictionary interning) | 252240 | 25 | 2.60% | row-group dictionaries and their builders [5%, 25%] | below band |
| BLOCK_ASSEMBLY_ENCODED (periodic flush_group, net of dict-builder release) | 0 | 0 | 0.00% | encoded block bytes held before the object is assembled [under 5%] | inside |
| BLOCK_ITERATION_TEARDOWN (per-row scratch Vecs dropped at loop body close, between one iteration's tail sample and the next iteration's start sample; mixes several structures' frees, not separable without restructuring the loop) | -8617832 | 0 | -88.71% | - | not pre-registered |
| REMAINDER (unattributed) | 640 | - | 0.01% | - | within 5% of stage delta |

| structure | len | capacity | capacity > 2x len? |
|---|---|---|---|
| blocks.pending | 3 | 4 | no |
| bloom_covered | 3 | n/a (map) | n/a |
| bloom_entries | 3 | 4 | no |
| col_blocks | 4 | 7 | no |
| col_present | 4 | 7 | no |
| first_blk | 20000 | 28672 | no |
| last_blk | 20000 | 28672 | no |
| postings_terms | 0 | n/a (map) | n/a |

Per-block transient high-water (largest live-bytes seen inside one block iteration, minus that iteration's start, max over all iterations): 9117378 bytes

### Trailing sections

| bucket | bytes | allocs | share of stage delta | pre-registered band | status |
|---|---|---|---|---|---|
| SECTION_STREAM_DIR_BUILD (StreamEntry constrution, blob.to_vec() copy) | 2309274 | 20002 | 33.17% | stream directory copies | not pre-registered |
| SECTION_FIELD_DIR_BUILD (FieldEntry construction) | 284 | 6 | 0.00% | - | not pre-registered |
| SECTION_BLOCKS_SKIP_BUILD (BlocksBuilder::finish_checked + SkipIndex::build) | 271521 | 91 | 3.90% | - | not pre-registered |
| SECTION_POSTINGS_FIELDS_BUILD (postings_fields assembly from postings_terms) | 0 | 0 | 0.00% | - | not pre-registered |
| SECTION_STREAM_DIR_ENCODE (StreamDir::encode second blob copy) | 3415887 | 4 | 49.06% | stream directory copies | not pre-registered |
| SECTION_REMAINING_DIRS_ENCODE (FIELD_DIR+BLOCKS+SKIP_IDX+PAGE_DIR push_section) | 965882 | 6 | 13.87% | - | not pre-registered |
| SECTION_BLOOM_ENCODE (BLOOM push_section) | 372 | 1 | 0.01% | - | not pre-registered |
| SECTION_POSTINGS_ENCODE (POSTINGS push_section, when any indexed field present) | 0 | 0 | 0.00% | - | not pre-registered |
| REMAINDER (unattributed) | -384 | - | -0.01% | - | within 5% of stage delta |

| structure | len | capacity | capacity > 2x len? |
|---|---|---|---|
| object | 521650 | 998766 | no |
| postings_fields | 0 | n/a (map) | n/a |
| sections | 6 | 8 | no |

Stream directory copies (SECTION_STREAM_DIR_BUILD + SECTION_STREAM_DIR_ENCODE) share of trailing sections delta: 82.22% (pre-registered: at least 70% of the trailing sections delta at 20000 streams is stream directory copies and per-stream block-range bookkeeping): inside

Method note: "per-stream block-range bookkeeping" (`first_blk`/`last_blk`) is built incrementally during the BLOCK stage, under `BLOCK_DIRS_STATS`, not during the trailing sections measured here; if the pre-registration meant to count it against the sections delta, this figure alone understates it and `BLOCK_DIRS_STATS`'s own row (above) is the other half.

