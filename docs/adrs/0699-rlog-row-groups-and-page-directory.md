# ADR-0699: RLOG row groups with column-major pages and a page directory

Status: Proposed

Issue: #699. Refs #680, #693, #698, ADR-0087, ADR-0102, ADR-0107, ADR-0029,
ADR-0066, ADR-0010.

## Context

On the full ClickBench tenant (100M rows, 8424 RLOG objects of ~1.3 MB,
issue #680) every full-window statement moves the same 23 GB from the
object store and issues the same 47,113 GETs whether it projects zero
columns or five: `SELECT COUNT(*)` and `SELECT SUM(AdvEngineID), COUNT(*),
AVG(ResolutionWidth)` cost the same 83-115 s cold at 32-way fetch
concurrency. ClickHouse reads ~100 MB for the second statement on the same
instance type. Two properties of the format cause the gap, and neither is a
tuning matter.

**A page's location is only known after its block's bytes arrive.** A block
(docs/log-segment-format.md, "BLOCKS") is `header || pages`, and the header
holds the page descriptors: `column_id`, `enc`, `comp`, `len`,
`uncomp_len`. The reader can already *decode* a column subset
(`read_block_columns`, ADR-0087) and the SQL scan uses that to decode only
the projected columns. But the fetcher cannot *fetch* a subset: SKIP_IDX
level 0 carries `block_offset` and `block_len`, nothing finer, so
ADR-0107's block-range fetcher reads whole blocks and the projection saves
decode time on bytes that were already paid for on the wire. ADR-0107's
own scope section says this in as many words: "column-level fetch savings
are not available today" without a frozen-format change.

**Blocks are small and block-major.** A block targets 8192 records, so the
tenant has 17,731 blocks across 8424 objects (12,207 blocks minimum for
100M rows at the target). Within a block the pages are laid out one per
column, in column order, and consecutive blocks follow each other. A
column's pages are therefore scattered at ~600 KB intervals across the
object. Even with page offsets known, reading one column of one object is
one ranged GET per block, ~6 KB each; across the tenant that is 12,000+
GETs per column, and at the ~5 MB/s per connection the sweep on #680
measured (latency-bound, ~240 KB per GET) the request count, not the byte
count, would again set the floor. The query flamegraph confirms the per-byte
CPU (crc32c, memcpy, TLS, memset) is what the 16 cores spend at the 75 s
plateau, and the same accounting says that at ~6 KB per request the
per-request cost dominates instead.

So the two changes have to land together: page addressing without larger
I/O units moves the bound from bytes to requests; larger I/O units without
page addressing keep reading every column. Parquet's shape (row group,
column chunk, page) is the known answer to exactly this pair, and it is the
shape ClickHouse's mark files plus column files also reduce to.

What stays true: the writer's block-level pruning structures (SKIP_IDX
level 0 min/max and null counts, per-block BLOOM, POSTINGS block lists) are
what make point and range predicates cheap, and they are keyed to the
8192-record block. Making the block itself 32x larger to get bigger I/O
units would coarsen every prune by 32x. The row group has to be a new
level above the block, not a bigger block.

## Decision

### 1. A row group is a run of consecutive blocks whose pages are stored column-major

A row group is `group_target_blocks` consecutive blocks (default 32, so
262,144 records at the block target). Within a row group, the BLOCKS
section stores the pages **grouped by column, then by block**: all of
column `c`'s pages for blocks `g*32 .. g*32+31` are contiguous (a *column
chunk*), and the column chunks of one row group follow each other in
`column_id` order. A block is no longer a contiguous byte range; it is a
logical unit that exists in SKIP_IDX (level 0, unchanged: min/max ts,
min/max stream_ref, `record_count`, per-column stats), in BLOOM (per
block, unchanged), and in POSTINGS (block lists, unchanged).

Consequences for the reader:
- A projection of `k` columns over a row group is `k` contiguous ranges
  (one per column chunk), each ~`32 x page_size`, instead of 32 whole
  blocks. For a 105-column tenant and a 2-column statement that is 2
  ranges of ~200 KB per row group instead of 32 x 600 KB.
- Block-level pruning is unchanged in granularity: a pruned block's pages
  are skipped *within* the column chunk read (the reader has the page
  table, decision 2, so it walks past them without decoding) or, when a
  whole row group is pruned, the chunk is not fetched at all. Skipping a
  pruned block's bytes on the wire inside a chunk is left to the
  coalescing fetcher: adjacent surviving pages coalesce into one range,
  a gap larger than the coalescing threshold splits it, the same policy
  ADR-0107 already applies across blocks.
- Page compression, encodings, and the per-column presence bitmap page are
  unchanged; a page is a page.

Consequences for the writer: it buffers `group_target_blocks` blocks'
worth of encoded pages before writing a row group (~30-40 MB compressed at
the tenant's page sizes; the #682 fix made the pre-encoding working set
one block, and this adds one row group of *encoded* pages on top, which is
bounded and stated). The last row group of an object is short. An object
with fewer blocks than `group_target_blocks` has one row group; the layout
is identical, so small L0 flush objects pay nothing for the level.

### 2. A PAGE_DIR section, with per-page checksums

A new mandatory section `PAGE_DIR` (kind 10; ADR-0029's rule makes an
additive section kind free of a version bump, but decision 3 bumps anyway)
lists, per row group, per column chunk, per page:

```
PAGE_DIR (uncompressed form):
  group_count: varint
  group_count groups:
    first_block: varint            index of the group's first block
    block_count: varint
    chunk_count: varint
    chunk_count chunks:
      column_id:   varint
      offset:      varint          absolute offset of the chunk's first page, into BLOCKS
      page_count:  varint          == number of blocks in this group that carry the column
      page_count pages:
        block:       varint        block index within the group (ascending)
        enc:         u8
        comp:        u8
        len:         varint        stored page bytes
        uncomp_len:  varint
        crc32c:      u32           over the page's stored bytes
```

Page offsets are derived: a chunk's pages are contiguous from `offset` in
listed order. The block header goes away in the new layout: its fields all
live here now, and a page that a query does not want is never read, so
nothing is lost by not having them inline.

Integrity, the question #699 named. Today one crc covers a whole block and
sits in SKIP_IDX level 0; a page-subset read could not verify it without
fetching the whole block, which defeats the point (the same reason BLOCKS
has no whole-section crc, "Checksum coverage map"). Under this ADR every
byte a reader interprets is still covered by a checksum it can verify on
its access path (ADR-0010 section 4):

| bytes | checksum | where | when verified |
|---|---|---|---|
| PAGE_DIR stored bytes | `Section.crc32c` | footer section entry | before decoding the section |
| one page's stored bytes | that page's `crc32c` | PAGE_DIR | before decompressing the page |

The block crc in SKIP_IDX level 0 is retained and is now defined as
crc32c over the concatenation of the block's pages in `column_id` order
(what a whole-block read assembles). A whole-block reader verifies it; a
page-subset reader verifies per page. The `enc`/`comp` bytes now sit under
the PAGE_DIR section crc rather than the block crc; a flipped tag still
fails a checksum before it can cause a misdecode. PAGE_DIR is read whole
on every open, the way SKIP_IDX is, and is sized by the page count: ~14
bytes per page, so ~3 KB for a two-block L0 object and ~1.5 MB for a
10M-row L1 part with 105 columns, read once per object per query.

A corrupt or undecodable PAGE_DIR is a loud `Corrupted` error, like
SKIP_IDX: without it pages cannot be located at all.

### 3. RLOG trailer version 4; the version-3 reader stays until the format floor passes it

The BLOCKS layout and the block header change, so this is a versioned
change, not an additive one, whatever PAGE_DIR's kind number implies.
`footer::VERSION` becomes 4 and `SUPPORTED_VERSIONS` becomes `{3, 4}`.

Migration class A (ADR-0066 decision 4): bulk data objects, converged by
retention, rewrite-on-touch, and `maintain migrate`; compaction (L0 to L1)
always emits version 4, so a compacted tenant converges in one pass. The
version-3 reader (block-major, header-in-block, block crc over the
contiguous block) remains as the N-1 reader per ADR-0066 decision 1 and is
deleted only by its own reviewed change once every bucket's recorded
format floor is at 4 (decision 3). ADR-0027's single-version regime, if
still in force at implementation time, allows the shortcut of wiping and
re-ingesting instead; the ADR does not depend on which regime applies,
because the reader keeps both paths either way and the cost of the second
path is a `match` on the version byte plus the existing block reader.

`SUPPORTED_VERSIONS`, the writer, the reader gate, `audit-versions`,
`migrate`, and the compactor's `OUTPUT_FORMAT_VERSION` all read the one
constant (the format-change skill's single-source rule), so the bump is
one edit.

### 4. Row group size and the block target are writer configuration; the defaults are the numbers above

`RlogConfig` gains `group_target_blocks` (default 32). `block_target_records`
stays 8192 and the block-level caps are untouched (`MAX_RECORDS`,
`MAX_PAGES`, the 64 MiB per-page uncompressed cap). The doc's "cap 8 MiB
uncompressed" per block is a writer target and stays. Nothing in this ADR
changes how big a block is; it changes how blocks are placed.

The bulk loader and the ingest router hand the writer whatever they flush;
a flush smaller than a row group produces a one-group object. Compaction
is where full row groups arise for the ClickBench shape, and the
`--compaction post` measurement on #680 is where the number is produced.

### 5. The fetcher reads column chunks; the plan phase reads PAGE_DIR, not pages

`LogSegmentFetcher`'s block-range path (ADR-0107) gains the version-4
shape: the suffix probe brings footer plus SKIP_IDX plus PAGE_DIR (three
small sections; the probe length is raised to cover a typical PAGE_DIR, and
falls back to a second ranged GET when it does not), then one coalesced
range per surviving `(row group, projected column)`, with pruned blocks'
pages skipped inside the range by the coalescing rule. The
`ColumnSelection` the scan already passes to decode becomes the fetch
selection too.

`plan_segment` (#691, #693) needs only survivor counts, and under this ADR
those come from SKIP_IDX plus PAGE_DIR without touching any page. That is
#693 part 2's "predicate-free plan needs no read" made structural: the plan
phase reads three small sections per object and never a page, for every
statement, not only the predicate-free ones.

The RSEG format (metrics) is out of scope; it already has per-series
ranges and its own fetcher.

## Rejected alternatives

1. **Bigger blocks alone** (raise `block_target_records` to 262,144). One
   change, no new level. Rejected because every block-keyed prune coarsens
   32x: SKIP_IDX min/max, per-block bloom sizing (a bloom over 262k
   records' tokens is either huge or useless), POSTINGS block lists.
   ClickBench's `WHERE` statements and every logs point query would pay
   for the full-scan statements' gain.

2. **A page directory alone, block-major layout kept** (the #699 issue
   text's first proposal). Gets the byte reduction (10-100x on narrow
   projections) and needs no version bump. Rejected as the whole answer
   because it moves the bound from bytes to requests: ~6 KB pages at ~240
   KB per GET of effective size means one column costs the same 12,000+
   round trips the whole object costs today. It is, however, exactly
   decision 2's section, and an implementation may land decision 2 on the
   version-3 layout first as a stepping stone if the reviewers prefer two
   smaller changes; the fetcher gains nothing measurable until decision 1
   follows.

3. **A separate object per column** (column files, the ClickHouse
   MergeTree shape). Maximal locality, but it multiplies commit records and
   object-store keys by the column count (105x on this tenant), breaks the
   one-object-one-commit-record contract of docs/catalog-and-mvcc.md, and
   makes the object-count-proportional plan phase 105x worse. The row group
   gets the locality within one object.

4. **Keep the block crc as the only integrity check and accept
   unverifiable page reads**. Rejected: ADR-0010 section 4 is a rule, not
   a preference, and a page-subset read that cannot verify what it decoded
   is the case it exists to forbid.

5. **Store the page table inside each row group instead of one PAGE_DIR
   section.** Saves a section but means a row group's table is at a
   position the reader has to find first, which is the header-in-block
   problem one level up. A single section under one crc, read whole with
   the footer, is what SKIP_IDX already does and what the probe already
   fetches.

## Consequences

- Bytes per statement on a compacted ClickBench tenant drop from the
  whole dataset to the projected columns: `q2`-class statements from ~23 GB
  to a few hundred MB; `COUNT(*)`-class to PAGE_DIR plus SKIP_IDX only
  (and to nothing at all once #698 answers them from catalog stats).
- Requests per statement drop from ~47,000 to roughly `row_groups x
  projected_columns`: ~400 row groups on a compacted 100M-row tenant, so
  ~800 ranges for a two-column statement, which at 32-way fetch
  concurrency is seconds, not minutes. The plan phase becomes three small
  reads per object.
- The writer holds one row group of encoded pages in addition to #682's
  one block of cells; the number is stated per configuration, not
  unbounded, and the memory test from #682 is extended to pin it.
- Version-3 objects stay readable through the N-1 reader; compaction
  rewrites them to version 4; `audit-versions` reports the mix. A tenant
  that is never compacted keeps paying version-3 costs, which the `post`
  versus `pre` compaction rows of the bench report will show.
- The acceptance anchor is the existing columnar-versus-row differential
  proptest plus a new one: an object written at version 4 and read through
  the page-subset path yields byte-identical decoded columns to the same
  object read whole, for every projection, at every row-group boundary
  case (one block, `group_target_blocks - 1`, exactly one group, one over).
  Corrupt-input tests cover a flipped page byte (fails the page crc, never
  a misdecode), a flipped PAGE_DIR byte (fails the section crc), and a
  truncated last row group.
- Not decided here: the request coalescing threshold across pruned blocks
  inside a chunk (ADR-0107's existing gap policy applies until measured),
  and whether SKIP_IDX level 1 (per 64 blocks) should be realigned to row
  groups; both are tunings on top of this layout, not part of it.

## Diagram

```
version 3 (block-major)                 version 4 (row groups, column-major)

BLOCKS:                                 BLOCKS:
  block 0: hdr | c0 | c1 | ... | c104     row group 0 (blocks 0..31):
  block 1: hdr | c0 | c1 | ... | c104       chunk c0:   b0 b1 b2 ... b31
  block 2: hdr | c0 | c1 | ... | c104       chunk c1:   b0 b1 b2 ... b31
  ...                                       ...
                                            chunk c104: b0 b1 b2 ... b31
                                          row group 1 (blocks 32..63): ...
SKIP_IDX L0: per block, block range+crc  SKIP_IDX L0: per block, stats + crc (pages in column order)
(page offsets: inside each block header) PAGE_DIR: per group, per chunk, per page: offset/len/enc/comp/crc

read 2 columns of 32 blocks:            read 2 columns of 32 blocks:
  32 GETs x whole block (~600 KB each)    2 GETs x one chunk (~200 KB each)
```
